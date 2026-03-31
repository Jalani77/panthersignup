"""
OnlyPanther FastAPI backend.
Routes:
  GET  /api/terms              – available Banner terms
  GET  /api/subjects?term=     – subjects for a term
  GET  /api/classes            – paginated class list (term, subject, query, page, size)
  GET  /api/prof/{name}        – RMP data for a professor
  GET  /api/profs/batch        – RMP data for a list of professors (POST body)
  POST /api/scrape             – trigger a fresh Banner scrape (background)
  GET  /api/health             – healthcheck
  GET  /                       – serve index.html
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from scraper import (
    batch_fetch_rmp,
    close_banner_session,
    fetch_rmp_rating,
    fetch_subjects,
    fetch_terms,
    get_best_term,
    get_cached_classes,
    init_db,
    scrape_all_classes,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)

# ── Lifespan ──────────────────────────────────────────────────────────────────

_scrape_lock = asyncio.Lock()
_scrape_in_progress = False
_last_scrape: float = 0.0
_startup_scrape_task: Optional[asyncio.Task] = None
_bootstrap_warm_task: Optional[asyncio.Task] = None
_bootstrap_cache_lock = asyncio.Lock()
_bootstrap_cache_payload: Optional[dict[str, Any]] = None
_bootstrap_cache_ts: float = 0.0
_bootstrap_cache_ttl_s = 120.0
_bootstrap_page_size = 30


def _make_classes_payload(term: str, classes: list[dict], page: int, size: int) -> dict[str, Any]:
    total = len(classes)
    start = (page - 1) * size
    page_data = classes[start : start + size]

    subj_counts: dict[str, int] = {}
    rarity_counts: dict[str, int] = {}
    for c in classes:
        subj_counts[c["subject"]] = subj_counts.get(c["subject"], 0) + 1
        rarity_counts[c["rarity"]] = rarity_counts.get(c["rarity"], 0) + 1

    return {
        "term": term,
        "total": total,
        "page": page,
        "size": size,
        "classes": page_data,
        "subject_breakdown": subj_counts,
        "rarity_breakdown": rarity_counts,
        "scrape_in_progress": _scrape_in_progress,
    }


async def _build_bootstrap_payload(force_refresh: bool = False) -> dict[str, Any]:
    global _bootstrap_cache_payload, _bootstrap_cache_ts
    now = time.time()
    if (
        not force_refresh
        and _bootstrap_cache_payload is not None
        and (now - _bootstrap_cache_ts) < _bootstrap_cache_ttl_s
    ):
        return _bootstrap_cache_payload

    async with _bootstrap_cache_lock:
        now = time.time()
        if (
            not force_refresh
            and _bootstrap_cache_payload is not None
            and (now - _bootstrap_cache_ts) < _bootstrap_cache_ttl_s
        ):
            return _bootstrap_cache_payload

        term = await get_best_term()
        terms = await fetch_terms()
        if not terms:
            terms = [{"code": term, "description": term}]
        selected_term = next((t for t in terms if t.get("code") == term), terms[0])
        term = selected_term.get("code", term)
        subjects = await fetch_subjects(term)

        cached_classes = await get_cached_classes(term)
        if not cached_classes and not _scrape_in_progress:
            # Keep HTML response fast: trigger scrape in background if needed.
            asyncio.create_task(_background_scrape(term=term))

        enriched = [enrich_class(c) for c in cached_classes]
        classes_payload = _make_classes_payload(term, enriched, page=1, size=_bootstrap_page_size)
        payload = {
            "term": term,
            "terms": terms,
            "subjects": subjects,
            "classes_response": classes_payload,
            "generated_at": now,
        }
        _bootstrap_cache_payload = payload
        _bootstrap_cache_ts = now
        return payload


async def _refresh_bootstrap_payload():
    try:
        await _build_bootstrap_payload(force_refresh=True)
    except Exception as exc:
        logger.warning("Bootstrap payload refresh failed: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _bootstrap_warm_task, _startup_scrape_task
    await init_db()
    # Kick off a background scrape on startup (non-blocking)
    _startup_scrape_task = asyncio.create_task(_background_scrape())
    _bootstrap_warm_task = asyncio.create_task(_refresh_bootstrap_payload())
    try:
        yield
    finally:
        for task, label in (
            (_startup_scrape_task, "startup scrape"),
            (_bootstrap_warm_task, "bootstrap warmup"),
        ):
            if not task:
                continue
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            elif not task.cancelled():
                exc = task.exception()
                if exc:
                    logger.error(
                        "%s task failed: %s",
                        label,
                        exc,
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
        await close_banner_session()


async def _background_scrape(term: Optional[str] = None):
    global _bootstrap_cache_ts, _scrape_in_progress, _last_scrape
    async with _scrape_lock:
        if _scrape_in_progress:
            return
        _scrape_in_progress = True
    try:
        logger.info("Starting background Banner scrape (term=%s) ...", term or "auto")
        await scrape_all_classes(term=term)
        _last_scrape = time.time()
        # Force bootstrap data refresh after successful scrape.
        _bootstrap_cache_ts = 0.0
        asyncio.create_task(_refresh_bootstrap_payload())
        logger.info("Background scrape complete.")
    except asyncio.CancelledError:
        logger.info("Background scrape task cancelled.")
        raise
    except Exception as exc:
        logger.exception("Background scrape error: %s", exc)
    finally:
        async with _scrape_lock:
            _scrape_in_progress = False


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="OnlyPanther API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Gamification helpers ──────────────────────────────────────────────────────

def compute_xp(cls: dict) -> int:
    """XP awarded for enrolling in / viewing a class quest."""
    seats = cls.get("seats_available", 0)
    max_seats = cls.get("seats_max", 1) or 1
    fill_pct = 1 - (seats / max_seats)
    # More XP for rarer (nearly full) classes
    base = 50
    rarity_bonus = int(fill_pct * 100)
    credit_bonus = int((cls.get("credit_hours") or 0) * 10)
    return base + rarity_bonus + credit_bonus


def class_rarity(cls: dict) -> str:
    seats = cls.get("seats_available", 0)
    max_s = cls.get("seats_max", 1) or 1
    pct = seats / max_s
    if seats == 0:
        return "LEGENDARY"
    if pct <= 0.05:
        return "EPIC"
    if pct <= 0.15:
        return "RARE"
    if pct <= 0.40:
        return "UNCOMMON"
    return "COMMON"


def prof_rarity(avg_rating: Optional[float]) -> str:
    if avg_rating is None:
        return "UNKNOWN"
    if avg_rating >= 4.5:
        return "LEGENDARY"
    if avg_rating >= 4.0:
        return "EPIC"
    if avg_rating >= 3.5:
        return "RARE"
    if avg_rating >= 3.0:
        return "UNCOMMON"
    return "COMMON"


RARITY_COLORS = {
    "LEGENDARY": "#FFD700",
    "EPIC": "#B44FEF",
    "RARE": "#3B82F6",
    "UNCOMMON": "#22C55E",
    "COMMON": "#94A3B8",
    "UNKNOWN": "#64748B",
}

BADGES = [
    {"id": "rare_avoider", "name": "Rare Avoider", "desc": "Enrolled in a RARE class", "icon": "💎"},
    {"id": "prof_slayer", "name": "Prof Slayer", "desc": "Took a Legendary professor", "icon": "⚔️"},
    {"id": "night_owl", "name": "Night Owl", "desc": "Enrolled in an evening class (after 6 PM)", "icon": "🦉"},
    {"id": "early_bird", "name": "Early Bird", "desc": "Enrolled in a class before 9 AM", "icon": "🐦"},
    {"id": "xp_grinder", "name": "XP Grinder", "desc": "Earned 500+ XP from class quests", "icon": "⚡"},
    {"id": "schedule_wizard", "name": "Schedule Wizard", "desc": "Built a 4+ class schedule with no conflicts", "icon": "🧙"},
    {"id": "streak_master", "name": "Streak Master", "desc": "Visited 7 days in a row", "icon": "🔥"},
    {"id": "polymath", "name": "Polymath", "desc": "Added classes from 4+ different subjects", "icon": "🎓"},
]

DAILY_CHALLENGES = [
    {"id": "dc_epic", "name": "Epic Hunter", "desc": "Find and preview an EPIC-rarity class", "xp": 75, "icon": "🔮"},
    {"id": "dc_seats", "name": "Seat Scout", "desc": "Find a class with fewer than 5 seats left", "xp": 100, "icon": "🎯"},
    {"id": "dc_prof", "name": "Prof Investigator", "desc": "Check the RMP rating for any professor", "xp": 50, "icon": "🔍"},
    {"id": "dc_schedule", "name": "Schedule Builder", "desc": "Add 3 classes to your schedule", "xp": 150, "icon": "📅"},
    {"id": "dc_legend", "name": "Legend Seeker", "desc": "Find a Legendary professor or class", "xp": 200, "icon": "👑"},
]


def enrich_class(cls: dict) -> dict:
    """Add gamification metadata to a class dict."""
    r = dict(cls)
    r["xp"] = compute_xp(cls)
    r["rarity"] = class_rarity(cls)
    r["rarity_color"] = RARITY_COLORS[r["rarity"]]
    return r


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok", "scrape_in_progress": _scrape_in_progress, "last_scrape": _last_scrape}


@app.get("/api/terms")
async def get_terms():
    terms = await fetch_terms()
    return {"terms": terms}


@app.get("/api/subjects")
async def get_subjects(term: str = Query(..., description="Banner term code e.g. 202608")):
    subjects = await fetch_subjects(term)
    return {"subjects": subjects}


@app.get("/api/classes")
async def get_classes(
    term: Optional[str] = None,
    subject: Optional[str] = None,
    query: Optional[str] = None,
    rarity: Optional[str] = None,
    open_only: bool = False,
    page: int = 1,
    size: int = 50,
):
    if not term:
        term = await get_best_term()

    classes = await get_cached_classes(term, subject or "")

    # If cache empty, trigger a targeted scrape only if the background scrape
    # is NOT already running (to avoid competing on the same Banner session).
    if not classes and not _scrape_in_progress:
        subjects_to_scrape = [subject] if subject else None
        classes = await scrape_all_classes(term=term, subjects=subjects_to_scrape)

    # Still empty? Return best-effort empty
    if not classes:
        return {
            "term": term,
            "total": 0,
            "page": page,
            "size": size,
            "classes": [],
            "scrape_in_progress": _scrape_in_progress,
        }

    # Enrich
    enriched = [enrich_class(c) for c in classes]

    # Filters
    if subject:
        enriched = [c for c in enriched if c["subject"].upper() == subject.upper()]
    if query:
        q = query.lower()
        enriched = [
            c for c in enriched
            if q in c["title"].lower()
            or q in c["professor"].lower()
            or q in c["subject"].lower()
            or q in c["crn"]
        ]
    if rarity:
        enriched = [c for c in enriched if c["rarity"] == rarity.upper()]
    if open_only:
        enriched = [c for c in enriched if c["seats_available"] > 0]

    total = len(enriched)
    start = (page - 1) * size
    page_data = enriched[start : start + size]

    # Subject breakdown for stats
    subj_counts: dict[str, int] = {}
    for c in enriched:
        subj_counts[c["subject"]] = subj_counts.get(c["subject"], 0) + 1

    rarity_counts: dict[str, int] = {}
    for c in enriched:
        rarity_counts[c["rarity"]] = rarity_counts.get(c["rarity"], 0) + 1

    return {
        "term": term,
        "total": total,
        "page": page,
        "size": size,
        "classes": page_data,
        "subject_breakdown": subj_counts,
        "rarity_breakdown": rarity_counts,
        "scrape_in_progress": _scrape_in_progress,
    }


@app.get("/api/prof/{name:path}")
async def get_prof(name: str):
    data = await fetch_rmp_rating(name)
    data["rarity"] = prof_rarity(data.get("avg_rating"))
    data["rarity_color"] = RARITY_COLORS[data["rarity"]]
    return data


class BatchProfRequest(BaseModel):
    names: list[str]


@app.post("/api/profs/batch")
async def get_profs_batch(req: BatchProfRequest):
    unique_names = list(dict.fromkeys(n.strip() for n in req.names if n and n.strip()))[:200]
    results = await batch_fetch_rmp(unique_names)
    enriched = {}
    for name, data in results.items():
        d = dict(data)
        d["rarity"] = prof_rarity(d.get("avg_rating"))
        d["rarity_color"] = RARITY_COLORS[d["rarity"]]
        enriched[name] = d
    return enriched


@app.post("/api/scrape")
async def trigger_scrape(
    background_tasks: BackgroundTasks,
    term: Optional[str] = None,
    force: bool = False,
):
    if _scrape_in_progress and not force:
        return {"status": "already_running"}
    background_tasks.add_task(_background_scrape, term)
    return {"status": "started", "term": term}


@app.get("/api/gamification/badges")
async def get_badges():
    return {"badges": BADGES}


@app.get("/api/gamification/challenges")
async def get_daily_challenges():
    return {"challenges": DAILY_CHALLENGES}


@app.get("/api/gamification/leaderboard")
async def get_leaderboard():
    """Return sample leaderboard (client-side XP tracking in localStorage)."""
    return {
        "leaderboard": [
            {"rank": 1, "name": "PantherKing42", "xp": 4200, "badge_count": 6},
            {"rank": 2, "name": "ScheduleWizard", "xp": 3800, "badge_count": 5},
            {"rank": 3, "name": "CRNHunter", "xp": 3100, "badge_count": 4},
            {"rank": 4, "name": "RarityChaser", "xp": 2750, "badge_count": 4},
            {"rank": 5, "name": "EpicPanther", "xp": 2400, "badge_count": 3},
        ]
    }


# ── Static file serving ───────────────────────────────────────────────────────

@app.get("/")
async def serve_frontend():
    html_path = Path(__file__).parent / "index.html"
    if html_path.exists():
        content = html_path.read_text(encoding="utf-8")
        try:
            payload = await _build_bootstrap_payload()
            payload_json = json.dumps(payload, separators=(",", ":")).replace("</", "<\\/")
            content = content.replace(
                "window.__BOOTSTRAP_DATA__ = null;",
                f"window.__BOOTSTRAP_DATA__ = {payload_json};",
                1,
            )
        except Exception as exc:
            logger.warning("Failed to embed bootstrap payload: %s", exc)
        return HTMLResponse(content=content)
    raise HTTPException(status_code=404, detail="index.html not found")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
