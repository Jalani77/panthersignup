"""
GSU Banner SSB scraper (GoSolar/PAWS) + RateMyProfessors GraphQL client.
Uses the public-facing Ellucian Banner StudentRegistrationSsb REST API.
Session cookies are obtained automatically (no student login required).
"""

import asyncio
import base64
import logging
import os
import re
import time
from typing import Any, Optional

import aiohttp
import aiosqlite

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
GSU_BANNER_BASE = "https://registration.gosolar.gsu.edu/StudentRegistrationSsb/ssb"
RMP_GRAPHQL_URL = "https://www.ratemyprofessors.com/graphql"
# GSU school ID on RMP is 360 → base64("School-360")
RMP_GSU_SCHOOL_ID = "U2Nob29sLTM2MA=="

# User-Agent that mimics a real browser to avoid bot detection
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Known GSU Fall/Summer 2026 term codes (Banner 6-digit format: YYYYTT)
# Fall 2026 → 202608, Summer 2026 → 202605, Spring 2026 → 202601
TERM_PRIORITIES = ["202608", "202605", "202601"]


# ── SQLite cache helpers ──────────────────────────────────────────────────────

DB_PATH = os.getenv("ONLYPANTHER_DB_PATH", "onlypanther.db")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS classes (
                crn TEXT PRIMARY KEY,
                term TEXT,
                subject TEXT,
                course_number TEXT,
                title TEXT,
                professor TEXT,
                seats_available INTEGER,
                seats_max INTEGER,
                schedule_type TEXT,
                days TEXT,
                begin_time TEXT,
                end_time TEXT,
                building TEXT,
                room TEXT,
                credit_hours REAL,
                campus TEXT,
                cached_at REAL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS professors (
                name TEXT PRIMARY KEY,
                rmp_id TEXT,
                avg_rating REAL,
                avg_difficulty REAL,
                num_ratings INTEGER,
                would_take_again REAL,
                department TEXT,
                cached_at REAL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS terms_cache (
                code TEXT PRIMARY KEY,
                description TEXT,
                cached_at REAL
            )
            """
        )
        await db.commit()


async def get_cached_classes(term: str, subject: str = "") -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if subject:
            cur = await db.execute(
                "SELECT * FROM classes WHERE term=? AND subject=? AND cached_at > ?",
                (term, subject.upper(), time.time() - 3600),
            )
        else:
            cur = await db.execute(
                "SELECT * FROM classes WHERE term=? AND cached_at > ?",
                (term, time.time() - 3600),
            )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


async def cache_classes(classes: list[dict]):
    async with aiosqlite.connect(DB_PATH) as db:
        now = time.time()
        for c in classes:
            await db.execute(
                """
                INSERT OR REPLACE INTO classes
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    c["crn"],
                    c["term"],
                    c["subject"],
                    c["course_number"],
                    c["title"],
                    c["professor"],
                    c["seats_available"],
                    c["seats_max"],
                    c["schedule_type"],
                    c["days"],
                    c["begin_time"],
                    c["end_time"],
                    c["building"],
                    c["room"],
                    c["credit_hours"],
                    c["campus"],
                    now,
                ),
            )
        await db.commit()


async def get_cached_prof(name: str) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM professors WHERE name=? AND cached_at > ?",
            (name, time.time() - 86400),
        )
        row = await cur.fetchone()
        return dict(row) if row else None


async def cache_prof(data: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO professors
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                data["name"],
                data.get("rmp_id"),
                data.get("avg_rating"),
                data.get("avg_difficulty"),
                data.get("num_ratings"),
                data.get("would_take_again"),
                data.get("department"),
                time.time(),
            ),
        )
        await db.commit()


# ── Banner SSB session management ─────────────────────────────────────────────

class BannerSession:
    """
    Maintains a persistent aiohttp session with Banner SSB cookies.
    The Banner SSB public class-search requires:
      1) GET /classSearch/classSearch   → receive JSESSIONID cookie
      2) POST /term/search?mode=search  → activate term; receive authorisation
      3) GET /searchResults/searchResults → paginated results
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._active_term: Optional[str] = None

    async def _make_session(self) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(ssl=False)
        return aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": UA},
            timeout=aiohttp.ClientTimeout(total=30),
        )

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = await self._make_session()
        return self._session

    async def initialise(self) -> bool:
        """Hit the classSearch page to obtain JSESSIONID."""
        session = await self.get_session()
        try:
            async with session.get(
                f"{GSU_BANNER_BASE}/classSearch/classSearch"
            ) as resp:
                return resp.status == 200
        except Exception as exc:
            logger.warning("Banner initialise failed: %s", exc)
            return False

    async def set_term(self, term_code: str) -> bool:
        """POST term to Banner to unlock searchResults for that term."""
        if self._active_term == term_code:
            return True
        session = await self.get_session()
        try:
            async with session.post(
                f"{GSU_BANNER_BASE}/term/search?mode=search",
                data=f"term={term_code}&studyPath=&studyPathText=&startDatepicker=&endDatepicker=",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ) as resp:
                if resp.status == 200:
                    self._active_term = term_code
                    return True
                return False
        except Exception as exc:
            logger.warning("Banner set_term failed: %s", exc)
            return False

    async def reset(self):
        """Reset form state so we can search different subjects."""
        session = await self.get_session()
        try:
            async with session.post(
                f"{GSU_BANNER_BASE}/classSearch/resetDataForm"
            ) as _:
                pass
        except Exception:
            pass
        self._active_term = None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# Module-level Banner session (reused across calls)
_banner_session = BannerSession()


async def close_banner_session():
    """Close the shared Banner session on application shutdown."""
    try:
        await _banner_session.close()
    except Exception as exc:
        logger.warning("Failed to close Banner session cleanly: %s", exc)


# ── Term discovery ─────────────────────────────────────────────────────────────

async def fetch_terms() -> list[dict]:
    """Return available terms from Banner, sorted newest first."""
    session = await _banner_session.get_session()
    # Ensure we have a JSESSIONID
    await _banner_session.initialise()
    try:
        async with session.get(
            f"{GSU_BANNER_BASE}/classSearch/getTerms",
            params={"offset": "1", "max": "20", "searchTerm": ""},
        ) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                return data
    except Exception as exc:
        logger.warning("fetch_terms failed: %s", exc)
    return []


async def get_best_term() -> str:
    """Return the most appropriate upcoming term code."""
    terms = await fetch_terms()
    codes = {t["code"] for t in terms}
    for code in TERM_PRIORITIES:
        if code in codes:
            return code
    # Fall back to first available
    if terms:
        return terms[0]["code"]
    return TERM_PRIORITIES[0]


# ── Subject list ───────────────────────────────────────────────────────────────

async def fetch_subjects(term_code: str) -> list[dict]:
    """Return all subjects offered for the given term."""
    session = await _banner_session.get_session()
    await _banner_session.initialise()
    subjects = []
    offset = 1
    batch = 500
    while True:
        try:
            async with session.get(
                f"{GSU_BANNER_BASE}/classSearch/get_subject",
                params={
                    "searchTerm": "",
                    "term": term_code,
                    "offset": str(offset),
                    "max": str(batch),
                },
            ) as resp:
                if resp.status != 200:
                    break
                batch_data = await resp.json(content_type=None)
                if not batch_data:
                    break
                subjects.extend(batch_data)
                if len(batch_data) < batch:
                    break
                offset += batch
        except Exception as exc:
            logger.warning("fetch_subjects failed: %s", exc)
            break
    return subjects


# ── Class search ───────────────────────────────────────────────────────────────

def _parse_days(meeting: dict) -> str:
    day_map = [
        ("monday", "M"),
        ("tuesday", "T"),
        ("wednesday", "W"),
        ("thursday", "R"),
        ("friday", "F"),
        ("saturday", "S"),
        ("sunday", "U"),
    ]
    return "".join(code for key, code in day_map if meeting.get(key))


def _fmt_time(t: str) -> str:
    """Convert Banner time '0915' → '9:15 AM'."""
    if not t or len(t) != 4:
        return t or "TBA"
    h, m = int(t[:2]), t[2:]
    period = "AM" if h < 12 else "PM"
    h12 = h if 1 <= h <= 12 else (12 if h == 0 else h - 12)
    return f"{h12}:{m} {period}"


def _flatten_class(raw: dict) -> dict:
    """Flatten a Banner searchResults data item into our schema."""
    faculty = raw.get("faculty", [])
    prof_name = ""
    if faculty:
        primary = next((f for f in faculty if f.get("primaryIndicator")), faculty[0])
        prof_name = primary.get("displayName", "")

    meetings = raw.get("meetingsFaculty", [])
    days, begin, end, building, room = "TBA", "TBA", "TBA", "TBA", "TBA"
    if meetings:
        mt = meetings[0].get("meetingTime", {})
        days = _parse_days(mt) or "TBA"
        begin = _fmt_time(mt.get("beginTime", ""))
        end = _fmt_time(mt.get("endTime", ""))
        building = mt.get("buildingDescription") or mt.get("building") or "TBA"
        room = mt.get("room") or "TBA"

    credit = raw.get("creditHourLow") or raw.get("creditHours") or 0

    # Decode HTML entities in title (Banner sometimes returns &amp; etc.)
    import html as _html
    title = _html.unescape(raw.get("courseTitle", "") or "")

    return {
        "crn": raw.get("courseReferenceNumber", ""),
        "term": raw.get("term", ""),
        "subject": raw.get("subject", ""),
        "course_number": raw.get("courseNumber", ""),
        "title": title,
        "professor": prof_name,
        "seats_available": raw.get("seatsAvailable", 0),
        "seats_max": raw.get("maximumEnrollment", 0),
        "schedule_type": raw.get("scheduleTypeDescription", ""),
        "days": days,
        "begin_time": begin,
        "end_time": end,
        "building": building,
        "room": room,
        "credit_hours": credit,
        "campus": raw.get("campusDescription", ""),
    }


async def fetch_classes_for_subject(
    session: aiohttp.ClientSession, term: str, subject: str
) -> list[dict]:
    """
    Fetch all sections for a given subject/term from Banner SSB.
    Resets the form before each subject to ensure Banner returns fresh results.
    """
    # Reset form state before each new subject query
    try:
        async with session.post(
            f"{GSU_BANNER_BASE}/classSearch/resetDataForm"
        ) as _:
            pass
    except Exception:
        pass

    results = []
    offset = 0
    page_size = 500

    while True:
        try:
            async with session.get(
                f"{GSU_BANNER_BASE}/searchResults/searchResults",
                params={
                    "txt_subject": subject,
                    "txt_term": term,
                    "startDatepicker": "",
                    "endDatepicker": "",
                    "pageOffset": str(offset),
                    "pageMaxSize": str(page_size),
                    "sortColumn": "subjectDescription",
                    "sortDirection": "asc",
                },
            ) as resp:
                if resp.status != 200:
                    break
                data = await resp.json(content_type=None)
                if not data.get("success"):
                    break
                items = data.get("data") or []
                results.extend(items)
                total = data.get("totalCount", 0)
                fetched = data.get("sectionsFetchedCount", len(items))
                offset += fetched
                if offset >= total or not items:
                    break
        except Exception as exc:
            logger.warning("fetch_classes subject=%s: %s", subject, exc)
            break
    return [_flatten_class(r) for r in results]


async def scrape_all_classes(
    term: Optional[str] = None,
    subjects: Optional[list[str]] = None,
    max_subjects: int = 200,
) -> list[dict]:
    """
    Main entry point: scrapes GSU Banner for classes.
    - Discovers the best upcoming term if none provided.
    - Fetches up to `max_subjects` subject codes.
    - Returns a flat list of class dicts.
    """
    await init_db()
    await _banner_session.initialise()

    if not term:
        term = await get_best_term()
        logger.info("Using term: %s", term)

    # Check cache first (full-term cache valid for 1 hour)
    cached = await get_cached_classes(term)
    if len(cached) > 50:
        logger.info("Returning %d cached classes for term %s", len(cached), term)
        return cached

    # Activate term in session
    ok = await _banner_session.set_term(term)
    if not ok:
        logger.error("Could not activate term %s in Banner", term)
        return cached

    if subjects is None:
        all_subjects = await fetch_subjects(term)
        subjects = [s["code"] for s in all_subjects[:max_subjects]]
        logger.info("Fetching %d subjects for term %s", len(subjects), term)

    session = await _banner_session.get_session()
    all_classes: list[dict] = []

    for i, subj in enumerate(subjects):
        classes = await fetch_classes_for_subject(session, term, subj)
        all_classes.extend(classes)
        if classes:
            logger.info(
                "Subject %s: %d sections (total so far: %d, progress: %d/%d)",
                subj, len(classes), len(all_classes), i + 1, len(subjects),
            )
        # Polite delay between requests
        await asyncio.sleep(0.2)

    if all_classes:
        await cache_classes(all_classes)

    logger.info("Total classes fetched: %d", len(all_classes))
    return all_classes


# ── RateMyProfessors GraphQL ──────────────────────────────────────────────────

RMP_AUTH = "Basic dGVzdDp0ZXN0"  # RMP public endpoint uses this static token

RMP_SCHOOL_QUERY = """
query NewSearchTeachersQuery($query: TeacherSearchQuery!) {
  newSearch {
    teachers(query: $query, first: 8) {
      edges {
        node {
          id
          firstName
          lastName
          avgRating
          avgDifficulty
          numRatings
          wouldTakeAgainPercent
          department
          school {
            name
            id
          }
        }
      }
    }
  }
}
"""


def _normalize_prof_name(display_name: str) -> tuple[str, str]:
    """Convert 'Last, First' → ('First', 'Last')."""
    if "," in display_name:
        last, first = display_name.split(",", 1)
        return first.strip(), last.strip()
    parts = display_name.strip().split()
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return display_name, ""


async def fetch_rmp_rating(prof_display_name: str) -> dict:
    """
    Fetch RMP data for a professor by display name.
    Returns a dict with rating fields.
    """
    cached = await get_cached_prof(prof_display_name)
    if cached:
        return cached

    first, last = _normalize_prof_name(prof_display_name)
    search_name = f"{first} {last}".strip()

    payload = {
        "query": RMP_SCHOOL_QUERY,
        "variables": {
            "query": {
                "text": search_name,
                "schoolID": RMP_GSU_SCHOOL_ID,
                "fallback": True,
            }
        },
    }

    result = {
        "name": prof_display_name,
        "rmp_id": None,
        "avg_rating": None,
        "avg_difficulty": None,
        "num_ratings": 0,
        "would_take_again": None,
        "department": None,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                RMP_GRAPHQL_URL,
                json=payload,
                headers={
                    "Authorization": RMP_AUTH,
                    "User-Agent": UA,
                    "Referer": "https://www.ratemyprofessors.com/",
                    "Origin": "https://www.ratemyprofessors.com",
                    "Content-Type": "application/json",
                },
                ssl=False,
            ) as resp:
                if resp.status != 200:
                    logger.warning("RMP returned HTTP %d for %s", resp.status, search_name)
                    await cache_prof(result)
                    return result
                data = await resp.json(content_type=None)
    except Exception as exc:
        logger.warning("RMP request failed for %s: %s", search_name, exc)
        await cache_prof(result)
        return result

    try:
        edges = (
            data.get("data", {})
            .get("newSearch", {})
            .get("teachers", {})
            .get("edges", [])
        )
        if not edges:
            await cache_prof(result)
            return result

        # Pick best match (same school preferred)
        node = None
        for edge in edges:
            n = edge.get("node", {})
            school = n.get("school") or {}
            if school.get("id") == RMP_GSU_SCHOOL_ID:
                node = n
                break
        if node is None:
            node = edges[0].get("node", {})

        result.update(
            {
                "rmp_id": node.get("id"),
                "avg_rating": node.get("avgRating"),
                "avg_difficulty": node.get("avgDifficulty"),
                "num_ratings": node.get("numRatings", 0),
                "would_take_again": node.get("wouldTakeAgainPercent"),
                "department": node.get("department"),
            }
        )
    except Exception as exc:
        logger.warning("RMP parse error for %s: %s", search_name, exc)

    await cache_prof(result)
    return result


async def batch_fetch_rmp(prof_names: list[str]) -> dict[str, dict]:
    """Fetch RMP ratings for multiple professors concurrently (rate-limited)."""
    sem = asyncio.Semaphore(5)

    async def _fetch(name: str) -> tuple[str, dict]:
        async with sem:
            data = await fetch_rmp_rating(name)
            await asyncio.sleep(0.2)
            return name, data

    tasks = [_fetch(n) for n in prof_names]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out = {}
    for r in results:
        if isinstance(r, tuple):
            out[r[0]] = r[1]
    return out
