import requests
from bs4 import BeautifulSoup

# URL for PAWS/GoSolar GSU class data
URL = 'https://paws.gosolar.gsu.edu'

# Function to fetch course data

def fetch_course_data():
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Example scraping logic - this needs to be modified based on the actual page structure.
    classes = []
    for course in soup.find_all('div', class_='course-container'):
        crn = course.find('span', class_='crn').text
        title = course.find('h2', class_='course-title').text
        professor = course.find('span', class_='professor').text
        seats = course.find('span', class_='available-seats').text
        time = course.find('span', class_='class-time').text
        location = course.find('span', class_='class-location').text
        classes.append({
            'CRN': crn,
            'Title': title,
            'Professor': professor,
            'Seats': seats,
            'Time': time,
            'Location': location
        })

    return classes

# if __name__ == '__main__':
#     course_data = fetch_course_data()
#     print(course_data)
