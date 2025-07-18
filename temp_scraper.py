import requests

url = "https://www.notifications.service.gov.uk/features/performance"
response = requests.get(url)
print(response.text)