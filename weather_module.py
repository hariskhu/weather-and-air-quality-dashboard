import requests
import os
from dotenv import load_dotenv

load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

def fetch(url: str) -> str:
    """
    Makes a GET request to given URL
    
    :param url: URL to request
    :type url: str
    :return: HTTP response as a string
    :rtype: str
    """
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=10
    )
    response.raise_for_status()
    return response.text

def fetch_weather(lat: float, lon: float) -> str:
    """
    Requests data from OpenWeather API
    
    :param lat: Location latitude
    :type lat: float
    :param lon: Location longitude
    :type lon: float
    :return: OpenWeather API response as a string
    :rtype: str
    """
    
    EXCLUDED = ",".join(['minutely', 'daily', 'alerts'])
    openweather_url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={EXCLUDED}&appid={OPENWEATHER_API_KEY}"
    return fetch(openweather_url)

def write_to_log(text: str) -> None:
    """
    Appends to text file log.txt, creating if it doesn't exist
    
    :param text: Text to append to log.txt
    :type text: str
    """

    with open("log.txt", "a") as f:
        f.write(text)

# Test
# write_to_log(fetch_weather(0, 0))