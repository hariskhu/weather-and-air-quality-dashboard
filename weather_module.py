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
    
    EXCLUDED = ",".join([
        'minutely',
        'daily',
        'alerts',
    ])

    openweather_url = (
        "https://api.openweathermap.org/data/3.0/onecall?"
        f"lat={lat}&lon={lon}"
        f"&exclude={EXCLUDED}"
        f"&appid={OPENWEATHER_API_KEY}"
    )

    return fetch(openweather_url)

def fetch_air_quality(lat: float, lon: float) -> str:
    """
    Requests air quality data from Open-Meteo
    
    :param lat: Location latitude
    :type lat: float
    :param lon: Location longitude
    :type lon: float
    :return: Open-Meteo API response as a string
    :rtype: str
    """

    CURRENT_PARAMS = ",".join([
        "us_aqi",
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
    ])

    open_meteo_url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality?"
        f"latitude={lat}&longitude={lon}"
        f"&current={CURRENT_PARAMS}&timeformat=unixtime"
    )

    return fetch(open_meteo_url)

def fetch_alerts(lat: float, lon: float) -> str:
    """
    Requests weather alters from NOAA
    
    :param lat: Location latitude
    :type lat: float
    :param lon: Location longitude
    :type lon: float
    :return: NOAA API results as a string
    :rtype: str
    """

    URGENCY = ",".join([
        "Immediate",
        "Expected",
    ])

    SEVERITY = ",".join([
        "Extreme",
        "Severe",
        "Moderate",
        "Minor",
        "Unknown",
    ])

    CERTAINTY = ",".join([
        "Observed",
        "Likely",
        "Possible",
        "Unlikely",
        "Unknown",
    ])

    NOAA_URL = (
        "https://api.weather.gov/alerts/active?"
        f"point={lat}%2C{lon}"
        f"&urgency={URGENCY}&severity={SEVERITY}&certainty={CERTAINTY}"
    )
    
    return fetch(NOAA_URL)

def write_to_log(text: str) -> None:
    """
    Appends to text file log.txt, creates if it doesn't exist
    
    :param text: Text to append to log.txt
    :type text: str
    """

    with open("log.txt", "a", encoding="utf-8") as f:
        f.write(f"{text}\n")

if __name__ == "__main__":
    # Testing
    virginia_beach = (36.78, -76.02)
    write_to_log(fetch_air_quality(*virginia_beach))
    write_to_log(fetch_weather(*virginia_beach))
    write_to_log(fetch_alerts(*virginia_beach))