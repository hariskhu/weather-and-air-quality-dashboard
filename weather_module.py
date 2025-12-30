import requests
import pandas as pd
import os
from dotenv import load_dotenv
from typing import Any

load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

def fetch(url: str) -> dict[str, Any]:
    """
    Makes a GET request to given URL
    """
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=10
    )
    response.raise_for_status()
    return response.json()

def fetch_weather(lat: float, lon: float) -> pd.DataFrame:
    """
    Requests current weather data from OpenWeather API
    """
    
    EXCLUDED = ",".join([
        'minutely',
        'hourly',
        'daily',
        'alerts',
    ])

    openweather_url = (
        "https://api.openweathermap.org/data/3.0/onecall?"
        f"lat={lat}&lon={lon}&exclude={EXCLUDED}"
        f"&appid={OPENWEATHER_API_KEY}"
    )

    json = fetch(openweather_url)
    normalized_json = pd.json_normalize(json)
    df = pd.DataFrame(normalized_json)
    # Unpack weather column dict
    df = df.assign(**df['current.weather'].iloc[0][0]).drop('current.weather', axis=1)
    return df

def fetch_air_quality(lat: float, lon: float) -> dict[str, Any]:
    """
    Requests air quality data from Open-Meteo
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

def fetch_alerts(lat: float, lon: float) -> dict[str, Any]:
    """
    Requests weather alters from NOAA
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

if __name__ == "__main__":
    virginia_beach = (36.78,-76.02)
    df = fetch_weather(*virginia_beach)