import requests
import pandas as pd
import os
import time
import logging
from dataclasses import dataclass, fields
from typing import Any
from dotenv import load_dotenv


load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
EMAIL = os.getenv("EMAIL")
GITHUB = "https://github.com/hariskhu"
CITY_DATA_FILEPATH = os.path.join("..", "data", "cities.csv")


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@dataclass
class FetchResponse:
    '''
    DataClass for FetchResponse to retrieve JSON response and metadata
    '''
    json: dict[str, Any]
    attempts: int
    request_time: float


def fetch(url: str, *, max_retries: int=3, api_name: str=None) -> FetchResponse:
    """
    Makes a GET request to given URL, return FetchResponse
    """

    start_time = time.perf_counter()

    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers={"User-Agent": f"({GITHUB}, {EMAIL})"}, # For NOAA
                timeout=10
            )
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            print(f"Request to {url} failed: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)
        
    end_time = time.perf_counter()
    json = response.json()
    attempts = attempt + 1
    request_time = end_time - start_time
    return FetchResponse(json=json, attempts=attempts, request_time=request_time)

def fetch_weather(loc: str, lat: float, lon: float) -> pd.DataFrame:
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

    fetch_response = fetch(openweather_url)
    json = fetch_response.json
    df = pd.json_normalize(json)
    # Unpack weather column dict
    df = df.assign(**df['current.weather'][0][0], loc=loc).drop('current.weather', axis=1)
    for field in fields(fetch_response):
        if field.name != 'json':
            value = getattr(fetch_response, field.name)
            df[field.name] = value
    return df

def fetch_all_weather(cities_filepath: str=CITY_DATA_FILEPATH) -> pd.DataFrame:
    """
    Requests all current weather data from cities
    """

    weather_list = []
    cities_df = pd.read_csv(cities_filepath)
    for row in cities_df.itertuples(index=False):
        loc, lat, lon, _ = row
        weather_list.append(fetch_weather(loc, lat, lon))
    
    return pd.concat(weather_list)

def fetch_all_air_quality(cities_filepath: str=CITY_DATA_FILEPATH) -> pd.DataFrame:
    """
    Requests air quality for all cities from Open-Meteo
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
        f"&current={CURRENT_PARAMS}&timezone=America/New_York&timeformat=unixtime"
    )

    cities_df = pd.read_csv(cities_filepath)
    lat_list, lon_list = [], []
    for row in cities_df.itertuples(index=False):
        lat, lon = row[1], row[2]
        lat_list.append(lat)
        lon_list.append(lon)

    open_meteo_url += (
        f"&latitude={','.join(map(str, lat_list))}"
        f"&longitude={','.join(map(str, lon_list))}"
    )

    fetch_response = fetch(open_meteo_url)
    json = fetch_response.json
    normalized_json = pd.json_normalize(json)
    cities_df.columns = map(str.lower, cities_df.columns)
    df = pd.DataFrame(normalized_json).round(1)
    df = pd.merge(cities_df, df, on=['latitude', 'longitude'], how='left')

    for field in fields(fetch_response):
        if field.name != 'json':
            value = getattr(fetch_response, field.name)
            df[field.name] = value
    return df

def fetch_alerts(forecast_zone: str) -> pd.DataFrame:
    """
    Requests weather alerts from NOAA
    """

    URGENCY = ",".join([
        "Immediate",
        "Expected",
        "Future",
        "Unknown",
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
        f"zone={forecast_zone}"
        f"&urgency={URGENCY}&severity={SEVERITY}&certainty={CERTAINTY}"
    )
    
    fetch_response = fetch(NOAA_URL)
    json = fetch_response.json

    df = pd.json_normalize(
        json['features'],
        record_path=None,
        meta=["id"],
        sep="_"
    )

    for field in fields(fetch_response):
        if field.name != 'json':
            value = getattr(fetch_response, field.name)
            df[field.name] = value
    return df

def fetch_all_alerts(cities_filepath: str=CITY_DATA_FILEPATH) -> pd.DataFrame:
    """
    Requests all currently active alerts from cities
    """

    alert_list = []
    cities_df = pd.read_csv(cities_filepath)

    for zone in cities_df['NOAA Forecast Zone'].unique():
        alert = fetch_alerts(zone)
        alert['NOAA Forecast Zone'] = zone
        alert_list.append(alert)

    alert_df = pd.concat(alert_list)
    alert_df.merge(cities_df, how='left', on='NOAA Forecast Zone')
    
    return alert_df

def main():
    print("<----- Weather module activated ----->")
    df1 = fetch_all_weather()
    df2 = fetch_all_air_quality()
    df3 = fetch_all_alerts()
    print(df1, df2, df3, sep='\n')
    return df1, df2, df3

if __name__ == "__main__":
    df1, df2, df3 = main()