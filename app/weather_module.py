import requests
import pandas as pd
import os
import time
import logging
from datetime import datetime
from dataclasses import dataclass, fields
from typing import Any
from dotenv import load_dotenv

load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
EMAIL = os.getenv("EMAIL")
GITHUB = "https://github.com/hariskhu"
CITY_DATA_FILEPATH = os.path.join("..", "data", "cities.csv")

logging.basicConfig(
    level=logging.INFO,
    format='[%(name)s %(levelname)s] %(asctime)s :: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)
logger.info('WEATHER MODULE ACTIVATED')

@dataclass
class FetchResponse:
    """
    DataClass for FetchResponse to retrieve JSON response and metadata
    """
    json: dict[str, Any]
    attempts: int
    request_timestamp: datetime
    request_duration: float

def fetch(url: str, *, max_retries: int=4, api_name: str="UNDEFINED", **kwargs) -> FetchResponse:
    """
    Makes a GET request to given URL, return FetchResponse
    that contains the JSON response and additional metadata
    """
    request_timestamp = datetime.now()
    start = time.perf_counter()
    params_to_log = ", ".join(f"{k}={v!r}" for k, v in kwargs.items()) if kwargs else "NONE"

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                headers={"User-Agent": f"({GITHUB}, {EMAIL})"}, # For NOAA
                timeout=10
            )
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error on attempt {attempt}/{max_retries} using fetch().")
        
        if attempt < max_retries:
            time.sleep(2 ** attempt)
        else:
            logger.error(f"Maximum retries reached when querying {api_name} with parameters {params_to_log}.")
        
    end = time.perf_counter()
    json = response.json()
    request_duration = end - start

    logger.info(f"Successful fetch() from {api_name} in {attempt} tries with parameters {params_to_log}.")
    return FetchResponse(json, attempt, request_timestamp, request_duration)

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

    fetch_response = fetch(
        openweather_url,
        api_name="OpenWeather",
        loc=loc,
        lat=lat,
        lon=lon,
    )
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
    logger.info("Begin fetch_all_weather().")

    weather_list = []
    cities_df = pd.read_csv(cities_filepath)
    for row in cities_df.itertuples(index=False):
        loc, lat, lon, _ = row
        weather_list.append(fetch_weather(loc, lat, lon))
    
    df = pd.concat(weather_list, ignore_index=True)
    logger.info("fetch_all_weather() complete.")
    if df.empty:
        logger.warning("fetch_all_weather() resulted in empty data.")
    return df

def fetch_all_air_quality(cities_filepath: str=CITY_DATA_FILEPATH) -> pd.DataFrame:
    """
    Requests air quality for all cities from Open-Meteo
    """
    logger.info("Begin fetch_all_air_quality().")

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

    fetch_response = fetch(open_meteo_url, api_name="Open-Meteo")
    json = fetch_response.json
    df = pd.json_normalize(json).round(1)
    cities_df.columns = map(str.lower, cities_df.columns)
    df = pd.merge(cities_df, df, on=['latitude', 'longitude'], how='left')

    for field in fields(fetch_response):
        if field.name != 'json':
            value = getattr(fetch_response, field.name)
            df[field.name] = value

    logger.info("fetch_all_air_quality() complete.")
    if df.empty:
        logger.warning("fetch_all_air_quality() resulted in empty data.")
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
    
    fetch_response = fetch(
        NOAA_URL, 
        api_name="NOAA",
        forecast_zone=forecast_zone,
    )
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
    logger.info("Begin fetch_all_alerts().")

    alert_list = []
    cities_df = pd.read_csv(cities_filepath)
    for zone in cities_df['NOAA Forecast Zone'].unique():
        alert = fetch_alerts(zone)
        alert['NOAA Forecast Zone'] = zone
        alert_list.append(alert)

    df = pd.concat(alert_list)
    df.merge(cities_df, how='left', on='NOAA Forecast Zone')

    logger.info("fetch_all_alerts() complete.")
    if df.empty:
        logger.warning("fetch_all_alerts() resulted in empty data.")
    return df

def parquet_to_s3(df: pd.DataFrame):
    """
    Saves a dataframe to S3 as a parquet
    """
    # TODO: IMPLEMENT THIS
    pass

def main():
    df1 = fetch_all_weather()
    df2 = fetch_all_air_quality()
    df3 = fetch_all_alerts()
    print(df1, df2, df3, sep='\n')
    return df1, df2, df3

if __name__ == "__main__":
    df1, df2, df3 = main()