import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv
from typing import Any

load_dotenv()

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
EMAIL = os.getenv("EMAIL")
GITHUB = "https://github.com/hariskhu"
CITY_DATA_FILEPATH = os.path.join("..", "data", "cities.csv")

def fetch(url: str, *, max_retries: int=3) -> dict[str, Any]:
    """
    Makes a GET request to given URL
    """

    # TODO: ADD METADATA

    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers={"User-Agent": f"({GITHUB}, {EMAIL})"}, # For NOAA
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request to {url} failed: {e}")
            # return metadata
        
        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)

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

    json = fetch(openweather_url)
    df = pd.json_normalize(json)
    # Unpack weather column dict
    df = df.assign(**df['current.weather'][0][0], loc=loc).drop('current.weather', axis=1)
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

    json = fetch(open_meteo_url)
    normalized_json = pd.json_normalize(json)
    cities_df.columns = map(str.lower, cities_df.columns)
    df = pd.DataFrame(normalized_json).round(1)
    df = pd.merge(cities_df, df, on=['latitude', 'longitude'], how='left')
    return df

def fetch_alerts(forecast_zone: str) -> pd.DataFrame:
    """
    Requests weather alerts from NOAA
    """

    # FIXME: CHANGE TO TAKE FORECAST ZONE. NEED ACTIVE ALERT TO DEBUG.

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
    
    json = fetch(NOAA_URL)
    
    # debug print
    print(json)

    df = pd.json_normalize(
        json["features"],
        record_path=None,
        meta=["id"],
        sep="_"
    )

    # Pull properties up
    # df = pd.json_normalize(
    #     [f["properties"] for f in json["features"]],
    #     sep="_"
    # )

    return df

def fetch_all_alerts(cities_filepath: str=CITY_DATA_FILEPATH) -> pd.DataFrame:
    """
    Requests all currently active alerts from cities
    """

    # FIXME: FIX MERGE

    alert_list = []
    cities_df = pd.read_csv(cities_filepath)

    for zone in cities_df['NOAA Forecast Zone'].unique():
        alert = fetch_alerts(zone)
        alert_list.append(alert)

    alert_df = pd.concat(alert_list)
    cities_df.merge(alert_df, how='left', on='NOAA Forecast Zone')
    
    return alert_df

if __name__ == "__main__":
    print("<----- Weather module activated ----->")
    # cities = pd.read_csv(CITY_DATA_FILEPATH)
    # bro = fetch_alerts(cities.iloc[20]['NOAA Forecast Zone'])
    # df1 = fetch_all_weather()
    # df2 = fetch_all_air_quality()
    df3 = fetch_all_alerts()