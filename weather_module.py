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

def fetch_all_weather(cities_filepath: str='cities.csv'):
    """
    Requests all current weather data from cities
    """

    weather_list = []
    cities_df = pd.read_csv(cities_filepath)
    for row in cities_df.itertuples(index=False):
        loc, lat, lon = row
        weather_list.append(fetch_weather(loc, lat, lon))
    
    return pd.concat(weather_list)

def fetch_all_air_quality(cities_filepath: str='cities.csv') -> pd.DataFrame:
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

def fetch_alerts(loc: str, lat: float, lon: float) -> pd.DataFrame:
    """
    Requests weather alerts from NOAA
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
    
    json = fetch(NOAA_URL)
    df = pd.json_normalize(
        json["features"],
        record_path=None,
        meta=["id"],
        sep="_"
    )

    # pull properties up
    props = pd.json_normalize(
        [f["properties"] for f in json["features"]],
        sep="_"
    )

    df = df.assign(Location=loc, Latitude=lat, Longitude=lon)
    return df

def fetch_all_alerts(cities_filepath: str='cities.csv'):
    """
    Requests all currently active alerts from cities
    """

    alert_list = []
    cities_df = pd.read_csv(cities_filepath)
    for row in cities_df.itertuples(index=False):
        loc, lat, lon = row
        alert_list.append(fetch_alerts(loc, lat, lon))
    
    return pd.concat(alert_list)

if __name__ == "__main__":
    roanoke = ('Roanoke', 37.3, -80.0)
    # df = fetch_all_alerts()
    # df = fetch_alerts(*roanoke)

    df1 = fetch_all_weather()
    df2 = fetch_all_air_quality()
    df3 = fetch_all_alerts()