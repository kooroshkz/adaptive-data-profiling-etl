"""Configuration for weather data ingestion."""

from datetime import datetime, timedelta

CITIES = {
    "amsterdam": {
        "name": "Amsterdam",
        "latitude": 52.3676,
        "longitude": 4.9041,
        "timezone": "Europe/Amsterdam"
    },
    "new_york": {
        "name": "New York",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "timezone": "America/New_York"
    },
    "london": {
        "name": "London",
        "latitude": 51.5074,
        "longitude": -0.1278,
        "timezone": "Europe/London"
    },
    "paris": {
        "name": "Paris",
        "latitude": 48.8566,
        "longitude": 2.3522,
        "timezone": "Europe/Paris"
    },
    "tokyo": {
        "name": "Tokyo",
        "latitude": 35.6762,
        "longitude": 139.6503,
        "timezone": "Asia/Tokyo"
    }
}

HISTORICAL_API_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"

BACKFILL_START_DATE = "2024-01-01"
BACKFILL_END_DATE = "2025-12-31"

def get_incremental_date():
    """Returns yesterday's date for incremental ingestion."""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "weather_code",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "et0_fao_evapotranspiration",
    "vapour_pressure_deficit",
    "wind_speed_10m",
    "wind_speed_100m",
    "wind_direction_10m",
    "wind_direction_100m",
    "wind_gusts_10m",
    "soil_temperature_0_to_7cm",
    "soil_temperature_7_to_28cm",
    "soil_temperature_28_to_100cm",
    "soil_temperature_100_to_255cm",
    "soil_moisture_0_to_7cm",
    "soil_moisture_7_to_28cm",
    "soil_moisture_28_to_100cm",
    "soil_moisture_100_to_255cm"
]

DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "apparent_temperature_max",
    "apparent_temperature_min",
    "apparent_temperature_mean",
    "sunrise",
    "sunset",
    "daylight_duration",
    "sunshine_duration",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_hours",
    "weather_code",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration"
]

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

import os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw")
