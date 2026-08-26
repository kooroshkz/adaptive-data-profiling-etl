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
    "apparent_temperature",
    "precipitation",
    "surface_pressure",
    "soil_temperature_7_to_28cm",
    "soil_moisture_7_to_28cm",
]

DAILY_VARIABLES = []

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

import os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # airflow/
REPO_ROOT = os.path.dirname(PROJECT_ROOT)                                    # repo root

# Fresh ingested raw data (scratch, safe to delete): airflow/data/raw locally,
# /opt/airflow/data/raw in the container.
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw")

# Durable, git-tracked artefacts (models, anomaly results, warehouse).
# Defaults to the repo's data/ folder; the container sets PROFILER_DATA_ROOT
# to the mounted repo data/ so the committed models are used with no AWS.
PROJECT_DATA_ROOT = os.environ.get("PROFILER_DATA_ROOT") or os.path.join(REPO_ROOT, "data")
MODELS_DIR = os.path.join(PROJECT_DATA_ROOT, "models", "v1")
ANOMALY_RESULTS_DIR = os.path.join(PROJECT_DATA_ROOT, "anomaly_results")
WAREHOUSE_PATH = os.path.join(PROJECT_DATA_ROOT, "warehouse.duckdb")
MART_DIR = os.path.join(PROJECT_DATA_ROOT, "mart")


def s3_enabled() -> bool:
    """True only when S3 is explicitly configured (bucket + credentials)."""
    return bool(
        os.getenv("S3_BUCKET")
        and os.getenv("AWS_ACCESS_KEY_ID")
        and os.getenv("AWS_SECRET_ACCESS_KEY")
    )
