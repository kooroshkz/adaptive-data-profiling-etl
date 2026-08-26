"""Utility functions for weather data ingestion."""

import time
import logging
from typing import Dict, Optional, Any
import requests
from datetime import datetime
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def make_api_request(
    url: str,
    params: Dict[str, Any],
    timeout: int = 30,
    max_retries: int = 3,
    retry_delay: int = 2
) -> Optional[Dict]:
    """Make HTTP GET request with retry logic."""
    for attempt in range(max_retries):
        try:
            logger.info(f"API request attempt {attempt + 1}/{max_retries}")
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            logger.info("API request successful")
            return data
            
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout (attempt {attempt + 1})")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            # Rate limit (429) or client errors (4xx) should fail immediately - no retry
            if e.response is not None and 400 <= e.response.status_code < 500:
                logger.error(f"Client error {e.response.status_code} - failing immediately (no retry)")
                raise
            # For server errors (5xx), retry
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                
        except ValueError as e:
            logger.error(f"Invalid JSON response: {e}")
            raise
            
    logger.error("All retry attempts failed")
    return None


def validate_weather_data(data: Dict, expected_keys: list) -> bool:
    """Validate API response contains expected data."""
    if not data:
        logger.error("Data is empty or None")
        return False
        
    missing_keys = [key for key in expected_keys if key not in data]
    if missing_keys:
        logger.error(f"Missing required keys: {missing_keys}")
        return False
        
    for key in expected_keys:
        if isinstance(data[key], dict):
            if 'time' in data[key] and len(data[key]['time']) == 0:
                logger.error(f"Empty time array in {key}")
                return False
                
    logger.info("Data validation passed")
    return True


def generate_batch_id() -> str:
    """Generate unique batch identifier."""
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info(f"Generated batch ID: {batch_id}")
    return batch_id


def log_ingestion_stats(city: str, start_date: str, end_date: str, record_count: int):
    """Log ingestion statistics."""
    logger.info("=" * 60)
    logger.info("INGESTION STATISTICS")
    logger.info("=" * 60)
    logger.info(f"City: {city}")
    logger.info(f"Date Range: {start_date} to {end_date}")
    logger.info(f"Records Ingested: {record_count:,}")
    logger.info("=" * 60)


def get_latest_local_timestamp(city_id: str) -> Optional[str]:
    """Return the latest data date (YYYY-MM-DD) from local parquet files.

    Reads the `time` column across all hourly parquet files already ingested for
    the city. This is the local, no-cloud counterpart to
    ``get_latest_s3_timestamp`` and is used by the smart ingestion mode.
    """
    import glob
    import pyarrow.parquet as pq
    from config import RAW_DATA_PATH

    city_dir = os.path.join(RAW_DATA_PATH, f"city={city_id}")
    files = sorted(glob.glob(os.path.join(city_dir, "hourly_*.parquet")))
    if not files:
        logger.info(f"   No existing local data for city={city_id}")
        return None

    latest: Optional[pd.Timestamp] = None
    for path in files:
        try:
            df = pq.read_table(path, columns=["time"]).to_pandas()
        except Exception as e:  # pragma: no cover - unreadable/partial file
            logger.warning(f"   Could not read {path}: {e}")
            continue
        if "time" in df.columns and len(df) > 0:
            file_max = pd.to_datetime(df["time"]).max()
            if latest is None or file_max > latest:
                latest = file_max

    if latest is None:
        return None
    latest_date = latest.strftime("%Y-%m-%d")
    logger.info(f"   Latest local data timestamp for {city_id}: {latest_date}")
    return latest_date


def get_latest_s3_timestamp(city_id: str, s3_bucket: str = None) -> Optional[str]:
    """Query S3 for the latest ingestion timestamp for a city.
    
    Returns:
        Latest date as string (YYYY-MM-DD), or None if no data exists.
    """
    import boto3
    import os
    import pyarrow.parquet as pq
    from datetime import datetime
    
    if not s3_bucket:
        s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
    
    try:
        s3 = boto3.client('s3')
        prefix = f'raw/city={city_id}/'
        
        logger.info(f"Querying S3 for latest timestamp: s3://{s3_bucket}/{prefix}")
        
        # List all parquet files for this city
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        
        if 'Contents' not in response or not response['Contents']:
            logger.info(f"   No existing data in S3 for city={city_id}")
            return None
        
        # Get the most recently modified file
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        latest_file = files[0]['Key']
        
        logger.info(f"   Latest S3 file: {latest_file}")
        
        # Read the parquet file from S3 to get max timestamp
        s3_path = f's3://{s3_bucket}/{latest_file}'
        table = pq.read_table(s3_path)
        df = table.to_pandas()
        
        if 'time' in df.columns and len(df) > 0:
            # Get the maximum timestamp from the 'time' column
            max_timestamp = pd.to_datetime(df['time']).max()
            latest_date = max_timestamp.strftime('%Y-%m-%d')
            logger.info(f"   ✓ Latest data timestamp in S3: {latest_date}")
            return latest_date
        else:
            logger.warning(f"      Parquet file has no 'time' column or is empty")
            return None
            
    except Exception as e:
        logger.error(f"      Failed to query S3: {e}")
        return None
