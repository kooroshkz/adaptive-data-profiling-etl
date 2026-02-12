"""Utility functions for weather data ingestion."""

import time
import logging
from typing import Dict, Optional, Any
import requests
from datetime import datetime

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
