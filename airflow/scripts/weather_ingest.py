#!/usr/bin/env python3
"""Weather data ingestion from Open-Meteo API to Parquet files."""

import os
import argparse
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import (
    CITIES, HISTORICAL_API_URL, FORECAST_API_URL,
    BACKFILL_START_DATE, BACKFILL_END_DATE,
    HOURLY_VARIABLES, DAILY_VARIABLES,
    REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
    RAW_DATA_PATH, get_incremental_date
)
from utils import (
    make_api_request, validate_weather_data,
    generate_batch_id, log_ingestion_stats, logger,
    get_latest_s3_timestamp
)


class WeatherIngestion:
    """Weather data ingestion handler."""
    
    def __init__(self, city_id: str):
        if city_id not in CITIES:
            raise ValueError(f"Unknown city: {city_id}. Available: {list(CITIES.keys())}")
            
        self.city_id = city_id
        self.city_config = CITIES[city_id]
        self.city_name = self.city_config["name"]
        self.batch_id = generate_batch_id()
        
        logger.info(f"Initialized ingestion for: {self.city_name}")
        
    def fetch_weather_data(
        self,
        start_date: str,
        end_date: str,
        use_historical_api: bool = True
    ) -> Optional[Dict]:
        """Fetch weather data from Open-Meteo API."""
        api_url = HISTORICAL_API_URL if use_historical_api else FORECAST_API_URL
        
        params = {
            "latitude": self.city_config["latitude"],
            "longitude": self.city_config["longitude"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(HOURLY_VARIABLES),
            "daily": ",".join(DAILY_VARIABLES),
            "timezone": self.city_config["timezone"]
        }
        
        logger.info(f"Fetching data for {self.city_name} ({start_date} to {end_date})")
        
        data = make_api_request(
            url=api_url,
            params=params,
            timeout=REQUEST_TIMEOUT,
            max_retries=MAX_RETRIES,
            retry_delay=RETRY_DELAY
        )
        
        if data and validate_weather_data(data, ["hourly", "daily"]):
            return data
        else:
            logger.error(f"Failed to fetch valid data for {self.city_name}")
            return None
            
    def transform_to_dataframe(self, raw_data: Dict) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Transform API response to DataFrames (hourly and daily)."""
        hourly_data = raw_data.get("hourly", {})
        daily_data = raw_data.get("daily", {})
        
        df_hourly = pd.DataFrame(hourly_data)
        df_hourly["city_id"] = self.city_id
        df_hourly["city_name"] = self.city_name
        df_hourly["latitude"] = raw_data.get("latitude")
        df_hourly["longitude"] = raw_data.get("longitude")
        df_hourly["timezone"] = raw_data.get("timezone")
        df_hourly["ingestion_timestamp"] = datetime.now(ZoneInfo("Europe/Amsterdam"))
        df_hourly["batch_id"] = self.batch_id
        df_hourly["time"] = pd.to_datetime(df_hourly["time"])
        
        df_daily = pd.DataFrame(daily_data)
        df_daily["city_id"] = self.city_id
        df_daily["city_name"] = self.city_name
        df_daily["latitude"] = raw_data.get("latitude")
        df_daily["longitude"] = raw_data.get("longitude")
        df_daily["timezone"] = raw_data.get("timezone")
        df_daily["ingestion_timestamp"] = datetime.now(ZoneInfo("Europe/Amsterdam"))
        df_daily["batch_id"] = self.batch_id
        df_daily["time"] = pd.to_datetime(df_daily["time"])
        
        logger.info(f"Transformed {len(df_hourly):,} hourly records and {len(df_daily):,} daily records")
        return df_hourly, df_daily
        
    def save_to_parquet(self, df_hourly: pd.DataFrame, df_daily: pd.DataFrame, start_date: str, end_date: str):
        """Save DataFrames to Parquet files with Hive-style partitioning."""
        # Create city partition directory: data/raw/city=amsterdam/
        city_partition_path = os.path.join(RAW_DATA_PATH, f"city={self.city_id}")
        os.makedirs(city_partition_path, exist_ok=True)
        
        hourly_filename = f"hourly_{start_date}_{end_date}_{self.batch_id}.parquet"
        hourly_filepath = os.path.join(city_partition_path, hourly_filename)
        
        daily_filename = f"daily_{start_date}_{end_date}_{self.batch_id}.parquet"
        daily_filepath = os.path.join(city_partition_path, daily_filename)
        
        df_hourly.to_parquet(hourly_filepath, engine='pyarrow', compression='snappy', index=False)
        df_daily.to_parquet(daily_filepath, engine='pyarrow', compression='snappy', index=False)
        
        hourly_size_mb = os.path.getsize(hourly_filepath) / (1024 * 1024)
        daily_size_mb = os.path.getsize(daily_filepath) / (1024 * 1024)
        
        logger.info(f"Saved hourly data: {hourly_filepath} ({hourly_size_mb:.2f} MB)")
        logger.info(f"Saved daily data: {daily_filepath} ({daily_size_mb:.2f} MB)")
        
        # Upload to S3 immediately after saving
        self.upload_to_s3(hourly_filepath, daily_filepath)
        
        return hourly_filepath, daily_filepath
    
    def upload_to_s3(self, hourly_filepath: str, daily_filepath: str):
        """Upload parquet files to S3 with Hive partitioning."""
        import boto3
        from pathlib import Path
        
        s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
        s3_client = boto3.client('s3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'eu-west-1')
        )
        
        logger.info(f"Uploading to S3 bucket: {s3_bucket}")
        
        for filepath in [hourly_filepath, daily_filepath]:
            # Extract city partition and filename from path
            # e.g., /opt/airflow/data/raw/city=amsterdam/hourly_2024-01-01_2024-01-01_20260310.parquet
            path_obj = Path(filepath)
            city_partition = path_obj.parent.name  # city=amsterdam
            filename = path_obj.name  # hourly_2024-01-01_2024-01-01_20260310.parquet
            
            s3_key = f'raw/{city_partition}/{filename}'
            
            try:
                logger.info(f"  → Uploading {city_partition}/{filename} to s3://{s3_bucket}/{s3_key}")
                s3_client.upload_file(filepath, s3_bucket, s3_key)
                logger.info(f"  ✓ Uploaded successfully")
            except Exception as e:
                logger.error(f"  ✗ Failed to upload {filename}: {e}")
                raise
        
    def check_data_exists(self, start_date: str, end_date: str) -> bool:
        """Check if data already exists in local parquet files."""
        # For incremental runs (single day), check if file exists
        if start_date == end_date:
            city_partition_path = os.path.join(RAW_DATA_PATH, f"city={self.city_id}")
            if os.path.exists(city_partition_path):
                # Check for any file matching the date pattern
                pattern = f"hourly_{start_date}_{end_date}_"
                files = [f for f in os.listdir(city_partition_path) if f.startswith(pattern)]
                if files:
                    logger.info(f"   ✓ Data already exists for {self.city_name} on {start_date} ({len(files)} files)")
                    return True
        return False
    
    def calculate_smart_date_range(self) -> Optional[tuple[str, str]]:
        """Calculate date range based on latest S3 data.
        
        Returns:
            (start_date, end_date) tuple if data needs ingestion, None if up-to-date.
        """
        from datetime import datetime, timedelta
        
        logger.info(f"Checking S3 for latest data timestamp...")
        
        # Query S3 for the latest timestamp
        latest_s3_date = get_latest_s3_timestamp(self.city_id)
        
        # Calculate today's date (or yesterday if prefer to avoid partial data)
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        target_date = yesterday  # Use yesterday to ensure complete data
        
        if not latest_s3_date:
            # No data in S3 - this is first-time ingestion
            logger.info(f"     No existing data found in S3 for {self.city_name}")
            logger.info(f"   → Will fetch data from yesterday: {target_date}")
            return (str(target_date), str(target_date))
        
        # Parse the latest S3 date
        latest_date = datetime.strptime(latest_s3_date, '%Y-%m-%d').date()
        
        # Check if we're already up-to-date
        if latest_date >= target_date:
            logger.info(f"     Data is up-to-date! Latest: {latest_date}, Target: {target_date}")
            logger.info(f"   → No ingestion needed for {self.city_name}")
            return None
        
        # Calculate the gap
        gap_days = (target_date - latest_date).days
        next_date = latest_date + timedelta(days=1)
        
        logger.info(f"      Data gap detected:")
        logger.info(f"      Latest in S3: {latest_date}")
        logger.info(f"      Target date:  {target_date}")
        logger.info(f"      Gap: {gap_days} day(s)")
        logger.info(f"   → Will fetch: {next_date} to {target_date}")
        
        return (str(next_date), str(target_date))
    
    def run(
        self,
        start_date: str,
        end_date: str,
        use_historical_api: bool = True,
        skip_if_exists: bool = True
    ) -> Optional[str]:
        """Execute full ingestion pipeline."""
        try:
            logger.info("=" * 70)
            logger.info("STARTING WEATHER INGESTION")
            logger.info("=" * 70)
            
            # Check if data already exists and skip if requested
            if skip_if_exists and self.check_data_exists(start_date, end_date):
                logger.info(f"SKIPPING: Data already exists for {self.city_name} ({start_date} to {end_date})")
                logger.info("=" * 70)
                return "SKIPPED"
            
            raw_data = self.fetch_weather_data(start_date, end_date, use_historical_api)
            if not raw_data:
                return None
                
            df_hourly, df_daily = self.transform_to_dataframe(raw_data)
            hourly_path, daily_path = self.save_to_parquet(df_hourly, df_daily, start_date, end_date)
            log_ingestion_stats(self.city_name, start_date, end_date, len(df_hourly))
            
            logger.info("INGESTION COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            
            return hourly_path
            
        except Exception as e:
            logger.error(f"INGESTION FAILED: {e}", exc_info=True)
            return None


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Ingest weather data from Open-Meteo API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python weather_ingest.py --city amsterdam --mode backfill
  python weather_ingest.py --city new_york --mode incremental
  python weather_ingest.py --city london --start-date 2024-01-01 --end-date 2024-01-31
        """
    )
    
    parser.add_argument(
        "--city",
        choices=list(CITIES.keys()),
        default="amsterdam",
        help="City to fetch data for"
    )
    
    parser.add_argument(
        "--mode",
        choices=["backfill", "incremental", "smart", "custom"],
        default="smart",
        help="Ingestion mode: smart (S3-aware, recommended), incremental (yesterday only), backfill, custom"
    )
    
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # Initialize ingestion handler
    ingestion = WeatherIngestion(args.city)
    
    # Determine date range based on mode
    if args.mode == "backfill":
        start_date = BACKFILL_START_DATE
        end_date = BACKFILL_END_DATE
        use_historical = True
        skip_check = False
        
    elif args.mode == "smart":
        # Smart mode: query S3 and calculate the gap
        date_range = ingestion.calculate_smart_date_range()
        
        if date_range is None:
            # Already up-to-date, nothing to ingest
            print(f"\nData is up-to-date for {args.city}. No ingestion needed.")
            exit(0)
        
        start_date, end_date = date_range
        use_historical = True  # Use historical API for any date range
        skip_check = False  # Don't skip, we already calculated the gap
        
    elif args.mode == "incremental":
        # Traditional incremental: always fetch yesterday
        incremental_date = get_incremental_date()
        start_date = incremental_date
        end_date = incremental_date
        use_historical = False
        skip_check = True  # Use traditional skip check
        
    else:  # custom mode
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date required for custom mode")
        start_date = args.start_date
        end_date = args.end_date
        use_historical = True
        skip_check = True
    
    # Run ingestion
    result = ingestion.run(start_date, end_date, use_historical, skip_if_exists=skip_check)
    
    if result == "SKIPPED":
        print(f"\nData already exists for {args.city} on {start_date} to {end_date}. Skipped.")
        exit(0)  # Exit successfully
    elif result:
        print(f"\nSuccess! Data saved to: {result}")
        exit(0)
    else:
        print("\nIngestion failed. Check logs above.")
        exit(1)


if __name__ == "__main__":
    main()
