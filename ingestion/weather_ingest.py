#!/usr/bin/env python3
"""Weather data ingestion from Open-Meteo API."""

import os
import argparse
import logging
from datetime import datetime
from typing import Dict, Optional
import pandas as pd

from config import (
    CITIES, HISTORICAL_API_URL, FORECAST_API_URL,
    BACKFILL_START_DATE, BACKFILL_END_DATE,
    HOURLY_VARIABLES, DAILY_VARIABLES,
    REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
    RAW_DATA_PATH, get_incremental_date
)
from utils import (
    make_api_request, validate_weather_data,
    generate_batch_id, log_ingestion_stats, logger
)


class WeatherIngestion:
    """Weather data ingestion handler."""
    
    def __init__(self, city_id: str):
        if city_id not in CITIES:
            raise ValueError(f"Unknown city: {city_id}")
            
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
            
    def transform_to_dataframe(self, raw_data: Dict) -> pd.DataFrame:
        """Transform API response to pandas DataFrame."""
        hourly_data = raw_data.get("hourly", {})
        df = pd.DataFrame(hourly_data)
        
        df["city_id"] = self.city_id
        df["city_name"] = self.city_name
        df["latitude"] = raw_data.get("latitude")
        df["longitude"] = raw_data.get("longitude")
        df["timezone"] = raw_data.get("timezone")
        df["ingestion_timestamp"] = datetime.now()
        df["batch_id"] = self.batch_id
        
        df["time"] = pd.to_datetime(df["time"])
        
        logger.info(f"Transformed {len(df):,} hourly records")
        
        return df
        
    def save_to_parquet(self, df: pd.DataFrame, start_date: str, end_date: str):
        """Save DataFrame to Parquet file."""
        os.makedirs(RAW_DATA_PATH, exist_ok=True)
        
        filename = f"{self.city_id}_{start_date}_{end_date}_{self.batch_id}.parquet"
        filepath = os.path.join(RAW_DATA_PATH, filename)
        
        df.to_parquet(
            filepath,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        logger.info(f"Saved to: {filepath}")
        logger.info(f"File size: {file_size_mb:.2f} MB")
        
        return filepath
        
    def run(
        self,
        start_date: str,
        end_date: str,
        use_historical_api: bool = True
    ) -> Optional[str]:
        """Execute full ingestion pipeline."""
        try:
            logger.info("=" * 70)
            logger.info("STARTING WEATHER INGESTION")
            logger.info("=" * 70)
            
            raw_data = self.fetch_weather_data(start_date, end_date, use_historical_api)
            if not raw_data:
                return None
                
            df = self.transform_to_dataframe(raw_data)
            filepath = self.save_to_parquet(df, start_date, end_date)
            
            log_ingestion_stats(self.city_name, start_date, end_date, len(df))
            
            logger.info("INGESTION COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            
            return filepath
            
        except Exception as e:
            logger.error(f"INGESTION FAILED: {e}", exc_info=True)
            return None


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(description="Ingest weather data from Open-Meteo API")
    
    parser.add_argument(
        "--city",
        choices=list(CITIES.keys()),
        default="amsterdam",
        help="City to fetch data for"
    )
    
    parser.add_argument(
        "--mode",
        choices=["backfill", "incremental", "custom"],
        default="custom",
        help="Ingestion mode"
    )
    
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    if args.mode == "backfill":
        start_date = BACKFILL_START_DATE
        end_date = BACKFILL_END_DATE
        use_historical = True
    elif args.mode == "incremental":
        incremental_date = get_incremental_date()
        start_date = incremental_date
        end_date = incremental_date
        use_historical = False
    else:
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date required for custom mode")
        start_date = args.start_date
        end_date = args.end_date
        use_historical = True
        
    ingestion = WeatherIngestion(args.city)
    result = ingestion.run(start_date, end_date, use_historical)
    
    if result:
        print(f"\nSuccess! Data saved to: {result}")
    else:
        print("\nIngestion failed. Check logs above.")
        exit(1)


if __name__ == "__main__":
    main()
