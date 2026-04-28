#!/usr/bin/env python3
"""Weather data ingestion from Open-Meteo API to Parquet files."""

import os
import argparse
import json
import random
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
import pandas as pd

from config import (
    CITIES, HISTORICAL_API_URL, FORECAST_API_URL,
    BACKFILL_START_DATE, BACKFILL_END_DATE,
    HOURLY_VARIABLES,
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
    
    def __init__(
        self,
        city_id: str,
        inject_synthetic_anomalies: bool = False,
        anomaly_rate: float = 0.05,
        anomaly_shift_pct_mean: float = 0.10,
        fixed_anomaly_rows: Optional[int] = None,
        per_column_anomaly_prob: float = 0.35,
    ):
        if city_id not in CITIES:
            raise ValueError(f"Unknown city: {city_id}. Available: {list(CITIES.keys())}")
        if anomaly_rate < 0.0 or anomaly_rate > 1.0:
            raise ValueError("anomaly_rate must be between 0.0 and 1.0")
        if anomaly_shift_pct_mean < 0.0:
            raise ValueError("anomaly_shift_pct_mean must be >= 0.0")
        if fixed_anomaly_rows is not None and fixed_anomaly_rows < 0:
            raise ValueError("fixed_anomaly_rows must be >= 0")
        if per_column_anomaly_prob < 0.0 or per_column_anomaly_prob > 1.0:
            raise ValueError("per_column_anomaly_prob must be between 0.0 and 1.0")
            
        self.city_id = city_id
        self.city_config = CITIES[city_id]
        self.city_name = self.city_config["name"]
        self.batch_id = generate_batch_id()
        self.inject_synthetic_anomalies = inject_synthetic_anomalies
        self.anomaly_rate = anomaly_rate
        self.anomaly_shift_pct_mean = anomaly_shift_pct_mean
        self.fixed_anomaly_rows = fixed_anomaly_rows
        self.per_column_anomaly_prob = per_column_anomaly_prob
        
        logger.info(f"Initialized ingestion for: {self.city_name}")
        if self.inject_synthetic_anomalies:
            logger.info(
                "Synthetic anomalies enabled: rate=%.2f%%, target mean shift=%.2f%%",
                self.anomaly_rate * 100,
                self.anomaly_shift_pct_mean * 100,
            )
            if self.fixed_anomaly_rows is not None:
                logger.info("Synthetic anomalies fixed rows per ingestion: %d", self.fixed_anomaly_rows)

    @staticmethod
    def _is_numeric_series(series: pd.Series) -> bool:
        """Check if a series is numeric and suitable for synthetic shifts."""
        return pd.api.types.is_numeric_dtype(series)

    def _apply_synthetic_anomalies(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
        """Inject synthetic anomalies into a dataframe and track exact shift percentage."""
        result_df = df.copy()

        tracked_cols = [
            "temperature_2m",
            "apparent_temperature",
            "precipitation",
            "surface_pressure",
            "soil_temperature_7_to_28cm",
            "soil_moisture_7_to_28cm",
        ]
        unchanged_details = json.dumps({col: None for col in tracked_cols}, ensure_ascii=True)

        # Metadata columns used by downstream dashboards and dbt.
        result_df["synthetic_anomaly_flag"] = False
        result_df["synthetic_shift_pct"] = 0.0
        result_df["synthetic_anomaly_target_column"] = ""
        result_df["synthetic_original_value"] = pd.NA
        result_df["synthetic_anomaly_batch_id"] = ""
        result_df["synthetic_anomaly_details_json"] = unchanged_details

        if not self.inject_synthetic_anomalies or result_df.empty:
            return result_df

        candidate_cols = [
            col
            for col in tracked_cols
            if col in result_df.columns and self._is_numeric_series(result_df[col])
        ]

        if not candidate_cols:
            logger.warning("No numeric columns found for synthetic anomalies on %s dataset", dataset_name)
            return result_df

        # Ensure columns can accept shifted float values without dtype warnings.
        result_df[candidate_cols] = result_df[candidate_cols].astype(float)

        total_rows = len(result_df)
        if self.fixed_anomaly_rows is not None:
            target_count = min(total_rows, self.fixed_anomaly_rows)
        else:
            target_count = int(round(total_rows * self.anomaly_rate))
            if self.anomaly_rate > 0 and target_count == 0:
                target_count = 1

        anomaly_indices = set(random.sample(range(total_rows), target_count)) if target_count else set()

        # Use a bounded random Shift around anomaly_shift_pct_mean with random direction.
        min_shift = max(0.0, self.anomaly_shift_pct_mean * 0.5)
        max_shift = self.anomaly_shift_pct_mean * 1.5

        for idx in anomaly_indices:
            details_map = {col: None for col in tracked_cols}
            changed_cols: List[str] = []
            primary_shift_pct: Optional[float] = None
            primary_original_value: Optional[float] = None

            for col in candidate_cols:
                if random.random() > self.per_column_anomaly_prob:
                    continue

                original_value = result_df.iat[idx, result_df.columns.get_loc(col)]
                if pd.isna(original_value):
                    continue

                shift_direction = random.choice([-1.0, 1.0])
                shift_magnitude = random.uniform(min_shift, max_shift)
                signed_shift_pct = shift_direction * shift_magnitude
                shifted_value = float(original_value) * (1.0 + signed_shift_pct)

                result_df.iat[idx, result_df.columns.get_loc(col)] = shifted_value
                details_map[col] = {
                    "actual": float(original_value),
                    "shift_pct": signed_shift_pct,
                }
                changed_cols.append(col)

                if primary_shift_pct is None:
                    primary_shift_pct = signed_shift_pct
                    primary_original_value = float(original_value)

            if not changed_cols:
                forced_col = random.choice(candidate_cols)
                original_value = result_df.iat[idx, result_df.columns.get_loc(forced_col)]
                if pd.notna(original_value):
                    shift_direction = random.choice([-1.0, 1.0])
                    shift_magnitude = random.uniform(min_shift, max_shift)
                    signed_shift_pct = shift_direction * shift_magnitude
                    shifted_value = float(original_value) * (1.0 + signed_shift_pct)
                    result_df.iat[idx, result_df.columns.get_loc(forced_col)] = shifted_value
                    details_map[forced_col] = {
                        "actual": float(original_value),
                        "shift_pct": signed_shift_pct,
                    }
                    changed_cols.append(forced_col)
                    primary_shift_pct = signed_shift_pct
                    primary_original_value = float(original_value)

            if not changed_cols:
                continue

            result_df.iat[idx, result_df.columns.get_loc("synthetic_anomaly_flag")] = True
            result_df.iat[idx, result_df.columns.get_loc("synthetic_shift_pct")] = float(primary_shift_pct)
            result_df.iat[idx, result_df.columns.get_loc("synthetic_anomaly_target_column")] = changed_cols[0]
            result_df.iat[idx, result_df.columns.get_loc("synthetic_original_value")] = float(primary_original_value)
            result_df.iat[idx, result_df.columns.get_loc("synthetic_anomaly_batch_id")] = self.batch_id
            result_df.iat[idx, result_df.columns.get_loc("synthetic_anomaly_details_json")] = json.dumps(
                details_map,
                ensure_ascii=True,
                sort_keys=True,
            )

        applied_count = int(result_df["synthetic_anomaly_flag"].sum())
        mean_shift_applied = (
            float(result_df.loc[result_df["synthetic_anomaly_flag"], "synthetic_shift_pct"].mean())
            if applied_count > 0
            else 0.0
        )

        logger.info(
            "Applied synthetic anomalies on %s dataset: %d/%d rows (%.2f%%), mean shift=%.2f%%",
            dataset_name,
            applied_count,
            total_rows,
            (applied_count / total_rows * 100) if total_rows else 0.0,
            mean_shift_applied * 100,
        )

        return result_df
        
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
        
        if data and validate_weather_data(data, ["hourly"]):
            return data
        else:
            logger.error(f"Failed to fetch valid data for {self.city_name}")
            return None
            
    def transform_to_dataframe(self, raw_data: Dict) -> pd.DataFrame:
        """Transform API response to hourly DataFrame."""
        hourly_data = raw_data.get("hourly", {})
        
        df_hourly = pd.DataFrame(hourly_data)
        df_hourly["city_id"] = self.city_id
        df_hourly["city_name"] = self.city_name
        df_hourly["latitude"] = raw_data.get("latitude")
        df_hourly["longitude"] = raw_data.get("longitude")
        df_hourly["timezone"] = raw_data.get("timezone")
        df_hourly["ingestion_timestamp"] = datetime.now(ZoneInfo("Europe/Amsterdam"))
        df_hourly["batch_id"] = self.batch_id
        df_hourly["time"] = pd.to_datetime(df_hourly["time"])
        df_hourly = self._apply_synthetic_anomalies(df_hourly, "hourly")
        
        logger.info(f"Transformed {len(df_hourly):,} hourly records")
        return df_hourly
        
    def save_to_parquet(self, df_hourly: pd.DataFrame, start_date: str, end_date: str):
        """Save hourly DataFrame to Parquet file with Hive-style partitioning."""
        # Create city partition directory: data/raw/city=amsterdam/
        city_partition_path = os.path.join(RAW_DATA_PATH, f"city={self.city_id}")
        os.makedirs(city_partition_path, exist_ok=True)
        
        hourly_filename = f"hourly_{start_date}_{end_date}_{self.batch_id}.parquet"
        hourly_filepath = os.path.join(city_partition_path, hourly_filename)
        
        df_hourly.to_parquet(hourly_filepath, engine='pyarrow', compression='snappy', index=False)
        
        hourly_size_mb = os.path.getsize(hourly_filepath) / (1024 * 1024)
        
        logger.info(f"Saved hourly data: {hourly_filepath} ({hourly_size_mb:.2f} MB)")
        
        # Upload to S3 immediately after saving
        self.upload_to_s3(hourly_filepath)
        
        return hourly_filepath
    
    def upload_to_s3(self, hourly_filepath: str):
        """Upload parquet file to S3 with Hive partitioning."""
        import boto3
        from pathlib import Path
        
        s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
        s3_client = boto3.client('s3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'eu-west-1')
        )
        
        logger.info(f"Uploading to S3 bucket: {s3_bucket}")
        
        for filepath in [hourly_filepath]:
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
                
            df_hourly = self.transform_to_dataframe(raw_data)
            hourly_path = self.save_to_parquet(df_hourly, start_date, end_date)
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
    parser.add_argument(
        "--inject-synthetic-anomalies",
        action="store_true",
        help="Inject synthetic anomalies into raw data (intended for backfill experiments)."
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.05,
        help="Fraction of rows to mutate with synthetic anomalies (default: 0.05)."
    )
    parser.add_argument(
        "--anomaly-shift-pct-mean",
        type=float,
        default=0.10,
        help="Target average positive shift percentage for synthetic anomalies (default: 0.10)."
    )
    parser.add_argument(
        "--fixed-anomaly-rows",
        type=int,
        default=None,
        help="Override anomaly rate and mutate exactly this many rows in the ingested dataframe."
    )
    parser.add_argument(
        "--per-column-anomaly-prob",
        type=float,
        default=0.35,
        help="Probability that each tracked weather column is mutated for an anomaly row."
    )
    
    args = parser.parse_args()
    
    # Initialize ingestion handler
    ingestion = WeatherIngestion(
        args.city,
        inject_synthetic_anomalies=args.inject_synthetic_anomalies,
        anomaly_rate=args.anomaly_rate,
        anomaly_shift_pct_mean=args.anomaly_shift_pct_mean,
        fixed_anomaly_rows=args.fixed_anomaly_rows,
        per_column_anomaly_prob=args.per_column_anomaly_prob,
    )
    
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
