#!/usr/bin/env python3
"""Sync data from S3 and refresh local database."""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"

def run_command(cmd, description):
    """Run shell command and handle errors."""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, shell=True, capture_output=False)
    if result.returncode != 0:
        print(f"Error: {description} failed")
        sys.exit(1)
    print(f"Success: {description} completed")

def main():
    print("SYNCING DATA FROM S3 AND REFRESHING DATABASE")
    
    # Step 1: Download from S3
    run_command(
        f"{PROJECT_ROOT}/.venv/bin/aws s3 sync s3://weather-data-koorosh-thesis/raw/ {DATA_DIR}/",
        "Downloading data from S3"
    )
    
    # Step 2: Rebuild DuckDB database
    run_command(
        f"{PROJECT_ROOT}/.venv/bin/python {PROJECT_ROOT}/duckdb/setup_database.py",
        "Rebuilding DuckDB database"
    )
    
    # Step 3: Run dbt models
    run_command(
        f"cd {PROJECT_ROOT}/weather_dbt && ../.venv/bin/dbt run",
        "Running dbt transformations"
    )
    
    print("\n" + "="*60)
    print("DATABASE READY FOR ANALYSIS")
    print("="*60)
    print(f"\nDatabase: {PROJECT_ROOT}/duckdb/weather.db")
    print("\nQuery the data:")
    print(f"  duckdb {PROJECT_ROOT}/duckdb/weather.db")
    print("\nOr use Python:")
    print(f"  python -c 'import duckdb; conn = duckdb.connect(\"duckdb/weather.db\"); print(conn.execute(\"SELECT * FROM analytics_mart.weather_daily LIMIT 5\").fetchdf())'")

if __name__ == "__main__":
    main()
