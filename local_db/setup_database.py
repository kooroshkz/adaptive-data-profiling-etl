#!/usr/bin/env python3
"""DuckDB database initialization script."""

import os
import duckdb
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "duckdb" / "weather.db"
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"


def ensure_directories():
    """Create necessary directories."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    logger.info("Directories verified")


def create_database():
    """Create DuckDB database and return connection."""
    logger.info(f"Creating DuckDB database at: {DB_PATH}")
    conn = duckdb.connect(str(DB_PATH))
    logger.info("Database connection established")
    return conn


def create_schemas(conn):
    """Create database schemas."""
    logger.info("Creating schemas...")
    schemas = ["raw", "staging", "mart"]
    
    for schema in schemas:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        logger.info(f"Created schema: {schema}")


def create_raw_tables(conn):
    """Create raw.weather_hourly and raw.weather_daily tables from Parquet files."""
    logger.info("Creating raw weather tables...")
    
    hourly_files = list(RAW_DATA_PATH.glob("*_hourly_*.parquet"))
    daily_files = list(RAW_DATA_PATH.glob("*_daily_*.parquet"))
    
    if not hourly_files:
        logger.warning("No hourly Parquet files found in data/raw/")
        logger.info("Run ingestion first: python ingestion/weather_ingest.py")
        return
    
    logger.info(f"Found {len(hourly_files)} hourly Parquet file(s)")
    logger.info(f"Found {len(daily_files)} daily Parquet file(s)")
    
    conn.execute(f"""
        CREATE OR REPLACE TABLE raw.weather_hourly AS
        SELECT * FROM read_parquet('{RAW_DATA_PATH}/*_hourly_*.parquet')
    """)
    
    hourly_result = conn.execute("SELECT COUNT(*) FROM raw.weather_hourly").fetchone()
    hourly_count = hourly_result[0]
    
    logger.info(f"Loaded {hourly_count:,} rows into raw.weather_hourly")
    
    if daily_files:
        conn.execute(f"""
            CREATE OR REPLACE TABLE raw.weather_daily AS
            SELECT * FROM read_parquet('{RAW_DATA_PATH}/*_daily_*.parquet')
        """)
        
        daily_result = conn.execute("SELECT COUNT(*) FROM raw.weather_daily").fetchone()
        daily_count = daily_result[0]
        
        logger.info(f"Loaded {daily_count:,} rows into raw.weather_daily")


def create_staging_table(conn):
    """Create staging.weather_hourly table with transformations."""
    logger.info("Creating staging.weather_hourly table...")
    
    conn.execute("""
        CREATE OR REPLACE TABLE staging.weather_hourly AS
        SELECT
            time,
            CAST(time AS DATE) AS date,
            EXTRACT(year FROM time) AS year,
            EXTRACT(month FROM time) AS month,
            EXTRACT(day FROM time) AS day,
            EXTRACT(hour FROM time) AS hour,
            EXTRACT(dow FROM time) AS day_of_week,
            EXTRACT(quarter FROM time) AS quarter,
            
            COALESCE(temperature_2m, 0.0) AS temperature_2m,
            COALESCE(relative_humidity_2m, 0) AS relative_humidity_2m,
            COALESCE(precipitation, 0.0) AS precipitation,
            COALESCE(wind_speed_10m, 0.0) AS wind_speed_10m,
            COALESCE(cloud_cover, 0) AS cloud_cover,
            COALESCE(pressure_msl, 1013.25) AS pressure_msl,
            
            city_id,
            city_name,
            latitude,
            longitude,
            timezone,
            
            ingestion_timestamp,
            batch_id,
            
            CASE WHEN temperature_2m IS NULL THEN 1 ELSE 0 END AS has_missing_temp,
            CASE WHEN precipitation IS NULL THEN 1 ELSE 0 END AS has_missing_precip
        FROM raw.weather_hourly
    """)
    
    result = conn.execute("SELECT COUNT(*) FROM staging.weather_hourly").fetchone()
    logger.info(f"Created staging.weather_hourly with {result[0]:,} rows")


def create_mart_tables(conn):
    """Create mart layer tables."""
    logger.info("Creating mart tables...")
    
    conn.execute("""
        CREATE OR REPLACE TABLE mart.weather_daily AS
        SELECT
            city_id,
            city_name,
            date,
            
            MIN(temperature_2m) AS temp_min,
            MAX(temperature_2m) AS temp_max,
            AVG(temperature_2m) AS temp_avg,
            STDDEV(temperature_2m) AS temp_stddev,
            
            SUM(precipitation) AS precip_total,
            MAX(precipitation) AS precip_max,
            COUNT(CASE WHEN precipitation > 0 THEN 1 END) AS hours_with_rain,
            
            AVG(wind_speed_10m) AS wind_avg,
            MAX(wind_speed_10m) AS wind_max,
            
            AVG(relative_humidity_2m) AS humidity_avg,
            AVG(pressure_msl) AS pressure_avg,
            AVG(cloud_cover) AS cloud_cover_avg,
            
            COUNT(*) AS total_hours,
            SUM(has_missing_temp) AS missing_temp_count,
            
            MAX(ingestion_timestamp) AS last_updated
        FROM staging.weather_hourly
        GROUP BY city_id, city_name, date
        ORDER BY date, city_id
    """)
    
    result = conn.execute("SELECT COUNT(*) FROM mart.weather_daily").fetchone()
    logger.info(f"Created mart.weather_daily with {result[0]:,} rows")
    
    conn.execute("""
        CREATE OR REPLACE TABLE mart.weather_anomalies AS
        WITH stats AS (
            SELECT
                city_id,
                AVG(temperature_2m) AS avg_temp,
                STDDEV(temperature_2m) AS stddev_temp
            FROM staging.weather_hourly
            GROUP BY city_id
        )
        SELECT
            w.time,
            w.city_id,
            w.city_name,
            w.temperature_2m,
            w.precipitation,
            
            (w.temperature_2m - s.avg_temp) / NULLIF(s.stddev_temp, 0) AS temp_zscore,
            
            CASE 
                WHEN ABS((w.temperature_2m - s.avg_temp) / NULLIF(s.stddev_temp, 0)) > 3 THEN TRUE
                ELSE FALSE
            END AS is_temp_anomaly
            
        FROM staging.weather_hourly w
        JOIN stats s ON w.city_id = s.city_id
        WHERE s.stddev_temp IS NOT NULL
        ORDER BY w.time
    """)
    
    result = conn.execute("SELECT COUNT(*) FROM mart.weather_anomalies").fetchone()
    anomaly_count = conn.execute("SELECT COUNT(*) FROM mart.weather_anomalies WHERE is_temp_anomaly = TRUE").fetchone()
    
    logger.info(f"Created mart.weather_anomalies with {result[0]:,} rows")
    logger.info(f"Found {anomaly_count[0]} temperature anomalies")


def run_health_checks(conn):
    """Run database health checks."""
    logger.info("Running health checks...")
    
    tables = conn.execute("""
        SELECT schema_name, table_name, estimated_size
        FROM duckdb_tables()
        WHERE schema_name IN ('raw', 'staging', 'mart')
        ORDER BY schema_name, table_name
    """).fetchall()
    
    logger.info("\nDatabase Tables:")
    for schema, table, size in tables:
        logger.info(f"  {schema}.{table} - {size:,} bytes")
    
    date_range = conn.execute("""
        SELECT 
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            COUNT(DISTINCT city_id) AS city_count
        FROM staging.weather_hourly
    """).fetchone()
    
    logger.info(f"\nData Coverage:")
    logger.info(f"  Date Range: {date_range[0]} to {date_range[1]}")
    logger.info(f"  Cities: {date_range[2]}")
    
    logger.info("\nAll health checks passed")


def main():
    """Main execution."""
    logger.info("=" * 70)
    logger.info("DUCKDB DATABASE SETUP")
    logger.info("=" * 70)
    
    try:
        ensure_directories()
        conn = create_database()
        create_schemas(conn)
        create_raw_tables(conn)
        create_staging_table(conn)
        create_mart_tables(conn)
        run_health_checks(conn)
        conn.close()
        
        logger.info("=" * 70)
        logger.info("DATABASE SETUP COMPLETE")
        logger.info("=" * 70)
        logger.info(f"\nDatabase location: {DB_PATH}")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
