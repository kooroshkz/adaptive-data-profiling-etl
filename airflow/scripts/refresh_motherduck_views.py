#!/usr/bin/env python3
"""Drop and recreate MotherDuck views for raw city data and daily aggregates."""

import argparse
import os

import duckdb


CITIES = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect_motherduck() -> duckdb.DuckDBPyConnection:
    token = os.getenv('MOTHERDUCK_TOKEN')
    if not token:
        raise RuntimeError('MOTHERDUCK_TOKEN is required')

    con = duckdb.connect(f'md:?motherduck_token={token}')
    con.execute('INSTALL httpfs')
    con.execute('LOAD httpfs')

    aws_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION', 'eu-west-1')
    con.execute(f'SET s3_region = {_sql_literal(aws_region)}')
    if aws_key:
        con.execute(f'SET s3_access_key_id = {_sql_literal(aws_key)}')
    if aws_secret:
        con.execute(f'SET s3_secret_access_key = {_sql_literal(aws_secret)}')

    return con


def create_raw_view_sql(bucket: str, city: str) -> str:
    return f"""
    CREATE OR REPLACE VIEW main.{city} AS
    SELECT * EXCLUDE (rn)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY time, city_id
                ORDER BY ingestion_timestamp DESC
            ) AS rn
        FROM read_parquet(
            's3://{bucket}/raw/city={city}/hourly_*.parquet',
            hive_partitioning=true,
            union_by_name=true
        )
    )
    WHERE rn = 1;
    """


def create_daily_view_sql(bucket: str, view_name: str, anomaly_filter: str = '') -> str:
    where_clause = f"WHERE {anomaly_filter}" if anomaly_filter else ''
    return f"""
    CREATE OR REPLACE VIEW main.{view_name} AS
    WITH source AS (
        SELECT *
        FROM read_parquet(
            's3://{bucket}/raw/city=*/hourly_*.parquet',
            hive_partitioning=true,
            union_by_name=true
        )
    ),
    deduped AS (
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY time, city_id
                    ORDER BY ingestion_timestamp DESC
                ) AS rn
            FROM source
        )
        WHERE rn = 1
    )
    SELECT
        city_id,
        city_name,
        CAST(time AS DATE) AS date,
        CAST(CAST(time AS DATE) AS TIMESTAMP) AS time,
        MIN(temperature_2m) AS temperature_2m_min,
        MAX(temperature_2m) AS temperature_2m_max,
        AVG(temperature_2m) AS temperature_2m_avg,
        MIN(apparent_temperature) AS apparent_temperature_min,
        MAX(apparent_temperature) AS apparent_temperature_max,
        AVG(apparent_temperature) AS apparent_temperature_avg,
        SUM(precipitation) AS precipitation_total,
        AVG(surface_pressure) AS surface_pressure_avg,
        AVG(soil_temperature_7_to_28cm) AS soil_temperature_7_to_28cm_avg,
        AVG(soil_moisture_7_to_28cm) AS soil_moisture_7_to_28cm_avg,
        COUNT(*) AS total_hours,
        SUM(CASE WHEN coalesce(synthetic_anomaly_flag, false) THEN 1 ELSE 0 END) AS anomaly_hours,
        CASE WHEN SUM(CASE WHEN coalesce(synthetic_anomaly_flag, false) THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END AS synthetic_anomaly_flag,
        CAST(NULL AS DOUBLE) AS synthetic_shift_pct,
        CAST(NULL AS VARCHAR) AS synthetic_anomaly_target_column,
        CAST(NULL AS VARCHAR) AS synthetic_anomaly_details_json,
        MAX(ingestion_timestamp) AS last_updated
    FROM deduped
    {where_clause}
    GROUP BY city_id, city_name, CAST(time AS DATE)
    ORDER BY date, city_id;
    """


def drop_existing_views(con: duckdb.DuckDBPyConnection, view_names: list[str]) -> None:
    for view_name in view_names:
        con.execute(f'DROP VIEW IF EXISTS main.{view_name}')


def drop_existing_databases(con: duckdb.DuckDBPyConnection, database_names: list[str]) -> None:
    for database_name in database_names:
        con.execute(f'DROP DATABASE IF EXISTS {database_name}')


def refresh_views(drop_only: bool = False) -> None:
    bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
    con = connect_motherduck()

    raw_database = 'raw_weather_data'
    analytics_database = 'weather_analytics'
    scratch_database = 'weather_reset_scratch'
    legacy_databases = ['weather_anomalies', 'weather_data']

    con.execute(f'CREATE DATABASE IF NOT EXISTS {scratch_database}')
    con.execute(f'USE {scratch_database}')

    drop_existing_databases(con, [raw_database, analytics_database, *legacy_databases])

    if drop_only:
        con.close()
        return

    con.execute(f'CREATE DATABASE IF NOT EXISTS {raw_database}')
    con.execute(f'CREATE DATABASE IF NOT EXISTS {analytics_database}')

    con.execute(f'USE {raw_database}')
    con.execute('CREATE SCHEMA IF NOT EXISTS main')
    con.execute('USE main')
    for city in CITIES:
        con.execute(create_raw_view_sql(bucket, city))

    con.execute(f'USE {analytics_database}')
    con.execute('CREATE SCHEMA IF NOT EXISTS main')
    con.execute('USE main')
    for city in CITIES:
        con.execute(create_daily_view_sql(bucket, f'weather_daily_with_anomalies_{city}'))
        con.execute(create_daily_view_sql(
            bucket,
            f'weather_daily_without_anomalies_{city}',
            "coalesce(synthetic_anomaly_flag, false) = false",
        ))

    con.execute(f'USE {raw_database}')
    drop_existing_databases(con, [scratch_database])

    con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description='Drop/recreate MotherDuck views for weather data')
    parser.add_argument('--drop-only', action='store_true', help='Only drop existing views, do not recreate them')
    args = parser.parse_args()

    refresh_views(drop_only=args.drop_only)


if __name__ == '__main__':
    main()