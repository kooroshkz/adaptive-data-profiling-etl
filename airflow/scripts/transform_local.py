#!/usr/bin/env python3
"""Local Transform stage: build the weather warehouse from raw parquet.

Self-contained replacement for the GitHub-Actions dbt job. Runs entirely inside
the Airflow container with DuckDB and needs no cloud services. It:

  1. reads the ingested raw hourly parquet from RAW_DATA_PATH,
  2. builds a deduplicated staging table (latest ingestion per time+city wins),
  3. builds two daily marts (with and without synthetic anomalies), mirroring the
     original dbt models,
  4. writes them to a local DuckDB warehouse and exports each to parquet so the
     dashboard can read them without a database connection.

Usage:
    python transform_local.py
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

import duckdb

from config import RAW_DATA_PATH, WAREHOUSE_PATH, MART_DIR

_DAILY_MEASURES = """
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
        COUNT(*) AS total_hours
"""


def _raw_files() -> list[str]:
    return sorted(glob.glob(os.path.join(RAW_DATA_PATH, "city=*", "hourly_*.parquet")))


def main() -> None:
    files = _raw_files()
    if not files:
        print(f"[WARN] No raw parquet under {RAW_DATA_PATH}. Nothing to transform.")
        return

    paths_literal = "[" + ", ".join(f"'{Path(p).as_posix()}'" for p in files) + "]"

    Path(WAREHOUSE_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(MART_DIR).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS main;")

    # ── staging: dedup by (time, city_id), latest ingestion wins ──────────────
    con.execute(f"""
    CREATE OR REPLACE TABLE main.stg_weather AS
    WITH raw AS (
        SELECT * FROM read_parquet({paths_literal}, union_by_name=true)
        WHERE time IS NOT NULL
    ),
    deduped AS (
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY time, city_id ORDER BY ingestion_timestamp DESC
            ) AS rn FROM raw
        ) WHERE rn = 1
    )
    SELECT
        time,
        CAST(time AS DATE) AS date,
        EXTRACT(year FROM time)    AS year,
        EXTRACT(month FROM time)   AS month,
        EXTRACT(day FROM time)     AS day,
        EXTRACT(hour FROM time)    AS hour,
        EXTRACT(dow FROM time)     AS day_of_week,
        EXTRACT(quarter FROM time) AS quarter,
        temperature_2m, apparent_temperature, precipitation, surface_pressure,
        soil_temperature_7_to_28cm, soil_moisture_7_to_28cm,
        city_id, city_name, latitude, longitude, timezone,
        ingestion_timestamp, batch_id,
        COALESCE(synthetic_anomaly_flag, false) AS synthetic_anomaly_flag,
        synthetic_shift_pct, synthetic_anomaly_target_column,
        synthetic_original_value, synthetic_anomaly_batch_id,
        synthetic_anomaly_details_json
    FROM deduped
    """)

    # ── mart: daily aggregates WITH anomalies ─────────────────────────────────
    con.execute(f"""
    CREATE OR REPLACE TABLE main.weather_daily_with_anomalies AS
    SELECT
        city_id, city_name, date,
        {_DAILY_MEASURES},
        SUM(CASE WHEN synthetic_anomaly_flag THEN 1 ELSE 0 END) AS anomaly_hours,
        MAX(ingestion_timestamp) AS last_updated
    FROM main.stg_weather
    GROUP BY city_id, city_name, date
    ORDER BY date, city_id
    """)

    # ── mart: daily aggregates WITHOUT synthetic anomalies ────────────────────
    con.execute(f"""
    CREATE OR REPLACE TABLE main.weather_daily_without_anomalies AS
    SELECT
        city_id, city_name, date,
        {_DAILY_MEASURES},
        MAX(ingestion_timestamp) AS last_updated
    FROM main.stg_weather
    WHERE COALESCE(synthetic_anomaly_flag, false) = false
    GROUP BY city_id, city_name, date
    ORDER BY date, city_id
    """)

    # ── export marts to parquet for the dashboard ─────────────────────────────
    for tbl in ("stg_weather", "weather_daily_with_anomalies", "weather_daily_without_anomalies"):
        out = os.path.join(MART_DIR, f"{tbl}.parquet")
        con.execute(f"COPY main.{tbl} TO '{out}' (FORMAT PARQUET);")
        n = con.execute(f"SELECT COUNT(*) FROM main.{tbl}").fetchone()[0]
        print(f"[OK] {tbl}: {n} rows -> {out}")

    con.close()
    print(f"[DONE] warehouse: {WAREHOUSE_PATH}")


if __name__ == "__main__":
    main()
