"""S3/local data loading helpers – mirrors experiments/automl/data_loader.py."""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd

from nn_configs import FEATURE_COLUMNS


def _load_env_fallback() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    env_path = repo_root / "airflow" / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and os.getenv(key) is None:
            os.environ[key] = value


_load_env_fallback()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def has_s3_credentials() -> bool:
    return bool(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))


def connect_s3_duckdb() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    aws_region = os.getenv("AWS_REGION", "eu-west-1")
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    con.execute(f"SET s3_region = {sql_literal(aws_region)}")
    if aws_access_key_id:
        con.execute(f"SET s3_access_key_id = {sql_literal(aws_access_key_id)}")
    if aws_secret_access_key:
        con.execute(f"SET s3_secret_access_key = {sql_literal(aws_secret_access_key)}")
    return con


def cache_dir() -> Path:
    path = Path(__file__).parent / "data_cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_city_data_from_local(
    city: str,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    repo_root = Path(__file__).resolve().parents[1]
    city_dir = repo_root / "airflow" / "data" / "raw" / f"city={city}"
    candidates = sorted(city_dir.glob("hourly_*.parquet"))
    if not candidates:
        return pd.DataFrame()

    paths_literal = "[" + ", ".join(f"'{p.as_posix()}'" for p in candidates) + "]"
    date_filters = ""
    params: list[str] = []
    if start_date:
        date_filters += " AND CAST(time AS DATE) >= CAST(? AS DATE)"
        params.append(start_date)
    if end_date:
        date_filters += " AND CAST(time AS DATE) <= CAST(? AS DATE)"
        params.append(end_date)

    con = duckdb.connect(":memory:")
    query = f"""
        SELECT
            time,
            city_id,
            {", ".join(f"MAX({c}) AS {c}" for c in FEATURE_COLUMNS)},
            CAST(MAX(COALESCE(CAST(synthetic_anomaly_flag AS INTEGER), 0)) AS INTEGER) AS y_true,
            MAX(ABS(COALESCE(CAST(synthetic_shift_pct AS DOUBLE), 0.0))) AS synthetic_shift_pct,
            MAX(CASE WHEN COALESCE(CAST(synthetic_anomaly_flag AS INTEGER), 0) = 1
                THEN synthetic_anomaly_details_json ELSE NULL END) AS synthetic_anomaly_details_json
        FROM read_parquet({paths_literal}, union_by_name=true)
        WHERE time IS NOT NULL{date_filters}
        GROUP BY time, city_id
        ORDER BY time
    """
    return con.execute(query, params).fetch_df()


def load_city_data_from_s3(
    con: duckdb.DuckDBPyConnection,
    bucket: str,
    city: str,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    where_clauses = ["city_id = ?", "time IS NOT NULL"]
    params: list[str] = [city]
    if start_date:
        where_clauses.append("CAST(time AS DATE) >= CAST(? AS DATE)")
        params.append(start_date)
    if end_date:
        where_clauses.append("CAST(time AS DATE) <= CAST(? AS DATE)")
        params.append(end_date)

    query = f"""
        SELECT
            time,
            city_id,
            {", ".join(f"MAX({c}) AS {c}" for c in FEATURE_COLUMNS)},
            CAST(MAX(COALESCE(CAST(synthetic_anomaly_flag AS INTEGER), 0)) AS INTEGER) AS y_true,
            MAX(ABS(COALESCE(CAST(synthetic_shift_pct AS DOUBLE), 0.0))) AS synthetic_shift_pct,
            MAX(CASE WHEN COALESCE(CAST(synthetic_anomaly_flag AS INTEGER), 0) = 1
                THEN synthetic_anomaly_details_json ELSE NULL END) AS synthetic_anomaly_details_json
        FROM read_parquet(
            's3://{bucket}/raw/city={city}/hourly_*.parquet',
            hive_partitioning=true,
            union_by_name=true
        )
        WHERE {' AND '.join(where_clauses)}
        GROUP BY time, city_id
        ORDER BY time
    """
    return con.execute(query, params).fetch_df()


def fetch_city_data(
    con: duckdb.DuckDBPyConnection,
    bucket: str,
    city: str,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    if has_s3_credentials():
        try:
            df = load_city_data_from_s3(con, bucket, city, start_date, end_date)
            return df
        except Exception as exc:
            print(f"[WARN] S3 fetch failed for {city}: {exc}. Falling back to local.")
    print(f"[INFO] Using local parquet for city={city}.")
    return load_city_data_from_local(city, start_date, end_date)
