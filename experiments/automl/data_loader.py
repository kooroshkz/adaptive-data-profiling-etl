"""S3 data loading and local caching helpers for AutoML experiments."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd

from pyod_configs import FEATURE_COLUMNS


def _load_env_fallback() -> None:
    """Load credentials from airflow/.env when not already present in process env."""
    repo_root = Path(__file__).resolve().parents[2]
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


def has_s3_credentials() -> bool:
    return bool(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))


def cache_dir() -> Path:
    path = Path("experiments/data/automl")
    path.mkdir(parents=True, exist_ok=True)
    return path


def committed_data_dir() -> Path:
    """Directory of committed per-city parquet files used for offline/reproducible runs.

    These files are checked into the repository so the experiments can run without
    any S3 setup (use ``--data-source local``).
    """
    return Path(__file__).resolve().parent.parent / "data" / "automl"


def committed_city_path(city: str) -> Path:
    return committed_data_dir() / f"{city}_all_all.parquet"


def load_city_data_from_committed(
    city: str,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    """Read a city's data from the committed parquet files (no S3, no network)."""
    path = committed_city_path(city)
    if not path.exists():
        return pd.DataFrame()

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
        SELECT *
        FROM read_parquet('{path.as_posix()}')
        WHERE time IS NOT NULL{date_filters}
        ORDER BY time
    """
    return con.execute(query, params).fetch_df()


def cached_city_path(city: str, start_date: str | None, end_date: str | None) -> Path:
    start_part = start_date or "all"
    end_part = end_date or "all"
    return cache_dir() / f"{city}_{start_part}_{end_part}.parquet"


def latest_local_city_parquet(city: str) -> Path | None:
    city_dir = Path("airflow/data/raw") / f"city={city}"
    candidates = sorted(city_dir.glob("hourly_*.parquet"))
    return candidates[-1] if candidates else None


def load_city_data_from_local(
    city: str,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    city_dir = Path("airflow/data/raw") / f"city={city}"
    candidates = sorted(city_dir.glob("hourly_*.parquet"))
    if not candidates:
        return pd.DataFrame()

    # Build a JSON array literal so DuckDB reads all files in one pass.
    # Multiple overlapping parquet files (different ingestion batches) can produce
    # duplicate (time, city_id) rows. We deduplicate by grouping on those keys and
    # taking MAX(synthetic_anomaly_flag) so that any injection run marking a timestamp
    # as anomalous wins over an older file that has NULL for that column.
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
    # Do NOT filter on synthetic_anomaly_flag IS NOT NULL — historical rows pre-dating
    # anomaly injection have NULL for that column, which is correctly treated as FALSE
    # via COALESCE. Filtering here would drop the vast majority of the dataset.
    where_clauses = [
        "city_id = ?",
        "time IS NOT NULL",
    ]
    params: list[str] = [city]

    if start_date:
        where_clauses.append("CAST(time AS DATE) >= CAST(? AS DATE)")
        params.append(start_date)
    if end_date:
        where_clauses.append("CAST(time AS DATE) <= CAST(? AS DATE)")
        params.append(end_date)

    # GROUP BY deduplicates overlapping parquet files (multiple ingestion batches
    # can cover the same date range). MAX on anomaly flag: any injection wins.
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


def fetch_and_cache_city_data(
    con: duckdb.DuckDBPyConnection | None,
    bucket: str,
    city: str,
    start_date: str | None,
    end_date: str | None,
    data_source: str = "auto",
) -> pd.DataFrame:
    """Load a city's data.

    data_source:
      - "local": read only the committed parquet files (no S3, no network).
      - "s3":    read only from S3 (requires AWS credentials).
      - "auto":  use S3 when credentials are available, otherwise fall back to the
                 local raw parquet mirror under airflow/data/raw.
    """
    if data_source == "local":
        print(f"[INFO] Reading committed local data for city={city}.")
        return load_city_data_from_committed(city, start_date, end_date)

    if data_source == "s3":
        df_city = load_city_data_from_s3(con, bucket, city, start_date, end_date)
    elif has_s3_credentials():
        try:
            df_city = load_city_data_from_s3(con, bucket, city, start_date, end_date)
        except duckdb.HTTPException as exc:
            print(f"[WARN] S3 fetch failed for city={city}: {exc}. Falling back to local parquet.")
            df_city = load_city_data_from_local(city, start_date, end_date)
    else:
        print(f"[INFO] No AWS credentials found; using local parquet mirror for city={city}.")
        df_city = load_city_data_from_local(city, start_date, end_date)

    cache_path = cached_city_path(city, start_date, end_date)
    if not df_city.empty:
        df_city.to_parquet(cache_path, index=False)
    return df_city