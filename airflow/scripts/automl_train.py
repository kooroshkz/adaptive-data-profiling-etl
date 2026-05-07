#!/usr/bin/env python3
"""Train AutoML anomaly detection models and persist them to S3.

Reads historical data from the local parquet mirror, trains one model per
city × column pair using the adaptive_profiler library, and stores each
artifact via the model_store defined in profiling_schema.yml.

Called by the weather_automl_train DAG (manual trigger).

Usage:
    python automl_train.py [--cities amsterdam london] [--schema /path/to/schema.yml]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

from adaptive_profiler import Profiler

_SCHEMA = _REPO_ROOT / "profiling_schema.yml"
_CITIES = ["amsterdam", "london", "new_york", "paris", "tokyo"]


def _load_env() -> None:
    env_path = _REPO_ROOT / "airflow" / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and os.getenv(k) is None:
            os.environ[k] = v


def load_city_data(city: str, feature_cols: list[str]) -> pd.DataFrame:
    """Load deduplicated historical parquet for *city* from the local mirror."""
    city_dir = _REPO_ROOT / "airflow" / "data" / "raw" / f"city={city}"
    candidates = sorted(city_dir.glob("hourly_*.parquet"))
    if not candidates:
        return pd.DataFrame()

    paths_literal = "[" + ", ".join(f"'{p.as_posix()}'" for p in candidates) + "]"
    cols_sql = ", ".join(f"MAX({c}) AS {c}" for c in feature_cols)

    con = duckdb.connect(":memory:")
    df = con.execute(f"""
        SELECT
            time,
            city_id,
            {cols_sql},
            CAST(MAX(COALESCE(CAST(synthetic_anomaly_flag AS INTEGER), 0)) AS INTEGER) AS y_true
        FROM read_parquet({paths_literal}, union_by_name=true)
        WHERE time IS NOT NULL
        GROUP BY time, city_id
        ORDER BY time
    """).fetch_df()
    con.close()
    return df


def main() -> None:
    _load_env()

    parser = argparse.ArgumentParser(description="Train adaptive_profiler models for all cities")
    parser.add_argument("--cities", nargs="*", default=_CITIES)
    parser.add_argument("--schema", default=str(_SCHEMA))
    args = parser.parse_args()

    profiler = Profiler.from_yaml(args.schema)
    feature_cols = profiler.automl_columns

    print(f"[INFO] Schema    : {args.schema}")
    print(f"[INFO] Store     : {profiler._store}")
    print(f"[INFO] Cities    : {args.cities}")
    print(f"[INFO] Columns   : {[c.name for c in feature_cols]}")
    print()

    for city in args.cities:
        print(f"── city={city} " + "─" * 40)
        df = load_city_data(city, [c.name for c in feature_cols])

        if df.empty:
            print(f"   [WARN] No parquet files found. Skipping.")
            continue

        print(f"   Loaded {len(df):,} rows")
        results = profiler.train(partition_key=city, df=df)
        for r in results:
            print(f"   {r}")

    print("\n[INFO] Training complete.")


if __name__ == "__main__":
    main()
