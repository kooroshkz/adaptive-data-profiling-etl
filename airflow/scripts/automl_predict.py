#!/usr/bin/env python3
"""Score one city's data and write predictions to the local results store.

Loads the most recent hourly parquet for *city*, calls Profiler.score() for
each configured column, and writes a predictions parquet to:

  data/anomaly_results/city={city}/date={date}/predictions.parquet

(also mirrored to S3 only if S3 is configured). Skips gracefully if no model has
been trained yet for a given city/column.

Called by weather_dag.py after each city's ingestion task.

Usage:
    python automl_predict.py --city amsterdam [--date 2026-05-06] [--schema ...]
"""

from __future__ import annotations

import argparse
import io
import os
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[2]

from adaptive_profiler import Profiler
from adaptive_profiler.storage import LocalStore

from config import RAW_DATA_PATH, ANOMALY_RESULTS_DIR, MODELS_DIR, s3_enabled

_SCHEMA = _REPO_ROOT / "profiling_schema.yml"


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


def load_city_date_data(city: str, run_date: str, feature_cols: list[str]) -> pd.DataFrame:
    """Load data for *city* on *run_date* from the local parquet mirror."""
    city_dir = Path(RAW_DATA_PATH) / f"city={city}"
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
            {cols_sql}
        FROM read_parquet({paths_literal}, union_by_name=true)
        WHERE time IS NOT NULL
          AND CAST(time AS DATE) = CAST('{run_date}' AS DATE)
        GROUP BY time, city_id
        ORDER BY time
    """).fetch_df()
    con.close()
    return df


def write_predictions(df: pd.DataFrame, city: str, run_date: str) -> str:
    """Write predictions to the local anomaly-results store (default).

    Mirrors to S3 only when S3 is explicitly configured.
    """
    out_dir = Path(ANOMALY_RESULTS_DIR) / f"city={city}" / f"date={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "predictions.parquet"
    df.to_parquet(out_path, index=False)

    if s3_enabled():
        return _upload_predictions_to_s3(df, city, run_date)
    return str(out_path)


def _upload_predictions_to_s3(df: pd.DataFrame, city: str, run_date: str) -> str:
    import boto3

    bucket = os.getenv("S3_BUCKET")
    key = f"anomaly_results/city={city}/date={run_date}/predictions.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "eu-west-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
    ).put_object(Bucket=bucket, Key=key, Body=buf.read())
    return f"s3://{bucket}/{key}"


def _models_are_lfs_pointers(city: str, feature_cols: list[str]) -> bool:
    """True if the local model files are unresolved Git LFS pointer stubs.

    Lets a clone made without git-lfs run the pipeline anyway: scoring is skipped
    with a clear message instead of crashing on a pointer file.
    """
    base = Path(MODELS_DIR)
    saw_existing = False
    for col in feature_cols:
        p = base / f"partition={city}" / f"col={col}" / "latest.pkl"
        if not p.exists():
            continue
        saw_existing = True
        try:
            head = p.read_bytes()[:64]
        except OSError:
            continue
        if not head.startswith(b"version https://git-lfs"):
            return False  # at least one real model present
    return saw_existing  # every model that exists was a pointer


def main() -> None:
    _load_env()

    parser = argparse.ArgumentParser(description="Score one city's data with adaptive_profiler")
    parser.add_argument("--city", required=True)
    parser.add_argument("--date", default=str(date.today()))
    parser.add_argument("--schema", default=str(_SCHEMA))
    args = parser.parse_args()

    city = args.city
    run_date = args.date

    profiler = Profiler.from_yaml(args.schema)
    # Force the local committed model store unless the schema explicitly asks
    # for S3. Keeps the git-tracked models working with no AWS credentials.
    if profiler._config.model_store.backend == "local":
        profiler._store = LocalStore(base_dir=MODELS_DIR)
    feature_cols = [c.name for c in profiler.automl_columns]

    print(f"[INFO] city={city} date={run_date} store={profiler._store}")

    # Show which models are already trained
    status = profiler.model_status(city)
    trained = [col for col, ok in status.items() if ok]
    missing = [col for col, ok in status.items() if not ok]
    if missing:
        print(f"[INFO] No model yet for: {missing}  (will skip those columns)")

    df = load_city_date_data(city, run_date, feature_cols)
    if df.empty:
        print(f"[INFO] No data for city={city} date={run_date}. Nothing to score.")
        return

    print(f"[INFO] Loaded {len(df)} rows")

    try:
        predictions = profiler.score(partition_key=city, df=df)
    except Exception:
        if _models_are_lfs_pointers(city, feature_cols):
            print(
                "[WARN] Model files are Git LFS pointers, not real models. "
                "Run `git lfs pull`, or trigger the weather_automl_train DAG to "
                "train models locally. Skipping scoring for this run."
            )
            return
        raise

    if predictions.empty:
        print("[INFO] No columns scored.")
        return

    n_flagged = predictions["automl_flag"].eq(1).sum()
    n_violations = predictions["quality_violation"].notna().sum()
    print(
        f"[INFO] Scored {predictions['column'].nunique()} columns | "
        f"anomaly flags={n_flagged} | quality violations={n_violations}"
    )

    out_path = write_predictions(predictions, city, run_date)
    print(f"[OK]   Predictions → {out_path}")


if __name__ == "__main__":
    main()
