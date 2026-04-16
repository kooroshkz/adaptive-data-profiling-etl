#!/usr/bin/env python3
"""Thin entrypoint for AutoML anomaly experiments."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from pyod_configs import DEFAULT_CITIES
from experiment_runner import run_experiments, save_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PyOD + Optuna AutoML anomaly experiments on S3 weather data")
    parser.add_argument("--cities", nargs="+", default=DEFAULT_CITIES)
    parser.add_argument("--scope", choices=["multivariate", "univariate", "both"], default="both")
    parser.add_argument("--n-trials", type=int, default=25)
    parser.add_argument("--start-date", default=None, help="Optional start date filter YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Optional end date filter YYYY-MM-DD")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET", "weather-data-koorosh-thesis"))
    parser.add_argument("--mlflow-tracking-uri", default=os.getenv("MLFLOW_TRACKING_URI"))
    parser.add_argument("--mlflow-experiment", default=os.getenv("MLFLOW_EXPERIMENT_NAME", "weather_automl"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path("experiments/automl/artifacts") / f"run_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_rows, best_models = run_experiments(
        cities=args.cities,
        scope=args.scope,
        n_trials=args.n_trials,
        seed=args.seed,
        start_date=args.start_date,
        end_date=args.end_date,
        bucket=args.s3_bucket,
        output_dir=output_dir,
        tracking_uri=args.mlflow_tracking_uri,
        experiment_name=args.mlflow_experiment,
    )

    save_outputs(output_dir=output_dir, summary_rows=summary_rows, best_models=best_models)
    print(f"[DONE] Experiment artifacts written to: {output_dir}")


if __name__ == "__main__":
    main()
