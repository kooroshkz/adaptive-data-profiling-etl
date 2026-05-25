#!/usr/bin/env python3
"""Entry point for Non-Linear / Auto-NN anomaly detection experiment."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

from nn_configs import DEFAULT_CITIES
from experiment_runner import run_experiments, save_outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Non-linear + Auto-NN anomaly detection experiment (AE, VAE, OCSVM)"
    )
    parser.add_argument("--cities", nargs="+", default=DEFAULT_CITIES)
    parser.add_argument("--scope", choices=["multivariate", "univariate", "both"], default="both")
    parser.add_argument("--n-trials", type=int, default=15,
                        help="Optuna trials per (city, column). Default 15 keeps total runtime reasonable.")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET", "weather-data-koorosh-thesis"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = Path(__file__).parent / "artifacts" / f"run_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[START] Non-Linear Experiment → {output_dir}")
    print(f"        cities={args.cities}  scope={args.scope}  n_trials={args.n_trials}")

    summary_rows, best_models = run_experiments(
        cities=args.cities,
        scope=args.scope,
        n_trials=args.n_trials,
        seed=args.seed,
        start_date=args.start_date,
        end_date=args.end_date,
        bucket=args.s3_bucket,
        output_dir=output_dir,
    )

    save_outputs(output_dir=output_dir, summary_rows=summary_rows, best_models=best_models)
    print(f"[DONE] Artifacts → {output_dir}")


if __name__ == "__main__":
    main()
