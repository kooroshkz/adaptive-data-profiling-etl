#!/usr/bin/env python3
"""Benchmark actual per-trial training time of every PyOD model on the real data.

Runs each algorithm (IForest, LOF, ECOD, HBOS, COPOD) once on every
(city, column) combination to measure fit+predict wall time. Multiplied
by n_trials gives a realistic estimate of what the full AutoML Optuna
search spent on each combination.

Outputs: artifacts/automl_per_trial_timing.csv
         artifacts/automl_per_model_timing.csv  (per algorithm, mean/median across all combos)
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).parent))
from data_loader import load_city_data_from_local
from nn_configs import DEFAULT_CITIES, FEATURE_COLUMNS

# ── PyOD models (same as AutoML experiment) ────────────────────────────────
from pyod.models.iforest import IForest
from pyod.models.lof import LOF
from pyod.models.ecod import ECOD
from pyod.models.hbos import HBOS
from pyod.models.copod import COPOD

AUTOML_N_TRIALS = 25   # what the real AutoML run used


def make_models() -> dict:
    """Return representative instances of every AutoML model (fixed typical params)."""
    return {
        "IForest": IForest(contamination=0.05, n_estimators=100, max_samples=0.8, random_state=42, n_jobs=1),
        "LOF":     LOF(contamination=0.05, n_neighbors=30, leaf_size=30),
        "ECOD":    ECOD(contamination=0.05),
        "HBOS":    HBOS(contamination=0.05, n_bins=20),
        "COPOD":   COPOD(contamination=0.05),
    }


def time_model_once(X: np.ndarray, model_name: str, seed: int = 42) -> float:
    """Fit + predict one model, return wall time in seconds."""
    models = make_models()
    m = models[model_name]
    t0 = time.perf_counter()
    m.fit(X)
    m.predict(X)
    return time.perf_counter() - t0


def preprocess(X: np.ndarray) -> np.ndarray:
    imp = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    return scaler.fit_transform(imp.fit_transform(X))


def run_benchmark(n_repeats: int = 3) -> pd.DataFrame:
    rows = []
    for city in DEFAULT_CITIES:
        print(f"[INFO] Loading {city}...")
        df = load_city_data_from_local(city, None, None)
        if df.empty:
            continue

        # Univariate benchmarks
        for col in FEATURE_COLUMNS:
            X_raw = df[[col]].to_numpy(dtype=float)
            X = preprocess(X_raw)

            for model_name in ["IForest", "LOF", "ECOD", "HBOS", "COPOD"]:
                trial_times = []
                for _ in range(n_repeats):
                    t = time_model_once(X, model_name)
                    trial_times.append(t)
                mean_t = float(np.mean(trial_times))
                rows.append({
                    "city": city,
                    "scope": "univariate",
                    "target_column": col,
                    "model": model_name,
                    "n_rows": len(X),
                    "n_features": 1,
                    "single_trial_s": round(mean_t, 5),
                    "estimated_search_s": round(mean_t * AUTOML_N_TRIALS, 3),
                })

        # Multivariate benchmark
        X_raw = df[FEATURE_COLUMNS].to_numpy(dtype=float)
        X = preprocess(X_raw)
        for model_name in ["IForest", "LOF", "ECOD", "HBOS", "COPOD"]:
            trial_times = []
            for _ in range(n_repeats):
                t = time_model_once(X, model_name)
                trial_times.append(t)
            mean_t = float(np.mean(trial_times))
            rows.append({
                "city": city,
                "scope": "multivariate",
                "target_column": "ALL_FEATURES",
                "model": model_name,
                "n_rows": len(X),
                "n_features": len(FEATURE_COLUMNS),
                "single_trial_s": round(mean_t, 5),
                "estimated_search_s": round(mean_t * AUTOML_N_TRIALS, 3),
            })

    return pd.DataFrame(rows)


def main() -> None:
    out_dir = Path(__file__).parent / "artifacts"
    out_dir.mkdir(exist_ok=True)

    print("[START] AutoML per-algorithm timing benchmark")
    df = run_benchmark(n_repeats=3)

    per_trial_path = out_dir / "automl_per_trial_timing.csv"
    df.to_csv(per_trial_path, index=False)
    print(f"[SAVED] {per_trial_path}")

    # Summary by algorithm
    summary = df.groupby("model").agg(
        mean_single_trial_s=("single_trial_s", "mean"),
        median_single_trial_s=("single_trial_s", "median"),
        max_single_trial_s=("single_trial_s", "max"),
        mean_estimated_search_s=("estimated_search_s", "mean"),
    ).round(4).reset_index()
    summary["n_trials"] = AUTOML_N_TRIALS

    per_model_path = out_dir / "automl_per_model_timing.csv"
    summary.to_csv(per_model_path, index=False)
    print(f"[SAVED] {per_model_path}")

    print("\nPer-algorithm summary (univariate + multivariate combined):")
    print(summary.to_string(index=False))

    # Best AutoML winner per (city, column) timing
    best_selected = {"LOF": 29, "ECOD": 4, "HBOS": 1, "COPOD": 1}
    total_weighted = sum(
        summary[summary["model"] == m]["mean_single_trial_s"].values[0] * AUTOML_N_TRIALS * cnt
        for m, cnt in best_selected.items()
        if len(summary[summary["model"] == m]) > 0
    )
    print(f"\nWeighted total AutoML search time (by model selection frequency): {total_weighted:.1f}s")
    print("[DONE]")


if __name__ == "__main__":
    main()
