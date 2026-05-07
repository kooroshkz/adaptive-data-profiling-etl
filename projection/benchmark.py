#!/usr/bin/env python3
"""
AutoML Scalability Benchmark
=============================
Empirically measures training time T(n, m, k) across a grid of:
  n = rows per partition
  m = number of monitored columns
  k = number of Optuna HPO trials

Fits the formula:  T(n, m, k, p) ≈ (α · n^β · m^δ · k) / p

p (compute power index) is measured on the current machine via a
GEMM micro-benchmark so results are hardware-normalised.

Usage:
  python benchmark.py [--quick]    # --quick runs a smaller grid
"""

import argparse
import json
import platform
import time
from itertools import product
from pathlib import Path

import numpy as np
import optuna
import pandas as pd
from pyod.models.copod import COPOD
from pyod.models.ecod import ECOD
from pyod.models.hbos import HBOS
from pyod.models.iforest import IForest
from pyod.models.lof import LOF

optuna.logging.set_verbosity(optuna.logging.WARNING)

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_GLOB = str(REPO_ROOT / "airflow" / "data" / "raw" / "city=amsterdam" / "hourly_*.parquet")
RESULTS_DIR = Path(__file__).parent
FEATURE_COLS = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "surface_pressure",
    "soil_temperature_7_to_28cm",
    "soil_moisture_7_to_28cm",
]

# ── Compute-power benchmark ───────────────────────────────────────────────────

def measure_compute_index(reps: int = 5, size: int = 1500) -> float:
    """GEMM micro-benchmark. Returns GFLOPS/s as compute index p."""
    A = np.random.randn(size, size).astype(np.float64)
    B = np.random.randn(size, size).astype(np.float64)
    flops = 2.0 * size**3
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        _ = A @ B
        times.append(time.perf_counter() - t0)
    median_sec = float(np.median(times))
    gflops = (flops / median_sec) / 1e9
    return gflops


# ── Single-model fit timer ────────────────────────────────────────────────────

_MODEL_REGISTRY = {
    "IForest": lambda p: IForest(contamination=p["contamination"], n_estimators=p["n_estimators"], random_state=42),
    "LOF":     lambda p: LOF(contamination=p["contamination"], n_neighbors=p["n_neighbors"]),
    "HBOS":    lambda p: HBOS(contamination=p["contamination"], n_bins=p["n_bins"]),
    "COPOD":   lambda p: COPOD(contamination=p["contamination"]),
    "ECOD":    lambda p: ECOD(contamination=p["contamination"]),
}

def _sample_params(trial: optuna.Trial, model_name: str) -> dict:
    cont = trial.suggest_float("contamination", 0.001, 0.1, log=True)
    if model_name == "IForest":
        return {"contamination": cont, "n_estimators": trial.suggest_int("n_estimators", 50, 200)}
    if model_name == "LOF":
        return {"contamination": cont, "n_neighbors": trial.suggest_int("n_neighbors", 5, 50)}
    if model_name == "HBOS":
        return {"contamination": cont, "n_bins": trial.suggest_int("n_bins", 10, 50)}
    return {"contamination": cont}


def time_single_trial(X: np.ndarray, model_name: str, trial: optuna.Trial) -> float:
    """Returns wall-clock seconds for one model fit."""
    params = _sample_params(trial, model_name)
    model = _MODEL_REGISTRY[model_name](params)
    t0 = time.perf_counter()
    model.fit(X)
    return time.perf_counter() - t0


def run_cell(X_full: np.ndarray, n: int, m: int, k: int) -> dict:
    """
    Run k Optuna trials on an n×m subsample. Returns timing stats.
    Models are drawn uniformly at random (as in the production AutoML).
    """
    rng = np.random.default_rng(42)
    idx = rng.choice(len(X_full), size=min(n, len(X_full)), replace=False)
    col_idx = rng.choice(X_full.shape[1], size=m, replace=False)
    X = X_full[np.ix_(idx, col_idx)]

    trial_times = []
    model_names = list(_MODEL_REGISTRY.keys())

    def objective(trial):
        mname = model_names[trial.number % len(model_names)]
        t = time_single_trial(X, mname, trial)
        trial_times.append(t)
        return rng.random()  # dummy objective — we only care about wall time

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=42))
    study.optimize(objective, n_trials=k, show_progress_bar=False)

    return {
        "n": n, "m": m, "k": k,
        "total_sec": sum(trial_times),
        "mean_trial_sec": float(np.mean(trial_times)),
        "median_trial_sec": float(np.median(trial_times)),
        "std_trial_sec": float(np.std(trial_times)),
        "n_actual": len(idx),
    }


# ── Grid sweep ────────────────────────────────────────────────────────────────

def build_grid(quick: bool) -> list[tuple[int, int, int]]:
    """Returns list of (n, m, k) parameter combinations to benchmark."""
    if quick:
        n_vals = [500, 1_000, 2_500, 5_000, 10_000]
        m_vals = [1, 3, 6]
        k_vals = [10, 20]
    else:
        n_vals = [500, 1_000, 2_500, 5_000, 10_000, 20_000]
        m_vals = [1, 2, 3, 4, 6]
        k_vals = [10, 20, 30]
    return list(product(n_vals, m_vals, k_vals))


def run_benchmark(quick: bool = False) -> pd.DataFrame:
    import duckdb
    print("Loading Amsterdam hourly data …")
    df_raw = duckdb.connect(":memory:").execute(
        f"SELECT {', '.join(FEATURE_COLS)} FROM read_parquet('{RAW_GLOB}', union_by_name=true)"
        " WHERE " + " AND ".join(f"{c} IS NOT NULL" for c in FEATURE_COLS)
    ).fetchdf()
    X_full = df_raw[FEATURE_COLS].values.astype(np.float64)
    print(f"  Data loaded: {X_full.shape[0]:,} rows × {X_full.shape[1]} columns")

    print("Measuring compute index (GEMM benchmark) …")
    p = measure_compute_index()
    print(f"  Compute index p = {p:.2f} GFLOPS/s  ({platform.processor() or platform.machine()})")

    grid = build_grid(quick)
    total = len(grid)
    rows = []
    for i, (n, m, k) in enumerate(grid, 1):
        print(f"  [{i:>3}/{total}]  n={n:>6,}  m={m}  k={k} … ", end="", flush=True)
        cell = run_cell(X_full, n, m, k)
        cell["p_gflops"] = p
        rows.append(cell)
        print(f"{cell['total_sec']:.2f}s  (mean trial {cell['mean_trial_sec']*1000:.1f}ms)")

    return pd.DataFrame(rows)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true", help="Smaller grid for fast testing")
    args = parser.parse_args()

    df = run_benchmark(quick=args.quick)

    out_csv = RESULTS_DIR / "benchmark_results.csv"
    df.to_csv(out_csv, index=False)
    print(f"\nSaved {len(df)} rows → {out_csv}")
    print(df.to_string())
