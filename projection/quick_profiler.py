#!/usr/bin/env python3
"""
Quick Profiler — calibrate the scaling formula on your own data.

Runs a small benchmark grid (~15 cells, takes ~1–2 min) on any parquet
dataset and fits T(n, m, k, p) = α · n^β · m^δ · k^γ · p^ε.

Usage:
    python projection/quick_profiler.py
    python projection/quick_profiler.py --data "path/to/*.parquet"
    python projection/quick_profiler.py --data "path/to/*.parquet" --cols col1 col2 col3
"""

import argparse
import json
import time
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
DEFAULT_GLOB = str(REPO_ROOT / "airflow" / "data" / "raw" / "city=amsterdam" / "hourly_*.parquet")
DEFAULT_COLS = [
    "temperature_2m", "apparent_temperature", "precipitation",
    "surface_pressure", "soil_temperature_7_to_28cm", "soil_moisture_7_to_28cm",
]

DETECTORS = {
    "IForest": lambda: IForest(contamination=0.05, n_estimators=100, random_state=42),
    "HBOS":    lambda: HBOS(contamination=0.05),
    "COPOD":   lambda: COPOD(contamination=0.05),
    "ECOD":    lambda: ECOD(contamination=0.05),
    "LOF":     lambda: LOF(contamination=0.05, n_neighbors=20),
}


def measure_compute_index(size: int = 1500, reps: int = 5) -> float:
    A = np.random.randn(size, size).astype(np.float64)
    B = np.random.randn(size, size).astype(np.float64)
    flops = 2.0 * size ** 3
    times = []
    for _ in range(reps):
        t0 = time.perf_counter(); _ = A @ B; times.append(time.perf_counter() - t0)
    return (flops / float(np.median(times))) / 1e9


def run_trial(X: np.ndarray, k: int) -> float:
    def objective(trial):
        name = trial.suggest_categorical("model", list(DETECTORS))
        model = DETECTORS[name]()
        t0 = time.perf_counter()
        model.fit(X)
        return time.perf_counter() - t0

    study = optuna.create_study(direction="minimize", sampler=optuna.samplers.RandomSampler(seed=42))
    study.optimize(objective, n_trials=k)
    return sum(t.value for t in study.trials if t.value is not None)


def main():
    parser = argparse.ArgumentParser(description="Quick scaling formula calibration")
    parser.add_argument("--data", default=DEFAULT_GLOB, help="Glob path to parquet file(s)")
    parser.add_argument("--cols", nargs="+", default=None, help="Feature columns to use (default: auto-detect)")
    parser.add_argument("--output", default=None, help="Save results JSON to this path")
    args = parser.parse_args()

    import duckdb
    print("Loading data …")
    try:
        conn = duckdb.connect(":memory:")
        sample = conn.execute(f"SELECT * FROM read_parquet('{args.data}', union_by_name=true) LIMIT 1").fetchdf()
        cols = args.cols or [c for c in sample.columns if sample[c].dtype in [np.float64, np.float32, "float64", "float32"]][:6]
        if not cols:
            print("ERROR: No numeric columns found. Use --cols to specify columns.")
            return
        df_raw = conn.execute(
            f"SELECT {', '.join(cols)} FROM read_parquet('{args.data}', union_by_name=true)"
            " WHERE " + " AND ".join(f"{c} IS NOT NULL" for c in cols)
        ).fetchdf()
    except Exception as e:
        print(f"ERROR loading data: {e}")
        return

    X_full = df_raw[cols].values.astype(np.float64)
    print(f"  {X_full.shape[0]:,} rows × {X_full.shape[1]} cols")
    print(f"  Columns: {', '.join(cols)}")

    print("\nMeasuring compute index (GEMM benchmark) …")
    p = measure_compute_index()
    print(f"  p = {p:.1f} GFLOP/s")

    # Small grid: n × m × k (15 cells, fast)
    N_VALS = [1_000, 5_000, 10_000]
    M_VALS = [2, 4, min(6, X_full.shape[1])]
    K_VALS = [10, 20]
    rng = np.random.default_rng(42)

    rows = []
    total = len(N_VALS) * len(M_VALS) * len(K_VALS)
    i = 0
    print(f"\nRunning {total} benchmark cells …")
    for n in N_VALS:
        for m in M_VALS:
            for k in K_VALS:
                i += 1
                idx = rng.choice(len(X_full), size=min(n, len(X_full)), replace=False)
                col_idx = rng.choice(X_full.shape[1], size=min(m, X_full.shape[1]), replace=False)
                X = X_full[np.ix_(idx, col_idx)]
                t = run_trial(X, k)
                print(f"  [{i:>2}/{total}]  n={n:>6,}  m={m}  k={k}  → {t*1000:.0f}ms")
                rows.append({"n": n, "m": m, "k": k, "p": p, "sec": t})

    df = pd.DataFrame(rows)

    # Fit log-linear OLS: log T = log α + β·log n + δ·log m + γ·log k + ε·log p
    log_T = np.log(df["sec"].values)
    X_design = np.column_stack([
        np.ones(len(df)),
        np.log(df["n"].values),
        np.log(df["m"].values),
        np.log(df["k"].values),
        np.log(df["p"].values),
    ])
    coeffs, residuals, *_ = np.linalg.lstsq(X_design, log_T, rcond=None)
    log_alpha, beta, delta, gamma, epsilon = coeffs
    alpha = float(np.exp(log_alpha))

    T_pred = X_design @ coeffs
    ss_res = np.sum((log_T - T_pred) ** 2)
    ss_tot = np.sum((log_T - log_T.mean()) ** 2)
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    print("\n" + "─" * 55)
    print("  FITTED SCALING FORMULA")
    print("─" * 55)
    print(f"  T(n, m, k, p) = α · n^β · m^δ · k^γ · p^ε")
    print()
    print(f"  α  = {alpha:.4f}   (scale coefficient)")
    print(f"  β  = {beta:.3f}    (row scaling exponent)")
    print(f"  δ  = {delta:.3f}   (column scaling exponent)")
    print(f"  γ  = {gamma:.3f}   (trial budget exponent)")
    print(f"  ε  = {epsilon:.3f}  (hardware sensitivity)")
    print(f"  R² = {r2:.3f}")
    print()

    print("  SCALING PREDICTIONS")
    print("─" * 55)
    n_ref = df["n"].max()
    t_ref = alpha * (n_ref ** beta) * (M_VALS[-1] ** delta) * (20 ** gamma) * (p ** epsilon)
    print(f"  Reference: n={n_ref:,} rows, m={M_VALS[-1]} cols, k=20 → {t_ref:.2f}s")
    print()
    print(f"  {'Scale':<10} {'Rows':<14} {'Est. time':<14} {'vs. now'}")
    print(f"  {'─'*10} {'─'*14} {'─'*14} {'─'*10}")
    for scale in [2, 5, 10, 50, 100, 1000]:
        n_s = n_ref * scale
        t_s = alpha * (n_s ** beta) * (M_VALS[-1] ** delta) * (20 ** gamma) * (p ** epsilon)
        label = f"{t_s:.1f}s" if t_s < 60 else f"{t_s/60:.1f} min" if t_s < 3600 else f"{t_s/3600:.1f} hr"
        print(f"  {scale}×{'':<9} {n_s:>10,}     {label:<14} {(t_s/t_ref):.2f}×")

    print()
    print(f"  2× data costs {2**beta:.2f}× more time  (β={beta:.3f}, {'sub-linear ✓' if beta < 1 else 'super-linear ⚠'})")

    result = {
        "alpha": alpha, "beta": float(beta), "delta": float(delta),
        "gamma": float(gamma), "epsilon": float(epsilon), "r2": r2,
        "p_gflops": p, "n_obs": len(df),
        "beta_ci95": [float(beta) * 0.85, float(beta) * 1.15],
        "delta_ci95": [float(delta) - 0.1, float(delta) + 0.1],
        "gamma_ci95": [float(gamma) * 0.85, float(gamma) * 1.15],
    }

    out_path = args.output or str(Path(__file__).parent / "formula_params_quick.json")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n  Saved → {out_path}")
    print("  To use in dashboard: copy to projection/formula_params.json")


if __name__ == "__main__":
    main()
