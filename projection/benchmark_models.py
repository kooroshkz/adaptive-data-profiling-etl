#!/usr/bin/env python3
"""
Per-model complexity breakdown.
Times each PyOD detector individually across an n × m grid
to derive model-specific exponents α_i, β_i, δ_i.
Also sweeps n_jobs (parallelism) for models that support it
to estimate the Amdahl efficiency exponent φ.
"""

import json
import platform
import time
from pathlib import Path

import numpy as np
import pandas as pd
from pyod.models.copod import COPOD
from pyod.models.ecod import ECOD
from pyod.models.hbos import HBOS
from pyod.models.iforest import IForest
from pyod.models.lof import LOF
from scipy.stats import linregress

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_GLOB  = str(REPO_ROOT / "airflow" / "data" / "raw" / "city=amsterdam" / "hourly_*.parquet")
RESULTS_DIR = Path(__file__).parent

FEATURE_COLS = [
    "temperature_2m", "apparent_temperature", "precipitation",
    "surface_pressure", "soil_temperature_7_to_28cm", "soil_moisture_7_to_28cm",
]

# ── helpers ────────────────────────────────────────────────────────────────────

def gemm_gflops(size: int = 1500, reps: int = 5) -> float:
    A = np.random.randn(size, size)
    B = np.random.randn(size, size)
    flops = 2.0 * size ** 3
    ts = [min((lambda: (t := time.perf_counter(), A @ B, time.perf_counter() - t)[2])() for _ in range(1)) for _ in range(reps)]
    times = []
    for _ in range(reps):
        t0 = time.perf_counter(); _ = A @ B; times.append(time.perf_counter() - t0)
    return (flops / float(np.median(times))) / 1e9


def mem_bandwidth_gbs(size: int = 50_000_000, reps: int = 5) -> float:
    """Read bandwidth: copy a large float64 array."""
    arr = np.random.randn(size)
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        _ = arr.copy()
        times.append(time.perf_counter() - t0)
    bytes_moved = arr.nbytes * 2  # read + write
    return (bytes_moved / float(np.median(times))) / 1e9


def time_fit(model, X: np.ndarray, reps: int = 3) -> float:
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        model.fit(X)
        times.append(time.perf_counter() - t0)
    return float(np.median(times))


# ── experiment 1: per-model n × m grid ─────────────────────────────────────────

def run_per_model(X_full: np.ndarray) -> pd.DataFrame:
    N_VALS = [500, 1_000, 2_500, 5_000, 10_000, 20_000]
    M_VALS = [1, 2, 3, 4, 6]
    CONT   = 0.01

    models = {
        "IForest": lambda m: IForest(contamination=CONT, n_estimators=100, random_state=42),
        "LOF":     lambda m: LOF(contamination=CONT, n_neighbors=20),
        "HBOS":    lambda m: HBOS(contamination=CONT, n_bins=20),
        "COPOD":   lambda m: COPOD(contamination=CONT),
        "ECOD":    lambda m: ECOD(contamination=CONT),
    }

    rng  = np.random.default_rng(42)
    rows = []
    total = len(models) * len(N_VALS) * len(M_VALS)
    i = 0
    for mname, factory in models.items():
        for n in N_VALS:
            for m in M_VALS:
                i += 1
                idx = rng.choice(len(X_full), size=min(n, len(X_full)), replace=False)
                col_idx = rng.choice(X_full.shape[1], size=m, replace=False)
                X = X_full[np.ix_(idx, col_idx)]
                t = time_fit(factory(m), X)
                print(f"  [{i:>3}/{total}]  {mname:<8}  n={n:>6,}  m={m}  → {t*1000:.1f}ms")
                rows.append({"model": mname, "n": n, "m": m, "sec": t})
    return pd.DataFrame(rows)


# ── experiment 2: parallelism sweep (IForest + LOF, n=20k, m=6) ───────────────

def run_parallelism(X_full: np.ndarray) -> pd.DataFrame:
    import os
    n_cores = os.cpu_count() or 8
    jobs_list = sorted({1, 2, 4, min(8, n_cores), min(16, n_cores)})
    n, m = 20_000, 6
    rng = np.random.default_rng(0)
    idx = rng.choice(len(X_full), size=n, replace=False)
    col_idx = rng.choice(X_full.shape[1], size=m, replace=False)
    X = X_full[np.ix_(idx, col_idx)]

    rows = []
    for jobs in jobs_list:
        for mname, factory in [
            ("IForest", lambda j: IForest(contamination=0.01, n_estimators=100, n_jobs=j, random_state=42)),
            ("LOF",     lambda j: LOF(contamination=0.01, n_neighbors=20, n_jobs=j)),
        ]:
            t = time_fit(factory(jobs), X)
            print(f"  {mname:<8}  n_jobs={jobs}  → {t*1000:.1f}ms")
            rows.append({"model": mname, "n_jobs": jobs, "sec": t})
    return pd.DataFrame(rows)


# ── fit per-model exponents ────────────────────────────────────────────────────

def fit_model_exponents(df: pd.DataFrame) -> dict:
    results = {}
    for mname, g in df.groupby("model"):
        # β: log T vs log n  (fixed m=6)
        sub_n = g[g["m"] == 6].sort_values("n")
        if len(sub_n) >= 3:
            slope_n, intercept_n, r_n, *_ = linregress(np.log(sub_n["n"]), np.log(sub_n["sec"]))
        else:
            slope_n, intercept_n, r_n = 1.0, 0.0, 0.0

        # δ: log T vs log m  (fixed n=10000 or max available)
        n_ref = 10_000 if 10_000 in g["n"].values else g["n"].max()
        sub_m = g[g["n"] == n_ref].sort_values("m")
        if len(sub_m) >= 3:
            slope_m, intercept_m, r_m, *_ = linregress(np.log(sub_m["m"]), np.log(sub_m["sec"]))
        else:
            slope_m, intercept_m, r_m = 0.0, 0.0, 0.0

        alpha_i = float(np.exp(intercept_n))
        results[mname] = {
            "alpha": alpha_i,
            "beta":  float(slope_n),
            "delta": float(slope_m),
            "r2_beta": float(r_n ** 2),
            "r2_delta": float(r_m ** 2),
        }
    return results


def fit_parallelism(df: pd.DataFrame) -> dict:
    results = {}
    for mname, g in df.groupby("model"):
        g = g.sort_values("n_jobs")
        # T ∝ 1/C^φ  →  log T = const - φ·log C
        slope, intercept, r, *_ = linregress(np.log(g["n_jobs"]), np.log(g["sec"]))
        # slope should be negative (more cores = less time)
        results[mname] = {
            "phi": float(-slope),   # positive: speedup exponent
            "r2": float(r ** 2),
        }
    return results


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import duckdb
    print("Loading data …")
    df_raw = duckdb.connect(":memory:").execute(
        f"SELECT {', '.join(FEATURE_COLS)} FROM read_parquet('{RAW_GLOB}', union_by_name=true)"
        " WHERE " + " AND ".join(f"{c} IS NOT NULL" for c in FEATURE_COLS)
    ).fetchdf()
    X_full = df_raw[FEATURE_COLS].values.astype(np.float64)
    print(f"  {X_full.shape[0]:,} rows × {X_full.shape[1]} cols")

    print("\n── Compute benchmarks ────────────────────────────────")
    p  = gemm_gflops()
    B  = mem_bandwidth_gbs()
    import os
    C  = os.cpu_count() or 8
    print(f"  p  = {p:.1f} GFLOPS/s  (GEMM)")
    print(f"  B  = {B:.1f} GB/s  (memory bandwidth)")
    print(f"  C  = {C} logical cores  ({platform.processor() or platform.machine()})")

    print("\n── Per-model n×m grid ────────────────────────────────")
    df_models = run_per_model(X_full)
    df_models.to_csv(RESULTS_DIR / "benchmark_models.csv", index=False)

    print("\n── Parallelism sweep ─────────────────────────────────")
    df_par = run_parallelism(X_full)
    df_par.to_csv(RESULTS_DIR / "benchmark_parallelism.csv", index=False)

    print("\n── Fitting exponents ─────────────────────────────────")
    model_exp = fit_model_exponents(df_models)
    par_exp   = fit_parallelism(df_par)

    print("\nPer-model exponents:")
    for m, v in model_exp.items():
        print(f"  {m:<8}  α={v['alpha']:.3e}  β={v['beta']:.3f} (R²={v['r2_beta']:.3f})  δ={v['delta']:.3f} (R²={v['r2_delta']:.3f})")

    print("\nParallelism exponents:")
    for m, v in par_exp.items():
        print(f"  {m:<8}  φ={v['phi']:.3f}  R²={v['r2']:.3f}")

    out = {
        "compute": {"p_gflops": p, "B_gbs": B, "C_cores": C, "machine": platform.machine(), "processor": platform.processor()},
        "model_exponents": model_exp,
        "parallelism": par_exp,
    }
    out_path = RESULTS_DIR / "model_params.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved → {out_path}")
