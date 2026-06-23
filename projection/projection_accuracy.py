#!/usr/bin/env python3
"""
Projection Accuracy Experiment
==============================
Answers: "How far is the projection tool's ESTIMATE from REALITY, and how does
that gap shrink as we give the tool a larger sample of the data?"

Method (per column, using the detector Optuna selects for that column):
  1. Measure the REAL per-fit training time t(n) across a grid of row counts n,
     on real weather data. Median over R warm repeats -> ground-truth curve.
     The column's AutoML search cost is k * t(n) (k = 25 trials); since k is a
     constant multiplier it cancels in the relative error, so we fit on t(n).
  2. For each data fraction f in {25%, 50%, 75%, 100%} of the full row count,
     fit the power law  t(n) = a * n^b  using ONLY the measured points with
     n <= f * n_full  (this is exactly what the projection tool sees when it is
     handed an f-sized sample).
  3. Extrapolate to the full scale: predict t_hat(n_full) = a * n_full^b.
  4. Compare against the measured ground truth t(n_full):
        error% = |t_hat - t_actual| / t_actual * 100
  As f -> 100% the prediction converges to reality (interpolation); at f = 25%
  it is a ~4x extrapolation. The error column shows the distance from reality.

Per-column timing is clean because each column uses ONE detector (its selected
one), unlike the global blended fit that mixes 5 detectors of different
complexity. This is why near-linear detectors (ECOD on surface pressure)
extrapolate far more accurately than super-linear LOF columns.

Usage:
  python projection/projection_accuracy.py
  python projection/projection_accuracy.py --city amsterdam --repeats 7
"""

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
from pyod.models.copod import COPOD
from pyod.models.ecod import ECOD
from pyod.models.hbos import HBOS
from pyod.models.iforest import IForest
from pyod.models.lof import LOF

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_GLOB = REPO_ROOT / "experiments" / "data" / "automl" / "{city}_all_all.parquet"
OUT_DIR = Path(__file__).parent / "accuracy_artifacts"
OUT_DIR.mkdir(exist_ok=True)

K_TRIALS = 25  # production trial budget (constant multiplier; cancels in error%)

# Detector factories at representative mid-range hyperparameters.
DETECTORS = {
    "IForest": lambda: IForest(contamination=0.05, n_estimators=100, random_state=42),
    "LOF":     lambda: LOF(contamination=0.05, n_neighbors=20),
    "HBOS":    lambda: HBOS(contamination=0.05, n_bins=30),
    "ECOD":    lambda: ECOD(contamination=0.05),
    "COPOD":   lambda: COPOD(contamination=0.05),
}

# Column -> detector that Optuna selected in the main experiment (Amsterdam).
# LOF dominates (80% of all 30 models); surface_pressure uses ECOD.
COLUMN_DETECTOR = {
    "temperature_2m":             "LOF",
    "apparent_temperature":       "LOF",
    "precipitation":              "LOF",
    "surface_pressure":           "ECOD",
    "soil_temperature_7_to_28cm": "LOF",
    "soil_moisture_7_to_28cm":    "LOF",
}

# Row grid. Chosen so each fraction has >=3 fit points below it.
#   25% of 20352 = 5088   -> points <= 5000 give 7 fit points
#   50% = 10176           -> +4 points
#   75% = 15264           -> +3 points
#   100% = full           -> +endpoints
N_GRID = [500, 1000, 1500, 2000, 3000, 4000, 5000,
          6000, 7500, 9000, 10000,
          12000, 14000, 15000,
          17000, 19000]
FRACTIONS = [0.25, 0.50, 0.75, 1.00]


def measure_fit_time(values: np.ndarray, detector_name: str, n: int,
                     repeats: int, rng: np.random.Generator) -> float:
    """Median wall-clock seconds for ONE detector fit on n sampled rows."""
    times = []
    # One warm-up fit (not recorded) to absorb first-call / allocation overhead.
    for r in range(repeats + 1):
        idx = rng.choice(len(values), size=min(n, len(values)), replace=False)
        X = values[idx].reshape(-1, 1)
        model = DETECTORS[detector_name]()
        t0 = time.perf_counter()
        model.fit(X)
        dt = time.perf_counter() - t0
        if r > 0:                      # skip warm-up
            times.append(dt)
    return float(np.median(times))


def fit_power_law(n_arr: np.ndarray, t_arr: np.ndarray) -> tuple[float, float, float]:
    """Pure power law. OLS in log space: log t = log a + b log n. Returns (a, b, R^2)."""
    ln_n, ln_t = np.log(n_arr), np.log(t_arr)
    A = np.column_stack([np.ones_like(ln_n), ln_n])
    coef, *_ = np.linalg.lstsq(A, ln_t, rcond=None)
    log_a, b = coef
    pred = A @ coef
    ss_res = np.sum((ln_t - pred) ** 2)
    ss_tot = np.sum((ln_t - ln_t.mean()) ** 2)
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 1.0
    return float(np.exp(log_a)), float(b), float(r2)


def fit_overhead(n_arr: np.ndarray, t_arr: np.ndarray) -> tuple[float, float, float, float]:
    """
    Overhead-augmented model:  t(n) = c + a * n^b.
    c = fixed per-fit setup cost (object creation, validation, allocation),
    independent of n; a*n^b = the part that scales with data size.
    Nonlinear least squares; falls back to pure power law if it fails.
    Returns (c, a, b, R^2).
    """
    p0 = [t_arr.min(), max(t_arr.max() / n_arr.max(), 1e-12), 1.0]
    try:
        popt, _ = curve_fit(
            lambda x, c, a, b: c + a * np.power(x, b),
            n_arr, t_arr, p0=p0, maxfev=30000,
            bounds=([0, 0, 0.1], [t_arr.max(), np.inf, 3.0]),
        )
        c, a, b = popt
        pred = c + a * np.power(n_arr, b)
        ss_res = np.sum((t_arr - pred) ** 2)
        ss_tot = np.sum((t_arr - t_arr.mean()) ** 2)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 1.0
        return float(c), float(a), float(b), float(r2)
    except Exception:
        a, b, r2 = fit_power_law(n_arr, t_arr)
        return 0.0, a, b, r2


def run_column(values: np.ndarray, detector: str, n_full: int,
               repeats: int, seed: int) -> dict:
    rng = np.random.default_rng(seed)
    grid = [n for n in N_GRID if n < n_full] + [n_full]

    # ---- 1. measure ground-truth timing curve ----
    measured = {}
    for n in grid:
        measured[n] = measure_fit_time(values, detector, n, repeats, rng)
    t_actual_full = measured[n_full]

    # ---- 2-4. fraction-based fit + extrapolate (both models) ----
    rows = []
    for f in FRACTIONS:
        cap = f * n_full
        fit_ns = np.array([n for n in grid if n <= cap])
        fit_ts = np.array([measured[n] for n in fit_ns])

        a, b, r2 = fit_power_law(fit_ns, fit_ts)
        pred_pure = a * n_full ** b
        err_pure = abs(pred_pure - t_actual_full) / t_actual_full * 100.0

        c2, a2, b2, r2o = fit_overhead(fit_ns, fit_ts)
        pred_oh = c2 + a2 * n_full ** b2
        err_oh = abs(pred_oh - t_actual_full) / t_actual_full * 100.0

        rows.append({
            "fraction_pct": int(f * 100),
            "rows_given": int(cap),
            "n_fit_points": int(len(fit_ns)),
            "max_n_seen": int(fit_ns.max()),
            # pure power law
            "beta_pure": b, "r2_pure": r2,
            "pred_pure_s": pred_pure * K_TRIALS, "err_pure_pct": err_pure,
            # overhead-augmented
            "c_overhead": c2, "beta_oh": b2, "r2_oh": r2o,
            "pred_oh_s": pred_oh * K_TRIALS, "err_oh_pct": err_oh,
            "actual_full_s": t_actual_full * K_TRIALS,
        })
    return {"detector": detector, "n_full": n_full,
            "t_actual_full_perfit_s": t_actual_full,
            "measured_curve": measured, "fractions": rows}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default="amsterdam")
    ap.add_argument("--repeats", type=int, default=7)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    import duckdb
    path = str(DATA_GLOB).format(city=args.city)
    df = duckdb.connect().execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
    print(f"Loaded {args.city}: {df.shape[0]} rows")

    all_rows = []
    curves = []
    for col, det in COLUMN_DETECTOR.items():
        vals = df[col].dropna().values.astype(np.float64)
        n_full = len(vals)
        print(f"\n=== {col}  (detector={det}, n_full={n_full}) ===")
        res = run_column(vals, det, n_full, args.repeats, args.seed)
        for r in res["fractions"]:
            r2 = dict(city=args.city, column=col, **r, detector=det)
            all_rows.append(r2)
            print(f"  f={r['fraction_pct']:>3}%  fit_pts={r['n_fit_points']:>2}  "
                  f"actual={r['actual_full_s']:.2f}s | "
                  f"pure: pred={r['pred_pure_s']:.2f}s err={r['err_pure_pct']:5.1f}% | "
                  f"+overhead: pred={r['pred_oh_s']:.2f}s err={r['err_oh_pct']:5.1f}%")
        for n, t in res["measured_curve"].items():
            curves.append(dict(city=args.city, column=col, detector=det,
                               n=n, perfit_s=t, search_s=t * K_TRIALS))

    res_df = pd.DataFrame(all_rows)
    cur_df = pd.DataFrame(curves)
    res_path = OUT_DIR / f"accuracy_{args.city}.csv"
    cur_path = OUT_DIR / f"curves_{args.city}.csv"
    res_df.to_csv(res_path, index=False)
    cur_df.to_csv(cur_path, index=False)
    print(f"\nSaved -> {res_path}\nSaved -> {cur_path}")

    # ---- summary: mean error by fraction (both models) ----
    print("\n" + "=" * 64)
    print("Mean |error%| across columns by data fraction given:")
    print(f"  {'fraction':<10}{'pure n^b':>12}{'+overhead':>14}")
    for f in [25, 50, 75, 100]:
        sub = res_df[res_df.fraction_pct == f]
        print(f"  {f:>3}% data  {sub.err_pure_pct.mean():>10.1f}%  {sub.err_oh_pct.mean():>12.1f}%")
    print("\nOverhead-model error at 25% (hardest extrapolation), by column:")
    sub = res_df[res_df.fraction_pct == 25].sort_values("err_oh_pct")
    for _, r in sub.iterrows():
        print(f"  {r['column']:<28} {r['detector']:<8} err={r['err_oh_pct']:5.1f}%")


if __name__ == "__main__":
    main()
