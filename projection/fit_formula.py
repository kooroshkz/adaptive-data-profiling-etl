#!/usr/bin/env python3
"""
Formula Fitting for AutoML Scalability
========================================
Fits the model:
    T(n, m, k) = α · n^β · m^δ · k^γ

Hardware speed is not a formula input. It is constant across all benchmark
runs (same machine) and is therefore absorbed into the baseline constant α.
α is reported together with the hardware specs of the benchmark machine so
readers know under what conditions it was measured.

In log-space this is linear:
    log T = log α + β·log n + δ·log m + γ·log k

Uses ordinary least squares on log-transformed observations.
Also reports R², residuals, and 95% bootstrap confidence intervals.

Usage:
  python fit_formula.py              # reads benchmark_results.csv
  python fit_formula.py --plot       # also saves a fit_plot.png
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

RESULTS_DIR = Path(__file__).parent


def load_and_validate(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"n", "m", "k", "total_sec"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    df = df[df["total_sec"] > 0].copy()
    return df


def fit_log_linear(df: pd.DataFrame) -> dict:
    """
    Fit log T = log α + β log n + δ log m + γ log k  via OLS.

    Hardware speed (p) is intentionally excluded: all benchmark runs were
    conducted on the same machine, so p has no variation and cannot be
    fitted as a separate term. α absorbs the hardware baseline.
    """
    log_T = np.log(df["total_sec"].values)
    log_n = np.log(df["n"].values)
    log_m = np.log(df["m"].values)
    log_k = np.log(df["k"].values)

    # Design matrix: [intercept, log_n, log_m, log_k]
    X = np.column_stack([np.ones(len(df)), log_n, log_m, log_k])
    result = np.linalg.lstsq(X, log_T, rcond=None)
    coeffs = result[0]
    log_alpha, beta, delta, gamma = coeffs

    alpha = np.exp(log_alpha)

    # R² and residuals
    T_pred = X @ coeffs
    ss_res = np.sum((log_T - T_pred) ** 2)
    ss_tot = np.sum((log_T - log_T.mean()) ** 2)
    r2 = 1.0 - ss_res / ss_tot

    # 95% CI via bootstrap (1000 resamples)
    n_boot = 1000
    rng = np.random.default_rng(0)
    boot_beta, boot_delta, boot_gamma = [], [], []
    for _ in range(n_boot):
        idx = rng.integers(0, len(df), len(df))
        Xb, yb = X[idx], log_T[idx]
        try:
            cb = np.linalg.lstsq(Xb, yb, rcond=None)[0]
            boot_beta.append(cb[1])
            boot_delta.append(cb[2])
            boot_gamma.append(cb[3])
        except Exception:
            pass

    def ci95(samples):
        return (float(np.percentile(samples, 2.5)), float(np.percentile(samples, 97.5)))

    return {
        "alpha": float(alpha),
        "beta":  float(beta),
        "delta": float(delta),
        "gamma": float(gamma),
        "r2":    float(r2),
        "beta_ci95":  ci95(boot_beta),
        "delta_ci95": ci95(boot_delta),
        "gamma_ci95": ci95(boot_gamma),
        "n_obs": len(df),
    }


def print_report(fit: dict, df: pd.DataFrame) -> None:
    print("\n" + "═" * 62)
    print("  AutoML Scalability Formula")
    print("═" * 62)
    print()
    print("  T(n, m, k)  ≈  α · n^β · m^δ · k^γ")
    print()
    print(f"  α  (baseline, machine-specific) = {fit['alpha']:.4e} s")
    print(f"  β  (row exponent)  = {fit['beta']:.4f}   95% CI [{fit['beta_ci95'][0]:.3f}, {fit['beta_ci95'][1]:.3f}]")
    print(f"  δ  (col exponent)  = {fit['delta']:.4f}   95% CI [{fit['delta_ci95'][0]:.3f}, {fit['delta_ci95'][1]:.3f}]")
    print(f"  γ  (trial exp.)    = {fit['gamma']:.4f}   95% CI [{fit['gamma_ci95'][0]:.3f}, {fit['gamma_ci95'][1]:.3f}]")
    print()
    print(f"  R²  = {fit['r2']:.4f}   (over {fit['n_obs']} observations)")
    print()
    print("─" * 62)
    print("  Note: α is tied to the benchmark hardware.")
    print("  β, δ, γ are hardware-independent exponents.")
    print("─" * 62)
    print("  Interpretation:")
    beta = fit["beta"]
    if beta < 1.0:
        verdict = f"sub-linear (β={beta:.3f} < 1) — scales well ✓"
    elif beta < 1.2:
        verdict = f"near-linear (β={beta:.3f}) — moderate cost growth"
    else:
        verdict = f"super-linear (β={beta:.3f} > 1) — retraining budget required"
    print(f"  β = {beta:.3f} → row scaling is {verdict}")
    print()
    # Practical projections
    n0 = 20_000
    c, m0, k0 = 5, 6, 25
    scales = [1, 2, 5, 10, 50, 100]
    print("  Projected full-pipeline train time (c=5 cities, m=6, k=25):")
    for s in scales:
        n = n0 * s
        T = c * fit["alpha"] * (n ** fit["beta"]) * (m0 ** fit["delta"]) * (k0 ** fit["gamma"])
        tag = " ← current scale" if s == 1 else ""
        print(f"    {s:>4}× data  (n={n/1000:.0f}k)  → {T:.1f}s  ({T/60:.1f} min){tag}")
    print("─" * 62)


def make_plot(fit: dict, df: pd.DataFrame, out_path: Path) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed — skipping plot")
        return

    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    fig.suptitle(
        f"AutoML Scalability  |  T ≈ α·n^β·m^δ·k^γ   "
        f"β={fit['beta']:.3f}  δ={fit['delta']:.3f}  R²={fit['r2']:.3f}",
        fontsize=12, fontweight="bold"
    )

    palette = plt.cm.viridis(np.linspace(0.2, 0.85, 6))
    k_fixed = int(df["k"].median())
    sub = df[df["k"] == k_fixed]

    # Panel A: T vs n
    ax = axes[0]
    for j, m_val in enumerate(sorted(sub["m"].unique())):
        g = sub[sub["m"] == m_val].sort_values("n")
        ax.scatter(g["n"], g["total_sec"], color=palette[j], s=30, zorder=3)
        n_line = np.linspace(g["n"].min(), g["n"].max(), 100)
        T_line = fit["alpha"] * (n_line ** fit["beta"]) * (m_val ** fit["delta"]) * (k_fixed ** fit["gamma"])
        ax.plot(n_line, T_line, color=palette[j], lw=1.5, label=f"m={m_val}")
    ax.set_xlabel("n (rows)")
    ax.set_ylabel("T (seconds)")
    ax.set_title(f"Row Scaling  β={fit['beta']:.3f}")
    ax.legend(fontsize=8)

    # Panel B: T vs m
    ax = axes[1]
    for j, n_val in enumerate(sorted(sub["n"].unique())):
        g = sub[sub["n"] == n_val].sort_values("m")
        ax.scatter(g["m"], g["total_sec"], color=palette[j], s=30, zorder=3)
        m_line = np.linspace(g["m"].min(), g["m"].max(), 50)
        T_line = fit["alpha"] * (n_val ** fit["beta"]) * (m_line ** fit["delta"]) * (k_fixed ** fit["gamma"])
        ax.plot(m_line, T_line, color=palette[j], lw=1.5, label=f"n={n_val//1000}k")
    ax.set_xlabel("m (columns)")
    ax.set_ylabel("T (seconds)")
    ax.set_title(f"Column Scaling  δ={fit['delta']:.3f}")
    ax.legend(fontsize=8)

    # Panel C: Actual vs Predicted
    ax = axes[2]
    log_n = np.log(df["n"].values)
    log_m = np.log(df["m"].values)
    log_k = np.log(df["k"].values)
    T_pred = np.exp(
        np.log(fit["alpha"]) + fit["beta"] * log_n
        + fit["delta"] * log_m + fit["gamma"] * log_k
    )
    ax.scatter(df["total_sec"], T_pred, alpha=0.6, s=20, color="#6366f1")
    lim = [min(df["total_sec"].min(), T_pred.min()) * 0.9,
           max(df["total_sec"].max(), T_pred.max()) * 1.1]
    ax.plot(lim, lim, "k--", lw=1, alpha=0.5, label="perfect fit")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Actual T (s)")
    ax.set_ylabel("Predicted T (s)")
    ax.set_title(f"Fit Quality  R²={fit['r2']:.4f}")
    ax.legend(fontsize=8)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Plot saved → {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--plot", action="store_true")
    parser.add_argument("--csv", default=str(RESULTS_DIR / "benchmark_results.csv"))
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found. Run benchmark.py first.")
        raise SystemExit(1)

    df = load_and_validate(csv_path)
    fit = fit_log_linear(df)
    print_report(fit, df)

    out_json = RESULTS_DIR / "formula_params.json"
    with open(out_json, "w") as f:
        json.dump(fit, f, indent=2)
    print(f"\nParameters saved → {out_json}")

    if args.plot:
        make_plot(fit, df, RESULTS_DIR / "fit_plot.png")
