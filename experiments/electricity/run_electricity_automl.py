#!/usr/bin/env python3
"""AutoML anomaly-detection experiment for GB electricity demand.

This is the electricity-domain counterpart to ``experiments/automl`` (the weather
experiment). It follows the *exact same* methodology used in the thesis:

  * Per column (**univariate**) and across both columns (**multivariate**), an
    Optuna study searches PyOD detectors (IForest / LOF / HBOS / COPOD / ECOD)
    and their hyper-parameters, optimising **F2** (recall-weighted) on a
    held-out split.
  * The best model is refit unsupervised on all rows (labels are never used for
    training) and scored against the injected ground truth.

The heavy lifting — model construction, the Optuna search space and the metric
functions — is **reused directly** from the weather experiment package
(``experiments/automl``) so both domains stay perfectly aligned. The only
electricity-specific pieces here are the feature columns, the local Parquet data
source (no S3) and the single ``GB`` partition.

Outputs are written in the same artifact layout the dashboard already reads::

    experiments/electricity/artifacts/run_<ts>/
        summary_metrics.csv
        best_models.json
        predictions_GB_<scope>_<column>.csv
        trials_GB_<scope>_<column>.csv

Usage::

    python experiments/electricity/run_electricity_automl.py
    python experiments/electricity/run_electricity_automl.py --n-trials 40
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import optuna
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

# Reuse the weather experiment's modelling/search/metric code verbatim.
_THIS_DIR = Path(__file__).resolve().parent
_AUTOML_DIR = _THIS_DIR.parent / "automl"
sys.path.insert(0, str(_AUTOML_DIR))

from metrics import EvalResult, compute_metrics  # noqa: E402
from optuna_config import create_study, suggest_model_and_params  # noqa: E402
from pyod_configs import build_model  # noqa: E402

optuna.logging.set_verbosity(optuna.logging.WARNING)

PARTITION_ID = "GB"
FEATURE_COLUMNS = ["initialDemandOutturn", "initialTransmissionSystemDemandOutturn"]
_DEFAULT_DATA = _THIS_DIR / "data" / "elexon_demand.parquet"


# ── Contextual feature engineering (univariate scope) ────────────────────────
def _contextual_features(work: pd.DataFrame, col: str) -> np.ndarray:
    """Turn a single non-stationary series into context-aware features.

    Electricity demand has a strong daily/weekly cycle, so a large *relative*
    shift at (say) 3am still lands inside the normal day-time value range — a
    **contextual anomaly** that a global detector reading raw values cannot see.
    We therefore detrend the seasonality before detection:

      * ``robust_z`` – value minus the typical value for its hour-of-day,
        scaled by that slot's inter-quartile spread (how unusual *for this time
        of day*).
      * ``delta_1``  – change vs the previous half-hour (a point spike shows up
        as a large jump-then-reversal).
      * ``delta_48`` – change vs the same time yesterday (24h = 48 half-hours).

    All three are derived from the *same* source column, so this stays a
    univariate (single-signal) detector — only the representation adapts.
    """
    t = pd.to_datetime(work["time"])
    v = work[col].astype(float)
    hour_of_day = t.dt.hour.to_numpy()

    tod = pd.Series(hour_of_day, index=v.index)
    base = v.groupby(tod).transform("median")
    spread = v.groupby(tod).transform(lambda s: (s.quantile(0.75) - s.quantile(0.25)) or 1.0)
    robust_z = (v - base) / spread
    delta_1 = v.diff()
    delta_48 = v - v.shift(48)

    feats = np.column_stack([robust_z.to_numpy(), delta_1.to_numpy(), delta_48.to_numpy()])
    return np.nan_to_num(feats, nan=0.0)


# ── Data preparation (mirrors experiment_runner.prepare_xy) ──────────────────
def prepare_xy(df: pd.DataFrame, feature_cols: list[str], target_column: str):
    """Return (times, X, y, y_values, shift_pct, original_values).

    Univariate (target_column is a real feature): the label is 1 only when that
    specific column was injected, read from ``synthetic_anomaly_details_json``.
    Detection uses :func:`_contextual_features` (seasonality-adjusted) rather
    than the raw value, because demand is non-stationary.
    Multivariate (target_column == "ALL_FEATURES"): row-level ``y_true``, raw
    columns (the cross-feature correlation already provides the context).
    """
    extra = [c for c in ("y_true", "synthetic_shift_pct", "synthetic_anomaly_details_json") if c in df.columns]
    work = df[["time", *feature_cols, *extra]].dropna(subset=["time"]).copy()
    work = work.sort_values("time").reset_index(drop=True)  # lag features need time order

    n = len(work)
    times = (pd.to_datetime(work["time"]).astype("int64") // 10**6).to_numpy(dtype="int64")
    univariate = target_column in FEATURE_COLUMNS
    if univariate:
        X = _contextual_features(work, target_column)
    else:
        X = work[feature_cols].to_numpy(dtype=float)
    y_values = work[feature_cols[0]].to_numpy(dtype=float)

    y = np.zeros(n, dtype=int)
    shift_pct = np.zeros(n, dtype=float)
    original_values = np.full(n, np.nan)

    col_specific = "synthetic_anomaly_details_json" in work.columns and target_column != "ALL_FEATURES"
    if col_specific:
        for i, js in enumerate(work["synthetic_anomaly_details_json"]):
            if pd.isna(js):
                continue
            try:
                detail = json.loads(str(js)).get(target_column)
            except (ValueError, TypeError):
                continue
            if isinstance(detail, dict):
                y[i] = 1
                shift_pct[i] = abs(float(detail.get("shift_pct", 0.0)))
                if detail.get("actual") is not None:
                    original_values[i] = float(detail["actual"])
    else:
        if "y_true" in work.columns:
            y = work["y_true"].to_numpy(dtype=int)
        if "synthetic_shift_pct" in work.columns:
            shift_pct = np.abs(work["synthetic_shift_pct"].to_numpy(dtype=float))

    return times, X, y, y_values, shift_pct, original_values


def _split(X: np.ndarray, y: np.ndarray, times: np.ndarray, ratio: float = 0.7):
    labels, counts = np.unique(y, return_counts=True)
    if len(labels) > 1 and counts.min() >= 2 and len(X) >= 4:
        idx = np.arange(len(X))
        idx_tr, idx_va = train_test_split(idx, test_size=1 - ratio, random_state=42, stratify=y)
        return X[idx_tr], X[idx_va], y[idx_tr], y[idx_va]
    s = max(1, min(int(len(X) * ratio), len(X) - 1))
    return X[:s], X[s:], y[:s], y[s:]


def _preprocess() -> Pipeline:
    return Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())])


def _fit_predict(X_train: np.ndarray, X_infer: np.ndarray, model_name: str, params: dict[str, Any]) -> np.ndarray:
    pre = _preprocess()
    model = build_model(model_name, params)
    model.fit(pre.fit_transform(X_train))
    return model.predict(pre.transform(X_infer)).astype(int)


# ── Optimise one scope (mirrors experiment_runner.optimize_scope) ────────────
def optimize_scope(target_column: str, feature_cols: list[str], df: pd.DataFrame, n_trials: int, seed: int):
    times, X, y, y_values, shift_pct, original_values = prepare_xy(df, feature_cols, target_column)
    X_tr, X_va, y_tr, y_va = _split(X, y, times)

    study = create_study(seed=seed)

    def objective(trial: optuna.Trial) -> float:
        model_name, params = suggest_model_and_params(trial)
        y_pred = _fit_predict(X_tr, X_va, model_name, params)
        _, _, _, f2 = compute_metrics(y_va, y_pred)
        return f2

    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    best_model = study.best_trial.params["model"]
    best_params = {k: v for k, v in study.best_trial.params.items() if k != "model"}

    y_pred_all = _fit_predict(X, X, best_model, best_params)
    precision, recall, f1, f2 = compute_metrics(y, y_pred_all)

    # ── Benchmark breakdown (the task's two extra concerns) ──────────────────
    #   * missed   : synthetic anomalies the model FAILED to flag (false negatives)
    #   * extra    : rows flagged that were NOT synthetic (false positives) — we
    #                cannot confirm these are real anomalies, and a high rate
    #                means the model is just "marking everything".
    tp = int(np.sum((y == 1) & (y_pred_all == 1)))
    fn = int(np.sum((y == 1) & (y_pred_all == 0)))
    fp = int(np.sum((y == 0) & (y_pred_all == 1)))
    n_true = int(np.sum(y == 1))
    n_clean = int(np.sum(y == 0))
    bench = {
        "n_synthetic": n_true,
        "n_caught": tp,
        "n_missed": fn,
        "missed_rate": (fn / n_true) if n_true else 0.0,          # = 1 - recall
        "n_extra_flags": fp,                                      # non-synthetic flagged
        "extra_flag_rate": (fp / n_clean) if n_clean else 0.0,    # FP among clean rows
    }

    scope_label = "univariate" if target_column in FEATURE_COLUMNS else "multivariate"
    result = EvalResult(
        city=PARTITION_ID,
        scope=scope_label,
        model_name=best_model,
        target_column=target_column,
        precision=precision,
        recall=recall,
        f1=f1,
        f2=f2,
        n_rows=len(y),
        n_positive_true=n_true,
        n_positive_pred=int(np.sum(y_pred_all == 1)),
    )

    predictions = pd.DataFrame(
        {
            "time_ms": times,
            "city_id": PARTITION_ID,
            "target_column": target_column,
            "y_value": y_values,
            "original_value": original_values,
            "y_true": y,
            "y_pred": y_pred_all,
            "is_correct": (y == y_pred_all).astype(int),
            "shift_pct": shift_pct,
        }
    )
    model_info = {"model": best_model, "params": best_params, "best_objective_f2": float(study.best_value)}
    return result, study.trials_dataframe(), predictions, model_info, bench


# ── Orchestration ────────────────────────────────────────────────────────────
def run(df: pd.DataFrame, scope: str, n_trials: int, seed: int, output_dir: Path):
    requested = ["multivariate", "univariate"] if scope == "both" else [scope]
    summary_rows: list[dict[str, Any]] = []
    best_models: dict[str, Any] = {}

    for current_scope in requested:
        feature_sets = (
            [("ALL_FEATURES", FEATURE_COLUMNS)]
            if current_scope == "multivariate"
            else [(c, [c]) for c in FEATURE_COLUMNS]
        )
        for target_column, feature_cols in feature_sets:
            key = f"{PARTITION_ID}:{current_scope}:{target_column}"
            print(f"[INFO] Running {key} (n_trials={n_trials})")
            result, trials_df, predictions_df, model_info, bench = optimize_scope(
                target_column, feature_cols, df, n_trials, seed
            )
            summary_rows.append({**result.__dict__, **bench})
            best_models[key] = model_info

            trials_df.to_csv(output_dir / f"trials_{PARTITION_ID}_{current_scope}_{target_column}.csv", index=False)
            predictions_df.to_csv(
                output_dir / f"predictions_{PARTITION_ID}_{current_scope}_{target_column}.csv", index=False
            )
            pd.DataFrame(summary_rows).to_csv(output_dir / "summary_metrics.csv", index=False)
            with (output_dir / "best_models.json").open("w", encoding="utf-8") as f:
                json.dump(best_models, f, indent=2, ensure_ascii=True)

            print(
                f"       -> {result.model_name}: P={result.precision:.3f} "
                f"R={result.recall:.3f} F1={result.f1:.3f} F2={result.f2:.3f}"
            )
            print(
                f"          caught {bench['n_caught']}/{bench['n_synthetic']} synthetic "
                f"(missed {bench['n_missed']}, {bench['missed_rate']:.1%}) | "
                f"extra flags {bench['n_extra_flags']} "
                f"({bench['extra_flag_rate']:.2%} of clean rows)"
            )
    return summary_rows, best_models


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data", default=str(_DEFAULT_DATA), help="Input parquet (from fetch_electricity_data.py).")
    p.add_argument("--scope", choices=["both", "univariate", "multivariate"], default="both")
    p.add_argument("--n-trials", type=int, default=30)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    data_path = Path(args.data)
    if not data_path.exists():
        raise SystemExit(f"Data not found: {data_path}\nRun fetch_electricity_data.py first.")

    df = pd.read_parquet(data_path)
    print(f"[INFO] Loaded {len(df):,} rows from {data_path}")
    print(f"[INFO] Anomalies in data: {int(df.get('y_true', pd.Series()).sum())}")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = _THIS_DIR / "artifacts" / f"run_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    run(df, scope=args.scope, n_trials=args.n_trials, seed=args.seed, output_dir=output_dir)
    print(f"\n[DONE] Experiment artifacts written to: {output_dir}")


if __name__ == "__main__":
    main()
