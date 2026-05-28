#!/usr/bin/env python3
"""
Reproduce the best model from a prior AutoML run and enrich predictions
with continuous confidence scores and human-readable labels.

Reproducibility strategy
------------------------
- Model params are loaded from best_models.json of the source run — no
  Optuna re-search, so model selection is identical.
- Data is loaded from the cached parquets in experiments/data/automl/
  (same files used during the original run).
- LOF, ECOD, HBOS, COPOD are fully deterministic given the same data.
  IForest uses random_state=42 (already set in build_model), so it is
  also reproducible.
- Preprocessing (median imputer + StandardScaler) is fit on all data,
  matching the `fit_predict_with_pipeline(X, X, ...)` call in the
  original experiment_runner.

Extra columns added to each prediction row
-------------------------------------------
anomaly_score     Raw output of model.decision_function(X). Units are
                  algorithm-specific (LOF: local reachability ratio,
                  ECOD/COPOD: -log tail probability, HBOS: histogram
                  density score, IForest: mean path length). Higher
                  always means more anomalous.

anomaly_prob      model.predict_proba(X)[:, 1] — min-max normalised to
                  [0, 1] relative to the training distribution.
                  0 = least anomalous point seen during training,
                  1 = most anomalous point seen during training.

score_percentile  Percentile rank of each point's anomaly_score within
                  the full dataset (0–100). Model- and scale-independent.

threshold_score   The decision_function value at the contamination
                  threshold — points above this line get y_pred=1.

is_synthetic      True when the row carries an injected anomaly label
                  (y_true == 1).

label             Human-readable outcome:
                    TP  true positive  (synthetic detected)
                    FP  false positive (normal flagged — false alarm)
                    FN  false negative (synthetic missed)
                    TN  true negative  (normal, correctly ignored)

model_name        Algorithm selected by Optuna in the source run.
contamination     Contamination hyperparameter used.
best_obj_f2       Best F2 score achieved during Optuna search (from
                  source run's best_models.json).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import percentileofscore

from utils.paths import AUTOML_DIR, DATA_DIR
from utils.preprocessing import build_preprocessing_pipeline

sys.path.insert(0, str(AUTOML_DIR))
from pyod_configs import FEATURE_COLUMNS, build_model
from experiment_runner import prepare_xy, split_train_valid
from metrics import compute_metrics


DEFAULT_SOURCE_RUN = AUTOML_DIR / "artifacts" / "run_20260428_093705"


def load_city_parquet(city: str) -> pd.DataFrame:
    """Load the cached all-dates parquet for a city."""
    path = DATA_DIR / f"{city}_all_all.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Cached parquet not found: {path}\n"
            "Run the original experiment first to generate the cache."
        )
    return pd.read_parquet(path)


def compute_threshold_score(model, preprocess: Pipeline, X_all: np.ndarray, contamination: float) -> float:
    """
    Return the decision_function value that separates the top-contamination
    fraction of training scores — i.e., the exact threshold the model uses
    internally to assign y_pred=1.
    """
    X_proc = preprocess.transform(X_all)
    scores = model.decision_function(X_proc)
    # PyOD sets threshold at the (1-contamination) quantile of training scores
    return float(np.percentile(scores, 100 * (1 - contamination)))


def enrich_predictions(
    city: str,
    target_column: str,
    feature_cols: list[str],
    df_city: pd.DataFrame,
    model_info: dict[str, Any],
) -> pd.DataFrame:
    """
    Refit the saved best model on all data and return an enriched predictions
    DataFrame with confidence scores and human-readable labels.
    """
    model_name: str = model_info["model"]
    params: dict[str, Any] = dict(model_info["params"])
    contamination: float = float(params["contamination"])
    best_obj_f2: float = float(model_info.get("best_objective_f2", float("nan")))

    times, X, y, y_values, shift_pct, original_values = prepare_xy(
        df_city, feature_cols, target_column=target_column
    )
    if len(X) < 20:
        print(f"  [WARN] Not enough rows for {city}:{target_column}, skipping.")
        return pd.DataFrame()

    # ── Preprocess + fit on ALL data (mirrors original experiment_runner) ───
    preprocess = build_preprocessing_pipeline()
    X_proc = preprocess.fit_transform(X)

    model = build_model(model_name, params)
    model.fit(X_proc)

    # ── Binary prediction ────────────────────────────────────────────────────
    y_pred = model.predict(X_proc).astype(int)

    # ── Continuous scores ────────────────────────────────────────────────────
    anomaly_score = model.decision_function(X_proc)
    # predict_proba: shape (n, 2); column 1 is P(anomaly)
    anomaly_prob = model.predict_proba(X_proc)[:, 1]
    score_percentile = np.array([
        percentileofscore(anomaly_score, s, kind="rank") for s in anomaly_score
    ])

    # Threshold score = the decision_function value at the contamination cut
    # (points scoring >= this get y_pred=1)
    threshold_score = float(np.percentile(anomaly_score, 100 * (1 - contamination)))

    # ── Human-readable label ─────────────────────────────────────────────────
    def _label(yt: int, yp: int) -> str:
        if yt == 1 and yp == 1:
            return "TP"
        if yt == 0 and yp == 1:
            return "FP"
        if yt == 1 and yp == 0:
            return "FN"
        return "TN"

    labels = [_label(int(yt), int(yp)) for yt, yp in zip(y, y_pred)]

    # ── Metrics ──────────────────────────────────────────────────────────────
    precision, recall, f1, f2 = compute_metrics(y, y_pred)

    # ── Build output DataFrame ───────────────────────────────────────────────
    times_dt = pd.to_datetime(times, unit="ms", utc=True).tz_convert(None)

    df_out = pd.DataFrame({
        "time":             times_dt,
        "city_id":          city,
        "target_column":    target_column,
        "model_name":       model_name,
        # --- value info ---
        "y_value":          y_values,
        "original_value":   original_values,
        "is_synthetic":     (y == 1),
        "shift_pct":        shift_pct,
        # --- prediction ---
        "y_true":           y,
        "y_pred":           y_pred,
        "label":            labels,
        # --- confidence ---
        "anomaly_score":    np.round(anomaly_score, 6),
        "anomaly_prob":     np.round(anomaly_prob, 6),
        "score_percentile": np.round(score_percentile, 2),
        "threshold_score":  round(threshold_score, 6),
        # --- model metadata ---
        "contamination":    contamination,
        "best_obj_f2":      round(best_obj_f2, 6),
    })

    # Add model-specific hyperparams as extra columns
    for k, v in params.items():
        if k != "contamination":
            df_out[f"param_{k}"] = v

    return df_out


def print_summary(df: pd.DataFrame, city: str, col: str, model_name: str) -> None:
    counts = df["label"].value_counts()
    tp = counts.get("TP", 0)
    fp = counts.get("FP", 0)
    fn = counts.get("FN", 0)
    tn = counts.get("TN", 0)

    fp_rows = df[df["label"] == "FP"]
    fn_rows = df[df["label"] == "FN"]
    tp_rows = df[df["label"] == "TP"]

    print(
        f"  {model_name:<6}  "
        f"TP={tp:4d}  FP={fp:4d}  FN={fn:4d}  TN={tn:5d}  |  "
        f"FP prob mean={fp_rows['anomaly_prob'].mean():.3f}  "
        f"FN prob mean={fn_rows['anomaly_prob'].mean():.3f}  "
        f"TP prob mean={tp_rows['anomaly_prob'].mean():.3f}"
        if not fp_rows.empty or not fn_rows.empty or not tp_rows.empty
        else f"  {model_name}  TP={tp} FP={fp} FN={fn} TN={tn}"
    )


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refit saved best models and produce enriched predictions with confidence scores."
    )
    parser.add_argument(
        "--source-run",
        type=Path,
        default=DEFAULT_SOURCE_RUN,
        help="Path to the source artifact run directory containing best_models.json",
    )
    parser.add_argument(
        "--scope",
        choices=["univariate", "multivariate", "both"],
        default="univariate",
        help="Which scopes to reproduce (default: univariate)",
    )
    parser.add_argument(
        "--cities",
        nargs="+",
        default=["amsterdam", "london", "new_york", "paris", "tokyo"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source_run: Path = args.source_run
    best_models_path = source_run / "best_models.json"
    if not best_models_path.exists():
        sys.exit(f"[ERROR] best_models.json not found in {source_run}")

    with best_models_path.open() as f:
        best_models: dict[str, Any] = json.load(f)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_dir = AUTOML_DIR / "artifacts" / f"scored_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Source run : {source_run}")
    print(f"Output dir : {output_dir}")
    print(f"Scopes     : {args.scope}")
    print()

    requested_scopes = (
        ["univariate", "multivariate"] if args.scope == "both"
        else [args.scope]
    )

    all_dfs: list[pd.DataFrame] = []

    for city in args.cities:
        try:
            df_city = load_city_parquet(city)
        except FileNotFoundError as exc:
            print(f"[WARN] {exc}")
            continue

        print(f"[{city}]  rows={len(df_city)}")

        for scope in requested_scopes:
            if scope == "multivariate":
                feature_sets = [("ALL_FEATURES", FEATURE_COLUMNS)]
            else:
                feature_sets = [(col, [col]) for col in FEATURE_COLUMNS]

            for target_column, feature_cols in feature_sets:
                key = f"{city}:{scope}:{target_column}"
                if key not in best_models:
                    print(f"  [SKIP] {key} not found in best_models.json")
                    continue

                model_info = best_models[key]
                print(f"  {target_column:<38}", end="  ")

                df_pred = enrich_predictions(
                    city=city,
                    target_column=target_column,
                    feature_cols=feature_cols,
                    df_city=df_city,
                    model_info=model_info,
                )

                if df_pred.empty:
                    continue

                print_summary(df_pred, city, target_column, model_info["model"])

                out_path = output_dir / f"scored_{city}_{scope}_{target_column}.csv"
                df_pred.to_csv(out_path, index=False)
                all_dfs.append(df_pred)

    # ── Combined output ──────────────────────────────────────────────────────
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined_path = output_dir / "scored_all.csv"
        combined.to_csv(combined_path, index=False)

        # FP-only view — false alarms with full confidence detail
        fp_only = combined[combined["label"] == "FP"].copy()
        fp_only_path = output_dir / "false_positives_all.csv"
        fp_only.to_csv(fp_only_path, index=False)

        # FN-only view — missed synthetics
        fn_only = combined[combined["label"] == "FN"].copy()
        fn_only_path = output_dir / "false_negatives_all.csv"
        fn_only.to_csv(fn_only_path, index=False)

        print()
        print(f"Wrote {len(combined):,} total rows  →  {combined_path}")
        print(f"Wrote {len(fp_only):,} false positives  →  {fp_only_path}")
        print(f"Wrote {len(fn_only):,} false negatives  →  {fn_only_path}")

        # ── Global label distribution ────────────────────────────────────────
        print()
        print("Label distribution (univariate only):")
        uni = combined[combined["target_column"] != "ALL_FEATURES"]
        by_label = uni.groupby(["city_id", "target_column", "label"]).size().unstack(fill_value=0)
        print(by_label.to_string())

        # ── Confidence score statistics per label ────────────────────────────
        print()
        print("Anomaly probability stats by label (mean ± std):")
        stats = (
            uni.groupby("label")["anomaly_prob"]
            .agg(["mean", "std", "min", "max", "count"])
            .round(4)
        )
        print(stats.to_string())

    print(f"\n[DONE] Artifacts written to: {output_dir}")


if __name__ == "__main__":
    main()
