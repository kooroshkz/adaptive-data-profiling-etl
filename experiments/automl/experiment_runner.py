"""Experiment orchestration for PyOD + Optuna AutoML runs."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import mlflow
import numpy as np
import pandas as pd
import optuna
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from data_loader import fetch_and_cache_city_data
from metrics import EvalResult, compute_metrics
from mlflow_utils import log_artifact_if_exists, start_run
from optuna_config import create_study, suggest_model_and_params
from pyod_configs import FEATURE_COLUMNS, build_model


def prepare_xy(df: pd.DataFrame, feature_cols: list[str]) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    work = df[["time", *feature_cols, "y_true"]].dropna(subset=["time"]).copy()
    times = (pd.to_datetime(work["time"]).astype("int64") // 10**6).to_numpy(dtype="int64")
    X = work[feature_cols].to_numpy(dtype=float)
    y = work["y_true"].to_numpy(dtype=int)
    # First feature column used as the scatter plot Y axis value.
    y_values = work[feature_cols[0]].to_numpy(dtype=float)
    return times, X, y, y_values


def split_train_valid(
    X: np.ndarray,
    y: np.ndarray,
    times: np.ndarray,
    split_ratio: float = 0.7,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    unique_labels, label_counts = np.unique(y, return_counts=True)
    has_enough_per_class_for_stratify = len(unique_labels) > 1 and int(label_counts.min()) >= 2
    if has_enough_per_class_for_stratify and len(X) >= 4:
        indices = np.arange(len(X))
        idx_train, idx_valid = train_test_split(
            indices, test_size=1 - split_ratio, random_state=42, stratify=y
        )
        return X[idx_train], X[idx_valid], y[idx_train], y[idx_valid], times[idx_train], times[idx_valid]

    split_idx = int(len(X) * split_ratio)
    split_idx = max(1, min(split_idx, len(X) - 1))
    return X[:split_idx], X[split_idx:], y[:split_idx], y[split_idx:], times[:split_idx], times[split_idx:]


def build_preprocessing_pipeline() -> Pipeline:
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )


def fit_predict_with_pipeline(
    X_train: np.ndarray,
    X_infer: np.ndarray,
    model_name: str,
    params: dict[str, Any],
) -> np.ndarray:
    """Fit preprocessing + model on X_train, predict on X_infer."""
    model = build_model(model_name, params)
    preprocess = build_preprocessing_pipeline()
    X_train_proc = preprocess.fit_transform(X_train)
    X_infer_proc = preprocess.transform(X_infer)
    model.fit(X_train_proc)
    return model.predict(X_infer_proc).astype(int)


def optimize_scope(
    city: str,
    target_column: str,
    times: np.ndarray,
    X: np.ndarray,
    y: np.ndarray,
    y_values: np.ndarray,
    n_trials: int,
    seed: int,
) -> tuple[EvalResult, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    # ── Hyperparameter search on full held-out validation split ───────────────
    # No subsampling — the full dataset is used so contamination tuning reflects
    # the real anomaly distribution. Objective is F2 (beta=2) which weights
    # recall twice as heavily as precision, pushing models to find more anomalies.
    X_train, X_valid, y_train, y_valid, _times_train, _times_valid = split_train_valid(X, y, times)

    study = create_study(seed=seed)

    def objective(trial: optuna.Trial) -> float:
        model_name, params = suggest_model_and_params(trial)
        y_pred = fit_predict_with_pipeline(X_train, X_valid, model_name, params)
        _, _, _, f2 = compute_metrics(y_valid, y_pred)
        return f2

    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    best_model_name = study.best_trial.params["model"]
    best_params = dict(study.best_trial.params)
    best_params.pop("model", None)

    # ── Refit best model on ALL data → predict on ALL data ───────────────────
    # Unsupervised models don't use labels during training so fitting on the full
    # dataset is valid and gives every point a prediction (no "not evaluated" gap).
    y_pred_all = fit_predict_with_pipeline(X, X, best_model_name, best_params)
    precision, recall, f1, f2 = compute_metrics(y, y_pred_all)

    scope_label = "univariate" if target_column in FEATURE_COLUMNS else "multivariate"

    result = EvalResult(
        city=city,
        scope=scope_label,
        model_name=best_model_name,
        target_column=target_column,
        precision=precision,
        recall=recall,
        f1=f1,
        f2=f2,
        n_rows=len(y),
        n_positive_true=int(np.sum(y == 1)),
        n_positive_pred=int(np.sum(y_pred_all == 1)),
    )

    trials_df = study.trials_dataframe()

    predictions_df = pd.DataFrame(
        {
            "time_ms": times,
            "city_id": city,
            "target_column": target_column,
            "y_value": y_values,   # actual sensor reading for scatter Y axis
            "y_true": y,
            "y_pred": y_pred_all,
            "is_correct": (y == y_pred_all).astype(int),
        }
    )

    model_info = {
        "model": best_model_name,
        "params": best_params,
        "best_objective_f2": float(study.best_value),
    }

    return result, trials_df, predictions_df, model_info


def save_outputs(output_dir: Path, summary_rows: list[EvalResult], best_models: dict[str, Any]) -> None:
    summary_df = pd.DataFrame([row.__dict__ for row in summary_rows])
    summary_df.to_csv(output_dir / "summary_metrics.csv", index=False)

    with (output_dir / "best_models.json").open("w", encoding="utf-8") as f:
        json.dump(best_models, f, indent=2, ensure_ascii=True)


def run_experiments(
    cities: list[str],
    scope: str,
    n_trials: int,
    seed: int,
    start_date: str | None,
    end_date: str | None,
    bucket: str,
    output_dir: Path,
    tracking_uri: str | None,
    experiment_name: str,
) -> tuple[list[EvalResult], dict[str, Any]]:
    from data_loader import connect_s3_duckdb
    from mlflow_utils import configure_mlflow

    configure_mlflow(experiment_name, tracking_uri)
    con = connect_s3_duckdb()

    summary_rows: list[EvalResult] = []
    best_models: dict[str, Any] = {}

    requested_scopes = [scope] if scope in {"multivariate", "univariate"} else ["multivariate", "univariate"]

    with start_run(run_name=f"automl_batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}", nested=False):
        for city in cities:
            df_city = fetch_and_cache_city_data(con, bucket, city, start_date, end_date)
            if df_city.empty:
                print(f"[WARN] No data found for city={city}. Skipping.")
                continue

            for current_scope in requested_scopes:
                feature_sets = [("ALL_FEATURES", FEATURE_COLUMNS)] if current_scope == "multivariate" else [
                    (col, [col]) for col in FEATURE_COLUMNS
                ]

                for target_column, feature_cols in feature_sets:
                    times, X, y, y_values = prepare_xy(df_city, feature_cols)
                    if len(X) < 20:
                        print(f"[WARN] Not enough rows for city={city}, target={target_column}. Skipping.")
                        continue

                    key = f"{city}:{current_scope}:{target_column}"
                    print(f"[INFO] Running {key} with n_trials={n_trials}")

                    result, trials_df, predictions_df, model_info = optimize_scope(
                        city=city,
                        target_column=target_column,
                        times=times,
                        X=X,
                        y=y,
                        y_values=y_values,
                        n_trials=n_trials,
                        seed=seed,
                    )

                    summary_rows.append(result)
                    best_models[key] = model_info

                    trials_path = output_dir / f"trials_{city}_{current_scope}_{target_column}.csv"
                    predictions_path = output_dir / f"predictions_{city}_{current_scope}_{target_column}.csv"
                    trials_df.to_csv(trials_path, index=False)
                    predictions_df.to_csv(predictions_path, index=False)

                    # Save summary incrementally so partial results are visible immediately
                    save_outputs(output_dir=output_dir, summary_rows=summary_rows, best_models=best_models)

                    with start_run(run_name=key, nested=True):
                        mlflow.log_params(
                            {
                                "city": city,
                                "scope": current_scope,
                                "target_column": target_column,
                                "n_trials": n_trials,
                                "seed": seed,
                                "start_date": start_date or "",
                                "end_date": end_date or "",
                                "n_rows": result.n_rows,
                                "n_positive_true": result.n_positive_true,
                                "n_positive_pred": result.n_positive_pred,
                                "model_name": result.model_name,
                            }
                        )
                        mlflow.log_metrics(
                            {
                                "precision": result.precision,
                                "recall": result.recall,
                                "f1": result.f1,
                                "f2": result.f2,
                                "best_objective_f2": model_info["best_objective_f2"],
                            }
                        )
                        mlflow.log_dict(model_info, "best_model.json")
                        mlflow.log_artifact(str(trials_path))
                        mlflow.log_artifact(str(predictions_path))

        log_artifact_if_exists(output_dir / "summary_metrics.csv")
        log_artifact_if_exists(output_dir / "best_models.json")
    return summary_rows, best_models
