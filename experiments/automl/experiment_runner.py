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


def prepare_xy(df: pd.DataFrame, feature_cols: list[str]) -> tuple[np.ndarray, np.ndarray]:
    work = df[["time", *feature_cols, "y_true"]].dropna(subset=["time"]).copy()
    X = work[feature_cols].to_numpy(dtype=float)
    y = work["y_true"].to_numpy(dtype=int)
    return X, y


def split_train_valid(
    X: np.ndarray,
    y: np.ndarray,
    split_ratio: float = 0.7,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    unique_labels, label_counts = np.unique(y, return_counts=True)
    has_enough_per_class_for_stratify = len(unique_labels) > 1 and int(label_counts.min()) >= 2
    if has_enough_per_class_for_stratify and len(X) >= 4:
        return train_test_split(X, y, test_size=1 - split_ratio, random_state=42, stratify=y)

    split_idx = int(len(X) * split_ratio)
    split_idx = max(1, min(split_idx, len(X) - 1))
    return X[:split_idx], X[split_idx:], y[:split_idx], y[split_idx:]


def fit_predict_with_pipeline(
    X_train: np.ndarray,
    X_valid: np.ndarray,
    model_name: str,
    params: dict[str, Any],
) -> np.ndarray:
    model = build_model(model_name, params)
    preprocess = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    X_train_proc = preprocess.fit_transform(X_train)
    X_valid_proc = preprocess.transform(X_valid)

    model.fit(X_train_proc)
    return model.predict(X_valid_proc).astype(int)


def optimize_scope(
    city: str,
    target_column: str,
    X: np.ndarray,
    y: np.ndarray,
    n_trials: int,
    seed: int,
) -> tuple[EvalResult, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    X_train, X_valid, y_train, y_valid = split_train_valid(X, y)

    study = create_study(seed=seed)

    def objective(trial: optuna.Trial) -> float:
        model_name, params = suggest_model_and_params(trial)
        y_pred = fit_predict_with_pipeline(X_train, X_valid, model_name, params)
        _, _, f1 = compute_metrics(y_valid, y_pred)
        return f1

    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    best_model_name = study.best_trial.params["model"]
    best_params = dict(study.best_trial.params)
    best_params.pop("model", None)

    y_pred_best = fit_predict_with_pipeline(X_train, X_valid, best_model_name, best_params)
    precision, recall, f1 = compute_metrics(y_valid, y_pred_best)

    result = EvalResult(
        city=city,
        scope="univariate" if target_column in FEATURE_COLUMNS else "multivariate",
        model_name=best_model_name,
        target_column=target_column,
        precision=precision,
        recall=recall,
        f1=f1,
        n_rows=len(y_valid),
        n_positive_true=int(np.sum(y_valid == 1)),
        n_positive_pred=int(np.sum(y_pred_best == 1)),
    )

    trials_df = study.trials_dataframe()
    predictions_df = pd.DataFrame(
        {
            "city_id": city,
            "target_column": target_column,
            "y_true": y_valid,
            "y_pred": y_pred_best,
            "is_correct": (y_valid == y_pred_best).astype(int),
        }
    )

    model_info = {
        "model": best_model_name,
        "params": best_params,
        "best_objective_f1": float(study.best_value),
    }

    return result, trials_df, predictions_df, model_info


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
                    X, y = prepare_xy(df_city, feature_cols)
                    if len(X) < 20:
                        print(f"[WARN] Not enough rows for city={city}, target={target_column}. Skipping.")
                        continue

                    key = f"{city}:{current_scope}:{target_column}"
                    print(f"[INFO] Running {key} with n_trials={n_trials}")

                    result, trials_df, predictions_df, model_info = optimize_scope(
                        city=city,
                        target_column=target_column,
                        X=X,
                        y=y,
                        n_trials=n_trials,
                        seed=seed,
                    )

                    summary_rows.append(result)
                    best_models[key] = model_info

                    trials_path = output_dir / f"trials_{city}_{current_scope}_{target_column}.csv"
                    predictions_path = output_dir / f"predictions_{city}_{current_scope}_{target_column}.csv"
                    trials_df.to_csv(trials_path, index=False)
                    predictions_df.to_csv(predictions_path, index=False)

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
                                "best_objective_f1": model_info["best_objective_f1"],
                            }
                        )
                        mlflow.log_dict(model_info, "best_model.json")
                        mlflow.log_artifact(str(trials_path))
                        mlflow.log_artifact(str(predictions_path))

        save_outputs(output_dir=output_dir, summary_rows=summary_rows, best_models=best_models)
        log_artifact_if_exists(output_dir / "summary_metrics.csv")
        log_artifact_if_exists(output_dir / "best_models.json")
    return summary_rows, best_models


def save_outputs(output_dir: Path, summary_rows: list[EvalResult], best_models: dict[str, Any]) -> None:
    summary_df = pd.DataFrame([row.__dict__ for row in summary_rows])
    summary_df.to_csv(output_dir / "summary_metrics.csv", index=False)

    with (output_dir / "best_models.json").open("w", encoding="utf-8") as f:
        json.dump(best_models, f, indent=2, ensure_ascii=True)