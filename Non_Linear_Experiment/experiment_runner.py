"""Experiment orchestration for Non-Linear / Auto-NN anomaly detection runs."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import optuna
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from data_loader import connect_s3_duckdb, fetch_city_data
from metrics import EvalResult, compute_metrics
from nn_configs import FEATURE_COLUMNS
from nn_models import build_detector
from optuna_nn import create_study, suggest_model_and_params


def prepare_xy(
    df: pd.DataFrame,
    feature_cols: list[str],
    target_column: str | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Identical logic to automl experiment_runner.prepare_xy."""
    all_extra = ["y_true", "synthetic_shift_pct", "synthetic_anomaly_details_json"]
    extra_cols = [c for c in all_extra if c in df.columns]
    work = df[["time", *feature_cols, *extra_cols]].dropna(subset=["time"]).copy()

    n = len(work)
    times = (pd.to_datetime(work["time"]).astype("int64") // 10**6).to_numpy(dtype="int64")
    X = work[feature_cols].to_numpy(dtype=float)
    y_values = work[feature_cols[0]].to_numpy(dtype=float)

    y = np.zeros(n, dtype=int)
    shift_pct = np.zeros(n, dtype=float)
    original_values: np.ndarray = np.full(n, np.nan)

    use_col_specific = (
        "synthetic_anomaly_details_json" in work.columns
        and target_column is not None
        and target_column != "ALL_FEATURES"
    )

    if use_col_specific:
        for i, json_str in enumerate(work["synthetic_anomaly_details_json"]):
            if pd.isna(json_str):
                continue
            try:
                d = json.loads(str(json_str))
                detail = d.get(target_column)
                if isinstance(detail, dict):
                    y[i] = 1
                    shift_pct[i] = abs(float(detail.get("shift_pct", 0.0)))
                    if detail.get("actual") is not None:
                        original_values[i] = float(detail["actual"])
            except (ValueError, TypeError, AttributeError):
                pass
    else:
        if "y_true" in work.columns:
            y = work["y_true"].to_numpy(dtype=int)
        if "synthetic_shift_pct" in work.columns:
            shift_pct = work["synthetic_shift_pct"].to_numpy(dtype=float)

    return times, X, y, y_values, shift_pct, original_values


def split_train_valid(
    X: np.ndarray,
    y: np.ndarray,
    times: np.ndarray,
    split_ratio: float = 0.7,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    unique_labels, label_counts = np.unique(y, return_counts=True)
    has_enough = len(unique_labels) > 1 and int(label_counts.min()) >= 2
    if has_enough and len(X) >= 4:
        indices = np.arange(len(X))
        idx_train, idx_valid = train_test_split(
            indices, test_size=1 - split_ratio, random_state=42, stratify=y
        )
        return X[idx_train], X[idx_valid], y[idx_train], y[idx_valid], times[idx_train], times[idx_valid]
    split_idx = max(1, min(int(len(X) * split_ratio), len(X) - 1))
    return X[:split_idx], X[split_idx:], y[:split_idx], y[split_idx:], times[:split_idx], times[split_idx:]


def preprocess(X_train: np.ndarray, X_infer: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Impute NaNs + StandardScale. Fit on train, apply to both."""
    pipe_impute = SimpleImputer(strategy="median")
    pipe_scale = StandardScaler()
    X_tr = pipe_scale.fit_transform(pipe_impute.fit_transform(X_train))
    X_inf = pipe_scale.transform(pipe_impute.transform(X_infer))
    return X_tr, X_inf


def optimize_scope(
    city: str,
    target_column: str,
    times: np.ndarray,
    X: np.ndarray,
    y: np.ndarray,
    y_values: np.ndarray,
    shift_pct: np.ndarray,
    original_values: np.ndarray,
    n_trials: int,
    seed: int,
) -> tuple[EvalResult, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    input_dim = X.shape[1]
    X_train, X_valid, y_train, y_valid, _, _ = split_train_valid(X, y, times)
    X_tr_p, X_val_p = preprocess(X_train, X_valid)

    study = create_study(seed=seed)

    def objective(trial: optuna.Trial) -> float:
        model_name, params = suggest_model_and_params(trial, input_dim=input_dim)
        det = build_detector(model_name, params, input_dim=input_dim, seed=seed)
        det.fit(X_tr_p)
        y_pred = det.predict(X_val_p)
        _, _, _, f2 = compute_metrics(y_valid, y_pred)
        return f2

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    t_search_start = time.perf_counter()
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    search_time_s = time.perf_counter() - t_search_start

    best_model_name = study.best_trial.params["model"]
    best_params = {k: v for k, v in study.best_trial.params.items() if k != "model"}

    # Reconstruct hidden_dims from Optuna params
    if best_model_name in ("AE", "VAE"):
        n_layers = best_params.get("n_layers", 1)
        layer_width = best_params.get("layer_width", max(4, input_dim * 4))
        hidden_dims = []
        w = layer_width
        for _ in range(n_layers):
            hidden_dims.append(max(2, w))
            w = max(2, w // 2)
        best_params["hidden_dims"] = hidden_dims

    # Reconstruct gamma
    if best_model_name == "OCSVM":
        gamma_mode = best_params.get("gamma_mode", "scale")
        if gamma_mode == "float":
            best_params["gamma"] = best_params.get("gamma_val", 1.0)
        else:
            best_params["gamma"] = gamma_mode

    # Refit on full data, time both train and inference
    X_all_p, _ = preprocess(X, X)
    best_det = build_detector(best_model_name, best_params, input_dim=input_dim, seed=seed)

    t0 = time.perf_counter()
    best_det.fit(X_all_p)
    train_time_s = time.perf_counter() - t0

    t1 = time.perf_counter()
    y_pred_all = best_det.predict(X_all_p)
    infer_time_s = time.perf_counter() - t1

    total_time_s = search_time_s + train_time_s

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
        train_time_s=round(train_time_s, 4),
        infer_time_s=round(infer_time_s, 6),
        total_time_s=round(total_time_s, 4),
        search_time_s=round(search_time_s, 4),
    )

    trials_df = study.trials_dataframe()
    predictions_df = pd.DataFrame({
        "time_ms": times,
        "city_id": city,
        "target_column": target_column,
        "y_value": y_values,
        "original_value": original_values,
        "y_true": y,
        "y_pred": y_pred_all,
        "is_correct": (y == y_pred_all).astype(int),
        "shift_pct": shift_pct,
    })

    model_info = {
        "model": best_model_name,
        "params": {k: v for k, v in best_params.items() if k not in ("hidden_dims",)},
        "hidden_dims": best_params.get("hidden_dims"),
        "best_objective_f2": float(study.best_value),
        "train_time_s": round(train_time_s, 4),
        "infer_time_s": round(infer_time_s, 6),
        "search_time_s": round(search_time_s, 4),
        "total_time_s": round(total_time_s, 4),
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
) -> tuple[list[EvalResult], dict[str, Any]]:
    con = connect_s3_duckdb()
    summary_rows: list[EvalResult] = []
    best_models: dict[str, Any] = {}

    requested_scopes = [scope] if scope in {"multivariate", "univariate"} else ["multivariate", "univariate"]

    for city in cities:
        df_city = fetch_city_data(con, bucket, city, start_date, end_date)
        if df_city.empty:
            print(f"[WARN] No data for city={city}, skipping.")
            continue

        for current_scope in requested_scopes:
            feature_sets = (
                [("ALL_FEATURES", FEATURE_COLUMNS)]
                if current_scope == "multivariate"
                else [(col, [col]) for col in FEATURE_COLUMNS]
            )

            for target_column, feature_cols in feature_sets:
                times, X, y, y_values, shift_pct, original_values = prepare_xy(
                    df_city, feature_cols, target_column=target_column
                )
                if len(X) < 20:
                    print(f"[WARN] Not enough rows for {city}:{target_column}, skipping.")
                    continue

                key = f"{city}:{current_scope}:{target_column}"
                print(f"[INFO] Running {key} | n_trials={n_trials} | n_rows={len(X)}")

                result, trials_df, predictions_df, model_info = optimize_scope(
                    city=city,
                    target_column=target_column,
                    times=times,
                    X=X,
                    y=y,
                    y_values=y_values,
                    shift_pct=shift_pct,
                    original_values=original_values,
                    n_trials=n_trials,
                    seed=seed,
                )

                summary_rows.append(result)
                best_models[key] = model_info

                trials_df.to_csv(output_dir / f"trials_{city}_{current_scope}_{target_column}.csv", index=False)
                predictions_df.to_csv(
                    output_dir / f"predictions_{city}_{current_scope}_{target_column}.csv", index=False
                )
                save_outputs(output_dir=output_dir, summary_rows=summary_rows, best_models=best_models)

                print(
                    f"       best={result.model_name:<6} P={result.precision:.3f} "
                    f"R={result.recall:.3f} F2={result.f2:.3f} "
                    f"train={result.train_time_s:.2f}s"
                )

    return summary_rows, best_models
