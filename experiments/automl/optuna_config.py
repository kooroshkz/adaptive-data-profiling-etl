"""Optuna search-space configuration for AutoML experiments."""

from __future__ import annotations

from typing import Any

import optuna

from pyod_configs import SUPPORTED_MODELS


def create_study(seed: int) -> optuna.Study:
    sampler = optuna.samplers.TPESampler(seed=seed)
    return optuna.create_study(direction="maximize", sampler=sampler)


def suggest_model_and_params(trial: optuna.Trial) -> tuple[str, dict[str, Any]]:
    model_name = trial.suggest_categorical("model", SUPPORTED_MODELS)

    # Wider contamination range — upper bound raised to 0.20 so the search
    # can reach the true anomaly rate (~5.3%) and beyond.
    params: dict[str, Any] = {
        "contamination": trial.suggest_float("contamination", 0.001, 0.20, log=True),
    }

    if model_name == "IForest":
        params["n_estimators"] = trial.suggest_int("n_estimators", 50, 200)
        params["max_samples"] = trial.suggest_float("max_samples", 0.3, 1.0)
    elif model_name == "LOF":
        params["n_neighbors"] = trial.suggest_int("n_neighbors", 5, 100)
        params["leaf_size"] = trial.suggest_int("leaf_size", 10, 80)
    elif model_name == "HBOS":
        params["n_bins"] = trial.suggest_int("n_bins", 5, 50)
        params["alpha"] = trial.suggest_float("alpha", 0.01, 0.5)
        params["tol"] = trial.suggest_float("tol", 0.05, 0.9)
    # COPOD and ECOD have no model-specific hyperparameters beyond contamination

    return model_name, params
