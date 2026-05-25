"""Optuna search space for non-linear / Auto-NN experiments.

This implements Neural Architecture Search (NAS) over three model families:
  - AE  : Autoencoder (hidden layer widths, depth, activation, lr, epochs)
  - VAE : Variational Autoencoder (same search space as AE)
  - OCSVM: One-Class SVM (nu, gamma)

The 'Auto' in Auto-NN refers to Optuna's TPE sampler searching over the
architecture space automatically, the same principle behind AutoKeras /
Auto-PyTorch but without their heavy TensorFlow/SMAC3 dependencies.
"""
from __future__ import annotations
from typing import Any

import optuna

from nn_configs import SUPPORTED_MODELS


def create_study(seed: int) -> optuna.Study:
    sampler = optuna.samplers.TPESampler(seed=seed)
    return optuna.create_study(direction="maximize", sampler=sampler)


def suggest_model_and_params(trial: optuna.Trial, input_dim: int) -> tuple[str, dict[str, Any]]:
    model_name = trial.suggest_categorical("model", SUPPORTED_MODELS)
    contamination = trial.suggest_float("contamination", 0.001, 0.20, log=True)
    params: dict[str, Any] = {"contamination": contamination}

    # Both AE and VAE share the same NAS search space.
    n_layers = trial.suggest_int("n_layers", 1, 3)
    # Width is relative to input_dim so the search scales sensibly.
    # Minimum width of 4 ensures even 1-D univariate models have non-trivial layers.
    max_width = max(4, input_dim * 16)
    layer_width = trial.suggest_int("layer_width", max(4, input_dim * 2), max_width)
    hidden_dims = []
    w = layer_width
    for _ in range(n_layers):
        hidden_dims.append(max(2, w))
        w = max(2, w // 2)
    params["hidden_dims"] = hidden_dims
    params["activation"] = trial.suggest_categorical("activation", ["relu", "tanh", "elu", "leaky_relu"])
    params["lr"] = trial.suggest_float("lr", 1e-4, 1e-2, log=True)
    params["epochs"] = trial.suggest_int("epochs", 20, 100)
    params["batch_size"] = trial.suggest_categorical("batch_size", [128, 256, 512])

    return model_name, params
