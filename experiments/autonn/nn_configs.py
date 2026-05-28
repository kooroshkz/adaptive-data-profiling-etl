"""Shared constants for Non-Linear / AutoNN experiment."""
from __future__ import annotations

DEFAULT_CITIES = ["amsterdam", "london", "new_york", "paris", "tokyo"]

FEATURE_COLUMNS = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "surface_pressure",
    "soil_temperature_7_to_28cm",
    "soil_moisture_7_to_28cm",
]

# Models available for Optuna NAS.
# AE  = custom PyTorch Autoencoder (architecture search via Optuna)
# VAE = custom PyTorch Variational Autoencoder (architecture search via Optuna)
# OCSVM is intentionally excluded: it is not a neural network and would make
# the "Auto-NN" label misleading. A pure neural-network search space ensures
# a fair, apples-to-apples comparison with the AutoML classical baselines.
SUPPORTED_MODELS = ["AE", "VAE"]
