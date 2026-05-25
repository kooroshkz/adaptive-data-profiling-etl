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

# Models available for Optuna search.
# AE  = custom PyTorch Autoencoder (NAS via Optuna)
# VAE = custom PyTorch Variational Autoencoder (NAS via Optuna)
# OCSVM = One-Class SVM, RBF kernel (sklearn); sub-sampled due to O(n^2) cost
SUPPORTED_MODELS = ["AE", "VAE", "OCSVM"]

# OCSVM sub-sample cap to keep O(n^2) training tractable
OCSVM_MAX_TRAIN_SAMPLES = 3_000
