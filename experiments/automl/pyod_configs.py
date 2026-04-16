"""PyOD model configuration for weather anomaly experiments."""

from __future__ import annotations

from typing import Any

from pyod.models.hbos import HBOS
from pyod.models.iforest import IForest
from pyod.models.lof import LOF
from pyod.models.ocsvm import OCSVM


DEFAULT_CITIES = ["amsterdam", "london", "new_york", "paris", "tokyo"]

FEATURE_COLUMNS = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "surface_pressure",
    "soil_temperature_7_to_28cm",
    "soil_moisture_7_to_28cm",
]

SUPPORTED_MODELS = ["IForest", "LOF", "OCSVM", "HBOS"]


def build_model(model_name: str, params: dict[str, Any]):
    if model_name == "IForest":
        return IForest(
            contamination=params["contamination"],
            n_estimators=params["n_estimators"],
            max_samples=params["max_samples"],
            random_state=42,
        )
    if model_name == "LOF":
        return LOF(
            contamination=params["contamination"],
            n_neighbors=params["n_neighbors"],
            leaf_size=params["leaf_size"],
            metric="minkowski",
            p=2,
        )
    if model_name == "OCSVM":
        return OCSVM(
            contamination=params["contamination"],
            kernel=params["kernel"],
            nu=params["nu"],
            gamma=params["gamma"],
        )
    if model_name == "HBOS":
        return HBOS(
            contamination=params["contamination"],
            n_bins=params["n_bins"],
            alpha=params["alpha"],
            tol=params["tol"],
        )
    raise ValueError(f"Unsupported model: {model_name}")