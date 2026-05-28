"""Shared preprocessing pipeline for experiment scripts."""

from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def build_preprocessing_pipeline() -> Pipeline:
    """Median imputer + StandardScaler — matches the original experiment_runner."""
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
    ])
