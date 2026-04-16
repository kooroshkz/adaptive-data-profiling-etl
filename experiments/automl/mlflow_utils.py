"""Helpers for MLflow tracking in AutoML experiments."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import mlflow


def configure_mlflow(experiment_name: str, tracking_uri: str | None = None) -> None:
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)


@contextmanager
def start_run(run_name: str, nested: bool = False) -> Iterator[None]:
    with mlflow.start_run(run_name=run_name, nested=nested):
        yield


def log_artifact_if_exists(file_path: Path) -> None:
    if file_path.exists():
        mlflow.log_artifact(str(file_path))