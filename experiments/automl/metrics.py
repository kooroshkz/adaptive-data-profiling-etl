"""Metric helpers for synthetic-anomaly evaluation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from sklearn.metrics import f1_score, precision_score, recall_score


@dataclass
class EvalResult:
    city: str
    scope: str
    model_name: str
    target_column: str
    precision: float
    recall: float
    f1: float
    n_rows: int
    n_positive_true: int
    n_positive_pred: int


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[float, float, float]:
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)
    return float(precision), float(recall), float(f1)