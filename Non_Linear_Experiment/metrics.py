"""Metric helpers for synthetic-anomaly evaluation (identical to automl version)."""
from __future__ import annotations
from dataclasses import dataclass, field

import numpy as np
from sklearn.metrics import fbeta_score, f1_score, precision_score, recall_score


@dataclass
class EvalResult:
    city: str
    scope: str
    model_name: str
    target_column: str
    precision: float
    recall: float
    f1: float
    f2: float
    n_rows: int
    n_positive_true: int
    n_positive_pred: int
    train_time_s: float = field(default=0.0)
    infer_time_s: float = field(default=0.0)
    search_time_s: float = field(default=0.0)
    total_time_s: float = field(default=0.0)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[float, float, float, float]:
    """Returns (precision, recall, f1, f2)."""
    precision = float(precision_score(y_true, y_pred, zero_division=0))
    recall = float(recall_score(y_true, y_pred, zero_division=0))
    f1 = float(f1_score(y_true, y_pred, zero_division=0))
    f2 = float(fbeta_score(y_true, y_pred, beta=2, zero_division=0))
    return precision, recall, f1, f2
