"""Shared metric helpers for experiment scripts."""

from __future__ import annotations

import numpy as np


def f2_score(tp: int | float, fp: int | float, fn: int | float) -> float:
    """F2 score (recall-weighted, beta=2) from raw TP/FP/FN counts."""
    p = tp / (tp + fp) if (tp + fp) else 0.0
    r = tp / (tp + fn) if (tp + fn) else 0.0
    return 5 * p * r / (4 * p + r) if (4 * p + r) else 0.0


def label_outcomes(y_true: np.ndarray, y_pred: np.ndarray) -> list[str]:
    """Return human-readable TP/FP/FN/TN label for each prediction."""
    labels = []
    for yt, yp in zip(y_true, y_pred):
        if yt == 1 and yp == 1:
            labels.append("TP")
        elif yt == 0 and yp == 1:
            labels.append("FP")
        elif yt == 1 and yp == 0:
            labels.append("FN")
        else:
            labels.append("TN")
    return labels
