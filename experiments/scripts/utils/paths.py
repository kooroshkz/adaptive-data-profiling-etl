"""Shared path constants and artifact discovery for experiment scripts."""

from __future__ import annotations

from pathlib import Path

# utils/ -> scripts/ -> experiments/ -> repo root
SCRIPTS_DIR  = Path(__file__).resolve().parents[1]
AUTOML_DIR   = SCRIPTS_DIR.parent / "automl"
DATA_DIR     = SCRIPTS_DIR.parent / "data" / "automl"


def latest_scored_dir() -> Path:
    """Return the most recent scored_* artifact directory under automl/artifacts/."""
    candidates = sorted(AUTOML_DIR.glob("artifacts/scored_*"))
    if not candidates:
        raise FileNotFoundError(
            "No scored_* artifact run found. Run rerun_with_scores.py first."
        )
    return candidates[-1]


def latest_automl_run() -> Path:
    """Return the most recent run_* artifact directory under automl/artifacts/."""
    candidates = sorted(AUTOML_DIR.glob("artifacts/run_*"))
    if not candidates:
        raise FileNotFoundError(
            "No run_* artifact found. Run the automl experiment first."
        )
    return candidates[-1]
