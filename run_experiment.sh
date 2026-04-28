#!/usr/bin/env bash
# Run the full AutoML experiment (all cities, multivariate + univariate).
# Downloads latest data from S3, runs Optuna tuning, writes artifacts to
# experiments/automl/artifacts/run_<timestamp>/.
#
# Usage (no arguments needed for a full run):
#   ./run_experiment.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$REPO_ROOT/experiments/automl/run_automl.py"

# Pick python: active venv → repo-root .venv → experiment-local .venv
if [[ -n "${VIRTUAL_ENV:-}" && -f "$VIRTUAL_ENV/bin/python" ]]; then
  PYTHON="$VIRTUAL_ENV/bin/python"
elif [[ -f "$REPO_ROOT/.venv/bin/python" ]]; then
  PYTHON="$REPO_ROOT/.venv/bin/python"
elif [[ -f "$REPO_ROOT/experiments/automl/.venv/bin/python" ]]; then
  PYTHON="$REPO_ROOT/experiments/automl/.venv/bin/python"
else
  echo "[ERROR] No virtualenv found. Create one and install requirements:"
  echo "        python -m venv .venv && .venv/bin/pip install -r experiments/automl/requirements.txt"
  exit 1
fi

cd "$REPO_ROOT"

echo "[INFO] Python : $PYTHON"
echo "[INFO] S3_BUCKET=${S3_BUCKET:-weather-data-koorosh-thesis}"
echo ""

"$PYTHON" "$SCRIPT" "$@"
