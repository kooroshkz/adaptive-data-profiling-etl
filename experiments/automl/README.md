# AutoML anomaly experiment (PyOD + Optuna)

Unsupervised anomaly detection on the weather hourly data. Optuna selects a PyOD
detector and its hyperparameters per column. The injected synthetic labels are
used only for evaluation and model selection, never as model inputs.

- Detectors: IForest, LOF, HBOS, COPOD, ECOD.
- Scopes: `univariate` (one model per column), `multivariate` (all columns), `both`.

## Run

```bash
pip install -r experiments/automl/requirements.txt

python experiments/automl/run_automl.py \
  --cities amsterdam london new_york paris tokyo \
  --scope both --n-trials 30 --data-source local
```

`--data-source local` reads the committed parquet (no cloud). Use
`--data-source s3` with AWS env vars to read from S3 instead. Optional date
filters: `--start-date 2024-01-01 --end-date 2026-04-13`.

## Outputs

A timestamped folder under `artifacts/` with:

- `summary_metrics.csv`: precision / recall / F1 / F2 per city and scope.
- `best_models.json`: chosen model and params.
- `predictions_*.csv`, `trials_*.csv`: row-level predictions and Optuna history.
