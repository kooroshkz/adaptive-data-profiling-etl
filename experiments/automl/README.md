# AutoML Anomaly Experiments (PyOD + Optuna)

This folder contains an isolated experiment runner for unsupervised anomaly detection on the weather raw hourly data in S3.

## Goal

- Train anomaly detectors without using the synthetic anomaly label as a feature.
- Auto-select model type and hyperparameters with Optuna.
- Evaluate detection quality against injected synthetic labels after prediction.

## Models searched

- `IForest` (Isolation Forest)
- `LOF` (Local Outlier Factor)
- `OCSVM` (One-Class SVM)
- `HBOS`

## Scopes

The runner supports:

- `multivariate`: all weather feature columns together
- `univariate`: one model per weather column
- `both`: run both modes

## Data

Expected source:

- `s3://$S3_BUCKET/raw/city=*/hourly_*.parquet`

Required columns:

- Features: `temperature_2m`, `apparent_temperature`, `precipitation`, `surface_pressure`, `soil_temperature_7_to_28cm`, `soil_moisture_7_to_28cm`
- Label for evaluation only: `synthetic_anomaly_flag`

## Usage

1. Install dependencies in your active virtual environment:

```bash
pip install -r experiments/automl/requirements.txt
```

2. Ensure env vars are available (or passed in shell):

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET`

3. Run experiment:

```bash
python experiments/automl/run_automl.py \
  --cities amsterdam london new_york paris tokyo \
  --scope both \
  --n-trials 30
```

Optional date filters:

```bash
python experiments/automl/run_automl.py --start-date 2024-01-01 --end-date 2026-04-13
```

## Outputs

A timestamped directory is created under `experiments/automl/artifacts/` with:

- `summary_metrics.csv`: precision/recall/f1/FPR per city and scope
- `best_models.json`: chosen model and params per run
- `predictions_*.csv`: row-level predictions with correctness
- `trials_*.csv`: Optuna trial history

## Notes

- Labels are never used as model inputs.
- Labels are used only for model selection and evaluation in this controlled thesis experiment setup.
