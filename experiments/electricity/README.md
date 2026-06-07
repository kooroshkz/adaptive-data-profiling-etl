# Electricity-domain anomaly experiment (thesis extension)

Secondary, **non-weather** benchmark for the Adaptive Profiler thesis, using the
same end-to-end recipe as the weather experiment but on a cleaner, keyless data
source: **GB national electricity demand** from the Elexon BMRS Insights API.

It demonstrates that the schema-driven, AutoML-based anomaly detection approach
generalises beyond weather to another tight, strongly-autocorrelated time series
(the demand columns score the same "GOOD / tight + smooth" profile as the
thesis's surface-pressure column — see `../data_source_probe.py`).

## Columns

| Column | Meaning | Cadence |
|---|---|---|
| `initialDemandOutturn` (INDO) | National demand met by the transmission system (MW) | half-hourly |
| `initialTransmissionSystemDemandOutturn` (ITSDO) | INDO + station load + interconnector exports (MW) | half-hourly |

The single national series is treated as one partition (`GB`), analogous to a
"city" in the weather experiment.

## Pipeline

```bash
# 1. One-time fetch (local Parquet, no S3) + inject anomalies at ingestion.
#    Each anomaly is shifted by a random 20%–50% of its value (random sign).
python experiments/electricity/fetch_electricity_data.py

# 2. Run the AutoML experiment (PyOD + Optuna, F2-optimised), producing
#    dashboard-compatible artifacts.
python experiments/electricity/run_electricity_automl.py
```

Outputs:

- `data/elexon_demand.parquet` — raw demand + injected anomalies + ground-truth metadata.
- `artifacts/run_<ts>/summary_metrics.csv`, `best_models.json`,
  `predictions_GB_<scope>_<column>.csv`, `trials_*.csv`.

## Anomaly injection

Mirrors `airflow/scripts/weather_ingest.py`. Defaults: `--anomaly-rate 0.04`,
shift magnitude `U[0.20, 0.50]`, per-column probability `0.5`. The exact shift is
recorded per row/column in `synthetic_anomaly_details_json` so univariate ground
truth is column-specific (same convention as the weather pipeline).

## Methodology reuse

`run_electricity_automl.py` imports the weather experiment's model construction,
Optuna search space and metric functions directly from `../automl`
(`pyod_configs.py`, `optuna_config.py`, `metrics.py`) — only the feature columns,
the local data source and the single `GB` partition differ. The detectors
searched are IForest, LOF, HBOS, COPOD and ECOD, optimised for F2.

> The standalone `adaptive-profiler` package powers the *production* Airflow path
> (`airflow/scripts/automl_train.py`). The **experiments** path here uses PyOD +
> Optuna directly, exactly like `experiments/automl`, so the dashboard renders
> both domains identically.

## Dashboard

Open the dashboard **Experiments** page and switch the **Domain** selector to
**Electricity** to visualise the demand series, injected anomalies and AutoML
detections (scatter + metrics table + shift-vs-detection chart) — identical UI to
the weather experiment.
