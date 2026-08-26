# Electricity anomaly experiment

A non-weather benchmark using the same recipe as the weather experiment, on GB
national electricity demand (Elexon BMRS Insights API, no key required). It checks
that the AutoML approach generalises to another tight, seasonal time series.

## Columns

| Column | Meaning | Cadence |
|---|---|---|
| `initialDemandOutturn` (INDO) | Demand met by the transmission system (MW) | half-hourly |
| `initialTransmissionSystemDemandOutturn` (ITSDO) | INDO plus station load and interconnector exports (MW) | half-hourly |

The national series is one partition (`GB`), like a city in the weather experiment.

## Run

```bash
pip install -r experiments/electricity/requirements.txt

# 1. Fetch demand (local Parquet) and inject anomalies at ingestion.
python experiments/electricity/fetch_electricity_data.py

# 2. Run the AutoML experiment (PyOD + Optuna, F2-optimised).
python experiments/electricity/run_electricity_automl.py
```

## Outputs

- `data/elexon_demand.parquet`: demand plus injected anomalies and ground truth.
- `artifacts/run_<ts>/`: `summary_metrics.csv`, `best_models.json`,
  `predictions_*.csv`, `trials_*.csv`.

## Notes

- Injection mirrors the weather pipeline: default rate 4%, shift 20% to 50% of the
  value (random sign), per-column probability 0.5, recorded per row in
  `synthetic_anomaly_details_json`.
- Model construction, search space, and metrics are reused from `../automl`.
  Detectors: IForest, LOF, HBOS, COPOD, ECOD.
- Dashboard: open the Experiments page and set Domain to Electricity.
