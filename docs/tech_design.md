# Technical design

The pipeline runs in Docker via Airflow and processes weather data in batches.
Storage defaults to local; S3 and MotherDuck are optional.

## Flow

1. Extract: Airflow tasks fetch hourly weather per city from the Open-Meteo API
   and write Parquet to `airflow/data/raw/city=<city>/`. Synthetic anomalies are
   injected at known positions for evaluation.
2. Detect: for each city, `automl_predict.py` loads the per-column model from
   `data/models/` and writes predictions to `data/anomaly_results/`.
3. Transform: `transform_local.py` builds a DuckDB warehouse
   (`data/warehouse.duckdb`) and daily marts (`data/mart/`) from the raw Parquet,
   deduplicating by (time, city).
4. Serve: the Next.js dashboard reads the local Parquet and shows the data and
   detected anomalies.

## Components

- Orchestration: Airflow (LocalExecutor) in Docker Compose, with Postgres for
  metadata.
- Quality checks: rule-based (data contract) plus per-column AutoML models,
  configured in `profiling_schema.yml`. Models are trained by the
  `weather_automl_train` DAG and stored under `data/models/v1/`.
- Warehouse: DuckDB file, read by the dashboard. Optional MotherDuck/S3 when set
  in `airflow/.env`.

## DAGs

- `weather_backfill` (manual): load history, then transform.
- `weather_ingestion` (daily): incremental fetch, detect, transform.
- `weather_automl_train` (manual): retrain per-column models.

## Storage

- Raw (scratch, regenerated each run): `airflow/data/raw/`.
- Durable, git-tracked: `data/models/`, `data/anomaly_results/`.
- Generated locally: `data/warehouse.duckdb`, `data/mart/`.
