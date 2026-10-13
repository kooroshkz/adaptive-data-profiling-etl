# Adaptive Data Profiling ETL

An ETL pipeline that ingests weather data, flags anomalies with per-column AutoML
models, and transforms it for analysis. It runs fully locally in Docker, with no
cloud accounts. The `experiments/` folder holds the standalone thesis experiments.

## Layout

| Path | What |
|---|---|
| `airflow/` | The pipeline: DAGs, scripts, Docker setup. |
| `experiments/` | Standalone anomaly-detection experiments. |
| `dashboard/` | Next.js dashboard for the data and results. |
| `data/` | Committed models (`data/models/`) and results (`data/anomaly_results/`). |
| `profiling_schema.yml` | Per-column checks and AutoML settings. |
| `docs/` | Design notes. |

## Run it locally

Prerequisites: Docker + Docker Compose, Git LFS (the ~110 MB models live in LFS),
and internet access. No `.env` and no cloud accounts needed.

```bash
git lfs install          # once, so the models download as real files
cd airflow
docker compose up -d --build
```

- Airflow: http://localhost:8080 (airflow / airflow)
- Dashboard: http://localhost:3000

In Airflow, unpause and trigger one DAG:

- `weather_backfill` (manual): load history from 2024-01-01 to yesterday. Run once.
- `weather_ingestion` (daily): fetch new data incrementally.

A run ingests to local parquet, scores with the models in `data/models/`, builds a
DuckDB warehouse (`data/warehouse.duckdb`) and marts (`data/mart/`), and writes
predictions to `data/anomaly_results/`. Open the dashboard to view them.

Notes:
- Without Git LFS the models are pointer stubs. The pipeline still runs and skips
  scoring until you `git lfs pull` or run the `weather_automl_train` DAG.
- Native Linux permissions: run `echo "AIRFLOW_UID=$(id -u)" > .env` if needed.

## Optional

- Retrain models: trigger the `weather_automl_train` DAG (writes `data/models/v1/`).
- Cloud: copy `airflow/.env.example` to `airflow/.env` and set AWS (S3), MotherDuck,
  or Brevo email. Left blank, everything stays local.

## Experiments

Standalone scripts that read committed data offline with `--data-source local`:

```bash
# Weather, classical AutoML (PyOD + Optuna)
pip install -r experiments/automl/requirements.txt
python experiments/automl/run_automl.py --cities amsterdam london new_york paris tokyo --scope both --n-trials 30 --data-source local

# Weather, neural autoencoder search
pip install -r experiments/autonn/requirements.txt
python experiments/autonn/run_experiment.py --cities amsterdam london new_york paris tokyo --scope both --n-trials 30 --data-source local

# Electricity (GB demand)
pip install -r experiments/electricity/requirements.txt
python experiments/electricity/fetch_electricity_data.py
python experiments/electricity/run_electricity_automl.py
```

Results land in each experiment's `artifacts/` folder.

## Notes

- Models: `data/models/v1/partition=<city>/col=<column>/latest.pkl`.
- Transform is a local DuckDB step (`airflow/scripts/transform_local.py`); the
  `weather_dbt/` project is kept for reference but not used.
- Dashboard without Docker: `cd dashboard && npm install && npm run dev`, using a
  Python that has `duckdb` (set `PYTHON_BIN` or use the repo `.venv`).
