# Adaptive Data Profiling ETL

This repository contains two separate parts:

1. **The ETL pipeline** (in `airflow/`): a self-contained Airflow pipeline that
   ingests weather data, detects anomalies with AutoML models, and transforms the
   data for analysis. It runs fully locally in Docker, with no cloud accounts.
2. **The experiments** (in `experiments/`): standalone scripts used for the thesis
   that read the stored data and compare anomaly-detection methods.

---

## Repository layout

| Folder | What it is |
|---|---|
| `airflow/` | The ETL pipeline (Airflow DAGs, scripts, Docker setup). |
| `experiments/` | Standalone anomaly-detection experiments. |
| `dashboard/` | Next.js dashboard for viewing the data and results. |
| `data/` | Git-tracked models (`data/models/`) and anomaly results (`data/anomaly_results/`). |
| `docs/` | Design notes and diagrams. |
| `profiling_schema.yml` | Column definitions and quality checks used by the pipeline. |

---

## Quickstart: run the whole pipeline locally (Docker)

No AWS, no MotherDuck, no accounts. You only need **Docker + Docker Compose** and
internet access (to fetch weather from the public Open-Meteo API). Trained models
are already committed in `data/models/`, so anomaly detection works out of the box.

1. Start everything (Airflow + dashboard):

   ```bash
   cd airflow
   docker compose up -d --build
   ```

   Airflow starts at http://localhost:8080 (user `airflow`, password `airflow`).
   The dashboard starts at http://localhost:3000.

2. In the Airflow UI, unpause and trigger a DAG:

   - **`weather_backfill`** (manual): loads history from 2024-01-01 to yesterday
     for all five cities. Run this once to populate data.
   - **`weather_ingestion`** (daily): fetches new data incrementally, scores it
     with the committed models, and rebuilds the local warehouse.

   Each run ingests locally, scores anomalies with the models in `data/models/`,
   builds a local DuckDB warehouse at `data/warehouse.duckdb` plus mart parquet in
   `data/mart/`, and writes predictions to `data/anomaly_results/`.

3. Open the dashboard at http://localhost:3000 to explore the data and the
   detected anomalies.

That is the whole loop: ingest, detect with local models, transform, and
visualise, all on your machine.

### Optional: retrain the models

Trigger the **`weather_automl_train`** DAG. It trains one model per city and
column and writes them to `data/models/v1/` (git-tracked), replacing the
committed ones.

### Optional: use cloud storage

Everything is local by default. To also mirror raw data and predictions to AWS S3,
copy `airflow/.env.example` to `airflow/.env` and fill in the AWS section (and, if
you like, MotherDuck or Brevo email alerts). Leave them blank to stay local.

---

## Run the experiments

The experiments are independent scripts. By default they read the input data
committed to this repository, so they run fully offline with `--data-source local`.

### AutoML anomaly detection (weather)

```bash
pip install -r experiments/automl/requirements.txt

python experiments/automl/run_automl.py \
  --cities amsterdam london new_york paris tokyo \
  --scope both \
  --n-trials 30 \
  --data-source local
```

### Neural-network anomaly detection (weather)

```bash
pip install -r experiments/autonn/requirements.txt

python experiments/autonn/run_experiment.py \
  --cities amsterdam london new_york paris tokyo \
  --scope both \
  --n-trials 30 \
  --data-source local
```

### Electricity anomaly detection

```bash
pip install -r experiments/electricity/requirements.txt

# 1. Download the data and inject known anomalies (run once).
python experiments/electricity/fetch_electricity_data.py

# 2. Run the experiment.
python experiments/electricity/run_electricity_automl.py
```

Results go to each experiment's `artifacts/` folder.

---

## Notes

- Models are git-tracked at `data/models/v1/partition=<city>/col=<column>/latest.pkl`.
- The Transform stage is a local DuckDB step (`airflow/scripts/transform_local.py`)
  that builds the warehouse and marts in-container. It replaces the previous
  GitHub-Actions dbt job. The `weather_dbt/` project is kept for reference but is
  not used by the local run.
- To run the dashboard outside Docker: `cd dashboard && npm install && npm run dev`,
  using a Python that has `duckdb` installed (set `PYTHON_BIN`, or use the repo `.venv`).
