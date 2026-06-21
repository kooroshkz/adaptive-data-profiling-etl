# Adaptive Data Profiling ETL

This repository contains two separate parts:

1. **The ETL pipeline** to be running on server to simulate a real time data pipeline using Airflow, dbt, and AWS S3. It ingests weather data, detects anomalies, and transforms the data for analysis.
2. **The experiments** – standalone scripts used for the thesis which reads the stored data and compare anomaly detection methods.

---

## Repository layout

| Folder | What it is |
|---|---|
| `airflow/` | The ETL pipeline (Airflow DAGs, scripts, Docker setup). |
| `weather_dbt/` | dbt project that transforms raw data |
| `experiments/` | Standalone anomaly-detection experiments. |
| `dashboard/` | Next.js dashboard for viewing the data and results. |
| `docs/` | Design notes and diagrams. |
| `profiling_schema.yml` | Column definitions and quality checks used by the pipeline. |
| `requirements.txt` | Python packages for the project. |

---

## Requirements

#### To run the ETL pipeline:

- Python 3.12
- Docker and Docker Compose (only needed to run the ETL pipeline)
- An AWS S3 bucket (the pipeline and most experiments read/write here)
- Optional: MotherDuck for SQL Warehousing

#### To run the experiments:

- Python 3.12
- The packages in each experiment's own `requirements.txt`
- No S3 or other accounts (the input data is committed to the repository; use
  `--data-source local`)

---

## First-time setup ETL pipeline

1. Create and activate a virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install the Python packages:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure credentials

```bash
cd airflow
cp .env.example .env
```

4. Start Airflow

```bash
docker-compose up -d # Will run Airflow webserver accessible at http://localhost:8080
```

5. Load the historical data (first run only)

In the Airflow web page, turn on and trigger the **`weather_backfill`** DAG. It
downloads weather data from 2024-01-01 up to yesterday for all five cities and
uploads it to S3.

6. Train the anomaly models

Trigger the **`weather_automl_train`** DAG. It trains one anomaly-detection
model per city and per column and saves them to S3 under `models/v1/`.

---

## Run the experiments

The experiments are independent scripts. By default they read the input data
from S3. You do **not** need to set up S3 to run them: the input data is
committed to this repository (under `experiments/data/`), so the experiments can
run fully offline.

Choose where the data comes from with `--data-source`:

- `--data-source local` – read the committed parquet files. No S3, no network,
  no credentials. Use this for a reproducible run.
- `--data-source s3` – read from the S3 bucket (needs AWS credentials).

### AutoML anomaly detection (weather)

```bash
pip install -r experiments/automl/requirements.txt

python experiments/automl/run_automl.py \
  --cities amsterdam london new_york paris tokyo \
  --scope both \
  --n-trials 30 \
  --data-source local
```

Results are written to a timestamped folder under
`experiments/automl/artifacts/`. See `experiments/automl/README.md` for details.

### Neural-network anomaly detection (weather)

```bash
pip install -r experiments/autonn/requirements.txt

python experiments/autonn/run_experiment.py \
  --cities amsterdam london new_york paris tokyo \
  --scope both \
  --n-trials 30 \
  --data-source local
```

Results go to `experiments/autonn/artifacts/`.

### Electricity anomaly detection


```bash
pip install -r experiments/electricity/requirements.txt

# 1. Download the data and inject known anomalies (run once).
python experiments/electricity/fetch_electricity_data.py

# 2. Run the experiment.
python experiments/electricity/run_electricity_automl.py
```

Data is written to `experiments/electricity/data/` and results to
`experiments/electricity/artifacts/`.

---

## View the results

The `dashboard/` folder is a Next.js app that reads the data from S3 and shows
the columns and detected anomalies.

To run the dashboard locally:

```bash
cd dashboard
npm install
npm run dev
```
