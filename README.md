# Adaptive Data Profiling ETL

Local ETL pipeline for weather data ingestion, transformation, and quality validation.

## Stack

- Apache Airflow - Orchestration
- DuckDB - Analytical database  
- dbt - SQL transformations
- Parquet - Columnar storage

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export AIRFLOW_HOME="$(pwd)/airflow"
airflow db init
airflow users create --username admin --password admin --role Admin \
  --email admin@example.com --firstname Admin --lastname User
```

## Structure

```
ingestion/        # API data fetching
dbt/models/       # SQL transformations (raw → staging → mart)
data/             # Parquet files
airflow/dags/     # Pipeline definitions
duckdb/           # Database files
```

## Running

Start Airflow:
```bash
# Terminal 1
source .venv/bin/activate && export AIRFLOW_HOME="$(pwd)/airflow"
airflow webserver --port 8080

# Terminal 2  
source .venv/bin/activate && export AIRFLOW_HOME="$(pwd)/airflow"
airflow scheduler
```

Access UI: http://localhost:8080 (admin/admin)

## Data Ingestion

```bash
# Single city, date range
python ingestion/weather_ingest.py --city amsterdam --start-date 2024-01-01 --end-date 2024-01-07

# Full backfill (2 years)
python ingestion/weather_ingest.py --city amsterdam --mode backfill
```

## Query Data

```bash
duckdb duckdb/weather.db
```

```sql
SELECT * FROM raw.weather LIMIT 10;
SELECT * FROM mart.weather_daily ORDER BY date DESC;
```

## Documentation

- [Requirements](docs/requirements.md)
- [Technical Design](docs/tech_design.md)  
- [Data Source](docs/upstream_data.md)
