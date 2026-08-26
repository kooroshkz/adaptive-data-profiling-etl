# Requirements

## Goal

A reproducible ETL pipeline that ingests weather data, applies rule-based and
ML-based quality checks, and stores results for analysis. It runs locally with no
cloud accounts; S3 and MotherDuck are optional.

## Functional

- Ingest historical (backfill) and daily weather from the Open-Meteo API.
- Store raw and transformed data as local Parquet, partitioned by city.
- Apply rule-based checks (type, range, not-null) and per-column AutoML anomaly
  detection at ingestion.
- Record predictions and daily aggregates for the dashboard.

## Non-functional

- One command to start (`docker compose up`), no required config.
- Reproducible: committed models and offline experiment data.
- Open-source tools only.

## Stack

- Orchestration: Apache Airflow (Docker).
- Storage: local Parquet plus a local DuckDB warehouse. Optional S3 / MotherDuck.
- Transform: DuckDB (Python). ML: adaptive-profiler (PyOD + Optuna).
- Dashboard: Next.js.
