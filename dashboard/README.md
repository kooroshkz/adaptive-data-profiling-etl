# Adaptive Data Profiling Dashboard

Interactive Next.js + TypeScript dashboard that reads weather parquet datasets from S3 (through DuckDB in a Python bridge) and visualizes selected columns as scatter plots with synthetic anomaly highlighting.

## Features

- Dataset switcher for `raw_hourly` and `raw_daily`
- City selection like MotherDuck data exploration
- X/Y numeric column selection for scatter plot axes
- Synthetic anomalies highlighted in a dedicated plot series
- KPI cards for row count, anomaly count/rate, and average shift percentage
- Recent anomaly sample table for quick inspection

## Prerequisites

- Node.js 20+
- Python environment with `duckdb` installed
- Access to S3 weather parquet files

## Environment Setup

1. Copy environment template:

```bash
cp .env.local.example .env.local
```

2. Fill in credentials in `.env.local`:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET`

Optional:

- `PYTHON_BIN` if you do not want default `../.venv/bin/python`

## Run Locally

```bash
npm install
npm run dev
```

Open `http://localhost:3000`.

## Data Query Architecture

- Frontend calls Next.js API routes:
	- `/api/weather/metadata`
	- `/api/weather/scatter`
- API routes execute `scripts/s3_query.py`
- Python uses DuckDB `httpfs` to query S3 parquet directly

This keeps AWS credentials server-side and avoids exposing S3 access from the browser.
