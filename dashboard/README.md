# Dashboard

Next.js dashboard that shows the weather data and detected anomalies as scatter
plots. It reads local parquet through a small Python + DuckDB helper
(`scripts/s3_query.py`). No cloud is required.

## Run

With the stack: the dashboard is a service in `../airflow/docker-compose.yml` and
starts on http://localhost:3000.

Standalone:

```bash
npm install
npm run dev          # http://localhost:3000
```

Needs a Python with `duckdb` installed: set `PYTHON_BIN` to it, or use the repo
`.venv`. If the raw parquet is not at the default
`../airflow/data/raw/city=*/hourly_*.parquet`, set `RAW_PARQUET_GLOB`.

## How it works

The API routes (`/api/weather/metadata`, `/api/weather/scatter`) run
`scripts/s3_query.py`, which queries the local parquet with DuckDB and returns
JSON. Set the AWS variables in `.env.local` only if you want to read from S3
instead of local files.
