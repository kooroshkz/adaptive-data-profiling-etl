# Requirements Analysis

## Project Scope

This project builds a local ETL pipeline for ingesting, storing, transforming, and validating weather data.
The main goal is to create a reliable data pipeline that can later be extended with ML-based anomaly detection.
The focus is on data quality, reproducibility, and realistic ETL behavior rather than cloud infrastructure.

- **Functional Requirements**:
    - ingest historical and daily weather data from a public API
    - support one-time backfill and daily incremental ingestion
    - store raw and processed data in structured tables
    - apply rule-based data quality checks
    - store validation results and metadata
    - allow later integration of ML-based profiling
- **Non-Functional Requirements**:
    - run fully on a local machine
    - be easy to reset and reproduce
    - use open-source tools


## Data Storage and Query Engine

### DuckDB

DuckDB is used as the analytical database because it runs **locally** without a server, is easy to set up, and supports SQL queries directly on **Parquet files**. DuckDB is fast for **analytical queries** and supports **SQL-based transformations**. Other databases such as **PostgreSQL** or **cloud warehouses** are not used because they add operational or **cost complexity** without clear benefit for this project.


## Data Transformation and Validation

### dbt

dbt is used for **data transformations** and **schema management** because transformations are written in SQL and easy to review have built-in support for **testing** and **documentation**. dbt allows us to **define models** in a structured way and makes project be able to store **metadata** for **future ML integration**. Custom scripts or heavy processing frameworks are avoided to keep the system simple and transparent.


## Pipeline Orchestration

### Apache Airflow

Apache Airflow is used to **orchestrate** the ETL pipeline as supports scheduling and **task dependencies** (DAGs) while allows **historical backfills** and daily runs. Airflow help with **observability** and **error handling** and let us easily simulate **realistic ETL workflows**. Simple schedulers or manual execution are not sufficient for realistic pipeline evaluation.


## Table Format and Versioning

### Apache Iceberg

Apache Iceberg is used as the **table format** because it supports **table versioning** and **snapshots** to follow schema changes over time and **revert** to previous experiment versions. It helps **reproduce experiments** on **historical data**. Using plain files without versioning would limit the ability to analyze data quality changes over time.