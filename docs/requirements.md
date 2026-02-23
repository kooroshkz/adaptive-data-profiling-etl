# Requirements Analysis

## Project Scope

This project builds an ETL pipeline for ingesting, storing, transforming, and validating weather data.
The main goal is to create a reliable data pipeline that can later be extended with ML-based anomaly detection.
The focus is on data quality, reproducibility, and realistic ETL behavior.

- **Functional Requirements**:
    - ingest historical and daily weather data from a public API
    - support one-time backfill and daily incremental ingestion
    - store raw and processed data in structured tables
    - apply rule-based data quality checks
    - store validation results and metadata
    - allow later integration of ML-based profiling
- **Non-Functional Requirements**:
    - run on AWS EC2 with managed storage
    - be easy to reset and reproduce
    - use open-source tools


## Data Storage and Query Engine

### MotherDuck and S3

MotherDuck is used as the analytical database because it provides cloud-hosted DuckDB with S3 integration, is easy to set up, and supports SQL queries directly on **Parquet files**. Parquet files are stored in **S3 with Hive partitioning** for efficient querying and scalability. DuckDB is fast for **analytical queries** and supports **SQL-based transformations**.


## Data Transformation and Validation

### dbt

dbt is used for **data transformations** and **schema management** because transformations are written in SQL and easy to review have built-in support for **testing** and **documentation**. dbt allows us to **define models** in a structured way and makes project be able to store **metadata** for **future ML integration**. Custom scripts or heavy processing frameworks are avoided to keep the system simple and transparent.


## Pipeline Orchestration

### Apache Airflow and GitHub Actions

Apache Airflow is used to **orchestrate** the ETL pipeline as supports scheduling and **task dependencies** (DAGs) while allows **historical backfills** and daily runs. Airflow help with **observability** and **error handling** and let us easily simulate **realistic ETL workflows**. dbt transformations run in **GitHub Actions** and Airflow waits for workflow completion to ensure pipeline integrity. Email notifications via Brevo API alert on failures.


## Data Versioning

### MotherDuck and S3

MotherDuck is used with **Parquet files in S3** for storage and supports **schema evolution** natively. Data versioning is achieved through **timestamped table names** and **Hive partition strategies** to track schema changes over time and **revert** to previous experiment versions. This helps **reproduce experiments** on **historical data** without adding external table format complexity.