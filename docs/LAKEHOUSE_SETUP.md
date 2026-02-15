# Lakehouse Architecture Setup Guide

## Architecture Overview

```
┌─────────────────────────────────────────────┐
│ EC2 t3.micro ($8/month)                     │
├─────────────────────────────────────────────┤
│ Airflow: Orchestration Only                │
│  1. Fetch raw weather data (5 cities)      │
│  2. Upload → S3 raw/*.parquet               │
│  3. Trigger GitHub Actions webhook          │
└─────────────────────────────────────────────┘
                    ↓ webhook
┌─────────────────────────────────────────────┐
│ GitHub Actions (FREE, 2000 min/month)      │
├─────────────────────────────────────────────┤
│ dbt + DuckDB Transformations:               │
│  1. Read s3://bucket/raw/*.parquet          │
│  2. Transform (staging → mart)              │
│  3. Write → s3://bucket/mart/*.parquet      │
└─────────────────────────────────────────────┘
                    ↓
        ┌───────────────────────┐
        │   Amazon S3 (Data Lake)   │
        ├───────────────────────┤
        │ /raw/                 │ ← Raw hourly/daily parquet
        │ /staging/             │ ← Cleaned data
        │ /mart/                │ ← Aggregated analytics tables
        └───────────────────────┘
                    ↑
        ┌───────────────────────┐
        │   MotherDuck UI       │ ← Query & Visualize
        └───────────────────────┘
```

## Benefits

- ✅ **No local database** - All data in S3
- ✅ **Cheap** - $8/month for EC2 + S3 storage costs
- ✅ **Scalable** - GitHub Actions has 8GB RAM for transformations
- ✅ **Separation of concerns** - Airflow for orchestration, GitHub for compute
- ✅ **Cloud-native** - MotherDuck queries S3 directly

---

## Setup Instructions

### 1. GitHub Repository Secrets

Add these secrets to your GitHub repository (`Settings` → `Secrets and variables` → `Actions`):

| Secret Name | Value | Description |
|------------|-------|-------------|
| `AWS_ACCESS_KEY_ID` | `AKIARNFXKIB3GC52PXPT` | Your AWS access key |
| `AWS_SECRET_ACCESS_KEY` | `Wn8MXDFpQf9ub/...` | Your AWS secret key |
| `AWS_REGION` | `us-east-1` | S3 bucket region |
| `S3_BUCKET` | `weather-data-koorosh-thesis` | Your S3 bucket name |

**How to add:**
```bash
# On GitHub:
1. Go to: https://github.com/kooroshkz/adaptive-data-profiling-etl/settings/secrets/actions
2. Click "New repository secret"
3. Add each secret above
```

### 2. GitHub Personal Access Token (for Airflow)

Create a token with `repo` scope to trigger workflows:

```bash
# On GitHub:
1. Go to: https://github.com/settings/tokens
2. Click "Generate new token (classic)"
3. Name: "Airflow Workflow Trigger"
4. Scope: Select "repo" (full control of private repositories)
5. Click "Generate token"
6. Copy the token (ghp_xxxxxxxxxxxx)
```

### 3. EC2 Environment Variables

Add the GitHub token to your EC2 Airflow environment:

**On EC2 server:**
```bash
# SSH to EC2
ssh airflow

# Edit .env file
cd ~/adaptive-data-profiling-etl/airflow
nano .env
```

**Add these lines to `.env`:**
```bash
# GitHub token for triggering workflows
GITHUB_TOKEN=ghp_your_token_here
GITHUB_REPO_OWNER=kooroshkz
GITHUB_REPO_NAME=adaptive-data-profiling-etl
```

**Restart Airflow to load new environment variables:**
```bash
docker-compose -f docker-compose.t3micro.yml down
docker-compose -f docker-compose.t3micro.yml up -d
```

### 4. Test the Pipeline

#### Option A: Trigger from Airflow UI

1. **SSH tunnel to Airflow:**
   ```bash
   # On your Mac
   ssh -i ~/.ssh/your-key.pem -L 8080:localhost:8080 ec2-user@52.54.106.82 -N
   ```

2. **Open Airflow UI:**
   - Browser: `http://localhost:8080`
   - Login: `airflow` / `airflow`

3. **Trigger DAG:**
   - Find `weather_ingestion` DAG
   - Toggle it ON (enable)
   - Click ▶️ "Trigger DAG"

4. **Watch execution:**
   - Task should complete: `install_dependencies` → `ingest_*` → `upload_to_s3` → `trigger_dbt_transform` → `log_completion`
   - All should turn green ✅

#### Option B: Trigger from EC2 CLI

```bash
# On EC2
docker exec airflow-airflow-scheduler-1 airflow dags unpause weather_ingestion
docker exec airflow-airflow-scheduler-1 airflow dags trigger weather_ingestion
```

#### Option C: Manual GitHub Actions Test

```bash
# On GitHub:
1. Go to: https://github.com/kooroshkz/adaptive-data-profiling-etl/actions
2. Click "dbt Transformations" workflow
3. Click "Run workflow" → "Run workflow"
4. Watch the execution
```

### 5. Verify Data in S3

**Check raw data uploaded:**
```bash
aws s3 ls s3://weather-data-koorosh-thesis/raw/ --recursive --human-readable
```

**Check transformed data:**
```bash
aws s3 ls s3://weather-data-koorosh-thesis/staging/ --recursive --human-readable
aws s3 ls s3://weather-data-koorosh-thesis/mart/ --recursive --human-readable
```

**Expected structure:**
```
s3://weather-data-koorosh-thesis/
├── raw/
│   ├── amsterdam_hourly_2026-02-13_2026-02-14_abc123.parquet
│   ├── new_york_hourly_2026-02-13_2026-02-14_abc123.parquet
│   └── ...
├── staging/
│   └── weather_hourly.parquet
└── mart/
    ├── weather_daily.parquet
    └── weather_anomalies.parquet
```

### 6. Connect MotherDuck to S3

1. **Go to MotherDuck:** https://app.motherduck.com

2. **Create S3 Secret:**
   - Click "Create a table from cloud storage"
   - Secret Name: `weather-data-koorosh-thesis`
   - Type: Amazon S3
   - Access Key ID: `AKIARNFXKIB3GC52PXPT`
   - Secret Access Key: (your secret)
   - Region: `us-east-1`
   - Click "Create secret"

3. **Query your data:**
   ```sql
   -- Query raw data
   SELECT * FROM 's3://weather-data-koorosh-thesis/raw/*.parquet' LIMIT 100;
   
   -- Query daily aggregations (transformed by dbt)
   SELECT * FROM 's3://weather-data-koorosh-thesis/mart/weather_daily.parquet' 
   ORDER BY date DESC LIMIT 100;
   
   -- Query anomalies
   SELECT * FROM 's3://weather-data-koorosh-thesis/mart/weather_anomalies.parquet'
   WHERE is_anomaly = true
   ORDER BY date DESC;
   ```

---

## Daily Operations

### Schedule

- **Airflow DAG runs daily at 2 AM UTC** (automatically)
- **Fetches weather data** for previous day
- **Uploads to S3** raw layer
- **Triggers GitHub Actions** for transformation
- **GitHub Actions transforms** and writes to mart layer

### Monitoring

**Check Airflow:**
```bash
# On EC2
docker-compose -f docker-compose.t3micro.yml logs -f airflow-scheduler
```

**Check GitHub Actions:**
- https://github.com/kooroshkz/adaptive-data-profiling-etl/actions

**Check S3 costs:**
- AWS Console → S3 → Metrics

### Troubleshooting

**If Airflow DAG fails:**
```bash
# Check logs
docker-compose -f docker-compose.t3micro.yml logs airflow-webserver
docker-compose -f docker-compose.t3micro.yml logs airflow-scheduler

# Check environment variables
docker exec airflow-airflow-scheduler-1 env | grep GITHUB_TOKEN
```

**If GitHub Actions workflow fails:**
- Check: https://github.com/kooroshkz/adaptive-data-profiling-etl/actions
- Common issues:
  - Missing secrets (AWS credentials)
  - S3 permissions
  - No raw data in S3 (Airflow didn't upload)

**If MotherDuck can't read S3:**
- Verify AWS credentials
- Check S3 bucket permissions
- Ensure files exist: `aws s3 ls s3://weather-data-koorosh-thesis/mart/`

---

## Cost Breakdown

| Service | Cost | Usage |
|---------|------|-------|
| EC2 t3.micro | ~$8/month | 24/7 Airflow orchestration |
| S3 Storage | ~$0.50/month | ~20GB weather data |
| S3 Requests | ~$0.10/month | Daily uploads + reads |
| GitHub Actions | $0 | <2000 min/month (free tier) |
| MotherDuck | $0 | Query only (no compute) |
| **Total** | **~$9/month** | |

---

## Next Steps

1. ✅ **Current:** Manual trigger test
2. ⏳ **Next:** Wait for 2 AM UTC for automatic run
3. ⏳ **Future:** Add more cities, increase frequency
4. ⏳ **Future:** Build MotherDuck dashboards
5. ⏳ **Future:** Add data quality alerts

---

## Architecture Decisions

### Why GitHub Actions for dbt?

- ✅ Free compute (2000 min/month)
- ✅ 8GB RAM (vs. 1.9GB on t3.micro)
- ✅ Offloads heavy transformations from EC2
- ✅ Built-in CI/CD integration
- ✅ Easy to scale if needed

### Why Not Run dbt on EC2?

- ❌ t3.micro only has 1.9GB RAM
- ❌ DuckDB transformations memory-intensive
- ❌ Would compete with Airflow for resources
- ❌ Risk of OOM kills (already had this issue)

### Why MotherDuck Instead of DuckDB?

- ✅ Cloud-hosted (no local setup)
- ✅ Built-in UI for querying/viz
- ✅ Reads S3 directly (no data duplication)
- ✅ Collaboration features (share queries)
- ✅ Free for query-only use case

---

## File Structure

```
adaptive-data-profiling-etl/
├── .github/workflows/
│   ├── daily-ingestion.yml       # OLD (now deprecated)
│   └── dbt-transform.yml          # NEW (transformation pipeline)
├── airflow/
│   ├── dags/
│   │   └── weather_dag.py         # Orchestration + GitHub trigger
│   ├── docker-compose.t3micro.yml # EC2 deployment config
│   └── .env                       # Environment variables + GITHUB_TOKEN
├── weather_dbt/
│   ├── models/
│   │   ├── staging/               # Staging models
│   │   ├── mart/                  # Analytics models
│   │   └── sources/               # S3 source definitions
│   └── dbt_project.yml
└── docs/
    └── LAKEHOUSE_SETUP.md         # This file
```
