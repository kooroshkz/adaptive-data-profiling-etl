# GitHub Actions + dbt Setup Guide

Your Airflow DAG successfully ingests data to S3. Now complete the GitHub Actions integration for dbt transformations.

## Architecture Flow
```
Airflow (EC2) → S3 raw/ → GitHub Actions (dbt) → S3 staging/ + mart/
```

---

## Step 1: Add GitHub Repository Secrets

Go to: **https://github.com/kooroshkz/adaptive-data-profiling-etl/settings/secrets/actions**

Click **"New repository secret"** and add these 4 secrets:

### 1. AWS_ACCESS_KEY_ID
```
Your AWS access key (same as EC2 .env)
```

### 2. AWS_SECRET_ACCESS_KEY
```
Your AWS secret key (same as EC2 .env)
```

### 3. AWS_REGION
```
us-east-1
(or your S3 bucket region)
```

### 4. S3_BUCKET
```
weather-data-koorosh-thesis
```

---

## Step 2: Create GitHub Personal Access Token

This allows Airflow to trigger GitHub Actions via webhook.

### Create Token:
1. Go to: **https://github.com/settings/tokens?type=beta**
2. Click **"Generate new token"** (fine-grained)
3. Fill in:
   - **Token name**: `Airflow Workflow Trigger`
   - **Expiration**: `90 days` (or custom)
   - **Repository access**: `Only select repositories` → `adaptive-data-profiling-etl`
   - **Permissions**:
     - Repository permissions:
       - **Actions**: `Read and write` ✅
       - **Contents**: `Read-only`
       - **Metadata**: `Read-only` (automatically selected)

4. Click **"Generate token"**
5. Copy the token (starts with `github_pat_...`)

---

## Step 3: Add GITHUB_TOKEN to EC2

SSH to EC2 and edit the `.env` file:

```bash
ssh airflow
cd ~/adaptive-data-profiling-etl/airflow
nano .env
```

Add these lines at the bottom:

```bash
# GitHub Actions webhook
GITHUB_TOKEN=github_pat_YOUR_TOKEN_HERE
GITHUB_REPO_OWNER=kooroshkz
GITHUB_REPO_NAME=adaptive-data-profiling-etl
```

**Save** (Ctrl+O, Enter, Ctrl+X)

Restart Airflow to load new env vars:

```bash
docker-compose -f docker-compose.t3micro.yml restart
```

---

## Step 4: Test Webhook Trigger (Local)

Test locally first to verify the setup:

```bash
cd /Users/kooroshkz/Desktop/adaptive-data-profiling-etl

# Set environment variables
export GITHUB_TOKEN="github_pat_YOUR_TOKEN"
export GITHUB_REPO_OWNER="kooroshkz"
export GITHUB_REPO_NAME="adaptive-data-profiling-etl"

# Test webhook call
python3 << 'EOF'
import requests
import os

github_token = os.getenv('GITHUB_TOKEN')
repo_owner = os.getenv('GITHUB_REPO_OWNER')
repo_name = os.getenv('GITHUB_REPO_NAME')

url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/dispatches'
headers = {
    'Authorization': f'token {github_token}',
    'Accept': 'application/vnd.github.v3+json'
}
payload = {
    'event_type': 'trigger-dbt-transform',
    'client_payload': {
        'triggered_by': 'test',
        'workflow': 'manual_test'
    }
}

response = requests.post(url, headers=headers, json=payload)
print(f"Status: {response.status_code}")
if response.status_code == 204:
    print("✅ Successfully triggered workflow!")
    print("Check: https://github.com/kooroshkz/adaptive-data-profiling-etl/actions")
else:
    print(f"❌ Failed: {response.text}")
EOF
```

**Expected**: Status 204, then check GitHub Actions tab for running workflow.

---

## Step 5: Test End-to-End on EC2

Once secrets are set up:

```bash
ssh airflow
cd ~/adaptive-data-profiling-etl/airflow

# Trigger full DAG (ingestion + dbt)
docker exec airflow-airflow-scheduler-1 airflow dags trigger weather_ingestion

# Monitor Airflow logs
docker-compose -f docker-compose.t3micro.yml logs -f airflow-scheduler
```

**Expected flow**:
1. ✅ Airflow ingests 5 cities → S3 raw/
2. ✅ `trigger_dbt_transform` task calls GitHub webhook → Status 204
3. ✅ GitHub Actions starts `dbt-transform.yml` workflow
4. ✅ dbt reads S3 raw/, transforms, writes to S3 staging/ + mart/
5. ✅ Check GitHub Actions: https://github.com/kooroshkz/adaptive-data-profiling-etl/actions

---

## Step 6: Verify transformed data in S3

After GitHub Actions completes:

```bash
aws s3 ls s3://weather-data-koorosh-thesis/raw/ --human-readable
aws s3 ls s3://weather-data-koorosh-thesis/staging/ --human-readable
aws s3 ls s3://weather-data-koorosh-thesis/mart/ --human-readable
```

**Expected files**:
- `raw/`: amsterdam_hourly_*.parquet, new_york_hourly_*.parquet, etc. (5 cities × 2 files = 10 files)
- `staging/`: weather_hourly.parquet
- `mart/`: weather_daily.parquet, weather_anomalies.parquet

---

## Troubleshooting

### Webhook returns 401 Unauthorized
- GitHub token expired or wrong permissions
- Regenerate token with **Actions: Read and write**

### Webhook returns 404 Not Found
- Repository name typo in .env
- Check `GITHUB_REPO_OWNER` and `GITHUB_REPO_NAME`

### GitHub Actions fails with S3 access errors
- GitHub secrets not set correctly
- Verify AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET

### GitHub Actions can't find raw data
- Airflow ingestion didn't upload to S3
- Check S3 bucket: `aws s3 ls s3://weather-data-koorosh-thesis/raw/`

---

## Quick Reference Commands

```bash
# Check if GITHUB_TOKEN is set on EC2
docker exec airflow-airflow-scheduler-1 env | grep GITHUB

# Manually trigger GitHub Actions (from local machine)
curl -X POST \
  -H "Authorization: token $GITHUB_TOKEN" \
  -H "Accept: application/vnd.github.v3+json" \
  https://api.github.com/repos/kooroshkz/adaptive-data-profiling-etl/dispatches \
  -d '{"event_type":"trigger-dbt-transform"}'

# Watch GitHub Actions logs
# Go to: https://github.com/kooroshkz/adaptive-data-profiling-etl/actions

# Check S3 data
aws s3 ls s3://weather-data-koorosh-thesis/ --recursive --human-readable | tail -20
```

---

## Next Steps

After successful end-to-end test:

1. ✅ Connect MotherDuck to S3 for querying
2. ✅ Set up systemd auto-start on EC2
3. ✅ Schedule daily 2 AM UTC runs
4. ✅ Monitor for 24-48 hours

🎉 **Your lakehouse pipeline will be complete!**
