"""
Weather Data Ingestion DAG
Runs daily at 2 AM UTC to fetch weather data for 5 cities
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

default_args = {
    'owner': 'koorosh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 2, 1),
}

dag = DAG(
    'weather_ingestion',
    default_args=default_args,
    description='Daily weather data ingestion for 5 cities',
    schedule_interval='0 2 * * *',  # 2 AM UTC daily
    catchup=False,
    max_active_tasks=2,  # Allow 2 parallel tasks max to avoid overwhelming t3.micro
    max_active_runs=1,
    tags=['weather', 'etl', 'daily'],
)

# Cities to ingest
CITIES = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']

# Install Python dependencies (only needs to run once, but safe to repeat)
install_deps = BashOperator(
    task_id='install_dependencies',
    bash_command='''
    pip install pandas pyarrow requests boto3 duckdb --quiet
    ''',
    dag=dag,
)

# Weather ingestion tasks for each city
# Priority weights stagger task starts (higher priority = starts first)
ingestion_tasks = []
for idx, city in enumerate(CITIES):
    task = BashOperator(
        task_id=f'ingest_{city}',
        bash_command=f'''
        sleep 1  # 1-second delay to rate-limit starts
        cd /opt/airflow/scripts
        python weather_ingest.py --city {city} --mode incremental
        ''',
        priority_weight=10 - idx,  # amsterdam=10, new_york=9, london=8, paris=7, tokyo=6
        dag=dag,
    )
    ingestion_tasks.append(task)

# Upload to S3 using Python boto3
upload_to_s3 = BashOperator(
    task_id='upload_to_s3',
    bash_command='''
    python3 << 'EOF'
import boto3
import os
from pathlib import Path

s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
s3_client = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'eu-west-1')
)

data_dir = Path('/opt/airflow/data/raw')
uploaded = 0

if data_dir.exists():
    # Upload maintaining Hive partition structure: city=amsterdam/*.parquet
    for city_partition in data_dir.glob('city=*'):
        if city_partition.is_dir():
            for parquet_file in city_partition.glob('*.parquet'):
                # Preserve directory structure: raw/city=amsterdam/hourly_xxx.parquet
                s3_key = f'raw/{city_partition.name}/{parquet_file.name}'
                print(f'Uploading {city_partition.name}/{parquet_file.name} to s3://{s3_bucket}/{s3_key}')
                s3_client.upload_file(str(parquet_file), s3_bucket, s3_key)
                uploaded += 1
    print(f'✅ Uploaded {uploaded} partitioned files to S3')
else:
    print(f'⚠️  Data directory {data_dir} does not exist')
EOF
    ''',
    dag=dag,
)

# Trigger GitHub Actions for dbt transformations
trigger_dbt_transform = BashOperator(
    task_id='trigger_dbt_transform',
    bash_command='''
    python3 << 'EOF'
import requests
import os

# GitHub repository details
github_token = os.getenv('GITHUB_TOKEN')
repo_owner = os.getenv('GITHUB_REPO_OWNER', 'kooroshkz')
repo_name = os.getenv('GITHUB_REPO_NAME', 'adaptive-data-profiling-etl')

if not github_token:
    print('⚠️  GITHUB_TOKEN not set, skipping transformation trigger')
    exit(0)

# Trigger via repository_dispatch
url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/dispatches'
headers = {
    'Authorization': f'token {github_token}',
    'Accept': 'application/vnd.github.v3+json'
}
payload = {
    'event_type': 'trigger-dbt-transform',
    'client_payload': {
        'triggered_by': 'airflow',
        'workflow': 'weather_ingestion'
    }
}

response = requests.post(url, headers=headers, json=payload)

if response.status_code == 204:
    print('✅ Successfully triggered dbt transformation workflow')
else:
    print(f'❌ Failed to trigger workflow: {response.status_code}')
    print(response.text)
    exit(1)
EOF
    ''',
    dag=dag,
)

# Wait for GitHub Actions to complete
wait_for_github_actions = BashOperator(
    task_id='wait_for_github_actions',
    bash_command='''
    python3 << 'EOF'
import requests
import os
import time

github_token = os.getenv('GITHUB_TOKEN')
repo_owner = os.getenv('GITHUB_REPO_OWNER', 'kooroshkz')
repo_name = os.getenv('GITHUB_REPO_NAME', 'adaptive-data-profiling-etl')

if not github_token:
    print('⚠️  GITHUB_TOKEN not set, skipping GitHub Actions wait')
    exit(0)

print('⏳ Waiting for GitHub Actions dbt workflow to complete...')

# Poll GitHub Actions API
url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/actions/runs'
headers = {
    'Authorization': f'token {github_token}',
    'Accept': 'application/vnd.github.v3+json'
}

max_wait_time = 600  # 10 minutes max
poll_interval = 15   # Check every 15 seconds
elapsed = 0

while elapsed < max_wait_time:
    response = requests.get(url, headers=headers, params={'per_page': 5})
    if response.status_code == 200:
        runs = response.json().get('workflow_runs', [])
        # Find the most recent dbt workflow
        dbt_runs = [r for r in runs if 'dbt' in r.get('name', '').lower()]
        if dbt_runs:
            latest_run = dbt_runs[0]
            status = latest_run.get('status')
            conclusion = latest_run.get('conclusion')
            
            print(f'📊 Status: {status}, Conclusion: {conclusion}')
            
            if status == 'completed':
                if conclusion == 'success':
                    print('✅ GitHub Actions dbt workflow completed successfully!')
                    exit(0)
                else:
                    print(f'❌ GitHub Actions workflow completed with status: {conclusion}')
                    exit(1)
    
    time.sleep(poll_interval)
    elapsed += poll_interval
    print(f'⏳ Still waiting... ({elapsed}s/{max_wait_time}s)')

print('⚠️  Timeout waiting for GitHub Actions (10 minutes)')
exit(0)  # Don't fail the DAG, just skip MotherDuck refresh
EOF
    ''',
    dag=dag,
)

# Refresh MotherDuck RAW tables (after S3 upload, before dbt)
refresh_motherduck_raw = BashOperator(
    task_id='refresh_motherduck_raw',
    bash_command='''
    python3 << 'EOF'
import duckdb
import os

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
aws_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION', 'us-east-1')

if not motherduck_token:
    print('⚠️  MOTHERDUCK_TOKEN not set, skipping MotherDuck raw refresh')
    print('💡 Set MOTHERDUCK_TOKEN in .env to enable auto-refresh')
    exit(0)

print('🦆 Connecting to MotherDuck...')

# Connect to MotherDuck
con = duckdb.connect(f'md:?motherduck_token={motherduck_token}')

# Set AWS credentials
con.execute(f"SET s3_access_key_id='{aws_key}';")
con.execute(f"SET s3_secret_access_key='{aws_secret}';")
con.execute(f"SET s3_region='{aws_region}';")

print('🔄 Refreshing RAW weather data tables...')

cities = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']

# Refresh raw_weather_data tables
con.execute('CREATE DATABASE IF NOT EXISTS raw_weather_data;')
con.execute('USE raw_weather_data;')

for city in cities:
    sql = f"""
    CREATE OR REPLACE TABLE {city} AS 
    SELECT * FROM read_parquet(
        's3://{s3_bucket}/raw/city={city}/hourly_*.parquet', 
        hive_partitioning=true
    );
    """
    con.execute(sql)
    count = con.execute(f'SELECT COUNT(*) FROM {city};').fetchone()[0]
    print(f'   ✓ raw_weather_data.{city}: {count} rows')

con.close()
print('✅ MotherDuck RAW tables refreshed successfully!')
print('📊 Next: Triggering dbt transformations...')
EOF
    ''',
    dag=dag,
)

# Refresh MotherDuck MART tables (after GitHub Actions dbt completes)
refresh_motherduck_mart = BashOperator(
    task_id='refresh_motherduck_mart',
    bash_command='''
    python3 << 'EOF'
import duckdb
import os

motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
aws_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION', 'us-east-1')

if not motherduck_token:
    print('⚠️  MOTHERDUCK_TOKEN not set, skipping MotherDuck mart refresh')
    print('💡 Set MOTHERDUCK_TOKEN in .env to enable auto-refresh')
    exit(0)

print('🦆 Connecting to MotherDuck...')

# Connect to MotherDuck
con = duckdb.connect(f'md:?motherduck_token={motherduck_token}')

# Set AWS credentials
con.execute(f"SET s3_access_key_id='{aws_key}';")
con.execute(f"SET s3_secret_access_key='{aws_secret}';")
con.execute(f"SET s3_region='{aws_region}';")

print('🔄 Refreshing MART (transformed) tables...')

cities = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']

# Refresh weather_data tables (daily aggregations)
con.execute('CREATE DATABASE IF NOT EXISTS weather_data;')
con.execute('USE weather_data;')

for city in cities:
    sql = f"""
    CREATE OR REPLACE TABLE {city} AS 
    SELECT * FROM read_parquet(
        's3://{s3_bucket}/mart/daily/city={city}/weather_daily.parquet', 
        hive_partitioning=true
    );
    """
    con.execute(sql)
    count = con.execute(f'SELECT COUNT(*) FROM {city};').fetchone()[0]
    print(f'   ✓ weather_data.{city}: {count} rows')

# Refresh weather_anomalies tables
con.execute('CREATE DATABASE IF NOT EXISTS weather_anomalies;')
con.execute('USE weather_anomalies;')

for city in cities:
    sql = f"""
    CREATE OR REPLACE TABLE {city} AS 
    SELECT * FROM read_parquet(
        's3://{s3_bucket}/mart/anomalies/city={city}/weather_anomalies.parquet', 
        hive_partitioning=true
    );
    """
    con.execute(sql)
    count = con.execute(f'SELECT COUNT(*) FROM {city};').fetchone()[0]
    print(f'   ✓ weather_anomalies.{city}: {count} rows')

con.close()
print('✅ MotherDuck MART tables refreshed successfully!')
EOF
    ''',
    dag=dag,
)

# Log completion
log_completion = BashOperator(
    task_id='log_completion',
    bash_command='''
    echo "Weather ingestion completed at $(date)"
    echo "Files uploaded to S3, dbt transformation triggered"
    ls -lh /opt/airflow/data/raw/*.parquet | tail -10
    ''',
    dag=dag,
)

# Define task dependencies
install_deps >> ingestion_tasks >> upload_to_s3 >> refresh_motherduck_raw >> trigger_dbt_transform >> wait_for_github_actions >> refresh_motherduck_mart >> log_completion
