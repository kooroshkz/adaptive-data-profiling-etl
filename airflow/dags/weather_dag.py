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
    tags=['weather', 'etl', 'daily'],
)

# Cities to ingest
CITIES = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']

# Install Python dependencies (only needs to run once, but safe to repeat)
install_deps = BashOperator(
    task_id='install_dependencies',
    bash_command='''
    pip install pandas pyarrow requests boto3 --quiet
    ''',
    dag=dag,
)

# Weather ingestion tasks for each city
ingestion_tasks = []
for city in CITIES:
    task = BashOperator(
        task_id=f'ingest_{city}',
        bash_command=f'''
        cd /opt/airflow/scripts
        python weather_ingest.py --city {city} --mode incremental
        ''',
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
    for parquet_file in data_dir.glob('*.parquet'):
        s3_key = f'raw/{parquet_file.name}'
        print(f'Uploading {parquet_file.name} to s3://{s3_bucket}/{s3_key}')
        s3_client.upload_file(str(parquet_file), s3_bucket, s3_key)
        uploaded += 1
    print(f'✅ Uploaded {uploaded} files to S3')
else:
    print(f'⚠️  Data directory {data_dir} does not exist')
EOF
    ''',
    dag=dag,
)

# Log completion
log_completion = BashOperator(
    task_id='log_completion',
    bash_command='''
    echo "Weather ingestion completed at $(date)"
    ls -lh /opt/airflow/data/raw/*.parquet | tail -10
    ''',
    dag=dag,
)

# Define task dependencies
install_deps >> ingestion_tasks >> upload_to_s3 >> log_completion
