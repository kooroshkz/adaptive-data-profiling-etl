"""
Weather Data Backfill DAG
Manual trigger to backfill historical data from 2024-01-01 to yesterday
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from dag_utils import (
    send_email_notification,
    build_failure_email,
    trigger_github_workflow,
    wait_for_github_workflow
)

default_args = {
    'owner': 'koorosh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2026, 1, 1),
}

dag = DAG(
    'weather_backfill',
    default_args=default_args,
    description='Manual backfill: Historical weather data from 2024-01-01 to yesterday',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_tasks=1,  # Process cities sequentially to avoid API rate limits
    max_active_runs=1,
    tags=['weather', 'etl', 'backfill', 'manual'],
)

# Cities to backfill
CITIES = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']

# Calculate date range: 2024-01-01 to yesterday
BACKFILL_START = "2024-01-01"
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
SYNTHETIC_ANOMALY_RATE = 0.01
SYNTHETIC_ANOMALY_SHIFT_PCT_MEAN = 0.10
SYNTHETIC_PER_COLUMN_PROB = 0.35

# Clean old parquet files from both local and S3 to prevent duplicates
def clean_s3_and_local_data():
    """Clean old parquet files from S3 and local storage"""
    import boto3
    import os
    import shutil
    
    s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
    cities = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']
    
    print("=" * 50)
    print("CLEANING OLD DATA TO PREVENT DUPLICATES")
    print("=" * 50)
    
    # Clean S3 files
    try:
        s3 = boto3.client('s3')
        print(f"\n🗑️  Cleaning S3 bucket: {s3_bucket}")
        
        for city in cities:
            prefix = f'raw/city={city}/'
            print(f"   Listing objects in {prefix}...")

            paginator = s3.get_paginator('list_objects_v2')
            deleted_count = 0
            for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
                objects_to_delete = [{'Key': obj['Key']} for obj in page.get('Contents', [])]
                if not objects_to_delete:
                    continue

                for i in range(0, len(objects_to_delete), 1000):
                    chunk = objects_to_delete[i:i + 1000]
                    s3.delete_objects(Bucket=s3_bucket, Delete={'Objects': chunk})
                    deleted_count += len(chunk)

            if deleted_count:
                print(f"   ✓ Deleted {deleted_count} files from S3")
            else:
                print(f"   (no files found)")
        
        print(f"✓ S3 cleanup completed")
    except Exception as e:
        print(f"⚠️  S3 cleanup failed: {e}")
        print("Continuing with local cleanup...")
    
    # Clean local files
    print(f"\n🗑️  Cleaning local storage...")
    local_raw = '/opt/airflow/data/raw'
    if os.path.exists(local_raw):
        for city in cities:
            city_dir = os.path.join(local_raw, f'city={city}')
            if os.path.exists(city_dir):
                files = os.listdir(city_dir)
                if files:
                    print(f"   Removing {len(files)} files from {city}...")
                    shutil.rmtree(city_dir)
                    os.makedirs(city_dir)
                    print(f"   ✓ Cleaned {city}")
    
    print(f"✓ Local cleanup completed")
    print("=" * 50)

clean_old_data = PythonOperator(
    task_id='clean_old_data',
    python_callable=clean_s3_and_local_data,
    dag=dag,
)

# Install Python dependencies
install_deps = BashOperator(
    task_id='install_dependencies',
    bash_command='''
    pip install pandas pyarrow requests boto3 duckdb --quiet
    ''',
    dag=dag,
)

# Log backfill info
log_backfill_info = BashOperator(
    task_id='log_backfill_info',
    bash_command=f'''
    echo "========================================"
    echo "WEATHER DATA BACKFILL"
    echo "========================================"
    echo "Start Date: {BACKFILL_START}"
    echo "End Date: {yesterday}"
    echo "Cities: {', '.join(CITIES)}"
    echo "Synthetic anomalies: enabled"
    echo "Synthetic anomaly rate: {SYNTHETIC_ANOMALY_RATE}"
    echo "Synthetic anomaly target mean shift: {SYNTHETIC_ANOMALY_SHIFT_PCT_MEAN}"
    echo "Synthetic per-column mutation probability: {SYNTHETIC_PER_COLUMN_PROB}"
    echo "========================================"
    ''',
    dag=dag,
)

# Backfill ingestion tasks for each city
# Sequential processing to avoid overwhelming the API
backfill_tasks = []
for idx, city in enumerate(CITIES):
    # Add 5-minute delay before each city (except first) to avoid rate limiting
    delay_command = "" if idx == 0 else "echo 'Waiting 5 minutes to avoid rate limit...'; sleep 300; "
    
    task = BashOperator(
        task_id=f'backfill_{city}',
        bash_command=f'''
        set -e  # Exit immediately if any command fails
        {delay_command}
        echo "Starting backfill for {city}..."
        cd /opt/airflow/scripts
                python weather_ingest.py \
                    --city {city} \
                    --mode custom \
                    --start-date {BACKFILL_START} \
                    --end-date {yesterday} \
                    --inject-synthetic-anomalies \
                    --anomaly-rate {SYNTHETIC_ANOMALY_RATE} \
                    --anomaly-shift-pct-mean {SYNTHETIC_ANOMALY_SHIFT_PCT_MEAN} \
                    --per-column-anomaly-prob {SYNTHETIC_PER_COLUMN_PROB}
        EXIT_CODE=$?
        if [ $EXIT_CODE -ne 0 ]; then
            echo "Backfill FAILED for {city} (exit code: $EXIT_CODE)"
            exit $EXIT_CODE
        fi
        echo "✓ Completed backfill for {city}"
        ''',
        dag=dag,
    )
    backfill_tasks.append(task)


# Trigger dbt transformations (reuses same function as daily job)
def trigger_dbt():
    """Trigger dbt transformation workflow in GitHub Actions and wait for completion"""
    payload = {
        'triggered_by': 'airflow_backfill',
        'workflow': 'weather_backfill'
    }
    success = trigger_github_workflow('trigger-dbt-transform', payload)
    if not success:
        raise Exception('Failed to trigger GitHub Actions workflow')
    
    # Wait for workflow to complete
    wait_for_github_workflow(
        workflow_name='dbt-transform.yml',
        timeout_minutes=30,  # Longer timeout for backfill
        poll_interval=20
    )

trigger_dbt_transform = PythonOperator(
    task_id='trigger_dbt_transform',
    python_callable=trigger_dbt,
    dag=dag,
)

# Log completion
log_completion = BashOperator(
    task_id='log_completion',
    bash_command=f'''
    echo "========================================"
    echo "BACKFILL COMPLETED SUCCESSFULLY"
    echo "========================================"
    echo "Date Range: {BACKFILL_START} to {yesterday}"
    echo "✓ Raw data uploaded to S3 per-city (real-time)"
    echo "✓ dbt transformations completed"
    echo "✓ MotherDuck VIEWs query S3 directly (always fresh)"
    echo "========================================"
    echo ""
    echo "Recent parquet files:"
    ls -lh /opt/airflow/data/raw/city=*/hourly_* | head -20
    ''',
    dag=dag,
)

# Failure notification (reuses same function as daily job)
def send_failure_notification(**context):
    """Send email notification on task failure"""
    subject, body = build_failure_email(context)
    if subject and body:
        send_email_notification(subject, body)
    else:
        print('No failed tasks found')

notify_failure = PythonOperator(
    task_id='notify_failure',
    python_callable=send_failure_notification,
    trigger_rule=TriggerRule.ONE_FAILED,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
# Process cities sequentially (chain them)
clean_old_data >> install_deps >> log_backfill_info >> backfill_tasks[0]

for i in range(len(backfill_tasks) - 1):
    backfill_tasks[i] >> backfill_tasks[i + 1]

# After all cities complete, trigger dbt transformations
backfill_tasks[-1] >> trigger_dbt_transform >> log_completion

# Failure notifications
[log_completion, clean_old_data, install_deps, trigger_dbt_transform] >> notify_failure
