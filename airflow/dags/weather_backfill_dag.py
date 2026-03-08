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
    wait_for_github_workflow,
    refresh_motherduck_tables,
    upload_parquet_to_s3
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
            
            response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                if objects_to_delete:
                    print(f"   Deleting {len(objects_to_delete)} objects from {city}...")
                    s3.delete_objects(
                        Bucket=s3_bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    print(f"   ✓ Deleted {len(objects_to_delete)} files from S3")
                else:
                    print(f"   (no files to delete)")
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
    echo "========================================"
    ''',
    dag=dag,
)

# Backfill ingestion tasks for each city
# Sequential processing to avoid overwhelming the API
backfill_tasks = []
for city in CITIES:
    task = BashOperator(
        task_id=f'backfill_{city}',
        bash_command=f'''
        echo "Starting backfill for {city}..."
        cd /opt/airflow/scripts
        python weather_ingest.py --city {city} --mode custom --start-date {BACKFILL_START} --end-date {yesterday}
        echo "✓ Completed backfill for {city}"
        ''',
        dag=dag,
    )
    backfill_tasks.append(task)

# Upload to S3 (reuses same function as daily job)
def upload_task():
    """Upload parquet files to S3"""
    upload_parquet_to_s3('/opt/airflow/data/raw', 'raw')

upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_task,
    dag=dag,
)

# Refresh MotherDuck RAW tables (reuses same function as daily job)
def refresh_raw_tables():
    """Refresh MotherDuck raw weather data tables from S3"""
    import os
    cities = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']
    s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
    
    def s3_pattern(city):
        return f's3://{s3_bucket}/raw/city={city}/hourly_*.parquet'
    
    refresh_motherduck_tables('raw_weather_data', cities, s3_pattern)

refresh_motherduck_raw = PythonOperator(
    task_id='refresh_motherduck_raw',
    python_callable=refresh_raw_tables,
    dag=dag,
)

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
    echo "✓ Raw data uploaded to S3"
    echo "✓ MotherDuck raw tables refreshed"
    echo "✓ dbt transformations completed"
    echo "✓ MotherDuck MART tables updated"
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

# After all cities complete, run the rest of the pipeline
backfill_tasks[-1] >> upload_to_s3 >> refresh_motherduck_raw >> trigger_dbt_transform >> log_completion

# Failure notifications
[log_completion, clean_old_data, install_deps, upload_to_s3, refresh_motherduck_raw, trigger_dbt_transform] >> notify_failure
