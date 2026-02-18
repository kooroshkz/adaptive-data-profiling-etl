"""
Weather Data Ingestion DAG
Runs daily at 2 AM UTC to fetch weather data for 5 cities
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

# Upload to S3
def upload_task():
    """Upload parquet files to S3"""
    upload_parquet_to_s3('/opt/airflow/data/raw', 'raw')

upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_task,
    dag=dag,
)

# Trigger GitHub Actions for dbt transformations and wait for completion
def trigger_dbt():
    """Trigger dbt transformation workflow in GitHub Actions and wait for completion"""
    payload = {
        'triggered_by': 'airflow',
        'workflow': 'weather_ingestion'
    }
    success = trigger_github_workflow('trigger-dbt-transform', payload)
    if not success:
        raise Exception('Failed to trigger GitHub Actions workflow')
    
    # Wait for workflow to complete
    wait_for_github_workflow(
        workflow_name='dbt-transform.yml',
        timeout_minutes=15,
        poll_interval=15
    )

trigger_dbt_transform = PythonOperator(
    task_id='trigger_dbt_transform',
    python_callable=trigger_dbt,
    dag=dag,
)

# Refresh MotherDuck RAW tables
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

# Log completion
log_completion = BashOperator(
    task_id='log_completion',
    bash_command='''
    echo "Weather ingestion pipeline completed successfully at $(date)"
    echo "✓ Raw data uploaded to S3"
    echo "✓ MotherDuck raw tables refreshed"
    echo "✓ dbt transformations completed in GitHub Actions"
    echo "✓ MotherDuck MART tables refreshed"
    ls -lh /opt/airflow/data/raw/*.parquet | tail -10
    ''',
    dag=dag,
)

# Failure notification function
def send_failure_notification(**context):
    """Send email notification on task failure"""
    subject, body = build_failure_email(context)
    if subject and body:
        send_email_notification(subject, body)
    else:
        print('No failed tasks found')

# Failure notification task
notify_failure = PythonOperator(
    task_id='notify_failure',
    python_callable=send_failure_notification,
    trigger_rule=TriggerRule.ONE_FAILED,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
install_deps >> ingestion_tasks >> upload_to_s3 >> refresh_motherduck_raw >> trigger_dbt_transform >> log_completion
[log_completion, install_deps, upload_to_s3, refresh_motherduck_raw, trigger_dbt_transform] >> notify_failure
