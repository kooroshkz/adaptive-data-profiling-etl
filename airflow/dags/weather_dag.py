"""
Weather Data Ingestion DAG
Runs daily at 2 AM UTC to fetch weather data for 5 cities
"""

import random
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
    max_active_tasks=1,
    max_active_runs=1,
    tags=['weather', 'etl', 'daily'],
)

# Cities to ingest
CITIES = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']
DAILY_ANOMALY_CITY_COUNT = 2
DAILY_ANOMALY_ROWS_PER_SELECTED_CITY = 1

# Install Python dependencies (only needs to run once, but safe to repeat)
install_deps = BashOperator(
    task_id='install_dependencies',
    bash_command='''
    pip install adaptive-profiler --quiet
    pip install pandas pyarrow requests boto3 duckdb --quiet
    ''',
    dag=dag,
)


def select_daily_anomaly_cities():
    """Pick two random cities per DAG run for sparse anomaly injection."""
    selected = random.sample(CITIES, DAILY_ANOMALY_CITY_COUNT)
    print(f"Selected cities for daily sparse anomalies: {selected}")
    return selected


select_anomaly_cities = PythonOperator(
    task_id='select_daily_anomaly_cities',
    python_callable=select_daily_anomaly_cities,
    dag=dag,
)

# Weather ingestion tasks for each city
# Tasks run sequentially (max_active_tasks=1) to avoid API rate limiting
# Each task uses "smart" mode:
#   1. Queries S3 for latest data timestamp
#   2. Calculates gap (e.g., if last data is 2026-03-08, fetches 2026-03-09 to today)
#   3. Ingests and uploads to S3 immediately if gap exists
#   4. Skips if already up-to-date
ingestion_tasks = []
for idx, city in enumerate(CITIES):
    task = BashOperator(
        task_id=f'ingest_{city}',
        bash_command=f'''
        set -e  # Exit immediately if command fails
        cd /opt/airflow/scripts
        ANOMALY_CITIES='{{{{ ti.xcom_pull(task_ids="select_daily_anomaly_cities") | join(",") }}}}'
        EXTRA_ARGS=""
        if echo ",$ANOMALY_CITIES," | grep -q ",{city},"; then
            EXTRA_ARGS="--inject-synthetic-anomalies --fixed-anomaly-rows {DAILY_ANOMALY_ROWS_PER_SELECTED_CITY}"
            echo "Applying sparse daily anomalies for {city} with args: $EXTRA_ARGS"
        else
            echo "No synthetic anomalies for {city} in this run"
        fi

        python weather_ingest.py --city {city} --mode smart $EXTRA_ARGS
        ''',
        priority_weight=10 - idx,  # amsterdam=10, new_york=9, london=8, paris=7, tokyo=6
        dag=dag,
    )
    ingestion_tasks.append(task)

# Note: Upload to S3 happens automatically during ingestion (no separate task needed)

# Anomaly detection tasks — run after each city's ingestion.
# Loads trained models from S3 (s3://bucket/models/v1/city=<city>/col=<col>/latest.pkl)
# and scores today's data. Skips gracefully if no model has been trained yet.
anomaly_detection_tasks = []
for idx, city in enumerate(CITIES):
    detect_task = BashOperator(
        task_id=f"detect_anomalies_{city}",
        bash_command=f"""
        set -e
        cd /opt/airflow/scripts
        RUN_DATE="{{{{ ds }}}}"
        python automl_predict.py --city {city} --date "$RUN_DATE"
        """,
        dag=dag,
    )
    anomaly_detection_tasks.append(detect_task)

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

# Log completion
log_completion = BashOperator(
    task_id='log_completion',
    bash_command='''
    echo "=========================================="
    echo "Weather ingestion completed at $(date)"
    echo "=========================================="
    echo "✓ Smart S3-aware ingestion (no duplicates, no gaps)"
    echo "✓ Sparse synthetic anomalies enabled (2 rows total across two cities per run)"
    echo "✓ Raw data uploaded to S3 per-city (real-time)"
    echo "✓ dbt transformations completed in GitHub Actions"
    echo "✓ MotherDuck VIEWs query S3 directly (always fresh)"
    echo "=========================================="
    ''',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # Run if either path succeeds
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
# ingest → detect → dbt → log
install_deps >> select_anomaly_cities >> ingestion_tasks
for ingest_task, detect_task in zip(ingestion_tasks, anomaly_detection_tasks):
    ingest_task >> detect_task
anomaly_detection_tasks >> trigger_dbt_transform >> log_completion
[log_completion, install_deps, select_anomaly_cities, trigger_dbt_transform] >> notify_failure
