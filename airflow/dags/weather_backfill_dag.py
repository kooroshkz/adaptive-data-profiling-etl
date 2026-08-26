"""
Weather Data Backfill DAG
Manual trigger to backfill historical data from 2024-01-01 to yesterday.
Fully local: writes parquet locally and builds the DuckDB warehouse in-container.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from dag_utils import send_email_notification, build_failure_email

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
    description='Manual backfill: historical weather data from 2024-01-01 to yesterday (local)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_tasks=1,  # Process cities sequentially to avoid API rate limits
    max_active_runs=1,
    tags=['weather', 'etl', 'backfill', 'manual'],
)

CITIES = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']
BACKFILL_START = "2024-01-01"
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
SYNTHETIC_ANOMALY_RATE = 0.01
SYNTHETIC_ANOMALY_SHIFT_PCT_MEAN = 0.10
SYNTHETIC_PER_COLUMN_PROB = 0.35

# Note: no cleanup step. The transform deduplicates by (time, city_id), keeping
# the latest ingestion, so re-running the backfill is safe without deleting data.

install_deps = BashOperator(
    task_id='install_dependencies',
    bash_command='''
    pip install pandas pyarrow requests duckdb --quiet
    ''',
    dag=dag,
)

log_backfill_info = BashOperator(
    task_id='log_backfill_info',
    bash_command=f'''
    echo "========================================"
    echo "WEATHER DATA BACKFILL (local)"
    echo "========================================"
    echo "Start Date: {BACKFILL_START}"
    echo "End Date: {yesterday}"
    echo "Cities: {', '.join(CITIES)}"
    echo "Synthetic anomalies: enabled (rate={SYNTHETIC_ANOMALY_RATE})"
    echo "========================================"
    ''',
    dag=dag,
)

# Backfill ingestion tasks for each city (sequential to avoid API rate limits)
backfill_tasks = []
for idx, city in enumerate(CITIES):
    delay_command = "" if idx == 0 else "echo 'Waiting 60s to avoid rate limit...'; sleep 60; "
    task = BashOperator(
        task_id=f'backfill_{city}',
        bash_command=f'''
        set -e
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
        echo "Completed backfill for {city}"
        ''',
        dag=dag,
    )
    backfill_tasks.append(task)

# Transform stage: build the local DuckDB warehouse + marts (in-container).
run_transform = BashOperator(
    task_id='run_transform',
    bash_command='''
    set -e
    cd /opt/airflow/scripts
    python transform_local.py
    ''',
    dag=dag,
)

log_completion = BashOperator(
    task_id='log_completion',
    bash_command=f'''
    echo "========================================"
    echo "BACKFILL COMPLETED"
    echo "========================================"
    echo "Date Range: {BACKFILL_START} to {yesterday}"
    echo "✓ Raw data written locally per-city"
    echo "✓ Transform built the local DuckDB warehouse + marts"
    echo "========================================"
    ''',
    dag=dag,
)


def send_failure_notification(**context):
    """Send email notification on task failure (skips silently if not configured)."""
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

# Dependencies: install -> log -> ingest (sequential) -> transform -> log
install_deps >> log_backfill_info >> backfill_tasks[0]
for i in range(len(backfill_tasks) - 1):
    backfill_tasks[i] >> backfill_tasks[i + 1]
backfill_tasks[-1] >> run_transform >> log_completion
[log_completion, install_deps, run_transform] >> notify_failure
