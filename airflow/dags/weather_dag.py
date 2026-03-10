"""
Weather Data Ingestion DAG
Runs daily at 2 AM UTC to fetch weather data for 5 cities
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from dag_utils import (
    send_email_notification,
    build_failure_email,
    trigger_github_workflow,
    wait_for_github_workflow,
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

# Weather ingestion tasks for each city with output tracking
# Priority weights stagger task starts (higher priority = starts first)
ingestion_tasks = []
for idx, city in enumerate(CITIES):
    task = BashOperator(
        task_id=f'ingest_{city}',
        bash_command=f'''
        set -e  # Exit immediately if command fails
        sleep 1  # 1-second delay to rate-limit starts
        cd /opt/airflow/scripts
        OUTPUT=$(python weather_ingest.py --city {city} --mode incremental 2>&1)
        EXIT_CODE=$?
        echo "$OUTPUT"
        
        # Check for failure first (429 rate limit, API errors, etc.)
        if [ $EXIT_CODE -ne 0 ]; then
            echo "FAILED"
            exit $EXIT_CODE
        fi
        
        # Push result to XCom (check if skipped)
        if echo "$OUTPUT" | grep -q "Data already exists"; then
            echo "SKIPPED"
        else
            echo "NEW_DATA"
        fi
        ''',
        priority_weight=10 - idx,  # amsterdam=10, new_york=9, london=8, paris=7, tokyo=6
        do_xcom_push=True,
        dag=dag,
    )
    ingestion_tasks.append(task)

# Check if any new data was ingested by checking XCom outputs
def check_new_data(**context):
    """Check if any ingestion task created new data and decide next steps"""
    ti = context['ti']
    
    # Get results from all ingestion tasks
    cities = ['amsterdam', 'new_york', 'london', 'paris', 'tokyo']
    results = []
    
    for city in cities:
        task_output = ti.xcom_pull(task_ids=f'ingest_{city}')
        if task_output:
            # Get last line which should be SKIPPED or NEW_DATA
            last_line = task_output.strip().split('\n')[-1]
            results.append((city, last_line))
            print(f"   {city}: {last_line}")
    
    # Check if any city has NEW_DATA
    new_data_count = sum(1 for _, result in results if 'NEW_DATA' in result)
    skipped_count = sum(1 for _, result in results if 'SKIPPED' in result)
    
    print(f"\nSummary: {new_data_count} cities with new data, {skipped_count} cities skipped")
    
    if new_data_count > 0:
        print(f"✓ Proceeding with upload and refresh pipeline")
        return 'upload_to_s3'
    else:
        print(f"⊘ All data already existed - skipping downstream tasks")
        return 'skip_downstream'

check_for_new_data = BranchPythonOperator(
    task_id='check_for_new_data',
    python_callable=check_new_data,
    provide_context=True,
    dag=dag,
)

# Dummy operator for skip path
skip_downstream = DummyOperator(
    task_id='skip_downstream',
    dag=dag,
)

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

# Log completion
log_completion = BashOperator(
    task_id='log_completion',
    bash_command='''
    echo "Weather ingestion pipeline completed successfully at $(date)"
    echo "✓ Raw data uploaded to S3"
    echo "✓ dbt transformations completed in GitHub Actions"
    echo "✓ MotherDuck VIEWs query S3 directly (always fresh)"
    ls -lh /opt/airflow/data/raw/*.parquet | tail -10
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
# Main path: install -> ingest -> check -> [upload -> dbt -> complete] OR [skip -> complete]
install_deps >> ingestion_tasks >> check_for_new_data
check_for_new_data >> upload_to_s3 >> trigger_dbt_transform >> log_completion
check_for_new_data >> skip_downstream >> log_completion
[log_completion, install_deps, upload_to_s3, trigger_dbt_transform] >> notify_failure
