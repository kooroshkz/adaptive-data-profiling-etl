"""
AutoML Training DAG
Manual-trigger DAG that trains per-column anomaly detection models for all
cities defined in adaptive_profiler/profiling_schema.yml and stores the
resulting artifacts on S3 at models/v1/city=<city>/col=<col>/latest.pkl.

Trigger with: airflow dags trigger weather_automl_train
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from dag_utils import build_failure_email, send_email_notification

default_args = {
    "owner": "koorosh",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
}

dag = DAG(
    "weather_automl_train",
    default_args=default_args,
    description="Manual-trigger: train AutoML anomaly models for all cities and save to S3",
    schedule_interval=None,  # manual trigger only
    catchup=False,
    tags=["weather", "automl", "training"],
)

install_deps = BashOperator(
    task_id="install_dependencies",
    bash_command="""
    pip install pyod optuna scikit-learn boto3 duckdb pyyaml pandas pyarrow --quiet
    """,
    dag=dag,
)

train_models = BashOperator(
    task_id="train_automl_models",
    bash_command="""
    set -euo pipefail
    cd /opt/airflow/scripts
    python automl_train.py
    """,
    execution_timeout=timedelta(hours=4),
    dag=dag,
)

log_completion = BashOperator(
    task_id="log_completion",
    bash_command="""
    echo "=================================================="
    echo "AutoML training completed at $(date)"
    echo "=================================================="
    echo "Models saved to S3: s3://$S3_BUCKET/models/v1/"
    echo "Each city/column: latest.pkl + latest_meta.json"
    echo "=================================================="
    """,
    dag=dag,
)


def send_failure_notification(**context):
    subject, body = build_failure_email(context)
    if subject and body:
        send_email_notification(subject, body)


notify_failure = PythonOperator(
    task_id="notify_failure",
    python_callable=send_failure_notification,
    trigger_rule=TriggerRule.ONE_FAILED,
    provide_context=True,
    dag=dag,
)

install_deps >> train_models >> log_completion
[install_deps, train_models, log_completion] >> notify_failure
