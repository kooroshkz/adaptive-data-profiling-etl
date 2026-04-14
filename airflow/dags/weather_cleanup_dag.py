"""
Weather Data Cleanup DAG
Manual fresh-start job that removes all weather parquet objects from S3 and local storage.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'koorosh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
}

dag = DAG(
    'weather_cleanup',
    default_args=default_args,
    description='Manual cleanup of all weather parquet objects and local staging data for a fresh start',
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    tags=['weather', 'etl', 'cleanup', 'manual'],
)


def clean_everything_for_fresh_start():
    """Delete all weather parquet objects from S3 and local raw/staging folders."""
    import os
    import shutil

    import boto3

    bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
    s3_prefixes = [
        'raw/',
        'staging/',
        'mart/',
    ]

    print('=' * 60)
    print('CLEANING WEATHER PIPELINE FOR FRESH START')
    print('=' * 60)

    s3 = boto3.client('s3')

    for prefix in s3_prefixes:
        print(f'Cleaning s3://{bucket}/{prefix}')
        paginator = s3.get_paginator('list_objects_v2')
        deleted = 0

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            contents = page.get('Contents', [])
            if not contents:
                continue

            objects = [{'Key': obj['Key']} for obj in contents]
            for start in range(0, len(objects), 1000):
                chunk = objects[start:start + 1000]
                s3.delete_objects(Bucket=bucket, Delete={'Objects': chunk})
                deleted += len(chunk)

        print(f'  deleted {deleted} objects under {prefix}')

    local_paths = [
        '/opt/airflow/data/raw',
        '/opt/airflow/data/staging',
    ]

    for local_path in local_paths:
        if os.path.exists(local_path):
            print(f'Cleaning local directory {local_path}')
            shutil.rmtree(local_path)
            os.makedirs(local_path, exist_ok=True)

    print('Fresh-start cleanup completed')
    print('=' * 60)


cleanup_task = PythonOperator(
    task_id='clean_everything_for_fresh_start',
    python_callable=clean_everything_for_fresh_start,
    dag=dag,
)
