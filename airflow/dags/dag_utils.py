"""Utility functions for Airflow DAGs"""

import os
import requests
import duckdb
import boto3
from pathlib import Path


def send_email_notification(subject, body, to_email=None):
    """Send email via Brevo API"""
    api_key = os.getenv('BREVO_API_KEY')
    alert_email = to_email or os.getenv('BREVO_ALERT_EMAIL')
    sender_email = os.getenv('BREVO_SENDER_EMAIL')
    sender_name = os.getenv('BREVO_SENDER_NAME', 'Airflow Weather Pipeline')
    
    if not all([api_key, alert_email, sender_email]):
        print('Email credentials not set, skipping notification')
        print(f'API Key: {"set" if api_key else "missing"}')
        print(f'Alert Email: {alert_email or "missing"}')
        print(f'Sender Email: {sender_email or "missing (MUST be verified in Brevo)"}')
        return False
    
    url = "https://api.brevo.com/v3/smtp/email"
    headers = {
        "accept": "application/json",
        "api-key": api_key,
        "content-type": "application/json"
    }
    
    # Convert plain text body to HTML
    html_body = body.replace('\n', '<br>')
    
    payload = {
        "sender": {
            "name": sender_name,
            "email": sender_email
        },
        "to": [{"email": alert_email}],
        "subject": subject,
        "htmlContent": f"<html><body><pre>{html_body}</pre></body></html>",
        "textContent": body
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code in [200, 201]:
            result = response.json()
            print(f'Email sent to {alert_email} (ID: {result.get("messageId")})')
            return True
        else:
            print(f'Failed to send email: {response.status_code} - {response.text}')
            return False
    except Exception as e:
        print(f'Failed to send email: {e}')
        return False


def build_failure_email(context):
    """Build failure notification email content from Airflow context"""
    task_instances = context['dag_run'].get_task_instances()
    failed_tasks = [ti for ti in task_instances if ti.state == 'failed']
    
    if not failed_tasks:
        return None, None
    
    failed_info = []
    for ti in failed_tasks:
        failed_info.append(f"""
Task: {ti.task_id}
State: {ti.state}
Start: {ti.start_date}
End: {ti.end_date}
Duration: {ti.duration}s
Try: {ti.try_number}/{ti.max_tries}
Log: {ti.log_url}
        """)
    
    subject = f"Airflow DAG Failed: {context['dag'].dag_id}"
    body = f"""
Weather Ingestion Pipeline Failed

DAG: {context['dag'].dag_id}
Run ID: {context['dag_run'].run_id}
Execution Date: {context['execution_date']}

Failed Tasks ({len(failed_tasks)}):
{''.join(failed_info)}

Check Airflow UI: http://52.54.106.82:8080
    """
    
    return subject, body


def trigger_github_workflow(event_type, client_payload=None):
    """Trigger GitHub Actions workflow via repository_dispatch"""
    github_token = os.getenv('GITHUB_TOKEN')
    repo_owner = os.getenv('GITHUB_REPO_OWNER', 'kooroshkz')
    repo_name = os.getenv('GITHUB_REPO_NAME', 'adaptive-data-profiling-etl')
    
    if not github_token:
        print('GITHUB_TOKEN not set, skipping workflow trigger')
        return False
    
    url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/dispatches'
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    payload = {
        'event_type': event_type,
        'client_payload': client_payload or {}
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 204:
        print(f'Successfully triggered workflow: {event_type}')
        return True
    else:
        print(f'Failed to trigger workflow: {response.status_code}')
        print(response.text)
        return False


def wait_for_github_workflow(workflow_name='dbt-transform.yml', timeout_minutes=15, poll_interval=15):
    """
    Wait for GitHub Actions workflow to complete and check result
    """
    import time
    from datetime import datetime, timedelta
    
    github_token = os.getenv('GITHUB_TOKEN')
    repo_owner = os.getenv('GITHUB_REPO_OWNER', 'kooroshkz')
    repo_name = os.getenv('GITHUB_REPO_NAME', 'adaptive-data-profiling-etl')
    
    if not github_token:
        raise Exception('GITHUB_TOKEN not set')
    
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3+json'
    }
    
    # Get recent workflow runs
    url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_name}/runs'
    
    print(f'Waiting for GitHub Actions workflow: {workflow_name}')
    print(f'Timeout: {timeout_minutes} minutes, Poll interval: {poll_interval} seconds')
    
    start_time = datetime.now()
    timeout = timedelta(minutes=timeout_minutes)
    
    # Wait a few seconds for workflow to start
    print('Waiting 10 seconds for workflow to start...')
    time.sleep(10)
    
    run_id = None
    status = None
    conclusion = None
    
    while datetime.now() - start_time < timeout:
        response = requests.get(url, headers=headers, params={'per_page': 5})
        
        if response.status_code != 200:
            print(f'Failed to fetch workflow runs: {response.status_code}')
            time.sleep(poll_interval)
            continue
        
        runs = response.json().get('workflow_runs', [])
        
        if not runs:
            print('No workflow runs found yet, waiting...')
            time.sleep(poll_interval)
            continue
        
        # Get the most recent run
        latest_run = runs[0]
        run_id = latest_run['id']
        status = latest_run['status']
        conclusion = latest_run['conclusion']
        created_at = latest_run['created_at']
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f'[{int(elapsed)}s] Run #{run_id}: status={status}, conclusion={conclusion}')
        
        if status == 'completed':
            if conclusion == 'success':
                print(f'Workflow completed successfully!')
                print(f'Run URL: {latest_run["html_url"]}')
                return True
            else:
                error_msg = f'Workflow failed with conclusion: {conclusion}'
                print(error_msg)
                print(f'Run URL: {latest_run["html_url"]}')
                raise Exception(error_msg)
        
        # Still running
        time.sleep(poll_interval)
    
    # Timeout reached
    timeout_msg = f'Timeout: Workflow did not complete within {timeout_minutes} minutes'
    if run_id:
        timeout_msg += f' (Run #{run_id} status: {status})'
    print(f'{timeout_msg}')
    raise Exception(timeout_msg)


def refresh_motherduck_tables(database, tables, s3_pattern_fn):
    """
    Create/refresh MotherDuck VIEWs that query S3 directly (no caching).
    VIEWs always read fresh data from S3, no refresh needed.
    
    Args:
        database: MotherDuck database name
        tables: List of table names
        s3_pattern_fn: Function that takes table name and returns S3 pattern
    """
    motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
    s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
    aws_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION', 'us-east-1')
    
    if not motherduck_token:
        print('MOTHERDUCK_TOKEN not set, skipping MotherDuck refresh')
        return False
    
    print(f'Connecting to MotherDuck...')
    con = duckdb.connect(f'md:?motherduck_token={motherduck_token}')
    
    con.execute(f"SET s3_access_key_id='{aws_key}';")
    con.execute(f"SET s3_secret_access_key='{aws_secret}';")
    con.execute(f"SET s3_region='{aws_region}';")
    
    print(f'Creating {database} VIEWs (direct S3 query, no caching)...')
    con.execute(f'CREATE DATABASE IF NOT EXISTS {database};')
    con.execute(f'USE {database};')
    con.execute('CREATE SCHEMA IF NOT EXISTS main;')
    con.execute('USE main;')
    
    success_count = 0
    failed_tables = []
    
    for table in tables:
        s3_pattern = s3_pattern_fn(table)
        try:
            # Create VIEW instead of TABLE - always queries S3 directly.
            # Deduplicate by city_id + time so the latest ingested file wins.
            sql = f"""
            DROP VIEW IF EXISTS {table};
            CREATE OR REPLACE VIEW {table} AS 
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY time, city_id 
                    ORDER BY ingestion_timestamp DESC
                ) as rn
                FROM read_parquet('{s3_pattern}', hive_partitioning=true, union_by_name=true)
            ) WHERE rn = 1;
            """
            con.execute(sql)
            
            # Test the view and get count
            count = con.execute(f'SELECT COUNT(*) FROM {table};').fetchone()[0]
            print(f'   ✓ {database}.{table}: VIEW created → {count} rows (deduplicated, live S3 query)')
            success_count += 1
        except Exception as e:
            print(f'   ⚠️  {database}.{table}: SKIPPED - {str(e)[:100]}')
            failed_tables.append(table)
    
    con.close()
    
    if failed_tables:
        print(f'\n⚠️  Warning: {len(failed_tables)} tables skipped: {", ".join(failed_tables)}')
        print(f'✓ Successfully refreshed {success_count}/{len(tables)} tables')
        return False  # Indicate partial failure
    else:
        print(f'✓ All {database} tables refreshed successfully!')
        return True


def check_data_exists_in_s3(city, date, s3_prefix='raw'):
    """Check if data for a given city and date already exists in S3"""
    import boto3
    from botocore.exceptions import ClientError
    
    s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
    prefix = f'{s3_prefix}/city={city}/hourly_{date}_{date}_'
    
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        
        if 'Contents' in response and len(response['Contents']) > 0:
            files = [obj['Key'] for obj in response['Contents']]
            print(f"   ✓ Data already exists for {city} on {date}: {len(files)} files found")
            return True
        else:
            print(f"   → No existing data found for {city} on {date}")
            return False
    except ClientError as e:
        print(f"   ⚠️  Error checking S3 for {city} on {date}: {e}")
        return False


def upload_parquet_to_s3(local_dir, s3_prefix):
    """Upload parquet files from local directory to S3 with Hive partitioning"""
    s3_bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
    s3_client = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION', 'eu-west-1')
    )
    
    data_dir = Path(local_dir)
    uploaded = 0
    
    if not data_dir.exists():
        print(f'Data directory {data_dir} does not exist')
        return 0
    
    for city_partition in data_dir.glob('city=*'):
        if city_partition.is_dir():
            for parquet_file in city_partition.glob('*.parquet'):
                s3_key = f'{s3_prefix}/{city_partition.name}/{parquet_file.name}'
                print(f'Uploading {city_partition.name}/{parquet_file.name} to s3://{s3_bucket}/{s3_key}')
                s3_client.upload_file(str(parquet_file), s3_bucket, s3_key)
                uploaded += 1
    
    print(f'Uploaded {uploaded} files to S3')
    return uploaded
