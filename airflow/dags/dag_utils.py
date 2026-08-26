"""Utility functions for Airflow DAGs.

Only optional email notifications remain. Cloud helpers (GitHub Actions trigger,
MotherDuck refresh, S3 upload/checks) were removed: the pipeline now runs fully
locally. Email is optional and skips silently when Brevo is not configured.
"""

import os
import requests


def send_email_notification(subject, body, to_email=None):
    """Send email via Brevo API. No-op (returns False) if not configured."""
    api_key = os.getenv('BREVO_API_KEY')
    alert_email = to_email or os.getenv('BREVO_ALERT_EMAIL')
    sender_email = os.getenv('BREVO_SENDER_EMAIL')
    sender_name = os.getenv('BREVO_SENDER_NAME', 'Airflow Weather Pipeline')

    if not all([api_key, alert_email, sender_email]):
        print('Email not configured (BREVO_* unset), skipping notification')
        return False

    url = "https://api.brevo.com/v3/smtp/email"
    headers = {
        "accept": "application/json",
        "api-key": api_key,
        "content-type": "application/json",
    }
    html_body = body.replace('\n', '<br>')
    payload = {
        "sender": {"name": sender_name, "email": sender_email},
        "to": [{"email": alert_email}],
        "subject": subject,
        "htmlContent": f"<html><body><pre>{html_body}</pre></body></html>",
        "textContent": body,
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code in [200, 201]:
            print(f'Email sent to {alert_email}')
            return True
        print(f'Failed to send email: {response.status_code} - {response.text}')
        return False
    except Exception as e:
        print(f'Failed to send email: {e}')
        return False


def build_failure_email(context):
    """Build failure notification email content from Airflow context."""
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

Check Airflow UI: http://localhost:8080
    """

    return subject, body
