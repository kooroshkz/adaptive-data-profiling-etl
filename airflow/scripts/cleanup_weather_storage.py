#!/usr/bin/env python3
"""Remove all weather parquet objects and local raw/staging directories for a fresh start."""

import os
import shutil

import boto3


def clean_everything_for_fresh_start() -> None:
    bucket = os.getenv('S3_BUCKET', 'weather-data-koorosh-thesis')
    s3_prefixes = ['raw/', 'staging/', 'mart/']

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

    local_paths = ['/opt/airflow/data/raw', '/opt/airflow/data/staging']
    for local_path in local_paths:
        if os.path.exists(local_path):
            print(f'Cleaning local directory {local_path}')
            shutil.rmtree(local_path)
            os.makedirs(local_path, exist_ok=True)

    print('Fresh-start cleanup completed')
    print('=' * 60)


if __name__ == '__main__':
    clean_everything_for_fresh_start()