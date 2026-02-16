#!/bin/bash
# Fix permission issue for Airflow data directory on EC2

mkdir -p data/raw # Ensure raw data directory exists
sudo chown -R 50000:0 data # Setting ownership to airflow user (UID 50000)
docker-compose -f docker-compose.t3micro.yml restart