#!/bin/bash
# Fix permission issue for Airflow data directory on EC2

echo "Creating data/raw directory..."
mkdir -p data/raw

echo "Setting ownership to airflow user (UID 50000)..."
sudo chown -R 50000:0 data

echo "Verifying permissions..."
ls -la data/

echo "✅ Permissions fixed! Restarting Airflow containers..."
docker-compose -f docker-compose.t3micro.yml restart

echo "Done! Wait 60 seconds for containers to be healthy, then trigger DAG."
