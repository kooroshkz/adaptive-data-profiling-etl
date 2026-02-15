#!/bin/bash
# Quick fix for permission issues on EC2

echo "🔧 Fixing Airflow data directory permissions..."
echo ""

cd ~/adaptive-data-profiling-etl/airflow || exit 1

# Create data directories if they don't exist
mkdir -p ./data/raw ./data/staging

# Fix ownership (Airflow runs as UID 50000)
echo "Setting ownership to UID 50000 (airflow user)..."
sudo chown -R 50000:0 ./data

# Verify
ls -la ./data

echo ""
echo "✅ Permissions fixed!"
echo ""
echo "Now restart containers:"
echo "  docker-compose restart"
echo ""
echo "Or do a full restart to pick up latest code:"
echo "  docker-compose down"
echo "  docker-compose up -d"
