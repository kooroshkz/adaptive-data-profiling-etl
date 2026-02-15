#!/bin/bash
# Fix script for EC2 webserver timeout issue
# Run this on your EC2 instance

set -e

echo "=== Fixing Airflow EC2 Restart Issue ==="
echo ""
echo "Root cause: Webserver timeout on t3.micro (slow CPU)"
echo "Solution: Increased timeout to 10 minutes, reduced workers to 1"
echo ""

# Stop containers
echo "1. Stopping containers..."
docker-compose down

# Pull latest changes
echo ""
echo "2. Pulling latest config from repository..."
git pull origin main

# Start containers
echo ""
echo "3. Starting containers (this will take ~2-3 minutes)..."
docker-compose up -d

echo ""
echo "=== Containers started! ==="
echo ""
echo "IMPORTANT: The webserver needs up to 2-3 minutes to fully start on t3.small."
echo "Please wait patiently before checking status."
echo ""
echo "Monitor startup progress:"
echo "  docker-compose logs -f airflow-webserver"
echo ""
echo "Check status after 3 minutes:"
echo "  docker-compose ps"
echo "  curl http://localhost:8080/health"
echo ""
echo "The webserver is ready when you see:"
echo '  {"metadatabase":{"status":"healthy"},"scheduler":{"status":"healthy"}}'
echo ""
