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
docker-compose -f docker-compose.t3micro.yml down

# Pull latest changes
echo ""
echo "2. Pulling latest config from repository..."
git pull origin main

# Start containers
echo ""
echo "3. Starting containers (this will take ~5-10 minutes)..."
docker-compose -f docker-compose.t3micro.yml up -d

echo ""
echo "=== Containers started! ==="
echo ""
echo "IMPORTANT: The webserver needs up to 10 minutes to fully start on t3.micro."
echo "Please wait patiently before checking status."
echo ""
echo "Monitor startup progress:"
echo "  docker-compose -f docker-compose.t3micro.yml logs -f airflow-webserver"
echo ""
echo "Check status after 10 minutes:"
echo "  docker-compose -f docker-compose.t3micro.yml ps"
echo "  curl http://localhost:8080/health"
echo ""
echo "The webserver is ready when you see:"
echo '  {"metadatabase":{"status":"healthy"},"scheduler":{"status":"healthy"}}'
echo ""
