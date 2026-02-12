#!/bin/bash
# Quick deployment script for Airflow on EC2

set -e

echo "🚀 Airflow Deployment Script"
echo "=============================="

# Check if .env exists
if [ ! -f .env ]; then
    echo "❌ .env file not found!"
    echo "📝 Creating .env from .env.example..."
    cp .env.example .env
    echo "⚠️  Please edit .env and add your AWS credentials"
    echo "   nano .env"
    exit 1
fi

# Create directories
echo "📁 Creating directories..."
mkdir -p dags logs plugins scripts data/raw

# Set permissions for Airflow user
echo "🔐 Setting permissions..."
echo -e "AIRFLOW_UID=$(id -u)" >> .env

# Start Airflow
echo "🐳 Starting Airflow with Docker Compose..."
docker-compose up -d

echo ""
echo "⏳ Waiting for Airflow to start (30 seconds)..."
sleep 30

# Check status
echo ""
echo "📊 Checking Airflow status..."
docker-compose ps

echo ""
echo "✅ Airflow is running!"
echo ""
echo "🌐 Access Airflow UI:"
echo "   - SSH Tunnel: ssh -L 8080:localhost:8080 ec2-user@YOUR_EC2_IP"
echo "   - Then open: http://localhost:8080"
echo "   - Login: airflow / airflow"
echo ""
echo "📝 Useful commands:"
echo "   - View logs: docker-compose logs -f"
echo "   - Stop: docker-compose down"
echo "   - Restart: docker-compose restart"
echo ""
