#!/bin/bash
# EC2 Setup Script for Airflow Deployment
# Run this on a fresh Amazon Linux 2023 EC2 instance
set -e

echo "🚀 Starting Airflow EC2 Setup..."

# Update system
echo "📦 Updating system packages..."
sudo dnf update -y

# Install Docker
echo "🐳 Installing Docker..."
sudo dnf install docker -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

# Install Docker Compose
echo "🔧 Installing Docker Compose..."
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Install Git
echo "📚 Installing Git..."
sudo dnf install git -y

# Install AWS CLI (already installed on Amazon Linux 2023, but update)
echo "☁️  Updating AWS CLI..."
sudo dnf install awscli -y

# Create project directory
echo "📁 Setting up project directory..."
mkdir -p ~/projects
cd ~/projects

echo "✅ Base setup complete!"
echo ""
echo "⚠️  IMPORTANT: You must LOGOUT and LOGIN again for docker group changes to take effect!"
echo ""
echo "After re-login, run the following commands:"
echo ""
echo "  cd ~/projects"
echo "  git clone https://github.com/kooroshkz/adaptive-data-profiling-etl.git"
echo "  cd adaptive-data-profiling-etl/airflow"
echo "  nano .env  # Add your AWS credentials"
echo "  ./deploy.sh"
echo ""
echo "Then setup auto-start with:"
echo "  sudo systemctl enable airflow-docker"
echo "  sudo systemctl start airflow-docker"
