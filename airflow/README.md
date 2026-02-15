# Airflow Deployment on AWS EC2

> **🚀 For detailed EC2 deployment guide, see [EC2_DEPLOYMENT.md](EC2_DEPLOYMENT.md)**
> 
> **📘 For quick command reference, see [QUICK_REFERENCE.md](QUICK_REFERENCE.md)**

## Quick Start - Local Testing

### 1. Prerequisites
- Docker Desktop installed and running
- AWS credentials with S3 access
- Git

### 2. Setup
```bash
# Clone repository
git clone https://github.com/kooroshkz/adaptive-data-profiling-etl.git
cd adaptive-data-profiling-etl/airflow

# Create .env file
cp .env.example .env
nano .env  # Add your AWS credentials

# Deploy
./deploy.sh
```

### 3. Access Airflow UI
- URL: http://localhost:8080
- Login: `airflow` / `airflow`

## Production Deployment on EC2

### Quick Deploy to EC2 (Amazon Linux 2023)

```bash
# 1. SSH into EC2
ssh -i your-key.pem ec2-user@YOUR_EC2_IP

# 2. Run automated setup
curl -sSL https://raw.githubusercontent.com/kooroshkz/adaptive-data-profiling-etl/main/airflow/ec2-setup.sh | bash

# 3. Logout and login again
exit
ssh -i your-key.pem ec2-user@YOUR_EC2_IP

# 4. Clone and configure
cd ~/projects
git clone https://github.com/kooroshkz/adaptive-data-profiling-etl.git
cd adaptive-data-profiling-etl/airflow
nano .env  # Add AWS credentials

# 5. Deploy
./deploy.sh

# 6. Enable auto-start
sudo cp airflow-docker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable airflow-docker
sudo systemctl start airflow-docker
```

### Access Airflow on EC2

**Option 1: SSH Tunnel (Recommended)**
```bash
# On your local machine
ssh -i your-key.pem -L 8080:localhost:8080 ec2-user@YOUR_EC2_IP -N

# Access: http://localhost:8080
```

**Option 2: Tailscale VPN**
```bash
# On EC2
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up

# Access via Tailscale IP: http://TAILSCALE_IP:8080
```

## EC2 Free Tier Details

- **Instance**: t3.micro (1 vCPU, 1GB RAM)
- **Cost**: FREE for 12 months (750 hours/month)
- **After free tier**: ~$7.50/month
- **Storage**: 20GB gp3 (~$1.60/month)
- **Total**: $0/month (year 1), ~$9/month (after)

---

## Original Local Deployment Guide (Below)

## EC2 Setup (Free Tier)

### 1. Launch EC2 Instance
```bash
# Instance Type: t3.micro (FREE for 12 months)
# AMI: Amazon Linux 2023 or Ubuntu 22.04
# Storage: 20GB gp3 (FREE tier: 30GB)
# Security Group: SSH (22) from your IP only
```

### 2. Install Docker on EC2
```bash
# SSH into EC2
ssh -i your-key.pem ec2-user@your-ec2-ip

# Install Docker
sudo yum update -y
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -a -G docker ec2-user

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Logout and login again for group changes
exit
```

### 3. Deploy Airflow
```bash
# Clone your repo on EC2
git clone https://github.com/kooroshkz/adaptive-data-profiling-etl.git
cd adaptive-data-profiling-etl/airflow

# Create .env file
cp .env.example .env
nano .env  # Add your AWS credentials

# Create required directories
mkdir -p dags logs plugins scripts

# Start Airflow
docker-compose up -d

# Check logs
docker-compose logs -f

# Check status
docker-compose ps
```

## Accessing Airflow UI

### Option 1: SSH Tunnel (Easiest)
```bash
# On your laptop
ssh -L 8080:localhost:8080 -i your-key.pem ec2-user@your-ec2-ip

# Access: http://localhost:8080
# Login: airflow / airflow
```

### Option 2: Tailscale VPN (Recommended)
```bash
# On EC2
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up

# On your laptop (install Tailscale app)
# Access: http://ec2-tailscale-ip:8080
```

### Option 3: CloudFlare Tunnel (Public URL)
```bash
# On EC2
docker run -d --name cloudflared \
  --network airflow_default \
  cloudflare/cloudflared:latest \
  tunnel --url http://airflow-webserver:8080

# Get the URL
docker logs cloudflared | grep trycloudflare.com
```

### Option 4: nginx + Basic Auth (More secure for open internet)
```bash
# Install nginx on EC2
sudo yum install -y nginx

# Create password file
sudo yum install -y httpd-tools
sudo htpasswd -c /etc/nginx/.htpasswd admin

# Configure nginx (see nginx.conf below)
sudo systemctl start nginx
sudo systemctl enable nginx

# Update Security Group: Allow 80 from your IP
# Access: http://your-ec2-ip
```

## Managing Airflow

### Start/Stop
```bash
# Start
docker-compose up -d

# Stop
docker-compose down

# Restart
docker-compose restart

# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

### Create Admin User
```bash
docker-compose run airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password your-password
```

### Update DAGs
```bash
# Just copy DAG files to ./dags/ folder
# Airflow picks them up automatically (may take 30s)
cp ../dags/weather_dag.py ./dags/
```

## Cost Optimization

### Auto-Shutdown at Night (Save 50% costs after free tier)
```bash
# Add to crontab on EC2
crontab -e

# Stop at 11 PM, start at 1 AM (before 2 AM job)
0 23 * * * cd /home/ec2-user/adaptive-data-profiling-etl/airflow && docker-compose down
0 1 * * * cd /home/ec2-user/adaptive-data-profiling-etl/airflow && docker-compose up -d
```

### Or use EC2 Scheduler
```bash
# Install AWS CLI
pip3 install awscli

# Create start/stop scripts and use EventBridge/Lambda
```

## Monitoring

### Check Airflow Health
```bash
curl http://localhost:8080/health
```

### Check DAG Status
```bash
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow dags state weather_ingestion
```

## Troubleshooting

### Airflow won't start
```bash
# Check logs
docker-compose logs

# Reset database
docker-compose down -v
docker-compose up -d
```

### Out of memory (t3.micro only has 1GB)
```bash
# Reduce worker concurrency in docker-compose.yml
AIRFLOW__CELERY__WORKER_CONCURRENCY: 1
```

### Can't access UI
```bash
# Check if port 8080 is open
sudo netstat -tlnp | grep 8080

# Check Docker network
docker network inspect airflow_default
```

## Security Best Practices

1. **Change default password** in .env file
2. **Restrict EC2 Security Group** to your IP only
3. **Use Tailscale or SSH tunnel** instead of opening port 8080 to internet
4. **Enable EC2 IMDSv2** (metadata protection)
5. **Rotate AWS credentials** regularly
6. **Use IAM role** instead of hardcoded credentials (better approach)

## Costs

### Free Tier (12 months)
- EC2 t3.micro: 750 hours/month FREE
- EBS: 30GB FREE
- Data transfer: 100GB/month OUT FREE
- **Total**: $0/month

### After Free Tier
- EC2 t3.micro: ~$7.50/month (24/7)
- EBS 20GB: ~$2/month
- Data transfer: ~$1/month
- **Total**: ~$10.50/month

### With Auto-Shutdown (12 hours/day)
- EC2 running 12h/day: ~$3.75/month
- **Total**: ~$6.75/month
