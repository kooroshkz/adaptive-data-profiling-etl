# Deploy Airflow to AWS EC2

This guide walks you through deploying Airflow on an AWS EC2 instance for 24/7 operation.

## Prerequisites

- AWS Account with EC2 access
- SSH key pair for EC2
- AWS credentials (for S3 access from Airflow)

## Step 1: Launch EC2 Instance

### Via AWS Console

1. Go to **EC2 Dashboard** → **Launch Instance**
2. Configure:
   - **Name**: `airflow-server`
   - **AMI**: Amazon Linux 2023 (free tier eligible)
   - **Instance type**: `t3.micro` (1GB RAM, free tier for 12 months)
   - **Key pair**: Create or select existing SSH key
   - **Security Group**: 
     - SSH (22) from your IP
     - HTTP (8080) from your IP (or use SSH tunnel - recommended)
   - **Storage**: 20 GB gp3 (free tier allows 30 GB)
3. Click **Launch Instance**

### Via AWS CLI (Alternative)

```bash
# Create security group
aws ec2 create-security-group \
  --group-name airflow-sg \
  --description "Airflow server security group"

# Allow SSH from your IP
aws ec2 authorize-security-group-ingress \
  --group-name airflow-sg \
  --protocol tcp --port 22 \
  --cidr YOUR_IP/32

# Launch instance
aws ec2 run-instances \
  --image-id ami-xxxxxxxxx \
  --instance-type t3.micro \
  --key-name your-key-name \
  --security-groups airflow-sg \
  --block-device-mappings DeviceName=/dev/xvda,Ebs={VolumeSize=20,VolumeType=gp3} \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=airflow-server}]'
```

## Step 2: Connect to EC2

```bash
# Replace with your key and EC2 IP
ssh -i ~/.ssh/your-key.pem ec2-user@YOUR_EC2_IP
```

## Step 3: Install Docker & Docker Compose on EC2

Run the automated setup script:

```bash
# Update system
sudo dnf update -y

# Install Docker
sudo dnf install docker -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Install git
sudo dnf install git -y

# Logout and login again for docker group to take effect
exit
```

**Re-connect via SSH** for docker group changes to apply.

## Step 4: Clone Repository & Setup Airflow

```bash
# Clone your repository
cd ~
git clone https://github.com/kooroshkz/adaptive-data-profiling-etl.git
cd adaptive-data-profiling-etl/airflow

# Create .env file with your AWS credentials
cat > .env << 'EOF'
AIRFLOW_UID=50000
AIRFLOW_GID=0

# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=eu-west-1
S3_BUCKET=weather-data-koorosh-thesis

# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
EOF

# Update .env with your actual credentials
nano .env
```

## Step 5: Deploy Airflow

```bash
# Run deployment script
chmod +x deploy.sh
./deploy.sh

# Check status
docker-compose ps
```

## Step 6: Access Airflow UI

### Option 1: SSH Tunnel (Recommended - Most Secure)

On your **local machine**:
```bash
# Create SSH tunnel
ssh -i ~/.ssh/your-key.pem -L 8080:localhost:8080 ec2-user@YOUR_EC2_IP -N

# Keep terminal open, then access: http://localhost:8080
# Login: airflow / airflow
```

### Option 2: Tailscale VPN (Best for Multiple Devices)

**On EC2:**
```bash
# Install Tailscale
curl -fsSL https://tailscale.com/install.sh | sh

# Start Tailscale (follow authentication URL)
sudo tailscale up

# Get Tailscale IP
tailscale ip -4
```

**On your local machine:**
- Install Tailscale: https://tailscale.com/download
- Login to same Tailscale account
- Access Airflow: `http://TAILSCALE_IP:8080`

### Option 3: CloudFlare Tunnel (Public URL)

**On EC2:**
```bash
# Install cloudflared
wget https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb
sudo dpkg -i cloudflared-linux-amd64.deb

# Authenticate
cloudflared tunnel login

# Create tunnel
cloudflared tunnel create airflow-tunnel

# Route traffic
cloudflared tunnel route dns airflow-tunnel airflow.yourdomain.com

# Run tunnel
cloudflared tunnel --config ~/.cloudflared/config.yml run airflow-tunnel
```

## Step 7: Verify Deployment

1. Access Airflow UI (using one of the methods above)
2. Login: `airflow` / `airflow`
3. Navigate to **weather_ingestion** DAG
4. Click **▶️ Trigger DAG**
5. Monitor execution - all tasks should turn green ✅
6. Verify S3 upload:
   ```bash
   aws s3 ls s3://weather-data-koorosh-thesis/raw/
   ```

## Step 8: Enable Auto-Start on Reboot

Ensure Airflow restarts automatically if EC2 reboots:

```bash
# Create systemd service
sudo tee /etc/systemd/system/airflow-docker.service > /dev/null << 'EOF'
[Unit]
Description=Airflow Docker Compose
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/ec2-user/adaptive-data-profiling-etl/airflow
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down
User=ec2-user

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl enable airflow-docker
sudo systemctl start airflow-docker

# Check status
sudo systemctl status airflow-docker
```

## Step 9: Disable GitHub Actions (Optional)

Since Airflow is now handling daily runs, disable the GitHub Actions workflow:

```bash
# On your local machine
cd ~/Desktop/adaptive-data-profiling-etl
mv .github/workflows/data-ingestion.yml .github/workflows/data-ingestion.yml.disabled

git add .
git commit -m "Disable GitHub Actions - moved to Airflow on EC2"
git push
```

## Monitoring & Maintenance

### Check Airflow Logs
```bash
cd ~/adaptive-data-profiling-etl/airflow
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

### Check Docker Status
```bash
docker-compose ps
docker stats
```

### Check Disk Space
```bash
df -h
du -sh ~/adaptive-data-profiling-etl/airflow/logs/
```

### Restart Airflow
```bash
cd ~/adaptive-data-profiling-etl/airflow
docker-compose restart
```

### Update DAGs
```bash
cd ~/adaptive-data-profiling-etl
git pull
docker-compose restart airflow-scheduler
```

## Cost Estimation

### Free Tier (First 12 months)
- **EC2 t3.micro**: FREE (750 hours/month)
- **EBS Storage**: FREE (30 GB)
- **Data Transfer**: FREE (15 GB out/month)
- **Total**: $0/month

### After Free Tier
- **EC2 t3.micro**: ~$7.50/month (on-demand)
- **EBS 20GB gp3**: ~$1.60/month
- **Total**: ~$9.10/month

### To Minimize Costs
1. Use **Reserved Instance** or **Savings Plan**: ~$4.50/month (saves 40%)
2. Use **Spot Instance** if acceptable: ~$2.25/month (saves 70%)
3. Stop instance when not needed (lose always-on benefit)

## Troubleshooting

### Cannot Connect to EC2
```bash
# Check security group allows your IP
# Update inbound rules if your IP changed
```

### Airflow Not Starting
```bash
# Check Docker is running
sudo systemctl status docker

# Check disk space
df -h

# Check docker-compose logs
docker-compose logs
```

### Tasks Failing
```bash
# Check scheduler logs
docker-compose logs airflow-scheduler

# Check specific task log in UI
# Or via CLI:
docker-compose exec airflow-scheduler airflow tasks test weather_ingestion upload_to_s3 2026-02-13
```

### S3 Upload Fails
```bash
# Verify AWS credentials in .env
cat .env | grep AWS

# Test AWS credentials
docker-compose exec airflow-scheduler aws s3 ls s3://weather-data-koorosh-thesis/
```

## Security Best Practices

1. ✅ **Use SSH tunnel** instead of exposing port 8080 publicly
2. ✅ **Change default password** in Airflow UI
3. ✅ **Keep .env secure** - never commit to git
4. ✅ **Update packages regularly**:
   ```bash
   sudo dnf update -y
   docker-compose pull
   docker-compose up -d
   ```
5. ✅ **Enable CloudWatch logs** for monitoring
6. ✅ **Setup CloudWatch alarms** for CPU/Memory

## Backup Strategy

### Backup DAGs (Automated via Git)
```bash
# Already tracked in git
git push
```

### Backup Airflow Database
```bash
# Export Airflow metadata
docker-compose exec postgres pg_dump -U airflow airflow > backup-$(date +%Y%m%d).sql
```

### Backup to S3
```bash
# Upload backups to S3
aws s3 cp backup-*.sql s3://weather-data-koorosh-thesis/backups/
```

## Next Steps

1. ✅ Deploy to EC2
2. ✅ Setup SSH tunnel or Tailscale
3. ✅ Verify DAG runs successfully
4. ✅ Monitor for 24 hours
5. ✅ Disable GitHub Actions
6. ⬜ Setup CloudWatch monitoring
7. ⬜ Configure email alerts for failures
8. ⬜ Document runbooks for team
