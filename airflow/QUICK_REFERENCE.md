# Airflow on EC2 - Quick Reference

## SSH Connection

### Basic SSH
```bash
ssh -i ~/.ssh/your-key.pem ec2-user@YOUR_EC2_IP
```

### SSH Tunnel for Airflow UI
```bash
# Local machine - creates tunnel, then access http://localhost:8080
ssh -i ~/.ssh/your-key.pem -L 8080:localhost:8080 ec2-user@YOUR_EC2_IP -N
```

### SSH Config (Add to ~/.ssh/config)
```
Host airflow-ec2
    HostName YOUR_EC2_IP
    User ec2-user
    IdentityFile ~/.ssh/your-key.pem
    LocalForward 8080 localhost:8080
```

Then connect with: `ssh airflow-ec2`

## Service Management

```bash
# Check Airflow service status
sudo systemctl status airflow-docker

# Start Airflow
sudo systemctl start airflow-docker

# Stop Airflow
sudo systemctl stop airflow-docker

# Restart Airflow
sudo systemctl restart airflow-docker

# Enable auto-start on boot
sudo systemctl enable airflow-docker

# View service logs
sudo journalctl -u airflow-docker -f
```

## Docker Commands

```bash
# Check all containers
docker-compose ps

# View logs
docker-compose logs -f                    # All services
docker-compose logs -f airflow-scheduler  # Scheduler only
docker-compose logs -f airflow-webserver  # Webserver only

# Restart specific service
docker-compose restart airflow-scheduler
docker-compose restart airflow-webserver

# Full restart
docker-compose down && docker-compose up -d

# Check resource usage
docker stats

# Clean up old containers
docker system prune -a
```

## Airflow DAG Management

```bash
# From inside scheduler container
docker-compose exec airflow-scheduler bash

# List DAGs
airflow dags list

# Pause/Unpause DAG
airflow dags pause weather_ingestion
airflow dags unpause weather_ingestion

# Trigger DAG manually
airflow dags trigger weather_ingestion

# Test specific task
airflow tasks test weather_ingestion upload_to_s3 2026-02-13
```

## Git Updates

```bash
# Update code from GitHub
cd ~/projects/adaptive-data-profiling-etl
git pull

# Restart to apply changes
docker-compose restart airflow-scheduler
```

## Monitoring

```bash
# Check disk space
df -h
du -sh ~/projects/adaptive-data-profiling-etl/airflow/logs/

# Check memory usage
free -h

# Check CPU usage
htop

# Check if Airflow is responding
curl http://localhost:8080/health

# View S3 uploaded files
aws s3 ls s3://weather-data-koorosh-thesis/raw/ --recursive
```

## Troubleshooting

### Containers Not Starting
```bash
# Check Docker daemon
sudo systemctl status docker

# Check logs
docker-compose logs

# Check disk space
df -h

# Rebuild containers
docker-compose down
docker-compose up -d --build
```

### Tasks Failing
```bash
# Check scheduler logs
docker-compose logs airflow-scheduler | grep ERROR

# Check specific task log
# Go to Airflow UI > weather_ingestion > Click failed task > View Log

# Test task manually
docker-compose exec airflow-scheduler airflow tasks test weather_ingestion upload_to_s3 2026-02-13
```

### S3 Upload Issues
```bash
# Verify credentials are loaded
docker-compose exec airflow-scheduler env | grep AWS

# Test S3 access
docker-compose exec airflow-scheduler aws s3 ls s3://weather-data-koorosh-thesis/

# Check boto3 is installed
docker-compose exec airflow-scheduler pip list | grep boto3
```

### Out of Memory
```bash
# Check memory usage
free -h
docker stats

# Reduce webserver workers (edit docker-compose.yml)
# AIRFLOW__WEBSERVER__WORKERS: 2  # Instead of 4

# Restart
docker-compose down && docker-compose up -d
```

### Database Issues
```bash
# Reset database (WARNING: loses history)
docker-compose down -v
docker-compose up -d

# Backup database first
docker-compose exec postgres pg_dump -U airflow airflow > backup.sql
```

## Backup & Restore

### Backup
```bash
# Backup database
docker-compose exec postgres pg_dump -U airflow airflow > airflow-backup-$(date +%Y%m%d).sql

# Upload to S3
aws s3 cp airflow-backup-*.sql s3://weather-data-koorosh-thesis/backups/
```

### Restore
```bash
# Download from S3
aws s3 cp s3://weather-data-koorosh-thesis/backups/airflow-backup-20260212.sql .

# Restore
docker-compose exec -T postgres psql -U airflow airflow < airflow-backup-20260212.sql
```

## Performance Tuning

### Reduce CPU Usage
```bash
# Edit docker-compose.yml
# Reduce scheduler parsing frequency
# AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: 300
```

### Reduce Memory Usage
```bash
# Reduce webserver workers
# AIRFLOW__WEBSERVER__WORKERS: 2

# Reduce parallelism
# AIRFLOW__CORE__PARALLELISM: 8
```

### Clean Old Logs
```bash
# Keep only last 7 days
find ~/projects/adaptive-data-profiling-etl/airflow/logs -type f -mtime +7 -delete

# Or via Airflow config
# AIRFLOW__LOGGING__LOG_MAX_AGE_DAYS: 7
```

## Security

### Change Airflow Admin Password
```bash
# In Airflow UI
# Admin menu > Users > admin > Edit > Change password
```

### Update EC2 Security Group
```bash
# Allow only your IP for SSH
aws ec2 authorize-security-group-ingress \
  --group-id sg-xxxxx \
  --protocol tcp --port 22 \
  --cidr YOUR_NEW_IP/32
```

### Rotate AWS Credentials
```bash
# Update .env file
nano ~/projects/adaptive-data-profiling-etl/airflow/.env

# Restart Airflow
docker-compose down && docker-compose up -d
```

## Cost Optimization

### Stop EC2 When Not Needed
```bash
# From local machine
aws ec2 stop-instances --instance-ids i-xxxxx

# Start again
aws ec2 start-instances --instance-ids i-xxxxx
```

### Use Spot Instance (Advanced)
- Request Spot instance instead of On-Demand
- Save ~70% on costs
- Risk of interruption

### Schedule EC2 Start/Stop
```bash
# Add cron job to stop at night
# crontab -e
0 22 * * * aws ec2 stop-instances --instance-ids i-xxxxx  # Stop at 10 PM
0 6 * * * aws ec2 start-instances --instance-ids i-xxxxx  # Start at 6 AM
```

## Health Checks

```bash
# Check all services healthy
docker-compose ps

# Test webserver
curl http://localhost:8080/health

# Test scheduler
curl http://localhost:8974/health

# Test database
docker-compose exec postgres pg_isready -U airflow

# Full health check script
cat > health-check.sh << 'EOF'
#!/bin/bash
echo "Webserver: $(curl -s http://localhost:8080/health | jq -r .metadatabase.status)"
echo "Scheduler: $(curl -s http://localhost:8974/health | jq -r .metadatabase.status)"
echo "Database: $(docker-compose exec -T postgres pg_isready -U airflow)"
echo "Disk: $(df -h / | tail -1 | awk '{print $5}')"
echo "Memory: $(free -h | grep Mem | awk '{print $3 "/" $2}')"
EOF
chmod +x health-check.sh
./health-check.sh
```
