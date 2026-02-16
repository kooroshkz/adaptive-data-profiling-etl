.PHONY: up down ps restart run permission log

up:
	docker-compose up -d

down:
	docker-compose down

ps:
	docker-compose ps

restart:
	docker-compose restart

run:
	docker exec airflow-airflow-scheduler-1 airflow dags trigger weather_ingestion

permission:
	sudo chown -R ec2-user:ec2-user ~/adaptive-data-profiling-etl

log:
	docker-compose logs -f airflow-scheduler
