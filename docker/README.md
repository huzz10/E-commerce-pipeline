# Docker Setup Guide

This directory contains Docker configurations for running the E-commerce Data Pipeline locally.

## Prerequisites

- Docker Desktop installed and running
- Docker Compose installed

## Quick Start

### 1. Start Infrastructure Services

```bash
cd docker
docker-compose up -d
```

This will start:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Kafka UI (port 8080) - Web interface for Kafka
- Jupyter Notebook (port 8888) - For data exploration
- Airflow (port 8081) - For orchestration

### 2. Verify Services

```bash
# Check Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check Airflow
curl http://localhost:8081/health

# Access Kafka UI
open http://localhost:8080

# Access Jupyter
open http://localhost:8888
```

### 3. Create Kafka Topic

```bash
docker exec kafka kafka-topics --create \
  --topic ecommerce_events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 4. Run Pipeline Components

```bash
# Run Kafka producer
python kafka/producer.py --events 100

# Run Spark streaming (in separate terminal)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark/streaming_pipeline.py
```

## Building Custom Images

### Spark Image

```bash
docker build -f Dockerfile.spark -t ecommerce-spark:latest ..
```

### ML Image

```bash
docker build -f Dockerfile.ml -t ecommerce-ml:latest ..
```

## Stopping Services

```bash
docker-compose down
```

To remove volumes:

```bash
docker-compose down -v
```

## Troubleshooting

### Kafka not accessible
- Ensure ports 9092 and 2181 are not in use
- Check logs: `docker logs kafka`

### Airflow not starting
- Wait a few minutes for initialization
- Check logs: `docker logs airflow-webserver`
- Initialize database: `docker exec airflow-webserver airflow db init`

### Port conflicts
- Modify ports in `docker-compose.yml` if needed

