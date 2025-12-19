# Quick Start Guide

Get the E-commerce Data Pipeline running in 5 minutes!

## Prerequisites Check

```bash
# Check Python version (3.10+)
python --version

# Check Java (for Spark)
java -version

# Check Docker
docker --version
docker-compose --version
```

## Step-by-Step Setup

### 1. Install Dependencies

**Linux/Mac:**
```bash
chmod +x setup.sh
./setup.sh
```

**Windows:**
```powershell
.\setup.ps1
```

**Manual:**
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Start Infrastructure

```bash
cd docker
docker-compose up -d
```

Wait 30-60 seconds for services to start, then verify:

```bash
# Check Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check services
docker ps
```

### 3. Create Kafka Topic

```bash
docker exec kafka kafka-topics --create \
  --topic ecommerce_events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 4. Run Pipeline (3 Terminals)

**Terminal 1: Kafka Producer**
```bash
source venv/bin/activate  # Windows: venv\Scripts\activate
python kafka/producer.py --events 100 --delay 0.5
```

**Terminal 2: Spark Streaming** (if Spark is installed)
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/streaming_pipeline.py
```

**Terminal 3: Monitor Kafka** (optional)
```bash
docker exec -it kafka kafka-console-consumer \
  --topic ecommerce_events \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

### 5. Verify Results

**Check Kafka UI:** http://localhost:8080

**Check Jupyter:** http://localhost:8888

**Check Airflow:** http://localhost:8081 (user: airflow, password: airflow)

## Common Issues

### Kafka not accessible
```bash
# Check if Kafka is running
docker ps | grep kafka

# Check logs
docker logs kafka

# Restart services
cd docker
docker-compose restart kafka
```

### Spark not found
```bash
# Install Spark
# Download from https://spark.apache.org/downloads.html
# Or use Docker:
docker run -it apache/spark-py:3.5.0 bash
```

### Port conflicts
Edit `docker/docker-compose.yml` to change ports:
- Kafka: 9092
- Kafka UI: 8080
- Jupyter: 8888
- Airflow: 8081

## Next Steps

1. **Configure GCP** (for cloud deployment):
   ```bash
   gcloud auth application-default login
   # Update configs/config.yaml with your project ID
   ```

2. **Run Batch Processing**:
   ```bash
   spark-submit spark/batch_processing.py --historical-csv data.csv
   ```

3. **Train ML Models**:
   ```bash
   python ml/train.py --data data/daily_sales.parquet
   ```

4. **Generate Predictions**:
   ```bash
   python ml/inference.py --data data/daily_sales.parquet --model xgboost
   ```

## Testing Individual Components

```bash
# Test Kafka producer
python kafka/producer.py --events 10

# Test Kafka consumer
python kafka/consumer.py --max-messages 10

# Test feature engineering
python ml/feature_engineering.py

# Test BigQuery client (requires GCP setup)
python gcp/bigquery_client.py
```

## Need Help?

- Check the main [README.md](README.md) for detailed documentation
- Review logs in `logs/` directory
- Check Docker logs: `docker logs <container-name>`

