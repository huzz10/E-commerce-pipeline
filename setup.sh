#!/bin/bash

# Setup script for E-commerce Data Pipeline

echo "🚀 Setting up E-commerce Data Pipeline..."

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p logs
mkdir -p models
mkdir -p data
mkdir -p checkpoints

# Create Kafka topic (if Docker is running)
echo "🔧 Setting up Kafka topic..."
if docker ps | grep -q kafka; then
    docker exec kafka kafka-topics --create \
        --topic ecommerce_events \
        --bootstrap-server localhost:9092 \
        --partitions 3 \
        --replication-factor 1 \
        2>/dev/null || echo "Topic already exists"
else
    echo "⚠️  Kafka not running. Start Docker Compose first: cd docker && docker-compose up -d"
fi

# Initialize BigQuery tables (if GCP is configured)
echo "📊 Initializing BigQuery tables..."
if command -v gcloud &> /dev/null; then
    python gcp/bigquery_client.py || echo "⚠️  BigQuery setup skipped (configure GCP credentials)"
else
    echo "⚠️  gcloud CLI not found. Install Google Cloud SDK for BigQuery setup."
fi

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Activate virtual environment: source venv/bin/activate"
echo "2. Start infrastructure: cd docker && docker-compose up -d"
echo "3. Run Kafka producer: python kafka/producer.py --events 100"
echo "4. Run Spark streaming: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark/streaming_pipeline.py"

