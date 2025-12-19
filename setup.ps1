# PowerShell setup script for E-commerce Data Pipeline

Write-Host "🚀 Setting up E-commerce Data Pipeline..." -ForegroundColor Green

# Create virtual environment
Write-Host "📦 Creating virtual environment..." -ForegroundColor Yellow
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install dependencies
Write-Host "📥 Installing dependencies..." -ForegroundColor Yellow
python -m pip install --upgrade pip
pip install -r requirements.txt

# Create necessary directories
Write-Host "📁 Creating directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path logs, models, data, checkpoints | Out-Null

# Create Kafka topic (if Docker is running)
Write-Host "🔧 Setting up Kafka topic..." -ForegroundColor Yellow
if (docker ps | Select-String -Pattern "kafka") {
    docker exec kafka kafka-topics --create `
        --topic ecommerce_events `
        --bootstrap-server localhost:9092 `
        --partitions 3 `
        --replication-factor 1 `
        2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "⚠️  Topic may already exist" -ForegroundColor Yellow
    }
} else {
    Write-Host "⚠️  Kafka not running. Start Docker Compose first: cd docker; docker-compose up -d" -ForegroundColor Yellow
}

# Initialize BigQuery tables (if GCP is configured)
Write-Host "📊 Initializing BigQuery tables..." -ForegroundColor Yellow
if (Get-Command gcloud -ErrorAction SilentlyContinue) {
    python gcp/bigquery_client.py
    if ($LASTEXITCODE -ne 0) {
        Write-Host "⚠️  BigQuery setup skipped (configure GCP credentials)" -ForegroundColor Yellow
    }
} else {
    Write-Host "⚠️  gcloud CLI not found. Install Google Cloud SDK for BigQuery setup." -ForegroundColor Yellow
}

Write-Host "✅ Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Activate virtual environment: .\venv\Scripts\Activate.ps1"
Write-Host "2. Start infrastructure: cd docker; docker-compose up -d"
Write-Host "3. Run Kafka producer: python kafka/producer.py --events 100"
Write-Host "4. Run Spark streaming: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark/streaming_pipeline.py"

