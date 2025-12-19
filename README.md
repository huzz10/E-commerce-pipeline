# E-commerce Real-Time Analytics & Demand Prediction Pipeline

A production-grade, end-to-end data pipeline for real-time e-commerce analytics and demand forecasting. This project demonstrates industry-standard practices for building scalable data pipelines.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
├─────────────────────────────────────────────────────────────────┤
│  Historical Data (CSV/Parquet)  │  Real-Time Events (Kafka)     │
└──────────────┬──────────────────┴──────────────┬─────────────────┘
               │                                 │
               │                                 │
┌──────────────▼─────────────────────────────────▼──────────────┐
│                    KAFKA CLUSTER                              │
│              Topic: ecommerce_events                          │
└──────────────┬────────────────────────────────────────────────┘
               │
               │
┌──────────────▼────────────────────────────────────────────────┐
│          SPARK STRUCTURED STREAMING                           │
│  • Data Validation & Schema Enforcement                       │
│  • Deduplication                                              │
│  • Windowed Aggregations (5-min, 1-hour)                      │
│  • Real-time Metrics Calculation                              │
└──────────────┬────────────────────────────────────────────────┘
               │
               │
    ┌──────────┴──────────┐
    │                     │
    ▼                     ▼
┌──────────┐        ┌──────────┐
│   GCS    │        │   GCS    │
│  BRONZE  │        │  SILVER  │
│  (Raw)   │        │ (Cleaned)│
└────┬─────┘        └────┬─────┘
     │                   │
     └──────────┬────────┘
                │
                ▼
        ┌───────────────┐
        │ SPARK BATCH   │
        │  PROCESSING   │
        │  • Joins      │
        │  • Aggregates │
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │   BIGQUERY    │
        │     GOLD      │
        │  (Analytics)  │
        └───────┬───────┘
                │
        ┌───────┴───────┐
        │               │
        ▼               ▼
┌──────────────┐  ┌──────────────┐
│   ML TRAIN   │  │   ML INFER   │
│   (XGBoost)  │  │  (Predictions)│
└──────┬───────┘  └──────┬───────┘
       │                 │
       └────────┬─────────┘
                │
                ▼
        ┌───────────────┐
        │   BIGQUERY    │
        │  Predictions │
        └───────────────┘
                │
                ▼
        ┌───────────────┐
        │   AIRFLOW     │
        │ Orchestration │
        └───────────────┘
```

## 🛠️ Tech Stack

### Core Technologies
- **Python 3.10+**: Core logic, ML models, producers, consumers
- **Apache Kafka**: Real-time event streaming and ingestion
- **Apache Spark 3.5.0**: Stream processing (Structured Streaming) and batch processing
- **Apache Airflow 2.7.3**: Workflow orchestration and scheduling
- **SQL**: Analytics queries and aggregations

### Cloud Platform (GCP)
- **Google Cloud Storage (GCS)**: Data lake (Bronze/Silver/Gold layers)
- **BigQuery**: Analytics data warehouse
- **Cloud Composer** (Optional): Managed Airflow

### ML & Data Science
- **scikit-learn**: Baseline Linear Regression model
- **XGBoost**: Advanced gradient boosting model
- **pandas & numpy**: Data manipulation and feature engineering

## 📁 Project Structure

```
ecommerce-data-pipeline/
├── kafka/                    # Kafka producers and consumers
│   ├── producer.py           # Event simulation producer
│   └── consumer.py           # Test consumer
│
├── spark/                    # Spark processing jobs
│   ├── streaming_pipeline.py # Structured Streaming pipeline
│   └── batch_processing.py   # Batch processing jobs
│
├── airflow/                  # Airflow DAGs
│   └── dags/
│       ├── ecommerce_pipeline_dag.py
│       └── spark_streaming_dag.py
│
├── sql/                      # BigQuery SQL queries
│   ├── daily_revenue.sql
│   ├── category_growth.sql
│   ├── demand_vs_actual.sql
│   └── top_selling_products.sql
│
├── ml/                       # ML pipeline
│   ├── feature_engineering.py
│   ├── train.py              # Model training
│   └── inference.py          # Demand prediction
│
├── gcp/                      # GCP clients
│   ├── bigquery_client.py
│   └── gcs_client.py
│
├── utils/                    # Utilities
│   ├── logger.py
│   └── config_loader.py
│
├── docker/                   # Docker setup
│   ├── docker-compose.yml
│   ├── Dockerfile.spark
│   └── Dockerfile.ml
│
├── configs/                  # Configuration files
│   └── config.yaml
│
├── tests/                    # Unit tests
│
├── data.csv                  # Historical data
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## 🚀 Quick Start

### Prerequisites

1. **Python 3.10+** installed
2. **Java 8+** (for Spark)
3. **Docker & Docker Compose** (for local infrastructure)
4. **GCP Account** (for cloud deployment)
5. **Apache Spark 3.5.0** (for local Spark jobs)

### Local Setup

#### 1. Clone and Install Dependencies

```bash
# Clone repository
git clone <repository-url>
cd ecommerce-data-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

#### 2. Start Infrastructure (Docker)

```bash
cd docker
docker-compose up -d

# Wait for services to start (30-60 seconds)
# Verify Kafka is running
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

#### 3. Create Kafka Topic

```bash
docker exec kafka kafka-topics --create \
  --topic ecommerce_events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

#### 4. Configure GCP (Optional for Local Testing)

Update `configs/config.yaml` with your GCP credentials:

```yaml
gcp:
  project_id: "your-gcp-project-id"
  bucket_name: "ecommerce-data-lake"
  dataset_id: "ecommerce_analytics"
```

Set up GCP authentication:

```bash
gcloud auth application-default login
```

#### 5. Run Pipeline Components

**Terminal 1: Kafka Producer (Simulate Events)**
```bash
python kafka/producer.py --events 1000 --delay 0.1
```

**Terminal 2: Spark Streaming Pipeline**
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/streaming_pipeline.py
```

**Terminal 3: Spark Batch Processing**
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/batch_processing.py \
  --historical-csv data.csv
```

**Terminal 4: ML Training**
```bash
python ml/train.py --data data/daily_sales.parquet
```

**Terminal 5: ML Inference**
```bash
python ml/inference.py \
  --data data/daily_sales.parquet \
  --model xgboost \
  --horizon next_day \
  --save-bq
```

## 📊 Data Flow

### 1. Data Ingestion
- **Kafka Producer** simulates real-time e-commerce events:
  - `order_created`: Purchase events
  - `product_viewed`: Product view events
  - `add_to_cart`: Cart addition events

### 2. Stream Processing (Spark Structured Streaming)
- Consumes events from Kafka topic `ecommerce_events`
- Performs:
  - **Data Validation**: Schema enforcement, null checks
  - **Deduplication**: Removes duplicate events
  - **Windowed Aggregations**:
    - 5-minute windows: Real-time revenue per product
    - 1-hour windows: Category revenue trends
  - **Metrics Calculation**:
    - Revenue per product
    - Revenue per category
    - Order volume trends
    - Top-selling products

### 3. Storage Layers (Medallion Architecture)

**Bronze (Raw)**: Raw events stored in GCS as Parquet
- Path: `gs://bucket/bronze/events/`
- Partitioned by: `event_date`

**Silver (Cleaned)**: Validated and cleaned events
- Path: `gs://bucket/silver/cleaned_events/`
- Partitioned by: `event_date`, `category`

**Gold (Analytics)**: Aggregated analytics tables
- Stored in BigQuery
- Tables: `daily_sales`, `category_revenue`, `demand_predictions`
- Partitioned by: `event_date`
- Clustered by: `product_id`, `category`

### 4. Batch Processing
- Joins historical data with streaming aggregates
- Creates daily sales and demand tables
- Calculates category growth metrics

### 5. ML Pipeline

**Feature Engineering**:
- Historical sales (rolling averages: 7d, 14d, 30d)
- Temporal features (day of week, month, seasonality)
- Category features
- Product-specific features

**Model Training**:
- Baseline: Linear Regression
- Advanced: XGBoost Regressor

**Inference**:
- Predicts next-day demand
- Predicts next-week demand
- Saves predictions to BigQuery

### 6. Orchestration (Airflow)
- Daily DAG runs:
  1. Simulate Kafka events
  2. Run batch processing
  3. Train ML models (weekly)
  4. Generate predictions
  5. Run analytics queries

## 🔧 Configuration

Edit `configs/config.yaml` to customize:

- **Kafka**: Bootstrap servers, topics, consumer groups
- **GCP**: Project ID, bucket names, dataset IDs
- **Spark**: App name, master URL, checkpoint locations
- **ML**: Model parameters, feature lists
- **Airflow**: DAG schedules, retry policies

## 📈 Analytics Queries

Run BigQuery analytics:

```bash
python sql/run_analytics.py
```

Available queries:
1. **Daily Revenue**: Revenue trends with growth metrics
2. **Category Growth**: Category-wise revenue analysis
3. **Demand vs Actual**: Compare predictions with actual sales
4. **Top Selling Products**: Product performance metrics

## 🐳 Docker Deployment

### Local Development

```bash
cd docker
docker-compose up -d
```

Access services:
- **Kafka UI**: http://localhost:8080
- **Jupyter**: http://localhost:8888
- **Airflow**: http://localhost:8081 (user: airflow, password: airflow)

### Production Deployment

Build and push images:

```bash
# Spark image
docker build -f docker/Dockerfile.spark -t gcr.io/PROJECT_ID/ecommerce-spark:latest .
docker push gcr.io/PROJECT_ID/ecommerce-spark:latest

# ML image
docker build -f docker/Dockerfile.ml -t gcr.io/PROJECT_ID/ecommerce-ml:latest .
docker push gcr.io/PROJECT_ID/ecommerce-ml:latest
```

## ☁️ GCP Deployment

### 1. Set Up GCP Resources

```bash
# Create GCS bucket
gsutil mb -p PROJECT_ID -l us-central1 gs://ecommerce-data-lake

# Create BigQuery dataset
bq mk --dataset --location=US PROJECT_ID:ecommerce_analytics
```

### 2. Deploy Spark Jobs to Dataproc

```bash
# Create Dataproc cluster
gcloud dataproc clusters create ecommerce-cluster \
  --region=us-central1 \
  --zone=us-central1-a \
  --master-machine-type=n1-standard-4 \
  --worker-machine-type=n1-standard-4 \
  --num-workers=2

# Submit streaming job
gcloud dataproc jobs submit pyspark \
  --cluster=ecommerce-cluster \
  --region=us-central1 \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar \
  spark/streaming_pipeline.py
```

### 3. Deploy Airflow to Cloud Composer

```bash
# Create Composer environment
gcloud composer environments create ecommerce-airflow \
  --location=us-central1 \
  --python-version=3

# Upload DAGs
gcloud composer environments storage dags import \
  --environment=ecommerce-airflow \
  --location=us-central1 \
  --source=airflow/dags/
```

### 4. Set Up ML Pipeline

```bash
# Train models (Cloud Run or Compute Engine)
gcloud run deploy ml-training \
  --source . \
  --platform managed \
  --region us-central1 \
  --memory 4Gi \
  --timeout 3600
```

## 📝 Event Schema

```json
{
  "event_time": "2024-01-15T10:30:00.000Z",
  "user_id": "U1234",
  "product_id": "P001",
  "category": "Electronics",
  "price": "299.99",
  "quantity": "2",
  "event_type": "order_created"
}
```

## 🧪 Testing

Run tests:

```bash
pytest tests/ -v
```

## 📚 Key Features

✅ **Real-time Stream Processing**: Spark Structured Streaming with Kafka  
✅ **Data Quality**: Validation, schema enforcement, deduplication  
✅ **Scalable Architecture**: Medallion (Bronze/Silver/Gold) data architecture  
✅ **ML Pipeline**: End-to-end ML workflow with feature engineering  
✅ **Orchestration**: Airflow DAGs with retries and error handling  
✅ **Analytics**: SQL queries for business insights  
✅ **Production-ready**: Logging, configuration management, error handling  
✅ **Cloud-native**: GCP integration (GCS, BigQuery)  

## 🎯 Resume-Ready Project Description

**Real-Time E-commerce Analytics & Demand Prediction Pipeline**

Built an end-to-end production-grade data pipeline processing millions of e-commerce events daily. Architected a scalable solution using Apache Kafka for real-time ingestion, Apache Spark Structured Streaming for stream processing, and Google Cloud Platform (GCS, BigQuery) for data storage. Implemented a Medallion architecture (Bronze/Silver/Gold) ensuring data quality through validation, deduplication, and schema enforcement. Developed ML models (XGBoost, Linear Regression) for demand forecasting with feature engineering including rolling averages, temporal features, and category encodings. Orchestrated workflows using Apache Airflow with automated retries and monitoring. Achieved real-time analytics with windowed aggregations (5-min, 1-hour) and batch processing for historical data joins. Technologies: Python, Spark, Kafka, Airflow, BigQuery, GCS, XGBoost, SQL.







