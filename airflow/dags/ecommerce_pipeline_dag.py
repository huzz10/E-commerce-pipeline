"""
Airflow DAG for E-commerce Data Pipeline

This DAG orchestrates:
1. Kafka producer simulation
2. Spark streaming pipeline (runs continuously)
3. Spark batch processing jobs
4. ML model training
5. ML inference
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from utils.logger import setup_logger

logger = setup_logger(__name__)

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['data-engineer@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ecommerce_pipeline',
    default_args=default_args,
    description='E-commerce Real-Time Analytics & Demand Prediction Pipeline',
    schedule_interval='@daily',  # Run daily
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'streaming', 'ml', 'analytics'],
)


def run_kafka_producer(**context):
    """Run Kafka producer to simulate events."""
    import subprocess
    from pathlib import Path
    
    project_root = Path(__file__).parent.parent.parent
    producer_script = project_root / "kafka" / "producer.py"
    
    cmd = [
        "python",
        str(producer_script),
        "--events", "1000",
        "--delay", "0.1"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Kafka producer failed: {result.stderr}")
    
    logger.info("Kafka producer completed successfully")
    return "Success"


def run_spark_batch_job(**context):
    """Run Spark batch processing job."""
    import subprocess
    from pathlib import Path
    
    project_root = Path(__file__).parent.parent.parent
    batch_script = project_root / "spark" / "batch_processing.py"
    historical_csv = project_root / "data.csv"
    
    cmd = [
        "spark-submit",
        "--master", "local[*]",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        str(batch_script),
        "--historical-csv", str(historical_csv)
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Spark batch job failed: {result.stderr}")
    
    logger.info("Spark batch job completed successfully")
    return "Success"


def run_ml_training(**context):
    """Run ML model training."""
    import subprocess
    from pathlib import Path
    
    project_root = Path(__file__).parent.parent.parent
    train_script = project_root / "ml" / "train.py"
    
    # Use daily_sales data from Gold layer (or Silver layer)
    # In production, this would be loaded from BigQuery or GCS
    data_path = project_root / "data" / "daily_sales.parquet"
    
    if not data_path.exists():
        logger.warning(f"Training data not found at {data_path}, skipping training")
        return "Skipped - no training data"
    
    cmd = [
        "python",
        str(train_script),
        "--data", str(data_path),
        "--target", "daily_quantity"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"ML training failed: {result.stderr}")
    
    logger.info("ML training completed successfully")
    return "Success"


def run_ml_inference(**context):
    """Run ML inference for demand prediction."""
    import subprocess
    from pathlib import Path
    
    project_root = Path(__file__).parent.parent.parent
    inference_script = project_root / "ml" / "inference.py"
    
    # Use historical data
    data_path = project_root / "data" / "daily_sales.parquet"
    
    if not data_path.exists():
        logger.warning(f"Inference data not found at {data_path}, skipping inference")
        return "Skipped - no inference data"
    
    cmd = [
        "python",
        str(inference_script),
        "--data", str(data_path),
        "--model", "xgboost",
        "--horizon", "next_day",
        "--save-bq"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"ML inference failed: {result.stderr}")
    
    logger.info("ML inference completed successfully")
    return "Success"


# Task 1: Simulate Kafka events (runs for a short period)
kafka_producer_task = PythonOperator(
    task_id='simulate_kafka_events',
    python_callable=run_kafka_producer,
    dag=dag,
)

# Task 2: Run Spark batch processing
spark_batch_task = PythonOperator(
    task_id='spark_batch_processing',
    python_callable=run_spark_batch_job,
    dag=dag,
)

# Task 3: Train ML models
ml_training_task = PythonOperator(
    task_id='ml_model_training',
    python_callable=run_ml_training,
    dag=dag,
)

# Task 4: Run ML inference
ml_inference_task = PythonOperator(
    task_id='ml_demand_prediction',
    python_callable=run_ml_inference,
    dag=dag,
)

# Task 5: Run BigQuery analytics queries
bigquery_analytics_task = BashOperator(
    task_id='bigquery_analytics',
    bash_command='python {{ params.script_path }}',
    params={'script_path': str(Path(__file__).parent.parent.parent / "sql" / "run_analytics.py")},
    dag=dag,
)

# Define task dependencies
kafka_producer_task >> spark_batch_task
spark_batch_task >> ml_training_task
ml_training_task >> ml_inference_task
ml_inference_task >> bigquery_analytics_task

