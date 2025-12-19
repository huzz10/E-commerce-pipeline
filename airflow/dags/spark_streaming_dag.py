"""
Airflow DAG for Spark Streaming Pipeline

This DAG manages the Spark Structured Streaming pipeline which runs continuously.
In production, this would be managed separately (e.g., Dataproc cluster).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_streaming_pipeline',
    default_args=default_args,
    description='Spark Structured Streaming Pipeline (Continuous)',
    schedule_interval=None,  # Triggered manually or by external scheduler
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'streaming', 'spark'],
)

# Note: In production, Spark Streaming runs continuously
# This DAG is for monitoring/restarting the streaming job
start_streaming_task = BashOperator(
    task_id='start_spark_streaming',
    bash_command='spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 {{ params.script_path }}',
    params={'script_path': 'spark/streaming_pipeline.py'},
    dag=dag,
)

start_streaming_task

