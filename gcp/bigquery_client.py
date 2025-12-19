"""
BigQuery Client for E-commerce Analytics

This module handles BigQuery operations:
- Creating tables
- Loading data from GCS
- Writing streaming aggregates
- Querying analytics
"""
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import List, Dict, Any
import pandas as pd

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger
from utils.config_loader import load_config, get_gcp_config

logger = setup_logger(__name__)


class BigQueryClient:
    """
    Client for BigQuery operations.
    """
    
    def __init__(self, project_id: str = None, dataset_id: str = None):
        """
        Initialize BigQuery client.
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
        """
        config = load_config()
        gcp_config = get_gcp_config(config)
        
        self.project_id = project_id or gcp_config.get('project_id', 'your-gcp-project-id')
        self.dataset_id = dataset_id or gcp_config.get('dataset_id', 'ecommerce_analytics')
        
        self.client = bigquery.Client(project=self.project_id)
        
        # Ensure dataset exists
        self._create_dataset_if_not_exists()
        
        logger.info(f"BigQuery client initialized for project: {self.project_id}, dataset: {self.dataset_id}")
    
    def _create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist."""
        dataset_ref = self.client.dataset(self.dataset_id)
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset.description = "E-commerce analytics dataset"
            
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {self.dataset_id}")
    
    def create_table_from_schema(self, table_name: str, schema: List[bigquery.SchemaField], 
                                 partition_field: str = None, cluster_fields: List[str] = None):
        """
        Create a BigQuery table from schema.
        
        Args:
            table_name: Name of the table
            schema: List of SchemaField objects
            partition_field: Field to partition by
            cluster_fields: Fields to cluster by
        """
        table_ref = self.client.dataset(self.dataset_id).table(table_name)
        
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {table_name} already exists")
            return
        except NotFound:
            pass
        
        table = bigquery.Table(table_ref, schema=schema)
        
        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                field=partition_field,
                type_=bigquery.TimePartitioningType.DAY
            )
        
        if cluster_fields:
            table.clustering_fields = cluster_fields
        
        table = self.client.create_table(table)
        logger.info(f"Created table {table_name}")
    
    def load_from_gcs(self, table_name: str, gcs_uri: str, 
                      schema: List[bigquery.SchemaField] = None,
                      write_disposition: str = "WRITE_APPEND"):
        """
        Load data from GCS to BigQuery.
        
        Args:
            table_name: Target table name
            gcs_uri: GCS URI (gs://bucket/path)
            schema: Table schema (optional)
            write_disposition: WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY
        """
        table_ref = self.client.dataset(self.dataset_id).table(table_name)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition
        )
        
        if schema:
            job_config.schema = schema
        
        load_job = self.client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        
        logger.info(f"Loading data from {gcs_uri} to {table_name}")
        load_job.result()  # Wait for job to complete
        
        table = self.client.get_table(table_ref)
        logger.info(f"Loaded {table.num_rows} rows to {table_name}")
    
    def insert_dataframe(self, table_name: str, df: pd.DataFrame, 
                        write_disposition: str = "WRITE_APPEND"):
        """
        Insert pandas DataFrame to BigQuery.
        
        Args:
            table_name: Target table name
            df: pandas DataFrame
            write_disposition: WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY
        """
        table_ref = self.client.dataset(self.dataset_id).table(table_name)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition
        )
        
        job = self.client.load_table_from_dataframe(
            df,
            table_ref,
            job_config=job_config
        )
        
        job.result()  # Wait for job to complete
        logger.info(f"Inserted {len(df)} rows to {table_name}")
    
    def query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as pandas DataFrame.
        
        Args:
            query: SQL query string
        
        Returns:
            pandas DataFrame with results
        """
        logger.info(f"Executing query: {query[:100]}...")
        
        query_job = self.client.query(query)
        results = query_job.result()
        
        df = results.to_dataframe()
        logger.info(f"Query returned {len(df)} rows")
        
        return df
    
    def create_daily_sales_table_schema(self):
        """Create schema for daily_sales table."""
        schema = [
            bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("daily_revenue", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("daily_quantity", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("daily_orders", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("avg_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("max_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("unique_customers", "INTEGER", mode="NULLABLE"),
        ]
        
        self.create_table_from_schema(
            "daily_sales",
            schema,
            partition_field="event_date",
            cluster_fields=["product_id", "category"]
        )
    
    def create_demand_predictions_table_schema(self):
        """Create schema for demand_predictions table."""
        schema = [
            bigquery.SchemaField("prediction_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("predicted_demand", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("prediction_horizon", "STRING", mode="REQUIRED"),  # "next_day" or "next_week"
            bigquery.SchemaField("model_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("model_version", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        self.create_table_from_schema(
            "demand_predictions",
            schema,
            partition_field="prediction_date",
            cluster_fields=["product_id", "category"]
        )


def main():
    """Main function for testing."""
    client = BigQueryClient()
    client.create_daily_sales_table_schema()
    client.create_demand_predictions_table_schema()


if __name__ == "__main__":
    main()

