"""
Spark Batch Processing Jobs

This module handles batch processing tasks:
- Joining historical data with streaming aggregates
- Creating daily sales and demand tables
- Data quality checks
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max,
    date_format, to_date, when, coalesce, lit
)
from pyspark.sql.types import DateType

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger
from utils.config_loader import load_config, get_spark_config, get_gcp_config

logger = setup_logger(__name__)


class BatchProcessingPipeline:
    """
    Batch processing pipeline for historical and aggregated data.
    """
    
    def __init__(self):
        """Initialize Spark session and load configurations."""
        config = load_config()
        spark_config = get_spark_config(config)
        gcp_config = get_gcp_config(config)
        
        self.app_name = "EcommerceBatchProcessing"
        self.master = spark_config.get('master', 'local[*]')
        
        self.silver_path = gcp_config.get('silver_path', 'gs://ecommerce-data-lake/silver/cleaned_events')
        self.gold_path = gcp_config.get('gold_path', 'gs://ecommerce-data-lake/gold/aggregated')
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        logger.info("Batch Processing Pipeline initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        spark = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def load_historical_data(self, csv_path: str):
        """
        Load historical data from CSV.
        
        Args:
            csv_path: Path to CSV file
        
        Returns:
            DataFrame with historical data
        """
        logger.info(f"Loading historical data from: {csv_path}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_path)
        
        # Transform to match our schema
        df = df.select(
            col("InvoiceDate").alias("event_time"),
            col("CustomerID").cast("string").alias("user_id"),
            col("StockCode").alias("product_id"),
            col("Description").alias("product_description"),
            col("Quantity").cast("integer").alias("quantity"),
            col("UnitPrice").cast("double").alias("price"),
            col("Country").alias("country")
        )
        
        # Add event_date and calculate revenue
        df = df.withColumn(
            "event_date",
            to_date(col("event_time"), "M/d/yyyy H:mm")
        )
        
        df = df.withColumn(
            "revenue",
            col("price") * col("quantity")
        )
        
        # Add category (simplified - in production, use product catalog)
        df = df.withColumn(
            "category",
            lit("Unknown")  # Placeholder - should map from product catalog
        )
        
        return df
    
    def load_streaming_aggregates(self, silver_path: str = None):
        """
        Load cleaned streaming data from Silver layer.
        
        Args:
            silver_path: Path to Silver layer data
        
        Returns:
            DataFrame with streaming aggregates
        """
        silver_path = silver_path or self.silver_path
        
        logger.info(f"Loading streaming aggregates from: {silver_path}")
        
        df = self.spark.read.parquet(silver_path)
        
        return df
    
    def create_daily_sales_table(self, df):
        """
        Create daily sales aggregation table.
        
        Args:
            df: Input DataFrame with sales data
        
        Returns:
            Daily sales DataFrame
        """
        logger.info("Creating daily sales table")
        
        daily_sales = df.filter(col("event_type") == "order_created") \
            .groupBy(
                col("event_date"),
                col("product_id"),
                col("category")
            ) \
            .agg(
                spark_sum("revenue").alias("daily_revenue"),
                spark_sum("quantity_numeric").alias("daily_quantity"),
                count("*").alias("daily_orders"),
                avg("price_numeric").alias("avg_price"),
                max("price_numeric").alias("max_price"),
                count("user_id").alias("unique_customers")
            ) \
            .orderBy(col("event_date").desc(), col("daily_revenue").desc())
        
        return daily_sales
    
    def create_category_growth_table(self, df):
        """
        Create category-wise growth metrics.
        
        Args:
            df: Input DataFrame
        
        Returns:
            Category growth DataFrame
        """
        logger.info("Creating category growth table")
        
        category_daily = df.filter(col("event_type") == "order_created") \
            .groupBy(
                col("event_date"),
                col("category")
            ) \
            .agg(
                spark_sum("revenue").alias("category_revenue"),
                count("*").alias("category_orders"),
                count("product_id").alias("unique_products"),
                count("user_id").alias("unique_customers")
            )
        
        # Calculate day-over-day growth
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy("category").orderBy("event_date")
        
        category_growth = category_daily \
            .withColumn(
                "prev_day_revenue",
                spark_sum("category_revenue").over(
                    window_spec.rowsBetween(-1, -1)
                )
            ) \
            .withColumn(
                "revenue_growth",
                when(
                    col("prev_day_revenue").isNotNull(),
                    ((col("category_revenue") - col("prev_day_revenue")) / col("prev_day_revenue")) * 100
                ).otherwise(lit(0))
            ) \
            .select(
                col("event_date"),
                col("category"),
                col("category_revenue"),
                col("category_orders"),
                col("unique_products"),
                col("unique_customers"),
                col("revenue_growth")
            ) \
            .orderBy(col("event_date").desc(), col("category_revenue").desc())
        
        return category_growth
    
    def join_historical_streaming(self, historical_df, streaming_df):
        """
        Join historical data with streaming aggregates.
        
        Args:
            historical_df: Historical batch data
            streaming_df: Streaming aggregated data
        
        Returns:
            Joined DataFrame
        """
        logger.info("Joining historical and streaming data")
        
        # Aggregate historical data by date and product
        historical_agg = historical_df \
            .groupBy(
                col("event_date"),
                col("product_id"),
                col("category")
            ) \
            .agg(
                spark_sum("revenue").alias("historical_revenue"),
                spark_sum("quantity").alias("historical_quantity")
            )
        
        # Aggregate streaming data by date and product
        streaming_agg = streaming_df.filter(col("event_type") == "order_created") \
            .groupBy(
                col("event_date"),
                col("product_id"),
                col("category")
            ) \
            .agg(
                spark_sum("revenue").alias("streaming_revenue"),
                spark_sum("quantity_numeric").alias("streaming_quantity")
            )
        
        # Full outer join
        joined_df = historical_agg.join(
            streaming_agg,
            on=["event_date", "product_id", "category"],
            how="full_outer"
        )
        
        # Calculate total revenue and quantity
        joined_df = joined_df.withColumn(
            "total_revenue",
            coalesce(col("historical_revenue"), lit(0)) + 
            coalesce(col("streaming_revenue"), lit(0))
        ).withColumn(
            "total_quantity",
            coalesce(col("historical_quantity"), lit(0)) + 
            coalesce(col("streaming_quantity"), lit(0))
        )
        
        return joined_df
    
    def write_to_gold(self, df, table_name: str, output_path: str = None):
        """
        Write aggregated data to Gold layer (GCS).
        
        Args:
            df: Input DataFrame
            table_name: Name of the table
            output_path: GCS output path
        """
        output_path = output_path or f"{self.gold_path}/{table_name}"
        
        logger.info(f"Writing {table_name} to Gold layer: {output_path}")
        
        df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .parquet(output_path)
        
        logger.info(f"Successfully wrote {table_name} to {output_path}")
    
    def run_batch_job(self, historical_csv_path: str = None):
        """
        Run the complete batch processing job.
        
        Args:
            historical_csv_path: Path to historical CSV data
        """
        logger.info("Starting batch processing job")
        
        try:
            # Load streaming data
            streaming_df = self.load_streaming_aggregates()
            
            # Create daily sales table
            daily_sales = self.create_daily_sales_table(streaming_df)
            self.write_to_gold(daily_sales, "daily_sales")
            
            # Create category growth table
            category_growth = self.create_category_growth_table(streaming_df)
            self.write_to_gold(category_growth, "category_growth")
            
            # If historical data provided, join with streaming
            if historical_csv_path:
                historical_df = self.load_historical_data(historical_csv_path)
                joined_df = self.join_historical_streaming(historical_df, streaming_df)
                self.write_to_gold(joined_df, "historical_streaming_joined")
            
            logger.info("Batch processing job completed successfully")
            
        except Exception as e:
            logger.error(f"Batch job error: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()


def main():
    """Main function to run batch processing."""
    import argparse
    
    parser = argparse.ArgumentParser(description='E-commerce Batch Processing')
    parser.add_argument('--historical-csv', type=str, help='Path to historical CSV file')
    
    args = parser.parse_args()
    
    pipeline = BatchProcessingPipeline()
    pipeline.run_batch_job(historical_csv_path=args.historical_csv)


if __name__ == "__main__":
    main()

