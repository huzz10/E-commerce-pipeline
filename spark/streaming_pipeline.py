"""
Spark Structured Streaming Pipeline for E-commerce Events

This module processes real-time e-commerce events from Kafka, performs:
- Data validation
- Schema enforcement
- Deduplication
- Windowed aggregations
- Writes to GCS (Bronze/Silver) and BigQuery (Gold)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as spark_sum, count, 
    avg, max as spark_max, min as spark_min,
    current_timestamp, to_timestamp, date_format,
    expr, when, isnan, isnull, lit, row_number
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)
from pyspark.sql.window import Window

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger
from utils.config_loader import load_config, get_spark_config, get_kafka_config, get_gcp_config

logger = setup_logger(__name__)


class EcommerceStreamingPipeline:
    """
    Spark Structured Streaming pipeline for processing e-commerce events.
    """
    
    # Define schema for e-commerce events
    EVENT_SCHEMA = StructType([
        StructField("event_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("event_type", StringType(), True)
    ])
    
    def __init__(self):
        """Initialize Spark session and load configurations."""
        config = load_config()
        spark_config = get_spark_config(config)
        kafka_config = get_kafka_config(config)
        gcp_config = get_gcp_config(config)
        
        self.app_name = spark_config.get('app_name', 'EcommerceStreamingPipeline')
        self.master = spark_config.get('master', 'local[*]')
        self.checkpoint_location = spark_config.get('checkpoint_location', '/tmp/checkpoints')
        self.batch_interval = spark_config.get('batch_interval', '60 seconds')
        
        self.kafka_bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')
        self.kafka_topic = kafka_config.get('topic', 'ecommerce_events')
        
        self.bronze_path = gcp_config.get('bronze_path', 'gs://ecommerce-data-lake/bronze/events')
        self.silver_path = gcp_config.get('silver_path', 'gs://ecommerce-data-lake/silver/cleaned_events')
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        logger.info("Spark Streaming Pipeline initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        spark = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.master) \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def read_from_kafka(self):
        """
        Read streaming data from Kafka.
        
        Returns:
            DataFrame with Kafka source
        """
        logger.info(f"Reading from Kafka topic: {self.kafka_topic}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        return df
    
    def parse_events(self, kafka_df):
        """
        Parse JSON events from Kafka and apply schema.
        
        Args:
            kafka_df: Kafka source DataFrame
        
        Returns:
            Parsed DataFrame with event schema
        """
        logger.info("Parsing events from Kafka")
        
        # Extract value and parse JSON
        df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("value"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        )
        
        # Parse JSON with schema
        df = df.withColumn(
            "parsed_value",
            from_json(col("value"), self.EVENT_SCHEMA)
        )
        
        # Flatten the struct
        df = df.select(
            col("kafka_key"),
            col("kafka_timestamp"),
            col("partition"),
            col("offset"),
            col("parsed_value.event_time").alias("event_time"),
            col("parsed_value.user_id").alias("user_id"),
            col("parsed_value.product_id").alias("product_id"),
            col("parsed_value.category").alias("category"),
            col("parsed_value.price").alias("price"),
            col("parsed_value.quantity").alias("quantity"),
            col("parsed_value.event_type").alias("event_type")
        )
        
        return df
    
    def validate_and_clean(self, df):
        """
        Validate and clean the data.
        
        Args:
            df: Input DataFrame
        
        Returns:
            Cleaned DataFrame
        """
        logger.info("Validating and cleaning data")
        
        # Convert event_time to timestamp
        df = df.withColumn(
            "event_timestamp",
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
        )
        
        # Convert price and quantity to numeric
        df = df.withColumn(
            "price_numeric",
            col("price").cast("double")
        ).withColumn(
            "quantity_numeric",
            col("quantity").cast("integer")
        )
        
        # Add event_date for partitioning
        df = df.withColumn(
            "event_date",
            date_format(col("event_timestamp"), "yyyy-MM-dd")
        )
        
        # Validate required fields
        df = df.filter(
            col("user_id").isNotNull() &
            col("product_id").isNotNull() &
            col("event_type").isNotNull() &
            col("event_timestamp").isNotNull()
        )
        
        # Filter out invalid prices/quantities
        df = df.filter(
            (col("price_numeric").isNotNull()) &
            (col("price_numeric") >= 0) &
            (col("quantity_numeric").isNotNull()) &
            (col("quantity_numeric") > 0)
        )
        
        # Add calculated fields
        df = df.withColumn(
            "revenue",
            col("price_numeric") * col("quantity_numeric")
        )
        
        return df
    
    def deduplicate(self, df):
        """
        Remove duplicate events based on (user_id, product_id, event_time).
        
        Args:
            df: Input DataFrame
        
        Returns:
            Deduplicated DataFrame
        """
        logger.info("Deduplicating events")
        
        window_spec = Window.partitionBy(
            "user_id", "product_id", "event_time"
        ).orderBy(col("kafka_timestamp").desc())
        
        df = df.withColumn("row_num", row_number().over(window_spec))
        df = df.filter(col("row_num") == 1).drop("row_num")
        
        return df
    
    def aggregate_revenue_by_product(self, df):
        """
        Calculate revenue per product with windowed aggregations.
        
        Args:
            df: Input DataFrame
        
        Returns:
            Aggregated DataFrame
        """
        logger.info("Aggregating revenue by product")
        
        # Filter only order_created events for revenue calculation
        orders_df = df.filter(col("event_type") == "order_created")
        
        # Windowed aggregations: 5-minute and 1-hour windows
        windowed_5min = window(
            col("event_timestamp"),
            "5 minutes"
        )
        
        windowed_1hour = window(
            col("event_timestamp"),
            "1 hour"
        )
        
        # 5-minute aggregations
        agg_5min = orders_df \
            .withWatermark("event_timestamp", "10 minutes") \
            .groupBy(
                windowed_5min,
                col("product_id"),
                col("category")
            ) \
            .agg(
                spark_sum("revenue").alias("total_revenue_5min"),
                count("*").alias("order_count_5min"),
                avg("price_numeric").alias("avg_price_5min")
            ) \
            .select(
                col("window.start").alias("window_start_5min"),
                col("window.end").alias("window_end_5min"),
                col("product_id"),
                col("category"),
                col("total_revenue_5min"),
                col("order_count_5min"),
                col("avg_price_5min")
            )
        
        # 1-hour aggregations
        agg_1hour = orders_df \
            .withWatermark("event_timestamp", "2 hours") \
            .groupBy(
                windowed_1hour,
                col("product_id"),
                col("category")
            ) \
            .agg(
                spark_sum("revenue").alias("total_revenue_1hour"),
                count("*").alias("order_count_1hour"),
                avg("price_numeric").alias("avg_price_1hour")
            ) \
            .select(
                col("window.start").alias("window_start_1hour"),
                col("window.end").alias("window_end_1hour"),
                col("product_id"),
                col("category"),
                col("total_revenue_1hour"),
                col("order_count_1hour"),
                col("avg_price_1hour")
            )
        
        return agg_5min, agg_1hour
    
    def aggregate_revenue_by_category(self, df):
        """
        Calculate revenue per category.
        
        Args:
            df: Input DataFrame
        
        Returns:
            Aggregated DataFrame
        """
        logger.info("Aggregating revenue by category")
        
        orders_df = df.filter(col("event_type") == "order_created")
        
        windowed_1hour = window(
            col("event_timestamp"),
            "1 hour"
        )
        
        agg = orders_df \
            .withWatermark("event_timestamp", "2 hours") \
            .groupBy(
                windowed_1hour,
                col("category")
            ) \
            .agg(
                spark_sum("revenue").alias("total_revenue"),
                count("*").alias("order_count"),
                count("product_id").alias("unique_products")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("category"),
                col("total_revenue"),
                col("order_count"),
                col("unique_products")
            )
        
        return agg
    
    def write_to_bronze(self, df, output_path: str = None):
        """
        Write raw events to GCS Bronze layer.
        
        Args:
            df: Input DataFrame
            output_path: GCS output path
        """
        output_path = output_path or self.bronze_path
        
        logger.info(f"Writing to Bronze layer: {output_path}")
        
        query = df.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{self.checkpoint_location}/bronze") \
            .partitionBy("event_date") \
            .outputMode("append") \
            .trigger(processingTime=self.batch_interval) \
            .start()
        
        return query
    
    def write_to_silver(self, df, output_path: str = None):
        """
        Write cleaned events to GCS Silver layer.
        
        Args:
            df: Input DataFrame
            output_path: GCS output path
        """
        output_path = output_path or self.silver_path
        
        logger.info(f"Writing to Silver layer: {output_path}")
        
        query = df.writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", f"{self.checkpoint_location}/silver") \
            .partitionBy("event_date", "category") \
            .outputMode("append") \
            .trigger(processingTime=self.batch_interval) \
            .start()
        
        return query
    
    def write_to_console(self, df, output_mode: str = "append"):
        """
        Write DataFrame to console (for testing/debugging).
        
        Args:
            df: Input DataFrame
            output_mode: Output mode (append, complete, update)
        """
        logger.info("Writing to console")
        
        query = df.writeStream \
            .outputMode(output_mode) \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime=self.batch_interval) \
            .start()
        
        return query
    
    def run_pipeline(self):
        """Run the complete streaming pipeline."""
        logger.info("Starting streaming pipeline")
        
        try:
            # Read from Kafka
            kafka_df = self.read_from_kafka()
            
            # Parse events
            parsed_df = self.parse_events(kafka_df)
            
            # Validate and clean
            cleaned_df = self.validate_and_clean(parsed_df)
            
            # Deduplicate
            deduplicated_df = self.deduplicate(cleaned_df)
            
            # Write to Bronze (raw events)
            bronze_query = self.write_to_bronze(deduplicated_df)
            
            # Write to Silver (cleaned events)
            silver_query = self.write_to_silver(deduplicated_df)
            
            # Calculate aggregations
            agg_5min, agg_1hour = self.aggregate_revenue_by_product(deduplicated_df)
            agg_category = self.aggregate_revenue_by_category(deduplicated_df)
            
            # Write aggregations to console (can be modified to write to BigQuery)
            agg_query_5min = self.write_to_console(agg_5min)
            agg_query_1hour = self.write_to_console(agg_1hour)
            agg_query_category = self.write_to_console(agg_category)
            
            logger.info("Streaming pipeline started. Waiting for termination...")
            
            # Wait for all queries
            bronze_query.awaitTermination()
            silver_query.awaitTermination()
            agg_query_5min.awaitTermination()
            agg_query_1hour.awaitTermination()
            agg_query_category.awaitTermination()
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()


def main():
    """Main function to run the pipeline."""
    pipeline = EcommerceStreamingPipeline()
    pipeline.run_pipeline()


if __name__ == "__main__":
    main()

