"""
Kafka Producer for E-commerce Real-Time Event Simulation

This module simulates real-time e-commerce events and publishes them to Kafka.
Events include: order_created, product_viewed, add_to_cart
"""
import json
import time
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger
from utils.config_loader import load_config, get_kafka_config

logger = setup_logger(__name__)


class EcommerceEventProducer:
    """
    Producer for simulating and publishing e-commerce events to Kafka.
    """
    
    # Sample product catalog
    PRODUCTS = [
        {"product_id": "P001", "category": "Electronics", "price": 299.99},
        {"product_id": "P002", "category": "Electronics", "price": 599.99},
        {"product_id": "P003", "category": "Clothing", "price": 49.99},
        {"product_id": "P004", "category": "Clothing", "price": 79.99},
        {"product_id": "P005", "category": "Home & Garden", "price": 129.99},
        {"product_id": "P006", "category": "Home & Garden", "price": 89.99},
        {"product_id": "P007", "category": "Books", "price": 19.99},
        {"product_id": "P008", "category": "Books", "price": 24.99},
        {"product_id": "P009", "category": "Sports", "price": 149.99},
        {"product_id": "P010", "category": "Sports", "price": 199.99},
    ]
    
    EVENT_TYPES = ["order_created", "product_viewed", "add_to_cart"]
    
    def __init__(self, bootstrap_servers: str = None, topic: str = None):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Kafka topic name
        """
        config = load_config()
        kafka_config = get_kafka_config(config)
        
        self.bootstrap_servers = bootstrap_servers or kafka_config.get('bootstrap_servers', 'localhost:9092')
        self.topic = topic or kafka_config.get('topic', 'ecommerce_events')
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True
        )
        
        logger.info(f"Kafka producer initialized for topic: {self.topic}")
    
    def generate_event(self) -> Dict[str, Any]:
        """
        Generate a random e-commerce event.
        
        Returns:
            Event dictionary with required schema
        """
        event_type = random.choice(self.EVENT_TYPES)
        product = random.choice(self.PRODUCTS)
        user_id = f"U{random.randint(1000, 9999)}"
        
        # Generate event timestamp
        event_time = datetime.now().isoformat()
        
        event = {
            "event_time": event_time,
            "user_id": user_id,
            "product_id": product["product_id"],
            "category": product["category"],
            "price": str(product["price"]),
            "quantity": str(random.randint(1, 5)) if event_type == "order_created" else "1",
            "event_type": event_type
        }
        
        return event
    
    def publish_event(self, event: Dict[str, Any]) -> bool:
        """
        Publish a single event to Kafka.
        
        Args:
            event: Event dictionary to publish
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use product_id as key for partitioning
            key = event.get("product_id")
            
            future = self.producer.send(
                self.topic,
                key=key,
                value=event
            )
            
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Event published - Topic: {record_metadata.topic}, "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to publish event: {e}")
            return False
    
    def simulate_events(self, num_events: int = 100, delay_seconds: float = 1.0):
        """
        Simulate and publish multiple events.
        
        Args:
            num_events: Number of events to generate
            delay_seconds: Delay between events (in seconds)
        """
        logger.info(f"Starting event simulation: {num_events} events")
        
        success_count = 0
        failure_count = 0
        
        for i in range(num_events):
            event = self.generate_event()
            
            if self.publish_event(event):
                success_count += 1
            else:
                failure_count += 1
            
            # Log progress every 10 events
            if (i + 1) % 10 == 0:
                logger.info(f"Published {i + 1}/{num_events} events")
            
            # Delay between events
            time.sleep(delay_seconds)
        
        logger.info(
            f"Event simulation completed - Success: {success_count}, Failed: {failure_count}"
        )
    
    def close(self):
        """Close the Kafka producer."""
        self.producer.close()
        logger.info("Kafka producer closed")


def main():
    """Main function to run the producer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='E-commerce Kafka Event Producer')
    parser.add_argument('--events', type=int, default=100, help='Number of events to generate')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between events (seconds)')
    parser.add_argument('--bootstrap-servers', type=str, help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, help='Kafka topic name')
    
    args = parser.parse_args()
    
    producer = EcommerceEventProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    try:
        producer.simulate_events(num_events=args.events, delay_seconds=args.delay)
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()

