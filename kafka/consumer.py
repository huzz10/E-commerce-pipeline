"""
Kafka Consumer for E-commerce Events (for testing/debugging)

This module provides a simple consumer to verify events are being published correctly.
"""
import json
from kafka import KafkaConsumer
from typing import Iterator, Dict, Any

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.logger import setup_logger
from utils.config_loader import load_config, get_kafka_config

logger = setup_logger(__name__)


class EcommerceEventConsumer:
    """
    Consumer for reading e-commerce events from Kafka.
    """
    
    def __init__(self, bootstrap_servers: str = None, topic: str = None, 
                 consumer_group: str = None):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Kafka topic name
            consumer_group: Consumer group ID
        """
        config = load_config()
        kafka_config = get_kafka_config(config)
        
        self.bootstrap_servers = bootstrap_servers or kafka_config.get('bootstrap_servers', 'localhost:9092')
        self.topic = topic or kafka_config.get('topic', 'ecommerce_events')
        self.consumer_group = consumer_group or kafka_config.get('consumer_group', 'test_consumer')
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            consumer_timeout_ms=10000,  # 10 seconds timeout
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=self.consumer_group,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        logger.info(f"Kafka consumer initialized for topic: {self.topic}")
    
    def consume_events(self, max_messages: int = None) -> Iterator[Dict[str, Any]]:
        """
        Consume events from Kafka.
        
        Args:
            max_messages: Maximum number of messages to consume (None for unlimited)
        
        Yields:
            Event dictionaries
        """
        count = 0
        
        try:
            for message in self.consumer:
                event = message.value
                logger.info(f"Received event: {event}")
                yield event
                
                count += 1
                if max_messages and count >= max_messages:
                    break
                    
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close the Kafka consumer."""
        self.consumer.close()
        logger.info("Kafka consumer closed")


def main():
    """Main function to run the consumer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='E-commerce Kafka Event Consumer')
    parser.add_argument('--max-messages', type=int, help='Maximum messages to consume')
    parser.add_argument('--bootstrap-servers', type=str, help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, help='Kafka topic name')
    
    args = parser.parse_args()
    
    consumer = EcommerceEventConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    try:
        for event in consumer.consume_events(max_messages=args.max_messages):
            print(f"Event: {json.dumps(event, indent=2)}")
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")


if __name__ == "__main__":
    main()

