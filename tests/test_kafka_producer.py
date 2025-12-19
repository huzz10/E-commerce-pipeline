"""
Unit tests for Kafka Producer
"""
import pytest
from unittest.mock import Mock, patch
from kafka.producer import EcommerceEventProducer


class TestEcommerceEventProducer:
    """Test cases for EcommerceEventProducer"""
    
    @patch('kafka.producer.KafkaProducer')
    def test_producer_initialization(self, mock_kafka_producer):
        """Test producer initialization"""
        producer = EcommerceEventProducer(
            bootstrap_servers='localhost:9092',
            topic='test_topic'
        )
        assert producer.bootstrap_servers == 'localhost:9092'
        assert producer.topic == 'test_topic'
        mock_kafka_producer.assert_called_once()
    
    def test_generate_event(self):
        """Test event generation"""
        producer = EcommerceEventProducer()
        event = producer.generate_event()
        
        assert 'event_time' in event
        assert 'user_id' in event
        assert 'product_id' in event
        assert 'category' in event
        assert 'price' in event
        assert 'quantity' in event
        assert 'event_type' in event
        assert event['event_type'] in ['order_created', 'product_viewed', 'add_to_cart']
    
    @patch('kafka.producer.KafkaProducer')
    def test_publish_event(self, mock_kafka_producer):
        """Test event publishing"""
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_future.get.return_value = Mock(partition=0, offset=100, topic='test_topic')
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = EcommerceEventProducer()
        event = producer.generate_event()
        result = producer.publish_event(event)
        
        assert result is True
        mock_producer_instance.send.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

