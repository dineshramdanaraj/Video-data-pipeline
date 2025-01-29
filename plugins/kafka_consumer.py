# plugins/kafka_consumer.py
from kafka import KafkaConsumer
import json
import os
from typing import Dict, Any
from dotenv import load_dotenv

def create_kafka_consumer(bootstrap_servers: list=['localhost:9092'],
                         topic: str='video_events') -> KafkaConsumer:
    """Create a Kafka consumer instance."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='video_processor',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def consume_message(consumer: KafkaConsumer) -> Dict[str, Any]:
    """Consume a single message from Kafka."""
    try:
        # Get the next message
        message = next(consumer)
        if message:
            return {
                "path": message.value.get("path"),
                "arrival_time": message.value.get("arrival_time"),
                "has_metadata": message.value.get("has_metadata", False),
                "deleted": message.value.get("deleted", False)
            }
    except StopIteration:
        return None
    except Exception as e:
        print(f"Error consuming message: {str(e)}")
        return None

def get_kafka_message() -> Dict[str, Any]:
    """Get a message from Kafka for Airflow task."""
    load_dotenv()
    consumer = create_kafka_consumer()
    try:
        message = consume_message(consumer)
        return message
    finally:
        consumer.close()
