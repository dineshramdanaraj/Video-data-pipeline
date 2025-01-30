from kafka import KafkaConsumer
import json
from typing import Dict, Any

def create_kafka_consumer(bootstrap_servers: list = ['kafka:9092'],  # Use kafka:9092 for Docker
                         topic: str = 'Video_Watcher') -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Start reading from the earliest message
        enable_auto_commit=True,  # Automatically commit offsets
        group_id='video_processor',  # Consumer group ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Deserialize JSON messages
        api_version=(2, 6, 0)  # Explicit Kafka API version
    )

def get_kafka_message() -> Dict[str, Any]:
    consumer = create_kafka_consumer()
    try:
        # Poll for messages
        message_batch = consumer.poll(timeout_ms=1000)  # Wait for 1 second for messages
        if message_batch:
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    print(f"Received message: {message.value}")
                    return {
                        "path": message.value.get("path"),
                        "arrival_time": message.value.get("arrival_time"),
                        "has_metadata": message.value.get("has_metadata", False),
                        "deleted": message.value.get("deleted", False)
                    }
        return None
    finally:
        consumer.close()

if __name__ == "__main__":
    get_kafka_message()