# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.6
#   kernelspec:
#     display_name: datalake-NzdzUkV_-py3.12
#     language: python
#     name: python3
# ---

# %%
import json
import logging
from datetime import datetime

from airflow.api.client.local_client import Client
from kafka import KafkaConsumer


# %%

# %%
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# %%
def start_kafka_consumer():
    """
    Kafka consumer that listens to the video_events topic and triggers an Airflow DAG
    when a message (video event) is consumed.
    """
    # Kafka consumer setup
    consumer = KafkaConsumer(
        'video_events',  # Kafka topic to listen to
        bootstrap_servers=['localhost:9092'],  # Kafka bootstrap servers
        group_id='video_event_group',  # Kafka consumer group ID
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))  
        # Deserialize the message to JSON
    )

    # Initialize the Airflow client
    airflow_client = Client()  # Local Airflow client to trigger DAG runs

    logger.info("Kafka Consumer started. Listening for messages...")

    previous_arrival_time = None

    for message in consumer:
        try:
            # Extract and parse the Kafka message
            video_event_data = message.value
            current_arrival_time = datetime.strptime(video_event_data['arrival_time'], '%Y-%m-%d %H:%M:%S')

            logger.info(f"Consumed message: {video_event_data}")
             # Process new or modified videos based on the arrival time logic
            if previous_arrival_time is None or current_arrival_time > previous_arrival_time:
                # Only process if the current video event has a newer arrival time
                logger.info(f"Processing video with path: {video_event_data['path']}")

                # Trigger the Airflow DAG with the video event data
                airflow_client.trigger_dag(
                    dag_id='video_processing_dag',  # The DAG to trigger
                    conf={'message': video_event_data}  # Pass the message as configuration
                )

                # Update the previous arrival time pointer after processing the event
                previous_arrival_time = current_arrival_time

        except Exception as e:
            logger.error(f"Error triggering DAG: {str(e)}")
            continue



# %%
if __name__ == "__main__":
    start_kafka_consumer()
