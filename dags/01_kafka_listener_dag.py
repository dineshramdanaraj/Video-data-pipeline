from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from confluent_kafka import Consumer, KafkaError

from datalake.common_func import read_config  # Import the read_client function

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_kafka_message():
    """Fetch a message from Kafka using Confluent Kafka Consumer."""
    consumer_config = read_config()
    consumer_config["group.id"] = "python-group-1"
    consumer_config["auto.offset.reset"] = "earliest"
    consumer = Consumer(consumer_config)
    
    consumer.subscribe(['Video_Watcher'])  # Subscribe to the Kafka topic

    try:
        msg = consumer.poll(timeout=10.0)  # Poll for messages with a timeout
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of partition")
            else:
                print(f"Error: {msg.error()}")
            return None
        return msg.value().decode('utf-8')  # Return the message value
    finally:
        consumer.close()

def process_buffered_messages(**context):
    """Process Kafka messages and trigger the video_processing_dag."""
    message = get_kafka_message()
    if message:
        # Read client-related inputs using the read_client function
        
        context['task_instance'].xcom_push(key='kafka_message', value=message)
        return message
    return None

def trigger_processing_dag(**context):
    """Trigger the video_processing_dag with the Kafka message."""
    message = context['task_instance'].xcom_pull(task_ids='process_messages', key='kafka_message')
    if not message:
        return None
    
    trigger = TriggerDagRunOperator(
        task_id=f'trigger_processing_{datetime.now().timestamp()}',
        trigger_dag_id='video_processing_dag',
        conf={'kafka_message': message},
        wait_for_completion=True,
        dag=context['dag']
    )
    trigger.execute(context)

with DAG(
    'kafka_listener_dag',
    default_args=default_args,
    schedule_interval='*/2 * * * *',  
    catchup=False
) as dag:

    process_messages = PythonOperator(
        task_id='process_messages',
        python_callable=process_buffered_messages,
        provide_context=True
    )

    trigger_dag = PythonOperator(
        task_id='trigger_processing_dag',
        python_callable=trigger_processing_dag,
        provide_context=True
    )

    process_messages >> trigger_dag