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
# So that modules can be reloaded without restarting the kernel
# %reload_ext autoreload
# %autoreload 2

# %%
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from common_func import Video, send_email

# %%
logger = logging.getLogger(__name__)


# %%
# Setup logging
def process_video_message(**context):
    """
    This function processes the Kafka message passed as the DAG's configuration (conf).
    """
    # Get the Kafka message passed in the 'conf' parameter
    message = context['dag_run'].conf.get('message')  # Retrieve the video event data
    
    if not message:
        logger.error("No message found in DAG conf!")
        return

    logger.info(f"Processing video event: {message}")

    video = Video(**message)

    if video.deleted:
        ...
    
    if not video.has_metadata:
        subject = "Missing metadata"
        body = f" The video {video.path}, is missing metadata. \n Please add the metadata to process the video"
        send_email(subject= subject, body= body, to_email= os.getenv("EMAIL"))

    else:
    # Log the video event details (e.g., path, arrival_time)
        logger.info(f"Video path: {message['path']}")
        logger.info(f"Video arrival time: {message['arrival_time']}")


    # Return an example message to mark the task as complete
        return f"Processed video event: {message['path']}"




# %%
with DAG(
    'video_processing_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # No schedule, triggered by Kafka consumer
    catchup=False  # Disable backfilling
) as dag:

    process_video_task = PythonOperator(
        task_id='process_video',
        python_callable=process_video_message,
        provide_context=True  # Ensure context is passed to the callable
    )
