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
import importlib
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv
from hamilton import driver

from datalake.common_func import Video

# %%
load_dotenv()

DAG_CONSTANTS = {
    "SILVER-LAYER": {
        "VIDEO_CONFIG": {
        "256x144": 100000,   # 144p
        "426x240": 250000,   # 240p
        "640x360": 500000,   # 360p
        "854x480": 750000,   # 480p
        "1280x720": 1500000, # 720p
        "1920x1080": 3000000,  # 1080p
        "2560x1440": 6000000,  # 1440p (2K)
        "3840x2160": 13000000,  # 2160p (4K)
        "7680x4320": 52000000  # 4320p (8K)
    },
        "STAGING_DIRECTORY": "./video_directory/local_staging"
    
},
    "GOLD-LAYER":{
        "EMAIL_RECIEVER": os.getenv("EMAIL_RECIEVER"),
        "PG_CONN": os.getenv("PG_CONN_URI") 
}
}

# %%


# Import Hamilton DAG modules

common_func = importlib.import_module(name="common_func", package="datalake")
bronze_sink = importlib.import_module("bronze_layer.02_sink", package="datalake" )

silver_common = importlib.import_module("silver_layer.silver_common", package="datalake")
video_check = importlib.import_module("silver_layer.02_video_check", package="datalake")
derivative = importlib.import_module("silver_layer.03_derivative", package="datalake")
update_video_data = importlib.import_module("silver_layer.05_update_video_data", package="datalake")

gold_common = importlib.import_module("gold_layer.gold_common", package="datalake")
create_schema = importlib.import_module("gold_layer.02_create_schema", package="datalake")
export_to_postgres = importlib.import_module("gold_layer.03_export_to_postgres", package="datalake")
email  = importlib.import_module("gold_layer.04_email", package="datalake")
# gold_layer = importlib.import_module("gold_common")

def consume_kafka_message(**kwargs):
    # Implement Kafka consumer logic here
    message = {
        "path": "./video_directory/local_sink/4114797-uhd_3840_2160_25fps.mp4",
        "arrival_time": "2025-01-28T19:00:00",
        "has_metadata": True,
        "deleted": False
    }
    kwargs['ti'].xcom_push(key='kafka_message', value=message)

def run_bronze_layer(**kwargs):
    ti = kwargs['ti']
    # Add error handling for XCom pull
    message = ti.xcom_pull(task_ids='kafka_consumer_task', key='kafka_message')
    bronze_driver = driver.Builder().with_modules(bronze_sink, common_func).build()
    result = bronze_driver.execute(["kafka_message_to_video"], inputs={"message": message})
    kwargs['ti'].xcom_push(key='video_data', value=result["kafka_message_to_video"])



def run_silver_layer(**kwargs):
    ti = kwargs['ti']
    video_data = ti.xcom_pull(task_ids='bronze_task', key='video_data')
    silver_driver = driver.Builder().with_modules(silver_common,
                                                  common_func,
                                                  video_check,
                                                  derivative,
                                                  update_video_data).build()
    result = silver_driver.execute(["update_video_data"], 
                                   inputs={"video": video_data,
                                           "DAG_CONSTANTS": DAG_CONSTANTS['SILVER-LAYER']})
    kwargs['ti'].xcom_push(key='silver_result', value=result["update_video_data"])

def run_gold_layer(**kwargs):
    ti = kwargs['ti']
    video_data = ti.xcom_pull(task_ids='silver_task', key='silver_result')
    gold_driver = driver.Builder().with_modules(gold_common,
                                                common_func,
                                                create_schema,
                                                export_to_postgres,
                                                email).build()
    result = gold_driver.execute(["logger"], 
                                 inputs={"video": video_data,
                                          "DAG_CONSTANTS": DAG_CONSTANTS['GOLD-LAYER']})
    return result["logger"]




# %%
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# %%



# %%
with DAG('video_processing_dag', default_args=default_args, schedule_interval=None) as dag:
    kafka_consumer_task = PythonOperator(
        task_id='kafka_consumer_task',
        python_callable=consume_kafka_message,
    )

    bronze_task = PythonOperator(
        task_id='bronze_task',
        python_callable=run_bronze_layer,
    )


    silver_task = PythonOperator(
        task_id='silver_task',
        python_callable=run_silver_layer,
    )

    gold_task = PythonOperator(
        task_id='gold_task',
        python_callable=run_gold_layer,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    kafka_consumer_task>> bronze_task >> silver_task >> gold_task
