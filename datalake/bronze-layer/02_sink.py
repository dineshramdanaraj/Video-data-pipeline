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
from kafka import KafkaConsumer
from datetime import datetime
from datalake.common_func import Video


# %%
def process_video_arrival(video_data: Video):
    print(f"New video arrived: {video_data['path']} at {video_data['arrival_time']}")
    


# %%
def process_video_deletion(video_data: Video):
    print(f"Video deleted: {video_data['path']}")


# %%
def process_video_modification(video_data: Video):
    print(f"Video modified: {video_data['path']} at {video_data['arrival_time']}")


# %%
def main():
    consumer = KafkaConsumer(
        'video_events',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    previous_arrival_time = None

    for message in consumer:
        video_data = message.value
        current_arrival_time = datetime.strptime(video_data['arrival_time'], '%Y-%m-%d %H:%M:%S')
        
        if video_data['deleted']:
            process_video_deletion(video_data)
        elif previous_arrival_time is None or current_arrival_time > previous_arrival_time:
            if not video_data.get('processed'):
                process_video_arrival(video_data)
            else:
                process_video_modification(video_data)
            
            # Update the previous arrival time pointer
            previous_arrival_time = current_arrival_time



# %%
