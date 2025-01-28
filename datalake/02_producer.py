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
import os
import time
from dataclasses import asdict
from datetime import datetime

from kafka import KafkaProducer
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from datalake.common_func import Video


# %%
class VideoFileHandler(FileSystemEventHandler):
    def __init__(self, kafka_producer: KafkaProducer, topic: str):
        self.producer = kafka_producer
        self.topic = topic
        self.videos = {}  # Store video objects

    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            file_name, file_extension = os.path.splitext(file_path)
            
            if file_extension.lower() in ['.mp4', '.avi', '.mov']:
                arrival_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                has_metadata = os.path.exists(f"{file_name}.json")
                video = Video(path=file_path, arrival_time=arrival_time, has_metadata=has_metadata)
                self.videos[file_path] = video
                print(f"Video file arrived: {video.path} at {video.arrival_time}")
                self.send_to_kafka(video)

    def on_deleted(self, event):
        if not event.is_directory and event.src_path in self.videos:
            video = self.videos[event.src_path]
            video.deleted = True
            print(f"Video file deleted: {video.path}")
            self.send_to_kafka(video)
            del self.videos[event.src_path]

    def on_modified(self, event):
        if not event.is_directory and event.src_path in self.videos:
            video = self.videos[event.src_path]
            video.arrival_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"Video file modified: {video.path} at {video.arrival_time}")
            self.send_to_kafka(video)

    def send_to_kafka(self, video: Video):
        video_json = json.dumps(asdict(video))
        self.producer.send(self.topic, value=video_json.encode('utf-8'))
        self.producer.flush()
        print(f"Sent video data to Kafka topic: {self.topic}")


# %%
def watch_directory(path: str, kafka_producer: KafkaProducer, topic: str)-> None:
    event_handler = VideoFileHandler(kafka_producer, topic)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

# %%
if __name__ == "__main__":
    watch_dir = "C:/Program Files/chop assignment/video_directory/local_sink"
    kafka_bootstrap_servers = ['localhost:9092']
    kafka_topic = 'video_events'

    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    print(f"Watching directory: {watch_dir}")
    watch_directory(watch_dir, kafka_producer=producer, topic=kafka_topic)
