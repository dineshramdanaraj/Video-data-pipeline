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

# %% [markdown]
# ## Producer For Kafka
# - Uses WatchDog to monitor the local directory
# - Sends message based on the events occuring in the directory

# %%
#Commpon Impirts
import json
import os
import time
from dataclasses import asdict
from datetime import datetime

# %%
from confluent_kafka import Producer
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# %%
from datalake.common_func import Video, read_config


# %%
class VideoFileHandler(FileSystemEventHandler):
    def __init__(self, kafka_producer: Producer, topic: str):
        self.producer = kafka_producer
        self.topic = topic
        self.videos = {}  # Store video objects

    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            file_name, file_extension = os.path.splitext(file_path)
            file_path = os.path.normpath(file_path).replace("\\", "/")  # Normalize the path

            if file_extension.lower() in ['.mp4', '.avi', '.mov']:
                # Handle video files
                arrival_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(f"Checking for metadata file: {file_name}.json")
                has_metadata = os.path.exists(f"{file_name}.json")
                video = Video(path=file_path, arrival_time=arrival_time, has_metadata=has_metadata)
                self.videos[file_path] = video
                print(f"Video file arrived: {video.path} at {video.arrival_time} with {has_metadata}")
                self.send_to_kafka(video)

            elif file_extension.lower() == '.json':
                # Handle JSON files (metadata)
                arrival_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                video_extensions = ['.mp4', '.avi', '.mov']
                for ext in video_extensions:
                    video_file_path = f"{file_name}{ext}"
                    if os.path.exists(video_file_path):
                        # If a corresponding video file exists, create a Video object
                        video = Video(path=video_file_path, arrival_time=arrival_time, has_metadata=True)
                        self.videos[video_file_path] = video
                        print(f"Metadata file arrived: {file_path} for video {video_file_path}")
                        self.send_to_kafka(video)
                        break  
                    


    def on_deleted(self, event):
        file_path = event.src_path
        file_path = os.path.normpath(file_path).replace("\\", "/")
        if not event.is_directory and file_path in self.videos:

            video = self.videos[file_path]
            video.deleted = True
            print(f"Video file deleted: {video.path}")
            self.send_to_kafka(video)
            del self.videos[file_path]

    def on_modified(self, event):

        file_path = event.src_path
        file_path = os.path.normpath(file_path).replace("\\", "/")
        if not event.is_directory and file_path in self.videos:
            video = self.videos[file_path]
            video.arrival_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"Video file modified: {video.path} at {video.arrival_time}")
            self.send_to_kafka(video)

    def send_to_kafka(self, video: Video):
        file_path = os.path.normpath(video.path).replace("\\", "/")
        video.path = file_path
        video_json = json.dumps(asdict(video))
        self.producer.produce(
            topic=self.topic,
            value=video_json.encode('utf-8') 
        )
        self.producer.flush()  # Ensure the message is sent
        print(f"Sent video data to Kafka topic: {self.topic}")

    def delivery_report(self, err, msg):
        """Callback to report the success or failure of message delivery."""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# %%
def watch_directory(path: str, kafka_producer: Producer, topic: str) -> None:
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
    watch_dir = "./video_directory/local_sink"
    config = read_config()
    kafka_topic = "Video_Watcher"
    producer = Producer(config)

    print(f"Watching directory: {watch_dir}")
    watch_directory(watch_dir, kafka_producer=producer, topic=kafka_topic)
