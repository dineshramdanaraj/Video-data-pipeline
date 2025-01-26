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
# ---

# %%
import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
import duckdb
from dataclasses import dataclass
from typing import Optional


# %%
@dataclass
class Video:
    path: str
    arrival_time: str
    has_metadata: bool
    quality_rating: Optional[int] = None
    processed: bool = False
    annotated: bool = False


# %%
class VideoFileHandler(FileSystemEventHandler):
    def __init__(self, duckdb_conn: duckdb.DuckDBPyConnection):
        self.duckdb_conn = duckdb_conn

    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            file_name, file_extension = os.path.splitext(file_path)
            
            if file_extension.lower() in ['.mp4', '.avi', '.mov']:
                arrival_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                has_metadata = os.path.exists(f"{file_name}.json")
                video = Video(path=file_path, arrival_time=arrival_time, has_metadata=has_metadata)
                print(f"Video file arrived: {video.path} at {video.arrival_time}")
                self.log_arrival(video)

    def log_arrival(self, video: Video):
        self.duckdb_conn.execute('''INSERT INTO video_log (video_path, arrival_time, has_metadata) 
                    VALUES (?, ?, ?)''', 
                 (video.path, video.arrival_time, video.has_metadata))



# %%
def setup_database(duckdb_conn: duckdb.DuckDBPyConnection):
    duckdb_conn.execute('''CREATE TABLE IF NOT EXISTS video_log
                    (video_path VARCHAR, arrival_time TIMESTAMP, 
                     has_metadata BOOLEAN, quality_rating INTEGER,
                     processed BOOLEAN, annotated BOOLEAN)''')
    

def watch_directory(path: str, duckdb_conn: duckdb.DuckDBPyConnection):
    setup_database(duckdb_conn)
    event_handler = VideoFileHandler(duckdb_conn)
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
    watch_dir = "C:/Program Files/chop assignment/video_directory/local_sink"  # Replace with the actual directory path
    duckdb_conn = duckdb.connect(":memory:")
    print(f"Watching directory: {watch_dir}")
    watch_directory(watch_dir, duckdb_conn=duckdb_conn)

# %%
duckdb_conn.execute(f"SELECT * FROM video_log").df()
