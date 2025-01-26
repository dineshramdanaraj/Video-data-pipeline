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
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import os
import json
import subprocess
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# %%
path = Path(os.getenv("WATCH_DIRECTORY"))
print(path)

# %%
VIDEO_CONFIG = {
    "256x144": 100000,   # 144p
    "426x240": 250000,   # 240p
    "640x360": 500000,   # 360p
    "854x480": 750000,   # 480p
    "1280x720": 1500000, # 720p
    "1920x1080": 3000000,  # 1080p
    "2560x1440": 6000000,  # 1440p (2K)
    "3840x2160": 13000000,  # 2160p (4K)
    "7680x4320": 52000000  # 4320p (8K)
}


# %%
def check_video_quality(video_path):
    results = {
        "corruption": False,
        "blank_content": False,
        "file_size_anomaly": False
    }
    
    # Check file size
    file_size = os.path.getsize(video_path)
    if file_size < 1000:  # Arbitrary threshold, adjust as needed
        results["file_size_anomaly"] = True
    
    # Use ffprobe to get video information
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        video_path
    ]
    
    try:
        output = subprocess.check_output(cmd).decode('utf-8')
        video_info = json.loads(output)
        
        # Check for corruption
        if "streams" not in video_info or len(video_info["streams"]) == 0:
            results["corruption"] = True
        
        # Check for blank content
        for stream in video_info.get("streams", []):
            if stream["codec_type"] == "video":
                if int(stream["duration_ts"]) == 0:
                    results["blank_content"] = True
                break
        
    except subprocess.CalledProcessError:
        results["corruption"] = True
    
    return results



# %%
def record_quality_check(video_path, results):
    # This function would typically write to a database
    # For this example, we'll just print the results
    print(f"Quality check results for {video_path}:")
    print(json.dumps(results, indent=2))


# %%
def video_size_quality_rating(video_config: dict, video_path: str, threshold: float=0.25)-> int:
    # Get video information using ffprobe
    ffprobe_cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        video_path
    ]
    rating = 0
    try:
        output = subprocess.check_output(ffprobe_cmd).decode('utf-8')
        video_info = json.loads(output)
    except subprocess.CalledProcessError:
        return "Error: Unable to probe video file"

    # Extract relevant information
    duration = float(video_info['format']['duration'])
    bitrate = float(video_info['format']['bit_rate'])
    resolution = f"{video_info['streams'][0]['width']}x{video_info['streams'][0]['height']}"
    
    if resolution in video_config:
        if bitrate >= video_config[resolution]:
            rating += 2
        elif bitrate >= (1-threshold)*(video_config[resolution]):
            rating+=1
    # Calculate expected file size
    expected_size = (bitrate * duration) / 8  # Convert bits to bytes
    
    # Get actual file size
    actual_size = os.path.getsize(video_path)
    
    # Compare sizes
    size_ratio = actual_size / expected_size
    
    if size_ratio  > (1 - threshold) and size_ratio < (1 + threshold):
        rating+=1
    
    return rating



# %%
# Example usage
video_path = "/video_directory/4114797-uhd_3840_2160_25fps.mp4"
result = check_video_quality(video_path)
print(result)

# %%
video_size_quality_rating(video_config=VIDEO_CONFIG, video_path=video_path)
