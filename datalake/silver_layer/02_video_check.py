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
import os
from typing import Union

from datalake.common_func import Video


# %%
def check_video_quality(DAG_CONSTANTS: dict,video_path_fix: Video, ffmpeg_probe: Union[dict, str])-> dict:
    results = {
        "corruption": False,
        "blank_content": False,
        "file_size_anomaly": False
    }
    video_path = video_path_fix.path
    # Check file size
    file_size = os.path.getsize(video_path)
    if file_size < 1000:  # Arbitrary threshold, adjust as needed
        results["file_size_anomaly"] = True
    
    # Use ffprobe to get video information
    
    video_info = ffmpeg_probe
    
    if video_info == "corrupt":
        results["corruption"] = True
    else:
        # Check for corruption
        if "streams" not in video_info or len(video_info["streams"]) == 0:
            results["corruption"] = True
        
        # Check for blank content
        for stream in video_info.get("streams", []):
            if stream["codec_type"] == "video":
                if int(float(stream.get("duration", "0"))) == 0:
                    results["blank_content"] = True
                break
    
    return results



# %%
def video_size_quality_rating(
        DAG_CONSTANTS: dict, 
        check_video_quality: dict, 
        ffmpeg_probe: Union[dict, str],
        video_path_fix: Video,
        threshold: float=0.25)-> int:
    video_path = video_path_fix.path
    video_config = DAG_CONSTANTS['VIDEO_CONFIG']
    video_info = ffmpeg_probe
    if video_info == "corrupt" or check_video_quality["corruption"]:
        return 0
    elif check_video_quality["blank_content"]:
        return 1
    
    rating = 1
    
    # Extract relevant information
    duration = float(video_info['format']['duration'])
    bitrate = float(video_info['format']['bit_rate'])
    resolution = f"{video_info['streams'][0]['width']}x{video_info['streams'][0]['height']}"
    
    if resolution in video_config:
        if bitrate >= video_config[resolution]:
            rating += 2
        elif bitrate >= (1-threshold)*(video_config[resolution]):
            rating += 1
    
    # Calculate expected file size
    expected_size = (bitrate * duration) / 8  # Convert bits to bytes
    
    # Get actual file size
    actual_size = os.path.getsize(video_path)
    
    # Compare sizes
    size_ratio = actual_size / expected_size
    
    if (1 - threshold) < size_ratio < (1 + threshold):
        rating += 1
    
    return rating


