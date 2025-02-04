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
# ### Video Check
# - Follows the tasks provided in Description
# - Checks on the 3 parameters (Size, Corruption, Blankness of the content)
# - Assigns rating to video based on quality

# %%
#Common Imports
import os
import subprocess
from typing import Union

from datalake.common_func import Video


# %%
def _is_video_blank(video_path: str) -> bool:
    """
    Checks if a video is blank (e.g., entirely black, entirely white, or without meaningful content).

    Args:
        video_path (str): Path to the video file.

    Returns:
        bool: True if the video is blank, False otherwise.
    """
    try:
        # Use ffmpeg to extract frames and analyze them
        command = [
            "ffmpeg",
            "-i", video_path,  
            "-vf", "format=gray,blackdetect=d=0.1:pix_th=0.1", 
            "-f", "null", "-"
        ]

        # Run the ffmpeg command
        result = subprocess.run(command, stderr=subprocess.PIPE, text=True)

        # Check the output for black frame detection
        return "blackdetect" in result.stderr

    except Exception as e:
        print(f"Error analyzing video for blank content: {e}")
        return False


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
        
        else: 
            if _is_video_blank(video_path):
                results["blank_content"] = True
        
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


