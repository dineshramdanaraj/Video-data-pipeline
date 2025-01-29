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
import subprocess

from datalake.common_func import Video


# %%
def process_video(video: Video, DAG_CONSTANTS: dict) -> str:
    # Extract file name from input path
    input_path = video.path
    file_name = os.path.basename(input_path)
    output_file = file_name.replace('.mp4', '_processed.mp4')
    output_path = os.path.join(DAG_CONSTANTS['STAGING_DIRECTORY'], output_file)
    
    # Choose the target resolution (e.g., 720p)
    
    cmd = [
        "ffmpeg",
        "-i", input_path,
        "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",
        output_path
    ]
    
    subprocess.run(cmd, check=True)
    return output_path
