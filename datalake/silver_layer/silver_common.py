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
# ### Silver Common
# - Contains all the common functions used accross silver layer
#   

# %%
#Common Imports
import json
import os
import subprocess
from typing import Union

from datalake.common_func import Video


# %%
def video_path_fix(video: Video) -> Video:
    video_path = video.path
    video_path = os.path.normpath(video_path)
    video_path = video_path.replace("\\", "/")
    video.path = video_path
    return video


# %%
def ffmpeg_probe(video_path_fix: Video) -> Union[dict, str]:
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        video_path_fix.path
    ]
    
    try:
        output = subprocess.check_output(cmd).decode('utf-8')
        video_info = json.loads(output)
        return video_info
    except subprocess.CalledProcessError:
        return "corrupt"
