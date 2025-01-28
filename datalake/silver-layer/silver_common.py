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
import subprocess
from typing import Union


# %%
def ffmpeg_probe(DAG_CONSTANTS: dict) -> Union[dict, str]:
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        DAG_CONSTANTS['VIDEO_INPUT']
    ]
    
    try:
        output = subprocess.check_output(cmd).decode('utf-8')
        video_info = json.loads(output)
        return video_info
    except subprocess.CalledProcessError:
        return "corrupt"


# %%
