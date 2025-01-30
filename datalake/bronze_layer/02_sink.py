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
# ### Sink
# - Acts as a data sink by converting kafka messages into Video data.

# %%
from datetime import datetime

from datalake.common_func import Video
from typing import Union, Optional
import json


# %%
def kafka_message_to_video(message: Optional[Union[dict, str]]) -> Optional[Video]:
    # Handle None input
    if message is None:
        return None

    # If message is a string, parse it into a dictionary
    if isinstance(message, str):
        message = json.loads(message)
    

    # Ensure message is a dictionary after parsing
    if not isinstance(message, dict):
        raise TypeError(f"Expected a dictionary or JSON string, got {type(message)}")

    # Extract fields and create Video object
    
    video = Video(
            path=message['path'],
            arrival_time=datetime.fromisoformat(message['arrival_time']),
            has_metadata=message.get('has_metadata'), 
            deleted=message.get('deleted')
        )
    

    return video
