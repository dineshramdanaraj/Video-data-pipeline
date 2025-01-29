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
from datetime import datetime

from datalake.common_func import Video


# %%
def kafka_message_to_video(message: dict) -> Video:
    if message['deleted']:
        video = Video(
            path=message['path'],
            arrival_time=datetime.fromisoformat(message['arrival_time']),
            has_metadata=message['has_metadata'],
            deleted=True
        )
    elif not message['has_metadata']:
        video = Video(
            path=message['path'],
            arrival_time=datetime.fromisoformat(message['arrival_time']),
            has_metadata=False,
            deleted=False
        )
    else:
        video = Video(
            path=message['path'],
            arrival_time=datetime.fromisoformat(message['arrival_time']),
            has_metadata=True,
            deleted=False
        )
    return video



# %%
