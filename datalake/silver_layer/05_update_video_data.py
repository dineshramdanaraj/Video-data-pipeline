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
# ### Update Video Data
# - based on the previous 2 files (derivative and video_check)
# - The file update the Video data by creating a Video Process class and ssigning the resultant values
#   

# %%
from typing import Union, Optional

from datalake.common_func import Video, VideoProcess


# %%

def update_video_data(video_path_fix: Video,
                        video_size_quality_rating: int,
                        check_video_quality: dict,
                        process_video: Optional[str]) -> Video:
    if video_path_fix.deleted or not video_path_fix.has_metadata:
        print("skipped")
        return video_path_fix
    else:
        video_process = VideoProcess(derivative_path=process_video, 
                                    size_anamoly=check_video_quality['file_size_anomaly'],
                                    corruption= check_video_quality['corruption'],
                                    blank_content= check_video_quality['blank_content'],
                                    quality_rating= video_size_quality_rating)
        
        video_path_fix.video_process = video_process
        print("processed")
        return video_path_fix

# %%
