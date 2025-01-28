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
from datalake.common_func import Video, VideoProcess


# %%
def update_video_data(video: Video,
                        video_size_quality_rating: int,
                        check_video_quality: dict,
                        process_video: str) -> VideoProcess:
    video_process = VideoProcess(derivative_path=process_video, 
                                 size_anamoly=check_video_quality['file_size_anomaly'],
                                 corruption= check_video_quality['corruption'],
                                 blank_content= check_video_quality['blank_content'],
                                 quality_rating= video_size_quality_rating)
    
    video.video_process = video_process
    print("done")
    return video


# %%
