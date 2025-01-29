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
# ## Silver - Layer

# %%
# So that modules can be reloaded without restarting the kernel
# %reload_ext autoreload
# %autoreload 2


# %%
# Common imports
import importlib
from datetime import datetime

from hamilton import driver
from hamilton.execution import executors

from datalake.common_func import Video



# %%
silver_common = importlib.import_module("silver_common")
common_func = importlib.import_module("datalake.common_func")

video_check = importlib.import_module("02_video_check")
derivative = importlib.import_module("03_derivative")
update_video_data = importlib.import_module("05_update_video_data")


# https://github.com/DAGWorks-Inc/hamilton/issues/685

try:
    hamilton_driver = (
        driver.Builder()
        .with_modules(silver_common, common_func, video_check, derivative,update_video_data)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .build()
    )
except Exception as e:
    print(e)

# Visualize all functions
hamilton_driver.display_all_functions(orient="TB")

# %%
DAG_CONSTANTS = {
    "VIDEO_CONFIG": {
    "256x144": 100000,   # 144p
    "426x240": 250000,   # 240p
    "640x360": 500000,   # 360p
    "854x480": 750000,   # 480p
    "1280x720": 1500000, # 720p
    "1920x1080": 3000000,  # 1080p
    "2560x1440": 6000000,  # 1440p (2K)
    "3840x2160": 13000000,  # 2160p (4K)
    "7680x4320": 52000000  # 4320p (8K)
},
    "STAGING_DIRECTORY": "C:/Program Files/chop assignment/video_directory/local_staging",
    
}


video = Video(path="C:/Program Files/chop assignment/video_directory/local_staging/4114797-uhd_3840_2160_25fps.mp4",
arrival_time=datetime(2025, 1, 28, 4, 0),
has_metadata= True,
deleted=False)


# %%
result = hamilton_driver.execute(
    ["explore_country"],
    inputs={"DAG_CONSTANTS": DAG_CONSTANTS},
)
result["explore_country"]

# %%
result = hamilton_driver.execute(
    ["update_video_data"],
    inputs={"DAG_CONSTANTS": DAG_CONSTANTS, "video": video},
)
result["update_video_data"]

# %%
