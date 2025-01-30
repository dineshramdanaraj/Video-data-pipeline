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
# ## Gold - Layer
# - Hamilton Dag for the Layer

# %%
# So that modules can be reloaded without restarting the kernel
# %reload_ext autoreload
# %autoreload 2


# %%
# Common imports
import importlib
import os
from datetime import datetime

from dotenv import load_dotenv
from hamilton import driver
from hamilton.execution import executors

from datalake.common_func import Video, VideoProcess



# %%
gold_common = importlib.import_module("gold_common")
common_func = importlib.import_module("datalake.common_func")

create_schema = importlib.import_module("02_create_schema")
export_to_postgres = importlib.import_module("03_export_to_postgres")
email  = importlib.import_module("04_email")

# https://github.com/DAGWorks-Inc/hamilton/

try:
    hamilton_driver = (
        driver.Builder()
        .with_modules(gold_common, common_func, create_schema, export_to_postgres, email)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_local_executor(executors.SynchronousLocalTaskExecutor())
        .build()
    )
except Exception as e:
    print(e)

# Visualize all functions
hamilton_driver.display_all_functions(orient="TB")

# %%
load_dotenv()
DAG_CONSTANTS = {
    "EMAIL_RECIEVER": os.getenv("EMAIL_RECIEVER"),
    "PG_CONN": os.getenv("PG_CONN_URI") 
}

video_process = VideoProcess(derivative_path="video_directory/local_staging/4114797-uhd_3840_2160_25fps_processed.mp4",
                             size_anamoly= False,
                             corruption= False,
                             blank_content= False,
                             quality_rating= 4
                             )

video = Video(path="C:/Program Files/chop assignment/video_directory/local_staging/4114797-uhd_3840_2160_25fps.mp4",
arrival_time=datetime(2025, 1, 28, 4, 0),
has_metadata= True,
video_process= video_process,
deleted=False)

# %%
result = hamilton_driver.execute(
    ["logger"],
    inputs={"DAG_CONSTANTS": DAG_CONSTANTS, "video": video},
)
result["logger"]
