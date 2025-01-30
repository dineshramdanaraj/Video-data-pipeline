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
# ## Bronze - Layer
# - Hamilton Dag for the Layer

# %%
# So that modules can be reloaded without restarting the kernel
# %reload_ext autoreload
# %autoreload 2


# %%
# Common imports
import importlib

from hamilton import driver
from hamilton.execution import executors

# %%
common_func = importlib.import_module("datalake.common_func")

sink = importlib.import_module("02_sink")


# https://github.com/DAGWorks-Inc/hamilton/issues/685

try:
    hamilton_driver = (
        driver.Builder()
        .with_modules(sink, common_func)
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

}
message = {
"path": "C:/Program Files/chop assignment/video_directory/local_staging/sample_video.mp4",
"arrival_time": "2025-01-28T19:00:00",
"has_metadata": True,
"deleted": False
}

# %%
result = hamilton_driver.execute(
    ["kafka_message_to_video"],
    inputs={"message": message},
)
result["kafka_message_to_video"]

# %%
