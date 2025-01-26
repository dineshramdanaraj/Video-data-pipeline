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
# ## ffmpeg subprocess


# %% [markdown]
# ## smtp


# %% [markdown]
# ## connecting sql


# %% [markdown]
# ## connecting in memory data (duckdb)

# %%
from dataclasses import dataclass
from typing import Optional


# %%
@dataclass
class Video:
    path: str
    arrival_time: str
    has_metadata: bool
    quality_rating: Optional[int] = None
    processed: bool = False
    annotated: bool = False
    deleted: bool = False

# %% [markdown]
# ## any kafka code for bot prod and cons


# %% [markdown]
# ##any airflow common

