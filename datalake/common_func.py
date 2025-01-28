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
import os
import smtplib
from dataclasses import dataclass
from email.mime.text import MIMEText
from typing import Optional

import duckdb
from dotenv import load_dotenv


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


# %%
def duckdb_conn() -> duckdb.DuckDBPyConnection:
    """
    Create an in-memory DuckDB connection
    https://duckdb.org/docs/api/python/dbapi.html
    https://duckdb.org/docs/guides/python/multiple_threads.html
    """
    return duckdb.connect(":memory:")


# %%
def _send_email(subject: str, body: str, to_email: str)-> None:
    load_dotenv()
    from_email = os.getenv("EMAIL_USER")
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    smtp_server = smtplib.SMTP(os.getenv('SMTP_SERVER'), os.getenv('SMTP_PORT'))
    smtp_server.starttls()
    smtp_server.login(from_email, os.getenv('EMAIL_PASSWORD'))
    smtp_server.send_message(msg)
    smtp_server.quit()

# %%
