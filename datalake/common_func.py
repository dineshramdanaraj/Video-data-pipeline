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
# ### Common Functions
# - Used accross the DATALAKE

# %%
#Common Imports
import os
import smtplib
from dataclasses import dataclass
from email.mime.text import MIMEText
from typing import Optional

from dotenv import load_dotenv


# %%
@dataclass
class VideoProcess:
    size_anamoly: bool
    corruption: bool
    blank_content: bool
    quality_rating: int
    derivative_path: Optional[str] = None


# %%
@dataclass
class Video:
    path: str
    arrival_time: str
    has_metadata: bool = False
    video_process: Optional[VideoProcess] = None
    deleted: bool = False


# %%
def _send_email(subject: str, body: str, to_email: str)-> None:
    #SMTPlib for sending mail
    
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
def read_config()-> dict:
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config
