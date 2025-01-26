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
import smtplib
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv
load_dotenv()

# %%
# Example usage
subject_metadata = "Metadata Missing for Video File"
body_metadata = "The metadata file is missing for a recently uploaded video. Please create and upload the metadata file."

subject_annotation = "Video Ready for Annotation"
body_annotation = "A new video file has been processed and is ready for annotation."


# %%
def send_email(subject: str, body: str, to_email: str)-> None:
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
