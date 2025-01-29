# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.6
#   kernelspec:
#     display_name: env
#     language: python
#     name: python3
# ---

# %%
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()

# %%
# Example usage
subject_metadata = "Metadata Missing for Video File"
body_metadata = "The metadata file is missing for a recently uploaded video. Please create and upload the metadata file."

subject_annotation = "Video Ready for Annotation"
body_annotation = "A new video file has been processed and is ready for annotation."




# %%


def send_email(subject, body, to_email):
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
send_email("test-2", "helleo chop test", "dineshramdanaraj@ufl.edu")

# %%
