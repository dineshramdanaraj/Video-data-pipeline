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
from datalake.common_func import Video, _send_email


# %%
def _delete_notification(DAG_CONSTANTS: dict, video: Video)-> None:
    subject = "Deletion of a video"
    body = f"The existing video  {video.path} \n has been deleted from the directory"
    _send_email(subject= subject, body= body, to_email= DAG_CONSTANTS['EMAIL_RECIEVER'])


# %%
def _create_notification(DAG_CONSTANTS: dict, video: Video)-> None:
    subject = "Video Ready for Annotation"
    body = f"A new video file {video.path} \n has been processed and is ready for annotation."
    _send_email(subject= subject, body= body, to_email= DAG_CONSTANTS['EMAIL_RECIEVER'])


# %%
def _modification_notification(DAG_CONSTANTS: dict, video: Video)-> None:
    subject = "Modification of a video"
    body = f"The existing video  {video.path} \n has been modified in the directory"
    _send_email(subject= subject, body= body, to_email= DAG_CONSTANTS['EMAIL_RECIEVER'])


# %%
def logger(DAG_CONSTANTS: dict, export_video_to_postgres: dict,video: Video) -> str:
    table_names = export_video_to_postgres
    if video.deleted:
        _delete_notification(DAG_CONSTANTS=DAG_CONSTANTS, video=video)
    elif video.processed:
        _modification_notification(DAG_CONSTANTS=DAG_CONSTANTS, video=video)
    else:
        _create_notification(DAG_CONSTANTS=DAG_CONSTANTS, video=video)
    return f"Video logged into {table_names.keys()}"


