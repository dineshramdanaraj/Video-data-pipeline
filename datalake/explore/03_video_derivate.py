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
#     display_name: env
#     language: python
#     name: python3
# ---

# %%
import subprocess


# %%
def process_video(input_path:str, output_dir: str)-> str:
    output_path = f"{output_dir}/{input_path.replace('.mp4', '_processed.mp4')}"
    cmd = [
        "ffmpeg",
        "-i", input_path,
        "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2",
        output_path
    ]
    
    subprocess.run(cmd, check=True)
    return output_path

# %%
