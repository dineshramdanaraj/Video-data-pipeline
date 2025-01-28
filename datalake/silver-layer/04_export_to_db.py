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
import duckdb
from IPython.display import display

from datalake.common_func import Video


# %%
def create_table_video_process(duckdb_conn: duckdb.DuckDBPyConnection) -> str:
    table_name = "video_process"
    duckdb_conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Video_Path VARCHAR,
            Derivative_Path VARCHAR,
            Quality_Rating INTEGER,
            Corruption BOOLEAN,
            Blank BOOLEAN,
            Size_Anomaly BOOLEAN
        )
    """)
    return table_name


# %%
def import_silver_to_db(duckdb_conn: duckdb.DuckDBPyConnection,
                        DAG_CONSTANTS: dict,
                        video_size_quality_rating: int,
                        check_video_quality: dict,
                        create_table_video_process: str,
                        video: Video,
                        process_video: str) -> str:
    table_name = create_table_video_process
    duckdb_conn.execute(f"""
    INSERT INTO {table_name} (
        Video_Path,
        Derivative_Path,
        Quality_Rating,
        Corruption,
        Blank,
        Size_Anomaly
    ) VALUES (
        ?,
        ?,
        ?,
        ?,
        ?,
        ?
    )
    """, (
        video.path,
        process_video,
        video_size_quality_rating,
        check_video_quality['corruption'],
        check_video_quality['blank_content'],
        check_video_quality['file_size_anomaly']
    ))
    
    return table_name


# %%
def explore_country(
    import_silver_to_db: str, duckdb_conn: duckdb.DuckDBPyConnection
) -> None:
    df = duckdb_conn.execute(f"SELECT * FROM {import_silver_to_db}  LIMIT 5").df()
    display(df)
