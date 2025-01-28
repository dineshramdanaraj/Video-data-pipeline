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
from psycopg2 import pool

from datalake.common_func import Video


# %%
def export_video_to_postgres(
    create_video_schema: str,
    postgres_conn: pool.SimpleConnectionPool,
    video: Video
) -> dict:
    """
    Exports video data from a Video object to PostgreSQL with proper schema.
    Creates the table if it doesn't exist and adds data to the existing table.
    """
    schema_name = create_video_schema
    table_names = {}

    # Define target PostgreSQL table name
    table_name = f"{schema_name}.video_process"

    conn = postgres_conn.getconn()
    cur = conn.cursor()

    try:
        # Create target PostgreSQL table if it doesn't exist
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Video_Path VARCHAR,
            Arrival_Time TIMESTAMP,
            Has_Metadata BOOLEAN,
            Quality_Rating INTEGER,
            Processed BOOLEAN,
            Annotated BOOLEAN,
            Deleted BOOLEAN
        );
        """)

        # Insert data from the Video object into the PostgreSQL table
        cur.execute(f"""
        INSERT INTO {table_name} (
            Video_Path,
            Arrival_Time,
            Has_Metadata,
            Quality_Rating,
            Processed,
            Annotated,
            Deleted
        ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (
            video.path,
            video.arrival_time,
            video.has_metadata,
            video.quality_rating,
            video.processed,
            video.annotated,
            video.deleted
        ))

        conn.commit()
        table_names["video_process"] = table_name

    finally:
        cur.close()
        postgres_conn.putconn(conn)

    return table_names



# %%
