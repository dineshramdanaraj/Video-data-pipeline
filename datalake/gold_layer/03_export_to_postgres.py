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
def create_videos_table(
        create_video_schema: str,
        postgres_conn: pool.SimpleConnectionPool
) -> str:
    table_name = f"{create_video_schema}.videos"
    conn = postgres_conn.getconn()
    cur = conn.cursor()

    try:
        # Create target PostgreSQL table if it doesn't exist
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Video_Path VARCHAR,
            Arrival_Time TIMESTAMP,
            Has_Metadata BOOLEAN,
            Deleted BOOLEAN
        );
        """)
        conn.commit()
    finally:
        cur.close()
        postgres_conn.putconn(conn)
    print("table created - 1")
    return table_name



# %%
def create_videos_process_table(
        create_video_schema: str,
        postgres_conn: pool.SimpleConnectionPool
) -> str:
    table_name = f"{create_video_schema}.video_process"
    conn = postgres_conn.getconn()
    cur = conn.cursor()

    try:
        # Create target PostgreSQL table if it doesn't exist
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Derivative_Path VARCHAR,
            Quality_Rating INTEGER,
            Size_Anamoly BOOLEAN,
            Corruption BOOLEAN,
            Blank_Conntent BOOLEAN
        );
        """)
        conn.commit()
    finally:
        cur.close()
        postgres_conn.putconn(conn)
    print("table created - 2")
    
    return table_name
    


# %%
def export_video_to_postgres(
    create_videos_table: str,
    create_videos_process_table: str,
    postgres_conn: pool.SimpleConnectionPool,
    video: Video
) -> dict:
    """
    Exports video data from a Video object to PostgreSQL with proper schema.
    Creates the table if it doesn't exist and adds data to the existing table.
    """

    table_names = {}
    video_process_table = create_videos_process_table
    video_table = create_videos_table
    conn = postgres_conn.getconn()
    cur = conn.cursor()

    try:
       

        # Insert data from the Video object into the PostgreSQL table
        cur.execute(f"""
        INSERT INTO {video_table} (
            Video_Path,
            Arrival_Time,
            Has_Metadata,
            Deleted
        ) VALUES (%s, %s, %s, %s);
        """, (
            video.path,
            video.arrival_time,
            video.has_metadata,
            video.deleted
        ))
        if video.video_process:
            cur.execute(f"""
        INSERT INTO {video_process_table} (
            Derivative_Path,
            Quality_Rating,
            Size_Anamoly,
            Corruption,
            Blank_Conntent
        ) VALUES (%s, %s, %s, %s, %s);
        """, (
            video.video_process['derivative_path'],
            video.video_process['quality_rating'],
            video.video_process['size_anamoly'],
            video.video_process['corruption'],
            video.video_process['blank_content']
        ))

        conn.commit()
        table_names['video_process'] = video_process_table
        table_names['videos'] = video_table

    finally:
        cur.close()
        postgres_conn.putconn(conn)

    return table_names


