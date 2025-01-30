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
# ---

# %%
from psycopg2 import pool

from datalake.common_func import Video


# %%
def create_videos_table(
        create_video_schema: str,
        postgres_conn: pool.SimpleConnectionPool
) -> str:
    """
    Create the 'videos' table with 'video_path' as the primary key.
    """
    table_name = f"{create_video_schema}.videos"
    conn = postgres_conn.getconn()
    cur = conn.cursor()

    try:
        # Create target PostgreSQL table if it doesn't exist
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Video_Path VARCHAR PRIMARY KEY,  -- Primary key
            Arrival_Time TIMESTAMP,
            Has_Metadata BOOLEAN,
            Deleted BOOLEAN
        );
        """)
        conn.commit()
    finally:
        cur.close()
        postgres_conn.putconn(conn)
    print("Table 'videos' created with 'video_path' as primary key.")
    return table_name

# %%
def create_videos_process_table(
        create_video_schema: str,
        postgres_conn: pool.SimpleConnectionPool
) -> str:
    """
    Create the 'video_process' table with 'video_path' as a foreign key.
    """
    table_name = f"{create_video_schema}.video_process"
    conn = postgres_conn.getconn()
    cur = conn.cursor()

    try:
        # Create target PostgreSQL table if it doesn't exist
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Video_Path VARCHAR PRIMARY KEY,  -- Primary key
            Derivative_Path VARCHAR,
            Quality_Rating INTEGER,
            Size_Anamoly BOOLEAN,
            Corruption BOOLEAN,
            Blank_Content BOOLEAN,
            CONSTRAINT fk_video FOREIGN KEY (Video_Path) 
            REFERENCES {create_video_schema}.videos(Video_Path)  -- Foreign key
        );
        """)
        conn.commit()
    finally:
        cur.close()
        postgres_conn.putconn(conn)
    print("Table 'video_process' created with 'video_path' as foreign key.")
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
    If a row with the same primary key exists, it updates the existing row.
    """
    table_names = {}
    video_process_table = create_videos_process_table
    video_table = create_videos_table
    conn = postgres_conn.getconn()
    cur = conn.cursor()

    try:
        # Insert or update data in the 'videos' table
        cur.execute(f"""
        INSERT INTO {video_table} (
            Video_Path,
            Arrival_Time,
            Has_Metadata,
            Deleted
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (Video_Path) DO UPDATE
        SET
            Arrival_Time = EXCLUDED.Arrival_Time,
            Has_Metadata = EXCLUDED.Has_Metadata,
            Deleted = EXCLUDED.Deleted;
        """, (
            video.path,
            video.arrival_time,
            video.has_metadata,
            video.deleted
        ))

        # Insert or update data in the 'video_process' table if video_process exists
        if video.video_process:
            cur.execute(f"""
            INSERT INTO {video_process_table} (
                Video_Path,
                Derivative_Path,
                Quality_Rating,
                Size_Anamoly,
                Corruption,
                Blank_Content
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (Video_Path) DO UPDATE
            SET
                Derivative_Path = EXCLUDED.Derivative_Path,
                Quality_Rating = EXCLUDED.Quality_Rating,
                Size_Anamoly = EXCLUDED.Size_Anamoly,
                Corruption = EXCLUDED.Corruption,
                Blank_Content = EXCLUDED.Blank_Content;
            """, (
                video.path,
                video.video_process.derivative_path,
                video.video_process.quality_rating,
                video.video_process.size_anamoly,
                video.video_process.corruption,
                video.video_process.blank_content
            ))

        conn.commit()
        table_names['video_process'] = video_process_table
        table_names['videos'] = video_table

    finally:
        cur.close()
        postgres_conn.putconn(conn)

    return table_names
