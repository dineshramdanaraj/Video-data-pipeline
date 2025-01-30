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
# ### Postgres Schema
# - Creates a Schema in Postgres
# - Used for grouping tables inside a DB in Postgres

# %%
# Common imports
from psycopg2 import pool


# %%
def create_video_schema(postgres_conn: pool.SimpleConnectionPool) -> str:
    # Get connection from pool

    conn = postgres_conn.getconn()
    schema_name = "video"
    try:
        # Create cursor
        cur = conn.cursor()

        # Create schema if it doesn't exist
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

        # Commit the transaction
        conn.commit()
        # Close cursor
        cur.close()
    finally:
        # Return connection to pool
        postgres_conn.putconn(conn)

        # Close all pool connections

    return schema_name
