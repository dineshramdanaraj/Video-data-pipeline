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
from poi_datalake.gold_to_db import gold_layer_common
from psycopg2 import pool


# %%
def export_video_to_postgres(
    DAG_CONSTANTS: dict,
    create_video_schema: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    postgres_conn: pool.SimpleConnectionPool,
) -> dict:
    """
    Exports video tables from DuckDB to PostgreSQL with proper schema.
    """
    schema_name = create_video_schema
    table_names = {}

    tables = {"merge_video": "VIDEO"}

    for temp_table_name, staging_key in tables.items():
        table_name = f"{schema_name}.{temp_table_name}"

        duckdb_conn.execute(f"""
        CREATE OR REPLACE TABLE {temp_table_name} AS
        SELECT * FROM parquet_scan('{DAG_CONSTANTS["STAGING"][staging_key]}');
        """)

        conn = postgres_conn.getconn()
        cur = conn.cursor()

        try:
            gold_layer_common.convert_duckdb_to_postgres(
                duckdb_conn=duckdb_conn,
                pg_pool=postgres_conn,
                duckdb_table_name=temp_table_name,
                postgres_table_name=table_name,
            )
            conn.commit()
            table_names[temp_table_name] = table_name

        finally:
            cur.close()
            postgres_conn.putconn(conn)
            duckdb_conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

    return table_names
