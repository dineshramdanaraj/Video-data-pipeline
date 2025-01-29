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


# %%
def postgres_conn(DAG_CONSTANTS: dict) -> pool.SimpleConnectionPool:
    """
    Creates a PostgreSQL connection pool.

    Args:
        pg_connection_string: Connection string in format

    Returns:
        SimpleConnectionPool object
    """
    # Create connection pool with min=1 and max=20 connections
    pg_connection_string = DAG_CONSTANTS['PG_CONN']

    pg_pool = pool.SimpleConnectionPool(1, 20, pg_connection_string)

    # Test connection
    test_conn = pg_pool.getconn()
    pg_pool.putconn(test_conn)

    return pg_pool

# %%
