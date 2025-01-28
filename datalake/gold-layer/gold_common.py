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
import contextlib
import json
import os
import time
from datetime import date, datetime

import duckdb
import psycopg2
from dotenv import load_dotenv
from psycopg2 import pool


# %%
def postgres_conn() -> pool.SimpleConnectionPool:
    """
    Creates a PostgreSQL connection pool.

    Args:
        pg_connection_string: Connection string in format

    Returns:
        SimpleConnectionPool object
    """
    load_dotenv()
    # Create connection pool with min=1 and max=20 connections
    pg_connection_string = os.getenv("PG_CONN_URI")

    pg_pool = pool.SimpleConnectionPool(1, 20, pg_connection_string)

    # Test connection
    test_conn = pg_pool.getconn()
    pg_pool.putconn(test_conn)

    return pg_pool


# %%
def _convert_duckdb_to_postgres(
    duckdb_conn: duckdb.DuckDBPyConnection,
    pg_pool: pool.SimpleConnectionPool,
    duckdb_table_name: str,
    postgres_table_name: str,
) -> None:
    """
    Converts a DuckDB table to PostgreSQL using psycopg2 pool and cursor.
    """
    type_mapping = {
        "VARCHAR": "TEXT",
        "DOUBLE": "DOUBLE PRECISION",
        "INTEGER": "INTEGER",
        "VARCHAR[]": "TEXT[]",
        "JSON": "JSONB",
        "BOOLEAN": "BOOLEAN",
        "TIMESTAMP": "TIMESTAMP",
        "DATE": "DATE",
        "STRUCT": "JSONB",
        "LIST": "TEXT[]",
    }

    schema_query = f"""
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM duckdb_columns()
    WHERE table_name = '{duckdb_table_name}'
    ORDER BY column_index;
    """

    schema = duckdb_conn.execute(schema_query).fetchall()

    create_table_query = f"CREATE TABLE IF NOT EXISTS {postgres_table_name} (\n"
    columns = []
    for col_name, duck_type, is_nullable in schema:
        pg_type = type_mapping.get(duck_type, "TEXT")
        nullable = "NULL" if is_nullable else "NOT NULL"
        columns.append(f"{col_name} {pg_type} {nullable}")

    create_table_query += ",\n".join(columns) + "\n);"

    def format_array_literal(array):
        if not array:
            return "{}"
        formatted_items = []
        for item in array:
            if item is None:
                formatted_items.append("NULL")
            elif isinstance(item, str):
                escaped = item.replace("\\", "\\\\").replace('"', '\\"')
                formatted_items.append(f'"{escaped}"')
            elif isinstance(item, (dict, list)):
                formatted_items.append(f'"{json.dumps(item, default=str)}"')
            elif isinstance(item, (datetime, date)):
                formatted_items.append(f'"{item.isoformat()}"')
            else:
                formatted_items.append(str(item))
        return "{" + ",".join(formatted_items) + "}"

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        conn = None
        try:
            conn = pg_pool.getconn()
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {postgres_table_name}")
                conn.commit()
                cur.execute(create_table_query)
                conn.commit()

                chunk_size = 1000  # Reduced chunk size
                offset = 0

                while True:
                    data_query = f"""
                    SELECT * FROM {duckdb_table_name}
                    LIMIT {chunk_size} OFFSET {offset}
                    """
                    chunk = duckdb_conn.execute(data_query).fetchall()

                    if not chunk:
                        break

                    converted_chunk = []
                    for row in chunk:
                        converted_row = []
                        for value in row:
                            if isinstance(value, dict):
                                converted_row.append(json.dumps(value, default=str))
                            elif isinstance(value, (datetime, date)):
                                converted_row.append(value.isoformat())
                            elif isinstance(value, list):
                                converted_row.append(format_array_literal(value))
                            elif value is None:
                                converted_row.append(None)
                            else:
                                converted_row.append(value)
                        converted_chunk.append(tuple(converted_row))

                    column_names = ", ".join([col[0] for col in schema])
                    placeholders = ",".join(["%s"] * len(schema))
                    insert_query = f"INSERT INTO {postgres_table_name} ({column_names}) VALUES ({placeholders})"

                    cur.executemany(insert_query, converted_chunk)
                    conn.commit()

                    offset += chunk_size

                break  # Success, exit the retry loop

        except (psycopg2.OperationalError, psycopg2.pool.PoolError) as e:
            retry_count += 1
            if conn:
                with contextlib.suppress(Exception):
                    conn.rollback()
                pg_pool.putconn(conn, close=True)

            if retry_count == max_retries:
                raise Exception(f"Failed to process table after {max_retries} attempts: {str(e)}")  # noqa: B904

            time.sleep(1)  # Wait before retrying

        except Exception as e:
            if conn:
                with contextlib.suppress(Exception):
                    conn.rollback()
                pg_pool.putconn(conn, close=True)
            raise e

        finally:
            if conn:
                with contextlib.suppress(Exception):
                    pg_pool.putconn(conn)



# %%
