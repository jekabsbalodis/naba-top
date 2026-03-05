from collections.abc import Generator
from contextlib import contextmanager

import duckdb
import streamlit as st

key_id = st.secrets['garage']['key_id']
secret = st.secrets['garage']['secret']
endpoint = st.secrets['garage']['endpoint']
region = st.secrets['garage']['region']


@contextmanager
def s3_connection(
    key_id: str = key_id,
    secret: str = secret,
    endpoint: str = endpoint,
    region: str = region,
) -> Generator[duckdb.DuckDBPyConnection]:
    """Make an s3 connection with a configured secret
    to successfully connect to garage bucket."""
    conn = duckdb.connect()
    try:
        conn.execute(
            """--sql
             install httpfs;
             load httpfs;
             create or replace secret (
               type s3,
               key_id ?,
               secret ?,
               endpoint ?,
               region ?,
               url_style 'path'
             );
             """,
            (key_id, secret, endpoint, region),
        )
        yield conn

    finally:
        conn.close()
