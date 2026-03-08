from collections.abc import Generator
from contextlib import contextmanager

import duckdb


@contextmanager
def s3_connection(
    key_id: str,
    secret: str,
    endpoint: str,
    region: str,
    db_path: str = ':memory:',
) -> Generator[duckdb.DuckDBPyConnection]:
    """Make an s3 connection with a configured secret
    to successfully connect to garage bucket."""
    conn = duckdb.connect(db_path)
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
