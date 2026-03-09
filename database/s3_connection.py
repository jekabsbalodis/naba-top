"""Module for creating the databse connection to S3 bucket."""

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
    """Install and load the necessary plugin, create secret for S3 connection.

    Args:
        key_id: S3 connection key id.
        secret: S3 connection secret.
        endpoint: S3 bucket endpoint.
        region: S3 bucket region.
        db_path: DuckDB database path.
        Default - ':memory:' for an in-memory database.

    Returns:
        DuckDB connection object whith preconfigured S3 secret.

    """
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
