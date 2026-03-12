"""Module for creating the databse connection to S3 bucket."""

from collections.abc import Generator
from contextlib import contextmanager

import duckdb

from models import S3Config


@contextmanager
def s3_connection(
    s3_config: S3Config,
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
            (s3_config.key_id, s3_config.secret, s3_config.endpoint, s3_config.region),
        )
        yield conn

    finally:
        conn.close()
