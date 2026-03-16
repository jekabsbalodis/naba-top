"""S3 database connection module for naba-top application.

Provides context manager for establishing DuckDB connections to S3 storage bucket.
"""

from collections.abc import Generator
from contextlib import contextmanager

import duckdb

from models import S3Config


@contextmanager
def s3_connection(
    s3_config: S3Config,
    db_path: str = ':memory:',
) -> Generator[duckdb.DuckDBPyConnection]:
    """Create a DuckDB connection configured for S3 access.

    Sets up HTTPFS extension and S3 credentials for querying S3 buckets.

    Args:
        s3_config: S3 configuration containing credentials and endpoint.
        db_path: Path to DuckDB database file or ':memory:' (default).

    Returns:
        Generator yielding a configured DuckDB connection.

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
