from collections.abc import Generator
from contextlib import contextmanager

import duckdb

from models import S3Config


@contextmanager
def s3_connection(
    s3_config: S3Config,
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
            (s3_config.key_id, s3_config.secret, s3_config.endpoint, s3_config.region),
        )
        yield conn

    finally:
        conn.close()
