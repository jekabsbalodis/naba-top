"""Shared tasks for naba-top data pipeline.

Contains reusable Prefect tasks for web scraping, HTML parsing,
and S3 data upload operations.
"""

from pathlib import Path

import duckdb
import httpx2 as httpx
from pydantic import HttpUrl

from database.s3_connection import s3_connection
from models import S3Config


def fetch_webpage(url: str, email: str) -> httpx.Response:
    """Fetch webpage content with retry logic and custom user-agent.

    Args:
        url: URL to fetch.
        email: Email address for user-agent header.

    Returns:
        httpx.Response object with webpage content.

    Raises:
        httpx.HTTPStatusError: If request fails after retries.

    Uses exponential backoff for retries with 3x multiplier.

    """
    headers = {
        'user-agent': f'python-httpx {str(email)}',
        'accept': 'text/html',
    }
    with httpx.Client(headers=headers) as client:
        response = client.get(url=HttpUrl(url).unicode_string())
    response.raise_for_status()
    return response


def upload_data(
    sqlm_d: str, s3_config: S3Config | None = None, local_dir: Path | None = None
) -> None:
    """Upload database tables in Parquet format.

    Exports all database views and tables to a local direcotory
    or an S3 bucket as Parquet files.

    Args:
        sqlm_d: path to the SQLMesh data database
        s3_config: S3 configuration with credentials
        local_dir: local directory where to upload files in DEV environment

    """
    if (s3_config is None) and (local_dir is None):
        raise ValueError('Pass exactly one of s3_config or local_dir.')
    if s3_config is not None:
        conn_ctx = s3_connection(s3_config=s3_config)
        destination = 's3://naba-top'
    else:
        assert local_dir is not None
        upload_dir = local_dir
        upload_dir.mkdir(exist_ok=True)
        destination = upload_dir.absolute().as_posix()
        conn_ctx = duckdb.connect()

    with conn_ctx as conn:
        conn.execute(
            f"""--sql
             attach '{sqlm_d}' as sqlmesh_data (read_only)
            """,
            # """--sql
            #  attach ? as sqlm_d (read_only)
            # """,
            # (sqlm_d,),
        )
        res: list[tuple[str, str]] = conn.execute(
            """--sql
             select table_schema, table_name
             from information_schema.tables
             where table_catalog = ?
            """,
            ('sqlmesh_data',),
        ).fetchall()

        exported: list[tuple[str, str]] = []
        for schema, view in res:
            if '__' in schema or schema.startswith('sqlmesh'):
                continue
            if local_dir:
                (Path(destination) / schema).mkdir(parents=True, exist_ok=True)
            conn.execute(f"""--sql
                         copy
                             (select * from "sqlmesh_data"."{schema}"."{view}")
                         to '{destination}/{schema}/{view}.parquet'
                             (format parquet, overwrite_or_ignore true)
                        """)
            exported.append((schema, view))

        conn.execute("""--sql
                     create temp table manifest (
                         schema varchar,
                         view varchar,
                         exported_at timestamptz default now()
                     )
                     """)
        conn.executemany(
            """--sql
            insert into manifest (schema, view) values (?, ?)
            """,
            exported,
        )
        conn.execute(f"""--sql
                     copy
                         (select * from manifest)
                     to '{destination}/manifest.json'
                         (format json, overwrite_or_ignore true, array true)
                    """)
