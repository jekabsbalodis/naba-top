"""Shared tasks for naba-top data pipeline.

Contains reusable Prefect tasks for web scraping, HTML parsing,
and S3 data upload operations.
"""

import httpx
from bs4 import BeautifulSoup
from prefect import task
from prefect.tasks import exponential_backoff
from pydantic import HttpUrl

from database.s3_connection import s3_connection
from models import S3Config


@task(
    retries=5,
    retry_delay_seconds=exponential_backoff(backoff_factor=3),
)
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


@task
def parse_html(res: httpx.Response) -> BeautifulSoup:
    """Parse HTML response into BeautifulSoup document.

    Args:
        res: httpx.Response containing HTML content.

    Returns:
        BeautifulSoup parsed document tree.

    Uses lxml parser for efficient HTML parsing.

    """
    return BeautifulSoup(res.text, 'lxml')


@task
def upload_data(db_path: str, s3_config: S3Config) -> None:
    """Upload database tables to S3 storage in Parquet format.

    Exports all database views and tables to an S3 bucket as Parquet files.

    Args:
        db_path: Path to DuckDB database file
        s3_config: S3 configuration with credentials

    """
    with s3_connection(db_path=db_path, s3_config=s3_config) as conn:
        conn.execute(
            """ --sql

            copy
                (select * from all_songs_ranked)
            to 's3://naba-top/all_songs_ranked.parquet'
                (format parquet, overwrite_or_ignore true);

            copy
                (select * from top10)
            to 's3://naba-top/top10.parquet'
                (format parquet, overwrite_or_ignore true);

            copy
                (select * from top25)
            to 's3://naba-top/top25.parquet'
                (format parquet, overwrite_or_ignore true);

            copy
                (select * from charts)
            to 's3://naba-top/charts.parquet'
                (format parquet, overwrite_or_ignore true);

            copy
                (select * from songs)
            to 's3://naba-top/songs.parquet'
                (format parquet, overwrite_or_ignore true);
        """,
        )
