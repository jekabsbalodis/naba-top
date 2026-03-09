"""Module containing Prefect tasks shared between subflows."""

import httpx
from bs4 import BeautifulSoup
from prefect import task
from prefect.tasks import exponential_backoff
from pydantic import HttpUrl

from database.s3_connection import s3_connection


@task(
    retries=5,
    retry_delay_seconds=exponential_backoff(backoff_factor=3),
)
def fetch_webpage(url: str, email: str) -> httpx.Response:
    """Fetch webpage content with retry logic.

    Args:
        url: URL of the web page to fetch.
        email: Email address to include in request headers.

    Returns:
        HTTP response object.

    Raises:
        HTTP response error if request fails with 4xx or 5xx error.

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
    """Parse HTML content into BeautifulSoup object.

    Args:
        res: HTTP response with HTML content.

    Returns:
        Parsed HTML document.

    """
    return BeautifulSoup(res.text, 'lxml')


@task
def upload_data(
    db_path: str, key_id: str, secret: str, endpoint: str, region: str
) -> None:
    """Upload database tables and views to S3 storage in Parquet format.

    Args:
        db_path: Path to the DuckDB database file.
        key_id: S3 access key id.
        secret: S3 secret access key.
        endpoint: S3 endpoint URL.
        region: S3 bucket region.

    Note:
        Uploads the following tables/views: all_songs_ranked,
        top10, top25, charts, songs.

    """
    with s3_connection(
        db_path=db_path,
        key_id=key_id,
        secret=secret,
        endpoint=endpoint,
        region=region,
    ) as conn:
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
