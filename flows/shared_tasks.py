import duckdb
import httpx
from bs4 import BeautifulSoup
from prefect import task
from prefect.tasks import exponential_backoff
from pydantic import HttpUrl


@task(
    retries=5,
    retry_delay_seconds=exponential_backoff(backoff_factor=3),
)
def fetch_webpage(url: str, email: str) -> httpx.Response:
    """Read naba.lv webpage and return httpx response"""
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
    """Parse the returned response HTML text"""
    return BeautifulSoup(res.text, 'lxml')


@task
def upload_data(
    db_path: str, key_id: str, secret: str, endpoint: str, region: str
) -> None:
    """
    Upload the data in database to an S3 storage
    """
    with duckdb.connect(db_path, read_only=True) as conn:
        conn.execute(
            """ --sql
            install httpfs;

            load httpfs;

            create or replace secret (
                type s3,
                key_id '?',
                secret '?',
                endpoint '?',
                region '?',
                url_style 'path'
                );

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
            parameters=[key_id, secret, endpoint, region],
        )
