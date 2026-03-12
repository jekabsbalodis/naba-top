"""Main workflow of the data processing.

Orchestrates the data pipeline:
- Fetching webpage.
- Updating songs and charts.
- Uploading data to S3 storage.
"""

import asyncio
import os
from pathlib import Path

from prefect import flow
from prefect.blocks.system import Secret
from prefect.variables import Variable
from pydantic import EmailStr, HttpUrl, TypeAdapter

from flows.shared_tasks import fetch_webpage, parse_html, upload_data
from flows.update_charts import update_charts_flow
from flows.update_songs import update_songs_flow
from models import S3Config


def _validate_url(v: str) -> str:
    TypeAdapter(HttpUrl).validate_python(v)
    return v


def _validate_email(v: str) -> str:
    TypeAdapter(EmailStr).validate_python(v)
    return v


def _validate_db_path(v: str) -> str:
    p = Path(v)
    if not p.parent.exists():
        raise ValueError(f'DB_PATH parent directory does not exist: {p.parent}')
    return str(p)


def _load_secret(secret_name: str) -> str:
    secret = asyncio.run(Secret.aload(secret_name))
    return secret.get()


def _load_variable(variable_name: str) -> str:
    return str(asyncio.run(Variable.aget(variable_name)))


@flow(retries=3, retry_delay_seconds=600)
def main_flow(
    db_path: str | None = None,
    url: str | None = None,
    email: str | None = None,
    s3_config: S3Config | None = None,
) -> None:
    """Orchestrate the main flow of the pipeline.

    Args:
        db_path: Path to the database in which to insert the data.
        url: Link to the for scrapping.
        email: Email address that will be included in the request headers.
        s3_key_id: Key id for the database secret.
        s3_secret: Secret value for the database secret.
        s3_endpoint: Bucket endpoint for the database secret.
        s3_region: Bucket region for the database secret.

    """
    database_path = db_path or _load_variable('db_path')
    flow_url = url or _load_variable('flow_url')
    flow_email = email or _load_secret('flow-email')
    s3 = s3_config or S3Config(
        key_id=_load_secret('garage-key-id'),
        secret=_load_secret('garage-secret'),
        endpoint=_load_secret('garage-endpoint'),
        region=_load_secret('garage-region'),
    )

    flow_url = _validate_url(flow_url)
    flow_email = _validate_email(flow_email)

    data_dir = os.environ.get('NABA_TOP_DATA_DIR')
    if data_dir is None:
        raise LookupError('Environment variable for data dir is not set.')
    path = str(Path(data_dir) / database_path)

    path = _validate_db_path(path)

    response = fetch_webpage(flow_url, flow_email)
    soup = parse_html(response)

    update_songs_flow(soup, path)
    update_charts_flow(soup, path)

    upload_data(path, s3)


if __name__ == '__main__':
    main_flow.deploy(name='naba-top-scrape')
