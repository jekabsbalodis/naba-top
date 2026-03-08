import asyncio
import os
from pathlib import Path

from prefect import flow
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger
from prefect.variables import Variable
from pydantic import EmailStr, HttpUrl, TypeAdapter

from flows.shared_tasks import fetch_webpage, parse_html, upload_data
from flows.update_charts import update_charts_flow
from flows.update_songs import update_songs_flow


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
    s3_key_id: str | None = None,
    s3_secret: str | None = None,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
) -> None:
    """Main flow that orchestrates updating songs and charts table in db"""
    database_path = db_path or _load_variable('db_path')
    flow_url = url or _load_variable('flow_url')
    flow_email = email or _load_secret('flow-email')
    key_id = s3_key_id or _load_secret('garage-key-id')
    secret = s3_secret or _load_variable('garage-secret')
    endpoint = s3_endpoint or _load_variable('garage-endpoint')
    region = s3_region or _load_variable('garage-region')

    logger = get_run_logger()
    logger.info(flow_url)
    logger.info(flow_email)
    logger.info(endpoint)
    logger.info(path)
    flow_url = _validate_url(flow_url)
    endpoint = _validate_url(endpoint)
    flow_email = _validate_email(flow_email)

    data_dir = os.environ.get('NABA_TOP_DATA_DIR')
    if data_dir is None:
        raise LookupError('Environment variable for data dir is not set.')
    path = str(Path(data_dir) / database_path)

    logger.info(path)
    path = _validate_db_path(path)

    response = fetch_webpage(flow_url, flow_email)
    soup = parse_html(response)

    update_songs_flow(soup, path)
    update_charts_flow(soup, path)

    upload_data(path, key_id, secret, endpoint, region)


if __name__ == '__main__':
    main_flow.deploy(name='naba-top-scrape')
