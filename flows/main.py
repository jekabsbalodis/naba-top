import asyncio
from pathlib import Path

from prefect import flow
from prefect.blocks.system import Secret
from prefect.variables import Variable
from pydantic import EmailStr, HttpUrl, TypeAdapter

from flows.shared_tasks import fetch_webpage, parse_html
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
) -> None:
    """Main flow that orchestrates updating songs and charts table in db"""
    path = db_path or _load_variable('db_path')
    flow_url = url or _load_variable('flow_url')
    flow_email = email or _load_secret('flow-email')

    flow_url = _validate_url(flow_url)
    flow_email = _validate_email(flow_email)
    path = _validate_db_path(path)

    response = fetch_webpage(flow_url, flow_email)
    soup = parse_html(response)

    update_songs_flow(soup, path)
    update_charts_flow(soup, path)


if __name__ == '__main__':
    main_flow.deploy(name='naba-top-scrape')
