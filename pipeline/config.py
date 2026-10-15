"""Configuration for the naba-top pipeline."""

from models import S3Config
from pathlib import Path

import os
import re

from pydantic import HttpUrl, TypeAdapter, EmailStr


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise LookupError(f'Environment variable {name} is not set.')
    return value


def validate_url(v: str) -> str:
    """Return `v` unchanged if it is a valid HTTP(S) URL.

    Args:
        v: URL to validate.

    Returns:
        The same string.

    """
    TypeAdapter(HttpUrl).validate_python(v)
    return v


def validate_email(v: str) -> str:
    """Return `v` unchanged if it is a valid email address.

    Args:
        v: Email address to validate.

    Returns:
        The same string.

    """
    TypeAdapter(EmailStr).validate_python(v)
    return v


def validate_db_path(v: str) -> str:
    """Return the normalized path if its parent directory exists.

    Args:
        v: Database file path.

    Returns:
        The path as string.

    Raises:
        ValueError: If the parent directory does not exist.

    """
    p = Path(v)
    if not p.parent.exists():
        raise ValueError(f'DB_PATH parent directory does not exist: {p.parent}')
    return str(p)


def data_dir() -> Path:
    """Directory holding the database and staged files."""
    return Path(_env('NABA_TOP_DATA_DIR'))


def run_id() -> str:
    """Return the id of the current run shared by all of the steps."""

    value = (
        os.environ.get('NABA_TOP_RUN_ID') or os.environ.get('DAG_RUN_ID') or 'latest'
    )
    if not re.fullmatch(r'[A-Za-z0-9._-]+', value):
        raise ValueError(f'Invalid run id: {value!r}')
    return value


def stage_dir() -> Path:
    """Directory for files handed between steps (created if missing).

    If steps are executed with Dagu and `artifacts.enabled: true`, this is the artifacts
    dir for each run. For manual runs and tests it is `<data dir>/stage/<run id>`.
    """
    artifacts = os.environ.get('DAG_RUN_ARTIFACTS_DIR')
    path = Path(artifacts) if artifacts else data_dir() / 'stage' / run_id()
    path.mkdir(parents=True, exist_ok=True)
    return path


def db_path() -> str:
    """Return the validated path of the DuckDB database."""
    return validate_db_path(str(data_dir() / _env('NABA_TOP_DB_PATH')))


def scrape_target() -> tuple[str, str]:
    """Return the validated (url, email) used for the scraping request."""
    return validate_url(_env('NABA_TOP_URL')), validate_email(_env('NABA_TOP_EMAIL'))


def s3_config() -> S3Config:
    """S3 credentials from the environment."""
    return S3Config(
        key_id=_env('S3_KEY_ID'),
        secret=_env('S3_SECRET'),
        endpoint=_env('S3_ENDPOINT'),
        region=_env('S3_REGION'),
    )
