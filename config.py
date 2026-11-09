"""Global configuration for the app."""

import os
from enum import StrEnum
from pathlib import Path


class Environment(StrEnum):
    """Possible values for app environment."""

    PROD = 'production'
    DEV = 'development'


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise LookupError(f'Environment variable {name} is not set.')
    return value


def get_environment() -> Environment:
    """Return the current app environment.

    Raises:
        LookupError: if ENVIRONMENT is not set.
        ValueError: if ENVIRONMENT is not a valid Environment value.

    """
    env = _env('ENVIRONMENT')
    try:
        return Environment(env)
    except ValueError as e:
        valid = ', '.join(e.value for e in Environment)
        raise ValueError(
            f'Invalid ENVIRONMENT: {env!r}. Must be one of: {valid}'
        ) from e


def data_dir() -> Path:
    """Directory holding the database and staged files."""
    return Path(_env('NABA_TOP_DATA_DIR'))


def local_upload_dir() -> Path:
    """Directory for the local upload test in dev mode."""
    return data_dir() / 'upload'
