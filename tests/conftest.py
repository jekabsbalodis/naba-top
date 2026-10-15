import os
import tempfile
from pathlib import Path

import pytest

from database.init_db import init_db
from models import S3Config


@pytest.fixture
def db_path():
    path = str(Path(os.environ['NABA_TOP_DATA_DIR']) / 'database-test.duckdb')
    init_db(path)
    return path


@pytest.fixture
def flow_url() -> str:
    return 'https://www.example.com'


@pytest.fixture
def flow_email() -> str:
    return 'test@example.com'


@pytest.fixture
def s3_config():
    return S3Config(
        key_id='test_id',
        secret='test_secret',
        endpoint='s3.example.com',
        region='test_region',
    )


_temp_dir = tempfile.TemporaryDirectory()


def pytest_configure():
    os.environ['NABA_TOP_DATA_DIR'] = _temp_dir.name


def pytest_unconfigure():
    os.environ.pop('NABA_TOP_DATA_DIR', None)
    _temp_dir.cleanup()
