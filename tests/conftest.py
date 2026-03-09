import os
from pathlib import Path
import tempfile

import pytest
from prefect.testing.utilities import prefect_test_harness

from database.init_db import init_db
from models import S3Config


@pytest.fixture(autouse=True, scope='session')
def prefect_test_fixture():
    with prefect_test_harness(server_startup_timeout=60):
        yield


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
    os.environ['PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW'] = 'ignore'
    os.environ['NABA_TOP_DATA_DIR'] = _temp_dir.name


def pytest_unconfigure():
    os.environ.pop('PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW', None)
    os.environ.pop('NABA_TOP_DATA_DIR', None)
    _temp_dir.cleanup()
