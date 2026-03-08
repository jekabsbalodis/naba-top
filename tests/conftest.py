import os

import pytest
from prefect.testing.utilities import prefect_test_harness

from database.init_db import init_db


@pytest.fixture(autouse=True, scope='session')
def prefect_test_fixture():
    with prefect_test_harness(server_startup_timeout=60):
        yield


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / 'database-test.duckdb')
    init_db(path)
    return path


@pytest.fixture
def flow_url() -> str:
    return 'https://www.example.com'


@pytest.fixture
def flow_email() -> str:
    return 'test@example.com'


def pytest_configure():
    os.environ['PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW'] = 'ignore'


def pytest_unconfigure():
    os.environ.pop('PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW', None)
