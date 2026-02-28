import os

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope='session')
def prefect_test_fixture():
    with prefect_test_harness(server_startup_timeout=60):
        yield


def pytest_configure():
    """Set the NABA_TOP_ENV environment variable as `test` for all tests"""
    os.environ['NABA_TOP_ENV'] = 'test'
    os.environ['PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW'] = 'ignore'


def pytest_unconfigure():
    """Remove NABA_TOP_ENV environment value and
    test database after the tests have ran"""
    from config import config  # noqa: PLC0415

    if (
        os.path.exists(config.DB_PATH)
        and 'test' in config.DB_PATH.resolve().as_uri().lower()
    ):
        os.remove(config.DB_PATH)
        print(f'Test database {config.DB_PATH} has been deleted')
    os.environ.pop('NABA_TOP_ENV', None)
    os.environ.pop('PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW', None)
