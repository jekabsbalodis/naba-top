import os


def pytest_configure():
    """Set the TESTING environment variable for all tests"""
    os.environ['TESTING'] = 'true'


def pytest_unconfigure():
    """Remove TESTING environment and test database after the tests have ran"""
    from config import config  # noqa: PLC0415

    if os.path.exists(config.DB_PATH) and 'test' in str(config.DB_PATH).lower():
        os.remove(config.DB_PATH)
        print(f'Test database {config.DB_PATH} has been deleted')
    os.environ.pop('TESTING', None)
