import os


def pytest_configure():
    """Set the TESTING environment variable for all tests"""
    os.environ['TESTING'] = 'true'


def pytest_unconfigure():
    """Remove TESTING environment after the tests have ran"""
    os.environ.pop('TESTING', None)
