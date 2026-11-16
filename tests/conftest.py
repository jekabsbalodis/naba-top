"""Shared test setup."""

import os
import tempfile
from pathlib import Path

import httpx2 as httpx
import pytest

from database.init_db import init_db

_TMP = tempfile.TemporaryDirectory()
_DATA = Path(_TMP.name)

TEST_ENV = {
    'NABA_TOP_ENVIRONMENT': 'development',
    'NABA_TOP_GARAGE__ENDPOINT': 's3.example.com',
    'NABA_TOP_GARAGE__REGION': 'test-region',
    'NABA_TOP_GARAGE__READ__KEY_ID': 'read-id',
    'NABA_TOP_GARAGE__READ__SECRET': 'read-secret',
    'NABA_TOP_GARAGE__WRITE__KEY_ID': 'write-id',
    'NABA_TOP_GARAGE__WRITE__SECRET': 'write-secret',
    'NABA_TOP_AUTH__REDIRECT_URI': 'http://localhost:8501/oauth2callback',
    'NABA_TOP_AUTH__COOKIE_SECRET': 'cookie-secret',
    'NABA_TOP_AUTH__CLIENT_ID': 'client-id',
    'NABA_TOP_AUTH__CLIENT_SECRET': 'client-secret',
    'NABA_TOP_AUTH__SERVER_METADATA_URL': 'https://auth.example.com/.well-known/openid-configuration',
    'NABA_TOP_DATA__DATA_DIR': str(_DATA),
    'NABA_TOP_DATA__RAW_DATA_DB': str(_DATA / 'raw.duckdb'),
    'NABA_TOP_DATA__SQLMESH_STATE_DB': str(_DATA / 'sqlmesh_state.duckdb'),
    'NABA_TOP_DATA__SQLMESH_DATA_DB': str(_DATA / 'sqlmesh_data.duckdb'),
    'NABA_TOP_SCRAPE__URL': 'https://www.example.com/charts',
    'NABA_TOP_SCRAPE__EMAIL': 'test@example.com',
    'NABA_TOP_PIPELINE__NTFY_URL': 'https://ntfy.example.com/test',
}


def pytest_configure():
    os.environ.update(TEST_ENV)


def pytest_unconfigure():
    for name in TEST_ENV:
        os.environ.pop(name, None)
    _TMP.cleanup()


@pytest.fixture
def raw_config() -> dict:
    """A valid config as a plain dict, like `env.toml` parsed. For the rule tests:
    change one value in it and check that validation rejects it."""
    return {
        'environment': 'development',
        'garage': {
            'endpoint': 's3.example.com',
            'region': 'test-region',
            'read': {'key_id': 'read-id', 'secret': 'read-secret'},
            'write': {'key_id': 'write-id', 'secret': 'write-secret'},
        },
        'auth': {
            'redirect_uri': 'http://localhost:8501/oauth2callback',
            'cookie_secret': 'cookie-secret',
            'client_id': 'client-id',
            'client_secret': 'client-secret',
            'server_metadata_url': 'https://auth.example.com/.well-known/openid-configuration',
        },
        'data': {
            'data_dir': '/tmp/naba-top-test',
            'raw_data_db': '/tmp/naba-top-test/raw.duckdb',
            'sqlmesh_state_db': '/tmp/naba-top-test/sqlmesh_state.duckdb',
            'sqlmesh_data_db': '/tmp/naba-top-test/sqlmesh_data.duckdb',
        },
        'scrape': {
            'url': 'https://www.example.com/charts',
            'email': 'test@example.com',
        },
        'pipeline': {'ntfy_url': 'https://ntfy.example.com/test'},
    }


@pytest.fixture
def db_path(tmp_path) -> str:
    """Fresh empty database (songs and charts tables) for one test."""
    path = str(tmp_path / 'database-test.duckdb')
    init_db(path)
    return path


class FakeWeb:
    """Stands in for the website: answers every request with `html` and `status`."""

    def __init__(self) -> None:
        self.html = '<html></html>'
        self.status = 200
        self.requests: list[httpx.Request] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.status, text=self.html, request=request)


@pytest.fixture
def web(monkeypatch) -> FakeWeb:
    """Make every `httpx.Client` talk to the fake website instead of the internet."""
    fake = FakeWeb()
    real_client = httpx.Client

    def client_with_fake_transport(**kwargs):
        return real_client(transport=httpx.MockTransport(fake.handle), **kwargs)

    monkeypatch.setattr(httpx, 'Client', client_with_fake_transport)
    return fake
