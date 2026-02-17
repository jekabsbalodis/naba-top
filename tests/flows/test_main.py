from unittest.mock import MagicMock, patch

import duckdb
import httpx
import pytest

from config import config
from database.init_db import init_db
from flows.main import main_flow

NABA_HTML = """
<html><body>
<div class="songsList">
    <form>
        <label class="songLine">
            <div class="leftLine">
                <div class="songPlace">1</div>
                <div class="songName">Artist One - Song One</div>
            </div>
        </label>
        <div class="naba-top-song">
            <div class="song_vote_info">
                <div class="place_previous">2</div>
            </div>
        </div>
        <div class="newsCard__date songListDate">13.02.2026</div>
    </form>
</div>
<div class="songsList">
    <form>
        <label class="songLine">
            <div class="leftLine">
                <div class="songPlace">1</div>
                <div class="songName">Artist Two - Song Two</div>
            </div>
        </label>
        <div class="naba-top-song">
            <div class="song_vote_info">
                <div class="place_previous">2</div>
            </div>
        </div>
        <div class="newsCard__date songListDate">13.02.2026</div>
    </form>
</div>
</body></html>
"""


@pytest.fixture(autouse=True)
def setup_db():
    init_db()
    yield
    with duckdb.connect(config.DB_PATH) as conn:
        conn.execute('delete from charts')
        conn.execute('delete from songs')


@pytest.fixture(autouse=True)
def mock_http():
    response = httpx.Response(
        status_code=200,
        content=NABA_HTML,
        request=httpx.Request('GET', str(config.FLOW_URL)),
    )
    with patch('flows.shared_tasks.httpx.Client') as mock_client_cls:
        client = MagicMock()
        client.__enter__ = MagicMock(return_value=client)
        client.__exit__ = MagicMock(return_value=False)
        client.get.return_value = response
        mock_client_cls.return_value = client
        yield


class TestMainFlow:
    def test_runs_without_error(self):
        main_flow.fn(config.FLOW_URL, config.FLOW_EMAIL)

    def test_songs_are_inserted(self):
        main_flow.fn(config.FLOW_URL, config.FLOW_EMAIL)
        with duckdb.connect(config.DB_PATH) as conn:
            count = conn.sql('select count(*) from songs').fetchone()
        assert count is not None
        assert count[0] > 0

    def test_charts_are_inserted(self):
        main_flow.fn(config.FLOW_URL, config.FLOW_EMAIL)
        with duckdb.connect(config.DB_PATH) as conn:
            count = conn.sql('select count(*) from charts').fetchone()
        assert count is not None
        assert count[0] > 0

    def test_idempotent_on_second_run(self):
        main_flow.fn(config.FLOW_URL, config.FLOW_EMAIL)
        with duckdb.connect(config.DB_PATH) as conn:
            songs_first = conn.sql('select count(*) from songs').fetchone()
            charts_first = conn.sql('select count(*) from charts').fetchone()

        main_flow.fn(config.FLOW_URL, config.FLOW_EMAIL)
        with duckdb.connect(config.DB_PATH) as conn:
            songs_second = conn.sql('select count(*) from songs').fetchone()
            charts_second = conn.sql('select count(*) from charts').fetchone()

        assert songs_first == songs_second
        assert charts_first == charts_second
