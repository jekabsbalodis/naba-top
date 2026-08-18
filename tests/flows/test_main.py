from unittest.mock import MagicMock, patch, AsyncMock

import duckdb
import httpx
import pytest
from prefect.blocks.system import Secret
from prefect.variables import Variable
from pydantic import ValidationError

from database.init_db import init_db
from flows.main import (
    _load_secret,
    _load_variable,
    _validate_db_path,
    _validate_email,
    _validate_url,
    main_flow,
)


def _make_song_lines(chart: str, ranked: int, unranked: int) -> str:
    """Generate song HTML lines for test fixtures"""
    lines = []
    for i in range(1, ranked + 1):
        lines.append(f"""
        <label class="songLine">
            <div class="leftLine">
                <div class="songPlace">{i}</div>
                <div class="songName">Artist {chart} {i} - Song {chart} {i}</div>
            </div>
        </label>
        <div class="naba-top-song">
            <div class="song_vote_info">
                <div class="place_previous">{i}</div>
            </div>
        </div>""")
    for i in range(unranked):
        lines.append(f"""
        <label class="songLine">
            <div class="leftLine">
            <div class="songPlace">j</div>
            <div class="songName">Artist {chart} New {i} - Song {chart} New {i}</div>
            </div>
        </label>
        <div class="naba-top-song">
            <div class="song_vote_info">
                <div class="place_previous">j</div>
            </div>
        </div>""")
    return '\n'.join(lines)


NABA_HTML = f"""
<html><body>
<div class="songsList">
    <form>
        {_make_song_lines('Top10', ranked=10, unranked=5)}
        <div class="newsCard__date songListDate">13.02.2026</div>
    </form>
</div>
<div class="songsList">
    <form>
        {_make_song_lines('Top25', ranked=25, unranked=5)}
        <div class="newsCard__date songListDate">13.02.2026</div>
    </form>
</div>
</body></html>
"""


@pytest.fixture(autouse=True)
def setup_db(db_path):
    init_db(db_path)
    yield
    with duckdb.connect(db_path) as conn:
        conn.execute('delete from charts')
        conn.execute('delete from songs')


@pytest.fixture(autouse=True)
def mock_http(flow_url):
    response = httpx.Response(
        status_code=200,
        content=NABA_HTML,
        request=httpx.Request('GET', flow_url),
    )
    with patch('flows.shared_tasks.httpx.Client') as mock_client_cls:
        client = MagicMock()
        client.__enter__ = MagicMock(return_value=client)
        client.__exit__ = MagicMock(return_value=False)
        client.get.return_value = response
        mock_client_cls.return_value = client
        yield


@pytest.fixture(autouse=True)
def mock_upload():
    with patch('flows.shared_tasks.s3_connection', return_value=MagicMock()) as mock:
        yield mock


class TestMainFlow:
    def test_runs_without_error(self, db_path, flow_url, flow_email, s3_config):
        main_flow.fn(
            db_path=db_path, url=flow_url, email=flow_email, s3_config=s3_config
        )

    def test_songs_are_inserted(self, db_path, flow_url, flow_email, s3_config):
        main_flow.fn(
            db_path=db_path, url=flow_url, email=flow_email, s3_config=s3_config
        )
        with duckdb.connect(db_path) as conn:
            count = conn.sql('select count(*) from songs').fetchone()
        assert count is not None
        assert count[0] == 45

    def test_charts_are_inserted(self, db_path, flow_url, flow_email, s3_config):
        main_flow.fn(
            db_path=db_path, url=flow_url, email=flow_email, s3_config=s3_config
        )
        with duckdb.connect(db_path) as conn:
            count = conn.sql('select count(*) from charts').fetchone()
        assert count is not None
        assert count[0] == 45

    def test_idempotent_on_second_run(self, db_path, flow_url, flow_email, s3_config):
        main_flow.fn(
            db_path=db_path, url=flow_url, email=flow_email, s3_config=s3_config
        )
        with duckdb.connect(db_path) as conn:
            songs_first = conn.sql('select count(*) from songs').fetchone()
            charts_first = conn.sql('select count(*) from charts').fetchone()

        main_flow.fn(
            db_path=db_path, url=flow_url, email=flow_email, s3_config=s3_config
        )
        with duckdb.connect(db_path) as conn:
            songs_second = conn.sql('select count(*) from songs').fetchone()
            charts_second = conn.sql('select count(*) from charts').fetchone()

        assert songs_first == songs_second
        assert charts_first == charts_second

    def test_validate_url_raises_on_invalid_url(self):
        with pytest.raises(ValidationError):
            _validate_url('not-a-url')

    def test_validate_email_raises_on_invalid_email(self):
        with pytest.raises(ValidationError):
            _validate_email('not-an-email')

    def test_validate_db_path_returns_normalized_path(self, tmp_path):
        db_file = tmp_path / 'db.duckdb'
        assert _validate_db_path(str(db_file)) == str(db_file)

    def test_validate_db_path_raises_on_missing_parent(self):
        with pytest.raises(ValueError, match='DB_PATH parent directory does not exist'):
            _validate_db_path('/nonexistent/path/db.duckdb')

    def test_main_flow_raises_on_missing_data_dir(
        self, db_path, flow_url, flow_email, s3_config, monkeypatch
    ):
        monkeypatch.delenv('NABA_TOP_DATA_DIR', raising=False)
        with pytest.raises(
            LookupError, match='Environment variable for data dir is not set'
        ):
            main_flow.fn(
                db_path=db_path, url=flow_url, email=flow_email, s3_config=s3_config
            )

    def test_main_flow_uses_defaults_when_params_are_none(
        self, db_path, flow_url, flow_email, s3_config
    ):
        def mock_load_secret(secret_name):
            if secret_name == 'flow-email':
                return flow_email
            elif secret_name == 'garage-key-id':
                return 'test-key-id'
            elif secret_name == 'garage-secret':
                return 'test-secret'
            elif secret_name == 'garage-endpoint':
                return 's3.example.com'
            elif secret_name == 'garage-region':
                return 'test_region'
            return 'default-secret'

        def mock_load_variable(variable_name):
            if variable_name == 'db_path':
                return db_path
            elif variable_name == 'flow_url':
                return flow_url
            return 'default-value'

        with (
            patch('flows.main._load_secret', side_effect=mock_load_secret),
            patch('flows.main._load_variable', side_effect=mock_load_variable),
            patch('flows.main.fetch_webpage', return_value=MagicMock()) as mock_fetch,
            patch('flows.main.parse_html', return_value=MagicMock()),
            patch('flows.main.update_songs_flow'),
            patch('flows.main.update_charts_flow'),
            patch('flows.main.upload_data') as mock_upload,
        ):
            main_flow.fn(db_path=None, url=None, email=None, s3_config=None)

        mock_fetch.assert_called_once_with(flow_url, flow_email)
        uploaded_s3_config = mock_upload.call_args.args[1]
        assert uploaded_s3_config.key_id == 'test-key-id'
        assert uploaded_s3_config.secret == 'test-secret'
        assert uploaded_s3_config.endpoint == 's3.example.com'
        assert uploaded_s3_config.region == 'test_region'

    def test_load_secret(self, monkeypatch):
        mock_secret_block = MagicMock()
        mock_secret_block.get.return_value = 'test-secret-value'
        async_mock = AsyncMock(return_value=mock_secret_block)
        monkeypatch.setattr(Secret, 'aload', async_mock)

        result = _load_secret('test-secret-name')

        assert result == 'test-secret-value'
        async_mock.assert_called_once_with('test-secret-name')

    def test_load_variable(self, monkeypatch):
        async_mock = AsyncMock(return_value='test-variable-value')
        monkeypatch.setattr(Variable, 'aget', async_mock)

        result = _load_variable('test-variable-name')

        assert result == 'test-variable-value'
        async_mock.assert_called_once_with('test-variable-name')

    def test_load_variable_coerces_non_string_value(self, monkeypatch):
        async_mock = AsyncMock(return_value=123)
        monkeypatch.setattr(Variable, 'aget', async_mock)

        result = _load_variable('test-variable-name')

        assert result == '123'
        assert isinstance(result, str)
