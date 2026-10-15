from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import httpx
import polars as pl
import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from database.init_db import init_db
from pipeline import config
from pipeline.__main__ import app


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
    with patch('pipeline.shared_tasks.httpx.Client') as mock_client_cls:
        client = MagicMock()
        client.__enter__ = MagicMock(return_value=client)
        client.__exit__ = MagicMock(return_value=False)
        client.get.return_value = response
        mock_client_cls.return_value = client
        yield


@pytest.fixture(autouse=True)
def mock_upload():
    with patch('pipeline.shared_tasks.s3_connection', return_value=MagicMock()) as mock:
        yield mock


runner = CliRunner()


@pytest.fixture(autouse=True)
def env(monkeypatch, db_path, flow_url, flow_email, s3_config):
    """Point the pipeline's environment variables at the test fixtures."""
    monkeypatch.setenv('NABA_TOP_DB_PATH', Path(db_path).name)
    monkeypatch.setenv('NABA_TOP_URL', flow_url)
    monkeypatch.setenv('NABA_TOP_EMAIL', flow_email)
    monkeypatch.setenv('S3_KEY_ID', s3_config.key_id)
    monkeypatch.setenv('S3_SECRET', s3_config.secret)
    monkeypatch.setenv('S3_ENDPOINT', s3_config.endpoint)
    monkeypatch.setenv('S3_REGION', s3_config.region)
    for var in ('DAG_RUN_ARTIFACTS_DIR', 'DAG_RUN_ID', 'NABA_TOP_RUN_ID'):
        monkeypatch.delenv(var, raising=False)
    for f in config.stage_dir().glob('*'):
        f.unlink()


def _run(*args: str):
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, result.output
    return result


def _count(db_path: str, table: str) -> int:
    with duckdb.connect(db_path) as conn:
        row = conn.sql(f'select count(*) from {table}').fetchone()
    assert row is not None
    return row[0]


class TestRunAll:
    def test_inserts_songs_and_charts(self, db_path):
        _run('run-all')
        assert _count(db_path, 'songs') == 45
        assert _count(db_path, 'charts') == 45

    def test_idempotent_on_second_run(self, db_path):
        _run('run-all')
        first = (_count(db_path, 'songs'), _count(db_path, 'charts'))
        _run('run-all')
        assert (_count(db_path, 'songs'), _count(db_path, 'charts')) == first


class TestSteps:
    """Each step works on its own, using only the files of the previous steps."""

    def test_fetch_saves_html(self):
        _run('fetch')
        assert 'songsList' in (config.stage_dir() / 'page.html').read_text()

    def test_steps_chain_through_stage_files(self, db_path):
        _run('fetch')
        _run('parse-songs')
        assert (config.stage_dir() / 'songs.parquet').exists()
        assert _count(db_path, 'songs') == 0  # parsing does not touch the db
        _run('load-songs')
        assert _count(db_path, 'songs') == 45
        _run('parse-charts')
        _run('validate-charts')
        assert _count(db_path, 'charts') == 0  # validation happens before loading
        _run('load-charts')
        assert _count(db_path, 'charts') == 45
        _run('upload')

    def test_parse_can_be_rerun_without_refetching(self, db_path):
        _run('fetch')
        with patch('pipeline.shared_tasks.httpx.Client') as client_cls:
            _run('parse-songs')
            client_cls.assert_not_called()

    def test_validate_charts_fails_on_bad_data_before_load(self, db_path):
        _run('fetch')
        _run('parse-songs')
        _run('load-songs')
        _run('parse-charts')
        path = config.stage_dir() / 'charts.parquet'
        df = pl.read_parquet(path)
        df.head(len(df) - 10).write_parquet(path)  # drop some ranked rows

        result = runner.invoke(app, ['validate-charts'])

        assert result.exit_code != 0
        assert isinstance(result.exception, ValueError)
        assert _count(db_path, 'charts') == 0


class TestConfig:
    def test_missing_env_var_raises(self, monkeypatch):
        monkeypatch.delenv('NABA_TOP_DATA_DIR')
        with pytest.raises(LookupError, match='NABA_TOP_DATA_DIR'):
            config.data_dir()

    def test_upload_does_not_need_scrape_settings(self, monkeypatch):
        monkeypatch.delenv('NABA_TOP_URL')
        monkeypatch.delenv('NABA_TOP_EMAIL')
        assert config.s3_config().key_id == 'test_id'

    def test_validate_url_raises_on_invalid_url(self):
        with pytest.raises(ValidationError):
            config.validate_url('not-a-url')

    def test_validate_email_raises_on_invalid_email(self):
        with pytest.raises(ValidationError):
            config.validate_email('not-an-email')

    def test_validate_db_path_returns_normalized_path(self, tmp_path):
        db_file = tmp_path / 'db.duckdb'
        assert config.validate_db_path(str(db_file)) == str(db_file)

    def test_validate_db_path_raises_on_missing_parent(self):
        with pytest.raises(ValueError, match='DB_PATH parent directory does not exist'):
            config.validate_db_path('/nonexistent/path/db.duckdb')


class TestStageDir:
    def test_defaults_to_latest_folder_in_data_dir(self):
        assert config.stage_dir() == config.data_dir() / 'stage' / 'latest'

    def test_uses_dagu_run_id(self, monkeypatch):
        monkeypatch.setenv('DAG_RUN_ID', '20261015_090000_c1f4b2')
        assert config.stage_dir().name == '20261015_090000_c1f4b2'

    def test_manual_run_id_wins_over_dagu_run_id(self, monkeypatch):
        monkeypatch.setenv('DAG_RUN_ID', 'from-dagu')
        monkeypatch.setenv('NABA_TOP_RUN_ID', 'manual')
        assert config.run_id() == 'manual'

    def test_artifacts_dir_wins_when_dagu_sets_it(self, monkeypatch, tmp_path):
        artifacts = tmp_path / 'dag-run_20261015_090000Z_c1f4b2'
        monkeypatch.setenv('DAG_RUN_ARTIFACTS_DIR', str(artifacts))
        monkeypatch.setenv('DAG_RUN_ID', 'ignored')
        assert config.stage_dir() == artifacts
        assert artifacts.is_dir()

    @pytest.mark.parametrize('bad', ['${context.run.id}', '../escape', 'a b', 'a/b'])
    def test_rejects_unsafe_run_id(self, monkeypatch, bad):
        monkeypatch.setenv('DAG_RUN_ID', bad)
        with pytest.raises(ValueError, match='Invalid run id'):
            config.run_id()

    def test_run_all_uses_its_own_run_folder(self, db_path):
        _run('run-all')
        runs = [
            p for p in (config.data_dir() / 'stage').iterdir() if p.name != 'latest'
        ]
        assert any((p / 'charts.parquet').exists() for p in runs)

    def test_steps_share_the_artifacts_dir(self, monkeypatch, tmp_path, db_path):
        monkeypatch.setenv('DAG_RUN_ARTIFACTS_DIR', str(tmp_path / 'artifacts'))
        _run('fetch')
        _run('parse-songs')
        assert (tmp_path / 'artifacts' / 'songs.parquet').exists()
