"""End to end: run the real pipeline steps against a fake website and temp folders."""

import os
import shutil
from pathlib import Path

import duckdb
import polars as pl
import pytest
from typer.testing import CliRunner

from database.init_db import init_db
from pipeline.__main__ import app
from pipeline.config import config, stage_dir
from tests.helpers import make_sqlmesh_db, naba_page

runner = CliRunner()


@pytest.fixture(autouse=True)
def clean_slate(monkeypatch, web):
    """Every test starts with empty folders and databases, and the fake website."""
    for name in ('DAG_RUN_ARTIFACTS_DIR', 'DAG_RUN_ID', 'NABA_TOP_RUN_ID'):
        monkeypatch.delenv(name, raising=False)
    data = config.data
    shutil.rmtree(data.data_dir / 'stage', ignore_errors=True)
    shutil.rmtree(data.upload_dir, ignore_errors=True)
    for db in (data.raw_data_db, data.sqlmesh_data_db):
        for suffix in ('', '.wal'):
            Path(db + suffix).unlink(missing_ok=True)
    init_db(data.raw_data_db)
    make_sqlmesh_db(data.sqlmesh_data_db)  # normally created by SQLMesh
    web.html = naba_page()  # 45 songs: 10 + 25 ranked, 5 unranked in each chart
    yield
    os.environ.pop('NABA_TOP_RUN_ID', None)  # `run-all` sets this itself


def run(*args: str):
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, result.output
    return result


def count(table: str) -> int:
    with duckdb.connect(config.data.raw_data_db) as conn:
        row = conn.execute(f'select count(*) from {table}').fetchone()
    assert row is not None
    return row[0]


class TestRunAll:
    def test_loads_songs_and_charts(self):
        run('run-all')
        assert count('songs') == 45
        assert count('charts') == 45

    def test_running_twice_adds_nothing(self):
        run('run-all')
        run('run-all')
        assert (count('songs'), count('charts')) == (45, 45)

    def test_uploaded_files_are_what_streamlit_expects_in_development(self):
        run('run-all')
        assert config.data.require_upload_dir().is_dir()  # same check the app does
        assert (config.data.upload_dir / 'mart' / 'top10.parquet').exists()


class TestSteps:
    def test_fetch_saves_the_page(self, web):
        run('fetch')
        assert 'songsList' in (stage_dir() / 'page.html').read_text()
        assert len(web.requests) == 1

    def test_fetch_fails_when_the_website_errors(self, web):
        web.status = 503
        result = runner.invoke(app, ['fetch'])
        assert result.exit_code != 0
        assert not (stage_dir() / 'page.html').exists()

    def test_steps_hand_over_work_through_stage_files(self, web):
        run('fetch')
        run('parse-songs')
        assert (stage_dir() / 'songs.parquet').exists()
        assert count('songs') == 0  # parsing does not touch the database
        run('load-songs')
        assert count('songs') == 45
        run('parse-charts')
        run('validate-charts')
        assert count('charts') == 0  # validating does not either
        run('load-charts')
        assert count('charts') == 45
        assert len(web.requests) == 1  # only `fetch` goes to the website

    def test_bad_chart_data_stops_before_anything_is_loaded(self):
        for step in ('fetch', 'parse-songs', 'load-songs', 'parse-charts'):
            run(step)
        path = stage_dir() / 'charts.parquet'
        df = pl.read_parquet(path)
        df.head(len(df) - 10).write_parquet(path)  # drop some ranked rows

        result = runner.invoke(app, ['validate-charts'])

        assert result.exit_code != 0
        assert isinstance(result.exception, ValueError)
        assert count('charts') == 0

    def test_steps_share_the_dagu_artifacts_dir(self, monkeypatch, tmp_path):
        monkeypatch.setenv('DAG_RUN_ARTIFACTS_DIR', str(tmp_path / 'artifacts'))
        run('fetch')
        run('parse-songs')
        assert (tmp_path / 'artifacts' / 'songs.parquet').exists()
