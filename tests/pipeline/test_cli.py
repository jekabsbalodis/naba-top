"""End to end: run the real pipeline steps against a fake website and temp folders."""

import os
import re
import shutil
from pathlib import Path
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest
from typer.testing import CliRunner

from database.init_db import init_db
from pipeline.__main__ import app
from pipeline.config import config, stage_dir
from tests.helpers import make_sqlmesh_db, naba_page

runner = CliRunner()

# The names `data_transform/config.yaml` reads, set by the `update-models` step.
SQLMESH_ENV_VARS = (
    'NABA_TOP_DATA__DATA_DIR',
    'NABA_TOP_DATA__RAW_DATA_DB',
    'NABA_TOP_DATA__SQLMESH_DATA_DB',
    'NABA_TOP_DATA__SQLMESH_STATE_DB',
)


class FakeSQLMesh:
    """Stands in for `sqlmesh.Context`: records the call instead of running SQLMesh.

    A real `run` needs a `prod` environment that `sqlmesh plan` creates once,
    and which models are due depends on today's date, so it is not run in tests.
    """

    def __init__(self) -> None:
        self.project: str | None = None
        self.runs = 0
        self.env: dict[str, str | None] = {}
        self.failure = False

    def __call__(self, paths: str) -> FakeSQLMesh:  # `Context(paths=...)`
        self.project = paths
        return self

    def run(self) -> SimpleNamespace:
        self.runs += 1
        self.env = {name: os.environ.get(name) for name in SQLMESH_ENV_VARS}
        return SimpleNamespace(is_failure=self.failure)


@pytest.fixture(autouse=True)
def sqlmesh(monkeypatch) -> FakeSQLMesh:
    # conftest already sets these for the whole session; clear them so the tests
    # can tell whether the `update-models` step sets them itself
    for name in SQLMESH_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    fake = FakeSQLMesh()
    monkeypatch.setattr('pipeline.__main__.Context', fake)
    return fake


@pytest.fixture(autouse=True)
def clean_slate(monkeypatch, web):
    """Every test starts with empty folders and databases, and the fake website."""
    for name in ('DAG_RUN_ARTIFACTS_DIR', 'DAG_RUN_ID', 'NABA_TOP_RUN_ID'):
        monkeypatch.delenv(name, raising=False)
    data = config.data
    shutil.rmtree(data.data_dir / 'stage', ignore_errors=True)
    shutil.rmtree(data.upload_dir, ignore_errors=True)
    for db in (data.raw_data_path, data.sqlmesh_data_path):
        for suffix in ('', '.wal'):
            Path(f'{db}{suffix}').unlink(missing_ok=True)
    data.ensure_database_dirs()
    init_db(str(data.raw_data_path))
    make_sqlmesh_db(str(data.sqlmesh_data_path))  # normally created by SQLMesh
    web.html = naba_page()  # 45 songs: 10 + 25 ranked, 5 unranked in each chart
    yield
    os.environ.pop('NABA_TOP_RUN_ID', None)  # `run-all` sets this itself


def run(*args: str):
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, result.output
    return result


def count(table: str) -> int:
    with duckdb.connect(str(config.data.raw_data_path)) as conn:
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


class TestUpdateModels:
    def test_runs_sqlmesh_on_the_data_transform_project(self, sqlmesh):
        run('update-models')
        assert sqlmesh.runs == 1
        assert Path(sqlmesh.project or '').name == 'data_transform'
        assert (Path(sqlmesh.project or '') / 'config.yaml').is_file()

    def test_gives_sqlmesh_the_database_locations(self, sqlmesh):
        run('update-models')
        data = config.data
        assert sqlmesh.env == {
            'NABA_TOP_DATA__DATA_DIR': str(data.data_dir),
            'NABA_TOP_DATA__RAW_DATA_DB': data.raw_data_db,
            'NABA_TOP_DATA__SQLMESH_DATA_DB': data.sqlmesh_data_db,
            'NABA_TOP_DATA__SQLMESH_STATE_DB': data.sqlmesh_state_db,
        }

    def test_sqlmesh_config_only_uses_variables_the_step_sets(self, sqlmesh):
        """Catches a renamed variable in config.yaml or in the step."""
        run('update-models')
        config_yaml = (Path(sqlmesh.project or '') / 'config.yaml').read_text()
        used = set(re.findall(r"env_var\('([A-Z0-9_]+)'\)", config_yaml))
        assert used
        assert used <= set(SQLMESH_ENV_VARS)

    def test_fails_when_sqlmesh_fails(self, sqlmesh):
        sqlmesh.failure = True
        result = runner.invoke(app, ['update-models'])
        assert result.exit_code != 0
        assert isinstance(result.exception, RuntimeError)

    def test_run_all_includes_it(self, sqlmesh):
        run('run-all')
        assert sqlmesh.runs == 1


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
