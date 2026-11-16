"""Run id and stage folder: where the pipeline steps hand files to each other."""

import pytest

from pipeline.config import config, run_id, stage_dir


@pytest.fixture(autouse=True)
def no_run_variables(monkeypatch):
    for name in ('DAG_RUN_ARTIFACTS_DIR', 'DAG_RUN_ID', 'NABA_TOP_RUN_ID'):
        monkeypatch.delenv(name, raising=False)


def test_stage_dir_defaults_to_latest_inside_data_dir():
    path = stage_dir()
    assert path == config.data.data_dir / 'stage' / 'latest'
    assert path.is_dir()


def test_stage_dir_uses_the_dagu_run_id(monkeypatch):
    monkeypatch.setenv('DAG_RUN_ID', '20261015_090000_c1f4b2')
    assert stage_dir().name == '20261015_090000_c1f4b2'


def test_manual_run_id_wins_over_the_dagu_one(monkeypatch):
    monkeypatch.setenv('DAG_RUN_ID', 'from-dagu')
    monkeypatch.setenv('NABA_TOP_RUN_ID', 'manual')
    assert run_id() == 'manual'


def test_dagu_artifacts_dir_wins_and_is_created(monkeypatch, tmp_path):
    artifacts = tmp_path / 'artifacts' / 'run1'
    monkeypatch.setenv('DAG_RUN_ARTIFACTS_DIR', str(artifacts))
    monkeypatch.setenv('DAG_RUN_ID', 'ignored')
    assert stage_dir() == artifacts
    assert artifacts.is_dir()


@pytest.mark.parametrize('bad', ['${context.run.id}', '../escape', 'a b', 'a/b'])
def test_unsafe_run_ids_are_rejected(monkeypatch, bad):
    """The run id becomes a folder name, so it must not be able to leave `stage/`."""
    monkeypatch.setenv('DAG_RUN_ID', bad)
    with pytest.raises(ValueError, match='Invalid run id'):
        run_id()
