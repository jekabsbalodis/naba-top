"""Configuration for the naba-top pipeline."""

import os
import re
from pathlib import Path

from config_models import Config

config = Config()


def run_id() -> str:
    """Return the id of the current run shared by all of the steps."""
    value = (
        os.environ.get('NABA_TOP_RUN_ID') or os.environ.get('DAG_RUN_ID') or 'latest'
    )
    if not re.fullmatch(r'[A-Za-z0-9._-]+', value):
        raise ValueError(f'Invalid run id: {value!r}')
    return value


def stage_dir() -> Path:
    """Directory for files handed between steps (created if missing).

    If steps are executed with Dagu and `artifacts.enabled: true`, this is the artifacts
    dir for each run. For manual runs and tests it is `<data dir>/stage/<run id>`.
    """
    artifacts = os.environ.get('DAG_RUN_ARTIFACTS_DIR')
    if artifacts:
        path = Path(artifacts)
        path.mkdir(parents=True, exist_ok=True)
        return path
    return config.data.stage_dir(run_id=run_id())
