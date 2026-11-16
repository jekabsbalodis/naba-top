"""Streamlit side: where the app finds local parquet files in development."""

import pytest

from app.config import local_data_dir
from config_models import StreamlitConfig
from tests.helpers import StreamlitConfigFromDict


def make_config(raw_config: dict, tmp_path, environment: str) -> StreamlitConfig:
    raw_config['environment'] = environment
    raw_config['data']['data_dir'] = str(tmp_path)
    if environment == 'production':
        raw_config['auth']['redirect_uri'] = 'https://app.example.com'
    return StreamlitConfigFromDict.model_validate(raw_config)


def test_production_reads_from_s3_so_there_is_no_local_dir(raw_config, tmp_path):
    assert local_data_dir(make_config(raw_config, tmp_path, 'production')) is None


def test_development_returns_the_upload_dir(raw_config, tmp_path):
    upload = tmp_path / 'upload'
    (upload / 'mart').mkdir(parents=True)
    (upload / 'manifest.json').write_text('[]')
    (upload / 'mart' / 'top10.parquet').write_bytes(b'')

    config = make_config(raw_config, tmp_path, 'development')

    assert local_data_dir(config) == upload


def test_development_fails_clearly_when_there_is_no_data_yet(raw_config, tmp_path):
    config = make_config(raw_config, tmp_path, 'development')
    with pytest.raises(FileNotFoundError, match='upload'):
        local_data_dir(config)
