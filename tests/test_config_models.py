"""Rules for the config. Streamlit and the pipeline share these models."""

import json
import os

import pytest
from pydantic import ValidationError

from config_models import ENV_PREFIX, Data, StreamlitData
from scripts.make_dotenv import flatten
from tests.helpers import ConfigFromDict, ConfigFromEnv, StreamlitConfigFromDict


def set_value(data: dict, dotted_key: str, value) -> None:
    """set_value(d, 'auth.client_id', 'x') sets d['auth']['client_id'] = 'x'."""
    *parents, leaf = dotted_key.split('.')
    for parent in parents:
        data = data[parent]
    data[leaf] = value


# --- what is accepted -------------------------------------------------------


def test_full_config_is_valid(raw_config):
    ConfigFromDict.model_validate(raw_config)


def test_streamlit_ignores_pipeline_only_keys(raw_config):
    """Streamlit can read the full file and just skips what it doesn't use."""
    config = StreamlitConfigFromDict.model_validate(raw_config)
    assert config.garage.read.key_id == 'read-id'


def test_streamlit_needs_only_a_subset_in_production(raw_config):
    """The shape pasted into Streamlit Community Cloud secrets."""
    raw_config['environment'] = 'production'
    raw_config['auth']['redirect_uri'] = 'https://app.example.com'
    subset = {key: raw_config[key] for key in ('environment', 'garage', 'auth')}
    del subset['garage']['write']

    assert StreamlitConfigFromDict.model_validate(subset).data is None


def test_secrets_are_hidden_in_repr(raw_config):
    assert 'read-secret' not in repr(ConfigFromDict.model_validate(raw_config))


# --- what is rejected -------------------------------------------------------

INVALID_VALUES = [
    ('data.data_dir', 'relative/path'),
    ('auth.cookie_secret', '   '),
    ('auth.server_metadata_url', 'http://auth.example.com/metadata'),
    ('scrape.url', 'http://www.example.com'),
    ('scrape.email', 'not-an-email'),
    ('pipeline.ntfy_url', 'http://ntfy.example.com/topic'),
    ('garage.endpoint', ''),
    ('garage.write.secret', ''),
]


@pytest.mark.parametrize(
    ('key', 'bad_value'), INVALID_VALUES, ids=[key for key, _ in INVALID_VALUES]
)
def test_invalid_value_is_rejected(raw_config, key, bad_value):
    set_value(raw_config, key, bad_value)
    with pytest.raises(ValidationError):
        ConfigFromDict.model_validate(raw_config)


def test_http_redirect_is_rejected_in_production(raw_config):
    raw_config['environment'] = 'production'  # redirect_uri is http in the test config
    with pytest.raises(ValidationError, match='https'):
        StreamlitConfigFromDict.model_validate(raw_config)


def test_development_requires_the_data_table(raw_config):
    del raw_config['data']
    with pytest.raises(ValidationError, match='data_dir'):
        StreamlitConfigFromDict.model_validate(raw_config)


def test_full_config_requires_the_write_keys(raw_config):
    del raw_config['garage']['write']
    with pytest.raises(ValidationError):
        ConfigFromDict.model_validate(raw_config)


def test_full_config_rejects_unknown_keys(raw_config):
    raw_config['pipelin'] = {}  # typo
    with pytest.raises(ValidationError):
        ConfigFromDict.model_validate(raw_config)


# --- dotenv file <-> config -------------------------------------------------


def test_generated_dotenv_loads_back_into_the_same_config(raw_config, monkeypatch):
    """`make_dotenv` and the pipeline must agree on variable names."""
    for name in [name for name in os.environ if name.startswith(ENV_PREFIX)]:
        monkeypatch.delenv(name)
    for line in flatten(raw_config):
        name, value = line.split('=', 1)
        monkeypatch.setenv(name, json.loads(value))

    from_env = ConfigFromEnv()
    from_dict = ConfigFromDict.model_validate(raw_config)
    assert from_env.model_dump() == from_dict.model_dump()


# --- helpers on the data models ---------------------------------------------


def make_data(tmp_path) -> Data:
    return Data(
        data_dir=tmp_path,
        raw_data_db='raw.duckdb',
        sqlmesh_data_db='data.duckdb',
        sqlmesh_state_db='state.duckdb',
    )


def test_stage_dir_is_created_and_can_be_requested_again(tmp_path):
    data = make_data(tmp_path)
    path = data.stage_dir('run1')
    assert path == tmp_path / 'stage' / 'run1'
    assert path.is_dir()
    assert data.stage_dir('run1') == path  # every Dagu step asks for it again


def test_missing_data_dir_is_an_error(tmp_path):
    data = StreamlitData(data_dir=tmp_path / 'missing')
    with pytest.raises(FileNotFoundError, match='data_dir'):
        data.require_data_dir()


def test_ensure_upload_dir_creates_it(tmp_path):
    assert make_data(tmp_path).ensure_upload_dir().is_dir()


def make_upload(tmp_path, *, manifest: bool, parquet: bool):
    upload = tmp_path / 'upload'
    (upload / 'mart').mkdir(parents=True)
    if manifest:
        (upload / 'manifest.json').write_text('[]')
    if parquet:
        (upload / 'mart' / 'top10.parquet').write_bytes(b'')


def test_upload_dir_with_manifest_and_parquet_is_accepted(tmp_path):
    make_upload(tmp_path, manifest=True, parquet=True)
    data = StreamlitData(data_dir=tmp_path)
    assert data.require_upload_dir() == tmp_path / 'upload'


@pytest.mark.parametrize(
    ('manifest', 'parquet', 'message'),
    [(False, True, 'manifest.json'), (True, False, 'parquet')],
)
def test_incomplete_upload_dir_is_an_error(tmp_path, manifest, parquet, message):
    make_upload(tmp_path, manifest=manifest, parquet=parquet)
    with pytest.raises(FileNotFoundError, match=message):
        StreamlitData(data_dir=tmp_path).require_upload_dir()


def test_missing_upload_dir_is_an_error(tmp_path):
    with pytest.raises(FileNotFoundError, match='does not exist'):
        StreamlitData(data_dir=tmp_path).require_upload_dir()
