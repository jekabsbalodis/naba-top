"""Load streamlit app config."""

from pathlib import Path

import streamlit as st

from config_models import StreamlitConfig


@st.cache_resource()
def get_config() -> StreamlitConfig:
    """Load streamlit app config from st.secrets."""
    try:
        secrets = st.secrets.to_dict()
    except FileNotFoundError:
        secrets = {}
    return StreamlitConfig(**secrets)


def local_data_dir(config: StreamlitConfig) -> Path | None:
    """Local parquet data for development, None in production."""
    if config.environment != 'development' or config.data is None:
        return None
    return config.data.require_upload_dir()
