from typing import Literal, get_args

import polars as pl
import streamlit as st

from database.s3_connection import s3_connection

ViewName = Literal['songs', 'charts', 'top10', 'top25', 'all_songs_ranked']
ALLOWED_VIEWS: frozenset[str] = frozenset(get_args(ViewName))

key_id = st.secrets['garage']['key_id']
secret = st.secrets['garage']['secret']
endpoint = st.secrets['garage']['endpoint']
region = st.secrets['garage']['region']


@st.cache_data(ttl=60 * 60 * 24, show_spinner='Lejuplādē datus...', show_time=True)
def get_view(*, view: ViewName) -> pl.DataFrame:
    """Function to return the database view or table as a polars dataframe"""
    if view not in ALLOWED_VIEWS:
        raise ValueError(
            f"""Norādīts neatļauts skata vai tabulas nosaukums,
            izvēlies kādu no {ALLOWED_VIEWS}."""
        )
    with s3_connection(
        key_id=key_id,
        secret=secret,
        endpoint=endpoint,
        region=region,
    ) as conn:
        df = conn.sql(
            f"""--sql
            select
                *
            from
                's3://naba-top/{view}.parquet';
            """,
        ).pl()

    return df
