"""Data retrieval module for the Streamlit application.

Provides functions to fetch data from the database and process it for display.
"""

from collections.abc import Generator
from contextlib import contextmanager
from datetime import date, datetime

import duckdb
import polars as pl
import streamlit as st

import config as global_config
from database.s3_connection import s3_connection
from models import S3Config


def is_local() -> bool:
    """Get current app environment."""
    return global_config.get_environment() == global_config.Environment.DEV


@contextmanager
def db_context() -> Generator[tuple[duckdb.DuckDBPyConnection, str]]:
    """Yield a DuckDB connection and base source path for the current environment."""
    if is_local():
        source = global_config.local_upload_dir().absolute().as_posix()
        with duckdb.connect() as conn:
            yield conn, source
    else:
        s3_config = S3Config(
            key_id=st.secrets['garage']['key_id'],
            secret=st.secrets['garage']['secret'],
            endpoint=st.secrets['garage']['endpoint'],
            region=st.secrets['garage']['region'],
        )
        with s3_connection(s3_config=s3_config) as conn:
            yield conn, 's3://naba-top'


@st.cache_data(ttl=60 * 60 * 24)
def get_available_views() -> list[tuple[str, str, datetime]]:
    """Get schemas and views available in S3 storage."""
    with db_context() as (conn, source):
        manifest = source + '/manifest.json'
        return conn.execute(
            """--sql
            select * from read_json(?)
            """,
            (manifest,),
        ).fetchall()


@st.cache_data(ttl=60 * 60 * 24, show_spinner='Lejuplādē datus...', show_time=True)
def get_view(schema, view) -> pl.DataFrame:
    """Get the database view or table.

    Args:
        schema: Name of the database schema.
        view: Name of the database view or table.

    Returns:
        The selected view.

    """
    available = get_available_views()
    allowed = {(s, v) for s, v, _ in available}
    if (schema, view) not in allowed:
        raise ValueError(
            f'Nezināms skats: {schema}.{view}.\nIzvēlies kādu no {allowed}.'
        )
    with db_context() as (conn, source):
        base = f'{source}/{schema}/{view}'
        return conn.read_parquet(f'{base}.parquet').pl()


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def get_chart(week: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Get the top10 and top25 charts for the selected week.

    Args:
        week: The week for which to return the charts.

    Returns:
        Tuple of top10 and top25 charts for the selected week.

    """
    top_10_df = get_view(schema='mart', view='top10')
    top_25_df = get_view(schema='mart', view='top25')

    top_10_df = top_10_df.filter(pl.col('week') == week)
    top_25_df = top_25_df.filter(pl.col('week') == week)

    return top_10_df, top_25_df


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def get_date_range() -> tuple[date, date]:
    """Get date range of the available data.

    Returns:
        Tuple of min_week and max_week

    """
    charts = get_view(schema='fct', view='charts')

    weeks = charts['week']

    min_week = weeks.min()
    max_week = weeks.max()

    if isinstance(min_week, date) and isinstance(max_week, date):
        return min_week, max_week
    else:
        raise ValueError('Pieejamie dati nav atbiltoša formāta')


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def get_all_songs_ranked() -> pl.DataFrame:
    """Get database view for all songs ranked by normalized points.

    Returns a view with all the songs that have been in ranked places
    and their points summed up and normalized.

    Returns:
        Dataframe of all songs ranked.

    """
    return get_view(schema='mart', view='all_songs_ranked')
