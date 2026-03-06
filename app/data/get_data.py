from datetime import date
from typing import Literal, get_args

import polars as pl
import streamlit as st

from database.s3_connection import s3_connection

ViewName = Literal['songs', 'charts', 'top10', 'top25', 'all_songs_ranked']
ALLOWED_VIEWS: frozenset[str] = frozenset(get_args(ViewName))


@st.cache_data(ttl=60 * 60 * 24, show_spinner='Lejuplādē datus...', show_time=True)
def get_view(*, view: ViewName) -> pl.DataFrame:
    """
    Get the database view or table as a polars dataframe.
    Args:
        view (ViewName): name of the database view or table.
    Returns:
        Dataframe of selected view.
    """
    if view not in ALLOWED_VIEWS:
        raise ValueError(
            f"""Norādīts neatļauts skata vai tabulas nosaukums,
            izvēlies kādu no {ALLOWED_VIEWS}."""
        )
    with s3_connection() as conn:
        df = conn.sql(
            f"""--sql
            select
                *
            from
                's3://naba-top/{view}.parquet';
            """,
        ).pl()

    return df


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def get_chart(week: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Get the top10 and top25 charts for the selected week.
    Args:
        week (date): The week for which to return the charts.
    Returns:
        Tuple of dataframes - top10 and top25 dataframe.
    """
    top_10_df = get_view(view='top10')
    top_25_df = get_view(view='top25')

    top_10_df = top_10_df.filter(pl.col('week') == week)
    top_25_df = top_25_df.filter(pl.col('week') == week)

    return top_10_df, top_25_df


@st.cache_data(ttl=60 * 60 * 24, show_spinner=False)
def get_date_range() -> tuple[date, date]:
    """
    Get date range of the available data.
    Returns:
        Tuple of min_week and max_week
    """
    charts = get_view(view='charts')

    weeks = charts['week']

    min_week = weeks.min()
    max_week = weeks.max()

    if isinstance(min_week, date) and isinstance(max_week, date):
        return min_week, max_week
    else:
        raise ValueError('Pieejamie dati nav atbiltoša formāta')
