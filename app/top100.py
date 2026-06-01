"""Top 100 songs module to display all songs ranked by normalised points.

For unauthorized users displays Top 20 in random order
and rest of the songs (places 21+) in ranked order.

For authorized users displays all Top 100 songs in ranked order.
"""

import random
from dataclasses import dataclass

import polars as pl
import streamlit as st

from app.data.get_data import get_all_songs_ranked


@dataclass(frozen=True)
class Top100Config:
    """Configuration constants for the Top 101 chart selection and display.

    Attributes:
        total: Total number of songs in the final chart
            (101 including the first song just before the top).
        top10_quota: Minimum number of top10 songs that should be in the final chart.
        forecast_zone: Number of top songs hidden from unauthorised users.

    """

    total: int = 101
    top10_quota: int = 40
    forecast_zone: int = 20


_CONFIG = Top100Config()


def build_101_df() -> pl.DataFrame:
    """Get prepared view from the database and modify to meet Top 100 criteria.

    Keeps only the highest rated song for the artist.
    Extracts atleast top 40 songs that are coming from Top 10.
    Fills rest of the Top with songs remaining in the database view.
    """
    raw_df = get_all_songs_ranked()

    deduplicated_df = raw_df.unique(
        subset='artist',
        keep='first',
        maintain_order=True,
    )

    # deduplicated_df = raw_df

    top10_part = deduplicated_df.filter(pl.col('chart_type') == 'top10').head(
        _CONFIG.top10_quota
    )

    remaining_songs = deduplicated_df.filter(
        ~pl.col('song_id').is_in(top10_part['song_id'])
    ).head(_CONFIG.total - len(top10_part))

    top100 = (
        pl.concat([top10_part, remaining_songs])
        .sort('place', descending=False)
        .with_row_index('final_place', offset=1)
    )

    return top100


def build_summary(df: pl.DataFrame) -> None:
    """Show summary statistics about the Top 100."""
    total_songs = len(df)
    top10_songs = len(df.filter(pl.col('chart_type') == 'top10'))
    top25_songs = total_songs - top10_songs

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(label='Dziesmu skaits', value=total_songs)
    with col2:
        st.metric(label='Latviešu kompozīcijas', value=top10_songs)
    with col3:
        st.metric(label='Ārzemju dziesmas', value=top25_songs)


def build_ranked_list(df: pl.DataFrame, start: int = _CONFIG.forecast_zone) -> None:
    """Display a dataframe with songs in ranked order.

    Selects only the place, artist and songname columns.
    By default starts at the first song after the forecast zone.
    """
    display_df = df.filter(pl.col('final_place') > start).select(
        [
            pl.col('final_place'),
            pl.col('artist'),
            pl.col('song_name'),
        ]
    )

    column_config = {
        'artist': st.column_config.TextColumn(
            label='Izpildītājs',
        ),
        'song_name': st.column_config.TextColumn(
            label='Dziesmas nosaukums',
        ),
        'final_place': st.column_config.NumberColumn(
            label='Vieta',
            width=50,
            pinned=True,
        ),
    }

    st.dataframe(
        display_df,
        height=35 * len(display_df) + 38,
        hide_index=True,
        placeholder='-',
        column_config=column_config,
        column_order=['final_place', 'artist', 'song_name'],
    )


def build_forecast_zone(df: pl.DataFrame) -> None:
    """Display shuffled info of songs in top positions in the chart.

    Displayed only for the user that are not logged in.
    """

    songs = df.select(['artist', 'song_name']).to_dicts()

    random.shuffle(songs)

    cols_per_row = 4
    for row in range(0, _CONFIG.forecast_zone, cols_per_row):
        cols = st.columns(cols_per_row)
        for col, song in zip(cols, songs[row : row + cols_per_row], strict=True):
            with col:
                with st.container(border=True):
                    st.caption(song['artist'])
                    st.write(song['song_name'])


def top100_page() -> None:
    """Create page for Top 100 songs ranked."""
    col1, col2 = st.columns(2, vertical_alignment='bottom')

    with col1:
        st.title('Radio NABA Top&nbsp;100 dziesmu tops')

        st.markdown(
            'Šeit apkopotas dziesmas no Top&nbsp;10 un Top&nbsp;25 '
            'topiem un sakārtotas pēc iegūto punktu skaita'
        )

    with col2:
        build_summary(get_all_songs_ranked())

    if st.user.is_logged_in:
        build_ranked_list(build_101_df(), start=0)
        st.divider()
        st.button('Iziet', on_click=st.logout, type='tertiary')

    if not st.user.is_logged_in:
        st.subheader('Naba Top&nbsp;100 saraksta pirmās 20 dziesmas.')
        st.caption('Dziesmas attēlotas sajauktā secībā.')

        build_forecast_zone(build_101_df().head(20))

        st.subheader('Pārējās Topa dziesmas.')
        st.caption('Dziesmas attēlotas tādā secībā, kā tās ierindojas kopējā topā.')

        build_ranked_list(build_101_df())

        st.divider()
        st.button('Pievienoties', on_click=st.login, type='tertiary')
