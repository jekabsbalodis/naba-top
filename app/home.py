from datetime import date

import polars as pl
import streamlit as st

from app.data.get_data import get_view
from app.utils.format import get_date_string


@st.cache_data
def _get_latest_charts(week: date) -> tuple[pl.DataFrame, pl.DataFrame]:
    top_10_df = get_view(view='top10')
    top_25_df = get_view(view='top25')

    top_10_df = top_10_df.filter(pl.col('week') == week)
    top_25_df = top_25_df.filter(pl.col('week') == week)

    top_10_df = top_10_df['artist', 'song_name', 'place']
    top_25_df = top_25_df['artist', 'song_name', 'place']
    return top_10_df, top_25_df


@st.cache_data
def _get_latest_week() -> date:
    charts = get_view(view='charts')

    last_week = charts['week'].max()

    if last_week is None:
        raise ValueError('Nav pieejami dati par pēdējo nedēļu.')

    if isinstance(last_week, date):
        return last_week
    else:
        raise ValueError('Pieejamie dati nav atbilstoša formāta.')


def home() -> None:
    week = _get_latest_week()
    week_str = get_date_string(week)
    top10, top25 = _get_latest_charts(week)

    column_config = {
        'artist': st.column_config.TextColumn(
            label='Izpildītājs',
            # width='large',
        ),
        'song_name': st.column_config.TextColumn(
            label='Dziesmas nosakums',
            # width='large',
        ),
        'place': st.column_config.NumberColumn(
            label='Vieta',
            width=50,
            pinned=True,
        ),
    }

    st.markdown(f"""

    # Radio NABA Top&nbsp;10 un Top&nbsp;25

    Šeit apkopoti iknedēļas Radio NABA topi -
    Latvijas mūzikas Top&nbsp;10 un ārzemju mūzikas Top&nbsp;25.

    Latvijas Radio 6 - Latvijas Universitātes Radio NABA ir radiostacija,
    kas pirmo reizi ēterā izskanēja 2002.&nbsp; gada 1.&nbsp;decembrī.
    Tā ir tapusi ar Latvijas Universitātes un Latvijas Radio atbalstu.
    Vairāk par radiostaciju var izlasīt [tās mājaslapā](https://www.naba.lv/par/par-naba/).

    ## {week_str}
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            f'#### Latvijas mūzikas Top&nbsp;10 :red-badge[{len(top10)} dziesmas]'
        )
        st.dataframe(
            top10,
            height='stretch',
            hide_index=True,
            placeholder='-',
            column_config=column_config,
            column_order=['place', 'artist', 'song_name'],
        )

    with col2:
        st.markdown(
            f'#### Ārzemju mūzikas Top&nbsp;25 :red-badge[{len(top25)} dziesmas]'
        )
        st.dataframe(
            top25,
            height='stretch',
            hide_index=True,
            placeholder='-',
            column_config=column_config,
            column_order=['place', 'artist', 'song_name'],
        )
