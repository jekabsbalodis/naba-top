"""Home page module for the Streamlit application.

This module contains the landing page that users see when they first visit the site.
It displays the Top 10 and Top 25 charts for the latest week as well as some information
about the page and Radio Naba.
"""

import streamlit as st

from app.data.get_data import get_chart, get_date_range
from app.utils.format import get_date_string


def home() -> None:
    """Create the landing page with latest Top 10 and Top 25 charts."""
    _, week = get_date_range()
    week_str = get_date_string(week)
    top10, top25 = get_chart(week)

    column_config = {
        'artist': st.column_config.TextColumn(
            label='Izpildītājs',
        ),
        'song_name': st.column_config.TextColumn(
            label='Dziesmas nosakums',
        ),
        'place': st.column_config.NumberColumn(
            label='Vieta',
            width=50,
            pinned=True,
        ),
    }

    st.title('Radio NABA Top&nbsp;10 un Top&nbsp;25')

    st.markdown(f"""
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
            f'### Latvijas mūzikas Top&nbsp;10 :red-badge[{len(top10)} dziesmas]'
        )
        st.dataframe(
            top10,
            height=35 * len(top10) + 38,
            hide_index=True,
            placeholder='-',
            column_config=column_config,
            column_order=['place', 'artist', 'song_name'],
        )

    with col2:
        st.markdown(
            f'### Ārzemju mūzikas Top&nbsp;25 :red-badge[{len(top25)} dziesmas]'
        )
        st.dataframe(
            top25,
            height=35 * len(top25) + 38,
            hide_index=True,
            placeholder='-',
            column_config=column_config,
            column_order=['place', 'artist', 'song_name'],
        )
