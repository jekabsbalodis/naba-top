import polars as pl
import streamlit as st

from app.data.get_data import get_chart, get_date_range
from app.state.manage_state import StateKeys, load_state_value
from app.utils.format import get_date_string
from app.widgets.widgets import shared_slider


def top10_page() -> None:
    load_state_value(key=StateKeys.SELECTED_WEEK)
    _, latest_week = get_date_range()
    top10df, _ = get_chart(week=st.session_state[StateKeys.SELECTED_WEEK])
    top10df_new_entries = top10df.filter(pl.col('is_new_entry'))['artist', 'song_name']
    st.title('Latvijas mūzikas Top&nbsp;10')

    col1, col2 = st.columns([3, 7])

    with col1:
        st.markdown("""
                Šī sadaļa paredzēta Top&nbsp;10 sarakstu apskatei.
                Izvēlies datumu, un aplūko, kāds bija attiecīgās nedēļas tops.
                """)
        shared_slider(default=latest_week)
        st.divider()
        st.text(
            f'{get_date_string(st.session_state[StateKeys.SELECTED_WEEK])}'
            ' - šīs nedēļas jaunumi'
        )
        st.dataframe(top10df_new_entries)
