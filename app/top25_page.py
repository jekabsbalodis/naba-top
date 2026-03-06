import streamlit as st

from app.data.get_data import get_date_range
from app.state.manage_state import StateKeys, load_state_value
from app.widgets.widgets import shared_slider


def top25_page() -> None:
    load_state_value(key=StateKeys.SELECTED_WEEK)
    _, latest_week = get_date_range()
    shared_slider(default=latest_week)
    st.write('test')
    st.write(st.session_state)
