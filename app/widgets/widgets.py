from datetime import date, timedelta

import streamlit as st

from app.data.get_data import get_date_range
from app.state.manage_state import StateKeys, store_state_value


def shared_slider(
    default: date,
    key: str = StateKeys.SELECTED_WEEK,
):
    """
    Create a slider with predefined min and max value, set desired default value.
    Reuse the slider's value on another page.
    """
    min_date, max_date = get_date_range()
    slider = st.slider(
        'Datuma izvēle',
        min_value=min_date,
        max_value=max_date,
        step=timedelta(weeks=1),
        value=default,
        key='_' + key,
        on_change=store_state_value,
        kwargs={'key': key},
        label_visibility='visible',
        format='localized',
    )
    return slider
