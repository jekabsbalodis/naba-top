"""Custom widgets for the Streamlit application.

Provides reusable widgets with state management.
"""

from datetime import timedelta

import streamlit as st

from app.data.get_data import get_date_range
from app.state.manage_state import StateKeys, store_state_value


def shared_slider(
    key: str = StateKeys.SELECTED_WEEK,
):
    """Create a reusable date slider with state persistance.

    Args:
        key: The state key to use for storing the selected value.
        Defaults to SELECTED_WEEK from StateKeys enum.

    Returns:
        Preconfigured Streamlit slider widget.

    """
    min_date, max_date = get_date_range()
    slider = st.slider(
        'Datuma izvēle',
        min_value=min_date,
        max_value=max_date,
        step=timedelta(weeks=1),
        key='_' + key,
        on_change=store_state_value,
        kwargs={'key': key},
        label_visibility='visible',
        format='localized',
    )
    return slider
