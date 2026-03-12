"""Module for UI widget components."""

from datetime import date, timedelta

import streamlit as st

from app.data.get_data import get_date_range
from app.state.manage_state import StateKeys, store_state_value


def shared_slider(
    key: str = StateKeys.SELECTED_WEEK,
):
    """Create a slider with preconfigured parameters.

    Args:
        default: The default value to set for the slider.
        key: Value from StateKeys StrEnum to which this slider should be tied to.

    Returns:
        Streamlit slider widget.

    Note:
        The widget's key is similar to corresponding session state key,
        but have a prefixed '_'.

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
