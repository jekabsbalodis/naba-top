from enum import StrEnum

import streamlit as st

from app.data.get_data import get_date_range


class StateKeys(StrEnum):
    """
    Values for the session state.
    """

    SELECTED_WEEK = 'selected_week'


def init_state(session_state=st.session_state) -> None:
    if StateKeys.SELECTED_WEEK not in session_state:
        _, max_week = get_date_range()
        session_state[StateKeys.SELECTED_WEEK] = max_week


def store_state_value(session_state=st.session_state, *, key: str) -> None:
    session_state[key] = session_state['_' + key]


def load_state_value(session_state=st.session_state, *, key: str) -> None:
    session_state['_' + key] = session_state[key]
