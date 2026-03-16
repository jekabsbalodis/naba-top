"""State management module for the Streamlit application.

Handles session state value initialization and persistence accros page navigation.
"""

from enum import StrEnum

import streamlit as st

from app.data.get_data import get_date_range


class StateKeys(StrEnum):
    """Values for the session state."""

    SELECTED_WEEK = 'selected_week'


def init_state(session_state=st.session_state) -> None:
    """Initialize session state with default values.

    Args:
        session_state: The Streamlit session state object.

    """
    if StateKeys.SELECTED_WEEK not in session_state:
        _, max_week = get_date_range()
        session_state[StateKeys.SELECTED_WEEK] = max_week


def store_state_value(session_state=st.session_state, *, key: str) -> None:
    """Store a session state value.

    Copies temporary widget state to permanent session state value.

    Args:
        session_state: The Streamlit session state object.
        key: The state key to store.

    """
    session_state[key] = session_state['_' + key]


def load_state_value(session_state=st.session_state, *, key: str) -> None:
    """Load a session state value.

    Copies permanent session state value to temporary widget state.

    Args:
        session_state: The Streamlit session state object.
        key: The state key to store.

    """
    session_state['_' + key] = session_state[key]
