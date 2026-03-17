"""Formatting utilities for the Streamlit application."""

from datetime import date

from babel import Locale
from babel.dates import format_date

_locale = Locale('lv')


def get_date_string(date: date) -> str:
    """Format a date object as a localized Latvian date string.

    Args:
        date: The date object ot format.

    Returns:
        Formatted date string in Latvian long format.

    """
    return format_date(date, format='long', locale=_locale)
