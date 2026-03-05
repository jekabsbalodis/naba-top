from datetime import date

from babel import Locale
from babel.dates import format_date

_locale = Locale('lv')


def get_date_string(date: date) -> str:
    """Format a date object to string in Latvian locale."""
    return format_date(date, format='long', locale=_locale)
