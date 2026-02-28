from contextlib import contextmanager

from nicegui import ui


@contextmanager
def frame(navigation_title: str):
    """Custom frame to share between pages"""

    with ui.header().classes('justify-around'):
        ui.label('Radio Naba Top').classes('col-md-6')
        ui.label(navigation_title).classes('col-md-3')

    with ui.column().classes('absolute-top middle'):
        yield
