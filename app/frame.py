from contextlib import contextmanager

from nicegui import app, ui

MODES = [None, False, True]
ICONS = {None: 'brightness_auto', False: 'light_mode', True: 'dark_mode'}


@contextmanager
def frame(navigation_title: str | None = None):
    """Custom frame to share between pages"""
    dark = ui.dark_mode().bind_value(app.storage.user, 'dark_mode')

    def cycle():
        current = app.storage.user.get('dark_mode', None)
        current_index = MODES.index(current)
        next_index = (current_index + 1) % len(MODES)
        next_mode = MODES[next_index]
        dark.value = next_mode
        app.storage.user['dark_mode'] = next_mode
        dark_mode_button.props(f'flat round color=white icon={ICONS[next_mode]}')

    with ui.header().classes('items-center'):
        ui.link('Radio Naba Top', '/').classes(
            'text-bold text-lg text-white no-underline'
        )

        ui.space()

        if navigation_title:
            ui.label(navigation_title)
        ui.space()

        initial_mode = app.storage.user.get('dark_mode', None)
        dark_mode_button = ui.button(icon=ICONS[initial_mode], on_click=cycle).props(
            'flat round color=white'
        )

    with ui.column().classes('absolute-top middle'):
        yield
