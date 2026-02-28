from nicegui import ui

from app.frame import frame

# from auth import setup_auth
from config import config


@ui.page('/')
def home() -> None:
    with frame():
        ui.label('test')


ui.run(
    host=config.HOST,
    port=config.PORT,
    storage_secret=config.STORAGE_SECRET,
    title='test',
)
