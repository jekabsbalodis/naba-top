from nicegui import Client, ui

from auth import setup_auth
from config import config

setup_auth()


@ui.page('/')
def home(client: Client):
    user = client.request.session.get('user')

    if user:
        name = user.get('name') or user.get('email')
        ui.label(f'hello {name}')
        ui.button('logout', on_click=lambda: ui.navigate.to('/logout'))
    else:
        ui.label('hello')
        ui.button('login', on_click=lambda: ui.navigate.to('/login'))


ui.run(
    host=config.HOST,
    port=config.PORT,
    storage_secret=config.STORAGE_SECRET,
    title='test',
)
