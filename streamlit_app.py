import streamlit as st

from app.chart_page import top10_page, top25_page
from app.home import home
from app.state.manage_state import init_state

st.set_page_config(
    page_icon=':material/content_cut:',
    layout='wide',
    menu_items={
        'Get help': 'https://mastodon.social/@khorticija',
        'Report a bug': 'https://codeberg.org/clear9550/naba-top/issues',
        'About': None,
    },
)

st.set_option('client.toolbarMode', 'minimal')

pages = [
    st.Page(home, title='Naba Top', default=True, icon=':material/home:'),
    st.Page(
        top10_page,
        title='Top 10',
        url_path='/top10',
        icon=':material/brightness_empty:',
    ),
    st.Page(
        top25_page,
        title='Top 25',
        url_path='/top25',
        icon=':material/language:',
    ),
]

page = st.navigation(pages, position='top')

init_state()

page.run()
