import streamlit as st

from app.home import home
from app.state.manage_state import init_state
from app.top10_page import top10_page
from app.top25_page import top25_page

st.set_page_config(page_icon=':material/content_cut:', layout='wide')

pages = [
    st.Page(home, title='Naba Top', default=True, icon=':material/home:'),
    st.Page(
        top10_page,
        title='Top 10',
        url_path='/top10',
        icon=':material/brightness_empty:',
    ),
    # st.Page(
    #     top25_page,
    #     title='Top 25',
    #     url_path='/top25',
    #     icon=':material/language:',
    # ),
]

page = st.navigation(pages, position='top')

init_state()

page.run()
