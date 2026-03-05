import streamlit as st

from app.home import home

st.set_page_config(page_icon=':material/content_cut:', layout='wide')

pages = [
    st.Page(home, title='Naba Top', default=True, icon=':material/home:'),
    # st.Page(
    #     'app/second.py', title='test', url_path='/second', icon=':material/nature:'
    # ),
]

page = st.navigation(pages, position='top')

page.run()
