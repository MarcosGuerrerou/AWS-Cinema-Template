import streamlit as st, pandas as pd
from utils import TMDB

st.set_page_config(page_title='Tickets', page_icon='🍿')

st.markdown('# Now Showing!')

api = TMDB() 

if 'slate' not in st.session_state:
    slate = api.get_movies_playing(include_images = True, n_newest = 0)
    st.session_state['slate'] = slate
else:
    slate = st.session_state['slate']
    if not isinstance(slate, pd.DataFrame):
        slate = api.get_movies_playing(include_images = True, n_newest = 0)

