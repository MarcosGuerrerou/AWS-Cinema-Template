import streamlit as st, pandas as pd
from utils import TMDB

st.set_page_config(page_title='Showtimes!', page_icon='🍿')

st.markdown('# Now Showing!')

api = TMDB() 

if 'slate' not in st.session_state:
    slate = api.get_movies_playing(include_images = True, n_newest = 0)
    st.session_state['slate'] = slate
else:
    slate = st.session_state['slate']
    if not isinstance(slate, pd.DataFrame):
        slate = api.get_movies_playing(include_images = True, n_newest = 0)

for idx, row in slate.iterrows():
    with st.container():
        columns = st.columns(2)
        columns[0].image(row['img_url'])
        columns[1].markdown(f"**Title**: {row['title']}")
        columns[1].markdown("")
        columns[1].markdown("**Sypnosys**:")
        columns[1].markdown(row['overview'])
        columns[1].markdown("")
        columns[1].markdown(f"**Release Date**: {row['release_date']}")
