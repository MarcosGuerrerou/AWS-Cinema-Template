import streamlit as st, pandas as pd
from utils import TMDB

st.set_page_config(page_title='Coming Soon', page_icon='🍿')

st.markdown('# Coming Soon!')

api = TMDB() 

if 'upcoming' not in st.session_state:
    upcoming = api.get_movies_upcoming(include_images = True, n_newest = 0)
    st.session_state['upcoming'] = upcoming
else:
    upcoming = st.session_state['upcoming']
    if not isinstance(upcoming, pd.DataFrame):
        upcoming = api.get_movies_upcoming(include_images = True, n_newest = 0)


for idx, row in upcoming.iterrows():
    with st.container():
        columns = st.columns(2)
        columns[0].image(row['img_url'])
        columns[1].markdown(f"**Title**: {row['title']}")
        columns[1].markdown("")
        columns[1].markdown("**Sypnosys**:")
        columns[1].markdown(row['overview'])
        columns[1].markdown("")
        columns[1].markdown(f"**Release Date**: {row['release_date']}")