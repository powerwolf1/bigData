import streamlit as st


def reset_pagination():
    st.session_state.data = []
    st.session_state.skip = 0
    st.session_state.limit = 1000