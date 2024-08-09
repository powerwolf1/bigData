import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.abspath(os.path.join(current_dir, '..'))
# Add the project directory to the system path
sys.path.append(project_dir)

from functools import partial
import streamlit as st
import pandas as pd
from frontend.utils.display_functions import (display_tva_statistics, display_tax_reports,
                                              display_filtered_bon_zilnic_page, display_daily_transactions_page,
                                              display_validation_page)
from frontend.pages.main_page import main_page
from frontend.utils.fetch_data import fetch_counts
from frontend.utils.display_functions import (display_counts, display_sums_by_hour_page,
                                              display_sums_by_day_of_week_page, display_produs_management_page)
from frontend.utils.pagination_utils import reset_pagination
# Streamlit layout
st.set_page_config(
    page_title="MongoDB Data Dashboard",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def initialize_session_state():
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'Dashboard'
    if 'data' not in st.session_state:
        st.session_state.data = []
    if 'skip' not in st.session_state:
        st.session_state.skip = 0
    if 'limit' not in st.session_state:
        st.session_state.limit = 15000
    if "page_num" not in st.session_state:
        st.session_state.page_num = 1
    if "page_size" not in st.session_state:
        st.session_state.page_size = 25
    if "total_pages" not in st.session_state:
        st.session_state.total_pages = 1


initialize_session_state()


def set_filters():
    st.title("MongoDB Data Dashboard")
    col1, col2 = st.columns(2)

    with col1:
        collection = st.selectbox('Select Collection',
                                  ['ECR.produs', 'ECR.bon', 'ECR.bon_zilnic', 'ECR.firma', 'ECR.nui',
                                   'ECR.bon.parsed', 'ECR.bon_zilnic.parsed', 'ECR.produs.parsed'])
        if collection != st.session_state.get('collection', None):
            st.session_state.collection = collection
            reset_pagination()

    with col2:
        if st.session_state.get('current_page', '') != "Validation":
            date_from = st.date_input('From', value=st.session_state.get('date_from', pd.to_datetime('01-01-2021', dayfirst=True)))
            if date_from != st.session_state.get('date_from', None):
                st.session_state.date_from = date_from
                reset_pagination()

            date_to = st.date_input('To', value=st.session_state.get('date_to', pd.to_datetime('31-01-2021', dayfirst=True)))
            if date_to != st.session_state.get('date_to', None):
                st.session_state.date_to = date_to
                reset_pagination()


set_filters()


def interface():

    display_tva_statistics_partial = partial(display_tva_statistics, st.session_state.collection,
                                             st.session_state.date_from, st.session_state.date_to)

    # Define pages
    dashboard_page = st.Page(main_page, title="Dashboard", icon="🏠")
    validation_page = st.Page(display_validation_page, title="Validation", icon="🔢")
    produs_management_page = st.Page(display_produs_management_page, title="Produs Management Interface", icon="🔢")
    tva_stats_page = st.Page(display_tva_statistics_partial, title="TVA Statistics", icon="📈")
    tax_reports_page = st.Page(display_tax_reports, title="Tax Reports", icon="📋")
    fetch_counts_page_ = st.Page(fetch_counts_page, title="Fetch Counts", icon="🔢")
    transactions_by_hour_page = st.Page(display_sums_by_hour_page, title="Transactions per Hour", icon="🕒")
    transactions_by_day_of_week = st.Page(display_sums_by_day_of_week_page, title="Transactions per Day", icon="📅")
    filtering_bon_zilnic_nr_b = st.Page(display_filtered_bon_zilnic_page, title="Filtered Bon Zilnic", icon="🔍")
    daily_transactions_page = st.Page(display_daily_transactions_page, title="Daily Transactions", icon="📊")

    # Add pages to the sidebar
    pages = [dashboard_page, validation_page, tva_stats_page, tax_reports_page, fetch_counts_page_, transactions_by_hour_page,
             transactions_by_day_of_week, filtering_bon_zilnic_nr_b, daily_transactions_page, produs_management_page]

    # Add navigation
    pg = st.navigation(pages)
    pg.run()

    if st.session_state.current_page != pg.title:
        st.session_state.current_page = pg.title
        st.rerun()


def fetch_counts_page():
    if st.session_state.collection in ['ECR.bon', 'ECR.bon_zilnic', 'ECR.bon.parsed', 'ECR.bon_zilnic.parsed']:
        st.title("Fetch Counts")
        firma_filter = st.text_input('Filter by Firma')
        nui_filter = st.text_input('Filter by NUI ID')

        if st.button('Fetch Counts'):
            if not firma_filter and not nui_filter:
                st.error('Missing required fields: Firma or NUI')

            counts_data = fetch_counts(collection=st.session_state.collection, firma=firma_filter, nui_id=nui_filter,
                                       date_from=st.session_state.date_from, date_to=st.session_state.date_to)
            display_counts(counts_data)
    else:
        st.write('We only filter by Firma and NUI ID for ECR.bon and ECR.bon_zilnic!')


if __name__ == "__main__":
    interface()
