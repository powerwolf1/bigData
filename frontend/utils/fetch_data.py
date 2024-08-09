import streamlit as st
import requests
import pandas as pd
from frontend.utils.config import API_URL


# @st.cache_data(show_spinner=False)
def fetch_data(collection, date_from=None, date_to=None, page_number=None, items_per_page=None):
    url = f'{API_URL}/data'
    skip = page_number * items_per_page
    limit = items_per_page
    params = {
        'collection': collection,
        'limit': limit,
        'skip': skip
    }

    if date_from and date_to:
        params['from'] = date_from.strftime('%d-%m-%Y')
        params['to'] = date_to.strftime('%d-%m-%Y')

    try:
        response = requests.get(url, params=params)
        data = response.json()
        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            return pd.DataFrame(data)
        else:
            st.error("Unexpected data format received from the API")
            return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()


def fetch_schema(collection):
    url = f"{API_URL}/schema"
    params = {'collection': collection}

    try:
        response = requests.get(url, json=params)
        response.raise_for_status()
        return response.json().get('fields', [])
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching schema: {e}")
        return []


def fetch_tva_stats(collection, date_from, date_to):
    url = f'{API_URL}/tva_stats'
    params = {
        'collection': collection,
        'from': date_from.strftime('%d-%m-%Y'),
        'to': date_to.strftime('%d-%m-%Y')
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching TVA stats: {e}")
        return {}


def fetch_counts(collection=None, firma=None, nui_id=None, date_from=None, date_to=None):
    url = f'{API_URL}/filter_by_nui'
    params = {'collection': collection}

    if firma:
        params['firma'] = firma

    if nui_id:
        params['nui_id'] = nui_id

    if date_from and date_to:
        params['from'] = date_from.strftime('%d-%m-%Y')
        params['to'] = date_to.strftime('%d-%m-%Y')

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching counts: {e}")
        return {}


def fetch_nr_z_reports(date_from=None, date_to=None, nr_z=None):
    url = f"{API_URL}/nr_z_reports"
    params = {'collection': st.session_state.collection}

    if date_from and date_to:
        params['from'] = date_from.strftime('%d-%m-%Y')
        params['to'] = date_to.strftime('%d-%m-%Y')

    if nr_z:
        params['nr_z'] = nr_z

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching nr.Z reports: {e}")
        return {}


def fetch_collection_counts():
    url = f"{API_URL}/collection_counts"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching collection counts: {e}")
        return {}


def fetch_sums_by_hour(collection, date_from, date_to):
    url = f'{API_URL}/sums_by_hour'
    params = {
        'collection': collection,
        'from': date_from.strftime('%d-%m-%Y'),
        'to': date_to.strftime('%d-%m-%Y')
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching sums by hour: {e}")
        return []


def fetch_sums_by_day_of_week(collection, date_from, date_to):
    url = f'{API_URL}/sums_by_day_of_week'
    params = {
        'collection': collection,
        'from': date_from.strftime('%d-%m-%Y'),
        'to': date_to.strftime('%d-%m-%Y')
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching sums by day of week: {e}")
        return []


def fetch_filtered_bon_zilnic(collection, date_from, date_to, nr_b):
    url = f'{API_URL}/filtered_bon_zilnic'
    params = {
        'collection': collection,
        'from': date_from.strftime('%d-%m-%Y'),
        'to': date_to.strftime('%d-%m-%Y'),
        'nr_b': nr_b
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching filtered bon_zilnic: {e}")
        return []


def fetch_daily_transactions(collection, date_from, date_to):
    url = f'{API_URL}/daily_transactions'
    params = {
        'collection': collection,
        'from': date_from.strftime('%d-%m-%Y'),
        'to': date_to.strftime('%d-%m-%Y')
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching daily transactions: {e}")
        return []


def delete_collection(collection_name):
    try:
        url = f'{API_URL}/delete_collection'
        response = requests.post(url, json={"collection": collection_name})
        if response.status_code == 200:
            return f"Collection '{collection_name}' deleted successfully."
        else:
            return f"Failed to delete collection: {response.json().get('error')}"
    except Exception as e:
        return f"An error occurred during deleting collection: {str(e)}"


def convert_data_to_timestamp(collection_name):
    try:
        url = f'{API_URL}/convert_data_to_timestamp'
        response = requests.post(url, json={"collection": collection_name})
        if response.status_code == 200:
            return f"Collection '{collection_name}' converted successfully."
        else:
            return f"Failed to convert collection: {response.json().get('error')}"
    except Exception as e:
        return f"An error occurred during converting data to timestamp: {str(e)}"


def aggregate_data():
    try:
        url = f'{API_URL}/aggregate_data'
        response = requests.post(url)
        if response.status_code == 200:
            return f"Data aggregated successfully."
        else:
            return f"Failed to aggregate data: {response.json().get('error')}"
    except Exception as e:
        return f"An error occurred during data aggregation: {str(e)}"


def fetch_all_produs():
    url = f'{API_URL}/get_produs_documents'

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching all products: {e}")
        return []


def fetch_bon_by_id(bon_id):
    url = f'{API_URL}/get_bon_by_id'
    params = {
        'bon_id': bon_id
    }

    try:
        response = requests.get(url, json=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error getting bon by id: {e}")
        return []


def fetch_bon_zilnic(nr_z, DATA, total, totA, totB, totC, totD):
    url = f'{API_URL}/get_bon_zilnic'

    params = {
        'Z': nr_z,
        'DATA': DATA,
        'total': total,
        'totA': totA,
        'totB': totB,
        'totC': totC,
        'totD': totD
    }

    try:
        response = requests.get(url, json=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching bon zilnic: {e}")