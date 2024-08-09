import streamlit as st
import requests
from frontend.utils.config import API_URL
from bson.objectid import ObjectId

from frontend.utils.fetch_data import fetch_schema


def add_document(collection, fields):
    generate_id = st.checkbox('Generate ID', value=True)
    with st.form(key='add_document_form'):
        new_document_data = {}

        if not generate_id:
            new_document_data['_id'] = st.text_input('_id', key='custom_id')

        for field in fields:
            if field != '_id':
                new_document_data[field] = st.text_input(field)

        submitted = st.form_submit_button('Add Document')
        if submitted:
            try:
                if generate_id:
                    new_document_data['_id'] = str(ObjectId())

                add_url = f'{API_URL}/add'
                add_data = {
                    'collection': collection,
                    'new_document': new_document_data
                }
                response = requests.post(add_url, json=add_data)
                response.raise_for_status()
                st.success('Document added successfully.')
                st.rerun()
            except requests.exceptions.RequestException as e:
                st.error(f'Error adding document: {e}')


def update_document(collection, document_id, form_data):
    try:
        update_url = f'{API_URL}/update'
        update_data = {
            'collection': collection,
            'id': document_id,
            'update_fields': form_data
        }
        response = requests.post(update_url, json=update_data)
        response.raise_for_status()
        st.success('Document updated successfully.')
        # st.rerun()
    except requests.exceptions.RequestException as e:
        st.error(f"Error updating document: {e}")


def delete_document(collection, document_id):
    try:
        delete_url = f'{API_URL}/delete'
        delete_data = {
            'collection': collection,
            'id': document_id
        }
        response = requests.post(delete_url, json=delete_data)
        response.raise_for_status()
        st.success('Document deleted successfully.')
        # st.rerun()
    except requests.exceptions.RequestException as e:
        st.error(f'Error deleting document: {e}')


def create_bon_zilnic(data):
    url = f'{API_URL}/create_bon_zilnic'
    try:
        schema = fetch_schema('ECR.bon_zilnic')
        for field in schema:
            if field in data:
                data[field] = str(data[field])  # Convert to string if necessary
            else:
                data[field] = ''  # Use empty string if the field is missing (adjust as needed)

        response = requests.post(url, json=data)
        response.raise_for_status()

        st.success('Document created successfully.')
    except requests.exceptions.RequestException as e:
        st.error(f"Error creating bon zilnic: {e}")


def update_bon_zilnic(data):
    url = f'{API_URL}/update_bon_zilnic'
    try:
        # Fetch the schema to know which fields exist
        schema = fetch_schema('ECR.bon_zilnic')

        # Ensure each field from the schema is present in the data
        for field in schema:
            if field in data:
                data[field] = str(data[field])  # Convert to string if necessary
            else:
                data[field] = ''  # Use empty string if the field is missing (adjust as needed)

        # Send the update request to the backend
        response = requests.put(url, json=data)
        response.raise_for_status()  # Raise an error if the request failed

        st.success('Document updated successfully.')

    except requests.exceptions.RequestException as e:
        st.error(f"Error updating bon zilnic: {e}")
