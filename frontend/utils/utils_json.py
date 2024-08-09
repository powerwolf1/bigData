import requests
import json
import streamlit as st
from frontend.utils.config import API_URL
from frontend.utils.fetch_data import fetch_schema


def upload_json_file(collection):
    st.subheader('Upload JSON File to Add Data')

    uploader_file = st.file_uploader("Choose a JSON file", type="json")

    if uploader_file is not None:
        if st.button('Upload and Add Data'):
            try:
                new_documents = json.load(uploader_file)

                if isinstance(new_documents, dict):
                    new_documents = [new_documents]

                if isinstance(new_documents, list):
                    collection_schema = fetch_schema(collection)

                    # Validate fields
                    if collection_schema:
                        invalid_docs = []
                        schema_fields = set(collection_schema)

                        for doc in new_documents:
                            doc_fields = set(doc.keys())
                            if doc_fields != schema_fields:
                                invalid_docs.append(doc)

                        if invalid_docs:
                            st.error(
                                f"The following documents have fields that do not match the collection schema: {invalid_docs}")
                        else:

                            add_bulk_url = f"{API_URL}/add_bulk"
                            add_bulk_data = {
                                'collection': collection,
                                'new_documents': new_documents
                            }

                            response = requests.post(add_bulk_url, json=add_bulk_data)
                            response.raise_for_status()
                            st.success('Documents added successfully.')
                            st.rerun()
                    else:
                        st.error('Could not fetch collection schema for validation.')
                else:
                    st.error('The uploaded JSON file is not in the correct format.')
            except Exception as e:
                st.error(f'Error processing file: {e}')
