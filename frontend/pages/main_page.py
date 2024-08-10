import datetime
import streamlit as st
import pandas as pd
import altair as alt
from frontend.utils.fetch_data import fetch_schema, fetch_data
from frontend.utils.crud import add_document, update_document, delete_document
from frontend.utils.utils_json import upload_json_file
from frontend.utils.display_functions import display_summary_statistics


# Main Streamlit app
def main_page():
    display_summary_statistics()
    collection_y_axis_mapping = {
        'ECR.bon': 'total',
        'ECR.bon_zilnic': 'total_vanzari'
    }

    fields = fetch_schema(st.session_state.collection)

    # Initialize session state
    if "page_number" not in st.session_state:
        st.session_state.page_number = 0
    if "items_per_page" not in st.session_state:
        st.session_state.items_per_page = 100

    # Page size selection
    st.session_state.items_per_page = st.selectbox("Page Size", options=[100, 1000, 10000, 20000], index=0)

    data = pd.DataFrame(st.session_state.data)

    if 'DATA' not in data.columns:
        data = fetch_data(collection=st.session_state.collection,
                          date_from=st.session_state.date_from,
                          date_to=st.session_state.date_to,
                          page_number=st.session_state.page_number,
                          items_per_page=st.session_state.items_per_page)

    if not data.empty:
        st.dataframe(data)

        # Pagination controls
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Previous") and st.session_state.page_number > 0:
                st.session_state.page_number -= 1
                st.rerun()
        with col2:
            st.write(f"Page {st.session_state.page_number + 1}")
        with col3:
            if st.button("Next"):
                st.session_state.page_number += 1
                st.rerun()

        # Button to download data as JSON
        json_data = data.to_json(orient='records', date_format='iso')
        st.download_button(
            label='Download data as JSON',
            data=json_data,
            file_name=f'{st.session_state.collection}_data_{datetime.datetime.now()}.json',
            mime='application/json'
        )

        # select document to edit
        selected_index = st.selectbox('Select document to edit', data.index)
        selected_document = data.loc[selected_index]

        document_id = selected_document["_id"]
        if isinstance(document_id, dict) and "$oid" in document_id:
            document_id = document_id["$oid"]

        with st.expander(f'Edit Document ID: {document_id}'):
            # Initialize form data in session state if not already done
            if f'form_data_{document_id}' not in st.session_state:
                st.session_state[f'form_data_{document_id}'] = {
                    field: str(value) for field, value in selected_document.items() if field != "_id"
                }

            # Use st.form for controlled submission
            with st.form(key=f'edit_form_{document_id}'):
                # Display form fields
                for field in st.session_state[f'form_data_{document_id}'].keys():
                    st.session_state[f'form_data_{document_id}'][field] = st.text_input(field, value=
                    st.session_state[f'form_data_{document_id}'][field])

                # Update MongoDB Document
                update_submitted = st.form_submit_button('Update Document')
                if update_submitted:
                    update_document(st.session_state.collection, document_id,
                                    st.session_state[f'form_data_{document_id}'])
                    st.success("Document updated successfully!")
                    st.rerun()

            # Delete MongoDB document with confirmation
            if st.button('Delete Document', key=f'delete_button_{document_id}'):
                delete_document(collection=st.session_state.collection, document_id=document_id)
                st.success("Document deleted successfully!")
                st.rerun()

        # Ensure the 'DATA' field is of datetime type for the chart
        y_axis_field = collection_y_axis_mapping.get(st.session_state.collection, 'total')

        if 'DATA' in data.columns and y_axis_field in data.columns:
            data['DATA'] = pd.to_datetime(data['DATA'], format='%d-%m-%Y', dayfirst=True)

            chart = alt.Chart(data).mark_line().encode(
                x='DATA:T',
                y=alt.Y(y_axis_field, title='Total'),
                tooltip=['DATA:T', y_axis_field]
            ).properties(
                title=f'Values from {st.session_state.collection}',
                width=800,
                height=400
            ).interactive()

            st.altair_chart(chart, use_container_width=True)
        else:
            st.write("The collection does not contain a 'DATA' or 'total' field for date-based")

    else:
        st.write('No data available for the selected date range.')

    # Add new document
    with st.expander(f'Add Document'):
        add_document(collection=st.session_state.collection, fields=fields)

    # File uploader to upload JSON data and add to collection
    upload_json_file(collection=st.session_state.collection)

