from datetime import datetime

import requests.exceptions
import streamlit as st
import pandas as pd
import altair as alt
from frontend.utils.validation_functions import parse_collection
from frontend.utils.fetch_data import fetch_collection_counts, fetch_nr_z_reports, fetch_tva_stats, fetch_sums_by_hour, \
    fetch_sums_by_day_of_week, fetch_filtered_bon_zilnic, fetch_daily_transactions, delete_collection, \
    convert_data_to_timestamp, aggregate_data, fetch_all_produs, fetch_bon_by_id, fetch_bon_zilnic, fetch_schema
from frontend.utils.crud import create_bon_zilnic, update_bon_zilnic


def display_summary_statistics():
    # Display summary statistics
    collection_counts = fetch_collection_counts()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Case marcat înrolate", str(collection_counts.get('ECR.nui', '0')))
    col2.metric("Total încasari comercianți", str(collection_counts.get('ECR.firma', '0')))
    col3.metric("Total rapoarte zilnice emise", str(collection_counts.get('ECR.bon_zilnic', '0')))
    col4.metric("Total număr bonuri fiscale emise", str(collection_counts.get('ECR.bon', '0')))


def display_counts(counts_data):
    if counts_data:
        collection_count_data = counts_data.get('collection_count', [])

        if collection_count_data and isinstance(collection_count_data, list) and isinstance(collection_count_data[0], dict):
            collection_df = pd.DataFrame(collection_count_data)
            collection_df.columns = ['DATA', 'Count']
        else:
            collection_df = pd.DataFrame(columns=['DATA', 'Count'])

        collection_df['DATA'] = pd.to_datetime(collection_df['DATA'], errors='coerce', dayfirst=True)
        collection_df['Count'] = pd.to_numeric(collection_df['Count'], errors='coerce')

        collection_df.dropna(subset=['DATA'], inplace=True)

        total_counts = collection_df['Count'].sum()

        st.write("Total Counts by Collection:", total_counts)

        st.write('Summary Statistics:')
        st.write(collection_df)

        json_data = collection_df.to_json(orient='records', date_format='iso')
        st.download_button(
            label='Download data as JSON',
            data=json_data,
            file_name=f'{st.session_state.collection}_data_{datetime.now()}.json',
            mime='application/json'
        )

        st.write(collection_df.describe())

        chart = alt.Chart(collection_df).mark_bar().encode(
            x='DATA:T',
            y='Count:Q',
            tooltip=['DATA:T', 'Count:Q']
        ).properties(
            title='Document Counts by Collection'
        ).interactive()

        st.altair_chart(chart, use_container_width=True)

        st.write('Additional Charts:')

        line_chart = alt.Chart(collection_df).mark_line().encode(
            x='DATA:T',
            y='Count:Q',
            tooltip=['DATA:T', 'Count:Q']
        ).properties(
            title='Document Counts Over Time'
        ).interactive()

        st.altair_chart(line_chart, use_container_width=True)

        histogram = alt.Chart(collection_df).mark_bar().encode(
            x=alt.X('Count:Q', bin=True),
            y='count()',
            tooltip=['Count:Q', 'count()']
        ).properties(
            title='Counts Distribution'
        ).interactive()

        st.altair_chart(histogram, use_container_width=True)

        box_plot = alt.Chart(collection_df).mark_boxplot().encode(
            y='Count:Q',
            tooltip=['Count:Q']
        ).properties(
            title='Counts Box Plot'
        ).interactive()

        st.altair_chart(box_plot, use_container_width=True)


def display_tax_reports():
    if st.session_state.collection in ['ECR.bon', 'ECR.bon_zilnic', 'ECR.bon.parsed', 'ECR.bon_zilnic.parsed']:
        st.header("Analyze Daily Closing Tax Reports (nr.Z)")

        date_from_nr_z = st.date_input('From (nr.Z)', value=pd.to_datetime('2021-01-01'))
        date_to_nr_z = st.date_input('To (nr.Z)', value=pd.to_datetime('2021-12-31'))
        nr_z = st.text_input('Nr.Z')

        if st.button('Fetch nr.Z Reports'):
            try:
                nr_z_data = fetch_nr_z_reports(date_from=date_from_nr_z, date_to=date_to_nr_z, nr_z=nr_z)

                if nr_z_data:
                    nr_z_df = pd.DataFrame(nr_z_data.get('nr_z_data', []))
                    st.write("nr.Z DataFrame:", nr_z_df)

                    if 'DATA' in nr_z_df.columns and 'nr_z' in nr_z_df.columns:
                        nr_z_df['DATA'] = pd.to_datetime(nr_z_df['DATA'], format='%d-%m-%Y', errors='coerce')
                        nr_z_df['nr_z'] = pd.to_numeric(nr_z_df['nr_z'], errors='coerce')
                        nr_z_df.dropna(subset=['DATA'], inplace=True)

                        nr_z_aggregated = nr_z_df.groupby(['DATA', 'nr_z']).size().reset_index(name='count')

                        nr_z_chart = alt.Chart(nr_z_aggregated).mark_bar().encode(
                            x=alt.X('DATA:T', axis=alt.Axis(title='Date')),
                            y=alt.Y('count:Q', axis=alt.Axis(title='nr.Z Count')),
                            color='nr_z:N',
                            tooltip=['DATA:T', 'count:Q', 'nr_z:N']
                        ).properties(
                            title='Daily Closing Tax Reports (nr.Z) Over Time'
                        ).interactive()

                        st.altair_chart(nr_z_chart, use_container_width=True)
                    else:
                        st.error("The 'DATA' field is not available in the retrieved data.")

            except requests.exceptions.RequestException as e:
                st.error(f"Error fetching nr.Z reports: {e}")


def display_tva_statistics(collection, date_from, date_to):
    if collection in ['ECR.bon', 'ECR.bon_zilnic']:

        st.subheader('TVA Statistics')

        tva_stats = fetch_tva_stats(collection, date_from, date_to)
        if tva_stats:
            tva_df = pd.DataFrame([tva_stats])
            st.write('Total TVA Statistics.')
            st.write(tva_df)

            st.write('Summary Statistics.')
            st.write(tva_df.describe())

            tva_totals = {
                'Total TVA (0%)': tva_stats.get('total_totA', 0),
                'Total TVA (5%)': tva_stats.get('total_totB', 0),
                'Total TVA (9%)': tva_stats.get('total_totC', 0),
                'Total TVA (19%)': tva_stats.get('total_totD', 0)
            }

            tva_totals_df = pd.DataFrame(list(tva_totals.items()), columns=['TVA Type', 'Total'])
            bar_chart = alt.Chart(tva_totals_df).mark_bar().encode(
                x=alt.X('TVA Type', sort=None),
                y='Total',
                color='TVA Type'
            ).properties(
                title='TVA Totals by Percentage'
            )
            st.altair_chart(bar_chart, use_container_width=True)

            st.write('Additional Charts:')

            pie_chart = alt.Chart(tva_totals_df).mark_arc().encode(
                theta=alt.Theta(field="Total", type="quantitative"),
                color=alt.Color(field="TVA Type", type="nominal"),
                tooltip=['TVA Type', 'Total']
            ).properties(
                title='TVA Distribution'
            )
            st.altair_chart(pie_chart, use_container_width=True)

            if 'timestamp' in tva_stats:
                tva_df['timestamp'] = pd.to_datetime(tva_df['timestamp'], unit='ms')
                line_chart = alt.Chart(tva_df).mark_line().encode(
                    x='timestamp:T',
                    y='value:Q',
                    color='variable:N'
                ).transform_fold(
                    ['total_totA', 'total_totB', 'total_totC', 'total_totD']
                ).properties(
                    title='TVA Over Time'
                )

                st.altair_chart(line_chart, use_container_width=True)
        else:
            st.write('No data available for the selected date range.')
    else:
        st.write("This collection doesn't have TVA.")


def display_sums_by_hour_page():
    if st.session_state.collection in ['ECR.bon']:
        st.title("Sums of Transactions Grouped by Hour")

        if st.button('Fetch Hourly Transactions'):
            sums_by_hour_data = fetch_sums_by_hour(st.session_state.collection, st.session_state.date_from,
                                                   st.session_state.date_to)

            if sums_by_hour_data:
                df = pd.DataFrame(sums_by_hour_data)
                df['_id'] = df['_id'].apply(lambda x: f"{x['date']} {x['hour']}:00")
                df = df.rename(columns={"_id": "Hour", "total_sum": "Total Sum"})
                st.write(df)

                chart = alt.Chart(df).mark_line().encode(
                    x='Hour:T',
                    y='Total Sum:Q',
                    tooltip=['Hour:T', 'Total Sum:Q']
                ).properties(
                    title='Hourly Transactions Over Time'
                ).interactive()

                st.altair_chart(chart, use_container_width=True)

                # Bar chart
                bar_chart = alt.Chart(df).mark_bar().encode(
                    x='Hour:T',
                    y='Total Sum:Q',
                    tooltip=['Hour:T', 'Total Sum:Q']
                ).properties(
                    title='Hourly Transactions (Bar Chart)'
                ).interactive()

                st.altair_chart(bar_chart, use_container_width=True)

                # Box plot
                box_plot = alt.Chart(df).mark_boxplot().encode(
                    x=alt.X('hour(Hour):O', title='Hour of Day'),
                    y='Total Sum:Q',
                    tooltip=['Hour:T', 'Total Sum:Q']
                ).properties(
                    title='Distribution of Transactions by Hour'
                ).interactive()

                st.altair_chart(box_plot, use_container_width=True)

                # Heatmap
                heatmap = alt.Chart(df).mark_rect().encode(
                    x=alt.X('hour(Hour):O', title='Hour of Day'),
                    y=alt.Y('date(Hour):O', title='Date'),
                    color='Total Sum:Q',
                    tooltip=['Hour:T', 'Total Sum:Q']
                ).properties(
                    title='Heatmap of Transactions by Hour and Date'
                ).interactive()

                st.altair_chart(heatmap, use_container_width=True)

    else:
        st.write("This collection doesn't have Total Value.")


def display_sums_by_day_of_week_page():
    if st.session_state.collection in ['ECR.bon']:
        st.title("Sums of Transactions Grouped by Day of the Week")

        if st.button('Fetch Daily Transactions'):
            sums_by_day_of_week_data = fetch_sums_by_day_of_week(st.session_state.collection, st.session_state.date_from,
                                                                 st.session_state.date_to)

            if sums_by_day_of_week_data:
                df = pd.DataFrame(sums_by_day_of_week_data)
                df = df.rename(columns={"_id": "Day of Week", "total_sum": "Total Sum"})
                st.write(df)

                # Line chart
                line_chart = alt.Chart(df).mark_line().encode(
                    x='Day of Week:O',
                    y='Total Sum:Q',
                    tooltip=['Day of Week:O', 'Total Sum:Q']
                ).properties(
                    title='Daily Transactions Over Time'
                ).interactive()

                st.altair_chart(line_chart, use_container_width=True)

                # Bar chart
                bar_chart = alt.Chart(df).mark_bar().encode(
                    x='Day of Week:O',
                    y='Total Sum:Q',
                    tooltip=['Day of Week:O', 'Total Sum:Q']
                ).properties(
                    title='Daily Transactions (Bar Chart)'
                ).interactive()

                st.altair_chart(bar_chart, use_container_width=True)

                # Box plot
                box_plot = alt.Chart(df).mark_boxplot().encode(
                    x='Day of Week:O',
                    y='Total Sum:Q',
                    tooltip=['Day of Week:O', 'Total Sum:Q']
                ).properties(
                    title='Distribution of Transactions by Day of Week'
                ).interactive()

                st.altair_chart(box_plot, use_container_width=True)

    else:
        st.write("This collection doesn't have Total Value.")


def display_filtered_bon_zilnic_page():
    if st.session_state.collection == 'ECR.bon_zilnic':
        st.title("Filtered Bon Zilnic by Nr")

        nr = st.text_input('Nr (last 4 digits of ID)')
        if st.button('Fetch Filtered Data'):
            if not nr:
                st.error('Nr is required')
            else:
                filtered_data = fetch_filtered_bon_zilnic(st.session_state.collection, st.session_state.date_from,
                                                          st.session_state.date_to, nr)

                if filtered_data:
                    df = pd.DataFrame(filtered_data)
                    st.write("Data:", df)

                    if not df.empty:
                        df['DATA'] = pd.to_datetime(df['DATA'], format='%d-%m-%Y', errors='coerce')
                        df['total_vanzari'] = pd.to_numeric(df['total_vanzari'], errors='coerce')
                        df['numerar'] = pd.to_numeric(df['numerar'], errors='coerce')
                        df['card'] = pd.to_numeric(df['card'], errors='coerce')
                        df['hour'] = pd.to_datetime(df['ORA'], format='%H:%M:%S').dt.hour

                        st.write("Statistical Summary:")
                        st.write(df.describe())

                        # Additional charts and analysis
                        line_chart = alt.Chart(df).mark_line().encode(
                            x='DATA:T',
                            y='total_vanzari:Q',
                            tooltip=['DATA:T', 'total_vanzari:Q']
                        ).properties(
                            title='Total Vanzari Over Time'
                        ).interactive()

                        st.altair_chart(line_chart, use_container_width=True)

                        bar_chart = alt.Chart(df).mark_bar().encode(
                            x='DATA:T',
                            y='total_vanzari:Q',
                            tooltip=['DATA:T', 'total_vanzari:Q']
                        ).properties(
                            title='Total Vanzari (Bar Chart)'
                        ).interactive()

                        st.altair_chart(bar_chart, use_container_width=True)

                        box_plot = alt.Chart(df).mark_boxplot().encode(
                            x='DATA:T',
                            y='total_vanzari:Q',
                            tooltip=['DATA:T', 'total_vanzari:Q']
                        ).properties(
                            title='Distribution of Vanzari'
                        ).interactive()

                        st.altair_chart(box_plot, use_container_width=True)

                        # Hourly total transactions
                        hourly_df = df.groupby('hour').agg(total_sum=('total_vanzari', 'sum')).reset_index()
                        hourly_chart = alt.Chart(hourly_df).mark_bar().encode(
                            x='hour:O',
                            y='total_sum:Q',
                            tooltip=['hour:O', 'total_sum:Q']
                        ).properties(
                            title='Total Vanzari by Hour'
                        ).interactive()

                        st.altair_chart(hourly_chart, use_container_width=True)

                        # Pie chart for types of payments
                        payment_totals = df[['numerar', 'card']].sum().reset_index()
                        payment_totals.columns = ['Payment Type', 'Total']
                        payment_totals['Total'] = pd.to_numeric(payment_totals['Total'], errors='coerce')

                        pie_chart = alt.Chart(payment_totals).mark_arc().encode(
                            theta=alt.Theta(field="Total", type="quantitative"),
                            color=alt.Color(field="Payment Type", type="nominal"),
                            tooltip=['Payment Type', 'Total']
                        ).properties(
                            title='Total Vanzari by Payment Type'
                        )

                        st.altair_chart(pie_chart, use_container_width=True)

                        # Display numeric statistics for payment types
                        st.write("Total Payments by Type:")
                        st.write(payment_totals)

    else:
        st.write("This page is only for ECR.bon_zilnic collection.")


def display_daily_transactions_page():
    st.title("Daily Number of Transactions")

    if st.session_state.collection in ['ECR.bon_zilnic']:
        if st.button('Fetch Daily Transactions'):
            daily_transactions_data = fetch_daily_transactions(
                st.session_state.collection,
                st.session_state.date_from,
                st.session_state.date_to
            )

            if daily_transactions_data:
                df = pd.DataFrame(daily_transactions_data)
                df['Date'] = pd.to_datetime(df['date'], dayfirst=True).dt.strftime('%Y-%m-%d')
                df = df.rename(columns={"nr_bonuri": "Number of Transactions"})
                df = df.drop(columns=['date'])
                df = df.drop(columns=['count'])
                st.write(df)

                # Line Chart
                line_chart = alt.Chart(df).mark_line().encode(
                    x='Date:T',
                    y='Number of Transactions:Q',
                    tooltip=['Date:T', 'Number of Transactions:Q']
                ).properties(
                    title='Frequency of Daily Transactions'
                ).interactive()

                st.altair_chart(line_chart, use_container_width=True)

                # Bar Chart
                bar_chart = alt.Chart(df).mark_bar().encode(
                    x='Date:T',
                    y='Number of Transactions:Q',
                    tooltip=['Date:T', 'Number of Transactions:Q']
                ).properties(
                    title='Frequency of Daily Transactions'
                ).interactive()

                st.altair_chart(bar_chart, use_container_width=True)

                # Box Plot
                box_plot = alt.Chart(df).mark_boxplot().encode(
                    x='Date:T',
                    y='Number of Transactions:Q',
                    tooltip=['Date:T', 'Number of Transactions:Q']
                ).properties(
                    title='Box Plot of Daily Transactions'
                ).interactive()

                st.altair_chart(box_plot, use_container_width=True)
    else:
        st.write("This collection doesn't have transaction data.")


def display_validation_page():
    st.title("Validation Data Process")

    collections = {
        'ECR.bon': 'parsing_id_bon',
        'ECR.bon_zilnic': 'parsing_id_bon_zilnic',
        'ECR.produs': 'parsing_id_produs'
    }

    st.subheader(f"Convert Data to Timestamp for {st.session_state.collection}")
    if st.button(f"Convert Data to Timestamp for: {st.session_state.collection}"):
        result = convert_data_to_timestamp(st.session_state.collection)
        st.write(result)

    if st.button('Aggregate data'):
        result = aggregate_data()
        st.write(result)

    if st.session_state.collection in collections:
        st.subheader(f"Parse IDs for {st.session_state.collection}")
        if st.button(f"Parse {st.session_state.collection} IDs"):
            result = parse_collection(collections[st.session_state.collection])
            st.write(result)

    else:
        st.write("Please select a valid collection for validation.")

    if st.session_state.collection in ['ECR.bon.parsed', 'ECR.bon_zilnic.parsed', 'ECR.produs.parsed']:
        st.subheader(f"Delete {st.session_state.collection} Collection")
        if st.button(f"Delete {st.session_state.collection}"):
            response = delete_collection(st.session_state.collection)
            st.write(response)
    else:
        st.write("Please select a valid collection for deleting. (We can delete only Parsed collections.)")


def display_produs_management_page():
    st.title("Produs Management Interface")

    if 'all_produs' not in st.session_state:
        st.session_state.all_produs = fetch_all_produs()
        st.session_state.prod_dict = {prod["_id"]: prod for prod in st.session_state.all_produs}
        st.session_state.prod_ids = list(st.session_state.prod_dict.keys())
        st.session_state.prod_index = 0

    if not st.session_state.all_produs:
        st.write("No produs found.")
        return

    selected_prod_id = st.selectbox("Select Produs ID", st.session_state.prod_ids, index=st.session_state.prod_index)
    selected_prod = st.session_state.prod_dict[selected_prod_id]

    st.write("Produs Details:", selected_prod)

    if st.button("Next"):
        st.session_state.prod_index = (st.session_state.prod_index + 1) % len(st.session_state.prod_ids)
        st.rerun()

    if st.button("Find Bon Zilnic"):
        bon_id = selected_prod["bon_id"]
        bon = fetch_bon_by_id(bon_id)
        if bon:
            nr_z = bon["Z"]
            DATA = bon["DATA"]
            total = bon["total"]
            totA = bon["totA"]
            totB = bon["totB"]
            totC = bon["totC"]
            totD = bon["totD"]
            bon_zilnic = fetch_bon_zilnic(nr_z, DATA, total, totA, totB, totC, totD)

            st.write("Fetched Bon Zilnic:", bon_zilnic)

            if bon_zilnic:
                st.write("Bon Zilnic Details:", bon_zilnic)
                with st.expander("Edit Bon Zilnic"):
                    form_data_updating = {}
                    for field, value in bon_zilnic.items():
                        if field != "_id":
                            form_data_updating[field] = st.text_input(field, value=str(value))

                    form_data_updating['_id'] = bon_zilnic['_id']
                    print("updated form_data", form_data_updating)
                    st.button('Create Bon Zilnic', on_click=update_bon_zilnic, args=[form_data_updating])

            else:
                st.write("No matching Bon Zilnic found.")
                schema = fetch_schema('ECR.bon_zilnic')

                with st.expander("Create Bon Zilnic"):
                    form_data = {field: "" for field in schema}
                    form_data.update({
                        "nr": nr_z,
                        "DATA": DATA,
                        "total_vanzari": total,
                        "total_a": totA,
                        "total_b": totB,
                        "total_c": totC,
                        "total_d": totD
                    })
                    for field, value in form_data.items():
                        form_data[field] = st.text_input(field, value=str(value))

                    print(form_data, 'form_data')
                    st.button('Create Bon Zilnic', on_click=create_bon_zilnic, args=[form_data])
                # with st.expander("Create Bon Zilnic"):
                # with st.form(key="bon_zilnic_form"):
                #     form_data = {field: "" for field in schema}
                #     form_data.update({
                #         "nr": nr_z,
                #         "DATA": DATA,
                #         "total_vanzari": total,
                #         "total_a": totA,
                #         "total_b": totB,
                #         "total_c": totC,
                #         "total_d": totD
                #     })
                #
                #     for field in form_data.keys():
                #         form_data[field] = st.text_input(field, value=form_data[field])
                #
                #     print(form_data, 'form_data')
                #
                #     st.form_submit_button(label="Create Bon Zilnic", on_click=create_bon_zilnic, args=(form_data, ))

                    # if submit_button:
                    #     print("Creating Bon Zilnic with data:", form_data)
                    #     create_bon_zilnic(form_data)
                    #     st.success("Bon Zilnic created successfully!")
                    #     st.rerun()
