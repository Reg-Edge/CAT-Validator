import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import trailBuilderV2

st.set_page_config(layout="wide")

def style_nav_bar():
    st.markdown("""
        <style>
        div.stButton > button:first-child {
            border: 1px solid rgba(0, 0, 0, 0.1);
            border-radius: 20px;
            color: rgba(0, 0, 0, 0.6) !important;
            font-size: 16px;
            font-weight: bold;
            background-color: transparent !important;
        }
        div.stButton > button:hover {
            background-color: #f0f2f6 !important;
        }
        </style>
        """, unsafe_allow_html=True)
    st.sidebar.markdown("## CAT VALIDATOR")
style_nav_bar()

asset_classes = {
    'Equities': ['MECO','MECOM','MEMR','MEOC'],
    'Options': ['MEOC', 'MEOM', 'MEOR'],
    'Multilegs' : ['MEMRS','MENO'],
    'RFQs' : ['MEORS']

}

event_types = {
    'Orders': ['MECO','MECOM','MENO', 'MEOM'],
    'Routes': ['MEMR','MEMRS','MEOR'],
    'Trades' : ['MEOC','MEOC'],
    'Allocations' : ['MEORS']
}

if 'page' not in st.session_state:
    st.session_state.page = 'VIEW_CAT_DATA'

# Function to load data
@st.cache
def load_data():
    data = pd.read_csv('consolidated_data.csv')
    # Convert 'eventTimestamp' to datetime format
    data['eventTimestamp'] = pd.to_datetime(data['eventTimestamp'])
    return data

data = load_data()

# Function to set the page
def set_page(page_name):
    st.session_state.page = page_name

# Sidebar navigation using buttons
st.sidebar.button('VIEW_CAT_DATA', on_click=set_page, args=('VIEW_CAT_DATA',))
st.sidebar.button('CAT_SUMMARY', on_click=set_page, args=('CAT_SUMMARY_',))
st.sidebar.button('AUDIT TRAIL / DETAILED VIEW', on_click=set_page, args=('AUDIT TRAIL / DETAILED VIEW',))

# Page 1: VIEW_CAT_DATA
if st.session_state.page == 'VIEW_CAT_DATA':
    st.write(f"CAT SUBMISSIONS")

    col1, col2, col3,col4 = st.columns(4)

    with col1:
        unique_symbols = data['symbol'].dropna().unique()
        selected_symbol = st.selectbox('Symbol', options=['All'] + list(unique_symbols))

    with col2:
        # Determine a default date from your data
        # Example: using the latest date in your dataset
        default_date = data['eventTimestamp'].max().date()
        selected_date = st.date_input("Event Date", value=default_date, key='event_date')

    with col3:
        asset_classes_options = list(asset_classes.keys())
        selected_asset_class = st.selectbox('Asset Class', options=['All'] + list(asset_classes_options))

    with col4:
        event_categories = list(event_types.keys())
        selected_event_category = st.selectbox('Event Type', options=['All'] + list(event_categories))

    # Filter data based on selections

    if selected_symbol != 'All':
        data = data[data['symbol'] == selected_symbol]

    # # Filter by the selected date
    if selected_date is not None:
        data = data[data['eventTimestamp'].dt.date == selected_date]

    if selected_asset_class != 'All':
        data = data[data['type'].isin(asset_classes[selected_asset_class])]

    if selected_event_category != 'All':
        data = data[data['type'].isin(event_types[selected_event_category])]

    
    st.dataframe(data)
    st.write(f"Displaying {len(data)} rows of data")

if st.session_state.page == 'CAT_SUMMARY_':
    #code for the summary
    st.write(f"CAT SUBMISSIONS SUMMARY")
    pivot_table = pd.pivot_table(data, index='type', aggfunc='size').reset_index(name='Count')
    col1, col2, col3, col4 = st.columns(4)

    # Display a DataFrame in each column of the first row
    with col1:
        st.write(pivot_table, height=100)  # Adjust height as necessary

    with col2:
    # Using the pivot_table data to plot a bar chart
        fig, ax = plt.subplots()
        ax.bar(pivot_table['type'], pivot_table['Count'])
        ax.set_xlabel('Event Type')
        ax.set_ylabel('Count')
        ax.set_title('Event Type Counts')
        st.pyplot(fig)  # Adjust height as necessary

    with col3:
        st.write(pivot_table, height=100)  # Adjust height as necessary

    with col4:
    # Using the pivot_table data to plot a bar chart
        fig, ax = plt.subplots()
        ax.bar(pivot_table['type'], pivot_table['Count'])
        ax.set_xlabel('Event Type')
        ax.set_ylabel('Count')
        ax.set_title('Event Type Counts')
        st.pyplot(fig)  # Adjust height as necessary

    
    col5, col6, col7, col8 = st.columns(4)

    # Display a DataFrame in each column of the first row
    with col5:
        st.write(pivot_table, height=100)  # Adjust height as necessary

    with col6:
    # Using the pivot_table data to plot a bar chart
        fig, ax = plt.subplots()
        ax.bar(pivot_table['type'], pivot_table['Count'])
        ax.set_xlabel('type')
        ax.set_ylabel('Count')
        ax.set_title('Event Type Counts')
        st.pyplot(fig)  # Adjust height as necessary

    with col7:
        st.write(pivot_table, height=100)  # Adjust height as necessary

    with col8:
    # Using the pivot_table data to plot a bar chart
        fig, ax = plt.subplots()
        ax.bar(pivot_table['type'], pivot_table['Count'])
        ax.set_xlabel('Event Type')
        ax.set_ylabel('Count')
        ax.set_title('Event Type Counts')
        st.pyplot(fig)  # Adjust height as necessary


    
if st.session_state.page == 'AUDIT TRAIL / DETAILED VIEW':
    #code for the summary
    st.write(f"DETAILED VIEW")
    

    
    unique_order_ids = data['orderID'].dropna().unique()
    selected_order_id = st.selectbox('orderID', options=list(unique_order_ids))

    # if selected_order_id != 'All':
    #     data = data[data['orderID'] == selected_order_id]

    parentOrderID = trailBuilderV2.main(selected_order_id)
    st.write(parentOrderID)
    st.dataframe(data)
    st.write(f"Displaying {len(data)} rows of data")
