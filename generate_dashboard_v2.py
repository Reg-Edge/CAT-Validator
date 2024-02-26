import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import trailBuilderV2
class StreamlitApp:
    def __init__(self):
        self.setup_page()
        self.style_nav_bar()
        self.page_router()

    def setup_page(self):
        st.set_page_config(page_title="CAT Validator App", layout="wide")
        st.title("CAT Validator")

    def style_nav_bar(self):
        # Custom CSS to remove button borders and style the navigation bar
        st.markdown("""
            <style>
            div.stButton > button {
                border: none;
                color: #4CAF50;
                padding: 10px 24px;
                text-align: center;
                text-decoration: none;
                display: inline-block;
                font-size: 16px;
                margin: 4px 2px;
                transition-duration: 0.4s;
                cursor: pointer;
                background-color: white;
                color: black;
                border: 2px solid #4CAF50;
            }

            div.stButton > button:hover {
                background-color: #4CAF50;
                color: white;
            }
            </style>
            """, unsafe_allow_html=True)
        st.markdown("""
            <style>
                .dataframe-container, .dataframe-output {
                    width: 100% !important;
                }
            </style>
            """, unsafe_allow_html=True)
        

    def page_router(self):
        # Define page options
        self.page_options = ["CAT SUBMISSIONS DATA", "EXCEPTIONS", "SUMMARY DASHBOARD", "AUDIT TRAIL / DETAILED VIEW"]
        self.current_page = "CAT SUBMISSIONS DATA" if 'current_page' not in st.session_state else st.session_state.current_page

        # Display navigation bar
        cols = st.columns(len(self.page_options))
        for i, option in enumerate(self.page_options):
            with cols[i]:
                if st.button(option, key=option):
                    self.current_page = option
                    st.session_state.current_page = option

        # Page routing
        if self.current_page == "CAT SUBMISSIONS DATA":
            self.display_cat_submissions()
        elif self.current_page == "EXCEPTIONS":
            self.display_exceptions()
        elif self.current_page == "SUMMARY DASHBOARD":
            self.display_summary_dashboard()
        elif self.current_page == "AUDIT TRAIL / DETAILED VIEW":
            self.display_audit_trail()

    def display_cat_submissions(self):
        st.subheader("CAT Submissions Data")
        
        # Load the data
        data = self.load_data()
        
        # Asset classes and event types for filtering (example placeholders)
        asset_classes = {'Equities': ['MECO', 'MECOM', 'MEMR', 'MEOC'], 'Options': ['MEOC', 'MEOM', 'MEOR']}
        event_types = {'Orders': ['MECO', 'MECOM', 'MENO', 'MEOM'], 'Routes': ['MEMR', 'MEMRS', 'MEOR']}
        
        # Filter UI
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            unique_symbols = data['symbol'].dropna().unique()
            selected_symbol = st.selectbox('Symbol', options=['All'] + list(unique_symbols))
        
        with col2:
            default_date = data['eventTimestamp'].max().date()
            selected_date = st.date_input("Event Date", value=default_date, key='event_date')
        
        with col3:
            asset_classes_options = list(asset_classes.keys())
            selected_asset_class = st.selectbox('Asset Class', options=['All'] + asset_classes_options)
        
        with col4:
            event_categories = list(event_types.keys())
            selected_event_category = st.selectbox('Event Type', options=['All'] + event_categories)
        
        # Apply filters to the data
        if selected_symbol != 'All':
            data = data[data['symbol'] == selected_symbol]
        
        if selected_date is not None:
            data = data[data['eventTimestamp'].dt.date == selected_date]
        
        if selected_asset_class != 'All':
            data = data[data['type'].isin(asset_classes[selected_asset_class])]
        
        if selected_event_category != 'All':
            data = data[data['type'].isin(event_types[selected_event_category])]
        
        # Display filtered DataFrame
        st.dataframe(data)
        st.write(f"Displaying {len(data)} rows of data")

    def load_error_data(self):
        # Load the error log data
        error_data = pd.read_csv('error_log.csv')
        return error_data

    def display_exceptions(self):
        st.subheader("Exceptions")
        
        # Load the error data
        error_data = self.load_error_data()
        
        # Filter UI setup with Streamlit columns
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            unique_exception_types = error_data['Exception Type'].dropna().unique()
            selected_exception_type = st.selectbox('Exception Type', options=['All'] + list(unique_exception_types))
        
        with col2:
            unique_attribute_names = error_data['Attribute Name'].dropna().unique()
            selected_attribute_name = st.selectbox('Attribute Name', options=['All'] + list(unique_attribute_names))
        
        with col3:
            unique_allowed_values = error_data['Allowed Values'].dropna().unique()
            selected_allowed_values = st.selectbox('Allowed Values', options=['All'] + list(unique_allowed_values))
        
        with col4:
            unique_error_codes = error_data['Error Code'].dropna().unique()
            selected_error_code = st.selectbox('Error Code', options=['All'] + list(unique_error_codes))
        
        with col5:
            unique_event_types = error_data['Event Type'].dropna().unique()
            selected_event_type = st.selectbox('Event Type', options=['All'] + list(unique_event_types))
        
        # Applying filters based on selection
        if selected_exception_type != 'All':
            error_data = error_data[error_data['Exception Type'] == selected_exception_type]
        
        if selected_attribute_name != 'All':
            error_data = error_data[error_data['Attribute Name'] == selected_attribute_name]
        
        if selected_allowed_values != 'All':
            error_data = error_data[error_data['Allowed Values'] == selected_allowed_values]
        
        if selected_error_code != 'All':
            error_data = error_data[error_data['Error Code'] == selected_error_code]
        
        if selected_event_type != 'All':
            error_data = error_data[error_data['Event Type'] == selected_event_type]
        
        # Display the filtered DataFrame across the entire width
        st.dataframe(error_data)
        st.write(f"Displaying {len(error_data)} rows of data")

    def display_audit_trail(self):
        st.subheader("Audit Trail / Detailed View")

        # Load the main consolidated data
        data = self.load_data()
        unique_order_ids = data['orderID'].dropna().unique()
        selected_order_id = st.selectbox('orderID', options=['Select an orderID'] + list(unique_order_ids))

        if selected_order_id != 'Select an orderID':
            # Displaying the return value from trailBuilderV2.main(orderID)
            trail_info = trailBuilderV2.main(selected_order_id)
            st.write(trail_info)
            filtered_data = data[data['orderID'] == selected_order_id]
            st.dataframe(filtered_data)
            st.write(f"Displaying {len(filtered_data)} rows of data for orderID {selected_order_id}")
        else:
            st.write("Please select an orderID to view details.")
        

    def load_data(self):
        # Assuming 'consolidated_data.csv' contains your CAT submissions data
        data = pd.read_csv('consolidated_data.csv')
        data['eventTimestamp'] = pd.to_datetime(data['eventTimestamp'], errors='ignore')
        return data

    def display_summary_dashboard(self):
        st.subheader("Summary Dashboard")

        # Load error log data
        error_data = self.load_error_data()

        # Data Quality Exceptions by Error (including Error Code, Exception Type, and Count)
        error_summary = error_data.groupby(['Error Code', 'Exception Type']).size().reset_index(name='Count')
        
        # Data Quality Exceptions by Event Type (including Event Type, Exception Type, and Count)
        event_summary = error_data.groupby(['Event Type', 'Exception Type']).size().reset_index(name='Count')

        # Containers for the dashboards
        error_container = st.container()
        event_container = st.container()

        with error_container:
            st.subheader("Data Quality Exceptions by Error")
            col1, col2 = st.columns([1, 1])
            with col1:
                st.dataframe(error_summary)  # Display DataFrame
            with col2:
                # Bar chart for Data Quality Exceptions by Error (using Error Code)
                fig1, ax1 = plt.subplots(figsize=(8, 6))
                error_codes = error_summary['Error Code'].unique()
                colors1 = plt.cm.Paired(np.linspace(0, 1, len(error_codes)))
                for i, error_code in enumerate(error_codes):
                    df = error_summary[error_summary['Error Code'] == error_code]
                    ax1.bar(df['Error Code'], df['Count'], color=colors1[i], label=error_code)
                ax1.set_xlabel('Error Code')
                ax1.set_ylabel('Count')
                ax1.set_title('Data Quality Exceptions by Error')
                ax1.legend(title='Error Code')
                st.pyplot(fig1)

        with event_container:
            st.subheader("Data Quality Exceptions by Event Type")
            col3, col4 = st.columns([1, 1])
            with col3:
                st.dataframe(event_summary)  # Display DataFrame
            with col4:
                # Bar chart for Data Quality Exceptions by Event Type (using Event Type)
                fig2, ax2 = plt.subplots(figsize=(8, 6))
                event_types = event_summary['Event Type'].unique()
                colors2 = plt.cm.Paired(np.linspace(0, 1, len(event_types)))
                for i, event_type in enumerate(event_types):
                    df = event_summary[event_summary['Event Type'] == event_type]
                    ax2.bar(df['Event Type'], df['Count'], color=colors2[i], label=event_type)
                ax2.set_xlabel('Event Type')
                ax2.set_ylabel('Count')
                ax2.set_title('Data Quality Exceptions by Event Type')
                ax2.legend(title='Event Type')
                st.pyplot(fig2)


if __name__ == "__main__":
    app = StreamlitApp()
