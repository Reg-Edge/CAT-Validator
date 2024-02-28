import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import trailBuilderV2
class StreamlitApp:
    def __init__(self):
        self.setup_page()
        self.style_nav_bar()
        self.page_router()

    def setup_page(self):
        st.set_page_config(page_title="CAT Validator App", layout="wide")
        st.title('CAT VALIDATOR')
        st.markdown("""
    <style>
    /* Remove padding and margin from the main block */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 0 !important;
    }
    /* Additional selectors for fine-tuning might be needed */
    </style>
""", unsafe_allow_html=True)

    def style_nav_bar(self):
        # Custom CSS to remove button borders and style the navigation bar
        st.markdown("""
    <style>
    div.stButton > button {
        border: 2px solid #4CAF50; /* Green border */
        color: black; /* Text color */
        padding: 10px 24px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 16px;
        margin: 4px 2px;
        transition-duration: 0.4s;
        cursor: pointer;
        background-color: transparent; /* Button background */
        border-radius: 5px; /* Rounded corners */
    }
    div.stButton > button:hover {
        background-color: #4CAF50; /* Green background on hover */
        color: white; /* Text color on hover */
        box-shadow: 0 12px 16px 0 rgba(0,0,0,0.24), 0 17px 50px 0 rgba(0,0,0,0.19); /* Pop effect */
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
        
        st.markdown("<hr/>", unsafe_allow_html=True)


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

    def load_error_data(self,submission_date):
    # Format the filename based on the selected date
        filename = f"error_log_{submission_date.strftime('%Y%m%d')}.csv"
        try:
            data = pd.read_csv(filename)
            return data
        except FileNotFoundError:
            st.error(f"File {filename} not found.")
            return pd.DataFrame() 

    def display_exceptions(self):
        st.subheader("Exceptions")
        submission_date = st.date_input("Enter submission date")
        # Load the error data

        if submission_date:
            error_data = self.load_error_data(submission_date)

            if not error_data.empty:
            
            # Filter UI setup with Streamlit columns
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    unique_exception_types = error_data['Error Type'].dropna().unique()
                    selected_exception_type = st.selectbox('Error Type', options=['All'] + list(unique_exception_types))
                
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
        # Load error log data

        submission_date = st.date_input("Enter submission date")
        if submission_date:
            error_data = self.load_error_data(submission_date)
            if not error_data.empty:
                self.display_summary_stats()
                # Data Quality Exceptions by Error
                error_summary = error_data.groupby(['Error Code', 'Error Type']).size().reset_index(name='Count')
                
                # Data Quality Exceptions by Event Type
                event_summary = error_data.groupby(['Event Type', 'Error Type']).size().reset_index(name='Count')

                # Container for Data Quality Exceptions by Error with custom styling
                st.markdown('<div class="data-container">', unsafe_allow_html=True)
                self.plot_error_summary(error_summary)
                st.markdown('</div>', unsafe_allow_html=True)  # End of container

                # Container for Data Quality Exceptions by Event Type with custom styling
                st.markdown('<div class="data-container">', unsafe_allow_html=True)
                self.plot_event_summary(event_summary)
                st.markdown('</div>', unsafe_allow_html=True)  # End of container


    def plot_error_summary(self, error_summary):
        st.subheader("Data Quality Exceptions by Error")
        error_container = st.container()
        col1, col2 = st.columns([1, 1])
        with error_container:
            with col1:
                st.dataframe(error_summary, height=400)
            with col2:
                fig1 = px.bar(error_summary, x='Error Code', y='Count', color='Error Code', 
                            title="Data Quality Exceptions by Error", labels={'Count':'Number of Exceptions'})
                fig1.update_layout(
                    xaxis_title='Error Code', yaxis_title='Number of Exceptions', legend_title='Error Code',
                    paper_bgcolor='rgb(233,233,233)',  # Light gray background around the chart
                plot_bgcolor='rgb(233,233,233)',  # Light gray background inside the chart area
                margin=dict(t=60, l=60, b=60, r=60)  # Adjust margins to create a 'border' effect
                    )
                st.plotly_chart(fig1, use_container_width=True)

    def plot_event_summary(self, event_summary):
        st.subheader("Data Quality Exceptions by Event Type")
        col1, col2 = st.columns([1, 1])
        with col1:
            st.dataframe(event_summary, height=400)
        with col2:
            fig2 = px.bar(event_summary, x='Event Type', y='Count', color='Event Type', 
              title="Data Quality Exceptions by Event Type", labels={'Count':'Number of Exceptions'})
            fig2.update_layout(
                xaxis_title='Event Type',
                yaxis_title='Number of Exceptions',
                legend_title='Event Type',
                paper_bgcolor='rgb(233,233,233)',  # Light gray background around the chart
                plot_bgcolor='rgb(233,233,233)',  # Light gray background inside the chart area
                margin=dict(t=60, l=60, b=60, r=60)  # Adjust margins to create a 'border' effect
            )
            st.plotly_chart(fig2, use_container_width=True)
    def display_summary_stats(self):
        # Custom CSS to style the containers
        st.markdown("""
        <style>
        .custom-container {
            border: none;
            border-radius: 5px;
            padding: 20px;
            margin: 10px 0;
            text-align: center;
            background-color: #f9f9f9;
        }
        .title {
            font-size: 20px;
            font-weight: bold;
        }
        .number {
            font-size: 30px;
        }
        </style>
        """, unsafe_allow_html=True)

        # Data for the containers
        data = [
            {"title": "Submissions Sent", "number": "1,234"},
            {"title": "Submissions Received", "number": "1,100"},
            {"title": "Submissions Rejected", "number": "134"},
            {"title": "Exceptions Generated", "number": "56"}
        ]

        # Creating four equally spaced containers
        cols = st.columns(4)
        for i, item in enumerate(data):
            with cols[i]:
                st.markdown(f"""
                <div class="custom-container">
                    <div class="title">{item["title"]}</div>
                    <div class="number">{item["number"]}</div>
                </div>
                """, unsafe_allow_html=True)


if __name__ == "__main__":
    app = StreamlitApp()
