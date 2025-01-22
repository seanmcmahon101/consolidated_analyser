import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io

# --- Aesthetics / Page Configuration ---
st.set_page_config(
    page_title="Excel Data Analyzer",
    page_icon=":bar_chart:",
    layout="wide"
)

# Inject a little CSS styling to make the app look nicer
st.markdown(
    """
    <style>
    /* Main background color and font styling */
    .main {
        background-color: #f7f9fc;
        font-family: 'Open Sans', sans-serif;
    }
    /* Titles and headers */
    h1, h2, h3, h4 {
        color: #2c3e50;
        font-weight: 600;
    }
    /* Buttons */
    .stButton > button {
        background-color: #4CAF50;
        color: white;
        border-radius: 5px;
        font-size: 16px;
        font-weight: 600;
        padding: 8px 16px;
    }
    .stButton > button:hover {
        background-color: #45A049;
    }
    /* Download button */
    .stDownloadButton > button {
        background-color: #0073e6;
        color: white;
        border-radius: 5px;
        font-size: 16px;
        font-weight: 600;
        padding: 8px 16px;
    }
    .stDownloadButton > button:hover {
        background-color: #005bb5;
    }
    /* Expander styling */
    .streamlit-expanderHeader {
        font-size: 18px !important;
        color: #2c3e50 !important;
        font-weight: 600 !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- Constants ---
# Set of customers that we need in the final data - IMPORTANT: Use set for faster lookups
CUSTOMERS_NEEDED = {
    'HFCUSD',
    'HFINC',
    'HFKOREA',
    'HYDBRZ',
    'BOSOIL',
    'BOSOIL2',
    'BOSCHH',
    'BOSCHCH',
    'BRUEN',
    'BREXAU',
    'REXRO',
    'BREXSA',
    'BOSCHNURN'
}

REQUIRED_COLUMNS_CODATE = ['CustID', 'PromShip', 'LS', 'Ext Price']
REQUIRED_COLUMNS_IVRV = ['CustID', 'ExtPrice']
REQUIRED_COLUMNS_ARINVOICE = ['CustomerID', 'IvcDate', 'ExtPrice']  # RecordType REMOVED

# --- Helper Functions ---
def verify_codate_sheet(df, log_container):
    """Verifies if the codate dataframe has the required columns."""
    missing = [c for c in REQUIRED_COLUMNS_CODATE if c not in df.columns]
    if missing:
        log_container.error(f"Error: Codate file missing required columns: {missing}")
        return False
    return True

def verify_ivrv_sheet(df, log_container):
    """Verifies if the IVRV dataframe has the required columns."""
    if 'CustID' not in df.columns:
        log_container.error("Error: IVRV file missing 'CustID' column.")
        return False
    if 'Ext Price' not in df.columns and 'ExtPrice' not in df.columns:
        log_container.error("Error: IVRV file missing 'Ext Price' or 'ExtPrice' column.")
        return False
    return True

def verify_arinvoice_sheet(df, log_container):
    """Verifies if the ARINVOICE dataframe has the required columns."""
    missing = [c for c in REQUIRED_COLUMNS_ARINVOICE if c not in df.columns]
    if missing:
        log_container.error(f"Error: AR Invoice file missing required columns: {missing}")
        return False
    return True

def process_codate_data(df_codate, log_container):
    """Processes the codate dataframe as per the provided logic and logs to container."""
    if not verify_codate_sheet(df_codate, log_container):
        return None, None, None

    log_container.info("Starting Codate data processing...")
    df_codate['PromShip'] = pd.to_datetime(df_codate['PromShip'], errors='coerce')
    df_codate = df_codate.dropna(subset=['PromShip'])
    df_codate = df_codate[df_codate['PromShip'].dt.year < 2050]

    df_ls2 = df_codate[df_codate['LS'] == 2]
    df_regular = df_codate[df_codate['LS'].isin([3, 4])]

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    six_months = today + timedelta(days=182)

    df_ls2_filtered = df_ls2[df_ls2['PromShip'] <= six_months]
    df_final_codate = pd.concat([df_ls2_filtered, df_regular])

    total_ext_price_codate = df_final_codate['Ext Price'].sum()

    # Filter by needed customers
    df_final_codate = df_final_codate[df_final_codate['CustID'].isin(CUSTOMERS_NEEDED)]

    pivot_codate = pd.pivot_table(
        df_final_codate,
        values='Ext Price',
        index='CustID',
        aggfunc='sum'
    ).reset_index()

    log_container.success("Codate data processing complete.")
    return df_final_codate, pivot_codate, total_ext_price_codate

def process_ivrv_data(df_ivrv, log_container):
    """Processes the IVRV dataframe."""
    if not verify_ivrv_sheet(df_ivrv, log_container):
        return None

    log_container.info("Starting IVRV data processing...")

    if 'Ext Price' in df_ivrv.columns:
        df_ivrv.rename(columns={'Ext Price': 'ExtPrice'}, inplace=True)
    elif 'ExtPrice' in df_ivrv.columns:
        pass
    else:
        log_container.error("IVRV file missing 'Ext Price' or 'ExtPrice' column.")
        return None

    df_ivrv_processed = df_ivrv[['CustID', 'ExtPrice']].copy()
    df_ivrv_processed['CustID'] = df_ivrv_processed['CustID'].astype(str)

    log_container.success("IVRV data processing complete.")
    return df_ivrv_processed

def process_arinvoice_data(df_arinvoice, log_container):
    """Processes the ARINVOICE dataframe (now combined AR Invoice/Ship) and performs analysis."""
    if not verify_arinvoice_sheet(df_arinvoice, log_container):
        return None, None, None, None

    log_container.info("Starting AR Invoice/Ship data analysis...")

    # Calculate total ExtPrice BEFORE filtering
    initial_total_ext_price_arinvoice = df_arinvoice['ExtPrice'].sum()
    log_container.info(f"Initial Total 'ExtPrice' in AR Invoice/Ship data: £{initial_total_ext_price_arinvoice:,.2f}")

    # Filter by our customers
    log_container.info(f"Filtering AR Invoice/Ship data by required customers: {', '.join(CUSTOMERS_NEEDED)}...")
    rows_before_customer_filter = len(df_arinvoice)
    df_arinvoice_filtered = df_arinvoice[df_arinvoice['CustomerID'].isin(CUSTOMERS_NEEDED)].copy()
    customer_filtered_rows = rows_before_customer_filter - len(df_arinvoice_filtered)
    log_container.info(f"AR Invoice/Ship rows before customer filter: {rows_before_customer_filter}")
    log_container.info(f"AR Invoice/Ship rows after customer filter: {len(df_arinvoice_filtered)} (Removed: {customer_filtered_rows} rows).")

    # Drop blank rows from 'IvcDate'
    rows_before_date_dropna = len(df_arinvoice_filtered)
    df_arinvoice_filtered = df_arinvoice_filtered.dropna(subset=['IvcDate']).copy()
    date_dropped_rows = rows_before_date_dropna - len(df_arinvoice_filtered)
    log_container.info(f"AR Invoice/Ship rows before dropping blank 'IvcDate': {rows_before_date_dropna}")
    log_container.info(f"AR Invoice/Ship rows after dropping blank 'IvcDate': {len(df_arinvoice_filtered)} (Removed: {date_dropped_rows} rows).")

    # Summation after filtering and dropping dates
    total_ext_price_filtered_arinvoice = df_arinvoice_filtered['ExtPrice'].sum()
    total_ext_price_df_arinvoice = pd.DataFrame({'Total Ext Price': [total_ext_price_filtered_arinvoice]})
    log_container.info(f"Total 'ExtPrice' after customer and date filtering: £{total_ext_price_filtered_arinvoice:,.2f}")

    # Create pivot (customer summary)
    log_container.info("Creating AR Invoice/Ship customer summary pivot table...")
    customer_summary_arinvoice = pd.pivot_table(
        df_arinvoice_filtered,
        values=['ExtPrice'],
        index='CustomerID',
        aggfunc={'ExtPrice': 'sum'}
    ).reset_index()
    log_container.info(f"AR Invoice/Ship customer summary pivot table created with {len(customer_summary_arinvoice)} unique customers.")

    log_container.success("AR Invoice/Ship data analysis complete.")
    return df_arinvoice_filtered, customer_summary_arinvoice, total_ext_price_df_arinvoice, initial_total_ext_price_arinvoice

# --- Streamlit App ---
st.title("Excel Data Analyzer :bar_chart:")
st.markdown("Upload your Codate, IVRV, and AR Invoice/Ship Excel sheets, and I will process and blend them for analysis.")

# File Uploaders
codate_file = st.file_uploader("Upload Codate Excel File", type=["xlsx", "xls"])
ivrv_file = st.file_uploader("Upload IVRV (Missed Invoices) Excel File", type=["xlsx", "xls"])
arinvoice_file = st.file_uploader("Upload AR Invoice/Ship Excel File", type=["xlsx", "xls"])

process_button = st.button("Process Data")

log_expander = st.expander("Processing Log", expanded=False)
log_container = log_expander.container()

if process_button:
    if codate_file is None:
        log_container.error("Please upload the Codate Excel file.")
    elif ivrv_file is None:
        log_container.error("Please upload the IVRV Excel file.")
    elif arinvoice_file is None:
        log_container.error("Please upload the AR Invoice/Ship Excel file.")
    else:
        try:
            df_codate_original = pd.read_excel(codate_file)
            # We skip the first row for IVRV if needed, but your code shows header=1 for IVRV, so let's keep it.
            df_ivrv_original = pd.read_excel(ivrv_file, header=1)
            # IMPORTANT: skip the first row for the AR Invoice/Ship data
            df_arinvoice_original = pd.read_excel(arinvoice_file, skiprows=1, skipfooter=1)

            log_container.success("Files uploaded successfully!")

            with st.spinner("Processing data..."):
                processed_codate_df, pivot_codate_df, total_price_codate = process_codate_data(df_codate_original.copy(), log_container)
                processed_ivrv_df = process_ivrv_data(df_ivrv_original.copy(), log_container)
                processed_arinvoice_df, pivot_arinvoice_customer_summary_df, total_price_arinvoice_df, initial_total_price_arinvoice = process_arinvoice_data(df_arinvoice_original.copy(), log_container)

                if processed_codate_df is not None and processed_ivrv_df is not None and processed_arinvoice_df is not None:
                    # --- Blend Codate and IVRV Data ---
                    log_container.info("Blending Codate and IVRV data...")
                    df_codate_simple = processed_codate_df[['CustID', 'Ext Price']].rename(columns={'Ext Price': 'ExtPrice'}).copy()
                    df_blended = pd.concat([df_codate_simple, processed_ivrv_df], ignore_index=True)
                    log_container.info(f"Total rows after blending Codate + IVRV: {len(df_blended)}")

                    # Filter blended data by CUSTOMERS_NEEDED
                    log_container.info(f"Filtering blended data for required customers: {', '.join(CUSTOMERS_NEEDED)}...")
                    df_blended_filtered = df_blended[df_blended['CustID'].isin(CUSTOMERS_NEEDED)].copy()
                    log_container.info(f"Blended rows after customer filter: {len(df_blended_filtered)}")

                    # Create Blended Pivot
                    log_container.info("Creating Blended pivot table (ExtPrice sum by Customer)...")
                    pivot_blended_df = pd.pivot_table(
                        df_blended_filtered,
                        values='ExtPrice',
                        index='CustID',
                        aggfunc='sum'
                    ).reset_index()
                    log_container.info(f"Blended pivot table created with {len(pivot_blended_df)} unique customers.")
                    log_container.success("Data blending and pivot complete.")

                    # --- Display Results ---
                    with st.expander("Processed Codate Data Preview", expanded=False):
                        st.dataframe(processed_codate_df.head())
                    with st.expander("Codate Pivot Table", expanded=False):
                        st.dataframe(pivot_codate_df)
                    with st.expander("Processed IVRV Data Preview", expanded=False):
                        st.dataframe(processed_ivrv_df.head())
                    with st.expander("Blended Pivot Table", expanded=True):
                        st.dataframe(pivot_blended_df)

                    st.metric(label="Total Ext Price (Codate before customer filter)", 
                              value=f"£{total_price_codate:,.2f}")

                    # --- AR Invoice/Ship Combined Results Display ---
                    st.subheader("AR Invoice & Ship Analysis Results")
                    with st.expander("AR Invoice/Ship Raw Data Preview", expanded=False):
                        st.dataframe(processed_arinvoice_df.head())
                    with st.expander("AR Invoice/Ship Customer Summary Pivot Table", expanded=False):
                        st.dataframe(pivot_arinvoice_customer_summary_df)

                    st.metric(
                        label="Initial Total Ext Price (AR Invoice/Ship - Before Filters)",
                        value=f"£{initial_total_price_arinvoice:,.2f}"
                    )
                    st.metric(
                        label="Total Ext Price (AR Invoice/Ship - After Filters)",
                        value=f"£{total_price_arinvoice_df['Total Ext Price'].iloc[0]:,.2f}"
                    )

                    # --- Download Button ---
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        pivot_blended_df.to_excel(writer, sheet_name='Blended Pivot USE', index=False)
                        pivot_arinvoice_customer_summary_df.to_excel(writer, sheet_name='AR Ivc & Customer Summary USE', index=False)
                        processed_codate_df.to_excel(writer, sheet_name='Codate Filtered Data', index=False)
                        pivot_codate_df.to_excel(writer, sheet_name='Codate Pivot', index=False)
                        processed_ivrv_df.to_excel(writer, sheet_name='IVRV Data', index=False)
                        processed_arinvoice_df.to_excel(writer, sheet_name='AR Invoice & Ship Data', index=False)
                        #total_price_arinvoice_df.to_excel(writer, sheet_name='AR Ivc & Total Ext Price', index=False)

                    output.seek(0)
                    st.download_button(
                        label="Download Blended & AR Results Excel File",
                        data=output.getvalue(),
                        file_name="Blended_AR_Results.xlsx",
                        mime="application/vnd.ms-excel"
                    )

                else:
                    st.error("Data processing failed. Check the processing log for details.")

        except Exception as e:
            st.error(f"An error occurred during file processing. Please check the files and ensure they are valid.\n Error details: {e}")
