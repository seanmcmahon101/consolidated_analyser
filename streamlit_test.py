import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io

# --- Constants ---
# Set of customers that we need in the final data
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
REQUIRED_COLUMNS_IVRV = ['CustID', 'ExtPrice'] # Minimal required columns for IVRV

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


def process_codate_data(df_codate, log_container):
    """Processes the codate dataframe as per the provided logic and logs to container."""

    if not verify_codate_sheet(df_codate, log_container):
        return None

    log_container.info("Starting Codate data processing...")

    # 1) Convert PromShip to datetime
    log_container.info("Converting 'PromShip' column to datetime format...")
    df_codate['PromShip'] = pd.to_datetime(df_codate['PromShip'], errors='coerce')
    initial_rows = len(df_codate)
    df_codate = df_codate.dropna(subset=['PromShip']) # Remove rows with invalid dates
    rows_after_dropna = len(df_codate)
    dropped_date_rows = initial_rows - rows_after_dropna
    log_container.info(f"Converted 'PromShip' to datetime. Dropped {dropped_date_rows} rows with invalid dates.")

    # 2) Filter out dates beyond 2050
    log_container.info("Filtering out dates beyond the year 2050...")
    rows_before_2050_filter = len(df_codate)
    df_codate = df_codate[df_codate['PromShip'].dt.year < 2050]
    rows_after_2050_filter = len(df_codate)
    future_date_rows = rows_before_2050_filter - rows_after_2050_filter
    log_container.info(f"Removed {future_date_rows} rows with dates in or beyond 2050.")

    # 3) Split forecast (LS=2) from regular (LS=3,4)
    log_container.info("Splitting data into Forecast (LS=2) and Regular (LS=3,4) orders...")
    df_ls2 = df_codate[df_codate['LS'] == 2]
    df_regular = df_codate[df_codate['LS'].isin([3, 4])]
    log_container.info(f"Forecast orders (LS=2): {len(df_ls2)} rows.")
    log_container.info(f"Regular orders (LS=3,4): {len(df_regular)} rows.")

    # 4) 6-month filter for forecast
    log_container.info("Applying 6-month filter to Forecast orders...")
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    six_months = today + timedelta(days=182)
    rows_before_6month_filter = len(df_ls2)
    df_ls2_filtered = df_ls2[df_ls2['PromShip'] <= six_months]
    forecast_rows_removed = rows_before_6month_filter - len(df_ls2_filtered)
    log_container.info(f"Forecast orders before 6-month filter: {rows_before_6month_filter} rows.")
    log_container.info(f"Forecast orders after 6-month filter: {len(df_ls2_filtered)} rows (Removed: {forecast_rows_removed} rows).")

    # 5) Combine forecast + regular
    log_container.info("Combining filtered Forecast and Regular orders...")
    df_final_codate = pd.concat([df_ls2_filtered, df_regular])
    log_container.info(f"Combined Codate dataset size: {len(df_final_codate)} rows.")

    # 6) Compute total of Ext Price before customer filter
    total_ext_price_codate = df_final_codate['Ext Price'].sum()
    log_container.info(f"Total 'Ext Price' in Codate (before customer filter): ${total_ext_price_codate:,.2f}")

    # 7) Filter by CUSTOMERS_NEEDED
    log_container.info(f"Filtering Codate data for required customers: {', '.join(CUSTOMERS_NEEDED)}...")
    rows_before_customer_filter = len(df_final_codate)
    df_final_codate = df_final_codate[df_final_codate['CustID'].isin(CUSTOMERS_NEEDED)]
    customer_filtered_rows = rows_before_customer_filter - len(df_final_codate)
    log_container.info(f"Codate rows before customer filter: {rows_before_customer_filter}")
    log_container.info(f"Codate rows after customer filter: {len(df_final_codate)} (Removed: {customer_filtered_rows} rows).")

    # 8) Create pivot table (codate only)
    log_container.info("Creating Codate pivot table (Ext Price sum by Customer)...")
    pivot_codate = pd.pivot_table(
        df_final_codate,
        values='Ext Price',
        index='CustID',
        aggfunc='sum'
    ).reset_index() # Reset index to make CustID a regular column
    log_container.info(f"Codate pivot table created with {len(pivot_codate)} unique customers.")
    log_container.success("Codate data processing complete.")

    return df_final_codate, pivot_codate, total_ext_price_codate


def process_ivrv_data(df_ivrv, log_container):
    """Processes the IVRV dataframe."""
    if not verify_ivrv_sheet(df_ivrv, log_container):
        return None

    log_container.info("Starting IVRV data processing...")

    # Standardize Ext Price / ExtPrice column
    if 'Ext Price' in df_ivrv.columns:
        df_ivrv.rename(columns={'Ext Price': 'ExtPrice'}, inplace=True)
    elif 'ExtPrice' in df_ivrv.columns:
        pass # Already in 'ExtPrice' format
    else: # Should be caught by verify_ivrv_sheet, but for robustness
        log_container.error("IVRV file missing 'Ext Price' or 'ExtPrice' column (after verification).")
        return None

    # Keep only the needed columns for IVRV data
    df_ivrv_processed = df_ivrv[['CustID', 'ExtPrice']].copy()
    df_ivrv_processed['CustID'] = df_ivrv_processed['CustID'].astype(str) # Ensure CustID is string
    log_container.info(f"IVRV data processed, keeping columns 'CustID', 'ExtPrice'. Rows: {len(df_ivrv_processed)}")
    log_container.success("IVRV data processing complete.")
    return df_ivrv_processed


# --- Streamlit App ---
st.set_page_config(page_title="Excel Data Analyzer for Consolidated", page_icon=":bar_chart:", layout="wide")

st.title("Excel Data Analyzer for Consolidated :bar_chart:")
st.markdown("Upload your Codate and IVRV Excel sheets, and I will process and blend them for analysis.")

# File Uploaders
codate_file = st.file_uploader("Upload Codate Excel File", type=["xlsx", "xls"])
ivrv_file = st.file_uploader("Upload IVRV (Missed Invoices) Excel File", type=["xlsx", "xls"])

process_button = st.button("Process Data")

log_expander = st.expander("Processing Log", expanded=False)
log_container = log_expander.container()


if process_button:
    if codate_file is None:
        log_container.error("Please upload the Codate Excel file.")
    elif ivrv_file is None:
        log_container.error("Please upload the IVRV Excel file.")
    else:
        try:
            df_codate_original = pd.read_excel(codate_file)
            df_ivrv_original = pd.read_excel(ivrv_file, header=1) # Assuming header=1 for IVRV as per original logic
            log_container.success("Files uploaded successfully!")

            with st.spinner("Processing data..."):
                processed_codate_df, pivot_codate_df, total_price_codate = process_codate_data(df_codate_original.copy(), log_container) # Process Codate
                processed_ivrv_df = process_ivrv_data(df_ivrv_original.copy(), log_container) # Process IVRV

                if processed_codate_df is not None and processed_ivrv_df is not None:
                    # --- Blend Data ---
                    log_container.info("Blending Codate and IVRV data...")
                    df_codate_simple = processed_codate_df[['CustID', 'Ext Price']].rename(columns={'Ext Price': 'ExtPrice'}).copy() # Prepare codate for blend
                    df_blended = pd.concat([df_codate_simple, processed_ivrv_df], ignore_index=True)
                    log_container.info(f"Total rows after blending Codate + IVRV: {len(df_blended)}")

                    # Filter blended data by CUSTOMERS_NEEDED
                    log_container.info(f"Filtering blended data for required customers: {', '.join(CUSTOMERS_NEEDED)}...")
                    df_blended_filtered = df_blended[df_blended['CustID'].isin(CUSTOMERS_NEEDED)].copy() # Apply customer filter to blended
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
                    with st.expander("Blended Pivot Table", expanded=True): # Blended pivot expanded by default
                        st.dataframe(pivot_blended_df)

                    st.metric(label="Total Ext Price (Codate before customer filter)", value=f"${total_price_codate:,.2f}")


                    # --- Download Button ---
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        processed_codate_df.to_excel(writer, sheet_name='Codate Filtered Data', index=False)
                        pivot_codate_df.to_excel(writer, sheet_name='Codate Pivot', index=False)
                        processed_ivrv_df.to_excel(writer, sheet_name='IVRV Data', index=False)
                        pivot_blended_df.to_excel(writer, sheet_name='Blended Pivot', index=False)
                    output.seek(0)

                    st.download_button(
                        label="Download Blended Results Excel File",
                        data=output.getvalue(),
                        file_name="Blended_Results.xlsx",
                        mime="application/vnd.ms-excel"
                    )

                else:
                    st.error("Data processing failed. Check the processing log for details.")


        except Exception as e:
            st.error(f"An error occurred during file processing. Please check the files and ensure they are valid.\n Error details: {e}")