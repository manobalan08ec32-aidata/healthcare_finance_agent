import streamlit as st
import os
import pandas as pd
import asyncio
import json

# Direct import following the same pattern as llm_router_agent.py
from core.databricks_client import DatabricksClient

# Page configuration
st.set_page_config(
    page_title="Data Dictionary",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Simplified CSS styling - REMOVED gray box styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    .block-container {
        padding-top: 0.5rem !important;
        padding-bottom: 1rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 1400px !important;
        background-color: #FAFAF8 !important;
    }
    
    body {
        background-color: #FAFAF8 !important;
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    .header-section {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1rem;
        padding: 0.5rem 0;
        width: 100%;
    }
    
    .title-container {
        flex: 1;
        text-align: center;
        margin-left: -120px;
    }
    
    .main-title {
        font-size: 2.5rem;
        font-weight: 700 !important;
        color: #D74120;
        margin: 0;
        line-height: 1.2;
    }
    
    .back-button-header {
        flex: 0 0 auto;
    }
    
    .left-panel {
        padding: 1rem 0;
    }
    
    .right-panel {
        padding: 1rem 2rem;
        overflow-y: auto;
    }
    
    .section-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #D74120;
        margin-bottom: 1.5rem;
        border-bottom: 2px solid #D74120;
        padding-bottom: 0.5rem;
    }
    
    /* Style for the text below Available Datasets */
    .dataset-instruction {
        color: #333 !important;
        font-weight: 500 !important;
        margin-bottom: 1rem;
    }
    
    .loading-container {
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 3rem;
    }
    
    .spinner {
        width: 40px;
        height: 40px;
        border: 4px solid #f3f3f3;
        border-top: 4px solid #007bff;
        border-radius: 50%;
        animation: spin 1s linear infinite;
        margin-right: 1rem;
    }
    
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    .loading-text {
        font-size: 1.1rem;
        color: #666;
    }
    
    .empty-state {
        text-align: center;
        padding: 4rem 2rem;
        color: #6c757d;
    }
    
    .empty-state-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
    }
    
    /* Individual button styling - Base styles */
    .stButton > button {
        background: linear-gradient(45deg, #007bff, #0056b3) !important;
        color: white !important;
        border: none !important;
        border-radius: 25px !important;
        padding: 0.5rem 1.5rem !important;
        font-size: 0.9rem !important;
        font-weight: 500 !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 6px 20px rgba(0,123,255,0.3) !important;
        cursor: pointer !important;
    }
    
    .stButton > button:hover {
        background: linear-gradient(45deg, #0056b3, #004085) !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 25px rgba(0,123,255,0.4) !important;
    }
    
    .stButton > button:active {
        transform: translateY(-1px) !important;
    }
    
    /* Enhanced dataframe styling for text wrapping */
    .stDataFrame {
        font-size: 0.85rem !important;
    }
    
    .stDataFrame [data-testid="stDataFrameResizeHandle"] {
        display: block !important;
    }
    
    /* Force aggressive text wrapping - UPDATED */
    .stDataFrame div[data-testid="stDataFrame"] > div > div > div > div {
        white-space: pre-wrap !important;
        word-break: break-word !important;
        overflow-wrap: break-word !important;
        word-wrap: break-word !important;
    }
    
    /* Force text wrapping in all dataframe cells */
    .stDataFrame td, .stDataFrame th {
        white-space: normal !important;
        word-break: break-word !important;
        overflow-wrap: anywhere !important;
        word-wrap: break-word !important;
        vertical-align: top !important;
        padding: 12px !important;
        line-height: 1.5 !important;
        max-width: 400px !important;
        min-width: 100px !important;
    }
    
    /* Enhanced dataframe styling for better content display */
    .stDataFrame table {
        width: 100% !important;
        table-layout: auto !important;
    }
    
    /* Force cell content to wrap naturally with additional specificity */
    .stDataFrame tbody tr td {
        white-space: normal !important;
        word-break: break-word !important;
        overflow-wrap: anywhere !important;
        word-wrap: break-word !important;
        height: auto !important;
        vertical-align: top !important;
        padding: 12px !important;
        line-height: 1.5 !important;
    }
    
    /* Specific styling for Description column */
    .stDataFrame tbody tr td:last-child {
        max-width: 600px !important;
        white-space: normal !important;
        word-wrap: break-word !important;
    }
    
    /* Styled cards for sections */
    .metadata-card {
        background: white;
        border: 1px solid #EDE8E0;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    }
    
    .card-title {
        font-size: 1.2rem;
        font-weight: 700;
        color: #D74120;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    /* Dropdown styling */
    .stSelectbox > div > div {
        border: 2px solid #FF612B !important;
        border-radius: 8px !important;
    }
    
    .stSelectbox > label {
        font-weight: 600 !important;
        color: #333 !important;
    }
</style>
""", unsafe_allow_html=True)

async def get_table_schema(db_client, table_name):
    """Get table schema using DESCRIBE command with async execution"""
    try:
        sql_query = f"DESCRIBE {table_name}"
        print(f"🔍 Executing: {sql_query}")
        
        # Using the async execute_sql_async method
        results = await db_client.execute_sql_async(sql_query)
        print(f"✅ Retrieved {len(results)} columns for {table_name}")
        
        if results:
            df_data = []
            for row in results:
                col_name = row.get('col_name', '')
                # Filter out junk records
                if col_name not in ["# Clustering Information", "# col_name"]:
                    df_data.append({
                        'Column Name': col_name,
                        'Data Type': row.get('data_type', ''),
                        'Description': row.get('comment', '') or 'No description available'
                    })
            
            df = pd.DataFrame(df_data)
            return df
        else:
            return None
            
    except Exception as e:
        print(f"❌ Error getting table schema: {str(e)}")
        st.error(f"Error retrieving table schema: {str(e)}")
        return None

async def get_table_description(db_client, table_name):
    """Get table description using information_schema.tables with async execution"""
    try:
        # Extract just the table name from the full qualified name
        actual_table_name = table_name.split('.')[-1]
        
        sql_query = f'SELECT comment as table_comment FROM prd_optumrx_orxfdmprdsa.information_schema.tables WHERE table_name="{actual_table_name}"'
        print(f"🔍 Executing: {sql_query}")
        
        results = await db_client.execute_sql_async(sql_query)
        print(f"✅ Retrieved table description for {actual_table_name}")
        
        if results and len(results) > 0:
            table_comment = results[0].get('table_comment', '')
            return table_comment if table_comment else 'No description available'
        else:
            return 'No description available'
            
    except Exception as e:
        print(f"❌ Error getting table description: {str(e)}")
        return 'Description not available'

async def get_calculated_metrics(db_client, table_name):
    """Get calculated metrics from metrics_config table for the specific table"""
    try:
        sql_query = f'SELECT column_name, description FROM prd_optumrx_orxfdmprdsa.rag.metrics_config WHERE table_name = "{table_name}"'
        print(f"🔍 Executing calculated metrics query: {sql_query}")
        
        results = await db_client.execute_sql_async(sql_query)
        print(f"✅ Retrieved {len(results)} calculated metrics for {table_name}")
        
        if results:
            df_data = []
            for row in results:
                df_data.append({
                    'Column Name': row.get('column_name', ''),
                    'Description': row.get('description', '') or 'No description available'
                })
            
            df = pd.DataFrame(df_data)
            return df
        else:
            return None
            
    except Exception as e:
        print(f"❌ Error getting calculated metrics: {str(e)}")
        return None

async def display_table_metadata(table_name, display_name):
    """Display table metadata with loading indicator using async methods"""
    
    # Show loading spinner
    loading_placeholder = st.empty()
    loading_placeholder.markdown("""
    <div class="loading-container">
        <div class="spinner"></div>
        <div class="loading-text">Loading table schema...</div>
    </div>
    """, unsafe_allow_html=True)
    
    try:
        # Initialize Databricks client the same way as in router agent
        db_client = DatabricksClient()
        
        # Get table description (async)
        table_description = await get_table_description(db_client, table_name)
        
        # Get table schema (async)
        schema_df = await get_table_schema(db_client, table_name)
        
        # Get calculated metrics (async)
        calculated_metrics_df = await get_calculated_metrics(db_client, table_name)
        
        # Clear loading spinner
        loading_placeholder.empty()
        
        if schema_df is not None and not schema_df.empty:
            # Show dataset information with styled card
            st.markdown(f"""
            <div class="metadata-card">
                <div class="card-title">📝 Table Description</div>
                <p style="margin: 0; line-height: 1.6; color: #333; word-wrap: break-word; white-space: pre-wrap;">{table_description}</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Display actual table columns with styled card
            st.markdown("""
            <div class="metadata-card">
                <div class="card-title">📋 Table Columns</div>
            """, unsafe_allow_html=True)
            
            # Remove Data Type column and convert to HTML for better wrapping
            schema_display = schema_df[['Column Name', 'Description']].copy()
            
            # Create HTML table with proper text wrapping
            html_table = '<table style="width: 100%; border-collapse: collapse; margin-top: 1rem;">'
            html_table += '<thead><tr>'
            html_table += '<th style="background-color: #f8f9fa; color: #D74120; font-weight: 600; padding: 12px; text-align: left; border: 1px solid #dee2e6; width: 30%;">Column Name</th>'
            html_table += '<th style="background-color: #f8f9fa; color: #D74120; font-weight: 600; padding: 12px; text-align: left; border: 1px solid #dee2e6; width: 70%;">Description</th>'
            html_table += '</tr></thead><tbody>'
            
            for idx, row in schema_display.iterrows():
                html_table += '<tr>'
                html_table += f'<td style="padding: 12px; border: 1px solid #dee2e6; vertical-align: top; font-weight: 500; color: #333;">{row["Column Name"]}</td>'
                html_table += f'<td style="padding: 12px; border: 1px solid #dee2e6; vertical-align: top; white-space: normal; word-wrap: break-word; line-height: 1.6; color: #555;">{row["Description"]}</td>'
                html_table += '</tr>'
            
            html_table += '</tbody></table>'
            
            st.markdown(html_table, unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Display calculated metrics section with styled card
            if calculated_metrics_df is not None and not calculated_metrics_df.empty:
                st.markdown("""
                <div class="metadata-card">
                    <div class="card-title">🧮 Calculated Columns</div>
                    <p style="color: #666; font-size: 0.9rem; margin: 0 0 1rem 0;">These are computed metrics and derived columns available for analysis</p>
                """, unsafe_allow_html=True)
                
                # Create HTML table for calculated metrics with proper text wrapping
                html_calc_table = '<table style="width: 100%; border-collapse: collapse; margin-top: 1rem;">'
                html_calc_table += '<thead><tr>'
                html_calc_table += '<th style="background-color: #f8f9fa; color: #D74120; font-weight: 600; padding: 12px; text-align: left; border: 1px solid #dee2e6; width: 30%;">Column Name</th>'
                html_calc_table += '<th style="background-color: #f8f9fa; color: #D74120; font-weight: 600; padding: 12px; text-align: left; border: 1px solid #dee2e6; width: 70%;">Description</th>'
                html_calc_table += '</tr></thead><tbody>'
                
                for idx, row in calculated_metrics_df.iterrows():
                    html_calc_table += '<tr>'
                    html_calc_table += f'<td style="padding: 12px; border: 1px solid #dee2e6; vertical-align: top; font-weight: 500; color: #333;">{row["Column Name"]}</td>'
                    html_calc_table += f'<td style="padding: 12px; border: 1px solid #dee2e6; vertical-align: top; white-space: normal; word-wrap: break-word; line-height: 1.6; color: #555;">{row["Description"]}</td>'
                    html_calc_table += '</tr>'
                
                html_calc_table += '</tbody></table>'
                
                st.markdown(html_calc_table, unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                # Show message if no calculated metrics found
                st.markdown("""
                <div class="metadata-card">
                    <div class="card-title">🧮 Calculated Columns</div>
                    <p style="color: #999; font-size: 0.9rem; margin: 0;">No calculated metrics available for this table</p>
                </div>
                """, unsafe_allow_html=True)
            
        else:
            st.error("❌ No schema data found or table does not exist.")
        
        # Ensure cleanup of async resources
        await db_client.close()
            
    except Exception as e:
        loading_placeholder.empty()
        st.error(f"❌ Failed to load table metadata: {str(e)}")
        print(f"❌ Exception in display_table_metadata: {str(e)}")
        # Ensure cleanup on error
        try:
            await db_client.close()
        except:
            pass

def load_metadata_config():
    """Load metadata configuration from JSON file"""
    try:
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(current_dir, "config", "metadata", "metadata.json")
        
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Error loading metadata configuration: {str(e)}")
        return {}

def get_functional_names_for_domain(metadata_config, domain):
    """Get functional names for a specific domain"""
    if domain not in metadata_config:
        return []
    
    functional_names = []
    for item in metadata_config[domain]:
        functional_names.extend(item.keys())
    return functional_names

def get_table_name_for_functional(metadata_config, domain, functional_name):
    """Get the actual table name for a functional name within a domain"""
    if domain not in metadata_config:
        return None
    
    for item in metadata_config[domain]:
        if functional_name in item:
            return item[functional_name]
    return None

def main():
    """Main metadata page with clean layout"""
    
    # Initialize domain_selection if not present
    if 'domain_selection' not in st.session_state:
        st.session_state.domain_selection = None

    # Check if user has selected a domain
    selected_domain = None
    if st.session_state.domain_selection:
        if isinstance(st.session_state.domain_selection, list) and len(st.session_state.domain_selection) > 0:
            selected_domain = st.session_state.domain_selection[0]
        elif isinstance(st.session_state.domain_selection, str):
            selected_domain = st.session_state.domain_selection
    
    # If no domain selected, redirect to main page
    if not selected_domain:
        st.error("⚠️ Please select a domain from the main page first.")
        st.info("👉 Go back to the main page and select either 'PBM Network' or 'Optum Pharmacy' domain first.")
        if st.button("⬅️ Back to Main Page", key="back_to_main_error"):
            st.switch_page("main.py")
        return
    
    # Load metadata configuration
    metadata_config = load_metadata_config()
    if not metadata_config:
        st.error("❌ Failed to load metadata configuration.")
        return
    
    # Header section with logo, title, and back button
    st.markdown('<div class="header-section">', unsafe_allow_html=True)
    
    col_logo, col_title, col_back = st.columns([1, 4, 1])
    
    with col_logo:
        # Logo
        try:
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            logo_path = os.path.join(current_dir, "assets", "optumrx_logo.png")
            
            if os.path.exists(logo_path):
                st.image(logo_path, width=120)
            else:
                st.markdown("🏥 **Logo**")
        except Exception as e:
            st.markdown("🏥 **OptumRx**")
    
    with col_title:
        st.markdown(f"""
        <div class="title-container">
            <div class="main-title" style="font-size:1.6rem; font-weight:700; color:#D74120; margin:0; line-height:1.2;">
                Data Dictionary - {selected_domain}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col_back:
        if st.button("⬅️ Back to Main Page", key="back_to_main", help="Return to main page"):
            st.switch_page("main.py")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Get functional names for the selected domain
    functional_names = get_functional_names_for_domain(metadata_config, selected_domain)
    
    if not functional_names:
        st.error(f"❌ No datasets found for domain: {selected_domain}")
        return
    
    # Full-width layout with dropdown at top
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <p style="color: #333; font-size: 1rem; font-weight: 500; margin-bottom: 0.5rem;">📊 Select Dataset:</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Dataset selection with dropdown using functional names
    dataset_option = st.selectbox(
        "Select Dataset",
        options=functional_names,
        index=None,
        key="dataset_selection",
        label_visibility="collapsed",
        placeholder="Choose a dataset to view its schema and metadata..."
    )
    
    # Display metadata based on selection
    if dataset_option:
        # Get the actual table name for the selected functional name
        table_name = get_table_name_for_functional(metadata_config, selected_domain, dataset_option)
        
        if table_name:
            # Run async function using asyncio.run()
            asyncio.run(display_table_metadata(
                table_name=table_name,
                display_name=dataset_option.replace('_', ' ').title()
            ))
        else:
            st.error(f"❌ Table name not found for: {dataset_option}")
    else:
        # Empty state when no dataset is selected
        st.markdown("""
        <div style="text-align: center; padding: 4rem 2rem; color: #999;">
            <div style="font-size: 4rem; margin-bottom: 1rem;">📊</div>
            <h3 style="color: #333; margin-bottom: 0.5rem;">Select a Dataset</h3>
            <p style="color: #666;">Choose a dataset from the dropdown above to view its metadata information, column details, and schema structure.</p>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
