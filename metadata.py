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
    .block-container {
        padding-top: 0.5rem !important;
        padding-bottom: 1rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 1400px !important;
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
        color: #002677;
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
        color: #002677;
        margin-bottom: 1.5rem;
        border-bottom: 2px solid #002677;
        padding-bottom: 0.5rem;
    }
    
    /* Style for the text below Available Datasets */
    .dataset-instruction {
        color: #002677 !important;
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
    
    .stDataFrame td, .stDataFrame th {
        white-space: pre-wrap !important;
        word-break: break-word !important;
        overflow-wrap: break-word !important;
        word-wrap: break-word !important;
        vertical-align: top !important;
        padding: 8px !important;
        line-height: 1.4 !important;
        max-width: none !important;
    }
    
    /* Enhanced dataframe styling for better content display */
    .stDataFrame table {
        width: 100% !important;
    }
    
    /* Force cell content to wrap naturally */
    .stDataFrame tbody tr td {
        white-space: pre-wrap !important;
        word-break: break-word !important;
        overflow-wrap: break-word !important;
        word-wrap: break-word !important;
        height: auto !important;
        vertical-align: top !important;
        padding: 8px !important;
        line-height: 1.4 !important;
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
            # Show dataset information instead of schema summary
            st.markdown(f"""
            <div style="margin-top: 1rem; margin-bottom: 1.5rem; padding: 1.5rem; background-color: #d9f6fa; border-radius: 8px; border-left: 4px solid #002677;">
                <h4 style="color: #002677; margin: 0 0 1rem 0;">📊 Dataset Information</h4>
                <p style="margin: 0; line-height: 1.6; color: #333;">{table_description}</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Display actual table columns
            st.markdown("""
            <div style="margin-top: 1.5rem; margin-bottom: 1rem;">
                <h4 style="color: #002677; margin: 0; font-size: 1.3rem; font-weight: 600;">📋 Table Columns</h4>
            </div>
            """, unsafe_allow_html=True)
            
            # Display the actual table schema
            st.dataframe(
                schema_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Column Name": st.column_config.TextColumn(
                        "Column Name",
                        help="Name of the column in the table"
                    ),
                    "Data Type": st.column_config.TextColumn(
                        "Data Type", 
                        help="Data type of the column"
                    ),
                    "Description": st.column_config.TextColumn(
                        "Description",
                        help="Full description of the column"
                    )
                }
            )
            
            # Display calculated metrics section
            if calculated_metrics_df is not None and not calculated_metrics_df.empty:
                st.markdown("""
                <div style="margin-top: 2rem; margin-bottom: 1rem;">
                    <h4 style="color: #002677; margin: 0; font-size: 1.3rem; font-weight: 600;">🧮 Calculated Columns</h4>
                    <p style="color: #666; font-size: 0.9rem; margin: 0.5rem 0 0 0;">These are computed metrics and derived columns available for analysis</p>
                </div>
                """, unsafe_allow_html=True)
                
                # Display calculated metrics table
                st.dataframe(
                    calculated_metrics_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Column Name": st.column_config.TextColumn(
                            "Column Name",
                            help="Name of the calculated metric"
                        ),
                        "Description": st.column_config.TextColumn(
                            "Description",
                            help="Full description of the calculated metric"
                        )
                    }
                )
            else:
                # Show message if no calculated metrics found
                st.markdown("""
                <div style="margin-top: 2rem; margin-bottom: 1rem;">
                    <h4 style="color: #002677; margin: 0; font-size: 1.3rem; font-weight: 600;">🧮 Calculated Columns</h4>
                    <p style="color: #666; font-size: 0.9rem; margin: 0.5rem 0 0 0;">No calculated metrics available for this table</p>
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
            <div class="main-title" style="font-size:1.6rem; font-weight:700; color:#002677; margin:0; line-height:1.2;">
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
    
    # Two-column layout: Left panel for dataset selection (1/4), Right panel for metadata (3/4)
    left_col, right_col = st.columns([1, 3])
    
    with left_col:
        # Section header - no gray box wrapper
        st.markdown(f"""
        <div class="section-header">
            Available Datasets for {selected_domain}
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="dataset-instruction"><strong>Click to view metadata information:</strong></div>', unsafe_allow_html=True)
        
        # Dataset selection with radio buttons using functional names
        dataset_option = st.radio(
            "Select Dataset",
            options=functional_names,
            index=None,
            key="dataset_selection",
            label_visibility="hidden"
        )
    
    with right_col:
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
            <div class="empty-state">
                <div class="empty-state-icon">📊</div>
                <h3>Select a Dataset</h3>
                <p>Choose a dataset from the left panel to view its metadata information, column details, and schema structure.</p>
            </div>
            """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
