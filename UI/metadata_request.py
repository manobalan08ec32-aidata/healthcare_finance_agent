import streamlit as st
import os
import pandas as pd
import asyncio
import json
from datetime import datetime

# Direct import following the same pattern as metadata.py
from core.databricks_client import DatabricksClient

# Page configuration
st.set_page_config(
    page_title="Metadata Update Request",
    page_icon="📝",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS styling using Optum colors
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 1rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 900px !important;
        background-color: #FAF8F2 !important;
    }
    
    body, .stApp {
        background-color: #FAF8F2 !important;
    }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Header section */
    .header-section {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1.5rem;
        padding: 0.5rem 0;
        width: 100%;
    }
    
    /* Page title */
    .page-title {
        font-size: 22px;
        font-weight: 700;
        color: #D74120;
        margin: 0;
        text-align: center;
    }
    
    /* Form card styling */
    .form-card {
        background: #FFFFFF;
        border: 1px solid #EDE8E0;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 20px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
    }
    
    .form-card-title {
        font-size: 16px;
        font-weight: 600;
        color: #2D3748;
        margin-bottom: 16px;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    
    /* Radio button styling */
    .stRadio > div {
        display: flex !important;
        flex-direction: row !important;
        gap: 24px !important;
    }
    
    .stRadio > div > label {
        padding: 12px 20px !important;
        border: 2px solid #EDE8E0 !important;
        border-radius: 8px !important;
        cursor: pointer !important;
        transition: all 0.2s ease !important;
        background: #FFFFFF !important;
    }
    
    .stRadio > div > label:hover {
        border-color: #FFD1AB !important;
        background: #FFFAF7 !important;
    }
    
    .stRadio > div > label[data-checked="true"],
    .stRadio > div > label:has(input:checked) {
        border-color: #FF612B !important;
        background: #FFFAF7 !important;
        box-shadow: 0 0 0 3px rgba(255, 97, 43, 0.12) !important;
    }
    
    /* Hide radio label */
    .stRadio > label {
        display: none !important;
    }
    
    /* Input field styling */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        border: 2px solid #EDE8E0 !important;
        border-radius: 8px !important;
        padding: 10px 14px !important;
        font-size: 14px !important;
    }
    
    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: #FF612B !important;
        box-shadow: 0 0 0 3px rgba(255, 97, 43, 0.12) !important;
    }
    
    /* Selectbox styling */
    .stSelectbox > div > div {
        border: 2px solid #EDE8E0 !important;
        border-radius: 8px !important;
    }
    
    .stSelectbox > div > div:focus-within {
        border-color: #FF612B !important;
        box-shadow: 0 0 0 3px rgba(255, 97, 43, 0.12) !important;
    }
    
    /* Submit button styling */
    .submit-button .stButton > button {
        background: linear-gradient(135deg, #D74120 0%, #FF612B 100%) !important;
        color: #FFFFFF !important;
        border: none !important;
        border-radius: 10px !important;
        padding: 14px 48px !important;
        font-size: 15px !important;
        font-weight: 600 !important;
        cursor: pointer !important;
        transition: all 0.2s ease !important;
        box-shadow: 0 4px 14px rgba(215, 65, 32, 0.25) !important;
        width: 100% !important;
    }
    
    .submit-button .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 20px rgba(215, 65, 32, 0.35) !important;
    }
    
    /* Back button styling */
    .back-button .stButton > button {
        background: none !important;
        border: 2px solid #EDE8E0 !important;
        color: #4A5568 !important;
        border-radius: 8px !important;
        padding: 8px 16px !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        box-shadow: none !important;
    }
    
    .back-button .stButton > button:hover {
        border-color: #FF612B !important;
        color: #FF612B !important;
        background: #FFFAF7 !important;
        transform: none !important;
    }
    
    /* Current info box */
    .current-info-box {
        background: #F7FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 16px;
        margin: 16px 0;
    }
    
    .current-info-title {
        font-size: 13px;
        font-weight: 600;
        color: #718096;
        margin-bottom: 8px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .current-info-content {
        font-size: 14px;
        color: #2D3748;
        line-height: 1.6;
    }
    
    /* Success message */
    .success-box {
        background: linear-gradient(135deg, #D9F6FA 0%, #FFFFFF 100%);
        border: 1px solid #38A169;
        border-left: 4px solid #38A169;
        border-radius: 8px;
        padding: 20px;
        margin: 20px 0;
    }
    
    .success-title {
        font-size: 16px;
        font-weight: 600;
        color: #38A169;
        margin-bottom: 8px;
    }
    
    .success-content {
        font-size: 14px;
        color: #2D3748;
    }
    
    /* Section divider */
    .section-divider {
        height: 1px;
        background: #EDE8E0;
        margin: 24px 0;
    }
    
    /* Field label */
    .field-label {
        font-size: 14px;
        font-weight: 500;
        color: #2D3748;
        margin-bottom: 6px;
    }
    
    /* Footer */
    .footer {
        text-align: center;
        padding: 20px 0;
        border-top: 1px solid #EDE8E0;
        margin-top: 40px;
    }
    
    .footer-text {
        font-size: 12px;
        color: #A0AEC0;
    }
</style>
""", unsafe_allow_html=True)


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


async def get_table_columns(db_client, table_name):
    """Get column names and descriptions for a table"""
    try:
        sql_query = f"DESCRIBE {table_name}"
        results = await db_client.execute_sql_async(sql_query)
        
        if results:
            columns = []
            for row in results:
                col_name = row.get('col_name', '')
                description = row.get('comment', '') or ''
                
                # Filter out junk records
                if col_name not in ["# Clustering Information", "# col_name"]:
                    columns.append({
                        'column_name': col_name,
                        'description': description.strip() if description else 'No description available'
                    })
            return columns
        return []
    except Exception as e:
        print(f"❌ Error getting table columns: {str(e)}")
        return []


def generate_request_id():
    """Generate a unique request ID"""
    now = datetime.now()
    date_part = now.strftime("%Y-%m-%d")
    time_part = now.strftime("%H%M%S")
    return f"REQ-{date_part}-{time_part}"


async def submit_request_to_databricks(request_data):
    """Submit the metadata request to Databricks table"""
    try:
        db_client = DatabricksClient()
        
        # Escape single quotes in string fields
        def escape_sql(value):
            if value is None:
                return "NULL"
            return "'" + str(value).replace("'", "''") + "'"
        
        # Build INSERT statement
        sql_query = f"""
        INSERT INTO prd_optumrx_orxfdmprdsa.rag.metadata_update_requests 
        (request_id, request_type, domain, dataset_name, table_name, column_name, 
         current_description, new_description, synonyms, business_justification, 
         requested_by, request_status, created_at, updated_at, reviewer_comments)
        VALUES (
            {escape_sql(request_data['request_id'])},
            {escape_sql(request_data['request_type'])},
            {escape_sql(request_data['domain'])},
            {escape_sql(request_data['dataset_name'])},
            {escape_sql(request_data['table_name'])},
            {escape_sql(request_data['column_name'])},
            {escape_sql(request_data['current_description'])},
            {escape_sql(request_data['new_description'])},
            {escape_sql(request_data['synonyms'])},
            {escape_sql(request_data['business_justification'])},
            {escape_sql(request_data['requested_by'])},
            'PENDING',
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            NULL
        )
        """
        
        print(f"🔍 Executing INSERT: {sql_query}")
        await db_client.execute_sql_async(sql_query)
        await db_client.close()
        
        return True, None
    except Exception as e:
        print(f"❌ Error submitting request: {str(e)}")
        return False, str(e)


def main():
    """Main metadata request page"""
    
    # Initialize domain_selection if not present
    if 'domain_selection' not in st.session_state:
        st.session_state.domain_selection = None
    
    # Get selected domain
    selected_domain = None
    if st.session_state.domain_selection:
        if isinstance(st.session_state.domain_selection, list) and len(st.session_state.domain_selection) > 0:
            selected_domain = st.session_state.domain_selection[0]
        elif isinstance(st.session_state.domain_selection, str):
            selected_domain = st.session_state.domain_selection
    
    # Get authenticated user
    user_email = st.session_state.get('authenticated_user', 'Unknown User')
    
    # If no domain selected, show message but still allow access
    if not selected_domain:
        selected_domain = "PBM Network"  # Default domain
    
    # Load metadata configuration
    metadata_config = load_metadata_config()
    if not metadata_config:
        st.error("❌ Failed to load metadata configuration.")
        return
    
    # Header section
    col_logo, col_title, col_back = st.columns([1, 4, 1])
    
    with col_logo:
        try:
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            logo_path = os.path.join(current_dir, "assets", "optumrx_logo.png")
            
            if os.path.exists(logo_path):
                st.image(logo_path, width=120)
            else:
                st.markdown('<span style="font-size: 22px; font-weight: 700; color: #FF612B;">Optum</span><span style="font-size: 22px; color: #2D3748;">Rx</span>', unsafe_allow_html=True)
        except Exception as e:
            st.markdown('<span style="font-size: 22px; font-weight: 700; color: #FF612B;">Optum</span><span style="font-size: 22px; color: #2D3748;">Rx</span>', unsafe_allow_html=True)
    
    with col_title:
        st.markdown(f'<p class="page-title">Metadata Update Request</p>', unsafe_allow_html=True)
    
    with col_back:
        st.markdown('<div class="back-button">', unsafe_allow_html=True)
        if st.button("⬅️ Back", key="back_to_main"):
            st.switch_page("main.py")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Show logged in user
    st.markdown(f"""
    <div style="text-align: right; margin-bottom: 20px;">
        <span style="color: #718096; font-size: 13px;">
            Submitting as: <strong style="color: #2D3748;">{user_email}</strong>
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize session state for form
    if 'request_submitted' not in st.session_state:
        st.session_state.request_submitted = False
    if 'submitted_request_id' not in st.session_state:
        st.session_state.submitted_request_id = None
    
    # Show success message if request was just submitted
    if st.session_state.request_submitted:
        st.markdown(f"""
        <div class="success-box">
            <div class="success-title">✅ Request Submitted Successfully!</div>
            <div class="success-content">
                Your metadata update request has been submitted.<br>
                <strong>Request ID:</strong> {st.session_state.submitted_request_id}<br>
                The FDM team will review your request and update the metadata accordingly.
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("Submit Another Request", key="new_request"):
            st.session_state.request_submitted = False
            st.session_state.submitted_request_id = None
            st.rerun()
        return
    
    # Request type selection
    st.markdown("""
    <div class="form-card">
        <div class="form-card-title">📋 What would you like to do?</div>
    """, unsafe_allow_html=True)
    
    request_type = st.radio(
        "Request Type",
        options=["Request New Column", "Update Existing Metadata"],
        index=0,
        key="request_type_radio",
        horizontal=True,
        label_visibility="collapsed"
    )
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Divider
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Get functional names for domain
    functional_names = get_functional_names_for_domain(metadata_config, selected_domain)
    
    if request_type == "Request New Column":
        # ============ NEW COLUMN REQUEST FORM ============
        st.markdown("""
        <div class="form-card">
            <div class="form-card-title">➕ Request New Column</div>
        """, unsafe_allow_html=True)
        
        # Dataset selection
        st.markdown('<p class="field-label">📊 Select Dataset <span style="color: #D74120;">*</span></p>', unsafe_allow_html=True)
        dataset_new = st.selectbox(
            "Select Dataset",
            options=functional_names,
            index=None,
            key="dataset_new_column",
            placeholder="Choose a dataset...",
            label_visibility="collapsed"
        )
        
        # New column name
        st.markdown('<p class="field-label">📝 New Column Name <span style="color: #D74120;">*</span></p>', unsafe_allow_html=True)
        new_column_name = st.text_input(
            "New Column Name",
            key="new_column_name",
            placeholder="e.g., rebate_amount, member_count",
            label_visibility="collapsed"
        )
        
        # Description
        st.markdown('<p class="field-label">💬 Description <span style="color: #D74120;">*</span></p>', unsafe_allow_html=True)
        new_description = st.text_area(
            "Description",
            key="new_column_description",
            placeholder="Describe what this column represents and how it should be calculated...",
            height=100,
            label_visibility="collapsed"
        )
        
        # Synonyms (optional)
        st.markdown('<p class="field-label">🏷️ Synonyms <span style="color: #718096;">(optional, comma-separated)</span></p>', unsafe_allow_html=True)
        new_synonyms = st.text_input(
            "Synonyms",
            key="new_column_synonyms",
            placeholder="e.g., rebate, discount, refund",
            label_visibility="collapsed"
        )
        
        # Business justification
        st.markdown('<p class="field-label">📄 Business Justification <span style="color: #D74120;">*</span></p>', unsafe_allow_html=True)
        new_justification = st.text_area(
            "Business Justification",
            key="new_column_justification",
            placeholder="Explain why this column is needed and how it will be used...",
            height=100,
            label_visibility="collapsed"
        )
        
        st.markdown("</div>", unsafe_allow_html=True)
        
        # Submit button
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown('<div class="submit-button">', unsafe_allow_html=True)
            if st.button("Submit Request", key="submit_new_column", use_container_width=True):
                # Validation
                if not dataset_new:
                    st.error("Please select a dataset.")
                elif not new_column_name or not new_column_name.strip():
                    st.error("Please enter a column name.")
                elif not new_description or not new_description.strip():
                    st.error("Please enter a description.")
                elif not new_justification or not new_justification.strip():
                    st.error("Please enter a business justification.")
                else:
                    # Get table name
                    table_name = get_table_name_for_functional(metadata_config, selected_domain, dataset_new)
                    
                    # Generate request ID
                    request_id = generate_request_id()
                    
                    # Prepare request data
                    request_data = {
                        'request_id': request_id,
                        'request_type': 'NEW_COLUMN',
                        'domain': selected_domain,
                        'dataset_name': dataset_new,
                        'table_name': table_name,
                        'column_name': new_column_name.strip(),
                        'current_description': None,
                        'new_description': new_description.strip(),
                        'synonyms': new_synonyms.strip() if new_synonyms else None,
                        'business_justification': new_justification.strip(),
                        'requested_by': user_email
                    }
                    
                    # Submit to Databricks
                    with st.spinner("Submitting request..."):
                        success, error = asyncio.run(submit_request_to_databricks(request_data))
                    
                    if success:
                        st.session_state.request_submitted = True
                        st.session_state.submitted_request_id = request_id
                        st.rerun()
                    else:
                        st.error(f"Failed to submit request: {error}")
            st.markdown('</div>', unsafe_allow_html=True)
    
    else:
        # ============ UPDATE METADATA FORM ============
        st.markdown("""
        <div class="form-card">
            <div class="form-card-title">✏️ Update Existing Metadata</div>
        """, unsafe_allow_html=True)
        
        # Dataset selection
        st.markdown('<p class="field-label">📊 Select Dataset <span style="color: #D74120;">*</span></p>', unsafe_allow_html=True)
        dataset_update = st.selectbox(
            "Select Dataset",
            options=functional_names,
            index=None,
            key="dataset_update_metadata",
            placeholder="Choose a dataset...",
            label_visibility="collapsed"
        )
        
        # Column selection (populated after dataset selection)
        columns_list = []
        selected_column_info = None
        
        if dataset_update:
            table_name = get_table_name_for_functional(metadata_config, selected_domain, dataset_update)
            
            if table_name:
                # Fetch columns
                with st.spinner("Loading columns..."):
                    db_client = DatabricksClient()
                    columns_list = asyncio.run(get_table_columns(db_client, table_name))
                
                if columns_list:
                    column_names = [col['column_name'] for col in columns_list]
                    
                    st.markdown('<p class="field-label">📋 Select Column <span style="color: #D74120;">*</span></p>', unsafe_allow_html=True)
                    selected_column = st.selectbox(
                        "Select Column",
                        options=column_names,
                        index=None,
                        key="column_update_metadata",
                        placeholder="Choose a column to update...",
                        label_visibility="collapsed"
                    )
                    
                    # Show current info if column selected
                    if selected_column:
                        selected_column_info = next((col for col in columns_list if col['column_name'] == selected_column), None)
                        
                        if selected_column_info:
                            st.markdown(f"""
                            <div class="current-info-box">
                                <div class="current-info-title">Current Information</div>
                                <div class="current-info-content">
                                    <strong>Column:</strong> {selected_column_info['column_name']}<br>
                                    <strong>Description:</strong> {selected_column_info['description']}
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                else:
                    st.warning("No columns found for this dataset.")
        
        # Only show update fields if column is selected
        if dataset_update and columns_list and 'column_update_metadata' in st.session_state and st.session_state.column_update_metadata:
            
            # New description
            st.markdown('<p class="field-label">📝 New Description <span style="color: #D74120;">*</span></p>', unsafe_allow_html=True)
            update_description = st.text_area(
                "New Description",
                key="update_column_description",
                placeholder="Enter the updated description for this column...",
                value=selected_column_info['description'] if selected_column_info and selected_column_info['description'] != 'No description available' else "",
                height=100,
                label_visibility="collapsed"
            )
            
            # Add synonyms
            st.markdown('<p class="field-label">🏷️ Add Synonyms <span style="color: #718096;">(optional, comma-separated)</span></p>', unsafe_allow_html=True)
            update_synonyms = st.text_input(
                "Add Synonyms",
                key="update_column_synonyms",
                placeholder="e.g., revenue, sales, income",
                label_visibility="collapsed"
            )
            
            # Reason for update
            st.markdown('<p class="field-label">📄 Reason for Update <span style="color: #D74120;">*</span></p>', unsafe_allow_html=True)
            update_reason = st.text_area(
                "Reason for Update",
                key="update_column_reason",
                placeholder="Explain why this update is needed...",
                height=100,
                label_visibility="collapsed"
            )
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Submit button
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.markdown('<div class="submit-button">', unsafe_allow_html=True)
                if st.button("Submit Request", key="submit_update_metadata", use_container_width=True):
                    # Validation
                    if not update_description or not update_description.strip():
                        st.error("Please enter a new description.")
                    elif not update_reason or not update_reason.strip():
                        st.error("Please enter a reason for the update.")
                    else:
                        # Get table name
                        table_name = get_table_name_for_functional(metadata_config, selected_domain, dataset_update)
                        
                        # Generate request ID
                        request_id = generate_request_id()
                        
                        # Prepare request data
                        request_data = {
                            'request_id': request_id,
                            'request_type': 'UPDATE_METADATA',
                            'domain': selected_domain,
                            'dataset_name': dataset_update,
                            'table_name': table_name,
                            'column_name': st.session_state.column_update_metadata,
                            'current_description': selected_column_info['description'] if selected_column_info else None,
                            'new_description': update_description.strip(),
                            'synonyms': update_synonyms.strip() if update_synonyms else None,
                            'business_justification': update_reason.strip(),
                            'requested_by': user_email
                        }
                        
                        # Submit to Databricks
                        with st.spinner("Submitting request..."):
                            success, error = asyncio.run(submit_request_to_databricks(request_data))
                        
                        if success:
                            st.session_state.request_submitted = True
                            st.session_state.submitted_request_id = request_id
                            st.rerun()
                        else:
                            st.error(f"Failed to submit request: {error}")
                st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.markdown("</div>", unsafe_allow_html=True)
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p class="footer-text">DANA v2.4 • Powered by Claude AI</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
