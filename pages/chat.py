import streamlit as st
import uuid
import time
import json
from datetime import datetime
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from workflows.langraph_workflow import HealthcareFinanceWorkflow
from langgraph.types import Command
import pandas as pd
import html
import hashlib
import threading
from typing import Dict, Any
import os

def get_workflow_for_session(session_id: str):
    """Get or create workflow instance for current session with proper user isolation"""
    
    # Store in session state instead of shared cache
    workflow_key = f"workflow_instance_{session_id}"
    
    if workflow_key not in st.session_state:
        print(f"🔧 Creating new workflow for session: {session_id}")
        try:
            db_client = DatabricksClient()
            workflow = HealthcareFinanceWorkflow(db_client)
            st.session_state[workflow_key] = workflow
            print(f"✅ Workflow created for session: {session_id}")
        except Exception as e:
            print(f"❌ Failed to create workflow for session {session_id}: {str(e)}")
            return None
    else:
        print(f"♻️ Using existing workflow for session: {session_id}")
    
    return st.session_state[workflow_key]

def initialize_session_state():
    """Initialize session state with unique identifiers and proper isolation"""
    
    # Enhanced session ID generation for Azure isolation
    if 'session_id' not in st.session_state:
        import os
        
        # Get Azure-specific identifiers
        azure_instance_id = os.environ.get('WEBSITE_INSTANCE_ID', f'local_{os.getpid()}')
        website_site_name = os.environ.get('WEBSITE_SITE_NAME', 'local')
        
        # Create truly unique session ID
        timestamp = str(time.time())
        random_component = str(uuid.uuid4())
        
        session_data = f"{website_site_name}_{azure_instance_id}_{timestamp}_{random_component}"
        session_hash = hashlib.md5(session_data.encode()).hexdigest()
        st.session_state.session_id = f"session_{session_hash[:16]}"
        
        print(f"🆔 Created session ID: {st.session_state.session_id}")
        print(f"🏗️ Azure Site: {website_site_name}, Instance: {azure_instance_id}")
    
    # Initialize session-specific variables
    session_prefix = st.session_state.session_id
    
    session_keys = [
        f'messages_{session_prefix}',
        f'processing_{session_prefix}',
        f'workflow_started_{session_prefix}',
        f'current_followup_questions_{session_prefix}',
        f'button_clicked_{session_prefix}',
        f'click_counter_{session_prefix}',
        f'last_clicked_question_{session_prefix}',
        f'workflow_instance_{session_prefix}',
        f'conversation_thread_id_{session_prefix}'  # Add persistent thread ID
    ]
    
    for key in session_keys:
        if key not in st.session_state:
            if 'messages' in key:
                st.session_state[key] = []
            elif 'processing' in key or 'workflow_started' in key or 'button_clicked' in key:
                st.session_state[key] = False
            elif 'questions' in key:
                st.session_state[key] = []
            elif 'counter' in key:
                st.session_state[key] = 0
            elif 'thread_id' in key:
                # Create persistent thread ID for this user's conversation
                azure_instance = os.environ.get('WEBSITE_INSTANCE_ID', 'local')
                st.session_state[key] = f"conv_{azure_instance}_{session_prefix}_{int(time.time())}"
            else:
                st.session_state[key] = None
    
    # Create convenience properties
    st.session_state.messages = st.session_state[f'messages_{session_prefix}']
    st.session_state.processing = st.session_state[f'processing_{session_prefix}']
    st.session_state.workflow_started = st.session_state[f'workflow_started_{session_prefix}']
    st.session_state.current_followup_questions = st.session_state[f'current_followup_questions_{session_prefix}']
    st.session_state.button_clicked = st.session_state[f'button_clicked_{session_prefix}']
    st.session_state.click_counter = st.session_state[f'click_counter_{session_prefix}']
    st.session_state.last_clicked_question = st.session_state[f'last_clicked_question_{session_prefix}']
    st.session_state.conversation_thread_id = st.session_state[f'conversation_thread_id_{session_prefix}']

# Page Config
st.set_page_config(
    page_title="Healthcare Finance Assistant",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Set sidebar width via CSS
st.markdown(
    """
    <style>
        [data-testid="stSidebar"] {
            min-width: 220px;
            max-width: 220px;
            width: 220px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# Custom CSS for chat interface
st.markdown("""
<style>
    /* Remove default Streamlit padding and margins */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        padding-left: 1rem;
        padding-right: 1rem;
        max-width: 100%;
    }
    
    /* Hide Streamlit header and footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Chat container styling */
    .chat-container {
        height: calc(100vh - 150px);
        overflow-y: auto;
        padding: 1rem;
        margin-bottom: 100px;
    }
    
    /* Chat input container - Fixed at bottom */
    .chat-input-container {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        background-color: white;
        border-top: 1px solid #e0e0e0;
        padding: 1rem;
        z-index: 1000;
        box-shadow: 0 -2px 10px rgba(0,0,0,0.1);
    }
    
    /* User message styling - Left aligned, full width */
    .user-message {
        display: flex;
        justify-content: flex-start;
        margin: 1rem 0;
    }
    
    .user-message-content {
        background-color: #007bff;
        color: white;
        padding: 12px 16px;
        border-radius: 18px;
        max-width: 95%;
        word-wrap: break-word;
    }
    
    /* Assistant message styling - Left aligned, full width */
    .assistant-message {
        display: flex;
        justify-content: flex-start;
        margin: 1rem 0;
    }
    .assistant-message-content {
        background-color: #f1f3f4;
        color: #333;
        padding: 12px 16px;
        border-radius: 18px;
        max-width: 95%;
        word-wrap: break-word;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
        font-size: 15px;
        line-height: 1.6;
        font-style: normal !important;
        font-weight: normal !important;
    }
    
    /* HTML headers within assistant messages */
    .assistant-message-content h1,
    .assistant-message-content h2,
    .assistant-message-content h3 {
        font-family: inherit;
        font-weight: 700;
        color: #1a1a1a;
        margin: 0.8em 0 0.4em 0;
    }
    
    .assistant-message-content h1 {
        font-size: 1.4em;
    }
    
    .assistant-message-content h2 {
        font-size: 1.3em;
    }
    
    .assistant-message-content h3 {
        font-size: 1.2em;
    }
    
    /* HTML paragraph styling */
    .assistant-message-content p {
        margin: 0.8em 0;
        color: #333;
        font-family: inherit;
        font-size: inherit;
        line-height: inherit;
    }
    
    /* HTML strong/bold styling - Make it more prominent */
    .assistant-message-content strong {
        font-weight: 700;
        color: #1a1a1a;
        font-family: inherit;
    }
    
    /* HTML emphasis/italic styling */
    .assistant-message-content em {
        font-style: italic;
        color: #444;
        font-family: inherit;
    }
    
    /* HTML line breaks */
    .assistant-message-content br {
        line-height: 1.8;
    }

    .followup-container {
        margin: 1rem 0;
        padding: 1rem;
        background-color: #f8f9fa;
        border-radius: 12px;
        border-left: 4px solid #007bff;
    }
    
    .followup-header {
        font-weight: 600;
        color: #333;
        margin-bottom: 0.8rem;
        font-size: 16px;
    }
    
    .followup-button {
        display: block;
        width: 100%;
        margin: 0.5rem 0;
        padding: 12px 16px;
        background-color: white;
        border: 2px solid #007bff;
        border-radius: 8px;
        color: #007bff;
        text-align: left;
        cursor: pointer;
        transition: all 0.2s ease;
        font-size: 14px;
        line-height: 1.4;
    }
    
    .followup-button:hover {
        background-color: #007bff;
        color: white;
        box-shadow: 0 2px 8px rgba(0,123,255,0.2);
    }
    
    .followup-button:active {
        transform: translateY(1px);
    }
    
    /* Rotating spinner with message */
    .spinner-container {
        display: flex;
        align-items: center;
        justify-content: flex-start;
        margin: 1rem 0;
    }
    .spinner {
        width: 20px;
        height: 20px;
        border: 2px solid #f3f3f3;
        border-top: 2px solid #007bff;
        border-radius: 50%;
        animation: spin 1s linear infinite;
        margin-left: 16px;
        margin-right: 10px;
    }
    
    .spinner-message {
        color: #666;
        font-size: 14px;
        font-style: italic;
    }
    
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    /* Welcome message */
    .welcome-message {
        text-align: center;
        color: #666;
        margin: 2rem 0;
        font-size: 1.1rem;
    }
            
    .followup-buttons-container {
        display: flex !important;
        flex-wrap: wrap !important;
        gap: 8px !important;
        align-items: center !important;
        justify-content: flex-start !important;
        max-width: 95% !important;
        margin: 0.5rem 0 !important;
        padding: 0 !important;
    }

    /* Much smaller, horizontal buttons */
    .stButton > button {
        background-color: white !important;
        color: #007bff !important;
        border: 1px solid #007bff !important;
        border-radius: 4px !important;
        padding: 4px 8px !important;
        margin: 0 !important;
        width: auto !important;
        text-align: center !important;
        font-size: 11px !important;
        line-height: 1.1 !important;
        transition: all 0.2s ease !important;
        min-height: 24px !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        max-width: 180px !important;
        flex-shrink: 0 !important;
    }

    .stButton > button:hover {
        background-color: #007bff !important;
        color: white !important;
        box-shadow: 0 1px 3px rgba(0,123,255,0.2) !important;
        transform: translateY(-0.5px) !important;
    }

    .stButton > button:focus {
        background-color: #007bff !important;
        color: white !important;
        box-shadow: 0 0 0 2px rgba(0,123,255,0.25) !important;
        outline: none !important;
    }

    /* Make button containers flexible */
    .element-container .stButton {
        margin: 0 !important;
        flex: 0 0 auto !important;
    }

    /* Override any conflicting Streamlit styles */
    .stButton button[kind="secondary"] {
        background-color: white !important;
        color: #007bff !important;
        border-color: #007bff !important;
    }

    .sidebar .sidebar-content {
        padding: 2rem 1rem;
    }
    
    .sidebar-title {
        color: #333;
        font-size: 1.2rem;
        font-weight: 600;
        margin-bottom: 1rem;
        text-align: center;
    }
    
    .back-button {
        margin: 1rem 0;
    }
    
    .stButton > button.back-btn {
        background: linear-gradient(45deg, #667eea, #764ba2) !important;
        color: white !important;
        border: none !important;
        border-radius: 25px !important;
        padding: 0.5rem 1.5rem !important;
        font-size: 0.9rem !important;
        font-weight: 500 !important;
        width: 100% !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton > button.back-btn:hover {
        background: linear-gradient(45deg, #764ba2, #667eea) !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 5px 15px rgba(102, 126, 234, 0.3) !important;
    }          
</style>
""", unsafe_allow_html=True)

def format_query_results_as_table(query_results):
    """Format query results and return both the dataframe and row count for proper display"""
    try:
        if not query_results or len(query_results) == 0:
            return None, "No data returned from query."
        
        print(f"🔍 Raw query results: {query_results}")
        
        # Convert to DataFrame
        df = pd.DataFrame(query_results)
        print(f"📊 DataFrame created with shape: {df.shape}")
        print(f"📊 DataFrame columns: {df.columns.tolist()}")
        
        def format_value(val):
            # Handle ISO date strings
            if isinstance(val, str) and 'T' in val and val.endswith('Z'):
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
                    return dt.strftime('%Y-%m-%d')
                except Exception:
                    return val
            # Handle year values (don't format as decimals)
            elif isinstance(val, str) and val.isdigit() and len(val) == 4:
                return val  # Keep year as is
            # Handle month values (1-12, don't add decimals)
            elif isinstance(val, str) and val.replace('.', '').isdigit():
                try:
                    numeric_val = float(val)
                    if 1 <= numeric_val <= 12 and '.' not in val:  # Month value
                        return str(int(numeric_val))
                    elif numeric_val > 1000:  # Large numbers like revenue
                        return f"{numeric_val:,.1f}"
                    else:
                        return f"{numeric_val:.1f}"
                except Exception:
                    return val
            # Handle large numeric strings (like your revenue values)
            elif isinstance(val, str) and val.replace('.', '').replace('-', '').isdigit():
                try:
                    numeric_val = float(val)
                    if numeric_val > 1000:  # Format large numbers with commas
                        return f"{numeric_val:,.1f}"
                    else:
                        return f"{numeric_val:.1f}"
                except Exception:
                    return val
            # Handle already formatted dollar amounts (with commas)
            elif isinstance(val, str) and val.startswith('$') and ',' in val:
                return val
            # Handle already formatted strings (with commas)
            elif isinstance(val, str) and ',' in val:
                return val
            # Handle scientific notation (like 1.378677675E9)
            elif isinstance(val, (int, float)) or (isinstance(val, str) and ('E' in val.upper() or 'e' in val)):
                try:
                    numeric_val = float(val)
                    if abs(numeric_val) >= 1000:  # Large numbers
                        return f"{numeric_val:,.1f}"
                    else:
                        return f"{numeric_val:.1f}"
                except Exception:
                    return str(val)
            elif pd.api.types.is_numeric_dtype(type(val)):
                try:
                    if pd.notna(val):
                        if abs(float(val)) >= 1000:
                            return f"{float(val):,.1f}"
                        else:
                            return f"{float(val):.1f}"
                    else:
                        return ""
                except Exception:
                    return str(val)
            else:
                return val
        
        # Apply formatting to all columns
        for col in df.columns:
            df[col] = df[col].apply(format_value)
        
        # Convert column names to title case for display
        df.columns = [col.replace('_', ' ').title() for col in df.columns]
        
        print(f"✅ Formatted DataFrame:\n{df}")
        
        # Return both the dataframe and row count message
        row_count_msg = f"*Showing all {len(df)} row{'s' if len(df) != 1 else ''}*"
        return df, row_count_msg
        
    except Exception as e:
        print(f"❌ Error formatting table: {str(e)}")
        return None, f"Error displaying table: {str(e)}"

def convert_text_to_safe_html(text):
    """Convert text to HTML while preserving intentional formatting"""
    if not text:
        return ""
    
    # Fix number-word combinations that cause italics (like "1.65billion")
    import re
    text = re.sub(r'(\d+\.?\d*)(billion|million|thousand|trillion)', r'\1 \2', text)
    
    # Convert markdown formatting to HTML
    # Bold: **text** -> <strong>text</strong>
    text = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', text)
    
    # Italic: *text* -> <em>text</em> (but be careful with asterisks in numbers)
    text = re.sub(r'(?<!\d)\*([^*\d]+)\*(?!\d)', r'<em>\1</em>', text)
    
    # Headers: ### text -> <h3>text</h3>
    text = re.sub(r'^### (.+)$', r'<h3>\1</h3>', text, flags=re.MULTILINE)
    text = re.sub(r'^## (.+)$', r'<h2>\1</h2>', text, flags=re.MULTILINE)
    text = re.sub(r'^# (.+)$', r'<h1>\1</h1>', text, flags=re.MULTILINE)
    
    # Convert line breaks to HTML
    # Double line breaks -> paragraph breaks
    paragraphs = text.split('\n\n')
    html_paragraphs = []
    
    for paragraph in paragraphs:
        if paragraph.strip():
            # Clean up single line breaks within paragraphs
            cleaned_paragraph = paragraph.strip().replace('\n', '<br>')
            # Don't wrap if it's already a header
            if not (cleaned_paragraph.startswith('<h') or cleaned_paragraph.startswith('<strong>')):
                html_paragraphs.append(f'<p>{cleaned_paragraph}</p>')
            else:
                html_paragraphs.append(cleaned_paragraph)
    
    return ''.join(html_paragraphs)

def extract_response_content(step_data):
    """Extract meaningful response content from workflow step with better debugging"""
    
    print(f"🔍 EXTRACT_RESPONSE_CONTENT - Raw step_data: {step_data}")
    
    if isinstance(step_data, dict):
        if 'step' in step_data:
            step_content = step_data['step']
        else:
            step_content = step_data
        
        print(f"🔍 Step content keys: {list(step_content.keys())}")
        
        for node_name, node_state in step_content.items():
            if node_name == '__start__':
                continue
            
            print(f"📊 Processing node: {node_name}")
            print(f"📊 Node state keys: {list(node_state.keys()) if isinstance(node_state, dict) else 'Not a dict'}")
            print(f"📊 Node state content: {node_state}")
            
            # Handle navigation controller domain clarification
            if node_name == 'navigation_controller':
                domain_followup = node_state.get('domain_followup_question')
                requires_clarification = node_state.get('requires_domain_clarification', False)
                
                print(f"🔍 Navigation - requires_clarification: {requires_clarification}")
                print(f"🔍 Navigation - domain_followup: {domain_followup}")
                
                if requires_clarification and domain_followup:
                    formatted_question = convert_text_to_safe_html(domain_followup)
                    return {
                        'type': 'text',
                        'content': formatted_question,
                        'immediate_render': True
                    }
                else:
                    print("🚫 Skipping navigation_controller output - no clarification needed")
                    continue
            
            elif node_name == 'router_agent':
                print("🚫 Skipping router_agent output - hidden from UI")
                continue
            
            elif node_name == 'sql_generator_agent':
                sql_query = node_state.get('sql_query', '')
                query_results = node_state.get('query_results', [])
                narrative_response = node_state.get('narrative_response', '')
                
                print(f"🔍 SQL Generator - Query: {bool(sql_query)}")
                print(f"🔍 SQL Generator - Query content: {sql_query[:100]}..." if sql_query else "No SQL")
                print(f"🔍 SQL Generator - Results count: {len(query_results) if query_results else 0}")
                print(f"🔍 SQL Generator - Results sample: {query_results[:2] if query_results else 'No results'}")
                print(f"🔍 SQL Generator - Narrative: {bool(narrative_response)}")
                print(f"🔍 SQL Generator - Narrative content: {narrative_response[:100]}..." if narrative_response else "No narrative")
                
                response_parts = []
                table_df = None
                
                # Process table data FIRST (before narrative)
                if query_results and isinstance(query_results, list) and len(query_results) > 0:
                    print(f"🔍 Processing {len(query_results)} query results")
                    table_df, table_info = format_query_results_as_table(query_results)
                    if table_df is not None:
                        print(f"✅ Table created successfully with {len(table_df)} rows")
                    else:
                        print(f"❌ Failed to create table: {table_info}")
                        response_parts.append(f"❌ <strong>Table Error:</strong> {table_info}<br><br>")
                
                # Narrative response AFTER table
                if narrative_response:
                    formatted_narrative = convert_text_to_safe_html(narrative_response)
                    response_parts.append(f"📈 <strong>Analysis:</strong><br><br>{formatted_narrative}")
                
                # Return with table data for immediate rendering
                if table_df is not None or response_parts:
                    result = {
                        'type': 'sql_with_table',
                        'content': "".join(response_parts),
                        'sql_query': sql_query,
                        'dataframe': table_df,
                        'immediate_render': True
                    }
                    print(f"✅ Returning SQL response - Table: {table_df is not None}, Content: {bool(response_parts)}")
                    return result
                elif sql_query:
                    return {
                        'type': 'text',
                        'content': f"⚡ <strong>Generated SQL:</strong><br><br>{sql_query}",
                        'sql_query': sql_query,
                        'immediate_render': True
                    }
                else:
                    print("❌ SQL Generator - No SQL query, results, or narrative found")
            
            # Follow-up question handler
            elif node_name == 'followup_question_agent':
                print(f"🔍 Followup Question Agent - Processing generated questions")
                
                followup_questions = node_state.get('followup_questions', [])
                success = node_state.get('followup_generation_success', False)
                
                print(f"🔍 Follow-up questions found: {followup_questions}")
                print(f"🔍 Follow-up questions count: {len(followup_questions)}")
                print(f"🔍 Generation success: {success}")
                
                if followup_questions and len(followup_questions) > 0:
                    print("🎯 QUESTIONS FOUND - STORING AND DISPLAYING FOLLOWUP BUTTONS")
                    
                    # Store in session state for persistence
                    session_prefix = st.session_state.session_id
                    st.session_state[f'current_followup_questions_{session_prefix}'] = followup_questions
                    st.session_state.current_followup_questions = followup_questions
                    
                    return {
                        'type': 'followup_questions',
                        'content': "💡 <strong>Would you like to explore further? Here are some suggested follow-up questions:</strong>",
                        'followup_questions': followup_questions,
                        'immediate_render': True
                    }
                else:
                    print("⚠️ No follow-up questions found - skipping display")
                    return None
                    
            # Root cause node
            elif node_name == 'root_cause_agent':
                print(f"🔍 Root Cause Agent - Processing detailed output")
                
                consolidated_query_details = node_state.get('consolidated_query_details', [])
                narrative_response = node_state.get('narrative_response', '')
                
                print(f"🔍 Consolidated query details count: {len(consolidated_query_details)}")
                print(f"🔍 Narrative response available: {bool(narrative_response)}")
                
                response_parts = []
                all_dataframes = []
                
                # Process each consolidated query detail individually
                if consolidated_query_details and len(consolidated_query_details) > 0:
                    
                    for i, query_detail in enumerate(consolidated_query_details, 1):
                        purpose = query_detail.get('purpose', 'Analysis')
                        sql = query_detail.get('sql', '')
                        sql_results = query_detail.get('sql_results', [])
                        insight_text = query_detail.get('insight_text', '')
                        success = query_detail.get('success', False)
                        row_count = query_detail.get('row_count', 0)
                        
                        print(f"🔍 Processing query {i}: Success: {success}, Rows: {row_count}")
                        
                        # Only process successful queries (ignore errors)
                        if success and row_count > 0:
                            if sql_results and isinstance(sql_results, list) and len(sql_results) > 0:
                                table_df, table_info = format_query_results_as_table(sql_results)
                                if table_df is not None:
                                    formatted_insight = ""
                                    if insight_text:
                                        formatted_insight = convert_text_to_safe_html(insight_text)
                                    
                                    all_dataframes.append({
                                        'title': f"{purpose}",
                                        'dataframe': table_df,
                                        'sql': sql,
                                        'insight': formatted_insight,
                                        'success': True
                                    })
                                    print(f"✅ Added successful analysis: {purpose}")
                                else:
                                    print(f"❌ Failed to create table for {purpose}: {table_info}")
                            else:
                                print(f"❌ No SQL results for {purpose}")
                        else:
                            print(f"❌ Skipping failed/empty query {i}: {purpose} - Success: {success}, Rows: {row_count}")
                
                # Add Consolidated Narrative Response at the end
                if narrative_response:
                    formatted_narrative = convert_text_to_safe_html(narrative_response)
                    response_parts.append(f"<strong>🎯 Consolidated Analysis Summary:</strong><br><br>{formatted_narrative}")
                
                # Return structured response with multiple dataframes
                if all_dataframes or response_parts:
                    return {
                        'type': 'root_cause_detailed',
                        'content': "".join(response_parts),
                        'dataframes': all_dataframes,
                        'immediate_render': True
                    }
                
                # Fallback if no details
                elif node_state.get('analysis_complete'):
                    return {
                        'type': 'text',
                        'content': "✅ <strong>Root cause analysis completed successfully!</strong><br><br>Analysis has been processed across multiple data sources.",
                        'immediate_render': True
                    }
            
            # FALLBACK FOR ANY OTHER NODE - This is crucial!
            else:
                print(f"🔍 Processing fallback node: {node_name}")
                if isinstance(node_state, dict) and node_state:
                    # Look for common response fields
                    for key in ['narrative_response', 'response', 'result', 'analysis', 'content', 'output']:
                        if key in node_state and node_state[key]:
                            formatted_content = convert_text_to_safe_html(str(node_state[key]))
                            print(f"✅ Found content in {key}: {formatted_content[:100]}...")
                            return {
                                'type': 'text',
                                'content': f"🤖 <strong>{node_name.replace('_', ' ').title()}:</strong><br><br>{formatted_content}",
                                'immediate_render': True
                            }
                    
                    # If no standard fields, show entire node state
                    print(f"⚠️ No standard response fields found in {node_name}, showing full state")
                    content = json.dumps(node_state, indent=2) if isinstance(node_state, dict) else str(node_state)
                    return {
                        'type': 'text',
                        'content': f"🤖 <strong>{node_name.replace('_', ' ').title()}:</strong><br><br><pre>{content}</pre>",
                        'immediate_render': True
                    }
    
    print("❌ No response content extracted from step_data")
    return None

def save_complete_ui_conversation_history(all_response_data, session_id):
    """Save COMPLETE conversation to UI chat history - everything users see gets preserved"""
    
    session_prefix = session_id
    
    print(f"💾 Saving complete UI history: {len(all_response_data)} responses")
    
    for i, response_data in enumerate(all_response_data):
        response_type = response_data.get('type', 'unknown')
        print(f"💾 Processing response {i+1}: {response_type}")
        
        # 1. ✅ SAVE FOLLOW-UP QUESTIONS AS VISIBLE CHAT MESSAGES
        if response_type == 'followup_questions':
            print(f"💾 Saving follow-up questions to chat history")
            
            # Save the header message
            st.session_state[f'messages_{session_prefix}'].append({
                'type': 'assistant',
                'content': response_data.get('content', ''),
                'response_type': 'followup_header',
                'timestamp': datetime.now()
            })
            
            # Save each follow-up question as a separate message for UI display
            followup_questions = response_data.get('followup_questions', [])
            if followup_questions:
                questions_html = "<div style='margin: 0.5rem 0;'>"
                for idx, question in enumerate(followup_questions, 1):
                    questions_html += f"<div style='margin: 0.3rem 0; padding: 0.5rem; background-color: #f0f8ff; border-left: 3px solid #007bff; border-radius: 4px;'>💡 <strong>Suggested:</strong> {question}</div>"
                questions_html += "</div>"
                
                st.session_state[f'messages_{session_prefix}'].append({
                    'type': 'assistant',
                    'content': questions_html,
                    'response_type': 'followup_suggestions',
                    'followup_questions': followup_questions,  # Keep original for reference
                    'timestamp': datetime.now()
                })
                print(f"💾 Saved {len(followup_questions)} follow-up questions to UI history")
        
        # 2. ✅ SAVE NAVIGATION CLARIFICATIONS AS CHAT MESSAGES  
        elif response_type == 'text':
            print(f"💾 Saving navigation/clarification to chat history")
            st.session_state[f'messages_{session_prefix}'].append({
                'type': 'assistant',
                'content': response_data.get('content', ''),
                'response_type': 'clarification',
                'timestamp': datetime.now()
            })
        
        # 3. ✅ SAVE SQL RESPONSES WITH COMPLETE CONTEXT
        elif response_type == 'sql_with_table':
            print(f"💾 Saving SQL analysis to chat history")
            st.session_state[f'messages_{session_prefix}'].append({
                'type': 'assistant',
                'content': response_data.get('content', ''),
                'dataframe': response_data.get('dataframe'),
                'sql_query': response_data.get('sql_query'),
                'response_type': 'sql_analysis',
                'timestamp': datetime.now()
            })
        
        # 4. ✅ SAVE ALL ROOT CAUSE ANALYSES INDIVIDUALLY
        elif response_type == 'root_cause_detailed':
            print(f"💾 Saving detailed root cause analysis to chat history")
            
            # Save each individual analysis as a separate chat message
            dataframes_info = response_data.get('dataframes', [])
            print(f"💾 Processing {len(dataframes_info)} individual analyses")
            
            for df_idx, df_info in enumerate(dataframes_info):
                if df_info.get('success', False):
                    analysis_title = df_info.get('title', f'Analysis {df_idx + 1}')
                    analysis_content = df_info.get('insight', '')
                    
                    # Create a comprehensive analysis message
                    analysis_html = f"""
                    <div style="border: 1px solid #e0e0e0; border-radius: 8px; padding: 1rem; margin: 0.5rem 0; background-color: #fafafa;">
                        <h4 style="color: #007bff; margin: 0 0 0.5rem 0;">📊 {analysis_title}</h4>
                        {analysis_content if analysis_content else '<em>Data analysis completed - see table above</em>'}
                    </div>
                    """
                    
                    st.session_state[f'messages_{session_prefix}'].append({
                        'type': 'assistant',
                        'content': analysis_html,
                        'dataframe': df_info.get('dataframe'),
                        'sql_query': df_info.get('sql'),
                        'response_type': 'detailed_analysis',
                        'analysis_title': analysis_title,
                        'analysis_index': df_idx + 1,
                        'timestamp': datetime.now()
                    })
                    print(f"💾 Saved detailed analysis {df_idx + 1}: {analysis_title}")
            
            # Save consolidated summary if available
            if response_data.get('content'):
                st.session_state[f'messages_{session_prefix}'].append({
                    'type': 'assistant',
                    'content': response_data.get('content'),
                    'response_type': 'consolidated_summary',
                    'timestamp': datetime.now()
                })
                print(f"💾 Saved consolidated summary")
    
    # Update convenience property
    st.session_state.messages = st.session_state[f'messages_{session_prefix}']
    print(f"✅ Complete UI conversation history saved: {len(st.session_state.messages)} total messages")

def render_enhanced_chat_message(message):
    """Enhanced chat message rendering for all conversation types"""
    
    message_type = message['type']
    response_type = message.get('response_type', 'general')
    
    print(f"🎨 Rendering message: {message_type} / {response_type}")
    
    if message_type == 'user':
        st.markdown(f"""
        <div class="user-message">
            <div class="user-message-content">
                {message['content']}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    elif message_type == 'assistant':
        # Handle different types of assistant messages
        
        # Show analysis title for detailed analyses
        if response_type == 'detailed_analysis' and message.get('analysis_title'):
            st.markdown(f"### 📊 {message['analysis_title']}")
        
        # Show SQL expander for any message with SQL
        if message.get('sql_query'):
            with st.expander("🔍 View SQL Query", expanded=False):
                st.code(message['sql_query'], language='sql')
        
        # Show dataframe for any message with data
        if message.get('dataframe') is not None:
            st.dataframe(message['dataframe'], use_container_width=True)
            print(f"🎨 Displayed dataframe with {len(message['dataframe'])} rows")
        
        # Show content with appropriate styling based on response type
        if message.get('content'):
            
            # Special styling for follow-up suggestions
            if response_type == 'followup_suggestions':
                st.markdown(f"""
                <div class="assistant-message">
                    <div class="assistant-message-content">
                        {message['content']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Special styling for clarifications
            elif response_type == 'clarification':
                st.markdown(f"""
                <div class="assistant-message" style="border-left: 4px solid #ffa500;">
                    <div class="assistant-message-content" style="background-color: #fff8dc;">
                        🤔 <strong>Clarification:</strong><br><br>{message['content']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Special styling for detailed analyses
            elif response_type == 'detailed_analysis':
                st.markdown(f"""
                <div class="assistant-message" style="border-left: 4px solid #28a745;">
                    <div class="assistant-message-content">
                        {message['content']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Special styling for consolidated summaries
            elif response_type == 'consolidated_summary':
                st.markdown(f"""
                <div class="assistant-message" style="border-left: 4px solid #6f42c1;">
                    <div class="assistant-message-content" style="background-color: #f8f9ff;">
                        🎯 <strong>Summary:</strong><br><br>{message['content']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Default styling for other types
            else:
                st.markdown(f"""
                <div class="assistant-message">
                    <div class="assistant-message-content">
                        {message['content']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        # Add subtle timestamp and type indicator
        if response_type != 'general':
            timestamp = message.get('timestamp', datetime.now())
            if hasattr(timestamp, 'strftime'):
                time_str = timestamp.strftime('%H:%M')
            else:
                time_str = str(timestamp)[:5] if isinstance(timestamp, str) else "now"
            
            st.markdown(f"""
            <div style="text-align: right; color: #888; font-size: 0.7rem; margin-top: -0.5rem; margin-bottom: 1rem;">
                {response_type.replace('_', ' ').title()} • {time_str}
            </div>
            """, unsafe_allow_html=True)

def render_conversation_with_full_history():
    """Render complete conversation history with all preserved context"""
    
    print(f"🎨 Rendering conversation with {len(st.session_state.messages)} messages")
    
    if not st.session_state.messages:
        st.markdown("""
        <div class="welcome-message">
            👋 Welcome! I'm your Healthcare Finance Assistant.<br>
            Ask me about claims and Ledger related healthcare finance questions.<br>
            <em>I'll remember our complete conversation history including all analyses and suggestions.</em>
        </div>
        """, unsafe_allow_html=True)
        return
    
    # Group messages for better display
    message_groups = []
    current_group = []
    
    for message in st.session_state.messages:
        if message['type'] == 'user':
            # Start new group with user message
            if current_group:
                message_groups.append(current_group)
            current_group = [message]
        else:
            # Add assistant message to current group
            current_group.append(message)
    
    # Add the last group
    if current_group:
        message_groups.append(current_group)
    
    # Render each group
    for group_idx, group in enumerate(message_groups):
        print(f"🎨 Rendering message group {group_idx + 1} with {len(group)} messages")
        
        for message in group:
            render_enhanced_chat_message(message)
        
        # Add subtle separator between conversation turns
        if group_idx < len(message_groups) - 1:
            st.markdown("<hr style='border: none; border-top: 1px solid #eee; margin: 2rem 0;'>", unsafe_allow_html=True)

def render_sql_response(response_data):
    """Render SQL response with table and narrative"""
    
    # 1. SQL expander FIRST
    if response_data.get('sql_query'):
        with st.expander("🔍 View SQL Query", expanded=False):
            st.code(response_data['sql_query'], language='sql')
    
    # 2. TABLE SECOND (before narrative)
    if response_data.get('dataframe') is not None:
        st.dataframe(response_data['dataframe'], use_container_width=True)
        print(f"📊 Displayed table with {len(response_data['dataframe'])} rows")
    
    # 3. NARRATIVE LAST
    if response_data.get('content'):
        st.markdown(f"""
        <div class="assistant-message">
            <div class="assistant-message-content">
                {response_data['content']}
            </div>
        </div>
        """, unsafe_allow_html=True)

def render_root_cause_response(response_data):
    """Render root cause analysis response with multiple dataframes"""
    
    print(f"🔥 RENDERING ROOT CAUSE DETAILED ANALYSIS")
    
    # Display each analysis one by one in the correct order
    if response_data.get('dataframes'):
        for idx, df_info in enumerate(response_data['dataframes']):
            print(f"🔥 Processing dataframe {idx+1}: {df_info.get('title', 'Unknown')}")
            
            # Only show successful analyses (ignore errors)
            if df_info.get('success', False):
                # 1. PURPOSE
                st.markdown(f"**📊 {df_info['title']}**")
                
                # 2. SQL (in expander)
                if df_info.get('sql'):
                    with st.expander("🔍 View SQL Query", expanded=False):
                        st.code(df_info['sql'], language='sql')
                
                # 3. TABLE OUTPUT
                if df_info.get('dataframe') is not None:
                    st.dataframe(df_info['dataframe'], use_container_width=True)
                    print(f"📊 Displayed table for {df_info['title']} with {len(df_info['dataframe'])} rows")
                
                # 4. DETAILS/INSIGHT
                if df_info.get('insight'):
                    st.markdown(f"""
                    <div class="assistant-message">
                        <div class="assistant-message-content">
                            {df_info['insight']}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    print(f"⚠️ No insight available for {df_info['title']}")
                
                # Separator between analyses
                st.markdown("---")
            else:
                print(f"❌ Skipping unsuccessful analysis: {df_info.get('title', 'Unknown')}")
    else:
        print("⚠️ No dataframes found in root_cause_detailed response")
    
    # Display consolidated summary at the end
    if response_data.get('content'):
        st.markdown(f"""
        <div class="assistant-message">
            <div class="assistant-message-content">
                {response_data['content']}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        print("⚠️ No consolidated content found")

def render_persistent_followup_questions():
    """Render persistent follow-up questions if they exist"""
    
    if st.session_state.current_followup_questions and not st.session_state.processing:
        print(f"🔥 Rendering persistent follow-up questions: {len(st.session_state.current_followup_questions)}")
        
        st.markdown(f"""
        <div class="assistant-message">
            <div class="assistant-message-content">
                💡 <strong>Would you like to explore further? Here are some suggested follow-up questions:</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Create buttons for persistent questions
        st.markdown('<div class="followup-buttons-container" style="display: flex; flex-wrap: wrap; gap: 8px; align-items: center;">', unsafe_allow_html=True)
        
        for i, question in enumerate(st.session_state.current_followup_questions):
            button_key = f"persistent_btn_{st.session_state.session_id}_{i}_{abs(hash(question)) % 10000}"
            
            button_clicked = st.button(
                question,
                key=button_key,
                type="secondary",
                help="Click to explore this follow-up question"
            )
            
            if button_clicked and not st.session_state.processing:
                print(f"🔥 Persistent button clicked: {question}")
                
                # Clear followup questions and start new query
                session_prefix = st.session_state.session_id
                st.session_state[f'current_followup_questions_{session_prefix}'] = []
                st.session_state.current_followup_questions = []
                start_processing(question)
        
        st.markdown('</div>', unsafe_allow_html=True)

def get_current_node_name(step_data):
    """Extract current node name for display"""
    if isinstance(step_data, dict):
        if 'step' in step_data:
            step_content = step_data['step']
        else:
            step_content = step_data
        
        for node_name, node_state in step_content.items():
            if node_name != '__start__':
                readable_name = node_name.replace('_', ' ').title()
                return readable_name
    
    return "Processing"

def get_next_agent_from_state(step_data):
    """Extract next agent from actual workflow state"""
    if isinstance(step_data, dict):
        if 'step' in step_data:
            step_content = step_data['step']
        else:
            step_content = step_data
        
        # Look for next_agent in any node state
        for node_name, node_state in step_content.items():
            if node_name != '__start__' and isinstance(node_state, dict):
                next_agent = node_state.get('next_agent_disp', '')
                if next_agent:
                    readable_next = next_agent.replace('_', ' ').title()
                    print(f"🎯 Found next_agent: {next_agent} -> {readable_next}")
                    return readable_next
    
    return None

def update_spinner(spinner_placeholder, current_node, next_agent):
    """Update spinner with current and next agent information"""
    
    if next_agent:
        spinner_message = f"🤖 Completed {current_node}... Next: {next_agent}"
    else:
        spinner_message = f"🤖 Running {current_node}..."
    
    spinner_placeholder.markdown(f"""
    <div class="spinner-container">
        <div class="spinner"></div>
        <div class="spinner-message">{spinner_message}</div>
    </div>
    """, unsafe_allow_html=True)

def start_processing(user_query):
    """Start processing a new user query"""
    
    print(f"🚀 Starting processing for: {user_query}")
    
    session_prefix = st.session_state.session_id
    
    # Reset all button flags
    st.session_state[f'button_clicked_{session_prefix}'] = False
    st.session_state[f'last_clicked_question_{session_prefix}'] = None
    st.session_state[f'click_counter_{session_prefix}'] += 1
    
    st.session_state.button_clicked = False
    st.session_state.last_clicked_question = None
    st.session_state.click_counter += 1
    
    # Clear current followup questions when starting new workflow
    st.session_state[f'current_followup_questions_{session_prefix}'] = []
    st.session_state.current_followup_questions = []
    
    # Add user message to session-specific chat
    st.session_state[f'messages_{session_prefix}'].append({
        'type': 'user',
        'content': user_query,
        'timestamp': datetime.now()
    })
    
    # Update convenience property
    st.session_state.messages = st.session_state[f'messages_{session_prefix}']
    
    # Set processing state
    st.session_state[f'processing_{session_prefix}'] = True
    st.session_state[f'workflow_started_{session_prefix}'] = False
    
    st.session_state.processing = True
    st.session_state.current_query = user_query
    st.session_state.workflow_started = False
    
    # Force a rerun to show the user message and start processing
    st.rerun()

def handle_followup_click(question, session_id):
    """Handle follow-up question click with session isolation"""
    
    print(f"🔥 Session {session_id} - Follow-up button clicked: {question}")
    
    session_prefix = session_id
    
    # Update session-specific state
    st.session_state[f'click_counter_{session_prefix}'] += 1
    st.session_state[f'last_clicked_question_{session_prefix}'] = question
    st.session_state[f'button_clicked_{session_prefix}'] = True
    st.session_state[f'current_followup_questions_{session_prefix}'] = []
    
    # Update convenience properties
    st.session_state.click_counter += 1
    st.session_state.last_clicked_question = question
    st.session_state.button_clicked = True
    st.session_state.current_followup_questions = []
    
    # Add user message to session-specific history
    st.session_state[f'messages_{session_prefix}'].append({
        'type': 'user',
        'content': question,
        'timestamp': datetime.now()
    })
    
    # Update convenience property
    st.session_state.messages = st.session_state[f'messages_{session_prefix}']
    
    # Set processing state
    st.session_state[f'processing_{session_prefix}'] = True
    st.session_state[f'workflow_started_{session_prefix}'] = False
    st.session_state.processing = True
    st.session_state.workflow_started = False
    st.session_state.current_query = question
    
    print(f"🔥 Session {session_id} - Set processing=True, workflow_started=False")
    st.rerun()

def render_immediate_response(response_data, step_count, session_id):
    """Render immediate response with session context"""
    
    if response_data['type'] == 'followup_questions':
        print(f"🔥 Session {session_id} - Rendering follow-up questions")
        
        # Display header
        st.markdown(f"""
        <div class="assistant-message">
            <div class="assistant-message-content">
                {response_data['content']}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Create buttons with session-specific keys
        followup_questions = response_data.get('followup_questions', [])
        if followup_questions:
            st.markdown('<div class="followup-buttons-container" style="display: flex; flex-wrap: wrap; gap: 8px;">', unsafe_allow_html=True)
            
            for i, question in enumerate(followup_questions):
                # Create session-specific button key
                button_key = f"immediate_btn_{session_id}_{step_count}_{i}_{abs(hash(question)) % 10000}"
                
                button_clicked = st.button(
                    question,
                    key=button_key,
                    type="secondary",
                    help="Click to explore this follow-up question"
                )

                if button_clicked and not st.session_state.processing:
                    handle_followup_click(question, session_id)
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    elif response_data['type'] == 'sql_with_table':
        render_sql_response(response_data)
    
    elif response_data['type'] == 'root_cause_detailed':
        render_root_cause_response(response_data)
    
    # Handle text responses (like navigation controller)
    elif response_data['type'] == 'text':
        print(f"🔥 Session {session_id} - Rendering text response")
        
        # Display the text content
        st.markdown(f"""
        <div class="assistant-message">
            <div class="assistant-message-content">
                {response_data['content']}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Fallback for any other response types
    else:
        print(f"⚠️ Session {session_id} - Unknown response type: {response_data['type']}")
        if response_data.get('content'):
            st.markdown(f"""
            <div class="assistant-message">
                <div class="assistant-message-content">
                    {response_data['content']}
                </div>
            </div>
            """, unsafe_allow_html=True)

def execute_workflow_streaming_with_ui_preservation(workflow):
    """Execute workflow with complete UI preservation"""
    
    st.session_state.workflow_started = True
    session_id = st.session_state.session_id
    session_prefix = session_id
    st.session_state[f'workflow_started_{session_prefix}'] = True
    
    try:
        print(f"🚀 Starting workflow with UI preservation for: {st.session_state.current_query}")
        
        spinner_placeholder = st.empty()
        step_count = 0
        all_response_data = []
        current_node = "Starting"
        
        # Show initial spinner
        spinner_placeholder.markdown(f"""
        <div class="spinner-container">
            <div class="spinner"></div>
            <div class="spinner-message">🤖 {current_node}...</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Use persistent conversation thread ID
        conversation_thread_id = st.session_state.conversation_thread_id
        config = {"configurable": {"thread_id": conversation_thread_id}}
        
        # Build conversation history for LangGraph context
        conversation_history = []
        for msg in st.session_state.messages:
            if msg['type'] == 'user':
                conversation_history.append(msg['content'])
        
        initial_state = {
            'original_question': st.session_state.current_query,
            'current_question': st.session_state.current_query,
            'session_id': session_id,
            'user_id': f"streamlit_user_{session_id}",
            'user_questions_history': conversation_history,
            'errors': [],
            'session_context': {
                'app_instance_id': session_id,
                'execution_timestamp': datetime.now().isoformat(),
                'conversation_turn': len(conversation_history) + 1,
                'conversation_thread_id': conversation_thread_id
            }
        }
        
        print(f"🔧 Workflow context: {len(conversation_history)} previous questions")
        
        try:
            # Get existing conversation state
            try:
                existing_state = workflow.app.get_state(config)
                if existing_state and existing_state.values:
                    merged_state = {**existing_state.values, **initial_state}
                    print(f"📋 Merged with existing conversation state")
                else:
                    merged_state = initial_state
                    print(f"📋 Using fresh conversation state")
            except Exception as state_error:
                print(f"⚠️ Could not retrieve conversation state: {state_error}")
                merged_state = initial_state
            
            # Execute workflow with streaming
            for step_data in workflow.app.stream(merged_state, config=config):
                step_count += 1
                print(f"📊 Step {step_count} - Processing...")
                
                current_node = get_current_node_name(step_data)
                next_agent = get_next_agent_from_state(step_data)
                
                # Extract response content
                response_data = extract_response_content(step_data)
                
                if response_data:
                    all_response_data.append(response_data)
                    print(f"📊 Response {len(all_response_data)}: {response_data.get('type')}")
                    
                    # Show immediate responses
                    if response_data.get('immediate_render', False):
                        spinner_placeholder.empty()
                        immediate_container = st.container()
                        
                        with immediate_container:
                            render_immediate_response(response_data, step_count, session_id)
                        
                        if next_agent:
                            spinner_placeholder = st.empty()
                            update_spinner(spinner_placeholder, current_node, next_agent)
                else:
                    update_spinner(spinner_placeholder, current_node, next_agent)
                
                time.sleep(0.3)
                
                if st.session_state.session_id != session_id:
                    break
            
            print(f"🏁 Workflow completed: {step_count} steps, {len(all_response_data)} responses")
                    
        except Exception as workflow_error:
            print(f"❌ Workflow execution error: {str(workflow_error)}")
            raise workflow_error
        
        # Clear spinner
        spinner_placeholder.empty()
        
        # ✅ SAVE COMPLETE UI CONVERSATION HISTORY
        print(f"💾 Saving complete UI conversation history...")
        save_complete_ui_conversation_history(all_response_data, session_id)
        
        print(f"✅ Workflow completed with complete UI preservation")
                
    except Exception as e:
        print(f"❌ Error in workflow execution: {str(e)}")
        
        # Save error to chat history too
        st.session_state[f'messages_{session_prefix}'].append({
            'type': 'assistant',
            'content': f"❌ I encountered an error processing your request: {str(e)}",
            'response_type': 'error',
            'timestamp': datetime.now()
        })
        st.session_state.messages = st.session_state[f'messages_{session_prefix}']
    
    finally:
        # Reset processing flags
        st.session_state[f'processing_{session_prefix}'] = False
        st.session_state[f'workflow_started_{session_prefix}'] = False
        st.session_state.processing = False
        st.session_state.workflow_started = False
        
        print(f"✅ Workflow execution completed with full UI preservation")

def render_chat_input(workflow):
    """Render fixed bottom chat input"""
    
    st.markdown('<div class="chat-input-container">', unsafe_allow_html=True)
    
    placeholder_text = "Ask about healthcare finance, claims, costs, member data..."
    
    user_query = st.chat_input(
        placeholder_text,
        key="chat_input_field",
        disabled=st.session_state.processing
    )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Process user input
    if user_query and not st.session_state.processing:
        print(f"User query received: {user_query}")
        start_processing(user_query)

def clear_all_caches():
    """Clear all caches - useful for debugging"""
    try:
        # Clear Streamlit caches
        st.cache_data.clear()
        st.cache_resource.clear()
        
        # Clear session-specific workflow
        session_id = st.session_state.get('session_id')
        if session_id:
            workflow_key = f"workflow_instance_{session_id}"
            if workflow_key in st.session_state:
                del st.session_state[workflow_key]
        
        print("🧹 All caches cleared")
        return True
    except Exception as e:
        print(f"❌ Error clearing caches: {e}")
        return False

def main():
    """Main function with complete conversation preservation"""
    
    with st.sidebar:
        st.markdown("### 🏥 Navigation")
        
        if st.button("⬅️ Back to Main Page", key="back_to_main"):
            st.switch_page("main.py")
        
        # Enhanced debug section
        st.markdown("---")
        st.markdown("### 🔧 Session Info")
        
        if st.button("🧹 Clear All Caches", key="clear_caches"):
            if clear_all_caches():
                st.success("Caches cleared!")
                st.rerun()
        
        # Show detailed session info
        if 'session_id' in st.session_state:
            st.text(f"Session: {st.session_state.session_id[:8]}...")
            st.text(f"Thread: {st.session_state.get('conversation_thread_id', 'None')[:12]}...")
            
            # Enhanced conversation stats
            messages = st.session_state.get('messages', [])
            user_msgs = [m for m in messages if m['type'] == 'user']
            assistant_msgs = [m for m in messages if m['type'] == 'assistant']
            
            st.text(f"Total Messages: {len(messages)}")
            st.text(f"User Questions: {len(user_msgs)}")
            st.text(f"AI Responses: {len(assistant_msgs)}")
            
            # Show message types breakdown
            if st.button("📊 Message Breakdown", key="msg_breakdown"):
                msg_types = {}
                for msg in messages:
                    msg_type = msg.get('response_type', msg['type'])
                    msg_types[msg_type] = msg_types.get(msg_type, 0) + 1
                
                for msg_type, count in msg_types.items():
                    st.text(f"{msg_type}: {count}")
        
        st.text(f"Azure: {os.environ.get('WEBSITE_INSTANCE_ID', 'local')}")
        st.text(f"Process: {os.getpid()}")
    
    try:
        # Initialize session state
        initialize_session_state()
        
        # Get workflow
        workflow = get_workflow_for_session(st.session_state.session_id)
        
        if workflow is None:
            st.error("❌ Failed to initialize workflow. Please refresh the page.")
            return
        
        # Main title with enhanced info
        st.markdown("# 🏥 Healthcare Finance Assistant")
        conversation_length = len([m for m in st.session_state.get('messages', []) if m['type'] == 'user'])
        st.markdown(f"*Session: {st.session_state.session_id[:8]}... | Conversation: {conversation_length} questions*")
        st.markdown("---")
        
        # Chat container with enhanced conversation display
        st.markdown('<div class="chat-container">', unsafe_allow_html=True)
        
        # ✅ RENDER COMPLETE CONVERSATION HISTORY
        render_conversation_with_full_history()
        
        # Render persistent follow-up questions
        render_persistent_followup_questions()
        
        # Execute workflow if processing
        if st.session_state.processing and not st.session_state.workflow_started:
            print(f"🚀 Starting workflow execution...")
            execute_workflow_streaming_with_ui_preservation(workflow)
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Render chat input
        render_chat_input(workflow)
        
    except Exception as e:
        print(f"❌ Error in main application: {str(e)}")
        st.error(f"Application Error: {str(e)}")
        
        # Reset on error
        if 'session_id' in st.session_state:
            session_prefix = st.session_state.session_id
            st.session_state[f'processing_{session_prefix}'] = False
            st.session_state[f'workflow_started_{session_prefix}'] = False

if __name__ == "__main__":
    main()(spinner_placeholder, current_node, next_agent)
                    else:
                        update_spinner
