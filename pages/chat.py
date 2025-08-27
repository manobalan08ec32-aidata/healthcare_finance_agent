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

# REFACTORED: The global cache and thread-local storage are no longer needed for session management.

def get_session_workflow():
    """
    Get or create the workflow instance for the current user session.
    The workflow is stored directly and safely in st.session_state.
    """
    if 'workflow' not in st.session_state:
        print(f"🔧 Creating new workflow for session: {st.session_state.session_id}")
        try:
            db_client = DatabricksClient()
            # Store the created workflow directly in the session state
            st.session_state.workflow = HealthcareFinanceWorkflow(db_client)
            print(f"✅ Workflow created and stored in session state for session: {st.session_state.session_id}")
        except Exception as e:
            print(f"❌ Failed to create workflow for session {st.session_state.session_id}: {str(e)}")
            st.error(f"Failed to create workflow: {e}")
            return None
    return st.session_state.workflow

def initialize_session_state():
    """
    Initialize session state variables directly. This is much simpler and safer.
    """
    # Create a unique session ID for LangGraph's thread_id, created only once per session.
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
        print(f"🆔 New session started: {st.session_state.session_id}")

    # REFACTORED: Initialize variables directly on st.session_state. No prefixes are needed.
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    if 'processing' not in st.session_state:
        st.session_state.processing = False
    if 'workflow_started' not in st.session_state:
        st.session_state.workflow_started = False
    if 'current_followup_questions' not in st.session_state:
        st.session_state.current_followup_questions = []
    if 'button_clicked' not in st.session_state:
        st.session_state.button_clicked = False
    if 'click_counter' not in st.session_state:
        st.session_state.click_counter = 0
    if 'last_clicked_question' not in st.session_state:
        st.session_state.last_clicked_question = None
    if 'current_query' not in st.session_state:
        st.session_state.current_query = ""


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

# Custom CSS for chat interface (Your original CSS is preserved)
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
        flex-direction: column !important; /* Stack buttons vertically */
        gap: 8px !important;
        align-items: stretch !important; /* Make buttons same width */
        justify-content: flex-start !important;
        max-width: 95% !important;
        margin: 0.5rem 0 !important;
        padding: 0 !important;
    }

    /* MODIFICATION 2: Button styles updated for full width and text wrapping */
    .stButton > button {
        background-color: white !important;
        color: #007bff !important;
        border: 1px solid #007bff !important;
        border-radius: 8px !important;
        padding: 10px 16px !important;
        margin: 0 !important;
        width: 100% !important; /* Make button take full width of its container */
        text-align: left !important; /* Align text to the left */
        font-size: 14px !important;
        line-height: 1.4 !important;
        transition: all 0.2s ease !important;   
        height: auto !important; /* Allow button to grow vertically */
        min-height: 40px !important;
        white-space: normal !important; /* Allow text to wrap */
        word-wrap: break-word !important;
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
            
            # Handle navigation controller domain clarification
            if node_name == 'navigation_controller':
                print(f"🔎 navigation_controller node_state: {json.dumps(node_state, indent=2, default=str)}")
                greeting_response = node_state.get('greeting_response')
                is_dml_ddl = node_state.get('is_dml_ddl', False)
                nav_error_msg = node_state.get('nav_error_msg')

                if nav_error_msg and nav_error_msg.strip():
                    print(f"🔍 Navigation - Found error")                    
                    formatted_greeting = convert_text_to_safe_html(nav_error_msg)
                    return {
                        'type': 'text',
                        'content': formatted_greeting,
                        'immediate_render': True
                    }
                
                if greeting_response and greeting_response.strip():
                    print(f"🔍 Navigation - Found greeting response: {greeting_response}")
                    print(f"🔍 Navigation - Is DML/DDL: {is_dml_ddl}")
                    
                    formatted_greeting = convert_text_to_safe_html(greeting_response)
                    return {
                        'type': 'text',
                        'content': formatted_greeting,
                        'immediate_render': True
                    }
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
                router_error_msg = node_state.get('router_error_msg')

                if router_error_msg and router_error_msg.strip():
                    print(f"🔍 Navigation - Found error")                    
                    formatted_greeting = convert_text_to_safe_html(router_error_msg)
                    return {
                        'type': 'text',
                        'content': formatted_greeting,
                        'immediate_render': True
                    }
                continue
            
            elif node_name == 'sql_generator_agent':
                sql_query = node_state.get('sql_query', '')
                query_results = node_state.get('query_results', [])
                narrative_response = node_state.get('narrative_response', '')
                sql_gen_error_msg = node_state.get('router_error_msg')

                if sql_gen_error_msg and sql_gen_error_msg.strip():
                    print(f"🔍 Navigation - Found error")                    
                    formatted_greeting = convert_text_to_safe_html(sql_gen_error_msg)
                    return {
                        'type': 'text',
                        'content': formatted_greeting,
                        'immediate_render': True
                    }
                
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
                follow_up_error_msg = node_state.get('follow_up_error_msg')

                if follow_up_error_msg and follow_up_error_msg.strip():
                    print(f"🔍 Navigation - Found error")                    
                    formatted_greeting = convert_text_to_safe_html(follow_up_error_msg)
                    return {
                        'type': 'text',
                        'content': formatted_greeting,
                        'immediate_render': True
                    }
                
                followup_questions = node_state.get('followup_questions', [])
                success = node_state.get('followup_generation_success', False)
                
                print(f"🔍 Follow-up questions found: {followup_questions}")
                print(f"🔍 Follow-up questions count: {len(followup_questions)}")
                print(f"🔍 Generation success: {success}")
                
                if followup_questions and len(followup_questions) > 0:
                    print("🎯 QUESTIONS FOUND - STORING AND DISPLAYING FOLLOWUP BUTTONS")
                    
                    # REFACTORED: Store in the simplified session state
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
                    
            # Root cause node - REMOVED CONSOLIDATED NARRATIVE
            elif node_name == 'root_cause_agent':
                print(f"🔍 Root Cause Agent - Processing detailed output")
                
                consolidated_query_details = node_state.get('consolidated_query_details', [])
                # REMOVED: narrative_response = node_state.get('narrative_response', '')
                
                print(f"🔍 Consolidated query details count: {len(consolidated_query_details)}")
                # REMOVED: print(f"🔍 Narrative response available: {bool(narrative_response)}")
                
                # REMOVED: response_parts = []
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
                
                # REMOVED: Add Consolidated Narrative Response at the end
                # if narrative_response:
                #     formatted_narrative = convert_text_to_safe_html(narrative_response)
                #     response_parts.append(f"<strong>🎯 Consolidated Analysis Summary:</strong><br><br>{formatted_narrative}")
                
                # Return structured response with multiple dataframes (NO CONSOLIDATED CONTENT)
                if all_dataframes:
                    return {
                        'type': 'root_cause_detailed',
                        'content': "",  # REMOVED: "".join(response_parts),
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
    
    # REMOVED: Display consolidated summary at the end
    # if response_data.get('content'):
    #     st.markdown(f"""
    #     <div class="assistant-message">
    #         <div class="assistant-message-content">
    #             {response_data['content']}
    #         </div>
    #     </div>
    #     """, unsafe_allow_html=True)
    # else:
    #     print("⚠️ No consolidated content found")

def render_chat_message(message):
    """
    Renders a single message from the chat history, routing to the correct display function.
    """
    with st.chat_message(message.get("role", "assistant")):
        response_type = message.get("response_type")
        
        if response_type == 'sql_with_table':
            render_sql_response(message)
        elif response_type == 'root_cause_detailed':
            render_root_cause_response(message)
        else: # Default rendering for simple text (user messages and simple assistant text)
            if message.get("role") == 'user':
                 st.markdown(f"""
                <div class="user-message">
                    <div class="user-message-content">
                        {message['content']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else: # Simple text from assistant
                st.markdown(f"""
                <div class="assistant-message">
                    <div class="assistant-message-content">
                        {message['content']}
                    </div>
                </div>
                """, unsafe_allow_html=True)

def render_persistent_followup_questions():
    """Renders follow-up buttons if they exist in the session state."""
    if st.session_state.current_followup_questions and not st.session_state.processing:
        st.markdown(f"""
        <div class="assistant-message">
            <div class="assistant-message-content">
                💡 <strong>Would you like to explore further? Here are some suggested follow-up questions:</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown('<div class="followup-buttons-container" style="display: flex; flex-wrap: wrap; gap: 8px; align-items: center;">', unsafe_allow_html=True)
        
        for i, question in enumerate(st.session_state.current_followup_questions):
            st.button(
                question,
                key=f"persistent_btn_{i}_{abs(hash(question))}",
                on_click=start_processing,
                args=(question,) # Pass the question to the callback
            )
        
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
    """
    Callback to set the state for processing a new query.
    Handles state changes for both chat input and button clicks.
    """
    st.session_state.messages.append({'role': 'user', 'content': user_query})
    st.session_state.current_followup_questions = []
    st.session_state.processing = True
    st.session_state.current_query = user_query
    st.session_state.workflow_started = False
    # No st.rerun() is needed here; on_click handles it.

def handle_followup_click(question, session_id):
    """Handle follow-up question click with session isolation"""
    
    print(f"🔥 Session {session_id} - Follow-up button clicked: {question}")
    
    # REFACTORED: Update state directly, no prefixes needed.
    st.session_state.click_counter += 1
    st.session_state.last_clicked_question = question
    st.session_state.button_clicked = True
    st.session_state.current_followup_questions = []
    
    st.session_state.messages.append({
        'type': 'user',
        'content': question,
        'timestamp': datetime.now()
    })
    
    st.session_state.processing = True
    st.session_state.workflow_started = False
    st.session_state.current_query = question
    
    print(f"🔥 Session {session_id} - Set processing=True, workflow_started=False")
    st.rerun()

def render_immediate_response(response_data, step_count, session_id):
    """Render immediate response with session context"""
    
    if response_data['type'] == 'followup_questions':
        print(f"🔥 Session {session_id} - Rendering follow-up questions")
        
        st.markdown(f"""
        <div class="assistant-message">
            <div class="assistant-message-content">
                {response_data['content']}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        followup_questions = response_data.get('followup_questions', [])
        if followup_questions:
            st.markdown('<div class="followup-buttons-container" style="display: flex; flex-wrap: wrap; gap: 8px;">', unsafe_allow_html=True)
            
            for i, question in enumerate(followup_questions):
                button_key = f"immediate_btn_{session_id}_{step_count}_{i}_{abs(hash(question)) % 10000}"
                
                st.button(
                    question,
                    key=button_key,
                    type="secondary",
                    help="Click to explore this follow-up question",
                    on_click=start_processing,
                    args=(question,)
                )
            
            st.markdown('</div>', unsafe_allow_html=True)
    
    elif response_data['type'] == 'sql_with_table':
        render_sql_response(response_data)
    
    elif response_data['type'] == 'root_cause_detailed':
        render_root_cause_response(response_data)
    
    elif response_data['type'] == 'text':
        print(f"🔥 Session {session_id} - Rendering text response")
        st.markdown(f"""
        <div class="assistant-message">
            <div class="assistant-message-content">
                {response_data['content']}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
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

def save_feedback_data(session_id, user_response, feedback_type):
    """Saves user feedback to the Unity Catalog table."""
    try:
        db_client = st.session_state.workflow.db_client
        # Use parameterized query to prevent SQL injection
        feedback_query = """
            INSERT INTO fdmenh.chatbot_user_feedback (
                session_id, user_response, feedback_type, created_at
            ) VALUES (?, ?, ?, current_timestamp())
        """
        db_client.execute_sql(feedback_query, [session_id, user_response, feedback_type])
        print("✅ Feedback saved successfully.")
    except Exception as e:
        print(f"❌ Failed to save feedback data: {e}")
        st.error(f"Failed to save feedback data: {e}")


def save_audit_data(session_id, full_state, node_name):
    """Saves the full state to the Unity Catalog audit table."""
    try:
        db_client = st.session_state.workflow.db_client
        # Use parameterized query with proper JSON handling
        audit_query = """
            INSERT INTO fdmenh.chatbot_audit_tracking (
                session_id, full_state_output, node_name, created_at
            ) VALUES (?, ?, ?, current_timestamp())
        """
        # Convert state to JSON string safely
        full_state_json = json.dumps(full_state, default=str)
        db_client.execute_sql(audit_query, [session_id, full_state_json, node_name])
        print(f"✅ Audit data saved successfully for node: {node_name}")
    except Exception as e:
        print(f"❌ Failed to save audit data: {e}")

def render_feedback_ui(user_question):
    """Renders the feedback UI with thumbs-up/down and an optional text box."""
    
    feedback_id = st.session_state.get('feedback_target_id', 'default')
    feedback_key = f"feedback_submitted_{feedback_id}"
    
    if feedback_key not in st.session_state:
        st.session_state[feedback_key] = None
    
    if st.session_state[feedback_key] is None:
        st.markdown("<br>Did this answer your question?")
        
        # Use columns for side-by-side buttons
        col1, col2, col3 = st.columns([0.1, 0.1, 0.8])
        
        with col1:
            if st.button("👍", key=f"thumbs_up_{feedback_id}", help="This response was helpful"):
                save_feedback_data(st.session_state.session_id, user_question, "thumbs_up")
                st.session_state[feedback_key] = "up"
                st.success("Thanks for the feedback!")
                st.rerun()
                
        with col2:
            if st.button("👎", key=f"thumbs_down_{feedback_id}", help="This response needs improvement"):
                st.session_state[feedback_key] = "down"
                st.rerun()
                
    elif st.session_state[feedback_key] == "down":
        # Show feedback form
        with st.form(key=f"feedback_form_{feedback_id}"):
            feedback_text = st.text_area("Please tell us how we can improve:", key=f"feedback_text_{feedback_id}")
            col1, col2 = st.columns([0.2, 0.8])
            with col1:
                submit_button = st.form_submit_button(label="Submit")
            
            if submit_button:
                feedback_response = f"thumbs_down: {feedback_text}" if feedback_text.strip() else "thumbs_down: No comment"
                save_feedback_data(st.session_state.session_id, feedback_response, "thumbs_down")
                st.session_state[feedback_key] = "submitted"
                st.success("Thanks for the feedback!")
                st.rerun()
                
    elif st.session_state[feedback_key] in ["up", "submitted"]:
        st.success("Thanks for the feedback!")


def save_to_session_history(all_response_data):
    """
    Saves the final, consolidated assistant response to chat history.
    """
    assistant_message = {
        "role": "assistant",
        "content": "",
        "response_type": "text", # Default type
        "timestamp": datetime.now()
    }
    
    has_content = False
    for data in all_response_data:
        if data['type'] == 'sql_with_table':
            assistant_message.update(data)
            assistant_message['response_type'] = 'sql_with_table'
            has_content = True
        elif data['type'] == 'root_cause_detailed':
            assistant_message.update(data)
            assistant_message['response_type'] = 'root_cause_detailed'
            has_content = True
        elif data['type'] == 'text':
            assistant_message['content'] += data['content'] + "<br>"
            has_content = True
        elif data['type'] == 'followup_questions':
            # Follow-up questions are not saved to history, but to a separate state variable
            st.session_state.current_followup_questions = data.get('followup_questions', [])

    # Only append to history if there was actual content to display
    if has_content:
        st.session_state.messages.append(assistant_message)

def execute_workflow_streaming(workflow):
    """
    Executes the LangGraph workflow, collects all responses, and saves them to history.
    """
    st.session_state.workflow_started = True
    session_id = st.session_state.session_id
    
    try:
        print(f"🚀 Starting workflow execution for: {st.session_state.current_query}")
        
        all_response_data = []
        spinner_placeholder = st.empty()
        current_node = "Starting"
        
        spinner_placeholder.markdown(f"""
        <div class="spinner-container">
            <div class="spinner"></div>
            <div class="spinner-message">🤖 {current_node}...</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Prepare the initial state and config for the LangGraph agent
        conversation_history = [msg['content'] for msg in st.session_state.messages if msg.get('role') == 'user']
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
                'conversation_turn': len(conversation_history)
            }
        }
        config = {"configurable": {"thread_id": session_id}}
        
        # Stream the workflow and collect all response data packets
        for step_data in workflow.app.stream(initial_state, config=config):
            # Update the spinner with the current progress
            current_node = get_current_node_name(step_data)
            next_agent = get_next_agent_from_state(step_data)
            update_spinner(spinner_placeholder, current_node, next_agent)

            # Extract structured data from the current step
            response_data = extract_response_content(step_data) 
            if response_data:
                all_response_data.append(response_data)
        
        spinner_placeholder.empty()
        
        # After the stream is complete, save the entire collected response to history
        save_to_session_history(all_response_data)

    except Exception as e:
        st.error(f"An error occurred during workflow execution: {e}")
        import traceback
        traceback.print_exc()
        # Also save the error to the chat history for visibility
        st.session_state.messages.append({
            "role": "assistant",
            "content": f"I encountered an error: {e}"
        })

    finally:
        # Reset processing flags and rerun the app to display the final state
        st.session_state.processing = False
        st.session_state.workflow_started = False
        st.rerun()

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
    
    if user_query and not st.session_state.processing:
        print(f"User query received: {user_query}")
        start_processing(user_query)

def main():
    """Main Streamlit application with session isolation"""

    with st.sidebar:
        st.markdown("### 🏥 Navigation")
        if st.button("⬅️ Back to Main Page", key="back_to_main"):
            st.switch_page("main.py")
    
    try:
        initialize_session_state()
        workflow = get_session_workflow()
        
        if workflow is None:
            st.error("❌ Failed to initialize workflow. Please refresh the page.")
            return
        
        st.markdown("# 🏥 Healthcare Finance Assistant")
        st.markdown(f"*Session: {st.session_state.session_id[:8]}...*")
        st.markdown("---")
        
        st.markdown('<div class="chat-container">', unsafe_allow_html=True)
        
        if not st.session_state.messages:
            st.markdown("""
            <div class="welcome-message">
                👋 Welcome! I'm your Healthcare Finance Assistant.<br>
                Ask me about claims and Ledger related healthcare finance questions.
            </div>
            """, unsafe_allow_html=True)
        
        # Render the entire chat history, including complex messages
        for message in st.session_state.messages:
            render_chat_message(message)
        
        # Render follow-up questions from the last turn
        render_persistent_followup_questions()
        
        # If processing, show a spinner and run the workflow
        if st.session_state.processing and not st.session_state.workflow_started:
            execute_workflow_streaming(workflow)
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # The chat input is now handled outside the main container for fixed positioning
        if prompt := st.chat_input("Ask a question...", disabled=st.session_state.processing):
            start_processing(prompt)
            st.rerun()
        
    except Exception as e:
        st.error(f"Application Error: {str(e)}")
        st.session_state.processing = False
        st.session_state.workflow_started = False

if __name__ == "__main__":
    main()
