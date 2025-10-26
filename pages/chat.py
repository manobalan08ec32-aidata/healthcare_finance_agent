Application Error: There are multiple elements with the same key='feedback_87174d2a-6c5b-4f43-be6d-2022af2ae3b1_msg_2_thumbs_up'. To fix this, please make sure that the key argument is unique for each element you create.


import streamlit as st
import asyncio
import uuid
import time
import json
import re
import pandas as pd
import atexit
import traceback
from datetime import datetime
from core.databricks_client import DatabricksClient
from workflows.langraph_workflow import AsyncHealthcareFinanceWorkflow
from typing import Dict, Any

# Page configuration - Force sidebar to be expanded
st.set_page_config(
    page_title="Healthcare Finance Assistant",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Set sidebar width via CSS and hide collapse button
st.markdown(
    """
    <style>
        /* Force sidebar to be visible and set width */
        [data-testid="stSidebar"] {
            min-width: 220px !important;
            max-width: 220px !important;
            width: 220px !important;
            display: block !important;
            visibility: visible !important;
        }
        
        /* Ensure sidebar content is visible */
        [data-testid="stSidebar"] > div {
            display: block !important;
            visibility: visible !important;
        }
        
        /* Hide the sidebar collapse button - multiple selectors for Azure compatibility */
        [data-testid="collapsedControl"],
        [data-testid="stSidebarNav"] button[kind="header"],
        .css-1d391kg,
        .e1fqkh3o0,
        button[aria-label="Close sidebar"] {
            display: none !important;
            visibility: hidden !important;
        }
        
        /* Hide any arrow icons in sidebar - Azure App Service compatibility */
        [data-testid="stSidebar"] [data-baseweb="icon"],
        [data-testid="stSidebar"] svg,
        [data-testid="stSidebar"] .css-1kyxreq {
            display: none !important;
        }
        
        /* Force sidebar to stay expanded - Azure App Service fix */
        [data-testid="stSidebar"][aria-expanded="false"] {
            display: block !important;
            min-width: 220px !important;
            width: 220px !important;
        }
        
        /* Override any Azure-specific CSS that might be hiding sidebar */
        .stApp [data-testid="stSidebar"] {
            transform: none !important;
            left: 0 !important;
        }
    </style>
    
    <script>
    // JavaScript to force sidebar to be visible on Azure App Service
    function forceSidebarVisible() {
        const sidebar = document.querySelector('[data-testid="stSidebar"]');
        if (sidebar) {
            sidebar.style.display = 'block';
            sidebar.style.visibility = 'visible';
            sidebar.style.transform = 'none';
            sidebar.style.left = '0';
            sidebar.style.width = '220px';
            sidebar.style.minWidth = '220px';
            sidebar.style.maxWidth = '220px';
        }
        
        // Hide any collapse/expand buttons
        const collapseBtn = document.querySelector('[data-testid="collapsedControl"]');
        if (collapseBtn) {
            collapseBtn.style.display = 'none';
        }
        
        // Force sidebar container to be visible
        const sidebarContainer = document.querySelector('.css-1d391kg');
        if (sidebarContainer) {
            sidebarContainer.style.display = 'block';
            sidebarContainer.style.visibility = 'visible';
        }
    }
    
    // Run immediately and on DOM changes
    forceSidebarVisible();
    
    // Create observer to watch for DOM changes (sidebar only)
    const observer = new MutationObserver(function(mutations) {
        forceSidebarVisible();
    });
    
    // Start observing
    observer.observe(document.body, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ['style', 'class']
    });
    
    // Run periodically as backup
    setInterval(forceSidebarVisible, 1000);
    

    </script>
    """,
    unsafe_allow_html=True,
)

#############################################
# REWRITTEN: MINIMAL TWO-NODE ASYNC HANDLER  #
#############################################

async def run_streaming_workflow_async(workflow, user_query: str):
    """Stream node events and render incrementally; then run follow-up generation."""
    session_id = st.session_state.session_id
    user_questions_history = [m['content'] for m in st.session_state.messages if m.get('role') == 'user']
    
    # Get domain selection from session state (inherited from main page) and normalize to string
    raw_domain_selection = st.session_state.get('domain_selection', None)
    if isinstance(raw_domain_selection, list):
        print(f"⚠️ domain_selection came as list {raw_domain_selection}; normalizing to first item")
        domain_selection = raw_domain_selection[0] if raw_domain_selection else None
    else:
        domain_selection = raw_domain_selection
    print(f"🔍 Using domain selection (string) in chat workflow: {domain_selection} (type={type(domain_selection).__name__})")
    print(f"🔍 Session state domain_selection raw: {raw_domain_selection}")
    print(f"🔍 Current session_id: {session_id}")
    
    # Check if domain_selection is valid before proceeding
    if not domain_selection or domain_selection.strip() == '':
        st.error("⚠️ Please select a domain from the main page before asking questions.")
        st.info("👉 Go back to the main page and select either 'PBM Network' or 'Optum Pharmacy' domain first.")
        return
    
    initial_state = {
        'original_question': user_query,
        'current_question': user_query,
        'session_id': session_id,
        'user_id': get_authenticated_user(),  # Use authenticated user instead of session-based ID
        'user_questions_history': user_questions_history,
    'domain_selection': domain_selection,  # Pass normalized domain selection to workflow
        'errors': [],
        'session_context': {
            'app_instance_id': session_id,
            'execution_timestamp': datetime.now().isoformat(),
            'conversation_turn': len(user_questions_history)
        }
    }
    config = {"configurable": {"thread_id": session_id}}

    last_state = None
    followup_placeholder = st.empty()
    node_placeholders = {}
    status_region = st.container()

    try:
        async for ev in workflow.astream_events(initial_state, config=config):
            et = ev.get('type')
            name = ev.get('name')
            state = ev.get('data', {}) or {}
            
            # Debug logging
            print(f"🎭 UI Event: type={et}, name={name}, state_keys={list(state.keys()) if isinstance(state, dict) else 'None'}")
            
            # Create placeholder when we first see a node (not for workflow end markers)
            if name not in ('__end__', 'workflow_end') and name and name not in node_placeholders:
                node_placeholders[name] = status_region.empty()
                node_placeholders[name].info(f"▶️ {name} running…")
            
            # Handle node completion events
            if et in ('node_end', 'workflow_end'):
                # Only show completion for specific nodes we want to track
                if name in node_placeholders:
                    if name in ['router_agent']:  # Only show completion for important nodes
                        node_placeholders[name].success(f"✅ {name} done")
                    else:
                        # Just clear the placeholder without showing "done" message
                        node_placeholders[name].empty()
                
                # Handle specific node outputs based on actual state content
                if name == 'entry_router':
                    print(f"✅ Entry router completed - routing to next node")
                
                elif name == 'navigation_controller':
                    print(f"🧭 Navigation controller completed - checking outputs...")
                    nav_err = state.get('nav_error_msg')
                    greeting = state.get('greeting_response')
                    
                    print(f"   nav_error_msg: {nav_err}")
                    print(f"   greeting_response: {greeting}")
                    
                    if nav_err:
                        # Clear all placeholders silently and show error
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(nav_err, message_type="error")
                        return
                    if greeting:
                        # Clear all placeholders silently and show greeting
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(greeting, message_type="greeting")
                        return
                
                elif name == 'router_agent':
                    print(f"🎯 Router agent completed - checking outputs... ")
                    if state.get('sql_followup_but_new_question', False):
                        print("🔄 New question detected - workflow will continue to navigation_controller")
                        # Clear placeholders but don't break/return - let stream continue to next node
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        # Add a temporary status message
                        st.info("🔄 Processing your new question...")
                        # Don't break or return - just continue to let the stream proceed
                        continue
                    if state.get('router_error_msg'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('router_error_msg'), message_type="error")
                        return
                    if state.get('needs_followup'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('sql_followup_question'), message_type="needs_followup")
                        return
                    if state.get('requires_dataset_clarification') and state.get('dataset_followup_question'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('dataset_followup_question'), message_type="dataset_clarification")
                        return
                    if state.get('missing_dataset_items') and state.get('user_message'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('user_message'), message_type="missing_items")
                        return
                    if state.get('phi_found') and state.get('user_message'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('user_message'), message_type="phi_pii_error")
                        return
                    if state.get('sql_followup_topic_drift') and state.get('user_message'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('user_message'), message_type="error")
                        return
                    
                    sql_result = state.get('sql_result', {})
                    if sql_result and sql_result.get('success'):
                        # Clear node progress indicators
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        
                        # Display selection_reasoning after AI rewritten question if available
                        functional_names = state.get('functional_names', [])
                        if functional_names:
                            add_selection_reasoning_message(functional_names)
                            print(f"✅ Added functional_names display: {functional_names}")
                        
                        # Render SQL results immediately
                        rewritten_question = state.get('rewritten_question', initial_state['current_question'])
                        table_name = state.get('selected_dataset', None)
                        add_sql_result_message(sql_result, rewritten_question, table_name)
                        last_state = state
                        
                        # Mark that SQL results are ready and we need to start narrative generation
                        st.session_state.sql_rendered = True
                        st.session_state.narrative_state = state
                        st.session_state.followup_state = state
                        
                        print("🔄 SQL results rendered - triggering UI update to show results before narrative generation")
                        # Force UI to update by breaking and using session state to continue narrative and follow-up later
                        break
                    else:
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message("No results available or SQL execution failed.", message_type="error")
                        return
                
                elif name == 'strategy_planner_agent':
                    print(f"🧠 Strategic planner completed - checking outputs...")
                    
                    # Check for errors first
                    if state.get('strategy_planner_err_msg'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('strategy_planner_err_msg'), message_type="error")
                        return
                    
                    # Check for strategic results
                    strategic_results = state.get('strategic_query_results', [])
                    if strategic_results:
                        # Clear node progress indicators
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        
                        # Render strategic analysis immediately
                        strategic_reasoning = state.get('strategic_reasoning', '')
                        add_strategic_analysis_message(strategic_results, strategic_reasoning)
                        last_state = state
                        
                        print("🔄 Strategic analysis rendered - checking for drillthrough availability")
                        
                        # Check if drillthrough exists and should be executed
                        drillthrough_exists = state.get('drillthrough_exists', '')
                        if drillthrough_exists == "Available":
                            # Mark strategic analysis as rendered and prepare for drillthrough
                            st.session_state.strategic_rendered = True
                            st.session_state.drillthrough_state = state
                            
                            print("🔄 Strategic analysis complete - drillthrough will be executed separately")
                            # Force UI to update by breaking and using session state to continue drillthrough later
                            break
                        else:
                            # No drillthrough available - this is the end, prepare for follow-ups
                            st.session_state.sql_rendered = True
                            st.session_state.followup_state = state
                            break
                    else:
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message("No strategic analysis results available.", message_type="error")
                        return
                
                elif name == 'drillthrough_planner_agent':
                    print(f"🔧 Drillthrough planner completed - checking outputs...")
                    
                    # Check for errors first
                    if state.get('drillthrough_planner_err_msg'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('drillthrough_planner_err_msg'), message_type="error")
                        return
                    
                    # Check for drillthrough results
                    drillthrough_results = state.get('drillthrough_query_results', [])
                    if drillthrough_results:
                        # Clear node progress indicators
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        
                        # Render drillthrough analysis immediately
                        drillthrough_reasoning = state.get('drillthrough_reasoning', '')
                        add_drillthrough_analysis_message(drillthrough_results, drillthrough_reasoning)
                        last_state = state
                        
                        print("🔄 Drillthrough analysis rendered - workflow complete")
                        break
                    else:
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message("No drillthrough analysis results available.", message_type="error")
                        return
                
                # Capture final state for follow-up generation
                if et == 'workflow_end':
                    print(f"🏁 Workflow completed - final state captured")
                    # Clear remaining placeholders when workflow ends
                    for placeholder in node_placeholders.values():
                        placeholder.empty()
                    if not last_state:
                        last_state = state
        # Handle follow-up generation based on where we broke out
        if st.session_state.get('sql_rendered', False):
            print("🔄 SQL was rendered - follow-up will be handled in next execution cycle")
            # Don't generate follow-ups here - let the main loop handle it after UI updat
        elif last_state and last_state.get('sql_result', {}).get('success'):
            # This handles the case where workflow completed normally without early break
            try:
                print("🔄 Starting follow-up generation after complete workflow...")
                followup_state = last_state
                follow_res = await workflow.ainvoke_followup(followup_state, config)
                fq = follow_res.get('followup_questions', [])
                if fq:
                    st.session_state.current_followup_questions = fq
                    followup_placeholder.empty()
                    add_assistant_message("💡 **Would you like to explore further? Here are some suggested follow-up questions:**", message_type="followup_questions")
                    print(f"✅ Generated {len(fq)} follow-up questions")
                else:
                    followup_placeholder.empty()
                    print("ℹ️ No follow-up questions generated")
            except Exception as fe:
                followup_placeholder.error(f"Follow-up generation failed: {fe}")
                print(f"❌ Follow-up generation error: {fe}")
        elif last_state:
            print("ℹ️ No SQL results to generate follow-ups for")
        else:
            print("⚠️ No final state captured from workflow")
            
    except Exception as e:
        add_assistant_message(f"Workflow failed: {e}", message_type="error")
        print(f"❌ Workflow error: {e}")

def run_streaming_workflow(workflow, user_query: str):
    asyncio.run(run_streaming_workflow_async(workflow, user_query))

## Removed legacy background follow-up + polling functions (replaced by direct streaming approach)

# ============ SESSION MANAGEMENT (keep your existing functions) ============

def get_session_workflow():
    """
    Get or create the ASYNC workflow instance for the current user session.
    """
    if 'workflow' not in st.session_state:
        print(f"🔧 Creating new ASYNC workflow for session: {st.session_state.session_id}")
        try:
            db_client = DatabricksClient()
            # Store the created ASYNC workflow directly in the session state
            st.session_state.workflow = AsyncHealthcareFinanceWorkflow(db_client)
            # Store the db_client for cleanup
            st.session_state.db_client = db_client
            print(f"✅ Async workflow created and stored in session state for session: {st.session_state.session_id}")
        except Exception as e:
            print(f"❌ Failed to create async workflow for session {st.session_state.session_id}: {str(e)}")
            st.error(f"Failed to create workflow: {e}")
            return None
    return st.session_state.workflow

def cleanup_session():
    """Cleanup function to properly close aiohttp sessions and background threads"""
    try:
        session_id = getattr(st.session_state, 'session_id', 'unknown')
        print(f"🧹 Cleaning up session: {session_id}")
        
        # Clean up background thread if it exists
        if hasattr(st.session_state, 'background_thread') and st.session_state.background_thread:
            thread = st.session_state.background_thread
            if thread.is_alive():
                print(f"⚠️ Background thread still running during cleanup: {thread.name}")
                # Note: daemon threads will be killed automatically when main process exits
            st.session_state.background_thread = None
        
        # Clean up database client
        if hasattr(st.session_state, 'db_client') and st.session_state.db_client:
            # Run async cleanup in a new event loop since Streamlit might have closed the main one
            import asyncio
            try:
                asyncio.run(st.session_state.db_client.close())
                print("✅ Successfully closed database client sessions")
            except Exception as cleanup_error:
                print(f"⚠️ Error during DB cleanup: {cleanup_error}")
    except Exception as e:
        print(f"⚠️ General cleanup error: {e}")

# Register cleanup function to run on app shutdown
import atexit
atexit.register(cleanup_session)

def get_authenticated_user():
    """
    Get the authenticated user from session state (passed from main.py)
    Returns the user email or a fallback value
    """
    return st.session_state.get('authenticated_user', 'unknown_user')

def get_authenticated_user_name():
    """
    Get the authenticated user name from session state (passed from main.py)
    Returns the user name or a fallback value
    """
    return st.session_state.get('authenticated_user_name', 'FDM User')

def initialize_session_state():
    """
    Initialize session state variables directly. (Keep your existing implementation)
    """
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
        print(f"🆕 New session started: {st.session_state.session_id}")

    # Initialize variables directly on st.session_state
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
    if 'session_overview_shown' not in st.session_state:
        st.session_state.session_overview_shown = False
    if 'last_clicked_question' not in st.session_state:
        st.session_state.last_clicked_question = None
    if 'current_query' not in st.session_state:
        st.session_state.current_query = ""
    if 'current_conversation_start' not in st.session_state:
        st.session_state.current_conversation_start = -1
    if 'sql_rendered' not in st.session_state:
        st.session_state.sql_rendered = False
    if 'narrative_rendered' not in st.session_state:
        st.session_state.narrative_rendered = False
    if 'narrative_state' not in st.session_state:
        st.session_state.narrative_state = None
    if 'followup_state' not in st.session_state:
        st.session_state.followup_state = None
    if 'strategic_rendered' not in st.session_state:
        st.session_state.strategic_rendered = False
    if 'drillthrough_state' not in st.session_state:
        st.session_state.drillthrough_state = None
    # Initialize domain_selection - inherit from main page selection or set to None
    if 'domain_selection' not in st.session_state:
        st.session_state.domain_selection = None
        print(f"🔧 Domain selection initialized to None in chat.py")
    
    # Authenticated user info should always come from main.py - don't initialize here
    # Just log what we have (or fallback if nothing was passed)
    authenticated_user = get_authenticated_user()
    authenticated_user_name = get_authenticated_user_name()
    
    print(f"Current authenticated user: {authenticated_user}")
    print(f"Current authenticated user name: {authenticated_user_name}")
    
    # Warning if we're using fallback values (indicates main.py didn't pass user info)
    if authenticated_user == 'unknown_user':
        print("⚠️ WARNING: Using fallback authenticated_user - main.py may not have passed user info")
    if authenticated_user_name == 'FDM User':
        print("⚠️ WARNING: Using fallback authenticated_user_name - main.py may not have passed user info")
    # Removed obsolete follow-up polling/background keys

# ============ CHAT HISTORY MANAGEMENT ============

def convert_text_to_safe_html(text):
    """Convert text to HTML while preserving intentional formatting and bullet points"""
    if not text:
        return ""
    
    import re

    # Fix number-word combinations that cause italics (like "1.65billion")
    text = re.sub(r'(\d+\.?\d*)(billion|million|thousand|trillion)', r'\1 \2', text)

    # Convert markdown formatting to HTML
    text = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', text)
    text = re.sub(r'(?<!\d)\*([^*\d]+)\*(?!\d)', r'<em>\1</em>', text)
    text = re.sub(r'^### (.+)$', r'<h3>\1</h3>', text, flags=re.MULTILINE)
    text = re.sub(r'^## (.+)$', r'<h2>\1</h2>', text, flags=re.MULTILINE)
    text = re.sub(r'^# (.+)$', r'<h1>\1</h1>', text, flags=re.MULTILINE)

    # Convert markdown bullet points to HTML lists
    def bullets_to_ul(text):
        lines = text.split('\n')
        html_lines = []
        in_list = False
        for line in lines:
            if re.match(r'^\s*[-*]\s+', line):
                if not in_list:
                    html_lines.append('<ul>')
                    in_list = True
                item = re.sub(r'^\s*[-*]\s+', '', line)
                html_lines.append(f'<li>{item}</li>')
            else:
                if in_list:
                    html_lines.append('</ul>')
                    in_list = False
                html_lines.append(line)
        if in_list:
            html_lines.append('</ul>')
        return '\n'.join(html_lines)

    text = bullets_to_ul(text)

    # Convert double line breaks to paragraphs, single to <br>
    paragraphs = text.split('\n\n')
    html_paragraphs = []
    for paragraph in paragraphs:
        if paragraph.strip():
            cleaned_paragraph = paragraph.strip().replace('\n', '<br>')
            # Don't wrap if it's already a header or list
            if not (cleaned_paragraph.startswith('<h') or cleaned_paragraph.startswith('<ul>') or cleaned_paragraph.startswith('<strong>')):
                html_paragraphs.append(f'<p>{cleaned_paragraph}</p>')
            else:
                html_paragraphs.append(cleaned_paragraph)
    return ''.join(html_paragraphs)

def format_sql_data_for_streamlit(data):
    """Format SQL data for proper Streamlit display with numeric conversion"""
    import pandas as pd
    import re
    
    if not data:
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    def format_value(val, column_name=""):
        # Handle None/NaN values
        if pd.isna(val) or val is None:
            return ""
        
        # Convert to string first
        val_str = str(val)
        
        # Check if this is a client ID or similar identifier column - avoid comma formatting
        is_id_column = any(id_pattern in column_name.lower() for id_pattern in ['client_id', 'member_id', 'patient_id', 'id'])
        
        # Check if this is a percentage column
        is_percentage_column = any(pct_pattern in column_name.lower() for pct_pattern in ['percent', 'pct', '_pct', 'ratio'])
        
        # Check if this is a monetary column
        is_monetary_column = any(money_pattern in column_name.lower() for money_pattern in ['revenue', 'expense', 'amount', 'amt', 'cost', 'fee'])
        
        # Handle scientific notation (e.g., 8.0000E433)
        if re.match(r'^-?\d+\.?\d*[eE][+-]?\d+$', val_str):
            try:
                numeric_val = float(val_str)
                # Format large scientific notation as integers with commas
                if abs(numeric_val) >= 1000:
                    return f"{int(round(numeric_val)):,}"
                else:
                    return f"{numeric_val:.2f}"
            except ValueError:
                return val_str
        
        # Handle regular numbers
        try:
            numeric_val = float(val_str)
            
            # Check if it's a year value (4-digit number in reasonable year range)
            if numeric_val == int(numeric_val) and 1900 <= int(numeric_val) <= 2100:
                return str(int(numeric_val))  # Keep years without commas
            
            # Handle percentage columns - add % symbol
            if is_percentage_column:
                if 0 <= abs(numeric_val) <= 1:
                    # Value between 0-1, multiply by 100 and add %
                    return f"{numeric_val:.2%}"
                else:
                    # Value already in percentage form (e.g., 25.5), just add %
                    return f"{numeric_val:.2f}%"
            
            # Handle monetary columns - add $ symbol and remove decimals
            if is_monetary_column:
                
                if abs(numeric_val) >= 1000:
                    return f"$ {int(round(numeric_val)):,}"
                else:
                    return f"$ {int(round(numeric_val))}"
            
            # Handle ID columns - never add commas regardless of size
            if is_id_column:
                if numeric_val == int(numeric_val):
                    return str(int(numeric_val))
                else:
                    return f"{numeric_val:.2f}"
            
            # Handle large numbers (no decimals for integers)
            if numeric_val == int(numeric_val):
                if abs(numeric_val) >= 1000:
                    return f"{int(numeric_val):,}"
                else:
                    return str(int(numeric_val))
            else:
                # Show 2 decimals for actual decimal values
                return f"{numeric_val:.2f}"
                
        except (ValueError, TypeError):
            # Not a number, return as-is
            return val_str
    
    # Apply formatting to all columns
    for col in df.columns:
        df[col] = df[col].apply(lambda x: format_value(x, col))
    
    return df

def render_feedback_section(title, sql_query, data, narrative, user_question=None, message_idx=None, table_name=None):
    """Render feedback section with thumbs up/down buttons - only for current question"""
    
    # NOTE: We trust the show_feedback parameter passed from render_single_sql_result
    # The historical marking is already handled in start_processing and checked before calling this function
    # No need to check if this is the "last message" because follow-up questions may be added after SQL results
    
    # Create a unique key for this feedback section using multiple factors
    # Include session_id and message_idx for uniqueness
    session_id = st.session_state.get('session_id', 'default')
    if message_idx is not None:
        feedback_key = f"feedback_{session_id}_msg_{message_idx}"
    else:
        # Fallback for edge cases
        timestamp = int(time.time() * 1000)
        feedback_key = f"feedback_{session_id}_{timestamp}"
    
    print(f"🔑 Generated feedback key: {feedback_key} | Showing feedback for message_idx={message_idx}")
    
    # Show feedback buttons if not already submitted
    if not st.session_state.get(f"{feedback_key}_submitted", False):
        st.markdown("---")
        st.markdown("**Was this helpful?** ")
        
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("👍 Yes, helpful", key=f"{feedback_key}_thumbs_up"):
                # Insert positive feedback - use user_question if available, otherwise fallback
                rewritten_question = user_question or st.session_state.get('current_query', title)
                
                sql_result_dict = {
                    'title': title,
                    'sql_query': sql_query,
                    'query_results': data,
                    'narrative': narrative,
                    'user_question': user_question  # Include user_question in sql_result
                }
                
                # Run feedback insertion
                success = asyncio.run(_insert_feedback_row(
                    rewritten_question, sql_query, True, table_name
                ))
                
                if success:
                    st.session_state[f"{feedback_key}_submitted"] = True
                    st.success("Thank you for your feedback! 👍")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to save feedback")
        
        with col2:
            if st.button("👎 Needs improvement", key=f"{feedback_key}_thumbs_down"):
                st.session_state[f"{feedback_key}_show_form"] = True
                st.rerun()
        
        # Show feedback form for thumbs down
        if st.session_state.get(f"{feedback_key}_show_form", False):
            with st.form(key=f"{feedback_key}_form"):
                feedback_text = st.text_area(
                    "How can we improve this response?",
                    placeholder="Please describe what could be better...",
                    height=100
                )
                
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.form_submit_button("Submit Feedback"):
                        # Insert negative feedback - use user_question if available, otherwise fallback
                        rewritten_question = user_question or st.session_state.get('current_query', title)
                        
                        sql_result_dict = {
                            'title': title,
                            'sql_query': sql_query,
                            'query_results': data,
                            'narrative': narrative,
                            'user_question': user_question  # Include user_question in sql_result
                        }
                        
                        success = asyncio.run(_insert_feedback_row(
                            rewritten_question, sql_query, False, table_name, feedback_text
                        ))
                        
                        if success:
                            st.session_state[f"{feedback_key}_submitted"] = True
                            st.session_state[f"{feedback_key}_show_form"] = False
                            st.success("Thank you for your feedback! We'll work on improving.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Failed to save feedback")
                
                with col2:
                    if st.form_submit_button("Cancel"):
                        st.session_state[f"{feedback_key}_show_form"] = False
                        st.rerun()
    else:
        # Show feedback submitted message
        st.markdown("---")
        st.success("✅ Thank you for your feedback!")

def render_sql_results(sql_result, rewritten_question=None, show_feedback=True, message_idx=None, table_name=None):
    """Render SQL results with title, expandable SQL query, data table, and narrative"""
    
    print(f"📊 render_sql_results called with: type={type(sql_result)}, show_feedback={show_feedback}")
    
    # Ensure sql_result is a dictionary
    if not isinstance(sql_result, dict):
        print(f"❌ SQL result is not a dict: {type(sql_result)} - {str(sql_result)[:100]}...")
        st.error(f"Error: Invalid SQL result format (expected dict, got {type(sql_result).__name__})")
        return
    
    # Get user_question from sql_result for feedback
    user_question = sql_result.get('user_question')
    
    # Check if multiple results
    if sql_result.get('multiple_results', False):
        query_results = sql_result.get('query_results', [])
        for i, result in enumerate(query_results):
            title = result.get('title', f'Query {i+1}')
            sql_query = result.get('sql_query', '')
            data = result.get('data', [])
            narrative = result.get('narrative', '')
            
            # ⭐ Pass message_idx here
            render_single_sql_result(title, sql_query, data, narrative, user_question, show_feedback, message_idx, table_name)
    else:
        # Single result - use rewritten_question if available, otherwise default
        title = rewritten_question if rewritten_question else "Analysis Results"
        sql_query = sql_result.get('sql_query', '')
        data = sql_result.get('query_results', [])
        narrative = sql_result.get('narrative', '')
        
        # ⭐ Pass message_idx here
        render_single_sql_result(title, sql_query, data, narrative, user_question, show_feedback, message_idx, table_name)


def render_single_sql_result(title, sql_query, data, narrative, user_question=None, show_feedback=True, message_idx=None, table_name=None):
    """Render a single SQL result with warm gold background for title and narrative"""
    
    # Title with custom narrative-content styling
    st.markdown(f"""
    <div class="narrative-content">
        <strong> 🤖 AI Rewritten Question:</strong> {title}
    </div>
    """, unsafe_allow_html=True)
    
    # SQL Query in collapsible section
    if sql_query:
        # Use HTML details/summary instead of st.expander to avoid Azure rendering issues
        st.markdown(f"""
        <details style="margin: 10px 0; border: 1px solid #e1e5e9; border-radius: 6px; padding: 0;">
            <summary style="background-color: #f8f9fa; padding: 8px 12px; cursor: pointer; border-radius: 6px 6px 0 0; font-weight: 500; color: #495057;">
                View SQL Query
            </summary>
            <div style="padding: 12px; background-color: #f8f9fa; border-radius: 0 0 6px 6px;">
                <pre style="background-color: #2d3748; color: #e2e8f0; padding: 12px; border-radius: 4px; overflow-x: auto; font-family: 'Courier New', monospace; font-size: 12px; line-height: 1.4; margin: 0;"><code>{sql_query}</code></pre>
            </div>
        </details>
        """, unsafe_allow_html=True)
    
    # Data table
    if data:
        formatted_df = format_sql_data_for_streamlit(data)
        row_count = len(formatted_df) if hasattr(formatted_df, 'shape') else 0
        if row_count > 5000:
            st.warning("⚠️ SQL query output exceeds more than 5000 rows. Please rephrase your query to reduce the result size.")
        elif not formatted_df.empty:
            st.dataframe(
                formatted_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No data to display")
    
    # Narrative with custom styling
    if narrative:
        safe_narrative_html = convert_text_to_safe_html(narrative)
        st.markdown(f"""
        <div class="narrative-content">
            <div style="font-weight: 600; margin-bottom: 8px; display: flex; align-items: center;">
                <span style="margin-right: 8px;">💡</span>
                Key Insights
            </div>
            <div>
                {safe_narrative_html}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Add feedback buttons after narrative (only for current session)
        if show_feedback:
            render_feedback_section(title, sql_query, data, narrative, user_question, message_idx, table_name)

def render_strategic_analysis(strategic_results, strategic_reasoning=None, show_feedback=True):
    """Render strategic analysis response with reasoning and multiple strategic queries"""
    
    print(f"🔥 RENDERING STRATEGIC ANALYSIS")
    
    # 1. SHOW STRATEGIC REASONING FIRST
    if strategic_reasoning:
        formatted_reasoning = convert_text_to_safe_html(strategic_reasoning)
        st.markdown(f"""
        <div class="narrative-content">
            <div style="font-weight: 600; margin-bottom: 8px; display: flex; align-items: center;">
                <span style="margin-right: 8px;">🧠</span>
                Strategic Reasoning
            </div>
            <div>
                {formatted_reasoning}
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("---")
    
    # 2. PROCESS EACH STRATEGIC QUERY
    if strategic_results and isinstance(strategic_results, list):
        for idx, strategic_result in enumerate(strategic_results):
            title = strategic_result.get('title', f'Strategic Analysis {idx+1}')
            sql_query = strategic_result.get('sql_query', '')
            data = strategic_result.get('sql_output', strategic_result.get('data', []))
            narrative = strategic_result.get('narrative', '')
            
            # Show individual strategic query result
            render_single_strategic_result(title, sql_query, data, narrative, idx + 1, show_feedback)
            
            if idx < len(strategic_results) - 1:
                st.markdown("---")
    else:
        st.error("No strategic query results found or invalid format.")
        print(f"🔥 ERROR: strategic_results is not a list: {type(strategic_results)}")

def render_single_strategic_result(title, sql_query, data, narrative, index, show_feedback=True):
    """Render a single strategic analysis result"""
    
    # Title with strategic analysis styling
    st.markdown(f"""
    <div class="narrative-content">
        <strong>🎯 Strategic Analysis #{index}:</strong> {title}
    </div>
    """, unsafe_allow_html=True)
    
    # SQL Query in collapsible section
    if sql_query:
        # Use HTML details/summary instead of st.expander to avoid Azure rendering issues
        st.markdown(f"""
        <details style="margin: 10px 0; border: 1px solid #e1e5e9; border-radius: 6px; padding: 0;">
            <summary style="background-color: #f8f9fa; padding: 8px 12px; cursor: pointer; border-radius: 6px 6px 0 0; font-weight: 500; color: #495057;">
                View Strategic SQL Query #{index}
            </summary>
            <div style="padding: 12px; background-color: #f8f9fa; border-radius: 0 0 6px 6px;">
                <pre style="background-color: #2d3748; color: #e2e8f0; padding: 12px; border-radius: 4px; overflow-x: auto; font-family: 'Courier New', monospace; font-size: 12px; line-height: 1.4; margin: 0;"><code>{sql_query}</code></pre>
            </div>
        </details>
        """, unsafe_allow_html=True)
    
    # Data table
    if data:
        formatted_df = format_sql_data_for_streamlit(data)
        row_count = len(formatted_df) if hasattr(formatted_df, 'shape') else 0
        if row_count > 5000:
            st.warning("⚠️ SQL query output exceeds more than 5000 rows. Please rephrase your query to reduce the result size.")
        elif not formatted_df.empty:
            st.dataframe(
                formatted_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No data to display")
    
    # Strategic narrative
    if narrative:
        safe_narrative_html = convert_text_to_safe_html(narrative)
        st.markdown(f"""
        <div class="narrative-content">
            <div style="font-weight: 600; margin-bottom: 8px; display: flex; align-items: center;">
                <span style="margin-right: 8px;">📊</span>
                Strategic Insights
            </div>
            <div>
                {safe_narrative_html}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Add feedback buttons for strategic analysis (only for current session)
        # if show_feedback:
        #     render_feedback_section(f"Strategic: {title}", sql_query, data, narrative)

def render_drillthrough_analysis(drillthrough_results, drillthrough_reasoning=None, show_feedback=True):
    """Render drillthrough analysis response with reasoning and multiple operational queries"""
    
    print(f"🔥 RENDERING DRILLTHROUGH ANALYSIS")
    
    # 1. SHOW DRILLTHROUGH REASONING FIRST
    if drillthrough_reasoning:
        formatted_reasoning = convert_text_to_safe_html(drillthrough_reasoning)
        st.markdown(f"""
        <div class="narrative-content">
            <div style="font-weight: 600; margin-bottom: 8px; display: flex; align-items: center;">
                <span style="margin-right: 8px;">🔧</span>
                Operational Reasoning
            </div>
            <div>
                {formatted_reasoning}
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("---")
    
    # 2. PROCESS EACH DRILLTHROUGH QUERY
    if drillthrough_results and isinstance(drillthrough_results, list):
        for idx, drillthrough_result in enumerate(drillthrough_results):
            title = drillthrough_result.get('title', f'Drillthrough Analysis {idx+1}')
            sql_query = drillthrough_result.get('sql_query', '')
            data = drillthrough_result.get('sql_output', drillthrough_result.get('data', []))
            narrative = drillthrough_result.get('narrative', '')
            causational_insights = drillthrough_result.get('causational_insights', '')
            
            # Show individual drillthrough query result
            render_single_drillthrough_result(title, sql_query, data, narrative, causational_insights, idx + 1, show_feedback)
            
            if idx < len(drillthrough_results) - 1:
                st.markdown("---")
    else:
        st.error("No drillthrough query results found or invalid format.")
        print(f"🔥 ERROR: drillthrough_results is not a list: {type(drillthrough_results)}")

def render_single_drillthrough_result(title, sql_query, data, narrative, causational_insights, index, show_feedback=True):
    """Render a single drillthrough analysis result"""
    
    # Title with drillthrough analysis styling
    st.markdown(f"""
    <div class="narrative-content">
        <strong>🔍 Drillthrough Analysis #{index}:</strong> {title}
    </div>
    """, unsafe_allow_html=True)
    
    # SQL Query in collapsible section
    if sql_query:
        # Use HTML details/summary instead of st.expander to avoid Azure rendering issues
        st.markdown(f"""
        <details style="margin: 10px 0; border: 1px solid #e1e5e9; border-radius: 6px; padding: 0;">
            <summary style="background-color: #f8f9fa; padding: 8px 12px; cursor: pointer; border-radius: 6px 6px 0 0; font-weight: 500; color: #495057;">
                View Drillthrough SQL Query #{index}
            </summary>
            <div style="padding: 12px; background-color: #f8f9fa; border-radius: 0 0 6px 6px;">
                <pre style="background-color: #2d3748; color: #e2e8f0; padding: 12px; border-radius: 4px; overflow-x: auto; font-family: 'Courier New', monospace; font-size: 12px; line-height: 1.4; margin: 0;"><code>{sql_query}</code></pre>
            </div>
        </details>
        """, unsafe_allow_html=True)
    
    # Data table
    if data:
        formatted_df = format_sql_data_for_streamlit(data)
        row_count = len(formatted_df) if hasattr(formatted_df, 'shape') else 0
        if row_count > 5000:
            st.warning("⚠️ SQL query output exceeds more than 5000 rows. Please rephrase your query to reduce the result size.")
        elif not formatted_df.empty:
            st.dataframe(
                formatted_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No data to display")
    
    # Drillthrough narrative and causational insights
    if narrative or causational_insights:
        display_content = causational_insights if causational_insights else narrative
        safe_narrative_html = convert_text_to_safe_html(display_content)
        st.markdown(f"""
        <div class="narrative-content">
            <div style="font-weight: 600; margin-bottom: 8px; display: flex; align-items: center;">
                <span style="margin-right: 8px;">🔬</span>
                Operational Insights
            </div>
            <div>
                {safe_narrative_html}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Add feedback buttons for drillthrough analysis (only for current session)
        # if show_feedback:
        #     render_feedback_section(f"Drillthrough: {title}", sql_query, data, display_content)

def add_assistant_message(content, message_type="standard"):
    """
    Add assistant message to chat history - used for both real-time and preserved history
    """
    message = {
        "role": "assistant",
        "content": content,
        "message_type": message_type,
        "timestamp": datetime.now().isoformat()
    }
    
    st.session_state.messages.append(message)
    content_preview = (content or '')[:50] + "..." if content and len(content) > 50 else (content or '')
    print(f"✅ Added {message_type} message (total messages now: {len(st.session_state.messages)}): {content_preview}")
    
    # Debug: Show current message state
    print(f"📊 Current message summary:")
    for i, msg in enumerate(st.session_state.messages[-3:]):  # Show last 3 messages
        role = msg.get('role', 'unknown')
        msg_type = msg.get('message_type', 'standard')
        msg_content = msg.get('content', '') or ''
        content_preview = msg_content[:30] + "..." if len(msg_content) > 30 else msg_content
        print(f"  Message {len(st.session_state.messages)-3+i}: {role}({msg_type}) - {content_preview}")
    
    # Handle special state updates
    if message_type == "domain_clarification":
        st.session_state.current_followup_questions = []

def add_selection_reasoning_message(functional_names):
    """
    Add functional_names message to chat history for proper rendering
    functional_names: list of strings representing selected datasets
    """
    message = {
        "role": "assistant",
        "content": functional_names,  # Store as list instead of string
        "message_type": "selection_reasoning",
        "timestamp": datetime.now().isoformat()
    }
    
    st.session_state.messages.append(message)
    print(f"✅ Added functional_names message (total messages now: {len(st.session_state.messages)})")

def add_sql_result_message(sql_result, rewritten_question=None, table_name=None):
    """
    Add SQL result message to chat history for proper rendering
    """
    message = {
        "role": "assistant",
        "content": "SQL analysis complete",  # Placeholder content
        "message_type": "sql_result",
        "timestamp": datetime.now().isoformat(),
        "sql_result": sql_result,
        "rewritten_question": rewritten_question,
        "table_name": table_name
    }
    
    st.session_state.messages.append(message)
    print(f"✅ Added SQL result message (total messages now: {len(st.session_state.messages)})")
    
    # Debug: Show SQL result summary
    if sql_result.get('multiple_results'):
        print(f"  📊 Multiple SQL results: {len(sql_result.get('query_results', []))} queries")
    else:
        data_count = len(sql_result.get('query_results', []))
        print(f"  📊 Single SQL result: {data_count} rows returned")

async def _insert_session_tracking_row(sql_result: Dict[str, Any]):
    """Insert the sql_result JSON into tracking table after narrative generation.
    Columns: session_id, user_id (hardcoded), user_question, state_info (variant), insert_ts (current_timestamp)
    """
    try:
        db_client = st.session_state.get('db_client')
        if not db_client:
            print("⚠️ No db_client in session; skipping tracking insert")
            return
        session_id = st.session_state.get('session_id', 'unknown')
        user_id = get_authenticated_user()
        
        # Get user_question from sql_result user_question field (rewritten question from workflow)
        user_question = sql_result.get('user_question', st.session_state.get('current_query', ''))
        user_question_escaped = user_question.replace("'", "\\'") if user_question else ''
        
        # Serialize sql_result as JSON for VARIANT column
        payload_json = json.dumps(sql_result, ensure_ascii=False)
        
        # Only escape single quotes for Databricks (don't escape newlines or other backslashes)
        payload_escaped = payload_json.replace("'", "\\'")
        
        insert_sql = f"""
        INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotsession_tracking
            (session_id, user_id, user_question, state_info, insert_ts)
        VALUES
            ('{session_id}', '{user_id}', '{user_question_escaped}', '{payload_escaped}', current_timestamp())
        """
        
        print("🗄️ Inserting session tracking row (length chars):", len(payload_json))
        await db_client.execute_sql_async_audit(insert_sql)
        print("✅ Session tracking insert succeeded")
            
    except Exception as e:
        print(f"⚠️ Session tracking insert failed: {e}")


async def _fetch_session_history():
    """Fetch last 5 sessions for the user from fdmbotsession_tracking table.
    Returns list of sessions with session_id and insert_ts (timestamp).
    """
    try:
        db_client = st.session_state.get('db_client')
        if not db_client:
            print("⚠️ No db_client in session; skipping history fetch")
            return []
        
        user_id = get_authenticated_user()
        
        # Modified query to fetch both session_id and max timestamp
        fetch_sql = f"""
        SELECT session_id, MAX(insert_ts) as last_activity
        FROM prd_optumrx_orxfdmprdsa.rag.fdmbotsession_tracking
        WHERE user_id = '{user_id}'
        GROUP BY session_id 
        ORDER BY MAX(insert_ts) DESC
        LIMIT 5
        """
        
        print("🕐 Fetching session history for user:", user_id)
        print("🔍 SQL Query:", fetch_sql)

        result = await db_client.execute_sql_async_audit(fetch_sql)

        print(f"🔍 Database result type: {type(result)}")
        print(f"🔍 Database result: {result}")
        
        # Handle different response formats from database client
        sessions = []
        
        if isinstance(result, list):
            # Direct list response
            sessions = result
            print(f"✅ Direct list response with {len(sessions)} sessions")
        elif isinstance(result, dict):
            if result.get('success'):
                sessions = result.get('data', [])
                print(f"✅ Dict response with success=True, {len(sessions)} sessions")
            else:
                print("⚠️ Dict response with success=False")
                print(f"🔍 Error details: {result}")
                return []
        else:
            print(f"⚠️ Unexpected result type: {type(result)}")
            return []
        
        # Debug each session
        for i, session in enumerate(sessions):
            if isinstance(session, dict):
                session_id = session.get('session_id', 'NO_ID')
                last_activity = session.get('last_activity', 'NO_TIMESTAMP')
                print(f"  Session {i+1}: {session_id} | Last Activity: {last_activity}")
            else:
                print(f"  Session {i+1}: Unexpected session type: {type(session)} - {session}")
        
        return sessions
            
    except Exception as e:
        print(f"⚠️ Session history fetch failed: {e}")
        import traceback
        print(f"🔍 Full traceback: {traceback.format_exc()}")
        return []
    
async def _fetch_session_records(session_id: str):
    """Fetch all records for a specific session_id in chronological order"""
    try:
        db_client = st.session_state.get('db_client')
        if not db_client:
            print("⚠️ No db_client in session; skipping session records fetch")
            return []
        
        user_id = get_authenticated_user()
        
        fetch_sql = f"""
        SELECT session_id, user_question, state_info, insert_ts
        FROM prd_optumrx_orxfdmprdsa.rag.fdmbotsession_tracking
        WHERE user_id = '{user_id}' AND session_id = '{session_id}'
        ORDER BY insert_ts ASC
        """
        
        print(f"🎯 EXECUTING DATABASE QUERY: Fetching all records for session: {session_id}")
        print("🔍 SQL Query:", fetch_sql)
        
        result = await db_client.execute_sql_async_audit(fetch_sql)
        
        print(f"🔍 Database result type: {type(result)}")
        
        # Handle different response formats from database client
        records = []
        
        if isinstance(result, list):
            records = result
            print(f"✅ Direct list response with {len(records)} records")
        elif isinstance(result, dict):
            if result.get('success'):
                records = result.get('data', [])
                print(f"✅ Dict response with success=True, {len(records)} records")
            else:
                print("⚠️ Dict response with success=False")
                print(f"🔍 Error details: {result}")
                return []
        else:
            print(f"⚠️ Unexpected result type: {type(result)}")
            return []
        
        # Debug each record
        for i, record in enumerate(records):
            if isinstance(record, dict):
                print(f"  Record {i+1}: {record.get('user_question', 'NO_QUESTION')[:50]}... - {record.get('insert_ts', 'NO_TIMESTAMP')}")
                print(f"    State_info present: {bool(record.get('state_info'))}")
            elif isinstance(record, str):
                print(f"  Record {i+1}: String record: {record[:100]}...")
            else:
                print(f"  Record {i+1}: Unexpected record type: {type(record)} - {str(record)[:100]}...")
        
        return records
            
    except Exception as e:
        print(f"⚠️ Session records fetch failed: {e}")
        import traceback
        print(f"🔍 Full traceback: {traceback.format_exc()}")
        return []

async def _insert_feedback_row(user_question: str, sql_result: str, positive_feedback: bool, table_name: str = "", feedback_text: str = ""):
    """Insert feedback into fdmbotfeedback_tracking table.

    Args:
        user_question: The rewritten question from state
        sql_result: The SQL query string (not a dict)
        positive_feedback: True for thumbs up, False for thumbs down
        feedback_text: User's text feedback (for thumbs down)
        table_name: Can be a string or a list of strings
    """
    try:
        db_client = st.session_state.get('db_client')
        if not db_client:
            print("⚠️ No db_client in session; skipping feedback insert")
            return False

        session_id = st.session_state.get('session_id', 'unknown')
        user_id = get_authenticated_user()

        # Escape single quotes and replace \n with blank in sql_result string
        if sql_result:
            payload_escaped = sql_result.replace("'", "\\'").replace("\n", " ")
        else:
            payload_escaped = ""

        # Escape user inputs
        user_question_escaped = user_question.replace("'", "\\'") if user_question else ''
        feedback_escaped = feedback_text.replace("'", "\\'") if feedback_text else ''

        # Handle table_name: extract string(s) from list or use as-is if string
        if isinstance(table_name, list):
            table_name_clean = ",".join(str(t).strip("[]'\"") for t in table_name)
        else:
            table_name_clean = str(table_name).strip("[]'\"")

        insert_sql = f"""
        INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking
            (session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name)
        VALUES
            ('{session_id}', '{user_id}', '{user_question_escaped}', '{payload_escaped}', {positive_feedback}, '{feedback_escaped}', current_timestamp(), '{table_name_clean}')
        """

        print(f"🗄️ Inserting feedback: {'👍' if positive_feedback else '👎'} - {len(payload_escaped)} chars")
        await db_client.execute_sql_async_audit(insert_sql)
        print("✅ Feedback insert succeeded")
        return True

    except Exception as e:
        print(f"⚠️ Feedback insert failed: {e}")
        return False

async def _fetch_last_session_summary():
    """Fetch the latest session summary for the authenticated user from session_summary table"""
    try:
        db_client = st.session_state.get('db_client')
        if not db_client:
            print("⚠️ No db_client in session; skipping session summary fetch")
            return None
        
        user_id = get_authenticated_user()
        
        fetch_sql = f"""
        SELECT session_id, summary, insert_ts
        FROM prd_optumrx_orxfdmprdsa.rag.session_summary
        WHERE user_id = '{user_id}'
        ORDER BY insert_ts DESC
        LIMIT 1
        """
        
        print(f"🔍 Fetching last session summary for user: {user_id}")
        result = await db_client.execute_sql_async_audit(fetch_sql)
        
        # Handle different response formats from database client
        if isinstance(result, list) and result:
            return result[0]
        elif isinstance(result, dict) and result.get('success'):
            data = result.get('data', [])
            return data[0] if data else None
        else:
            print(f"⚠️ No session summary found or unexpected result format: {type(result)}")
            return None
            
    except Exception as e:
        print(f"⚠️ Session summary fetch failed: {e}")
        return None

async def _generate_session_overview(narrative_summary: str):
    """Use LLM to generate a brief overview of the last session"""
    try:
        # Get the database client to access the Claude API
        db_client = st.session_state.get('db_client')
        if not db_client:
            print("⚠️ No db_client available for LLM generation")
            return None
        
        # Create a narrative-focused prompt for the LLM with adaptive length
        user_prompt = f"""You are a helpful healthcare finance assistant. Create a narrative overview of the user's last session that matches the content richness.

Previous session summary:
{narrative_summary}

Instructions:
1. ADAPTIVE LENGTH: Write based on the content available in the summary:
   - If summary is very brief/limited (like "not many rows to generate insights"): Keep it SHORT - 2-3 sentences maximum
   - If summary has moderate content: Write 4-5 sentences in 1-2 paragraphs  
   - If summary is rich with details: Write up to 8-12 sentences in 2-3 paragraphs (MAXIMUM)
   - DO NOT pad with filler content - match the story length to actual content depth

2. Use ONLY the exact information from the summary - don't invent numbers, metrics, or details
3. Tell the story chronologically following the user's question sequence 
4. If data was limited/insufficient, acknowledge briefly and pivot forward - don't dwell on it
5. Use professional, conversational tone like catching up with a colleague
6. End with a forward-looking sentence encouraging new exploration
7. Format as clean HTML using <p> tags for paragraphs
8. Focus on substance over length - quality over quantity

Remember: The goal is an accurate, proportional retelling. If there's little to tell, keep it brief. If there's a rich story, tell it fully but concisely."""
        
        # Use the database client's Claude API method
        llm_response = await db_client.call_claude_api_endpoint_async([
            {"role": "user", "content": user_prompt}
        ])
        
        return llm_response
        
    except Exception as e:
        print(f"⚠️ Session overview generation failed: {e}")
        return None

def render_last_session_overview():
    """Fetch and display a brief overview of the user's last session"""
    try:
        # Only show if this is a fresh session (no messages yet)
        if st.session_state.messages:
            return
        
        # Check if we've already shown the session overview in this session
        if st.session_state.get('session_overview_shown', False):
            return
        
        # Fetch the last session summary
        last_summary = asyncio.run(_fetch_last_session_summary())
        
        if not last_summary:
            print("ℹ️ No previous session summary found")
            # Mark as shown even if no summary, so we don't keep trying
            st.session_state.session_overview_shown = True
            return
        
        # Extract the narrative summary
        if isinstance(last_summary, dict):
            narrative_summary = last_summary.get('summary', '')
            session_id = last_summary.get('session_id', 'Unknown')
            insert_ts = last_summary.get('insert_ts', 'Unknown')
        else:
            print(f"⚠️ Unexpected summary format: {type(last_summary)}")
            return
        
        if not narrative_summary or narrative_summary.strip() == '':
            print("ℹ️ Empty narrative summary found")
            # Mark as shown even if empty summary, so we don't keep trying
            st.session_state.session_overview_shown = True
            return
        
        print(f"📋 Found last session summary from {insert_ts}")
        
        # Generate LLM narrative overview for all sessions
        try:
            spinner_placeholder = st.empty()
            with spinner_placeholder, st.spinner("✍️ Writing the story of our last conversation..."):
                overview="This feature is disabled to save cost for now. Will be enabled during demo's"
                # overview = asyncio.run(_generate_session_overview(narrative_summary))
            spinner_placeholder.empty()
            
            
        except Exception as e:
            print(f"⚠️ LLM overview generation failed, using fallback: {e}")
            overview = None
        
        # Display the overview
        if overview:
            # Clean up the LLM response - remove ```html and ``` markers
            cleaned_overview = overview.strip()
            if cleaned_overview.startswith('```html'):
                cleaned_overview = cleaned_overview[7:]  # Remove ```html
            if cleaned_overview.endswith('```'):
                cleaned_overview = cleaned_overview[:-3]  # Remove ```
            cleaned_overview = cleaned_overview.strip()
            
            # Use LLM-generated narrative overview with dark blue font
            st.markdown(f"""
            <div style="background-color: #f0f8ff; border: 1px solid #4a90e2; border-radius: 8px; padding: 16px; margin: 16px 0;">
                <div style="display: flex; align-items: center; margin-bottom: 12px;">
                    <span style="font-size: 1.2rem; margin-right: 8px;">💭</span>
                    <strong style="color: #1e3a8a; font-size: 1.1rem;">What we discussed last time</strong>
                </div>
                <div style="color: #1e3a8a; line-height: 1.6; font-weight: 500;">
                    {cleaned_overview}
                </div>
                <div style="margin-top: 12px; padding-top: 8px; border-top: 1px solid #e3f2fd; color: #1e3a8a; font-size: 0.9rem;">
                    <em>Ready to continue? Ask me a new question! 🚀</em>
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            # Fallback: Show a simple message with the session info
            st.markdown(f"""
            <div style="background-color: #f0f8ff; border: 1px solid #4a90e2; border-radius: 8px; padding: 16px; margin: 16px 0;">
                <div style="display: flex; align-items: center; margin-bottom: 12px;">
                    <span style="font-size: 1.2rem; margin-right: 8px;">💭</span>
                    <strong style="color: #1e3a8a; font-size: 1.1rem;">Welcome back!</strong>
                </div>
                <div style="color: #1e3a8a; line-height: 1.6; font-weight: 500;">
                    I found your previous session from <strong>{insert_ts}</strong>. Model could not process at this time!
                </div>
                <div style="margin-top: 12px; padding-top: 8px; border-top: 1px solid #e3f2fd; color: #1e3a8a; font-size: 0.9rem;">
                    <em>Ready to continue? Ask me a new question! 🚀</em>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        print("✅ Last session overview displayed")
        
        # Mark that we've shown the session overview for this session
        st.session_state.session_overview_shown = True
        
    except Exception as e:
        print(f"⚠️ Error rendering last session overview: {e}")
        # Mark as shown even on error, so we don't keep trying
        st.session_state.session_overview_shown = True

def reset_session_overview_flag():
    """Reset the session overview flag to allow showing overview again"""
    st.session_state.session_overview_shown = False
    print("🔄 Session overview flag reset - will show on next fresh session")

def render_historical_session_by_id(session_id: str):
    """Render all records for a historical session in chronological order.
    
    Args:
        session_id: The session ID to load all records for
    """
    try:
        print(f"🎯 USER ACTION: Loading historical session {session_id}")
        
        # Check if we already have this session cached to avoid refetching
        cache_key = f"session_records_{session_id}"
        if cache_key in st.session_state:
            print(f"📦 Using cached records for session {session_id}")
            records = st.session_state[cache_key]
        else:
            print(f"🔄 Fetching records from database for session {session_id}")
            records = asyncio.run(_fetch_session_records(session_id))
            # Cache the records to avoid refetching
            st.session_state[cache_key] = records
        
        if not records:
            add_assistant_message(f"No records found for session: {session_id}", message_type="standard")
            return
        
        print(f"📚 Rendering {len(records)} records for session: {session_id}")
        
        # Process each record in chronological order (already ordered by insert_ts ASC)
        for i, record in enumerate(records):
            print(f"🔍 Processing record {i+1}: type={type(record)}")
            
            if isinstance(record, dict):
                user_question = record.get('user_question', f'Question {i+1}')
                state_info_json = record.get('state_info', '')
                insert_ts = record.get('insert_ts', '')
            elif isinstance(record, str):
                # If record is a string, try to parse it as JSON
                try:
                    record_dict = json.loads(record)
                    user_question = record_dict.get('user_question', f'Question {i+1}')
                    state_info_json = record_dict.get('state_info', '')
                    insert_ts = record_dict.get('insert_ts', '')
                except json.JSONDecodeError:
                    print(f"⚠️ Could not parse record as JSON: {record[:100]}...")
                    continue
            else:
                print(f"⚠️ Skipping unexpected record type {type(record)}: {record}")
                continue
            
            print(f"  Processing record {i+1}: {user_question[:50]}... ({insert_ts})")
            
            # Add user message
            st.session_state.messages.append({
                "role": "user",
                "content": user_question,
                "message_type": "historical",
                "timestamp": insert_ts,
                "historical": True  # Mark as historical message
            })
            
            # Handle the assistant response
            if state_info_json and state_info_json.strip():
                try:
                    # Clean up the JSON string - handle different storage formats
                    cleaned_json = state_info_json.strip()
                    
                    print(f"    🔍 Raw JSON first 100 chars: {cleaned_json[:100]}...")
                    
                    # Case 1: Triple quoted strings ("""{"json"}""")
                    if cleaned_json.startswith('"""') and cleaned_json.endswith('"""'):
                        cleaned_json = cleaned_json[3:-3]
                        print("    🔄 Removed triple quotes")
                    
                    # Case 2: Double quoted JSON strings ("{\"key\":\"value\"}")
                    elif cleaned_json.startswith('"') and cleaned_json.endswith('"'):
                        # This is a JSON string stored as a quoted string
                        try:
                            # Use json.loads to properly handle all escape sequences
                            cleaned_json = json.loads(cleaned_json)
                            print("    🔄 Parsed as quoted JSON string")
                        except json.JSONDecodeError as e:
                            print(f"    ⚠️ JSON decode error: {e}")
                            # If that fails, try a more aggressive cleaning approach
                            try:
                                # Remove outer quotes and try to fix common control character issues
                                cleaned_json = cleaned_json[1:-1]
                                # Replace common escape sequences more carefully
                                cleaned_json = cleaned_json.replace('\\"', '"')
                                # Don't replace \\n with \n - keep them as \\n for JSON parsing
                                print("    🔄 Manually unescaped quoted JSON")
                            except Exception as manual_error:
                                print(f"    ❌ Manual parsing also failed: {manual_error}")
                                raise e
                    
                    print(f"    🔍 Cleaned JSON preview: {str(cleaned_json)[:100]}...")
                    
                    # Parse the final JSON (it might already be parsed from case 2)
                    if isinstance(cleaned_json, str):
                        try:
                            sql_result = json.loads(cleaned_json)
                            print(f"    ✅ Successfully parsed final JSON")
                        except json.JSONDecodeError as json_error:
                            print(f"    🔄 Final JSON parse failed, trying control character cleanup: {json_error}")
                            try:
                                # More aggressive control character cleaning
                                import re
                                # Replace any unescaped control characters (except already escaped ones)
                                # Focus on newlines and other problematic characters
                                cleaned_json_safe = re.sub(r'(?<!\\)[\x00-\x1f]', lambda m: '\\n' if ord(m.group()) == 10 else f'\\u{ord(m.group()):04x}', cleaned_json)
                                sql_result = json.loads(cleaned_json_safe)
                                print(f"    ✅ Success after control character cleanup")
                            except json.JSONDecodeError as final_error:
                                print(f"    ❌ Final JSON parse error after cleanup: {final_error}")
                                error_pos = getattr(final_error, 'pos', 0)
                                print(f"    📝 Problematic area around char {error_pos}: '{cleaned_json[max(0, error_pos-20):error_pos+20]}'")
                                sql_result = {"error": f"JSON parse error: {final_error}", "raw_data": cleaned_json[:200]}
                    else:
                        sql_result = cleaned_json  # Already parsed in case 2
                    
                    # Add assistant message for the SQL result
                    message = {
                        "role": "assistant",
                        "content": "",
                        "message_type": "sql_result",
                        "timestamp": insert_ts,
                        "sql_result": sql_result,
                        "rewritten_question": user_question,
                        "historical": True  # Mark as historical message
                    }
                    st.session_state.messages.append(message)
                    print(f"    ✅ Added SQL result for record {i+1}")
                    
                except json.JSONDecodeError as e:
                    print(f"    ❌ JSON parse error for record {i+1}: {e}")
                    print(f"    🔍 Raw JSON: {state_info_json[:200]}...")
                    add_assistant_message(f"Data parsing error for: {user_question}", message_type="standard")
                    
            else:
                # No state_info available
                add_assistant_message(f"Historical response (limited data): {user_question}", message_type="standard")
                print(f"    ⚠️ No state_info for record {i+1}")
        
        print(f"✅ Completed rendering session {session_id} with {len(records)} records")
        
    except Exception as e:
        st.error(f"Error loading historical session {session_id}: {e}")
        print(f"❌ Error rendering historical session {session_id}: {e}")
        add_assistant_message(f"Error loading session: {session_id}", message_type="standard")

# Removed render_chat_history function - session history now in sidebar only

def render_sidebar_history():
    """Render session history in the sidebar with clickable timestamps (single line)."""
    
    st.markdown("### 📚 Recent Sessions")
    
    # Check if we have a database client first
    db_client = st.session_state.get('db_client')
    if not db_client:
        st.info("Database not connected")
        return
    
    # Auto-fetch history on load
    if 'cached_history' not in st.session_state:
        try:
            print("🔄 Auto-fetching history from database...")
            history = asyncio.run(_fetch_session_history())
            st.session_state.cached_history = history
            print(f"📦 Cached {len(history)} sessions")
        except Exception as e:
            print(f"❌ Failed to fetch history: {e}")
            st.session_state.cached_history = []
    
    history = st.session_state.get('cached_history', [])
    print(f"🗂️ Displaying {len(history)} cached sessions")
    
    if not history:
        st.info("No previous sessions found")
        return
    
    # Display historical sessions by timestamp (ordered DESC)
    for idx, session_item in enumerate(history):
        # Handle dict responses from database (should have session_id and last_activity)
        if isinstance(session_item, dict):
            session_id = session_item.get('session_id', f'unknown_{idx}')
            last_activity = session_item.get('last_activity', 'Unknown Time')
            
            # Format timestamp for display - COMPACT FORMAT FOR SINGLE LINE
            try:
                # Parse timestamp and format it nicely
                from datetime import datetime
                if isinstance(last_activity, str):
                    # Try parsing ISO format timestamp
                    dt = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
                    # OPTION 1: Compact format without year - "Jan 15 2:30PM"
                    display_timestamp = dt.strftime("%b %d %I:%M%p")
                    
                    # OPTION 2: Even more compact with slashes - "1/15 2:30PM" (uncomment to use)
                    # display_timestamp = dt.strftime("%m/%d %I:%M%p")
                    
                    # OPTION 3: 24-hour format, no year - "Jan 15 14:30" (uncomment to use)
                    # display_timestamp = dt.strftime("%b %d %H:%M")
                else:
                    # If it's already a datetime object
                    display_timestamp = last_activity.strftime("%b %d %I:%M%p")
            except Exception as e:
                print(f"⚠️ Failed to format timestamp: {e}")
                display_timestamp = str(last_activity)
            
            # Add emoji with compact timestamp
            display_text = f"🕒 {display_timestamp}"
            
        elif isinstance(session_item, str):
            # Fallback for string-only response (shouldn't happen with new query)
            session_id = session_item
            display_text = f"Session: {session_id[:12]}..."
            
        else:
            print(f"⚠️ Unexpected session type: {type(session_item)} - {session_item}")
            continue
        
        # Create a clickable button for each session with timestamp display
        if st.button(
            display_text, 
            key=f"history_{idx}_{session_id}",
            help=f"Load session from {display_timestamp}\nSession ID: {session_id}",
            use_container_width=True
        ):
            # Clear current chat and load historical session
            # Preserve domain_selection before clearing everything
            current_domain_selection = st.session_state.get('domain_selection')
            
            st.session_state.messages = []
            st.session_state.sql_rendered = False
            st.session_state.narrative_rendered = False
            st.session_state.processing = False
            st.session_state.session_overview_shown = True  # Don't show overview when loading historical session
            
            # Set session ID to the historical session ID to continue the conversation thread
            st.session_state.session_id = session_id
            
            # Restore domain_selection so new questions work properly
            if current_domain_selection:
                st.session_state.domain_selection = current_domain_selection
                print(f"🔄 Session ID updated to historical session: {session_id}, preserved domain_selection: {current_domain_selection}")
            else:
                # If no domain selection exists, this might cause "Domain Not Found" error
                print(f"⚠️ Session ID updated to historical session: {session_id}, no domain_selection to preserve - this may cause errors!")
                print(f"💡 User may need to go back to main page to select domain again")
            
            # Render the complete historical session
            render_historical_session_by_id(session_id)
            st.rerun()


def add_strategic_analysis_message(strategic_results, strategic_reasoning=None):
    """
    Add strategic analysis message to chat history for proper rendering
    """
    message = {
        "role": "assistant",
        "content": "Strategic analysis complete",  # Placeholder content
        "message_type": "strategic_analysis",
        "timestamp": datetime.now().isoformat(),
        "strategic_results": strategic_results,
        "strategic_reasoning": strategic_reasoning
    }
    
    st.session_state.messages.append(message)
    print(f"✅ Added strategic analysis message (total messages now: {len(st.session_state.messages)})")
    print(f"  🧠 Strategic results: {len(strategic_results)} analyses")

def add_drillthrough_analysis_message(drillthrough_results, drillthrough_reasoning=None):
    """
    Add drillthrough analysis message to chat history for proper rendering
    """
    message = {
        "role": "assistant",
        "content": "Drillthrough analysis complete",  # Placeholder content
        "message_type": "drillthrough_analysis", 
        "timestamp": datetime.now().isoformat(),
        "drillthrough_results": drillthrough_results,
        "drillthrough_reasoning": drillthrough_reasoning
    }
    
    st.session_state.messages.append(message)
    print(f"✅ Added drillthrough analysis message (total messages now: {len(st.session_state.messages)})")
    print(f"  🔧 Drillthrough results: {len(drillthrough_results)} analyses")

def start_processing(user_query: str):
    """Start processing user query - with proper message management"""
    print(f"🎯 Starting processing for: {user_query}")
    
    # Mark all existing SQL results as historical before adding new question
    for msg in st.session_state.messages:
        if msg.get('message_type') == 'sql_result':
            msg['historical'] = True
            print(f"🕰️ Marked SQL result message as historical")
    
    # Clear follow-up questions when user types a new question
    if hasattr(st.session_state, 'current_followup_questions') and st.session_state.current_followup_questions:
        print("🗑️ Clearing follow-up questions due to new user input")
        st.session_state.current_followup_questions = []
        
        # Also remove the "Would you like to explore further?" message from chat history if it exists
        if (st.session_state.messages and 
            st.session_state.messages[-1].get('message_type') == 'followup_questions'):
            st.session_state.messages.pop()
            print("🗑️ Removed follow-up intro message from chat history")
    
    # Add user message to history
    st.session_state.messages.append({
        "role": "user",
        "content": user_query
    })
    
    # IMPORTANT: Mark where this conversation starts so we can manage responses properly
    st.session_state.current_conversation_start = len(st.session_state.messages) - 1
    
    # Set processing state and reset narrative state
    st.session_state.current_query = user_query
    st.session_state.processing = True
    st.session_state.workflow_started = False
    st.session_state.narrative_rendered = False
    st.session_state.narrative_state = None

# ============ CSS STYLING FOR MESSAGE TYPES ============

def add_message_type_css():
    """Add CSS for different message types"""
    st.markdown("""
    <style>
    /* Import Inter font from Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Apply Inter font globally */
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    /* Main title styling */
    h1 {
        color: #1e3a8a !important;  /* Dark blue color */
        font-weight: 600 !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    
    /* Real-time animation for new messages */
    @keyframes slideIn {
        from {
            opacity: 0;
            transform: translateY(10px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    .new-message {
        animation: slideIn 0.3s ease-out;
    }
    
    /* Enhanced spinner for better UX */
    .spinner-container {
        background-color: #faf8f2;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 8px 0;
        border-left: 4px solid #007bff;
    }
    
    .spinner-message {
        color: #495057;
        font-weight: 500;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    /* Base message styling */
    .chat-container {
        display: flex;
        flex-direction: column;
        gap: 12px;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    

    
    /* User message - moved to left side */
    .user-message {
        display: flex;
        justify-content: flex-start;  /* Changed from flex-end to flex-start */
        margin: 8px 0;
    }
    
    .user-message-content {
        background-color: #D9F6FA;  /* Light blue background for user messages */
        color: #1e3a8a;  /* Dark blue text */
        padding: 12px 16px;
        border-radius: 18px;
        max-width: 100%;  /* Same as system messages */
        min-width: 200px;  /* Added minimum width */
        word-wrap: break-word;
        border: 1px solid #bbdefb;
        flex-grow: 1;  /* Allow it to expand within the flex container */
    }
    
    .assistant-message {
        display: flex;
        justify-content: flex-start;
        margin: 8px 0;
    }
    
    /* Background #faf8f2 for assistant messages with black font */
    .assistant-message-content {
        background-color: #faf8f2 !important;
        color: black !important;
        padding: 12px 16px;
        border-radius: 18px;
        max-width: 100% !important;  /* Increased from 80% to 95% */
        min-width: 300px !important; /* Minimum width for single-line messages */
        word-wrap: break-word;
        border: 1px solid #e0e0e0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        overflow: visible !important; /* Allow content to extend if needed */
    }
    
    /* Custom styling for AI rewritten question and narrative content */
    .narrative-content {
        color: #333;
        padding: 12px 16px;
        margin: 8px 0;
        max-width: 100%;
        word-wrap: break-word;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
        font-size: 15px;
        line-height: 1.6;
        font-style: normal !important;
        font-weight: normal !important;
        background-color: #faf8f2 !important;
        border: 1px solid #f0ede4 !important;
        border-radius: 12px !important;
    }
    
    /* Specific styling for follow-up questions message to keep it on single line with larger container */
    .assistant-message-content {
        max-width: 100% !important;  /* Increase from 80% to 95% for follow-up messages */
        min-width: 300px !important; /* Ensure minimum width for single-line text */
        overflow: visible !important; /* Allow content to be visible */
    }
    
    .assistant-message-content strong {
        white-space: nowrap !important;
        display: inline-block !important;
        max-width: none !important;
        font-size: 15px !important; /* Ensure consistent font size */
    }
    
    /* Follow-up question container and button styling - using dark blue company standard */
    .followup-container {
        margin: 1rem 0;
        padding: 1rem;
        background-color: #f8f9fa;
        border-radius: 12px;
        border-left: 4px solid #1e3a8a;
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
        border: 2px solid #1e3a8a;
        border-radius: 8px;
        color: #1e3a8a;
        text-align: left;
        cursor: pointer;
        transition: all 0.2s ease;
        font-size: 14px;
        line-height: 1.4;
    }
    
    .followup-button:hover {
        background-color: #1e3a8a;
        color: white;
        box-shadow: 0 2px 8px rgba(30, 58, 138, 0.2);
    }
    
    /* Feedback section styling */
    .feedback-section {
        margin: 1rem 0;
        padding: 0.5rem 0;
        border-top: 1px solid #e0e0e0;
    }
    
    /* Feedback buttons styling */
    div[data-testid="column"] button {
        width: 100% !important;
        font-size: 14px !important;
        padding: 8px 16px !important;
        border-radius: 6px !important;
        transition: all 0.2s ease !important;
    }
    
    /* Thumbs up button - green theme */
    div[data-testid="column"]:first-child button {
        background-color: #f0f9ff !important;
        border: 2px solid #22c55e !important;
        color: #22c55e !important;
    }
    
    div[data-testid="column"]:first-child button:hover {
        background-color: #22c55e !important;
        color: white !important;
    }
    
    /* Thumbs down button - orange theme */
    div[data-testid="column"]:last-child button {
        background-color: #fef3e2 !important;
        border: 2px solid #f59e0b !important;
        color: #f59e0b !important;
    }
    
    div[data-testid="column"]:last-child button:hover {
        background-color: #f59e0b !important;
        color: white !important;
    }
    
    .followup-button:active {
        transform: translateY(1px);
    }
    
    /* Follow-up buttons container styling from working chat_old.py */
    .followup-buttons-container {
        display: flex !important;
        flex-direction: column !important;
        gap: 8px !important;
        align-items: stretch !important;
        justify-content: flex-start !important;
        max-width: 100% !important;
        margin: 0.5rem 0 !important;
        padding: 0 !important;
    }
    
    /* Follow-up buttons styling - using dark blue company standard */
    .stButton > button {
        background-color: white !important;
        color: #1e3a8a !important;
        border: 1px solid #1e3a8a !important;
        border-radius: 8px !important;
        padding: 10px 16px !important;
        margin: 0 !important;
        width: 100% !important;
        text-align: left !important;
        font-size: 14px !important;
        line-height: 1.4 !important;
        transition: all 0.2s ease !important;   
        height: auto !important;
        min-height: 40px !important;
        white-space: normal !important;
        word-wrap: break-word !important;
    }


    .stButton > button:hover {
        background-color: #1e3a8a !important;
        color: white !important;
        box-shadow: 0 1px 3px rgba(30, 58, 138, 0.2) !important;
        transform: translateY(-0.5px) !important;
    }

    .stButton > button:focus {
        background-color: #1e3a8a !important;
        color: white !important;
        box-shadow: 0 0 0 2px rgba(30, 58, 138, 0.25) !important;
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
        color: #1e3a8a !important;
        border-color: #1e3a8a !important;
    }
    
    /* Chat input box styling - smaller initial size with auto-expansion */
    .stChatInput > div > div > textarea {
        min-height: 45px !important;
        max-height: 200px !important;
        height: 45px !important;
        resize: none !important;
        overflow-y: auto !important;
        word-wrap: break-word !important;
        white-space: pre-wrap !important;
        font-size: 14px !important;
        line-height: 1.4 !important;
        padding: 12px 14px !important;
        border-radius: 12px !important;
        border: 2px solid #e1e5e9 !important;
        transition: all 0.2s ease !important;
    }
    
    .stChatInput > div > div > textarea:focus {
        border-color: #1e3a8a !important;
        box-shadow: 0 0 0 2px rgba(30, 58, 138, 0.1) !important;
        outline: none !important;
    }
    
    /* Chat input container */
    .stChatInput {
        position: sticky !important;
        bottom: 0 !important;
        background-color: white !important;
        padding: 10px 0 !important;
        border-top: 1px solid #e1e5e9 !important;
        margin-top: 20px !important;
    }
    
    /* Auto-expand textarea script integration */
    .stChatInput > div > div > textarea {
        field-sizing: content !important;
    }
    
    /* DataFrame styling - right align columns for better numerical data readability */
    .stDataFrame table {
        width: 100% !important;
    }
    
    .stDataFrame table td, 
    .stDataFrame table th {
        text-align: right !important;
        padding: 8px 12px !important;
        border-bottom: 1px solid #e1e5e9 !important;
    }
    
    /* Keep the first column (index/row labels) left-aligned if needed */
    .stDataFrame table td:first-child,
    .stDataFrame table th:first-child {
        text-align: left !important;
        font-weight: 500 !important;
    }
    
    /* Header styling */
    .stDataFrame table th {
        background-color: #f8f9fa !important;
        font-weight: 600 !important;
        color: #1e3a8a !important;
        border-bottom: 2px solid #1e3a8a !important;
    }
    
    /* Alternating row colors for better readability */
    .stDataFrame table tr:nth-child(even) {
        background-color: #f8f9fa !important;
    }
    
    .stDataFrame table tr:hover {
        background-color: #e3f2fd !important;
    }

    </style>
    
    <script>
    // Auto-expand textarea functionality
    function autoExpand() {
        const textareas = document.querySelectorAll('.stChatInput textarea');
        textareas.forEach(function(textarea) {
            if (!textarea.hasAttribute('data-auto-expand')) {
                textarea.setAttribute('data-auto-expand', 'true');
                
                // Set initial height
                textarea.style.height = '45px';
                
                // Add input event listener
                textarea.addEventListener('input', function() {
                    // Reset height to calculate new height
                    this.style.height = '45px';
                    
                    // Calculate new height based on scroll height
                    const newHeight = Math.min(this.scrollHeight, 200);
                    this.style.height = newHeight + 'px';
                });
                
                // Add paste event listener
                textarea.addEventListener('paste', function() {
                    setTimeout(() => {
                        this.style.height = '45px';
                        const newHeight = Math.min(this.scrollHeight, 200);
                        this.style.height = newHeight + 'px';
                    }, 0);
                });
            }
        });
    }
    
    // Run on page load
    document.addEventListener('DOMContentLoaded', autoExpand);
    
    // Run periodically to catch dynamically added textareas
    setInterval(autoExpand, 500);
    </script>
    """, unsafe_allow_html=True)

# ============ MAIN APPLICATION ============

def main():
    """Main Streamlit application with async workflow integration and real-time chat history"""
    
    # Safety check: Ensure user came from main.py with proper authentication
    initialize_session_state()
    authenticated_user = get_authenticated_user()
    
    if authenticated_user == 'unknown_user':
        st.error("⚠️ Please start from the main page to ensure proper authentication.")
        st.info("👉 Click the button below to go to the main page and select your team.")
        if st.button("Go to Main Page"):
            st.switch_page("main.py")
        st.stop()

    # Add enhanced CSS styling
    add_message_type_css()

    with st.sidebar:
        st.markdown("### Navigation")
        
        # Custom styling for the back button
        st.markdown("""
        <style>
        .back-button {
            background-color: white;
            color: #1e3a8a;
            border: 1px solid #1e3a8a;
            padding: 8px 16px;
            border-radius: 4px;
            text-decoration: none;
            font-weight: 500;
            display: inline-block;
            width: 100%;
            text-align: center;
            margin: 10px 0;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
        }
        .back-button:hover {
            background-color: #f0f9ff;
        }
        
        /* Sidebar session history buttons - compact and small */
        [data-testid="stSidebar"] .stButton > button[key*="history_"] {
            text-align: left !important;
            background-color: #f8f9fa !important;
            border: 1px solid #e0e0e0 !important;
            padding: 4px 6px !important;
            margin: 2px 0 !important;
            border-radius: 4px !important;
            font-size: 10px !important;
            line-height: 1.2 !important;
            width: 100% !important;
            height: auto !important;
            min-height: 24px !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        
        [data-testid="stSidebar"] .stButton > button[key*="history_"]:hover {
            background-color: #e9ecef !important;
            border-color: #1e3a8a !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
        if st.button("Back to Main Page", key="back_to_main"):
            st.switch_page("main.py")
    
    try:
        workflow = get_session_workflow()
        
        if workflow is None:
            st.error("Failed to initialize async workflow. Please refresh the page.")
            return
        
        # Show spinner in main area while fetching session history
        if 'cached_history' not in st.session_state:
            # Show spinner in main chat area while fetching history
            spinner_placeholder = st.empty()
            with spinner_placeholder:
                with st.spinner("🔄 Loading application and fetching session history..."):
                    # Fetch history in background while showing spinner
                    with st.sidebar:
                        st.markdown("---")
                        render_sidebar_history()
            # Clear the spinner once history is loaded
            spinner_placeholder.empty()
        else:
            # History already cached, just render sidebar
            with st.sidebar:
                st.markdown("---")
                render_sidebar_history()
        
        st.markdown("""
        <div style="margin-top: -20px; margin-bottom: 10px;">
            <h2 style="color: #1e3a8a; font-weight: 600; font-size: 1.8rem; margin-bottom: 0;">Finance Analytics Assistant</h2>
        </div>
        """, unsafe_allow_html=True)
        
        # Display current domain selection if available
        if st.session_state.get('domain_selection'):
            domain_display = ", ".join(st.session_state.domain_selection)
            st.markdown(f"""
            <div style="background-color: #e3f2fd; padding: 8px 12px; border-radius: 6px; margin-bottom: 10px; color: #1e3a8a; font-size: 0.9rem;">
                🏢 <strong>Selected Team:</strong> {domain_display}
            </div>
            """, unsafe_allow_html=True)
        
        # Show overview of last session if available (only for fresh sessions)
        render_last_session_overview()
        
        st.markdown("---")
        
        # Chat container for messages
        chat_container = st.container()
        
        with chat_container:
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            
            # if not st.session_state.messages:
            #     st.markdown("""
            #     <div class="narrative-content">
            #         Welcome! I'm your Healthcare Finance Assistant.<br>
            #         Ask me about claims and Ledger related healthcare finance questions.<br>
            #     </div>
            #     """, unsafe_allow_html=True)
            
            # Render all chat messages (including real-time updates)
            for idx, message in enumerate(st.session_state.messages):
                render_chat_message_enhanced(message, idx)
            
            # Render follow-up questions if they exist
            render_persistent_followup_questions()
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Processing indicator and streaming workflow execution
        if st.session_state.processing:
            with st.spinner("Running SQL Generation..."):
                run_streaming_workflow(workflow, st.session_state.current_query)
            st.session_state.processing = False
            st.session_state.workflow_started = False
            st.rerun()
        
        # Handle narrative generation after SQL results have been rendered
        elif st.session_state.get('sql_rendered', False) and not st.session_state.get('narrative_rendered', False):
            narrative_state = st.session_state.get('narrative_state')
            if narrative_state:
                with st.spinner("Generating Insights..."):
                    try:
                        print("📝 Generating narrative after SQL display...")
                        
                        # Run narrative generation
                        async def generate_narrative():
                            session_id = st.session_state.session_id
                            config = {"configurable": {"thread_id": session_id}}
                            return await workflow.ainvoke_narrative(narrative_state, config)
                        
                        narrative_res = asyncio.run(generate_narrative())
                        
                        # Add diagnostic logging to understand the response structure
                        print("🔍 Narrative response keys:", list(narrative_res.keys()) if isinstance(narrative_res, dict) else "Not a dict")
                        print("🔍 narrative_complete in response:", narrative_res.get('narrative_complete', 'NOT_FOUND'))
                        
                        # The narrative_complete flag should be in the returned state, not the agent's return value
                        narrative_complete_flag = narrative_res.get('narrative_complete', False)
                        
                        # Get the updated sql_result from the returned state (narrative agent modifies it in-place)
                        updated_sql_result = narrative_res.get('sql_result', {})
                        print("🔍 Updated sql_result has narrative:", bool(updated_sql_result.get('narrative')))
                        
                        if narrative_complete_flag:
                            # Update the existing SQL result message with narrative
                            if updated_sql_result:
                                # Find and update the last SQL result message
                                for i in range(len(st.session_state.messages) - 1, -1, -1):
                                    msg = st.session_state.messages[i]
                                    if msg.get('message_type') == 'sql_result':
                                        msg['sql_result'] = updated_sql_result
                                        break
                            else:
                                # Fallback: try to locate sql_result from narrative_state if not returned explicitly
                                updated_sql_result = (st.session_state.get('narrative_state') or {}).get('sql_result', {})
                            
                            # Fire and forget insert of tracking row (after we have narrative)
                            if updated_sql_result:
                                try:
                                    asyncio.run(_insert_session_tracking_row(updated_sql_result))
                                except RuntimeError:
                                    # If already in an event loop (unlikely here due to asyncio.run wrapping), schedule task
                                    loop = asyncio.get_event_loop()
                                    loop.create_task(_insert_session_tracking_row(updated_sql_result))
                            
                            print("✅ Narrative generation completed")
                        else:
                            print("⚠️ Narrative generation completed with issues - checking if narrative exists anyway...")
                            # Even if flag is missing, check if narrative was actually added
                            if updated_sql_result and updated_sql_result.get('narrative'):
                                print("🔄 Found narrative content despite missing flag - updating message and inserting")
                                # Update the message anyway
                                for i in range(len(st.session_state.messages) - 1, -1, -1):
                                    msg = st.session_state.messages[i]
                                    if msg.get('message_type') == 'sql_result':
                                        msg['sql_result'] = updated_sql_result
                                        break
                                
                                # Insert tracking row
                                try:
                                    asyncio.run(_insert_session_tracking_row(updated_sql_result))
                                except RuntimeError:
                                    loop = asyncio.get_event_loop()
                                    loop.create_task(_insert_session_tracking_row(updated_sql_result))
                            else:
                                print("❌ No narrative content found in sql_result")
                            
                    except Exception as ne:
                        st.error(f"Narrative generation failed: {ne}")
                        print(f"❌ Narrative generation error: {ne}")
                    
                    # Mark narrative as rendered and prepare for follow-up
                    st.session_state.narrative_rendered = True
                    st.session_state.narrative_state = None
                    st.rerun()
        
        # Handle follow-up generation after narrative has been rendered
        elif st.session_state.get('narrative_rendered', False):
            followup_state = st.session_state.get('followup_state')
            if followup_state:
                with st.spinner("Generating follow-up questions..."):
                    try:
                        print("🔄 Generating follow-up questions after narrative completion...")
                        
                        # Run follow-up generation
                        async def generate_followups():
                            session_id = st.session_state.session_id
                            config = {"configurable": {"thread_id": session_id}}
                            return await workflow.ainvoke_followup(followup_state, config)
                        
                        follow_res = asyncio.run(generate_followups())
                        fq = follow_res.get('followup_questions', [])
                        
                        if fq:
                            st.session_state.current_followup_questions = fq
                            add_assistant_message("💡 **Would you like to explore further? Here are some suggested follow-up questions:**", message_type="followup_questions")
                            print(f"✅ Generated {len(fq)} follow-up questions")
                        else:
                            print("ℹ️ No follow-up questions generated")
                            
                    except Exception as fe:
                        st.error(f"Follow-up generation failed: {fe}")
                        print(f"❌ Follow-up generation error: {fe}")
                    
                    # Clean up session state
                    st.session_state.sql_rendered = False
                    st.session_state.narrative_rendered = False
                    st.session_state.followup_state = None
                    st.rerun()
        
        # Handle drillthrough execution after strategic analysis has been rendered
        elif st.session_state.get('strategic_rendered', False):
            drillthrough_state = st.session_state.get('drillthrough_state')
            if drillthrough_state:
                with st.spinner("Running Drillthrough Analysis..."):
                    try:
                        print("🔄 Running drillthrough analysis after strategic display...")
                        
                        # Run drillthrough analysis
                        async def execute_drillthrough():
                            session_id = st.session_state.session_id
                            config = {"configurable": {"thread_id": session_id}}
                            return await workflow.ainvoke_drillthrough(drillthrough_state, config)
                        
                        drillthrough_res = asyncio.run(execute_drillthrough())
                        
                        # Extract drillthrough results
                        drillthrough_results = drillthrough_res.get('drillthrough_query_results', [])
                        drillthrough_reasoning = drillthrough_res.get('drillthrough_reasoning', '')
                        
                        if drillthrough_results:
                            add_drillthrough_analysis_message(drillthrough_results, drillthrough_reasoning)
                            print(f"✅ Drillthrough analysis complete: {len(drillthrough_results)} analyses")
                            print("🏁 Analysis workflow complete - no follow-up questions will be generated")
                        else:
                            print("ℹ️ No drillthrough results generated")
                            add_assistant_message("No drillthrough analysis results available.", message_type="error")
                        
                    except Exception as e:
                        print(f"❌ Drillthrough execution failed: {e}")
                        add_assistant_message(f"Drillthrough analysis failed: {e}", message_type="error")
                    
                    # Clear the flag to prevent re-running
                    st.session_state.strategic_rendered = False
                    st.session_state.drillthrough_state = None
                    st.rerun()
        
        # Chat input at the bottom
        if prompt := st.chat_input("Ask a question...", disabled=st.session_state.processing):
            # Immediately clear follow-up questions when new input is detected
            # if hasattr(st.session_state, 'current_followup_questions'):
            #     st.session_state.current_followup_questions = []
            start_processing(prompt)
            st.rerun()
        
    except Exception as e:
        st.error(f"Application Error: {str(e)}")
        st.session_state.processing = False
        st.session_state.workflow_started = False
    finally:
        # Ensure cleanup on any app termination
        if st.session_state.get('db_client'):
            pass  # Cleanup will be handled by atexit

# Keep your existing render_persistent_followup_questions function
def render_persistent_followup_questions():
    """Render followup questions as simple styled buttons - left aligned like before"""
    # Don't show follow-up questions if processing is active or if the list is empty
    if (hasattr(st.session_state, 'current_followup_questions') and 
        st.session_state.current_followup_questions and 
        not st.session_state.get('processing', False)):
        
        # Display the follow-up questions with simple Streamlit buttons
        for idx, question in enumerate(st.session_state.current_followup_questions):
            # Create more unique key using session_id, timestamp, and question content
            session_id = st.session_state.get('session_id', 'default')
            message_count = len(st.session_state.get('messages', []))
            question_hash = abs(hash(question))
            button_key = f"followup_{session_id}_{message_count}_{idx}_{question_hash}"
            
            if st.button(
                question, 
                key=button_key,
                help=f"Click to explore: {question}",
                use_container_width=True
            ):
                print(f"🔄 Follow-up question clicked: {question}")
                # Clear the follow-up questions to hide buttons
                st.session_state.current_followup_questions = []
                
                # Remove the "Would you like to explore further?" message from chat history
                if (st.session_state.messages and 
                    st.session_state.messages[-1].get('message_type') == 'followup_questions'):
                    st.session_state.messages.pop()
                    print("🗑️ Removed follow-up intro message from chat history")
                
                # Start processing the selected question
                start_processing(question)
                st.rerun()

def render_chat_message_enhanced(message, message_idx):
    """Enhanced chat message rendering with message type awareness"""
    
    role = message.get('role', 'user')
    content = message.get('content', '')
    message_type = message.get('message_type', 'standard')
    timestamp = message.get('timestamp', '')
    is_historical = message.get('historical', False)  # Check if this is a historical message
    
    if role == 'user':
        # Use custom sky blue background for user messages with icon
        # Properly format the content to handle newlines and special characters
        safe_content_html = convert_text_to_safe_html(content)
        st.markdown(f"""
        <div class="user-message">
            <div style="display: flex; align-items: flex-start; gap: 8px;">
                <div style="flex-shrink: 0; width: 32px; height: 32px; background-color: #4f9cf9; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: white; font-weight: 600; font-size: 14px;">
                    👤
                </div>
                <div class="user-message-content">
                    {safe_content_html}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    elif role == 'assistant':
        # Handle SQL result messages specially
        if message_type == "sql_result":
            sql_result = message.get('sql_result')
            rewritten_question = message.get('rewritten_question')
            table_name = message.get('table_name')
            if sql_result:
                print(f"🔍 Rendering SQL result: type={type(sql_result)}")
                if isinstance(sql_result, str):
                    print(f"    ⚠️ SQL result is string, not dict: {sql_result[:100]}...")
                    # Try to parse as JSON if it's a string
                    try:
                        sql_result = json.loads(sql_result)
                        print(f"    ✅ Successfully parsed string to dict")
                    except json.JSONDecodeError as e:
                        print(f"    ❌ Failed to parse SQL result string as JSON: {e}")
                        st.error("Error: Could not parse SQL result data")
                        return
                render_sql_results(sql_result, rewritten_question, show_feedback=not is_historical,message_idx=message_idx, table_name=table_name)
            return
        
        # Handle strategic analysis messages
        elif message_type == "strategic_analysis":
            strategic_results = message.get('strategic_results')
            strategic_reasoning = message.get('strategic_reasoning')
            if strategic_results:
                render_strategic_analysis(strategic_results, strategic_reasoning, show_feedback=not is_historical)
            return
        
        # Handle drillthrough analysis messages
        elif message_type == "drillthrough_analysis":
            drillthrough_results = message.get('drillthrough_results')
            drillthrough_reasoning = message.get('drillthrough_reasoning')
            if drillthrough_results:
                render_drillthrough_analysis(drillthrough_results, drillthrough_reasoning, show_feedback=not is_historical)
            return
        
        # Handle selection reasoning messages - use sky blue background like last session story
        elif message_type == "selection_reasoning":
            # content is now a list of functional_names
            if isinstance(content, list):
                datasets_text = ", ".join(content)
            else:
                datasets_text = str(content)
            
            st.markdown(f"""
            <div style="background-color: #f0f8ff; border: 1px solid #4a90e2; border-radius: 8px; padding: 12px; margin: 12px 0; max-width: 420px;">
                <div style="display: flex; align-items: center;">
                    <span style="font-size: 1rem; margin-right: 8px;">📊</span>
                    <strong style="color: #1e3a8a; font-size: 0.95rem;">Selected Datasets: {datasets_text}</strong>
                </div>
            </div>
            """, unsafe_allow_html=True)
            return
        
        # Use consistent warm gold background for all system messages with icon
        safe_content_html = convert_text_to_safe_html(content)
        st.markdown(f"""
        <div class="assistant-message">
            <div style="display: flex; align-items: flex-start; gap: 8px;">
                <div style="flex-shrink: 0; width: 32px; height: 32px; background-color: #ff6b35; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: white; font-weight: 600; font-size: 14px;">
                    🤖
                </div>
                <div class="assistant-message-content">
                    {safe_content_html}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
