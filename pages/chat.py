import streamlit as st
import asyncio
import uuid
import time
import json
import re
import pandas as pd
import atexit
import traceback
import html
import threading
from datetime import datetime
from typing import Dict, Any, Optional, List

# Chart visualization imports
import plotly.express as px
import plotly.graph_objects as go

# CRITICAL: Import config FIRST to set up Application Insights connection string
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# NOW import logger and other modules (after config is loaded)
from core.logger import setup_logger, log_with_user_context
from core.databricks_client import DatabricksClient
from workflows.langraph_workflow import AsyncHealthcareFinanceWorkflow

# Initialize logger (connection string is now set by config module)
logger = setup_logger(__name__)

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
        /* ===== OPTUM COLOR PALETTE ===== */
        :root {
            --optum-orange: #FF612B;
            --optum-cream: #FAF8F2;
            --optum-cyan: #D9F6FA;
            --optum-white: #FFFFFF;
            --optum-peach: #FFD1AB;
            --optum-amber: #F9A667;
            --optum-red: #D74120;
            --optum-gray: #EDE8E0;
        }
        
        /* ===== FONT IMPORT ===== */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
        
        /* ===== GLOBAL APP STYLING ===== */
        .stApp {
            background-color: #FAFAF8 !important;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif !important;
        }
        
        /* Force sidebar to be visible and set width */
        [data-testid="stSidebar"] {
            min-width: 240px !important;
            max-width: 240px !important;
            width: 240px !important;
            display: block !important;
            visibility: visible !important;
            background: linear-gradient(180deg, #FFFFFF 0%, #FAFAF8 100%) !important;
            border-right: 1px solid #EDE8E0 !important;
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
            min-width: 240px !important;
            width: 240px !important;
        }
        
        /* Override any Azure-specific CSS that might be hiding sidebar */
        .stApp [data-testid="stSidebar"] {
            transform: none !important;
            left: 0 !important;
        }
        
        /* Make sidebar button text smaller for historical sessions */
        [data-testid="stSidebar"] button[kind="secondary"] {
            font-size: 0.75rem !important;
        }
        
        [data-testid="stSidebar"] button p {
            font-size: 0.75rem !important;
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
    
    # Get domain selection from session state
    raw_domain_selection = st.session_state.get('domain_selection', None)
    if isinstance(raw_domain_selection, list):
        domain_selection = raw_domain_selection[0] if raw_domain_selection else None
    else:
        domain_selection = raw_domain_selection
    
    if not domain_selection or domain_selection.strip() == '':
        st.error("⚠️ Please select a domain from the main page before asking questions.")
        st.info("👉 Go back to the main page and select either 'PBM Network' or 'Optum Pharmacy' domain first.")
        return
    
    initial_state = {
        'original_question': user_query,
        'current_question': user_query,
        'session_id': session_id,
        'user_id': get_authenticated_user(),
        'user_email': get_authenticated_user(),  # Add user email for logging context
        'user_questions_history': user_questions_history,
        'domain_selection': domain_selection,
        'user_selected_datasets': st.session_state.get('user_selected_datasets', []),  # List of selected datasets (empty = all)
        'selected_dataset_filter': st.session_state.get('selected_dataset_filter', []),  # Legacy support
        'column_index': st.session_state.get('column_index'),  # Column index for filter value search
        'errors': [],
        'session_context': {
            'app_instance_id': session_id,
            'execution_timestamp': datetime.now().isoformat(),
            'conversation_turn': len(user_questions_history)
        },
        # 🆕 CRITICAL: Load conversation_memory from session_state
        'conversation_memory': st.session_state.get('conversation_memory', {
            'dimensions': {},
            'analysis_context': {
                'current_analysis_type': None,
                'analysis_history': []
            }
        })
    }
    config = {"configurable": {"thread_id": session_id}}

    last_state = None
    followup_placeholder = st.empty()
    
    # Single status display and spinner
    status_display = st.empty()
    spinner_placeholder = st.empty()

    # Track which node is currently being shown in status
    current_status_node = None

    def show_spinner():
        """Show a persistent spinner indicator with user query"""
        spinner_placeholder.markdown(f"""
            <div style="display: flex; align-items: center; padding: 8px 12px; background-color: #FAF8F2; border-radius: 6px; margin-bottom: 10px;">
                <div class="spinner" style="
                    border: 3px solid #f3f3f3;
                    border-top: 3px solid #FF612B;
                    border-radius: 50%;
                    width: 20px;
                    height: 20px;
                    animation: spin 1s linear infinite;
                    margin-right: 10px;
                "></div>
                <span style="color: #2c3e50; font-weight: 500;">Processing: <strong>{user_query[:50]}{'...' if len(user_query) > 50 else ''}</strong></span>
            </div>
            <style>
                @keyframes spin {{
                    0% {{ transform: rotate(0deg); }}
                    100% {{ transform: rotate(360deg); }}
                }}
            </style>
        """, unsafe_allow_html=True)

    def hide_spinner():
        """Hide the spinner indicator"""
        spinner_placeholder.empty()

    def update_status_for_node(node_name: str):
        """Update status display for the given node that is STARTING"""
        nonlocal current_status_node
        
        if node_name == current_status_node:
            return  # Already showing this node's status
        
        current_status_node = node_name
        
        if node_name == 'entry_router':
            status_display.info("⏳ Routing your question...")
        elif node_name == 'navigation_controller':
            status_display.info("⏳ Validating your question...")
        elif node_name == 'router_agent':
            status_display.info("⏳ Analyzing the Metadata...")
            # Start progressive updates
            asyncio.create_task(show_progressive_router_status())
        elif node_name == 'strategy_planner_agent':
            status_display.info("⏳ Planning strategic analysis...")
        elif node_name == 'drillthrough_planner_agent':
            status_display.info("⏳ Preparing drillthrough analysis...")
        else:
            status_display.info(f"⏳ Processing: {node_name}...")

    async def show_progressive_router_status():
        """Show progressive status updates for router_agent node"""
        try:
            await asyncio.sleep(8)
            if current_status_node == 'router_agent':  # Still on router
                status_display.info("⏳ Generating SQL Plan...")

            await asyncio.sleep(15)
            if current_status_node == 'router_agent':  # Still on router
                status_display.info("⏳ Executing SQL query...")
        except Exception as e:
            print(f"⚠️ Progressive status update error: {e}")
    
    # Show spinner at start
    show_spinner()
    
    # Show initial status - workflow always starts with entry_router
    update_status_for_node('entry_router')
    
    log_event('info', "Workflow started", question=user_query[:100] if user_query else "N/A")

    try:
        async for ev in workflow.astream_events(initial_state, config=config):
            et = ev.get('type')
            name = ev.get('name')
            state = ev.get('data', {}) or {}
            
            print(f"📦 Event: type={et}, name={name}")
            
            # ✅ KEY FIX: When a node ENDS, show status for the NEXT node that's about to start
            if et == 'node_end':
                print(f"✅ Node completed: {name}")
                
                # Determine what the next node will be based on routing logic
                # and show its status IMMEDIATELY (before it starts processing)
                
                if name == 'entry_router':
                    # After entry_router, next is usually navigation_controller
                    # (unless there's dataset clarification or SQL followup)
                    if state.get('requires_dataset_clarification') or state.get('is_sql_followup'):
                        update_status_for_node('router_agent')
                    else:
                        update_status_for_node('navigation_controller')
                
                elif name == 'navigation_controller':
                    # Check navigation outputs
                    nav_err = state.get('nav_error_msg')
                    greeting = state.get('greeting_response')
                    user_friendly_msg = state.get('user_friendly_message')

                    if user_friendly_msg:
                        status_display.info(f"💬 {user_friendly_msg}")
                        print(f"💬 Displayed user-friendly message: {user_friendly_msg}")
                        await asyncio.sleep(2)
                    
                    if nav_err:
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(nav_err, message_type="error")
                        return
                    if greeting:
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(greeting, message_type="greeting")
                        return
                    
                    # After navigation, next is usually router_agent or END
                    question_type = state.get('question_type')
                    if question_type == 'root_cause':
                        update_status_for_node('strategy_planner_agent')
                    else:
                        update_status_for_node('router_agent')
                
                elif name == 'router_agent':
                    print(f"🎯 Router agent completed - checking outputs...")
                    
                    # Show success briefly before checking results
                    status_display.success("✅ Query execution complete")
                    await asyncio.sleep(0.5)
                    
                    # Check for various router outcomes
                    if state.get('sql_followup_but_new_question', False):
                        print("🔄 New question detected - will loop back to navigation")
                        update_status_for_node('navigation_controller')
                        continue
                    
                    if state.get('router_error_msg'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('router_error_msg'), message_type="error")
                        return
                    if state.get('needs_followup'):
                        hide_spinner()
                        status_display.empty()
                        # Get plan_approval_exists_flg from state
                        plan_approval_exists_flg = state.get('plan_approval_exists_flg', False)
                        # Pass the flag to add_assistant_message
                        add_assistant_message(
                            state.get('sql_followup_question'), 
                            message_type="needs_followup",
                            plan_approval_exists_flg=plan_approval_exists_flg
                        )
                        return
                    if state.get('requires_dataset_clarification') and state.get('dataset_followup_question'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('dataset_followup_question'), message_type="dataset_clarification")
                        return
                    if state.get('missing_dataset_items') and state.get('user_message'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('user_message'), message_type="missing_items")
                        return
                    if state.get('phi_found') and state.get('user_message'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('user_message'), message_type="phi_pii_error")
                        return
                    if state.get('sql_followup_topic_drift') and state.get('user_message'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('user_message'), message_type="error")
                        return
                    
                    sql_result = state.get('sql_result', {})
                    if sql_result and sql_result.get('success'):
                        hide_spinner()
                        status_display.empty()
                        
                        # Log SQL execution success
                        row_count = len(sql_result.get('data', [])) if sql_result.get('data') else 0
                        log_event('info', f"SQL executed successfully", row_count=row_count)
                        
                        functional_names = state.get('functional_names', [])
                        history_sql_used = state.get('history_sql_used', False)
                        if functional_names:
                            add_selection_reasoning_message(functional_names, history_sql_used)
                            print(f"✅ Added functional_names display: {functional_names} | history_sql_used: {history_sql_used}")
                        
                        rewritten_question = state.get('rewritten_question', initial_state['current_question'])
                        
                        # Extract selected_dataset (technical table names) from state for feedback tracking
                        selected_dataset = state.get('selected_dataset', [])
                        
                        # Extract sql_generation_story from state
                        sql_generation_story = state.get('sql_generation_story', '')
                        
                        # 🆕 DO NOT pass Power BI data here - it will be added by narrative agent
                        # Initial message has NO Power BI data (narrative agent will add it)
                        # Pass functional_names, selected_dataset, sql_story, and history_sql_used to sql_result for database tracking
                        add_sql_result_message(sql_result, rewritten_question, functional_names, None, selected_dataset, sql_generation_story, history_sql_used)
                        last_state = state
                        
                        st.session_state.sql_rendered = True
                        st.session_state.narrative_state = state
                        st.session_state.followup_state = state
                        
                        print("🔄 SQL results rendered - breaking")
                        break
                    else:
                        hide_spinner()
                        status_display.empty()
                        log_event('warning', "SQL returned no results")
                        add_assistant_message("No results available and please try different question.", message_type="error")
                        return
                
                elif name == 'strategy_planner_agent':
                    print(f"🧠 Strategic planner completed")
                    
                    if state.get('strategy_planner_err_msg'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('strategy_planner_err_msg'), message_type="error")
                        return
                    
                    strategic_results = state.get('strategic_query_results', [])
                    if strategic_results:
                        hide_spinner()
                        status_display.empty()
                        
                        strategic_reasoning = state.get('strategic_reasoning', '')
                        add_strategic_analysis_message(strategic_results, strategic_reasoning)
                        last_state = state
                        
                        drillthrough_exists = state.get('drillthrough_exists', '')
                        if drillthrough_exists == "Available":
                            st.session_state.strategic_rendered = True
                            st.session_state.drillthrough_state = state
                            break
                        else:
                            st.session_state.sql_rendered = True
                            st.session_state.followup_state = state
                            break
                    else:
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message("No strategic analysis results available.", message_type="error")
                        return
                
                elif name == 'drillthrough_planner_agent':
                    print(f"🔧 Drillthrough planner completed")
                    
                    if state.get('drillthrough_planner_err_msg'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('drillthrough_planner_err_msg'), message_type="error")
                        return
                    
                    drillthrough_results = state.get('drillthrough_query_results', [])
                    if drillthrough_results:
                        hide_spinner()
                        status_display.empty()
                        
                        drillthrough_reasoning = state.get('drillthrough_reasoning', '')
                        add_drillthrough_analysis_message(drillthrough_results, drillthrough_reasoning)
                        last_state = state
                        break
                    else:
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message("No drillthrough analysis results available.", message_type="error")
                        return
            
            elif et == 'workflow_end':
                log_event('info', "Workflow completed successfully")
                hide_spinner()
                status_display.empty()
                if not last_state:
                    last_state = state

        hide_spinner()
        
        # Handle follow-up generation
        if st.session_state.get('sql_rendered', False):
            print("🔄 SQL was rendered - follow-up will be handled in next execution cycle")
        elif last_state and last_state.get('sql_result', {}).get('success'):
            try:
                print("🔄 Starting follow-up generation")
                followup_state = last_state
                follow_res = await workflow.ainvoke_followup(followup_state, config)
                fq = follow_res.get('followup_questions', [])
                if fq:
                    st.session_state.current_followup_questions = fq
                    followup_placeholder.empty()
                    add_assistant_message("💡 **Would you like to explore further? Here are some suggested follow-up questions:**", message_type="followup_questions")
                    # Exit clarification mode - workflow is complete and follow-up questions are ready
                    st.session_state.in_clarification_mode = False
                else:
                    followup_placeholder.empty()
                    # Exit clarification mode even if no follow-up questions
                    st.session_state.in_clarification_mode = False
            except Exception as fe:
                followup_placeholder.error(f"Follow-up generation failed: {fe}")
                print(f"❌ Follow-up generation error: {fe}")
        elif last_state:
            print("ℹ️ No SQL results to generate follow-ups for")
        else:
            print("⚠️ No final state captured from workflow")
            
    except Exception as e:
        hide_spinner()
        status_display.empty()
        add_assistant_message(f"Workflow failed: {e}", message_type="error")
        log_event('error', f"Workflow failed: {str(e)}")

def run_streaming_workflow(workflow, user_query: str):
    asyncio.run(run_streaming_workflow_async(workflow, user_query))

## Removed legacy background follow-up + polling functions (replaced by direct streaming approach)

# ============ SESSION MANAGEMENT (keep your existing functions) ============

def ensure_db_client():
    """
    Ensure db_client exists in session state, recreate if missing.
    This handles cases where session state is cleared after inactivity.
    """
    if 'db_client' not in st.session_state or st.session_state.db_client is None:
        print("🔧 db_client not found in session - recreating...")
        try:
            st.session_state.db_client = DatabricksClient()
        except Exception as e:
            return None
    return st.session_state.db_client

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


def log_event(level: str, message: str, **extra):
    """
    Log an event with user context automatically included
    """
    user_email = get_authenticated_user()
    session_id = st.session_state.get('session_id', 'no_session')
    log_with_user_context(logger, level, message, user_email, session_id, **extra)


def get_available_datasets():
    """
    Get available datasets from metadata.json based on current domain selection.
    Returns a list of dataset names for the dropdown.
    """
    try:
        with open("config/metadata/metadata.json", "r") as f:
            metadata = json.load(f)
    except Exception as e:
        print(f"⚠️ Could not load metadata.json: {e}")
        return ["All Datasets"]
    
    # Get current domain selection
    domain_selection = st.session_state.get('domain_selection', None)
    
    datasets = set()
    
    if domain_selection:
        # If domain is selected, get datasets for that domain
        if isinstance(domain_selection, list):
            for domain in domain_selection:
                if domain in metadata:
                    for dataset_dict in metadata[domain]:
                        for dataset_name in dataset_dict.keys():
                            datasets.add(dataset_name)
        elif domain_selection in metadata:
            for dataset_dict in metadata[domain_selection]:
                for dataset_name in dataset_dict.keys():
                    datasets.add(dataset_name)
    else:
        # No domain filter - get all datasets
        for domain, dataset_list in metadata.items():
            for dataset_dict in dataset_list:
                for dataset_name in dataset_dict.keys():
                    datasets.add(dataset_name)
    
    # Sort and add "All Datasets" option at the start
    dataset_list = ["All Datasets"] + sorted(list(datasets))
    return dataset_list

def initialize_session_state():
    """
    Initialize session state variables directly. (Keep your existing implementation)
    """
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
        log_event('info', f"New chat session started: {st.session_state.session_id}")
    
    # Track the original session ID (the one we started with in this browser session)
    if 'original_session_id' not in st.session_state:
        st.session_state.original_session_id = st.session_state.session_id

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
    
    # Initialize question type tracking
    if 'question_type_selection' not in st.session_state:
        st.session_state.question_type_selection = "New Question"
        print(f"🔧 Question type initialized to 'New Question' for first question")
    
    # Track the last explicitly selected radio button value
    # This persists across questions to maintain continuity
    if 'last_radio_selection' not in st.session_state:
        st.session_state.last_radio_selection = "New Question"
        print(f"🔧 Last radio selection initialized to 'New Question'")
    
    # Track if at least one question has been asked (used only for first-time default)
    if 'first_question_asked' not in st.session_state:
        st.session_state.first_question_asked = False
        print(f"🔧 First question flag initialized to False")
    
    # Track if we're in clarification mode (should hide radio button)
    if 'in_clarification_mode' not in st.session_state:
        st.session_state.in_clarification_mode = False
        print(f"🔧 Clarification mode flag initialized to False")
    
    # Initialize dataset filter selection
    if 'selected_dataset_filter' not in st.session_state:
        st.session_state.selected_dataset_filter = []  # Empty list = all datasets
        print(f"🔧 Dataset filter initialized to 'All Datasets'")
    
    # Initialize user selected datasets (for multi-select UI)
    if 'user_selected_datasets' not in st.session_state:
        st.session_state.user_selected_datasets = []  # Empty list = all datasets
        print(f"🔧 User selected datasets initialized to empty (All Datasets)")
    
    # Track if user has explicitly loaded a historical session
    if 'session_explicitly_loaded' not in st.session_state:
        st.session_state.session_explicitly_loaded = False
        print(f"🔧 Session explicitly loaded flag initialized to False")

    # Initialize session cache refresh flag
    if 'refresh_session_cache' not in st.session_state:
        st.session_state.refresh_session_cache = False
    
    # Track if Cosmos state has been loaded for this session
    if 'cosmos_loaded' not in st.session_state:
        st.session_state.cosmos_loaded = False

    # Initialize plan approval state for button interactions
    if 'plan_approval_submitted' not in st.session_state:
        st.session_state.plan_approval_submitted = False
    if 'pending_user_input' not in st.session_state:
        st.session_state.pending_user_input = None

    # Authenticated user info should always come from main.py - don't initialize here
    # Just log what we have (or fallback if nothing was passed)
    authenticated_user = get_authenticated_user()
    authenticated_user_name = get_authenticated_user_name()
    
    # DEBUG: Log what we received from main.py
    print(f"🔍 DEBUG chat.py: authenticated_user = {authenticated_user}")
    print(f"🔍 DEBUG chat.py: authenticated_user_name = {authenticated_user_name}")
    print(f"🔍 DEBUG chat.py: session_state keys = {list(st.session_state.keys())}")
    
    # Log user login (only meaningful log)
    if authenticated_user != 'unknown_user':
        log_event('info', f"User authenticated: {authenticated_user_name}")
    
    # Warning if we're using fallback values (indicates main.py didn't pass user info)
    if authenticated_user == 'unknown_user':
        log_event('warning', "Using fallback user - main.py may not have passed user info")


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

def format_sql_followup_question(text):
    """Format SQL followup question with color highlighting for dataset and note sections
    
    - SELECTED DATASET section: Entire line in Dark Blue color (#003366) with bold
    - NOTE section onwards: Red color (#D74120) with italic for clear attention
    - Rest: Black (default) for main question
    """
    if not text:
        return ""
    
    import re
    
    # Don't escape HTML yet - we need to check for patterns first
    # Split into lines for processing
    lines = text.split('\n')
    formatted_lines = []
    in_note_section = False
    
    for line in lines:
        stripped_line = line.strip()
        
        # Escape HTML special characters for this line
        escaped_line = stripped_line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        
        # Check if this is the SELECTED DATASET line (case-insensitive) - make entire line dark blue
        if 'SELECTED DATASET:' in stripped_line.upper():
            # Entire line in dark blue with bold (including full table/dataset name)
            formatted_line = f'<span style="color: #003366; font-weight: 700;">{escaped_line}</span>'
        
        # Check if we're starting the NOTE section
        elif stripped_line.upper().startswith('NOTE:'):
            in_note_section = True
            # Use italic style and red color for NOTE section
            formatted_line = f'<span style="color: #D74120; font-style: italic;">{escaped_line}</span>'
        
        # If we're in the NOTE section, continue with red and italic
        elif in_note_section:
            formatted_line = f'<span style="color: #D74120; font-style: italic;">{escaped_line}</span>'
        
        # Otherwise, keep default black color for main question
        else:
            formatted_line = f'<span style="color: #000000;">{escaped_line}</span>'
        
        formatted_lines.append(formatted_line)
    
    # Join with <br> tags for line breaks
    result = '<br>'.join(formatted_lines)
    
    # Apply bold formatting to ** markers if any (process after color spans are added)
    result = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', result)
    
    return result

def format_plan_approval_content(text):
    """Format plan approval content with color highlighting for SQL plan summary display

    - Headers with emojis (📋🎯📊📌🔍🔗📈💡): Bold dark blue
    - Warning section (⚠️ Why Asking): Red/orange
    - Options line: Green emphasis
    - Note section: Gray italic
    - Separators (━): Gray
    """
    if not text:
        return ""

    lines = text.split('\n')
    formatted_lines = []

    for line in lines:
        stripped = line.strip()
        # Escape HTML special characters
        escaped = stripped.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

        # Headers with emojis - bold dark blue
        if any(e in stripped for e in ['📋', '🎯', '📊', '📌', '🔍', '🔗', '📈', '💡']):
            formatted = f'<span style="color: #003366; font-weight: 700;">{escaped}</span>'
        # Warning - red/orange
        elif '⚠️' in stripped or 'Why Asking' in stripped:
            formatted = f'<span style="color: #D74120; font-weight: 600;">{escaped}</span>'
        # Options - green
        elif 'Options:' in stripped:
            formatted = f'<span style="color: #2E7D32; font-weight: 600;">{escaped}</span>'
        # Separators - gray
        elif stripped.startswith('━'):
            formatted = f'<span style="color: #666;">{escaped}</span>'
        # Note - italic gray
        elif stripped.upper().startswith('NOTE:'):
            formatted = f'<span style="color: #D74120; font-style: italic;">{escaped}</span>'
        # Bullet points - keep default but with proper formatting
        elif stripped.startswith('•') or stripped.startswith('-'):
            formatted = f'<span style="color: #333; margin-left: 8px;">{escaped}</span>'
        else:
            formatted = escaped

        formatted_lines.append(formatted)

    return '<br>'.join(formatted_lines)

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
        is_id_column = any(id_pattern in column_name.lower() for id_pattern in ['client_id', 'member_id', 'patient_id', 'id','ndc_code','ndc'])
        
        # Check if this is a percentage column
        is_percentage_column = any(pct_pattern in column_name.lower() for pct_pattern in ['percent', 'pct', '_pct', 'ratio'])
        
        # Check if this is a monetary column
        is_monetary_column = any(money_pattern in column_name.lower() for money_pattern in ['revenue', 'expense', '_amount', 'amt', 'cost', 'fee'])
        
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
                    return f"{numeric_val:.2f}%"
                else:
                    # Value already in percentage form (e.g., 25.5), just add %
                    return f"{numeric_val:.2f}%"
            
            # Handle monetary columns - add $ symbol and remove decimals
            # Also add red color styling for negative values
            if is_monetary_column:
                rounded_val = int(round(numeric_val))
                if rounded_val < 0:
                    # Negative value - format with red color
                    if abs(rounded_val) >= 1000:
                        return f"{rounded_val:,}"  # Red styling applied via CSS class
                    else:
                        return f"{rounded_val}"
                else:
                    if abs(rounded_val) >= 1000:
                        return f"{rounded_val:,}"
                    else:
                        return f"{rounded_val}"

            
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

def render_feedback_section(title, sql_query, data, narrative, user_question=None, message_idx=None, table_name=None, sub_index=None):
    """Render feedback section with thumbs up/down buttons - only for current question"""
    
    # NOTE: We trust the show_feedback parameter passed from render_single_sql_result
    # The historical marking is already handled in start_processing and checked before calling this function
    
    # Create a unique key for this feedback section
    # Include session_id, message_idx, AND sub_index for uniqueness
    session_id = st.session_state.get('session_id', 'default')
    
    if message_idx is not None:
        if sub_index is not None:
            # Multiple SQL results in same message - include sub_index
            feedback_key = f"feedback_{session_id}_msg_{message_idx}_sub_{sub_index}"
        else:
            # Single SQL result in message
            feedback_key = f"feedback_{session_id}_msg_{message_idx}"
    else:
        # Fallback for edge cases
        timestamp = int(time.time() * 1000)
        feedback_key = f"feedback_{session_id}_{timestamp}"
    
    print(f"🔑 Generated feedback key: {feedback_key} | message_idx={message_idx} | sub_index={sub_index}")
    
    # Show feedback buttons if not already submitted
    if not st.session_state.get(f"{feedback_key}_submitted", False):
        st.markdown("**Was this helpful?** ")
        
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("👍 Yes, helpful", key=f"{feedback_key}_thumbs_up"):
                # Mark as submitted immediately (optimistic UI)
                st.session_state[f"{feedback_key}_submitted"] = True
                
                # Get feedback data
                rewritten_question = user_question or st.session_state.get('current_query', title)
                
                sql_result_dict = {
                    'title': title,
                    'sql_query': sql_query,
                    'query_results': data,
                    'narrative': narrative,
                    'user_question': user_question
                }
                
                # Run feedback insertion - handle event loop conflicts
                try:
                    try:
                        # Try asyncio.run() first (works when no event loop is running)
                        success = asyncio.run(_insert_feedback_row(
                            rewritten_question, sql_query, True, table_name
                        ))
                    except RuntimeError as re:
                        # Event loop already running - use alternative approach
                        if "already running" in str(re):
                            loop = asyncio.get_event_loop()
                            success = loop.run_until_complete(_insert_feedback_row(
                                rewritten_question, sql_query, True, table_name
                            ))
                        else:
                            raise re

                    if success:
                        log_event('info', "Positive feedback submitted", feedback_type="positive")
                        st.success("Thank you for your feedback! 👍")
                    else:
                        print(f"⚠️ Feedback submission returned False")
                        st.warning("Feedback submission encountered an issue, but we've noted it.")
                except Exception as e:
                    print(f"❌ Feedback error: {e}")
                    log_event('error', f"Feedback failed: {str(e)}", error_type=type(e).__name__)
                    st.error("Failed to submit feedback. Please try again.")

                time.sleep(1.0)
                st.rerun()
        
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
                        # Mark as submitted immediately (optimistic UI)
                        st.session_state[f"{feedback_key}_submitted"] = True
                        st.session_state[f"{feedback_key}_show_form"] = False
                        
                        # Get feedback data
                        rewritten_question = user_question or st.session_state.get('current_query', title)
                        
                        sql_result_dict = {
                            'title': title,
                            'sql_query': sql_query,
                            'query_results': data,
                            'narrative': narrative,
                            'user_question': user_question
                        }
                        
                        # Run feedback insertion - handle event loop conflicts
                        try:
                            try:
                                # Try asyncio.run() first (works when no event loop is running)
                                success = asyncio.run(_insert_feedback_row(
                                    rewritten_question, sql_query, False, table_name, feedback_text
                                ))
                            except RuntimeError as re:
                                # Event loop already running - use alternative approach
                                if "already running" in str(re):
                                    loop = asyncio.get_event_loop()
                                    success = loop.run_until_complete(_insert_feedback_row(
                                        rewritten_question, sql_query, False, table_name, feedback_text
                                    ))
                                else:
                                    raise re

                            if success:
                                log_event('info', "Negative feedback submitted", feedback_type="negative")
                                st.success("Thank you for your feedback! We'll work on improving.")
                            else:
                                print(f"⚠️ Negative feedback submission returned False")
                                st.warning("Feedback submission encountered an issue, but we've noted it.")
                        except Exception as e:
                            print(f"❌ Negative feedback error: {e}")
                            log_event('error', f"Negative feedback failed: {str(e)}", error_type=type(e).__name__)
                            st.error("Failed to submit feedback. Please try again.")

                        time.sleep(1.0)
                        st.rerun()
                
                with col2:
                    if st.form_submit_button("Cancel"):
                        st.session_state[f"{feedback_key}_show_form"] = False
                        st.rerun()
    else:
        # Show feedback submitted message
        st.markdown("---")
        st.success("✅ Thank you for your feedback!")

def render_sql_results(sql_result, rewritten_question=None, show_feedback=True, message_idx=None, functional_names=None, powerbi_data=None):
    """Render SQL results with title, expandable SQL query, data table, and narrative"""
    
    print(f"📊 render_sql_results called with: type={type(sql_result)}, show_feedback={show_feedback}, powerbi_data={powerbi_data is not None}")
    
    # Ensure sql_result is a dictionary
    if not isinstance(sql_result, dict):
        st.error(f"Error: Invalid SQL result format (expected dict, got {type(sql_result).__name__})")
        return
    
    # Get user_question from sql_result for feedback
    user_question = sql_result.get('user_question')
    
    # Get functional_names if not passed as parameter (for historical display)
    if not functional_names and 'functional_names' in sql_result:
        functional_names = sql_result.get('functional_names')
    
    # Get powerbi_data if not passed as parameter
    if not powerbi_data and 'powerbi_data' in sql_result:
        powerbi_data = sql_result.get('powerbi_data')
    
    # Get sql_generation_story if available
    sql_generation_story = sql_result.get('sql_generation_story', '')
    # Get table_name for feedback tracking (technical name)
    table_name = sql_result.get('selected_dataset')
    # Get chart_spec for visualization
    chart_spec = sql_result.get('chart_spec', None)

    # Determine if this is historical (inverse of show_feedback)
    is_historical = not show_feedback
    
    # Check if multiple results
    if sql_result.get('multiple_results', False):
        # Display SQL Plan once at the top for multiple queries using HTML details/summary
        if sql_generation_story and sql_generation_story.strip():
            safe_sql_story = html.escape(sql_generation_story)
            st.markdown(f"""
            <details style="margin: 5px 0; border: 1px solid #e1e5e9; border-radius: 6px; padding: 0;">
                <summary style="background-color: #f8f9fa; padding: 8px 12px; cursor: pointer; border-radius: 6px 6px 0 0; font-weight: 500; color: #495057;">
                    📋 View SQL Plan
                </summary>
                <div style="padding: 12px; background-color: #f8f9fa; border-radius: 0 0 6px 6px;">
                    <pre style="white-space: pre-wrap; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; font-size: 13px; line-height: 1.6; color: #2a2a2a;">{safe_sql_story}</pre>
                </div>
            </details>
            """, unsafe_allow_html=True)

        query_results = sql_result.get('query_results', [])
        for i, result in enumerate(query_results):
            title = result.get('title', f'Query {i+1}')
            sql_query = result.get('sql_query', '')
            data = result.get('data', [])
            narrative = result.get('narrative', '')
            
            # ⭐ Pass sub_index (i) AND is_historical AND functional_names AND table_name AND powerbi_data AND chart_spec AND history_sql_used, but NO sql_story (already shown once)
            render_single_sql_result(
                title, sql_query, data, narrative,
                user_question, show_feedback, message_idx,
                functional_names, sub_index=i, is_historical=is_historical, table_name=table_name, powerbi_data=powerbi_data, sql_story=None, chart_spec=chart_spec
            )
    else:
        # Single result - use rewritten_question if available, otherwise default
        title = rewritten_question if rewritten_question else "Analysis Results"
        sql_query = sql_result.get('sql_query', '')
        data = sql_result.get('query_results', [])
        narrative = sql_result.get('narrative', '')
        
        # ⭐ No sub_index needed for single result, but pass is_historical, functional_names, table_name, powerbi_data, sql_story, chart_spec, and history_sql_used
        render_single_sql_result(
            title, sql_query, data, narrative,
            user_question, show_feedback, message_idx,
            functional_names, sub_index=None, is_historical=is_historical, table_name=table_name, powerbi_data=powerbi_data, sql_story=sql_generation_story, chart_spec=chart_spec
        )


def render_single_sql_result(title, sql_query, data, narrative, user_question=None, show_feedback=True, message_idx=None, functional_names=None, sub_index=None, is_historical=False, table_name=None, powerbi_data=None, sql_story=None, chart_spec=None):
    """Render a single SQL result with warm gold background for title and narrative

    Args:
        functional_names: User-friendly dataset names for display (e.g., "PBM Claims")
        table_name: Technical table name for feedback tracking (e.g., "pbm_claims")
        powerbi_data: Power BI report matching data from narrative agent
        sql_story: Business-friendly explanation of SQL generation (2-3 lines)
        chart_spec: LLM-generated chart specification for visualization
    """

    # Create two-column layout for SQL Plan and SQL Query expanders
    col1, col2 = st.columns(2)

    with col1:
        # SQL Plan in collapsible section using HTML details/summary
        if sql_story and sql_story.strip():
            safe_sql_story = html.escape(sql_story)
            st.markdown(f"""
            <details style="margin: 5px 0; border: 1px solid #e1e5e9; border-radius: 6px; padding: 0;">
                <summary style="background-color: #f8f9fa; padding: 8px 12px; cursor: pointer; border-radius: 6px 6px 0 0; font-weight: 500; color: #495057;">
                    📋 View SQL Plan
                </summary>
                <div style="padding: 12px; background-color: #f8f9fa; border-radius: 0 0 6px 6px;">
                    <pre style="white-space: pre-wrap; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; font-size: 13px; line-height: 1.6; color: #2a2a2a;">{safe_sql_story}</pre>
                </div>
            </details>
            """, unsafe_allow_html=True)

    with col2:
        # SQL Query in collapsible section using HTML details/summary
        if sql_query:
            # Ensure sql_query is a string and clean it
            if not isinstance(sql_query, str):
                sql_query = str(sql_query)

            # Remove any [object Object] artifacts that may have been injected
            sql_query = sql_query.replace('[object Object]', '').replace(',[object Object],', '')

            # Escape HTML characters in SQL query to prevent rendering issues
            escaped_sql = html.escape(sql_query)

            st.markdown(f"""
            <details style="margin: 5px 0; border: 1px solid #e1e5e9; border-radius: 6px; padding: 0;">
                <summary style="background-color: #f8f9fa; padding: 8px 12px; cursor: pointer; border-radius: 6px 6px 0 0; font-weight: 500; color: #495057;">
                    🔍 View SQL Query
                </summary>
                <div style="padding: 12px; background-color: #f8f9fa; border-radius: 0 0 6px 6px;">
                    <pre style="background-color: #2d3748; color: #e2e8f0; padding: 12px; border-radius: 4px; overflow-x: auto; font-family: 'Courier New', monospace; font-size: 12px; line-height: 1.4; margin: 0; white-space: pre-wrap; word-wrap: break-word;"><code>{escaped_sql}</code></pre>
                </div>
            </details>
            """, unsafe_allow_html=True)
    
    # Data table - show directly without expander
    if data:
        formatted_df = format_sql_data_for_streamlit(data)
        row_count = len(formatted_df) if hasattr(formatted_df, 'shape') else 0
        
        st.markdown("**📊 Data Output:**")
        if row_count > 10000:
            st.warning("⚠️ SQL query output exceeds more than 10000 rows. Please rephrase your query to reduce the result size.")
        elif not formatted_df.empty:
            st.dataframe(
                formatted_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No data to display")

        # Display chart visualization in checkbox expander (only if chart is available)
        if chart_spec and chart_spec.get('render', False):
            try:
                # Convert data to DataFrame for chart (use raw data, not formatted)
                chart_df = pd.DataFrame(data) if isinstance(data, list) else data
                if not chart_df.empty:
                    # Use checkbox for chart to allow expand/collapse
                    chart_key = f"show_chart_{hash(str(data)[:100]) if data else 'empty'}_{st.session_state.get('message_counter', 0)}"
                    show_chart = st.checkbox("📈 View Visual (Click to expand)", key=chart_key, value=False)
                    
                    if show_chart:
                        render_chart_from_spec(chart_spec, chart_df)
            except Exception as e:
                st.error(f"❌ Chart display error: {e}")
                print(f"❌ Chart display error: {e}")

        # Add feedback buttons after narrative (only for current session)
        if show_feedback:
            # ⭐ Pass sub_index and table_name to render_feedback_section
            render_feedback_section(title, sql_query, data, narrative, user_question, message_idx, table_name, sub_index)
    
    # 🆕 DISPLAY NARRATIVE AND POWER BI REPORT (Power BI below narrative, not side-by-side)
    # Check if we have Power BI data to display (ONLY if report_found is true)
    has_powerbi_info = False
    if powerbi_data:
        report_found = powerbi_data.get('report_found', False)
        report_filter = powerbi_data.get('report_filter', '')
        report_url = powerbi_data.get('report_url', '')
        # Show Power BI section ONLY if report_found is true
        has_powerbi_info = report_found
        print(f"   has_powerbi_info={has_powerbi_info}")
    else:
        print(f"⚠️ No Power BI data provided to render_single_sql_result")
    
    # Display Narrative Insights (full width) - Optum styled insights card
    if narrative:
        safe_narrative_html = convert_text_to_safe_html(narrative)
        st.markdown(f"""
        <div class="insights-card">
            <div class="insights-header">
                <div class="insights-icon">💡</div>
                <div class="insights-title">Key Insights</div>
            </div>
            <div style="color: #2a2a2a; font-size: 14px; line-height: 1.7;">
                {safe_narrative_html}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Display Power BI Report Info BELOW narrative (only if report_found is true) - Optum styled
    if has_powerbi_info:
        report_found = powerbi_data.get('report_found', False)
        report_name = powerbi_data.get('report_name', '')
        report_url = powerbi_data.get('report_url', '')
        report_filter = powerbi_data.get('report_filter', '')
        
        # Build filter tags HTML
        filter_tags_html = ''
        if report_filter and report_filter.strip():
            # Parse filter string and create individual tags
            filters = report_filter.split(',')
            for f in filters:
                f = f.strip()
                if f:
                    filter_tags_html += f'<span class="filter-tag">{html.escape(f)}</span>'
        
        # Build the Optum styled report card
        report_card_html = f'''
        <div class="report-card">
            <div class="report-label">
                <span>📈</span> Recommended Report to reconcile
            </div>
            <div class="report-title">{html.escape(report_name)}</div>
            <div class="report-filters">
                {filter_tags_html if filter_tags_html else '<span class="filter-tag">No filters specified</span>'}
            </div>
            <a href="{html.escape(report_url)}" target="_blank" class="open-report-btn">
                Open Report <span>→</span>
            </a>
        </div>
        '''
        
        st.markdown(report_card_html, unsafe_allow_html=True)


def preprocess_chart_data(df: pd.DataFrame, chart_spec: Dict) -> tuple[pd.DataFrame, Dict, str]:
    """
    Simplified preprocessing for chart data.

    Only handles top_n truncation - LLM now makes all complex decisions.

    Returns:
        tuple: (filtered_df, updated_chart_spec, warning_message)
    """
    if df is None or df.empty:
        return df, chart_spec, ""

    warning_parts = []
    filtered_df = df.copy()
    updated_spec = chart_spec.copy()

    # ============ TOP_N TRUNCATION ============
    # If LLM specified top_n, sort by y_column and take top N
    top_n = chart_spec.get('top_n')
    y_col = chart_spec.get('y_column')

    if top_n and y_col and isinstance(y_col, str) and y_col in filtered_df.columns:
        original_len = len(filtered_df)

        # Convert y_col to numeric for sorting
        filtered_df[y_col] = pd.to_numeric(
            filtered_df[y_col].astype(str).str.replace(',', '').str.replace('$', '').str.replace('%', ''),
            errors='coerce'
        )

        # Sort by value descending and take top N
        filtered_df = filtered_df.sort_values(by=y_col, ascending=False).head(top_n)

        if len(filtered_df) < original_len:
            print(f"✅ Top {top_n} filtering: Showing {len(filtered_df)} of {original_len} rows")
            warning_parts.append(f"Showing top {len(filtered_df)} by {y_col}")

    # ============ VALIDATION: Ensure we have enough data ============
    if len(filtered_df) < 2:
        updated_spec['render'] = False
        updated_spec['reason'] = "Insufficient data for visualization"
        warning_parts.append("Chart skipped: Not enough data points")
        print(f"⚠️ Chart disabled: Only {len(filtered_df)} rows")

    # ============ SAFETY CHECK: Verify y_column is numeric ============
    if y_col and isinstance(y_col, str) and y_col in filtered_df.columns:
        test_numeric = pd.to_numeric(
            filtered_df[y_col].astype(str).str.replace(',', '').str.replace('$', '').str.replace('%', ''),
            errors='coerce'
        )
        if test_numeric.isna().all():
            print(f"⚠️ Safety check: y_column '{y_col}' has no numeric data")
            updated_spec['render'] = False
            updated_spec['reason'] = f"Column '{y_col}' contains no numeric values"
            warning_parts.append(f"Chart skipped: '{y_col}' is not numeric")

    # Build warning message
    warning_message = ""
    if warning_parts:
        warning_message = "📊 **Chart Note:** " + " | ".join(warning_parts)

    return filtered_df, updated_spec, warning_message


def render_chart_from_spec(chart_spec: Dict, df: pd.DataFrame) -> None:
    """
    Render a Plotly chart based on LLM-generated specification.

    SIMPLIFIED: Only supports 'line', 'bar', and 'bar_grouped' chart types.

    SAFETY: If anything goes wrong, we silently skip the chart rather than
    showing wrong data or breaking the UI. Better no chart than a wrong chart.

    Args:
        chart_spec: Dictionary with chart configuration from LLM
        df: DataFrame with the data to visualize
    """
    # ============ VALIDATION: Check chart_spec ============
    if not chart_spec or not isinstance(chart_spec, dict):
        print("ℹ️ Chart skipped: No valid chart_spec provided")
        return

    if not chart_spec.get('render', False):
        print(f"ℹ️ Chart skipped: render=False, reason={chart_spec.get('reason', 'Not specified')}")
        return

    chart_type = chart_spec.get('chart_type', 'none')
    if chart_type not in ['line', 'bar', 'bar_grouped']:
        print(f"ℹ️ Chart skipped: Unsupported chart_type '{chart_type}'. Only 'line', 'bar', and 'bar_grouped' supported.")
        return

    # ============ VALIDATION: Check DataFrame ============
    if df is None or df.empty:
        print("ℹ️ Chart skipped: DataFrame is empty or None")
        return

    # ============ PREPROCESSING: Handle top_n truncation ============
    df, chart_spec, chart_warning = preprocess_chart_data(df, chart_spec)

    # Check if preprocessing disabled the chart
    if not chart_spec.get('render', False):
        print(f"ℹ️ Chart skipped after preprocessing: {chart_spec.get('reason', 'Unknown')}")
        if chart_warning:
            st.markdown(f"<div style='color: #666; font-size: 0.85rem; padding: 8px; background: #f5f5f5; border-radius: 4px; margin: 8px 0;'>{chart_warning}</div>", unsafe_allow_html=True)
        return

    # Display chart warning/note if any
    if chart_warning:
        st.markdown(f"<div style='color: #666; font-size: 0.85rem; padding: 8px; background: #FFF8E7; border-left: 3px solid #FFA726; border-radius: 4px; margin: 8px 0;'>{chart_warning}</div>", unsafe_allow_html=True)

    if len(df) < 2:
        print(f"ℹ️ Chart skipped: Only {len(df)} row(s) - not enough data to visualize")
        return

    try:
        title = chart_spec.get('title', 'Data Visualization')
        x_col = chart_spec.get('x_column')
        y_col = chart_spec.get('y_column')
        group_col = chart_spec.get('group_column')  # For multi-line charts

        # ============ VALIDATION: Check columns ============
        if not x_col:
            print("⚠️ Chart skipped: x_column not specified")
            return

        if not y_col:
            print("⚠️ Chart skipped: y_column not specified")
            return

        if x_col not in df.columns:
            print(f"⚠️ Chart skipped: x_column '{x_col}' not found. Available: {list(df.columns)}")
            return

        # Validate y_column(s)
        if isinstance(y_col, str):
            if y_col not in df.columns:
                print(f"⚠️ Chart skipped: y_column '{y_col}' not found. Available: {list(df.columns)}")
                return
        elif isinstance(y_col, list):
            missing_cols = [c for c in y_col if c not in df.columns]
            if missing_cols:
                print(f"⚠️ Chart skipped: y_columns {missing_cols} not found. Available: {list(df.columns)}")
                return

        # Validate group_column if specified
        if group_col and group_col not in df.columns:
            print(f"⚠️ Chart skipped: group_column '{group_col}' not found. Available: {list(df.columns)}")
            return

        # Optum-inspired color palette
        colors = ['#FF612B', '#2D8CFF', '#4CAF50', '#FFA726', '#7E57C2',
                  '#26A69A', '#EF5350', '#5C6BC0', '#66BB6A', '#FFCA28']

        # Create a copy of dataframe for plotting
        plot_df = df.copy()

        # ============ CONVERT y_column TO NUMERIC ============
        if isinstance(y_col, str) and y_col in plot_df.columns:
            plot_df[y_col] = pd.to_numeric(
                plot_df[y_col].astype(str).str.replace(',', '').str.replace('$', '').str.replace('%', ''),
                errors='coerce'
            )
            if plot_df[y_col].isna().all():
                print(f"⚠️ Chart skipped: y_column '{y_col}' has no valid numeric data")
                return
        elif isinstance(y_col, list):
            for col in y_col:
                if col in plot_df.columns:
                    plot_df[col] = pd.to_numeric(
                        plot_df[col].astype(str).str.replace(',', '').str.replace('$', '').str.replace('%', ''),
                        errors='coerce'
                    )

        # ============ DROP NaN ROWS ============
        cols_to_check = [x_col] + (y_col if isinstance(y_col, list) else [y_col])
        if group_col:
            cols_to_check.append(group_col)
        original_len = len(plot_df)
        plot_df = plot_df.dropna(subset=cols_to_check)
        if len(plot_df) < original_len:
            print(f"ℹ️ Chart: Dropped {original_len - len(plot_df)} rows with NaN values")

        if plot_df.empty or len(plot_df) < 2:
            print(f"⚠️ Chart skipped: After cleaning, only {len(plot_df)} valid row(s) remain")
            return

        fig = None

        # ============ LINE CHART ============
        if chart_type == 'line':
            if group_col:
                # Multi-line chart with group_column as color/legend
                fig = px.line(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color=group_col,
                    title=title,
                    markers=True,
                    color_discrete_sequence=colors
                )
            else:
                # Single line chart
                fig = px.line(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    title=title,
                    markers=True,
                    color_discrete_sequence=colors
                )
            fig.update_layout(xaxis_title=x_col, yaxis_title=y_col if isinstance(y_col, str) else 'Value')

        # ============ BAR CHART (Category Rankings - Top 10 drugs, etc.) ============
        elif chart_type == 'bar':
            # Sort by value descending for ranking charts (highest first)
            if isinstance(y_col, str):
                plot_df = plot_df.sort_values(by=y_col, ascending=False)

            # Vertical bar chart
            fig = px.bar(
                plot_df,
                x=x_col,
                y=y_col,
                title=title,
                color_discrete_sequence=colors
            )
            fig.update_layout(
                xaxis_title=x_col,
                yaxis_title=y_col if isinstance(y_col, str) else 'Value'
            )

        # ============ BAR_GROUPED CHART (Actuals vs Forecast) ============
        elif chart_type == 'bar_grouped':
            fig = go.Figure()
            if isinstance(y_col, list) and len(y_col) >= 2:
                for i, col in enumerate(y_col):
                    fig.add_trace(go.Bar(
                        x=plot_df[x_col],
                        y=plot_df[col],
                        name=col,
                        marker_color=colors[i % len(colors)]
                    ))
                fig.update_layout(
                    barmode='group',
                    title=title,
                    xaxis_title=x_col,
                    yaxis_title='Value'
                )
            else:
                print(f"⚠️ Chart skipped: bar_grouped requires y_column to be a list of 2+ columns")
                return

        if fig:
            # Apply consistent styling
            fig.update_layout(
                template='plotly_white',
                font=dict(family='Inter, sans-serif', size=12),
                title_font_size=14,
                title_x=0.5,
                margin=dict(l=40, r=40, t=60, b=40),
                legend=dict(orientation='h', yanchor='bottom', y=-0.2, xanchor='center', x=0.5)
            )

            # Render in Streamlit with styled container
            st.markdown("""
            <div style="background: linear-gradient(135deg, #FAF8F2 0%, #FFFFFF 100%);
                        border: 1px solid #EDE8E0; border-radius: 8px; padding: 12px; margin: 10px 0;">
                <div style="display: flex; align-items: center; margin-bottom: 8px;">
                    <span style="font-size: 1rem; margin-right: 8px;">📊</span>
                    <span style="color: #2a2a2a; font-weight: 600; font-size: 0.9rem;">Data Visualization</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        # SAFETY: Never break the UI - skip chart silently
        import traceback
        print(f"❌ Chart rendering error: {e}")
        print(f"   Chart spec: {chart_spec}")
        print(f"   Traceback: {traceback.format_exc()}")


def render_strategic_analysis(strategic_results, strategic_reasoning=None, show_feedback=True):
    """Render strategic analysis response with reasoning and multiple strategic queries"""
    
    
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

def render_single_strategic_result(title, sql_query, data, narrative, index, show_feedback=True):
    """Render a single strategic analysis result"""
    
    # Title with strategic analysis styling
    st.markdown(f"""
    <div class="narrative-content">
        <strong>🎯 Strategic Analysis #{index}:</strong> {title}
    </div>
    """, unsafe_allow_html=True)
    
    # SQL Query in collapsible section using HTML details/summary
    if sql_query:
        # Ensure sql_query is a string and clean it
        if not isinstance(sql_query, str):
            sql_query = str(sql_query)
        
        # Remove any [object Object] artifacts that may have been injected
        sql_query = sql_query.replace('[object Object]', '').replace(',[object Object],', '')
        
        # Escape HTML characters in SQL query to prevent rendering issues
        escaped_sql = html.escape(sql_query)
        
        # Use HTML details/summary for SQL query expander
        st.markdown(f"""
        <details style="margin: 10px 0; border: 1px solid #e1e5e9; border-radius: 6px; padding: 0;">
            <summary style="background-color: #f8f9fa; padding: 8px 12px; cursor: pointer; border-radius: 6px 6px 0 0; font-weight: 500; color: #495057;">
                🔍 View Strategic SQL Query #{index} (Click to expand)
            </summary>
            <div style="padding: 12px; background-color: #f8f9fa; border-radius: 0 0 6px 6px;">
                <pre style="background-color: #2d3748; color: #e2e8f0; padding: 12px; border-radius: 4px; overflow-x: auto; font-family: 'Courier New', monospace; font-size: 12px; line-height: 1.4; margin: 0; white-space: pre-wrap; word-wrap: break-word;"><code>{escaped_sql}</code></pre>
            </div>
        </details>
        """, unsafe_allow_html=True)
    
    # Data table - show directly without expander
    if data:
        formatted_df = format_sql_data_for_streamlit(data)
        row_count = len(formatted_df) if hasattr(formatted_df, 'shape') else 0
        
        st.markdown(f"**📊 Strategic Data Output #{index}:**")
        if row_count > 10000:
            st.warning("⚠️ SQL query output exceeds more than 10000 rows. Please rephrase your query to reduce the result size.")
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

def render_single_drillthrough_result(title, sql_query, data, narrative, causational_insights, index, show_feedback=True):
    """Render a single drillthrough analysis result"""
    
    # Title with drillthrough analysis styling
    st.markdown(f"""
    <div class="narrative-content">
        <strong>🔍 Drillthrough Analysis #{index}:</strong> {title}
    </div>
    """, unsafe_allow_html=True)
    
    # SQL Query in collapsible section using HTML details/summary
    if sql_query:
        # Ensure sql_query is a string and clean it
        if not isinstance(sql_query, str):
            sql_query = str(sql_query)
        
        # Remove any [object Object] artifacts that may have been injected
        sql_query = sql_query.replace('[object Object]', '').replace(',[object Object],', '')
        
        # Escape HTML characters in SQL query to prevent rendering issues
        escaped_sql = html.escape(sql_query)
        
        # Use HTML details/summary for SQL query expander
        st.markdown(f"""
        <details style="margin: 10px 0; border: 1px solid #e1e5e9; border-radius: 6px; padding: 0;">
            <summary style="background-color: #f8f9fa; padding: 8px 12px; cursor: pointer; border-radius: 6px 6px 0 0; font-weight: 500; color: #495057;">
                🔍 View Drillthrough SQL Query #{index} (Click to expand)
            </summary>
            <div style="padding: 12px; background-color: #f8f9fa; border-radius: 0 0 6px 6px;">
                <pre style="background-color: #2d3748; color: #e2e8f0; padding: 12px; border-radius: 4px; overflow-x: auto; font-family: 'Courier New', monospace; font-size: 12px; line-height: 1.4; margin: 0; white-space: pre-wrap; word-wrap: break-word;"><code>{escaped_sql}</code></pre>
            </div>
        </details>
        """, unsafe_allow_html=True)
    
    # Data table - show directly without expander
    if data:
        formatted_df = format_sql_data_for_streamlit(data)
        row_count = len(formatted_df) if hasattr(formatted_df, 'shape') else 0
        
        st.markdown(f"**📊 Drillthrough Data Output #{index}:**")
        if row_count > 10000:
            st.warning("⚠️ SQL query output exceeds more than 10000 rows. Please rephrase your query to reduce the result size.")
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

def add_assistant_message(content, message_type="standard", plan_approval_exists_flg=False):
    """
    Add assistant message to chat history - used for both real-time and preserved history
    
    Args:
        content: Message content
        message_type: Type of message (standard, needs_followup, etc.)
        plan_approval_exists_flg: Flag indicating if this is a plan approval message
    """
    message = {
        "role": "assistant",
        "content": content,
        "message_type": message_type,
        "timestamp": datetime.now().isoformat(),
        "plan_approval_exists_flg": plan_approval_exists_flg
    }
    
    st.session_state.messages.append(message)
    content_preview = (content or '')[:50] + "..." if content and len(content) > 50 else (content or '')
    print(f"✅ Added {message_type} message (total messages now: {len(st.session_state.messages)}): {content_preview}")
    
    # Debug: Show current message state
    for i, msg in enumerate(st.session_state.messages[-3:]):  # Show last 3 messages
        role = msg.get('role', 'unknown')
        msg_type = msg.get('message_type', 'standard')
        msg_content = msg.get('content', '') or ''
        content_preview = msg_content[:30] + "..." if len(msg_content) > 30 else msg_content
        print(f"  Message {len(st.session_state.messages)-3+i}: {role}({msg_type}) - {content_preview}")
    
    # Handle special state updates
    if message_type == "domain_clarification":
        st.session_state.current_followup_questions = []
        # Enter clarification mode - hide radio button
        st.session_state.in_clarification_mode = True
    
    # Set clarification mode for router-generated clarification questions
    if message_type in ["needs_followup", "dataset_clarification"]:
        st.session_state.in_clarification_mode = True

def add_selection_reasoning_message(functional_names, history_sql_used=False):
    """
    Add functional_names message to chat history for proper rendering
    functional_names: list of strings representing selected datasets
    history_sql_used: boolean indicating if historical SQL pattern was used
    """
    message = {
        "role": "assistant",
        "content": functional_names,  # Store as list instead of string
        "message_type": "selection_reasoning",
        "timestamp": datetime.now().isoformat(),
        "history_sql_used": history_sql_used
    }
    
    st.session_state.messages.append(message)

def add_sql_result_message(sql_result, rewritten_question=None, functional_names=None, powerbi_data=None, selected_dataset=None, sql_generation_story=None, history_sql_used=False):
    """
    Add SQL result message to chat history for proper rendering
    Adds functional_names, powerbi_data, selected_dataset, sql_generation_story, and history_sql_used for session tracking and display

    Args:
        sql_result: SQL result dictionary
        rewritten_question: Rewritten question string
        functional_names: User-friendly dataset names (e.g., ["PBM Claims"])
        powerbi_data: Power BI report matching data
        selected_dataset: Technical table names (e.g., ["pbm_claims", "pbm_network"])
        sql_generation_story: Business-friendly explanation of SQL generation (2-3 lines)
        history_sql_used: Boolean indicating if trusted SQL pattern was used
    """
    # Add functional_names to sql_result dict if not already present
    if functional_names and 'functional_names' not in sql_result:
        sql_result['functional_names'] = functional_names
    
    # Add Power BI data to sql_result dict if not already present
    if powerbi_data and 'powerbi_data' not in sql_result:
        sql_result['powerbi_data'] = powerbi_data
    
    # Add selected_dataset (technical table names) to sql_result dict if not already present
    if selected_dataset and 'selected_dataset' not in sql_result:
        sql_result['selected_dataset'] = selected_dataset
    
    # Add sql_generation_story to sql_result dict if not already present
    if sql_generation_story and 'sql_generation_story' not in sql_result:
        sql_result['sql_generation_story'] = sql_generation_story
    
    message = {
        "role": "assistant",
        "content": "SQL analysis complete",  # Placeholder content
        "message_type": "sql_result",
        "timestamp": datetime.now().isoformat(),
        "sql_result": sql_result,
        "rewritten_question": rewritten_question,
        "functional_names": functional_names,  # Store functional_names for historical rendering
        "powerbi_data": powerbi_data,  # Store Power BI data for rendering
        "selected_dataset": selected_dataset,  # Store technical table names for feedback tracking
        "sql_generation_story": sql_generation_story,  # Store SQL generation story for UI display
        "history_sql_used": history_sql_used  # Store trusted SQL pattern indicator
    }
    
    st.session_state.messages.append(message)
    
    # Debug: Show SQL result summary
    if sql_result.get('multiple_results'):
        print(f"  📊 Multiple SQL results: {len(sql_result.get('query_results', []))} queries")
    else:
        data_count = len(sql_result.get('query_results', []))
        print(f"  📊 Single SQL result: {data_count} rows returned")

async def _insert_session_tracking_row(sql_result: Dict[str, Any], full_state: Dict[str, Any] = None):
    """Insert the sql_result JSON and state_info into tracking table after narrative generation.
    NEW TABLE: fdmbotsession_tracking_updt
    Columns: session_id, user_id, user_question, selected_dataset, sql_info, state_info, insert_ts
    
    Args:
        sql_result: The SQL result dictionary with query results
        full_state: The complete state dictionary from workflow (will remove query_results before storing)
    
    Note: selected_dataset column stores functional_names (user-friendly names like "PBM Claims")
          from router state, NOT technical table names
    """
    try:
        # Ensure db_client exists (recreate if session was cleared)
        db_client = ensure_db_client()
        if not db_client:
            print("⚠️ No db_client in session; skipping tracking insert")
            return
        session_id = st.session_state.get('session_id', 'unknown')
        user_id = get_authenticated_user()
        
        # Get user_question from sql_result user_question field (rewritten question from workflow)
        user_question = sql_result.get('user_question', st.session_state.get('current_query', ''))
        user_question_escaped = user_question.replace("'", "\\'") if user_question else ''
        
        # Get functional_names (user-friendly dataset names) from sql_result
        # This comes from router state: state.get('functional_names', [])
        functional_names = sql_result.get('functional_names', [])
        if isinstance(functional_names, list):
            selected_dataset = ', '.join(functional_names)  # Join with comma and space for display
        else:
            selected_dataset = str(functional_names) if functional_names else ''
        selected_dataset_escaped = selected_dataset.replace("'", "\\'")
        
        # Serialize sql_result as JSON (already includes functional_names from router)
        payload_json = json.dumps(sql_result, ensure_ascii=False)
        
        # CRITICAL: Remove newlines and escape single quotes (like _insert_feedback_row)
        payload_escaped = payload_json.replace("'", "\\'").replace("\n", " ")
        
        # NEW: Process full_state to create state_info (remove query_results)
        state_info_escaped = ''
        if full_state:
            try:
                # Create a copy of the state to avoid modifying the original
                state_copy = dict(full_state)
                
                # Remove query_results from sql_result if it exists in the state
                if 'sql_result' in state_copy and isinstance(state_copy['sql_result'], dict):
                    sql_result_copy = dict(state_copy['sql_result'])
                    # Remove the large query_results data
                    sql_result_copy.pop('query_results', None)
                    # Also remove data from multiple results if present
                    if 'query_results' in sql_result_copy and isinstance(sql_result_copy['query_results'], list):
                        for query in sql_result_copy['query_results']:
                            if isinstance(query, dict):
                                query.pop('data', None)
                    state_copy['sql_result'] = sql_result_copy
                
                # Serialize the cleaned state
                state_json = json.dumps(state_copy, ensure_ascii=False)
                
                # CRITICAL: Remove newlines and escape single quotes (same as sql_result)
                state_info_escaped = state_json.replace("'", "\\'").replace("\n", " ")
                print(f"✅ Prepared state_info for insert ({len(state_info_escaped)} chars)")
            except Exception as state_error:
                print(f"⚠️ Error processing state_info: {state_error}")
                state_info_escaped = ''
        
        # Use from_utc_timestamp to convert current UTC timestamp to CST/CDT (US/Central)
        insert_sql = f"""
        INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotsession_tracking_updt
            (session_id, user_id, user_question, selected_dataset, sql_info, state_info, insert_ts)
        VALUES
            ('{session_id}', '{user_id}', '{user_question_escaped}', '{selected_dataset_escaped}', '{payload_escaped}', '{state_info_escaped}', from_utc_timestamp(current_timestamp(), 'America/Chicago'))
        """
        await db_client.execute_sql_async_audit(insert_sql)
        print("✅ Session tracking insert succeeded (NEW TABLE with state_info)")
            
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
        
        # Modified query to fetch from NEW TABLE
        fetch_sql = f"""
        SELECT session_id, MAX(insert_ts) as last_activity
        FROM prd_optumrx_orxfdmprdsa.rag.fdmbotsession_tracking_updt
        WHERE user_id = '{user_id}'
        GROUP BY session_id 
        ORDER BY MAX(insert_ts) DESC
        LIMIT 7
        """
        
        print("🕐 Fetching session history for user (NEW TABLE):", user_id)
        result = await db_client.execute_sql_async_audit(fetch_sql)
        
        # Handle different response formats from database client
        sessions = []
        
        if isinstance(result, list):
            # Direct list response
            sessions = result
        elif isinstance(result, dict):
            if result.get('success'):
                sessions = result.get('data', [])
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
            else:
                print(f"  Session {i+1}: Unexpected session type: {type(session)} - {session}")
        
        return sessions
            
    except Exception as e:
        print(f"⚠️ Session history fetch failed: {e}")
        import traceback
        print(f"🔍 Full traceback: {traceback.format_exc()}")
        return []
    
async def _fetch_session_records(session_id: str):
    """Fetch all records for a specific session_id in chronological order from NEW TABLE"""
    try:
        db_client = st.session_state.get('db_client')
        if not db_client:
            print("⚠️ No db_client in session; skipping session records fetch")
            return []
        
        user_id = get_authenticated_user()
        
        # Query NEW TABLE with state_info column added
        fetch_sql = f"""
        SELECT session_id, user_question, selected_dataset, sql_info, state_info, insert_ts
        FROM prd_optumrx_orxfdmprdsa.rag.fdmbotsession_tracking_updt
        WHERE user_id = '{user_id}' AND session_id = '{session_id}'
        ORDER BY insert_ts ASC
        """        
        result = await db_client.execute_sql_async_audit(fetch_sql)
                
        # Handle different response formats from database client
        records = []
        
        if isinstance(result, list):
            records = result
        elif isinstance(result, dict):
            if result.get('success'):
                records = result.get('data', [])
            else:
                return []
        else:
            return []
        
        # Debug each record
        for i, record in enumerate(records):
            if isinstance(record, dict):
                print(f"    Selected dataset: {record.get('selected_dataset', 'N/A')}")
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

async def _send_feedback_email_async(user_question: str, feedback_text: str, sql_query: str, table_name: str):
    """Send email notification for negative feedback using Databricks notebook
    
    Args:
        user_question: The user's question that received feedback
        feedback_text: The feedback text provided by user
        sql_query: The SQL query that was executed
        table_name: The table(s) used in the query
    """
    try:
        db_client = st.session_state.get('db_client')
        if not db_client:
            return False
        
        # Get user info
        user_email = get_authenticated_user()
        user_name = get_authenticated_user_name()
        session_id = st.session_state.get('session_id', 'unknown')
        
        # DEBUG: Log what user info we have
        print(f"🔍 DEBUG _send_feedback_email_async: user_email = {user_email}")
        print(f"🔍 DEBUG _send_feedback_email_async: user_name = {user_name}")
        print(f"🔍 DEBUG _send_feedback_email_async: session_id = {session_id}")
        
        # Construct email subject
        subject = f"FDM Bot Feedback Alert raised by User: {user_name}"
        
        # Construct email body
        body = f"""
<html>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <h2 style="color: #d9534f;">FDM Bot Feedback - Needs Improvement</h2>
    
    <h3 style="color: #0275d8;">Question Asked:</h3>
    <div style="background-color: #e7f3ff; padding: 12px; border-radius: 4px; margin-bottom: 15px;">
        <p>{user_question}</p>
    </div>
    
    <h3 style="color: #d9534f;">User Feedback:</h3>
    <div style="background-color: #f8d7da; padding: 12px; border-radius: 4px; margin-bottom: 15px;">
        <p>{feedback_text if feedback_text else 'No additional feedback provided'}</p>
    </div>
    
    <h3 style="color: #5bc0de;">Dataset(s) Used:</h3>
    <div style="background-color: #d9edf7; padding: 12px; border-radius: 4px; margin-bottom: 15px;">
        <p>{table_name}</p>
    </div>
    
    <h3 style="color: #5cb85c;">SQL Query:</h3>
    <div style="background-color: #2d3748; color: #e2e8f0; padding: 12px; border-radius: 4px; margin-bottom: 15px; overflow-x: auto;">
        <pre style="margin: 0; font-family: 'Courier New', monospace; font-size: 12px; white-space: pre-wrap;">{sql_query[:1000]}{'...' if len(sql_query) > 1000 else ''}</pre>
    </div>
    
    <hr style="border: none; border-top: 1px solid #ddd; margin: 25px 0;">

    <div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #d9534f; margin: 20px 0;">
        <p><strong>User:</strong> {user_name} </p>
        <p><strong>Session ID:</strong> {session_id}</p>
        <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S CST')}</p>
    </div>
    
    <p style="color: #5cb85c; font-weight: bold;">
        🔔 The FDM team will review this feedback on priority and reach out if any additional information is required.
    </p>
    
    <p style="color: #666; font-size: 12px; margin-top: 30px;">
        This is an automated notification from the FDM Bot system.<br>
        For questions, please contact the FDM Support team.
    </p>
</body>
</html>
"""
        
        # Recipients list
        recipients = [
            user_email,  # Logged-in user
            "vivek_kishore@optum.com",  # Support team
            "ORXFDM_Cloud_Support@ds.uhc.com"  # Admin
        ]
        recipients_str = ";".join(recipients)
            
        # Call your production notebook using REST API
        notebook_path = "/Workspace/FDM/AI_RAG/send_feedback_email"
        
        # Construct parameters matching your production notebook widgets
        notebook_params = {
            "to_emails": recipients_str,
            "subject": subject,
            "body": body,
            "from_email": "fdm-bot-noreply@optum.com"
        }
        
        # Debug: Log parameters being sent to notebook
        
        # Verify body contains HTML
        if '<html>' in body.lower() and '</html>' in body.lower():
            print(f"   ✅ Body contains valid HTML tags")
        else:
            print(f"   ⚠️ WARNING: Body might not contain proper HTML!")
        
        # Call the notebook asynchronously using your existing job cluster
        # Cluster ID extracted from JDBC: 0226-101532-dse5plow
        # Ensure db_client exists (recreate if session was cleared)
        db_client = ensure_db_client()
        if db_client:
            result = await db_client.run_notebook_async(
                notebook_path, 
                notebook_params, 
                timeout=60,  # Increased timeout for safety
                cluster_id="0226-101532-dse5plow"  # Your job cluster from JDBC string
            )
            
            if result.get("success"):
                return True
            else:
                return False
        else:
            return False
        
    except Exception as e:
        print(f"⚠️ Email notification failed: {e}")
        return False


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
        # Ensure db_client exists (recreate if session was cleared)
        db_client = ensure_db_client()
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

        await db_client.execute_sql_async_audit(insert_sql)
        print("✅ Feedback insert succeeded")
        
        # 🆕 NEW: Send email notification for negative feedback (thumbs down)
        if not positive_feedback:
            print("📧 Negative feedback detected - triggering email notification")
            
            # Show spinner while sending email
            import streamlit as st
            email_spinner_placeholder = st.empty()
            with email_spinner_placeholder:
                with st.spinner("📧 Sending feedback notification email..."):
                    email_sent = await _send_feedback_email_async(
                        user_question=user_question,
                        feedback_text=feedback_text,
                        sql_query=sql_result,
                        table_name=table_name_clean
                    )
            
            # Clear spinner after completion
            email_spinner_placeholder.empty()
            
            if email_sent:
                print("✅ Feedback email notification sent successfully")
            else:
                print("⚠️ Feedback email notification failed (feedback still recorded)")
        
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
        
        result = await db_client.execute_sql_async_audit(fetch_sql)
        
        # Handle different response formats from database client
        if isinstance(result, list) and result:
            return result[0]
        elif isinstance(result, dict) and result.get('success'):
            data = result.get('data', [])
            return data[0] if data else None
        else:
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
        print(f"🔍 DEBUG: render_last_session_overview called")
      
        # Only show if this is a fresh session (no messages yet)
        if st.session_state.messages:
            print("❌ EARLY RETURN: Messages already exist - not showing overview")
            return
        
        # Check if we've already shown the session overview in this session
        if st.session_state.get('session_overview_shown', False):
            print("❌ EARLY RETURN: Overview already shown - not showing again")
            return
        
        print("✅ All checks passed - proceeding to fetch and display overview")
        
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
            return
        
        if not narrative_summary or narrative_summary.strip() == '':
            # Mark as shown even if empty summary, so we don't keep trying
            st.session_state.session_overview_shown = True
            return        
        # Generate LLM narrative overview for all sessions
        try:
            print("🎬 Starting LLM overview generation...")
            spinner_placeholder = st.empty()
            with spinner_placeholder, st.spinner("✍️ Writing the story of our last conversation..."):
                print("⏳ Spinner active - calling _generate_session_overview")
                overview="This feature is disabled to save cost for now. Will be enabled during demo's"
                # overview = asyncio.run(_generate_session_overview(narrative_summary))
                print(f"✅ LLM response received: {len(overview) if overview else 0} chars")
            spinner_placeholder.empty()
            print("✅ Spinner cleared")
            
            
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
            <div style="background-color: #FAF8F2; border: 1px solid #F9A667; border-radius: 8px; padding: 16px; margin: 16px 0;">
                <div style="display: flex; align-items: center; margin-bottom: 12px;">
                    <span style="font-size: 1.2rem; margin-right: 8px;">💭</span>
                    <strong style="color: #D74120; font-size: 1.1rem;">What we discussed last time</strong>
                </div>
                <div style="color: #D74120; line-height: 1.6; font-weight: 500;">
                    {cleaned_overview}
                </div>
                <div style="margin-top: 12px; padding-top: 8px; border-top: 1px solid #EDE8E0; color: #D74120; font-size: 0.9rem;">
                    <em>Ready to continue? Ask me a new question! 🚀</em>
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            # Fallback: Show a simple message with the session info
            st.markdown(f"""
            <div style="background-color: #FAF8F2; border: 1px solid #F9A667; border-radius: 8px; padding: 16px; margin: 16px 0;">
                <div style="display: flex; align-items: center; margin-bottom: 12px;">
                    <span style="font-size: 1.2rem; margin-right: 8px;">💭</span>
                    <strong style="color: #D74120; font-size: 1.1rem;">Welcome back!</strong>
                </div>
                <div style="color: #D74120; line-height: 1.6; font-weight: 500;">
                    I found your previous session from <strong>{insert_ts}</strong>. Model could not process at this time!
                </div>
                <div style="margin-top: 12px; padding-top: 8px; border-top: 1px solid #EDE8E0; color: #D74120; font-size: 0.9rem;">
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
    NOW USES: fdmbotsession_tracking_updt with cleaned sql_info (no newlines)
    
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
            print(f"🔄 Fetching records from NEW TABLE for session {session_id}")
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
                sql_info_json = record.get('sql_info', '')  # NEW: sql_info instead of state_info
                selected_dataset = record.get('selected_dataset', '')  # NEW: selected_dataset
                insert_ts = record.get('insert_ts', '')
            elif isinstance(record, str):
                # If record is a string, try to parse it as JSON
                try:
                    record_dict = json.loads(record)
                    user_question = record_dict.get('user_question', f'Question {i+1}')
                    sql_info_json = record_dict.get('sql_info', '')  # NEW
                    selected_dataset = record_dict.get('selected_dataset', '')  # NEW
                    insert_ts = record_dict.get('insert_ts', '')
                except json.JSONDecodeError:
                    print(f"⚠️ Could not parse record as JSON: {record[:100]}...")
                    continue
            else:
                print(f"⚠️ Skipping unexpected record type {type(record)}: {record}")
                continue
            
            print(f"  Processing record {i+1}: {user_question[:50]}... ({insert_ts})")
            print(f"    Selected dataset: {selected_dataset}")
            
            # Add user message
            st.session_state.messages.append({
                "role": "user",
                "content": user_question,
                "message_type": "historical",
                "timestamp": insert_ts,
                "historical": True  # Mark as historical message
            })
            
            # Handle the assistant response - MANUAL FIELD EXTRACTION (guaranteed structure)
            if sql_info_json and sql_info_json.strip():
                try:
                    print(f"    🔍 Raw sql_info first 100 chars: {sql_info_json[:100]}...")
                    
                    # MANUAL EXTRACTION: Parse each field separately using regex
                    import re
                    
                    # Extract success field
                    success_match = re.search(r'"success"\s*:\s*(true|false)', sql_info_json)
                    success = success_match.group(1) == 'true' if success_match else True
                
                    # Extract multiple_results field
                    multi_match = re.search(r'"multiple_results"\s*:\s*(true|false)', sql_info_json)
                    multiple_results = multi_match.group(1) == 'true' if multi_match else False
                    
                    if multiple_results:
                        # MULTIPLE RESULTS: query_results is an array of objects with title, sql_query, data, narrative
                        # Extract the entire query_results array
                        results_match = re.search(r'"query_results"\s*:\s*(\[.*?\])\s*,\s*"(?:total_queries|user_question)"', sql_info_json, re.DOTALL)
                        if results_match:
                            # This array contains objects, each with their own sql_query, data, narrative
                            # We need to manually extract each object since JSON parsing fails
                            query_results_str = results_match.group(1)
                            
                            # Find all objects in the array (each starts with {"title":)
                            query_objects = []
                            object_pattern = r'\{"title":\s*"(.*?)",\s*"sql_query":\s*"(.*?)",\s*"data":\s*(\[.*?\]),\s*"narrative":\s*"(.*?)"'
                            
                            for match in re.finditer(object_pattern, query_results_str, re.DOTALL):
                                title = match.group(1)
                                sql_query = match.group(2)
                                data_str = match.group(3)
                                narrative = match.group(4)
                                
                                try:
                                    data = json.loads(data_str)
                                except:
                                    data = []
                                
                                query_objects.append({
                                    "title": title,
                                    "sql_query": sql_query,
                                    "data": data,
                                    "narrative": narrative
                                })
                            
                            query_results = query_objects
                        else:
                            query_results = []
                        
                        sql_query = ""  # No top-level sql_query for multiple results
                        narrative = ""  # No top-level narrative for multiple results
                        
                    else:
                        # SINGLE RESULT: Standard structure
                        # Extract sql_query (everything between "sql_query": " and ", "query_results")
                        sql_query_match = re.search(r'"sql_query"\s*:\s*"(.*?)"\s*,\s*"query_results"', sql_info_json, re.DOTALL)
                        sql_query = sql_query_match.group(1) if sql_query_match else ""
                        
                        # Extract query_results (everything between "query_results": [ and ], "narrative")
                        results_match = re.search(r'"query_results"\s*:\s*(\[.*?\])\s*,\s*"narrative"', sql_info_json, re.DOTALL)
                        if results_match:
                            try:
                                query_results = json.loads(results_match.group(1))
                            except:
                                query_results = []
                        else:
                            query_results = []
                        
                        # Extract narrative (everything between "narrative": " and ", "summary" or ", "execution_attempts")
                        narrative_match = re.search(r'"narrative"\s*:\s*"(.*?)"\s*,\s*"(?:summary|execution_attempts)"', sql_info_json, re.DOTALL)
                        narrative = narrative_match.group(1) if narrative_match else ""
                    
                    # Extract user_question
                    user_q_match = re.search(r'"user_question"\s*:\s*"(.*?)"\s*,\s*"(?:title|selected_dataset)"', sql_info_json, re.DOTALL)
                    extracted_user_question = user_q_match.group(1) if user_q_match else user_question
                    
                    # Extract title (may end with } or ",)
                    title_match = re.search(r'"title"\s*:\s*"(.*?)"\s*[},]?\s*$', sql_info_json, re.DOTALL)
                    title = title_match.group(1) if title_match else extracted_user_question
                    
                    # Extract selected_dataset if present (can be string or array)
                    dataset_match = re.search(r'"selected_dataset"\s*:\s*(\[.*?\]|".*?")', sql_info_json)
                    if dataset_match:
                        dataset_str = dataset_match.group(1)
                        if dataset_str.startswith('['):
                            try:
                                selected_dataset = json.loads(dataset_str)
                            except:
                                selected_dataset = None
                        else:
                            selected_dataset = dataset_str.strip('"')
                    else:
                        selected_dataset = None
                    
                    # Also extract functional_names if present in sql_info_json (newer format)
                    functional_names_match = re.search(r'"functional_names"\s*:\s*(\[.*?\])', sql_info_json)
                    if functional_names_match:
                        try:
                            functional_names_from_json = json.loads(functional_names_match.group(1))
                        except:
                            functional_names_from_json = None
                    else:
                        functional_names_from_json = None
                    
                    # Extract sql_generation_story if present (newer format)
                    sql_story_match = re.search(r'"sql_generation_story"\s*:\s*"(.*?)"(?:\s*,|\s*})', sql_info_json, re.DOTALL)
                    sql_generation_story = sql_story_match.group(1) if sql_story_match else ""
                    if sql_generation_story:
                        print(f"    📖 Extracted sql_generation_story from historical record ({len(sql_generation_story)} chars)")
                    
                    # Determine functional_names to use:
                    # 1. First try functional_names from sql_info_json (newer records)
                    # 2. Then try selected_dataset from database column (user-friendly names stored there)
                    # 3. Finally try selected_dataset from sql_info_json (older records with technical names)
                    functional_names_to_use = None
                    if functional_names_from_json:
                        functional_names_to_use = functional_names_from_json
                        print(f"    📊 Using functional_names from sql_info_json: {functional_names_to_use}")
                    elif selected_dataset:  # From database column
                        # Parse the database column value (comma-separated string)
                        if isinstance(selected_dataset, str):
                            functional_names_to_use = [name.strip() for name in selected_dataset.split(',')]
                        else:
                            functional_names_to_use = selected_dataset
                        print(f"    📊 Using functional_names from database column: {functional_names_to_use}")
                    
                    # Build sql_result dict from extracted fields
                    sql_result = {
                        "success": success,
                        "multiple_results": multiple_results,
                        "sql_query": sql_query,
                        "query_results": query_results,
                        "narrative": narrative,
                        "user_question": extracted_user_question,
                        "title": title
                    }
                    
                    if selected_dataset:
                        sql_result["selected_dataset"] = selected_dataset
                    
                    # Add sql_generation_story if extracted
                    if sql_generation_story:
                        sql_result["sql_generation_story"] = sql_generation_story
                        print(f"       - SQL generation story length: {len(sql_generation_story)} chars")
                    
                    print(f"    ✅ Successfully extracted fields manually")
                    print(f"       - Multiple results: {multiple_results}")
                    if multiple_results:
                        print(f"       - Found {len(query_results)} query objects")
                    else:
                        print(f"       - Found {len(query_results)} query results")
                        print(f"       - SQL query length: {len(sql_query)} chars")
                    print(f"       - Narrative length: {len(narrative)} chars")
                    
                    # Extract Power BI data if stored (will be in sql_result from newer sessions)
                    powerbi_data = None
                    if 'powerbi_data' in sql_result:
                        powerbi_data = sql_result.get('powerbi_data')
                        print(f"       - Power BI data found: report_found={powerbi_data.get('report_found') if powerbi_data else None}")
                    
                    # Add assistant message for the SQL result
                    message = {
                        "role": "assistant",
                        "content": "",
                        "message_type": "sql_result",
                        "timestamp": insert_ts,
                        "sql_result": sql_result,
                        "rewritten_question": user_question,
                        "functional_names": functional_names_to_use,  # Add functional_names for historical rendering
                        "powerbi_data": powerbi_data,  # Add Power BI data for historical rendering
                        "sql_generation_story": sql_generation_story,  # Add SQL generation story for historical rendering
                        "historical": True  # Mark as historical message
                    }
                    st.session_state.messages.append(message)
                    print(f"    ✅ Added SQL result for record {i+1}")
                    
                except Exception as e:
                    print(f"    ❌ Unexpected error parsing record {i+1}: {e}")
                    import traceback
                    print(f"    🔍 Traceback: {traceback.format_exc()}")
                    print(f"    🔍 Raw sql_info: {sql_info_json[:200]}...")
                    # Create error result dict
                    error_result = {
                        "error": f"Parse error: {str(e)}",
                        "title": user_question,
                        "narrative": "Error parsing stored data - please contact support",
                        "sql_query": "",
                        "query_results": []
                    }
                    message = {
                        "role": "assistant",
                        "content": "",
                        "message_type": "sql_result",
                        "timestamp": insert_ts,
                        "sql_result": error_result,
                        "rewritten_question": user_question,
                        "historical": True
                    }
                    st.session_state.messages.append(message)
                    
            else:
                # No sql_info available
                add_assistant_message(f"Historical response (limited data): {user_question}", message_type="standard")
                print(f"    ⚠️ No sql_info for record {i+1}")
        
        print(f"✅ Completed rendering session {session_id} with {len(records)} records")
        
    except Exception as e:
        st.error(f"Error loading historical session {session_id}: {e}")
        print(f"❌ Error rendering historical session {session_id}: {e}")
        add_assistant_message(f"Error loading session: {session_id}", message_type="standard")
# Removed render_chat_history function - session history now in sidebar only

def render_sidebar_history():
    """Render session history in the sidebar as a dropdown - only shows history when viewing historical sessions."""
    
    # Display header with Optum styled section title
    st.markdown("""
        <div class="sidebar-section-title">
            <span>🕐</span> Recent Sessions
        </div>
        <style>
        /* Dark blue border for session history selectbox */
        div[data-testid="stSelectbox"][key="session_dropdown"] > div > div,
        [data-testid="stSidebar"] div[data-testid="stSelectbox"] > div > div {
            border: 2px solid #D74120 !important;
            border-radius: 6px !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Check if we have a database client first
    db_client = st.session_state.get('db_client')
    if not db_client:
        st.info("Database not connected")
        return
    
    # Get the ORIGINAL current session ID (the one we started with)
    original_session_id = st.session_state.get('original_session_id', st.session_state.get('session_id', ''))
    
    # Get the currently viewed session ID (might be historical)
    currently_viewed_session = st.session_state.get('session_id', '')
    
    # Auto-fetch history on load OR if cache needs refresh
    should_refresh = st.session_state.get('refresh_session_cache', False)
    
    if 'cached_history' not in st.session_state or should_refresh:
        try:
            print("🔄 Fetching session history from database...")
            history = asyncio.run(_fetch_session_history())
            st.session_state.cached_history = history
            st.session_state.refresh_session_cache = False
            print(f"📦 Cached {len(history)} sessions")
        except Exception as e:
            print(f"❌ Failed to fetch history: {e}")
            st.session_state.cached_history = []
    
    history = st.session_state.get('cached_history', [])
    print(f"🗂️ Building dropdown with {len(history)} sessions")
    
    # Build dropdown options - ONLY show historical sessions
    session_options = {}
    
    # Check if we're currently viewing a historical session (has been explicitly loaded)
    viewing_history = st.session_state.get('session_explicitly_loaded', False)
    
    # Get the currently viewed session ID
    currently_viewed_session = st.session_state.get('session_id', '')
    
    # Add a placeholder option for when no session is selected
    if not viewing_history:
        session_options["-- Select a session --"] = None
    
    # Add all sessions from database to dropdown
    for idx, session_item in enumerate(history):
        if isinstance(session_item, dict):
            session_id = session_item.get('session_id', f'unknown_{idx}')
            last_activity = session_item.get('last_activity', 'Unknown Time')
            
            # Format timestamp for display
            try:
                from datetime import datetime as dt_class
                if isinstance(last_activity, str):
                    dt_obj = dt_class.fromisoformat(last_activity.replace('Z', '+00:00'))
                    display_timestamp = dt_obj.strftime("%b %d %I:%M%p")
                elif hasattr(last_activity, 'strftime'):
                    display_timestamp = last_activity.strftime("%b %d %I:%M%p")
                else:
                    display_timestamp = str(last_activity)[:16]
            except Exception as e:
                print(f"⚠️ Failed to format timestamp: {e}")
                display_timestamp = str(last_activity)[:16]
            
            # Add indicator if this is the currently viewed session
            if session_id == currently_viewed_session:
                label = f"🔵 {display_timestamp}"  # Blue dot for currently viewed
            else:
                label = f"{display_timestamp}"
            
            session_options[label] = session_id
    
    # If no sessions found (besides placeholder), show info
    if len(session_options) <= 1:  # Only placeholder exists
        st.info("No previous sessions yet - start chatting to create history!")
        return
    
    # Find the index of the currently viewed session in the dropdown
    default_index = 0  # Default to placeholder
    if viewing_history:
        # Find which session is currently being viewed
        for idx, (label, sess_id) in enumerate(session_options.items()):
            if sess_id == currently_viewed_session:
                default_index = idx
                print(f"🔍 Currently viewing session at index {idx}: {label}")
                break
    
    # Show the dropdown
    selected_label = st.selectbox(
        "Session History",
        options=list(session_options.keys()),
        index=default_index,
        key="session_history_dropdown",
        label_visibility="collapsed",
        help=f"Switch between sessions ({len(session_options)} available)"
    )
    
    selected_session_id = session_options.get(selected_label)
    
    # Ignore if placeholder is selected
    if selected_session_id is None:
        return
    
    # Handle session switch if user selected a different session
    if selected_session_id and selected_session_id != currently_viewed_session:
        print(f"📂 User selected session: {selected_session_id}")
        
        # IMPORTANT: Before switching, refresh cache so current session is saved
        # This ensures when user comes back, they see their latest work
        st.session_state.refresh_session_cache = True
        
        # Clear current chat and load historical session
        # Preserve domain_selection before clearing everything
        current_domain_selection = st.session_state.get('domain_selection')
        
        st.session_state.messages = []
        st.session_state.sql_rendered = False
        st.session_state.narrative_rendered = False
        st.session_state.processing = False
        st.session_state.session_overview_shown = True  # Don't show overview when loading historical session
        
        # IMPORTANT: Make the selected historical session the CURRENT session
        # This way when user asks new questions, they continue in this session thread
        st.session_state.session_id = selected_session_id
        st.session_state.original_session_id = selected_session_id  # Update original to match
        st.session_state.session_explicitly_loaded = True  # Mark that user explicitly loaded this
        print(f"🔄 Session ID updated to: {selected_session_id} (now current session)")
        
        # Restore domain_selection so new questions work properly
        if current_domain_selection:
            st.session_state.domain_selection = current_domain_selection
            print(f"✅ Preserved domain_selection: {current_domain_selection}")
        else:
            print(f"⚠️ No domain_selection to preserve")
        
        # Render the complete historical session
        render_historical_session_by_id(selected_session_id)
        st.rerun()


def render_sidebar_dataset_selector():
    """Render dataset selector in sidebar with simple single-select dropdown"""
    
    # Get available datasets
    datasets = get_available_datasets()
    
    # Keep "All Datasets" at the beginning as default option
    available_datasets = datasets  # Already has "All Datasets" first
    
    # Debug: Log available datasets
    print(f"🔍 Available datasets: {available_datasets} (Total: {len(available_datasets)})")
    
    # Display header with Optum styled section title
    st.markdown("""
        <div class="sidebar-section-title">
            <span>🗂️</span> Filter by Dataset (Optional)
        </div>
        <style>
        /* Dark blue border for dataset selectbox */
        div[data-testid="stSelectbox"][key="dataset_selectbox"] > div > div,
        div[data-testid="stSelectbox"]:has(div[data-baseweb="select"]) > div > div {
            border: 2px solid #D74120 !important;
            border-radius: 6px !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Initialize selection in session state if not exists
    if 'user_selected_datasets' not in st.session_state:
        st.session_state.user_selected_datasets = []
    
    # Determine the default index for the dropdown
    default_index = 0  # Default to "All Datasets"
    
    # If user has a previous selection, find its index
    if st.session_state.user_selected_datasets and len(st.session_state.user_selected_datasets) > 0:
        selected_dataset = st.session_state.user_selected_datasets[0]
        try:
            default_index = available_datasets.index(selected_dataset)
        except ValueError:
            default_index = 0  # Fallback to "All Datasets" if not found
    
    # Create simple selectbox dropdown
    selected_dataset = st.selectbox(
        "Select a dataset:",
        options=available_datasets,
        index=default_index,
        key="dataset_selectbox",
        label_visibility="collapsed"  # Hide the label, we have our own header
    )
    
    # Update session state based on selection
    if selected_dataset == "All Datasets":
        # Empty list means all datasets
        st.session_state.user_selected_datasets = []
    else:
        # Single dataset selected, store as list for compatibility
        st.session_state.user_selected_datasets = [selected_dataset]
    
    # Also update the legacy field for backward compatibility
    st.session_state.selected_dataset_filter = st.session_state.user_selected_datasets
    
    # Log the selection
    if st.session_state.user_selected_datasets:
        print(f"📊 Dataset selected: {st.session_state.user_selected_datasets[0]}")
    else:
        print(f"📊 No dataset filter (All Datasets)")




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
    log_event('info', f"User query received: {user_query[:100]}", query_length=len(user_query))
    
    # Exit clarification mode when user provides answer
    if st.session_state.get('in_clarification_mode', False):
        st.session_state.in_clarification_mode = False
    
    # Get question type from session state - must match radio button selection exactly
    question_type = st.session_state.get("question_type_selection", "New Question")
    
    # Validate and normalize question type
    if question_type not in ("Follow-up", "New Question"):
        print(f"⚠️ Invalid question_type_selection: '{question_type}', resetting to default")
        question_type = "New Question" if not st.session_state.first_question_asked else "Follow-up"
        st.session_state.question_type_selection = question_type
    
    display_query = user_query
    workflow_query = f"{question_type.lower()} - {user_query}"
    print(f"🚀 Question type: '{question_type}' | Workflow query: '{workflow_query}' | First Q asked: {st.session_state.first_question_asked}")
    
    # 1. Mark all existing SQL results as historical/non-interactive
    for msg in st.session_state.messages:
        if msg.get('message_type') == 'sql_result':
            # This ensures feedback buttons are hidden for old results
            msg['historical'] = True 
            print(f"🕰️ Marked SQL result message as historical")
        
        # 1B. MARK OLD SELECTION_REASONING MESSAGES AS HISTORICAL
        # This prevents duplicate "Selected Datasets" display when asking new questions
        # from within a loaded historical session
        # CRITICAL: Preserve history_sql_used flag when marking as historical
        if msg.get('message_type') == 'selection_reasoning':
            if not msg.get('historical', False):  # Only mark if not already historical
                msg['historical'] = True
                print(f"🕰️ Marked old selection_reasoning as historical | history_sql_used={msg.get('history_sql_used', False)}")
        
        # 2. MARK FOLLOW-UP INTRO MESSAGE AS HISTORICAL
        # This prevents the follow-up message from re-rendering the buttons in a disabled state
        if msg.get('message_type') == 'followup_questions':
            msg['historical'] = True
            print("🕰️ Marked old follow-up intro message as historical")
    
    # 3. CLEAR INTERACTIVE FOLLOW-UP BUTTONS (MOST CRITICAL STEP)
    # The buttons render based on this list, so clearing it hides the buttons on re-run.
    if hasattr(st.session_state, 'current_followup_questions'):
        if st.session_state.current_followup_questions:
            print("🗑️ Clearing interactive follow-up questions list due to new user input")
            st.session_state.current_followup_questions = []
            
    # Remove the visible "Would you like to explore further?" message from chat history
    # This addresses the case where the user types a question instead of clicking a button.
    # We re-check the messages list *after* marking them as historical/non-interactive,
    # ensuring the last message is the one we want to remove.
    if (st.session_state.messages and 
        st.session_state.messages[-1].get('message_type') == 'followup_questions'):
        st.session_state.messages.pop()
        print("🗑️ Removed follow-up intro message from chat history")
        
    # Add user message to history (use the clean, original query for display)
    st.session_state.messages.append({
        "role": "user",
        "content": display_query
    })
    
    # IMPORTANT: Mark where this conversation starts so we can manage responses properly
    st.session_state.current_conversation_start = len(st.session_state.messages) - 1
    
    # Set processing state and reset narrative state
    st.session_state.current_query = workflow_query
    st.session_state.processing = True
    st.session_state.workflow_started = False
    st.session_state.narrative_rendered = False
    st.session_state.narrative_state = None
    
    # Store what the user selected THIS time (before next rerun)
    # This becomes the "last_radio_selection" for next question's default logic
    st.session_state.last_radio_selection = question_type
    print(f"📝 STORED: Saving '{question_type}' as last_radio_selection for next question")
    
    # Mark that first question has been asked - next time default will be "Follow-up"
    if not st.session_state.first_question_asked:
        st.session_state.first_question_asked = True
        print("✅ FIRST: Marked first_question_asked=True")

# ============ CSS STYLING FOR MESSAGE TYPES ============

def add_message_type_css():
    """Add CSS for different message types - Enhanced with Optum Brand Colors"""
    st.markdown("""
    <style>
    /* ===== INTER FONT ===== */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    /* Apply Inter font globally */
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    /* Main title styling - Optum Deep Red */
    h1 {
        color: #D74120 !important;
        font-weight: 700 !important;
        letter-spacing: -0.02em !important;
    }
    
    h2, h3 {
        color: #1a1a1a !important;
        font-weight: 600 !important;
    }
    
    /* ===== ANIMATIONS ===== */
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(12px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    .new-message {
        animation: fadeInUp 0.4s ease-out;
    }
    
    /* ===== ENHANCED SPINNER ===== */
    .spinner-container {
        background: linear-gradient(135deg, #FAF8F2 0%, #FFFFFF 100%);
        border-radius: 12px;
        padding: 16px 20px;
        margin: 12px 0;
        border-left: 4px solid #FF612B;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    
    .spinner-message {
        color: #4a4a4a;
        font-weight: 500;
    }
    
    /* ===== BASE CHAT CONTAINER ===== */
    .chat-container {
        display: flex;
        flex-direction: column;
        gap: 16px;
    }
    
    /* ===== USER MESSAGE - SOFT PEACH BACKGROUND WITH BLACK TEXT ===== */
    .user-message {
        display: flex;
        justify-content: flex-start;
        margin: 12px 0;
        animation: fadeInUp 0.4s ease-out;
    }
    
    .user-message-content {
        background: #FFD1AB !important;
        color: #1a1a1a !important;
        padding: 14px 20px;
        border-radius: 20px 20px 6px 20px;
        max-width: none;
        min-width: 200px;
        word-wrap: break-word;
        border: none !important;
        box-shadow: 0 4px 12px rgba(255, 209, 171, 0.4);
        font-weight: 500;
    }
    
    /* ===== ASSISTANT MESSAGE ===== */
    .assistant-message {
        display: flex;
        justify-content: flex-start;
        margin: 12px 0;
        animation: fadeInUp 0.4s ease-out;
    }
    
    .assistant-message-content {
        background-color: #FFFFFF !important;
        color: #1a1a1a !important;
        padding: 16px 20px;
        border-radius: 16px;
        max-width: 100% !important;
        min-width: 300px !important;
        word-wrap: break-word;
        border: 1px solid #EDE8E0 !important;
        box-shadow: 0 4px 20px rgba(0,0,0,0.06);
        overflow: visible !important;
    }
    
    .assistant-message-content strong {
        white-space: nowrap !important;
        display: inline-block !important;
        max-width: none !important;
        font-size: 15px !important;
        color: #D74120 !important;
    }
    
    /* ===== NARRATIVE/EXPLANATION CONTENT ===== */
    .narrative-content {
        color: #1a1a1a;
        padding: 18px 22px;
        margin: 12px 0;
        max-width: 100%;
        word-wrap: break-word;
        font-size: 15px;
        line-height: 1.7;
        font-style: normal !important;
        font-weight: normal !important;
        background: linear-gradient(135deg, #FAFAF8 0%, #FFFFFF 100%) !important;
        border: 1px solid #EDE8E0 !important;
        border-radius: 14px !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    
    /* ===== HOW I SOLVED THIS - EXPLANATION CARD ===== */
    .explanation-card {
        background: linear-gradient(135deg, #FAFAF8 0%, #FFFFFF 100%);
        border-left: 4px solid #FF612B;
        padding: 18px 22px;
        border-radius: 0 14px 14px 0;
        margin: 16px 0;
    }
    
    .explanation-title {
        font-size: 13px;
        font-weight: 600;
        text-transform: none;
        letter-spacing: normal;
        color: #FF612B;
        margin-bottom: 10px;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    
    .explanation-text {
        color: #1a1a1a;
        font-size: 14px;
        line-height: 1.7;
    }
    
    /* ===== DATASET BADGE - CYAN ===== */
    .dataset-badge {
        background: linear-gradient(135deg, #D9F6FA 0%, #FFFFFF 100%);
        border: 1px solid #D9F6FA;
        padding: 12px 18px;
        border-radius: 12px;
        font-size: 14px;
        font-weight: 600;
        color: #1a1a1a;
        display: inline-flex;
        align-items: center;
        gap: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    
    /* ===== TRUSTED SQL BADGE - GREEN ===== */
    .trusted-badge {
        background: linear-gradient(135deg, #e8fff3 0%, #FFFFFF 100%);
        border: 2px solid #4ecdc4;
        padding: 12px 18px;
        border-radius: 12px;
        font-size: 14px;
        font-weight: 600;
        color: #0d7d74;
        display: inline-flex;
        align-items: center;
        gap: 10px;
    }
    
    .trusted-badge-icon {
        background: #4ecdc4;
        color: white;
        width: 20px;
        height: 20px;
        border-radius: 50%;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
        font-weight: bold;
    }
    
    /* ===== NEW SQL BADGE - BLUE/VISIBLE ===== */
    .new-sql-badge {
        background: linear-gradient(135deg, #EBF5FF 0%, #FFFFFF 100%);
        border: 2px solid #3B82F6;
        padding: 12px 18px;
        border-radius: 12px;
        font-size: 14px;
        font-weight: 600;
        color: #1e40af;
        display: inline-flex;
        align-items: center;
        gap: 10px;
    }
    
    /* ===== KEY INSIGHTS CARD - WHITE WITH AMBER ACCENT ===== */
    .insights-card {
        background: #FFFFFF;
        border-radius: 12px;
        padding: 20px 24px;
        margin: 16px 0;
        border: 1px solid #EDE8E0;
        border-left: 4px solid #F9A667;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    
    .insights-header {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 18px;
    }
    
    .insights-icon {
        width: 36px;
        height: 36px;
        background: linear-gradient(135deg, #F9A667 0%, #FF612B 100%);
        border-radius: 10px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 18px;
        box-shadow: 0 4px 12px rgba(255, 97, 43, 0.25);
    }
    
    .insights-title {
        font-weight: 700;
        font-size: 16px;
        color: #1a1a1a;
    }
    
    .insight-item {
        display: flex;
        align-items: flex-start;
        gap: 14px;
        padding: 12px 0;
        border-bottom: 1px solid rgba(237, 232, 224, 0.5);
    }
    
    .insight-item:last-child {
        border-bottom: none;
        padding-bottom: 0;
    }
    
    .insight-bullet {
        width: 8px;
        height: 8px;
        background: #FF612B;
        border-radius: 50%;
        margin-top: 7px;
        flex-shrink: 0;
    }
    
    .insight-text {
        color: #1a1a1a;
        font-size: 14px;
        line-height: 1.6;
    }
    
    /* ===== REPORT RECOMMENDATION CARD ===== */
    .report-card {
        background: #FFFFFF;
        border-radius: 16px;
        padding: 22px 26px;
        margin: 20px 0;
        border: 2px dashed #F9A667;
        position: relative;
        overflow: hidden;
    }
    
    .report-label {
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #F9A667;
        margin-bottom: 10px;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    
    .report-title {
        font-size: 17px;
        font-weight: 700;
        color: #1a1a1a;
        margin-bottom: 14px;
    }
    
    .report-filters {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-bottom: 18px;
    }
    
    .filter-tag {
        background: #FAF8F2;
        border: 1px solid #EDE8E0;
        padding: 8px 14px;
        border-radius: 20px;
        font-size: 12px;
        color: #4a4a4a;
        font-weight: 500;
    }
    
    .open-report-btn {
        background: linear-gradient(135deg, #FF612B 0%, #D74120 100%);
        color: white !important;
        padding: 14px 28px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 14px;
        display: inline-flex;
        align-items: center;
        gap: 10px;
        text-decoration: none !important;
        transition: all 0.3s ease;
        box-shadow: 0 4px 16px rgba(255, 97, 43, 0.35);
        border: none;
        cursor: pointer;
    }
    
    .open-report-btn:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(255, 97, 43, 0.45);
        color: white !important;
    }
    
    /* ===== NEGATIVE NUMBER STYLING ===== */
    .negative-value {
        color: #D74120 !important;
        font-weight: 600;
    }
    
    /* ===== FOLLOW-UP CONTAINER ===== */
    .followup-container {
        margin: 1rem 0;
        padding: 1.25rem;
        background: linear-gradient(135deg, #FAFAF8 0%, #FFFFFF 100%);
        border-radius: 14px;
        border-left: 4px solid #FF612B;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }

    .followup-header {
        font-weight: 600;
        color: #1a1a1a;
        margin-bottom: 1rem;
        font-size: 16px;
    }

    .followup-button {
        display: block;
        width: 100%;
        margin: 0.5rem 0;
        padding: 14px 18px;
        background-color: white;
        border: 2px solid #D74120;
        border-radius: 10px;
        color: #D74120;
        text-align: left;
        cursor: pointer;
        transition: all 0.2s ease;
        font-size: 14px;
        line-height: 1.5;
        font-weight: 500;
    }

    .followup-button:hover {
        background: linear-gradient(135deg, #FF612B 0%, #F9A667 100%);
        color: white;
        box-shadow: 0 4px 12px rgba(255, 97, 43, 0.3);
        border-color: transparent;
    }
    
    .followup-button:active {
        transform: translateY(1px);
    }
    
    /* ===== FEEDBACK SECTION ===== */
    .feedback-section {
        margin: 1rem 0;
        padding: 0.75rem 0;
        border-top: 1px solid #EDE8E0;
    }
    
    /* Thumbs up button - green theme */
    div[data-testid="column"] button {
        width: 100% !important;
        font-size: 14px !important;
        padding: 10px 18px !important;
        border-radius: 10px !important;
        transition: all 0.2s ease !important;
        font-weight: 500 !important;
    }
    
    div[data-testid="column"]:first-child button {
        background-color: #f0fdf4 !important;
        border: 2px solid #22c55e !important;
        color: #16a34a !important;
    }
    
    div[data-testid="column"]:first-child button:hover {
        background-color: #22c55e !important;
        color: white !important;
        box-shadow: 0 4px 12px rgba(34, 197, 94, 0.3) !important;
    }
    
    /* Thumbs down button - Optum orange theme */
    div[data-testid="column"]:last-child button {
        background-color: #FFF7ED !important;
        border: 2px solid #FF612B !important;
        color: #D74120 !important;
    }
    
    div[data-testid="column"]:last-child button:hover {
        background-color: #FF612B !important;
        color: white !important;
        box-shadow: 0 4px 12px rgba(255, 97, 43, 0.3) !important;
    }
    
    /* ===== FOLLOW-UP BUTTONS CONTAINER ===== */
    .followup-buttons-container {
        display: flex !important;
        flex-direction: column !important;
        gap: 10px !important;
        align-items: stretch !important;
        justify-content: flex-start !important;
        max-width: 100% !important;
        margin: 0.75rem 0 !important;
        padding: 0 !important;
    }
    
    /* ===== GENERAL BUTTON STYLING - OPTUM ORANGE FOR FOLLOW-UPS ===== */
    .stButton > button {
        background-color: white !important;
        color: #D74120 !important;
        border: 2px solid #D74120 !important;
        border-radius: 10px !important;
        padding: 12px 18px !important;
        margin: 0 !important;
        width: 100% !important;
        text-align: left !important;
        font-size: 14px !important;
        line-height: 1.5 !important;
        transition: all 0.2s ease !important;
        height: auto !important;
        min-height: 44px !important;
        white-space: normal !important;
        word-wrap: break-word !important;
        font-weight: 500 !important;
    }

    .stButton > button:hover {
        background: linear-gradient(135deg, #FF612B 0%, #F9A667 100%) !important;
        color: white !important;
        box-shadow: 0 4px 12px rgba(255, 97, 43, 0.3) !important;
        transform: translateY(-1px) !important;
        border-color: transparent !important;
    }

    .stButton > button:focus {
        background: linear-gradient(135deg, #FF612B 0%, #D74120 100%) !important;
        color: white !important;
        box-shadow: 0 0 0 3px rgba(255, 97, 43, 0.2) !important;
        outline: none !important;
        border-color: transparent !important;
    }

    .element-container .stButton {
        margin: 0 !important;
        flex: 0 0 auto !important;
    }

    .stButton button[kind="secondary"] {
        background-color: white !important;
        color: #D74120 !important;
        border-color: #D74120 !important;
    }
    
    /* ===== CHAT INPUT - OPTUM STYLED ===== */
    .stChatInput > div > div > textarea {
        min-height: 48px !important;
        max-height: 200px !important;
        height: 48px !important;
        resize: none !important;
        overflow-y: auto !important;
        word-wrap: break-word !important;
        white-space: pre-wrap !important;
        font-size: 15px !important;
        line-height: 1.5 !important;
        padding: 14px 18px !important;
        border-radius: 14px !important;
        border: 2px solid #EDE8E0 !important;
        transition: all 0.2s ease !important;
        background: #FFFFFF !important;
    }
    
    .stChatInput > div > div > textarea:focus {
        border-color: #FF612B !important;
        box-shadow: 0 0 0 3px rgba(255, 97, 43, 0.1) !important;
        outline: none !important;
    }
    
    .stChatInput {
        position: sticky !important;
        bottom: 0 !important;
        background: linear-gradient(180deg, transparent 0%, #FAFAF8 30%) !important;
        padding: 16px 0 !important;
        margin-top: 24px !important;
    }
    
    .stChatInput > div > div > textarea {
        field-sizing: content !important;
    }
    
    /* ===== RADIO BUTTONS ===== */
    .stRadio > label {
        font-size: 14px !important;
        font-weight: 500 !important;
        color: #4a4a4a !important;
    }
    
    .stRadio > div {
        gap: 14px !important;
    }
    
    .stRadio > div > label > div {
        font-size: 14px !important;
    }
    
    /* ===== DATAFRAME TABLE - ENHANCED ===== */
    .stDataFrame table {
        width: 100% !important;
        border-collapse: collapse !important;
    }
    
    .stDataFrame table td, 
    .stDataFrame table th {
        text-align: right !important;
        padding: 12px 16px !important;
        border-bottom: 1px solid #EDE8E0 !important;
    }
    
    .stDataFrame table td:first-child,
    .stDataFrame table th:first-child {
        text-align: left !important;
        font-weight: 500 !important;
    }
    
    .stDataFrame table th {
        background-color: #FAFAF8 !important;
        font-weight: 600 !important;
        color: #4a4a4a !important;
        text-transform: uppercase !important;
        font-size: 11px !important;
        letter-spacing: 0.05em !important;
        border-bottom: 2px solid #EDE8E0 !important;
    }
    
    .stDataFrame table tr:nth-child(even) {
        background-color: #FAFAFA !important;
    }
    
    .stDataFrame table tr:hover {
        background-color: #FAFAF8 !important;
    }

    /* ===== SIDEBAR SESSION ITEMS ===== */
    .session-item {
        padding: 12px 14px;
        background: #FFFFFF;
        border-radius: 10px;
        margin-bottom: 8px;
        font-size: 13px;
        color: #4a4a4a;
        cursor: pointer;
        border: 1px solid #EDE8E0;
        transition: all 0.2s ease;
    }
    
    .session-item:hover {
        border-color: #FF612B;
        box-shadow: 0 2px 8px rgba(255, 97, 43, 0.15);
    }
    
    .session-time {
        font-size: 11px;
        color: #888;
        margin-top: 4px;
    }
    
    /* ===== SIDEBAR SECTION STYLING ===== */
    .sidebar-section {
        background: #FFFFFF;
        border-radius: 12px;
        padding: 14px;
        margin-bottom: 14px;
        border: 1px solid #EDE8E0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    
    .sidebar-section-title {
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #F9A667;
        margin-bottom: 12px;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    </style>
    
    <script>
    // Auto-expand textarea functionality
    function autoExpand() {
        const textareas = document.querySelectorAll('.stChatInput textarea');
        textareas.forEach(function(textarea) {
            if (!textarea.hasAttribute('data-auto-expand')) {
                textarea.setAttribute('data-auto-expand', 'true');
                
                textarea.style.height = '48px';
                
                textarea.addEventListener('input', function() {
                    this.style.height = '48px';
                    const newHeight = Math.min(this.scrollHeight, 200);
                    this.style.height = newHeight + 'px';
                });
                
                textarea.addEventListener('paste', function() {
                    setTimeout(() => {
                        this.style.height = '48px';
                        const newHeight = Math.min(this.scrollHeight, 200);
                        this.style.height = newHeight + 'px';
                    }, 0);
                });
            }
        });
    }
    
    document.addEventListener('DOMContentLoaded', autoExpand);
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
        # Custom styling for the back button - Optum styled
        st.markdown("""
        <style>
        .back-button {
            background-color: white;
            color: #D74120;
            border: 2px solid #FF612B;
            padding: 10px 18px;
            border-radius: 10px;
            text-decoration: none;
            font-weight: 600;
            display: inline-block;
            width: 100%;
            text-align: center;
            margin: 10px 0;
            transition: all 0.2s ease;
        }
        .back-button:hover {
            background: linear-gradient(135deg, #FF612B 0%, #F9A667 100%);
            color: white;
            border-color: transparent;
        }
        
        /* Sidebar session history buttons - Optum styled */
        [data-testid="stSidebar"] .stButton > button[key*="history_"] {
            text-align: left !important;
            background-color: #FFFFFF !important;
            border: 1px solid #EDE8E0 !important;
            padding: 8px 12px !important;
            margin: 4px 0 !important;
            border-radius: 8px !important;
            font-size: 12px !important;
            line-height: 1.3 !important;
            width: 100% !important;
            height: auto !important;
            min-height: 32px !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
            transition: all 0.2s ease !important;
        }
        
        [data-testid="stSidebar"] .stButton > button[key*="history_"]:hover {
            background-color: #FAFAF8 !important;
            border-color: #FF612B !important;
            box-shadow: 0 2px 8px rgba(255, 97, 43, 0.15) !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
        if st.button("Back to Main Page", key="back_to_main"):
            st.switch_page("main.py")
        
        st.markdown("---")
    
    try:
        workflow = get_session_workflow()
        
        if workflow is None:
            st.error("Failed to initialize async workflow. Please refresh the page.")
            return

        # --- FIX 1: Reset radio button to default "Follow-up" after processing ---
        # Check if processing just finished and the previous selection was 'New Question'
        # The 'processing' flag is set to False right before rerun() below.
        if (not st.session_state.get('processing', True) and 
            st.session_state.get('question_type_selection') == 'New Question'):
            
            # This ensures the radio button visually defaults to "Follow-up" for the next turn.
            st.session_state.question_type_radio = 'Follow-up'
            st.session_state.question_type_selection = 'Follow-up'
            print("🔄 Resetting question_type to 'Follow-up' for next turn.")
        # --- END FIX 1 ---
        
        # Show spinner in main area while fetching session history
        if 'cached_history' not in st.session_state:
            # Show spinner in main chat area while fetching history
            spinner_placeholder = st.empty()
            with spinner_placeholder:
                with st.spinner("🔄 Loading application and fetching session history..."):
                    # Fetch history in background while showing spinner
                    with st.sidebar:
                        # Order: Dataset Selection → Recent Sessions
                        render_sidebar_dataset_selector()
                        st.markdown("---")
                        render_sidebar_history()
            # Clear the spinner once history is loaded
            spinner_placeholder.empty()
        else:
            # History already cached, just render sidebar
            with st.sidebar:
                # Order: Dataset Selection → Recent Sessions
                render_sidebar_dataset_selector()
                st.markdown("---")
                render_sidebar_history()
        
        st.markdown("""
        <div style="margin-top: -20px; margin-bottom: 12px;">
            <h2 style="color: #D74120; font-weight: 700; font-size: 1.9rem; margin-bottom: 4px; letter-spacing: -0.02em;">Finance Analytics Assistant</h2>
            <p style="color: #888; font-size: 12px; margin: 0; letter-spacing: 0.05em;">POWERED BY DANA</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Display current domain selection if available - Optum styled
        if st.session_state.get('domain_selection'):
            domain_display = ", ".join(st.session_state.domain_selection)
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #D9F6FA 0%, #FFFFFF 100%); padding: 12px 16px; border-radius: 10px; margin-bottom: 14px; border: 1px solid #D9F6FA;">
                <span style="color: #4a4a4a; font-size: 13px;">🏢 <strong style="color: #1a1a1a;">Selected Team:</strong> <span style="color: #D74120; font-weight: 600;">{domain_display}</span></span>
            </div>
            """, unsafe_allow_html=True)
        
        # Show overview of last session if available (only for fresh sessions)
        render_last_session_overview()
        
        st.markdown("---")
        
        # Chat container for messages
        chat_container = st.container()
        
        with chat_container:
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            
            # Render all chat messages (including real-time updates)
            for idx, message in enumerate(st.session_state.messages):
                render_chat_message_enhanced(message, idx)
            
            # Render follow-up questions if they exist
            render_persistent_followup_questions()
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Processing indicator and streaming workflow execution
        if st.session_state.processing:
            # Run the workflow
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
                            # Update the existing SQL result message with narrative AND Power BI data
                            if updated_sql_result:
                                # Extract Power BI data from narrative response
                                powerbi_data_from_narrative = {
                                    'report_found': narrative_res.get('report_found', False),
                                    'report_url': narrative_res.get('report_url'),
                                    'report_filter': narrative_res.get('report_filter'),
                                    'report_name': narrative_res.get('report_name'),
                                    'match_type': narrative_res.get('match_type'),
                                    'report_reason': narrative_res.get('report_reason')
                                }
                                print(f"🔍 Power BI data from narrative: report_found={powerbi_data_from_narrative.get('report_found')}, name={powerbi_data_from_narrative.get('report_name')}")
                                
                                # Find and update the last SQL result message
                                for i in range(len(st.session_state.messages) - 1, -1, -1):
                                    msg = st.session_state.messages[i]
                                    if msg.get('message_type') == 'sql_result':
                                        msg['sql_result'] = updated_sql_result
                                        msg['powerbi_data'] = powerbi_data_from_narrative  # Update Power BI data
                                        print(f"✅ Updated message with narrative and Power BI data")
                                        break
                            else:
                                # Fallback: try to locate sql_result from narrative_state if not returned explicitly
                                updated_sql_result = (st.session_state.get('narrative_state') or {}).get('sql_result', {})
                            
                            # 🆕 CRITICAL FIX: Save updated conversation_memory back to session_state
                            if narrative_res and 'conversation_memory' in narrative_res:
                                st.session_state.conversation_memory = narrative_res['conversation_memory']
                                print(f"💾 Saved conversation_memory after narrative to session_state: {list(narrative_res['conversation_memory'].get('dimensions', {}).keys())}")
                            
                            # Fire and forget insert of tracking row (after we have narrative)
                            if updated_sql_result:
                                try:
                                    asyncio.run(_insert_session_tracking_row(updated_sql_result, narrative_res))
                                except RuntimeError:
                                    # If already in an event loop (unlikely here due to asyncio.run wrapping), schedule task
                                    loop = asyncio.get_event_loop()
                                    loop.create_task(_insert_session_tracking_row(updated_sql_result, narrative_res))
                            
                            print("✅ Narrative generation completed")
                        else:
                            print("⚠️ Narrative generation completed with issues - checking if narrative exists anyway...")
                            # Even if flag is missing, check if narrative was actually added
                            if updated_sql_result and updated_sql_result.get('narrative'):
                                print("🔄 Found narrative content despite missing flag - updating message and inserting")
                                
                                # Extract Power BI data from narrative response (fallback case)
                                powerbi_data_from_narrative = {
                                    'report_found': narrative_res.get('report_found', False),
                                    'report_url': narrative_res.get('report_url'),
                                    'report_filter': narrative_res.get('report_filter'),
                                    'report_name': narrative_res.get('report_name'),
                                    'match_type': narrative_res.get('match_type'),
                                    'report_reason': narrative_res.get('report_reason')
                                }
                                
                                # Update the message anyway
                                for i in range(len(st.session_state.messages) - 1, -1, -1):
                                    msg = st.session_state.messages[i]
                                    if msg.get('message_type') == 'sql_result':
                                        msg['sql_result'] = updated_sql_result
                                        msg['powerbi_data'] = powerbi_data_from_narrative  # Update Power BI data
                                        break
                                
                                # 🆕 CRITICAL FIX: Save updated conversation_memory even in fallback case
                                if narrative_res and 'conversation_memory' in narrative_res:
                                    st.session_state.conversation_memory = narrative_res['conversation_memory']
                                    print(f"💾 Saved conversation_memory (fallback) to session_state: {list(narrative_res['conversation_memory'].get('dimensions', {}).keys())}")
                                
                                # Insert tracking row
                                try:
                                    asyncio.run(_insert_session_tracking_row(updated_sql_result, narrative_res))
                                except RuntimeError:
                                    loop = asyncio.get_event_loop()
                                    loop.create_task(_insert_session_tracking_row(updated_sql_result, narrative_res))
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
                    
                    # ============ COSMOS DB: SAVE COMPLETE STATE SNAPSHOT ============
                    # Workflow is complete - save entire state to Cosmos                    
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
                    
                    # ============ COSMOS DB: SAVE COMPLETE STATE SNAPSHOT ============
                    # Drillthrough analysis complete - save entire state to Cosmos                    
                    st.rerun()

        st.markdown("""
        <style>
            /* Minimize radio button spacing */
            .stRadio {
                margin: 0 !important;
                padding: 0 !important;
                margin-bottom: -18px !important;
                margin-top: -12px !important;
            }
            
            .stRadio > div {
                margin: 0 !important;
                padding: 0 !important;
            }
            
            /* Reduce gap between radio and chat input */
            .stChatInput {
                margin-top: 0px !important;
            }
        </style>
        """, unsafe_allow_html=True)

        # Initialize the radio widget's session state based on business logic
        # This only happens once per "session context" to set the initial default
        radio_key = "question_type_radio_widget"
        
        # Check if we need to initialize or reset the radio widget
        if radio_key not in st.session_state:
            # First time - initialize based on first_question_asked flag
            if not st.session_state.first_question_asked:
                # First page load: default to "New Question"
                st.session_state[radio_key] = "New Question"
                st.session_state['radio_reset_triggered'] = False
                print("INIT: First page load, setting radio to 'New Question'")
            else:
                # Subsequent questions: default to "Follow-up"
                st.session_state[radio_key] = "Follow-up"
                st.session_state['radio_reset_triggered'] = False
                print("INIT: Subsequent question, setting radio to 'Follow-up'")
        else:
            # Widget exists - only do ONE-TIME reset after processing ends
            last_selection = st.session_state.get('last_radio_selection', 'Follow-up')
            was_processing = st.session_state.get('was_processing_before', False)
            is_processing_now = st.session_state.get('processing', False)
            reset_triggered = st.session_state.get('radio_reset_triggered', False)
            
            # Only reset ONCE: processing ended + user had New Question + not reset yet
            if was_processing and not is_processing_now and last_selection == "New Question" and not reset_triggered:
                st.session_state[radio_key] = "Follow-up"
                st.session_state['radio_reset_triggered'] = True
            
            # Update tracking flag
            st.session_state['was_processing_before'] = is_processing_now
        
        # **CONDITIONAL RENDERING: Only show radio button when NOT in clarification mode**
        # This hides the radio during router agent clarification questions
        if not st.session_state.get('in_clarification_mode', False):
            # Call the shared radio button rendering function
            render_radio_button_selector()
            print("✅ Radio button rendered (not in clarification mode)")
        else:
            print("🔒 Radio button hidden (in clarification mode)")

        # Handle plan approval button clicks
        if st.session_state.get('plan_approval_submitted', False):
            pending_input = st.session_state.get('pending_user_input', '')
            if pending_input:
                print(f"📋 Plan approval response: {pending_input}")
                # Clear the flags
                st.session_state.plan_approval_submitted = False
                st.session_state.pending_user_input = None
                # Process as if user typed this response
                start_processing(pending_input)
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
def render_radio_button_selector():
    """Render the radio button for selecting question type (Follow-up vs New Question).
    Dataset selector is now in the sidebar.
    This can be called from multiple places to ensure it's always available"""
    
    radio_key = "question_type_radio_widget"
    
    # NOTE: Initialization is now handled in main() function BEFORE this is called
    # We only handle the post-processing reset logic here
    if radio_key in st.session_state:
        # Check if we should do a one-time reset after processing
        last_selection = st.session_state.get('last_radio_selection', 'Follow-up')
        was_processing = st.session_state.get('was_processing_before', False)
        is_processing_now = st.session_state.get('processing', False)
        reset_triggered = st.session_state.get('radio_reset_triggered', False)
        
        if was_processing and not is_processing_now and last_selection == "New Question" and not reset_triggered:
            st.session_state[radio_key] = "Follow-up"
            st.session_state['radio_reset_triggered'] = True
        
        st.session_state['was_processing_before'] = is_processing_now
    
    # Callback for radio changes
    def on_radio_change():
        current_selection = st.session_state.get(radio_key, "New Question")
        st.session_state.last_radio_selection = current_selection
        st.session_state['radio_reset_triggered'] = False
        print(f"CALLBACK: Radio changed to: {current_selection}")
    
    # Render the radio button with label inline
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 4px;">
        <span style="font-weight: 600; font-size: 14px; white-space: nowrap;">Question Type:</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Get the current value from session state (already initialized in main())
    current_value = st.session_state.get(radio_key, "New Question")
    
    question_type = st.radio(
        "label",
        ("Follow-up", "New Question"),
        key=radio_key,
        index=0 if current_value == "Follow-up" else 1,
        on_change=on_radio_change,
        horizontal=True,
        label_visibility="collapsed"
    )
    
    st.session_state.question_type_selection = question_type
    
    print(f"RENDERED: Radio shows '{question_type}' | first_question_asked={st.session_state.first_question_asked}")
    
    return question_type

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
        
        # Radio button is already rendered in main(), don't render it again here
        # Removed duplicate call to prevent "multiple elements with same key" error

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
            functional_names = message.get('functional_names')  # Get functional_names from message
            powerbi_data = message.get('powerbi_data')  # Get Power BI data from message
            sql_generation_story = message.get('sql_generation_story')  # Get SQL story from message
            history_sql_used = message.get('history_sql_used', False)  # Get trusted SQL pattern indicator
            if sql_result:
                print(f"🔍 Rendering SQL result: type={type(sql_result)}, has_powerbi={powerbi_data is not None}, has_story={bool(sql_generation_story)}, history_sql_used={history_sql_used}")
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

                # Ensure sql_generation_story is in sql_result (for historical messages)
                if sql_generation_story and 'sql_generation_story' not in sql_result:
                    sql_result['sql_generation_story'] = sql_generation_story
                    print(f"    ✅ Added sql_generation_story to sql_result from message")

                # Feedback is only shown if the message is NOT historical
                render_sql_results(sql_result, rewritten_question, show_feedback=not is_historical, message_idx=message_idx, functional_names=functional_names, powerbi_data=powerbi_data)
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
        
        # Handle selection reasoning messages - show datasets and trusted SQL indicator side by side
        elif message_type == "selection_reasoning":
            # Render both current AND historical selection_reasoning messages
            # This ensures "Trusted SQL Pattern" indicator shows for ALL questions in history
            
            # content is now a list of functional_names
            if isinstance(content, list):
                datasets_text = ", ".join(content)
            else:
                datasets_text = str(content)
            
            # Check if history SQL was used
            history_sql_used = message.get('history_sql_used', False)
            print(f"🔍 RENDERING SELECTION_REASONING: is_historical={is_historical}, history_sql_used={history_sql_used} (type={type(history_sql_used).__name__})")
            print(f"🔍 Full message dict keys: {list(message.keys())}")
            print(f"🔍 Message content: {message}")
            
            # Use Streamlit columns for side-by-side layout
            col1, col2 = st.columns(2)
            
            with col1:
                # Selected Datasets Box - Optum Cyan styled badge
                st.markdown(f"""
                <div class="dataset-badge">
                    <span>📊</span>
                    <span>Selected Datasets: {datasets_text}</span>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                # Trusted SQL Indicator Box - conditional Optum styling
                if history_sql_used:
                    st.markdown(f"""
                    <div class="trusted-badge">
                        <span class="trusted-badge-icon">✓</span>
                        <span>Trusted SQL Pattern Used</span>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="new-sql-badge">
                        <span>⚡</span>
                        <span>New SQL Generated</span>
                    </div>
                    """, unsafe_allow_html=True)
            return

        # NEW CODE: Suppress historical followup_questions message
        elif message_type == "followup_questions":
            if is_historical:
                # If the user started a new question, the old follow-up message is marked historical. 
                # We skip rendering it to prevent the empty, greyed-out space.
                return 

        # Handle needs_followup messages with special formatting
        if message_type == "needs_followup":
            # Check if this is a plan_approval using the flag from state
            is_plan_approval = message.get('plan_approval_exists_flg', False)

            if is_plan_approval:
                # Plan approval - White background with blue left border for clean, professional look
                formatted_content = format_plan_approval_content(content)
                st.markdown(f"""
                <div class="assistant-message">
                    <div style="display: flex; align-items: flex-start; gap: 8px;">
                        <div style="flex-shrink: 0; width: 32px; height: 32px; background-color: #D74120; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: white; font-size: 16px;">
                            📋
                        </div>
                        <div style="background-color: #FFFFFF; padding: 16px 20px; border-radius: 12px; border-left: 4px solid #FF612B; flex: 1; box-shadow: 0 2px 8px rgba(0,0,0,0.06);">
                            {formatted_content}
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                # Add approval buttons (only for non-historical messages)
                if not is_historical:
                    approval_key = f"plan_approval_{message_idx}"
                    show_other_input = st.session_state.get(f"{approval_key}_show_other", False)
                    button_submitted = st.session_state.get(f"{approval_key}_submitted", False)

                    # Only show buttons if not yet submitted
                    if not button_submitted:
                        if not show_other_input:
                            # Show approval buttons with left margin to align with message content
                            st.markdown('<div style="margin-left: 40px; margin-top: 12px;">', unsafe_allow_html=True)
                            col1, col2, col3 = st.columns([1, 1, 2])
                            with col1:
                                if st.button("✅ Yes, Approve", key=f"{approval_key}_approve", type="primary"):
                                    st.session_state.pending_user_input = "approve"
                                    st.session_state.plan_approval_submitted = True
                                    st.session_state[f"{approval_key}_submitted"] = True
                                    st.rerun()
                            with col2:
                                if st.button("✏️ Modify Or Change Dataset", key=f"{approval_key}_other", type="secondary"):
                                    st.session_state[f"{approval_key}_show_other"] = True
                                    st.rerun()
                            st.markdown('</div>', unsafe_allow_html=True)
                        else:
                            # Show text input for custom response
                            st.markdown('<div style="margin-left: 40px; margin-top: 12px;">', unsafe_allow_html=True)
                            with st.form(key=f"{approval_key}_form"):
                                custom_response = st.text_area(
                                    "Provide your feedback or modifications:",
                                    placeholder="e.g., 'change to claims dataset' or 'add carrier filter' or 'modify time range to Q1-Q2'",
                                    height=80
                                )
                                form_col1, form_col2 = st.columns([1, 1])
                                with form_col1:
                                    if st.form_submit_button("Submit", type="primary"):
                                        if custom_response.strip():
                                            st.session_state.pending_user_input = custom_response.strip()
                                            st.session_state.plan_approval_submitted = True
                                            st.session_state[f"{approval_key}_show_other"] = False
                                            st.session_state[f"{approval_key}_submitted"] = True
                                            st.rerun()
                                        else:
                                            st.warning("Please enter your feedback")
                                with form_col2:
                                    if st.form_submit_button("Cancel"):
                                        st.session_state[f"{approval_key}_show_other"] = False
                                        st.rerun()
                            st.markdown('</div>', unsafe_allow_html=True)
            else:
                # Regular SQL followup - existing orange styling
                formatted_content = format_sql_followup_question(content)
                st.markdown(f"""
                <div class="assistant-message">
                    <div style="display: flex; align-items: flex-start; gap: 8px;">
                        <div style="flex-shrink: 0; width: 32px; height: 32px; background-color: #ff6b35; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: white; font-weight: 600; font-size: 14px;">
                            🤖
                        </div>
                        <div class="assistant-message-content">
                            {formatted_content}
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        else:
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
