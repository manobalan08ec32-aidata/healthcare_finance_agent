import streamlit as st
import asyncio
import uuid
import time
from datetime import datetime
from core.databricks_client import DatabricksClient
from workflows.langraph_workflow import AsyncHealthcareFinanceWorkflow
from typing import Dict, Any

# Page configuration
st.set_page_config(
    page_title="Healthcare Finance Assistant",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Set sidebar width via CSS and hide collapse button
st.markdown(
    """
    <style>
        [data-testid="stSidebar"] {
            min-width: 220px;
            max-width: 220px;
            width: 220px;
        }
        
        /* Hide the sidebar collapse button */
        [data-testid="collapsedControl"] {
            display: none !important;
        }
        
        /* Also hide the expand button when sidebar is collapsed */
        [data-testid="stSidebarNav"] button[kind="header"] {
            display: none !important;
        }
        
        /* Hide any arrow icons in sidebar */
        [data-testid="stSidebar"] [data-baseweb="icon"] {
            display: none !important;
        }
    </style>
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
    initial_state = {
        'original_question': user_query,
        'current_question': user_query,
        'session_id': session_id,
        'user_id': f"streamlit_user_{session_id}",
        'user_questions_history': user_questions_history,
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
            if name not in ('__end__','workflow_end'):
                if name not in node_placeholders:
                    node_placeholders[name] = status_region.empty()
                    node_placeholders[name].info(f"▶️ {name} running…")
            if et in ('node_end','workflow_end'):
                if name in node_placeholders:
                    node_placeholders[name].success(f"✅ {name} done")
                if name == 'navigation_controller':
                    nav_err = state.get('nav_error_msg')
                    greeting = state.get('greeting_response')
                    domain_q = state.get('domain_followup_question')
                    if nav_err:
                        add_assistant_message(nav_err, message_type="error")
                        return
                    if greeting:
                        add_assistant_message(greeting, message_type="greeting")
                        return
                    if state.get('requires_domain_clarification') and domain_q:
                        add_assistant_message(domain_q, message_type="domain_clarification")
                        return
                if name == 'router_agent':
                    if state.get('router_error_msg'):
                        add_assistant_message(state.get('router_error_msg'), message_type="error")
                        return
                    if state.get('requires_dataset_clarification') and state.get('dataset_followup_question'):
                        add_assistant_message(state.get('dataset_followup_question'), message_type="dataset_clarification")
                        return
                    if state.get('missing_dataset_items') and state.get('user_message'):
                        add_assistant_message(state.get('user_message'), message_type="missing_items")
                        return
                    sql_result = state.get('sql_result', {})
                    if sql_result and sql_result.get('success'):
                        rewritten_question = state.get('rewritten_question', initial_state['current_question'])
                        add_sql_result_message(sql_result, rewritten_question)
                        last_state = state
                        followup_placeholder.info("⏳ Generating follow-up questions…")
                    else:
                        add_assistant_message("No results available or SQL execution failed.", message_type="error")
                        return
                if et == 'workflow_end' and not last_state:
                    last_state = state
        if last_state and last_state.get('sql_result', {}).get('success'):
            try:
                followup_state = last_state
                follow_res = await workflow.ainvoke_followup(followup_state, config)
                fq = follow_res.get('followup_questions', [])
                if fq:
                    st.session_state.current_followup_questions = fq
                    followup_placeholder.empty()
                    add_assistant_message("💡 Would you like to explore further? Here are some suggested follow-up questions:", message_type="followup_questions")
                else:
                    followup_placeholder.empty()
            except Exception as fe:
                followup_placeholder.error(f"Follow-up generation failed: {fe}")
    except Exception as e:
        add_assistant_message(f"Workflow failed: {e}", message_type="error")

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
    if 'last_clicked_question' not in st.session_state:
        st.session_state.last_clicked_question = None
    if 'current_query' not in st.session_state:
        st.session_state.current_query = ""
    if 'current_conversation_start' not in st.session_state:
        st.session_state.current_conversation_start = -1
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
    
    def format_value(val):
        # Handle None/NaN values
        if pd.isna(val) or val is None:
            return ""
        
        # Convert to string first
        val_str = str(val)
        
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
            
            # Check if it's a percentage (between 0 and 1 or explicitly marked)
            if 0 <= abs(numeric_val) <= 1 and ('rate' in str(type(val)).lower() or 'percent' in str(type(val)).lower()):
                return f"{numeric_val:.2%}"
            
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
        df[col] = df[col].apply(format_value)
    
    return df

def render_sql_results(sql_result, rewritten_question=None):
    """Render SQL results with title, expandable SQL query, data table, and narrative"""
    
    # Check if multiple results
    if sql_result.get('multiple_results', False):
        query_results = sql_result.get('query_results', [])
        for i, result in enumerate(query_results):
            title = result.get('title', f'Query {i+1}')
            sql_query = result.get('sql_query', '')
            data = result.get('data', [])
            narrative = result.get('narrative', '')
            
            render_single_sql_result(title, sql_query, data, narrative)
    else:
        # Single result - use rewritten_question if available, otherwise default
        title = rewritten_question if rewritten_question else "Analysis Results"
        sql_query = sql_result.get('sql_query', '')
        data = sql_result.get('query_results', [])
        narrative = sql_result.get('narrative_response', '')
        
        render_single_sql_result(title, sql_query, data, narrative)

def render_single_sql_result(title, sql_query, data, narrative):
    """Render a single SQL result with warm gold background for title and narrative"""
    
    # Title with custom narrative-content styling
    st.markdown(f"""
    <div class="narrative-content">
        <strong>AI Rewritten Question:</strong> {title}
    </div>
    """, unsafe_allow_html=True)
    
    # SQL Query in expander
    if sql_query:
        with st.expander("🔍 View SQL Query", expanded=False):
            st.code(sql_query, language="sql")
    
    # Data table
    if data:
        formatted_df = format_sql_data_for_streamlit(data)
        if not formatted_df.empty:
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
    print(f"✅ Added {message_type} message (total messages now: {len(st.session_state.messages)}): {content[:50]}...")
    
    # Debug: Show current message state
    print(f"📊 Current message summary:")
    for i, msg in enumerate(st.session_state.messages[-3:]):  # Show last 3 messages
        role = msg.get('role', 'unknown')
        msg_type = msg.get('message_type', 'standard')
        content_preview = msg.get('content', '')[:30] + "..." if len(msg.get('content', '')) > 30 else msg.get('content', '')
        print(f"  Message {len(st.session_state.messages)-3+i}: {role}({msg_type}) - {content_preview}")
    
    # Handle special state updates
    if message_type == "domain_clarification":
        st.session_state.current_followup_questions = []

def add_sql_result_message(sql_result, rewritten_question=None):
    """
    Add SQL result message to chat history for proper rendering
    """
    message = {
        "role": "assistant",
        "content": "SQL analysis complete",  # Placeholder content
        "message_type": "sql_result",
        "timestamp": datetime.now().isoformat(),
        "sql_result": sql_result,
        "rewritten_question": rewritten_question
    }
    
    st.session_state.messages.append(message)
    print(f"✅ Added SQL result message (total messages now: {len(st.session_state.messages)})")
    
    # Debug: Show SQL result summary
    if sql_result.get('multiple_results'):
        print(f"  📊 Multiple SQL results: {len(sql_result.get('query_results', []))} queries")
    else:
        data_count = len(sql_result.get('query_results', []))
        print(f"  📊 Single SQL result: {data_count} rows returned")

def start_processing(user_query: str):
    """Start processing user query - with proper message management"""
    print(f"🎯 Starting processing for: {user_query}")
    
    # Add user message to history
    st.session_state.messages.append({
        "role": "user",
        "content": user_query
    })
    
    # IMPORTANT: Mark where this conversation starts so we can manage responses properly
    st.session_state.current_conversation_start = len(st.session_state.messages) - 1
    
    # Set processing state
    st.session_state.current_query = user_query
    st.session_state.processing = True
    st.session_state.workflow_started = False

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
        max-width: 95%;  /* Same as system messages */
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
        max-width: 80%;
        word-wrap: break-word;
        border: 1px solid #e0e0e0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Custom styling for AI rewritten question and narrative content */
    .narrative-content {
        color: #333;
        padding: 12px 16px;
        margin: 8px 0;
        max-width: 95%;
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
    
    /* Follow-up question buttons styling - matching user message dark blue */
    .stButton > button {
        background-color: #ffffff !important;
        color: #1e3a8a !important;
        border: 2px solid #1e3a8a !important;
        border-radius: 8px !important;
        padding: 12px 16px !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
        font-size: 15px !important;
        font-weight: 500 !important;
        text-align: left !important;
        transition: all 0.2s ease !important;
        margin: 6px 0 !important;
        height: auto !important;
        white-space: normal !important;
        word-wrap: break-word !important;
        min-height: 48px !important;
    }
    
    /* More specific selector to ensure dark blue text color */
    div[data-testid="stButton"] > button {
        color: #1e3a8a !important;
    }
    
    /* Even more specific selector for button text */
    .stButton > button span {
        color: #1e3a8a !important;
    }
    
    .stButton > button:hover {
        background-color: #1e3a8a !important;
        color: #ffffff !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 12px rgba(30, 58, 138, 0.2) !important;
    }
    
    .stButton > button:focus {
        border-color: #1e3a8a !important;
        box-shadow: 0 0 0 3px rgba(30, 58, 138, 0.2) !important;
    }
    </style>
    """, unsafe_allow_html=True)

# ============ MAIN APPLICATION ============

def main():
    """Main Streamlit application with async workflow integration and real-time chat history"""

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
        </style>
        """, unsafe_allow_html=True)
        
        if st.button("Back to Main Page", key="back_to_main"):
            st.switch_page("main.py")
    
    try:
        initialize_session_state()
        workflow = get_session_workflow()
        
        if workflow is None:
            st.error("Failed to initialize async workflow. Please refresh the page.")
            return
        
        st.markdown("# Healthcare Finance Assistant")
        st.markdown("---")
        
        # Chat container for messages
        chat_container = st.container()
        
        with chat_container:
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            
            if not st.session_state.messages:
                st.markdown("""
                <div class="narrative-content">
                    Welcome! I'm your Healthcare Finance Assistant.<br>
                    Ask me about claims and Ledger related healthcare finance questions.<br>
                </div>
                """, unsafe_allow_html=True)
            
            # Render all chat messages (including real-time updates)
            for idx, message in enumerate(st.session_state.messages):
                render_chat_message_enhanced(message, idx)
            
            # Render follow-up questions if they exist
            render_persistent_followup_questions()
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Processing indicator and streaming workflow execution
        if st.session_state.processing:
            with st.spinner("Running analysis..."):
                run_streaming_workflow(workflow, st.session_state.current_query)
            st.session_state.processing = False
            st.session_state.workflow_started = False
            st.rerun()
        
        # Chat input at the bottom
        if prompt := st.chat_input("Ask a question...", disabled=st.session_state.processing):
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
    """Render followup questions as clickable buttons"""
    if hasattr(st.session_state, 'current_followup_questions') and st.session_state.current_followup_questions:
        # Show buttons one by one vertically without the header
        for idx, question in enumerate(st.session_state.current_followup_questions):
            # Create a unique key using question content hash and index
            button_key = f"followup_{idx}_{hash(question) % 10000}"
            
            if st.button(
                question, 
                key=button_key,
                help=f"Click to explore: {question}",
                use_container_width=True
            ):
                print(f"🔄 Follow-up question clicked: {question}")
                # Clear the current follow-up questions to prevent re-rendering
                st.session_state.current_followup_questions = []
                # Start processing the selected question
                start_processing(question)
                st.rerun()

def render_chat_message_enhanced(message, message_idx):
    """Enhanced chat message rendering with message type awareness"""
    
    role = message.get('role', 'user')
    content = message.get('content', '')
    message_type = message.get('message_type', 'standard')
    timestamp = message.get('timestamp', '')
    
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
            if sql_result:
                render_sql_results(sql_result, rewritten_question)
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
