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
    /* Import Inter font from Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
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
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    /* Assistant message styling - Left aligned, full width */
    .assistant-message {
        display: flex;
        justify-content: flex-start;
        margin: 1rem 0;
    }
    .assistant-message-content {
        background-color: #faf8f2 !important;
        color: #333;
        padding: 12px 16px;
        border-radius: 18px;
        max-width: 95%;
        word-wrap: break-word;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
        font-size: 15px;
        line-height: 1.6;
        font-style: normal !important;
        font-weight: normal !important;
        border: 1px solid #f0ede4 !important;
    }
    
    
    /* NEW: Clean narrative/insight styling - NO BACKGROUND */
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
    
    /* HTML headers within assistant messages */
    .assistant-message-content h1,
    .assistant-message-content h2,
    .assistant-message-content h3,
    .narrative-content h1,
    .narrative-content h2,
    .narrative-content h3 {
        font-family: inherit;
        font-weight: 700;
        color: #1a1a1a;
        margin: 0.8em 0 0.4em 0;
    }
    
    .assistant-message-content h1,
    .narrative-content h1 {
        font-size: 1.4em;
    }
    
    .assistant-message-content h2,
    .narrative-content h2 {
        font-size: 1.3em;
    }
    
    .assistant-message-content h3,
    .narrative-content h3 {
        font-size: 1.2em;
    }
    
    /* HTML paragraph styling */
    .assistant-message-content p,
    .narrative-content p {
        margin: 0.8em 0;
        color: #333;
        font-family: inherit;
        font-size: inherit;
        line-height: inherit;
    }
    
    /* HTML strong/bold styling - Make it more prominent */
    .assistant-message-content strong,
    .narrative-content strong {
        font-weight: 700;
        color: #1a1a1a;
        font-family: inherit;
    }
    
    /* HTML emphasis/italic styling */
    .assistant-message-content em,
    .narrative-content em {
        font-style: italic;
        color: #444;
        font-family: inherit;
    }
    
    /* HTML line breaks */
    .assistant-message-content br,
    .narrative-content br {
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
