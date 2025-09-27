import streamlit as st
import asyncio
import uuid
import time
import json
from datetime import datetime
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from workflows.async_langraph_workflow import AsyncHealthcareFinanceWorkflow
from langgraph.types import Command
import pandas as pd
import html
import hashlib
import threading
from typing import Dict, Any

# ============ ASYNC STREAMING HELPER FUNCTIONS ============

def get_current_node_name(step_data: Dict) -> str:
    """Extract current node name from step data"""
    if step_data:
        for node_name, node_data in step_data.items():
            if node_name != "__end__":
                return node_name.replace("_", " ").title()
    return "Processing"

def get_next_agent_from_state(step_data: Dict) -> str:
    """Extract next agent from step data"""
    if step_data:
        for node_name, node_data in step_data.items():
            if isinstance(node_data, dict) and 'next_agent_disp' in node_data:
                return node_data.get('next_agent_disp', 'Processing')
    return "Processing"

def extract_response_content(step_data: Dict) -> Dict[str, Any]:
    """Extract response content from step data"""
    if not step_data:
        return None
        
    for node_name, node_data in step_data.items():
        if node_name != "__end__" and isinstance(node_data, dict):
            return {
                'node_name': node_name,
                'data': node_data,
                'greeting_response': node_data.get('greeting_response'),
                'domain_followup_question': node_data.get('domain_followup_question'),
                'requires_domain_clarification': node_data.get('requires_domain_clarification', False),
                'rewritten_question': node_data.get('rewritten_question'),
                'question_type': node_data.get('question_type'),
                'domain_selection': node_data.get('domain_selection'),
                'nav_error_msg': node_data.get('nav_error_msg')
            }
    
    return None

def update_spinner(spinner_placeholder, current_node: str, next_agent: str):
    """Update the spinner with current progress"""
    spinner_placeholder.markdown(f"""
    <div class="spinner-container">
        <div class="spinner"></div>
        <div class="spinner-message">🤖 {current_node}... Next: {next_agent}</div>
    </div>
    """, unsafe_allow_html=True)

# ============ ASYNC WORKFLOW EXECUTION ============

async def _execute_workflow_async(workflow, initial_state, config, spinner_placeholder, message_container):
    """
    Async version of workflow execution with real-time updates and chat history
    """
    print("🚀 Starting async workflow execution...")
    
    all_response_data = []
    current_node = "Starting"
    
    try:
        # Initial spinner
        update_spinner(spinner_placeholder, current_node, "Navigation Controller")
        
        # Stream the async workflow
        async for step_data in workflow.astream(initial_state, config=config):
            # Update the spinner with current progress
            current_node = get_current_node_name(step_data)
            next_agent = get_next_agent_from_state(step_data)
            update_spinner(spinner_placeholder, current_node, next_agent)

            # Extract structured data from the current step
            response_data = extract_response_content(step_data) 
            if response_data:
                all_response_data.append(response_data)
                print(f"📦 Collected response from {response_data['node_name']}")
                
                # REAL-TIME CHAT HISTORY UPDATE
                # Process and add to chat history immediately for each step
                process_step_response_to_history(response_data, message_container)
        
        return all_response_data
        
    except Exception as e:
        print(f"❌ Async workflow execution failed: {str(e)}")
        # Add error to chat history immediately
        add_error_to_chat_history(str(e), message_container)
        
        return [{
            'node_name': 'error',
            'data': {'nav_error_msg': str(e)},
            'greeting_response': f"An error occurred: {str(e)}",
            'domain_followup_question': None,
            'requires_domain_clarification': False,
            'nav_error_msg': str(e)
        }]

def execute_workflow_streaming(workflow):
    """
    Bridge function: Sync Streamlit to Async workflow execution with real-time chat updates
    """
    st.session_state.workflow_started = True
    session_id = st.session_state.session_id
    
    try:
        print(f"Starting workflow execution for: {st.session_state.current_query}")
        
        spinner_placeholder = st.empty()
        message_container = st.container()  # Container for real-time message updates
        current_node = "Starting"
        
        # Initial spinner
        update_spinner(spinner_placeholder, current_node, "Navigation Controller")
        
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
        
        # Execute async workflow in sync context with real-time updates
        all_response_data = asyncio.run(_execute_workflow_async(
            workflow, initial_state, config, spinner_placeholder, message_container
        ))
        
        spinner_placeholder.empty()
        
        # Final processing after workflow completion
        finalize_workflow_responses(all_response_data)
        
        print(f"Workflow execution completed. Collected {len(all_response_data)} response chunks.")

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
            print(f"✅ Async workflow created and stored in session state for session: {st.session_state.session_id}")
        except Exception as e:
            print(f"❌ Failed to create async workflow for session {st.session_state.session_id}: {str(e)}")
            st.error(f"Failed to create workflow: {e}")
            return None
    return st.session_state.workflow

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

# ============ REAL-TIME RESPONSE PROCESSING ============

def process_step_response_to_history(response_data, message_container):
    """
    Process each workflow step response and add to chat history in real-time
    """
    if not response_data:
        return
    
    node_name = response_data.get('node_name', 'unknown')
    print(f"Processing real-time response from {node_name}")
    
    # Handle different types of responses immediately
    
    # 1. Handle greeting responses
    greeting_response = response_data.get('greeting_response')
    if greeting_response and greeting_response.strip():
        add_assistant_message(greeting_response, message_type="greeting")
        print("Added greeting response to history")
        return
    
    # 2. Handle domain clarification requests
    domain_followup = response_data.get('domain_followup_question')
    requires_clarification = response_data.get('requires_domain_clarification', False)
    if domain_followup and domain_followup.strip() and requires_clarification:
        add_assistant_message(domain_followup, message_type="domain_clarification")
        # Update session state for domain clarification
        st.session_state.requires_domain_clarification = True
        st.session_state.pending_business_question = response_data.get('data', {}).get('pending_business_question', '')
        print("Added domain clarification to history")
        return
    
    # 3. Handle navigation errors
    nav_error = response_data.get('nav_error_msg')
    if nav_error:
        add_assistant_message(f"I encountered an issue: {nav_error}", message_type="error")
        print("Added error message to history")
        return
    
    # 4. Handle successful navigation (show progress)
    rewritten_question = response_data.get('rewritten_question')
    question_type = response_data.get('question_type')
    domain_selection = response_data.get('domain_selection')
    
    if rewritten_question and node_name == 'navigation_controller':
        # Show navigation success with what the system understood
        progress_message = f"I understand you're asking: **{rewritten_question}**"
        
        if domain_selection:
            domain_text = ", ".join(domain_selection)
            progress_message += f"\n\nI'll analyze this for: **{domain_text}**"
        
        if question_type == "why":
            progress_message += "\n\nThis looks like a 'why' question - I'll prepare a drill-through analysis."
        else:
            progress_message += "\n\nI'm preparing to analyze the data for you..."
        
        add_assistant_message(progress_message, message_type="navigation_success")
        print(f"Added navigation success message to history")
        return
    
    # 5. Handle other workflow steps (for future expansion)
    if node_name == 'entry_router':
        print("Entry router completed - preparing navigation...")
    elif node_name not in ['navigation_controller', 'entry_router']:
        # For future nodes like SQL generator, follow-up questions, etc.
        add_assistant_message(f"Processing step: {node_name.replace('_', ' ').title()}...", message_type="progress")
        print(f"Added progress message for {node_name}")

def add_assistant_message(content, message_type="standard"):
    """
    Add assistant message to chat history with proper formatting
    """
    # Create message with metadata
    message = {
        "role": "assistant",
        "content": content,
        "message_type": message_type,
        "timestamp": datetime.now().isoformat()
    }
    
    # Add to session state
    st.session_state.messages.append(message)
    
    # For certain message types, update additional state
    if message_type == "domain_clarification":
        st.session_state.current_followup_questions = []  # Clear old follow-ups
    elif message_type == "navigation_success":
        # This indicates navigation is complete and we're moving to next steps
        pass

def add_error_to_chat_history(error_message, message_container):
    """
    Add error message to chat history immediately
    """
    error_msg = f"I encountered an error: {error_message}"
    add_assistant_message(error_msg, message_type="error")
    print("Added error to chat history")

def finalize_workflow_responses(all_response_data):
    """
    Final processing after workflow completion - handle any remaining items
    """
    if not all_response_data:
        add_assistant_message("I didn't receive any response from the workflow. Please try again.", message_type="error")
        return
    
    # Check if we need to add any final messages
    last_response = all_response_data[-1] if all_response_data else None
    
    if last_response:
        # If the last step was successful navigation, add a completion message
        if (last_response.get('node_name') == 'navigation_controller' and 
            last_response.get('rewritten_question') and 
            not last_response.get('requires_domain_clarification')):
            
            completion_msg = "Navigation completed successfully! In the full system, I would now proceed with data analysis."
            add_assistant_message(completion_msg, message_type="completion")
    
    print(f"Finalized workflow with {len(all_response_data)} steps")

def start_processing(user_query: str):
    """Start processing user query"""
    print(f"🎯 Starting processing for: {user_query}")
    
    # Add user message to history
    st.session_state.messages.append({
        "role": "user",
        "content": user_query
    })
    
    # Set processing state
    st.session_state.current_query = user_query
    st.session_state.processing = True
    st.session_state.workflow_started = False

# ============ CSS STYLING FOR MESSAGE TYPES ============

def add_message_type_css():
    """Add CSS for different message types"""
    st.markdown("""
    <style>
    /* Enhanced message type styling */
    .error-message {
        background-color: #ffeaea !important;
        border-left: 4px solid #ff4444 !important;
        border-color: #ff4444 !important;
    }
    
    .clarification-message {
        background-color: #fff4e6 !important;
        border-left: 4px solid #ff9500 !important;
        border-color: #ff9500 !important;
    }
    
    .success-message {
        background-color: #f0f9ff !important;
        border-left: 4px solid #0066cc !important;
        border-color: #0066cc !important;
    }
    
    .progress-message {
        background-color: #f8f9fa !important;
        border-left: 4px solid #6c757d !important;
        border-color: #6c757d !important;
        font-style: italic;
    }
    
    .completion-message {
        background-color: #f0fff4 !important;
        border-left: 4px solid #28a745 !important;
        border-color: #28a745 !important;
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
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 8px 0;
        border-left: 4px solid #007bff;
    }
    
    .spinner-message {
        color: #495057;
        font-weight: 500;
    }
    </style>
    """, unsafe_allow_html=True)

# ============ MAIN APPLICATION ============

def main():
    """Main Streamlit application with async workflow integration and real-time chat history"""

    # Add enhanced CSS styling
    add_message_type_css()

    with st.sidebar:
        st.markdown("### Healthcare Finance Assistant")
        if st.button("Back to Main Page", key="back_to_main"):
            st.switch_page("main.py")
    
    try:
        initialize_session_state()
        workflow = get_session_workflow()
        
        if workflow is None:
            st.error("Failed to initialize async workflow. Please refresh the page.")
            return
        
        st.markdown("# Healthcare Finance Assistant (Async)")
        st.markdown(f"*Session: {st.session_state.session_id[:8]}...*")
        st.markdown("---")
        
        # Chat container for messages
        chat_container = st.container()
        
        with chat_container:
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            
            if not st.session_state.messages:
                st.markdown("""
                <div class="welcome-message">
                    Welcome! I'm your Healthcare Finance Assistant (Async Version).<br>
                    Ask me about claims and Ledger related healthcare finance questions.<br>
                    <small>Real-time responses enabled</small>
                </div>
                """, unsafe_allow_html=True)
            
            # Render all chat messages (including real-time updates)
            for idx, message in enumerate(st.session_state.messages):
                render_chat_message_enhanced(message, idx)
            
            # Render follow-up questions if they exist
            render_persistent_followup_questions()
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Processing indicator and workflow execution
        if st.session_state.processing and not st.session_state.workflow_started:
            # Show a processing indicator before starting workflow
            with st.spinner("Initializing workflow..."):
                time.sleep(0.1)  # Small delay to show the spinner
            execute_workflow_streaming(workflow)
        
        # Chat input at the bottom
        if prompt := st.chat_input("Ask a question...", disabled=st.session_state.processing):
            start_processing(prompt)
            st.rerun()
        
    except Exception as e:
        st.error(f"Application Error: {str(e)}")
        st.session_state.processing = False
        st.session_state.workflow_started = False

# Keep your existing render_persistent_followup_questions function
def render_persistent_followup_questions():
    """Render followup questions (placeholder - use your existing implementation)"""
    # Use your existing followup questions rendering logic here
    if hasattr(st.session_state, 'current_followup_questions') and st.session_state.current_followup_questions:
        st.markdown("### Follow-up Questions:")
        for question in st.session_state.current_followup_questions:
            if st.button(question, key=f"followup_{hash(question)}"):
                start_processing(question)
                st.rerun().*")
        st.markdown("---")
        
        # Chat container for messages
        chat_container = st.container()
        
        with chat_container:
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            
            if not st.session_state.messages:
                st.markdown("""
                <div class="welcome-message">
                    👋 Welcome! I'm your Healthcare Finance Assistant (Async Version).<br>
                    Ask me about claims and Ledger related healthcare finance questions.<br>
                    <small>Real-time responses enabled ⚡</small>
                </div>
                """, unsafe_allow_html=True)
            
            # Render all chat messages (including real-time updates)
            for idx, message in enumerate(st.session_state.messages):
                render_chat_message_enhanced(message, idx)
            
            # Render follow-up questions if they exist
            render_persistent_followup_questions()
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Processing indicator and workflow execution
        if st.session_state.processing and not st.session_state.workflow_started:
            # Show a processing indicator before starting workflow
            with st.spinner("Initializing workflow..."):
                time.sleep(0.1)  # Small delay to show the spinner
            execute_workflow_streaming(workflow)
        
        # Chat input at the bottom
        if prompt := st.chat_input("Ask a question...", disabled=st.session_state.processing):
            start_processing(prompt)
            st.rerun()
        
    except Exception as e:
        st.error(f"Application Error: {str(e)}")
        st.session_state.processing = False
        st.session_state.workflow_started = False

def render_chat_message_enhanced(message, message_idx):
    """Enhanced chat message rendering with message type awareness"""
    
    role = message.get('role', 'user')
    content = message.get('content', '')
    message_type = message.get('message_type', 'standard')
    timestamp = message.get('timestamp', '')
    
    if role == 'user':
        # User message styling (your existing styling)
        st.markdown(f"""
        <div class="user-message">
            <div class="user-message-content">
                {content}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    elif role == 'assistant':
        # Enhanced assistant message with type-specific styling
        css_class = get_message_css_class(message_type)
        
        # Add visual indicators for different message types
        if message_type == "greeting":
            icon = "👋"
        elif message_type == "domain_clarification": 
            icon = "❓"
        elif message_type == "navigation_success":
            icon = "🧭"
        elif message_type == "progress":
            icon = "⚙️"
        elif message_type == "error":
            icon = "❌"
        elif message_type == "completion":
            icon = "✅"
        else:
            icon = "🤖"
        
        st.markdown(f"""
        <div class="assistant-message">
            <div class="{css_class}">
                <div style="display: flex; align-items: center; margin-bottom: 8px;">
                    <span style="margin-right: 8px; font-size: 16px;">{icon}</span>
                    <small style="color: #666; font-size: 11px;">
                        {get_message_type_label(message_type)}
                        {f" • {format_timestamp(timestamp)}" if timestamp else ""}
                    </small>
                </div>
                {content}
            </div>
        </div>
        """, unsafe_allow_html=True)

def get_message_css_class(message_type):
    """Get CSS class based on message type"""
    if message_type == "error":
        return "assistant-message-content error-message"
    elif message_type == "domain_clarification":
        return "assistant-message-content clarification-message"
    elif message_type == "navigation_success":
        return "assistant-message-content success-message"
    elif message_type == "progress":
        return "assistant-message-content progress-message"
    elif message_type == "completion":
        return "assistant-message-content completion-message"
    else:
        return "assistant-message-content"

def get_message_type_label(message_type):
    """Get human-readable label for message type"""
    labels = {
        "greeting": "Welcome",
        "domain_clarification": "Domain Selection",
        "navigation_success": "Navigation Complete",
        "progress": "Processing",
        "error": "Error",
        "completion": "Complete",
        "standard": "Response"
    }
    return labels.get(message_type, "Response")

def format_timestamp(timestamp):
    """Format timestamp for display"""
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return dt.strftime("%H:%M:%S")
    except:
        return ""

if __name__ == "__main__":
    main()
