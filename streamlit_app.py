import streamlit as st
import json
import uuid
import time
from datetime import datetime
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from workflows.langraph_workflow import HealthcareFinanceWorkflow

# Page configuration
st.set_page_config(
    page_title="Healthcare Finance AI Agent",
    page_icon="🏥",
    layout="wide"
)

# Initialize session state
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'workflow_steps' not in st.session_state:
    st.session_state.workflow_steps = []

# Initialize workflow
@st.cache_resource
def initialize_workflow():
    """Initialize Databricks client and LangGraph workflow"""
    try:
        db_client = DatabricksClient()
        if not db_client.test_connection():
            st.error("❌ Failed to connect to Databricks")
            return None
        workflow = HealthcareFinanceWorkflow(db_client)
        return workflow
    except Exception as e:
        st.error(f"❌ Workflow initialization failed: {str(e)}")
        return None

def main():
    st.title("🏥 Healthcare Finance Assistant")
    st.markdown("Ask questions about claims, costs, and financial analysis")
    
    # Initialize workflow
    workflow = initialize_workflow()
    if not workflow:
        st.stop()
    
    # Create two columns - main chat and workflow steps
    col1, col2 = st.columns([2, 1])
    
    with col1:
        display_chat_interface(workflow)
    
    with col2:
        display_workflow_steps()

def display_chat_interface(workflow):
    """Chat interface with streaming support"""
    
    # Chat container
    chat_container = st.container()
    
    with chat_container:
        # Display chat messages
        if st.session_state.chat_history:
            for message in st.session_state.chat_history:
                display_message(message)
        else:
            st.info("💭 Hi! Ask me about healthcare finance data, claims analysis, or financial variances.")
    
    # Input area at bottom
    st.markdown("---")
    
    # Create input form
    with st.form(key="chat_form", clear_on_submit=True):
        col1, col2 = st.columns([5, 1])
        
        with col1:
            user_input = st.text_input(
                "",
                placeholder="Ask about claims, revenue, costs, variances...",
                disabled=st.session_state.processing,
                key="message_input"
            )
        
        with col2:
            submit_button = st.form_submit_button(
                "Send",
                disabled=st.session_state.processing,
                type="primary"
            )
        
        # Process input when form is submitted
        if submit_button and user_input and not st.session_state.processing:
            process_message_with_streaming(user_input, workflow)

def display_workflow_steps():
    """Display workflow steps in real-time"""
    
    st.subheader("🔄 Workflow Steps")
    
    if st.session_state.workflow_steps:
        for i, step in enumerate(st.session_state.workflow_steps):
            with st.container():
                # Step header
                agent_name = step.get('agent', 'Unknown')
                status = step.get('status', 'completed')
                timestamp = step.get('timestamp', '')
                
                # Status icon
                if status == 'running':
                    icon = "⏳"
                elif status == 'completed':
                    icon = "✅"
                elif status == 'error':
                    icon = "❌"
                else:
                    icon = "🔄"
                
                st.markdown(f"**{icon} {agent_name}**")
                
                # Step details
                if step.get('details'):
                    with st.expander(f"Details", expanded=(status == 'running')):
                        for detail in step['details']:
                            st.write(f"• {detail}")
                
                # Results
                if step.get('results'):
                    st.json(step['results'])
                
                st.markdown("---")
    else:
        st.write("No workflow steps yet. Send a message to start!")
    
    # Clear steps button
    if st.button("🗑️ Clear Steps"):
        st.session_state.workflow_steps = []
        st.rerun()

def process_message_with_streaming(user_input, workflow):
    """Process message with real-time streaming updates"""
    
    # Set processing state
    st.session_state.processing = True
    
    # Add user message immediately
    add_user_message(user_input)
    
    # Clear previous workflow steps
    st.session_state.workflow_steps = []
    
    # Add thinking message
    thinking_message = {
        'type': 'assistant',
        'content': '🤔 Starting workflow analysis...',
        'timestamp': datetime.now(),
        'processing': True
    }
    st.session_state.chat_history.append(thinking_message)
    
    # Create placeholders for real-time updates
    workflow_placeholder = st.empty()
    chat_placeholder = st.empty()
    
    try:
        # Stream the workflow execution
        for step_data in workflow.stream_workflow(
            user_question=user_input,
            session_id=st.session_state.session_id,
            user_id="streamlit_user"
        ):
            # Process step data
            process_workflow_step(step_data)
            
            # Update UI in real-time
            st.rerun()
        
        # Remove thinking message
        if st.session_state.chat_history and st.session_state.chat_history[-1].get('processing'):
            st.session_state.chat_history.pop()
        
        # Add final response
        add_final_response()
    
    except Exception as e:
        # Remove thinking message
        if st.session_state.chat_history and st.session_state.chat_history[-1].get('processing'):
            st.session_state.chat_history.pop()
        
        # Add error response
        add_error_response(str(e))
    
    finally:
        # Reset processing state
        st.session_state.processing = False
        st.rerun()

def process_workflow_step(step_data):
    """Process individual workflow step"""
    
    step = step_data.get('step', {})
    timestamp = step_data.get('timestamp', datetime.now().isoformat())
    
    # Extract step information
    for node_name, node_state in step.items():
        if node_name == '__start__':
            continue
            
        # Add or update workflow step
        step_info = {
            'agent': node_name,
            'status': 'completed',
            'timestamp': timestamp,
            'details': extract_step_details(node_name, node_state),
            'results': extract_step_results(node_name, node_state)
        }
        
        st.session_state.workflow_steps.append(step_info)

def extract_step_details(node_name, node_state):
    """Extract readable details from node state"""
    
    details = []
    
    if node_name == 'navigation_controller':
        if node_state.get('current_question'):
            details.append(f"Processing: {node_state['current_question']}")
        if node_state.get('question_type'):
            details.append(f"Question Type: {node_state['question_type']}")
        if node_state.get('next_agent'):
            details.append(f"Next Agent: {node_state['next_agent']}")
    
    elif node_name == 'router_agent':
        if node_state.get('selected_dataset'):
            details.append(f"Selected: {node_state['selected_dataset'].split('.')[-1]}")
        if node_state.get('selection_confidence'):
            details.append(f"Confidence: {node_state['selection_confidence']:.1%}")
    
    elif node_name == 'sql_generator_agent':
        details.append("Generating SQL for 'what' question")
        if node_state.get('selected_dataset'):
            details.append(f"Using: {node_state['selected_dataset'].split('.')[-1]}")
    
    elif node_name == 'root_cause_agent':
        details.append("Analyzing 'why' question")
        if node_state.get('current_question'):
            details.append(f"Question: {node_state['current_question']}")
    
    elif node_name == 'workflow_complete':
        details.append("Workflow completed successfully")
        if node_state.get('question_type'):
            details.append(f"Type: {node_state['question_type']}")
    
    return details

def extract_step_results(node_name, node_state):
    """Extract key results from node state"""
    
    results = {}
    
    if node_name == 'navigation_controller':
        results = {
            'rewritten_question': node_state.get('current_question'),
            'question_type': node_state.get('question_type'),
            'next_agent': node_state.get('next_agent')
        }
    
    elif node_name == 'router_agent':
        results = {
            'selected_dataset': node_state.get('selected_dataset'),
            'confidence': node_state.get('selection_confidence'),
            'reasoning': node_state.get('selection_reasoning', '')[:100] + '...' if node_state.get('selection_reasoning') else ''
        }
    
    elif node_name == 'sql_generator_agent':
        results = {
            'sql_generated': bool(node_state.get('generated_sql')),
            'has_results': bool(node_state.get('query_results'))
        }
    
    elif node_name == 'root_cause_agent':
        results = {
            'analysis_completed': bool(node_state.get('root_cause_analysis')),
            'variance_detected': node_state.get('variance_detected', False)
        }
    
    # Filter out None values
    return {k: v for k, v in results.items() if v is not None}

def add_user_message(message):
    """Add user message to chat immediately"""
    user_message = {
        'type': 'user',
        'content': message,
        'timestamp': datetime.now()
    }
    st.session_state.chat_history.append(user_message)

def add_final_response():
    """Add final response based on workflow results"""
    
    # Get the last workflow step to determine what happened
    if st.session_state.workflow_steps:
        last_step = st.session_state.workflow_steps[-1]
        agent_name = last_step.get('agent')
        
        if agent_name == 'sql_generator_agent':
            content = "✅ **SQL Analysis Complete!**\n\nGenerated SQL query and retrieved results for your 'what' question."
        elif agent_name == 'root_cause_agent':
            content = "✅ **Root Cause Analysis Complete!**\n\nAnalyzed the underlying causes for your 'why' question."
        elif agent_name == 'router_agent':
            dataset = last_step.get('results', {}).get('selected_dataset', 'Unknown')
            dataset_name = dataset.split('.')[-1] if dataset else 'Unknown'
            content = f"✅ **Dataset Selected!**\n\nSelected **{dataset_name}** dataset for analysis."
        else:
            content = "✅ **Analysis Complete!**\n\nWorkflow executed successfully."
    else:
        content = "✅ **Analysis Complete!**\n\nWorkflow executed successfully."
    
    response_message = {
        'type': 'assistant',
        'content': content,
        'timestamp': datetime.now(),
        'workflow_complete': True
    }
    st.session_state.chat_history.append(response_message)

def add_error_response(error_msg):
    """Add error response to chat"""
    error_message = {
        'type': 'assistant',
        'content': f"❌ Sorry, I encountered an error: {error_msg}",
        'timestamp': datetime.now(),
        'error': True
    }
    st.session_state.chat_history.append(error_message)

def display_message(message):
    """Display a single chat message"""
    
    msg_type = message['type']
    content = message['content']
    timestamp = message['timestamp']
    
    # Format timestamp
    time_str = timestamp.strftime("%H:%M")
    
    if msg_type == 'user':
        # User message (right side)
        st.markdown(f"""
        <div style="display: flex; justify-content: flex-end; margin: 10px 0;">
            <div style="background-color: #007bff; color: white; padding: 10px 15px; border-radius: 18px; max-width: 70%; word-wrap: break-word;">
                {content}
                <div style="font-size: 0.8em; opacity: 0.8; margin-top: 5px;">{time_str}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    elif msg_type == 'assistant':
        # Assistant message (left side)
        bg_color = "#f1f1f1"
        if message.get('error'):
            bg_color = "#ffebee"
        elif message.get('processing'):
            bg_color = "#fff3cd"
        elif message.get('workflow_complete'):
            bg_color = "#e8f5e8"
        
        st.markdown(f"""
        <div style="display: flex; justify-content: flex-start; margin: 10px 0;">
            <div style="background-color: {bg_color}; color: #333; padding: 10px 15px; border-radius: 18px; max-width: 70%; word-wrap: break-word;">
                {content}
                <div style="font-size: 0.8em; opacity: 0.7; margin-top: 5px;">{time_str}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

# Sidebar with simple controls
with st.sidebar:
    st.header("💬 Chat Controls")
    
    if st.button("🗑️ Clear Chat"):
        st.session_state.chat_history = []
        st.session_state.workflow_steps = []
        st.rerun()
    
    if st.button("🔄 New Session"):
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.chat_history = []
        st.session_state.workflow_steps = []
        st.rerun()
    
    st.markdown("---")
    st.markdown("**Session Info:**")
    st.markdown(f"**ID:** `{st.session_state.session_id[:8]}...`")
    st.markdown(f"**Messages:** {len(st.session_state.chat_history)}")
    st.markdown(f"**Steps:** {len(st.session_state.workflow_steps)}")
    
    st.markdown("---")
    st.markdown("**Examples:**")
    st.markdown("- What are Q3 pharmacy claims costs?")
    st.markdown("- Why are medical costs higher than forecast?")
    st.markdown("- Show me utilization trends")

if __name__ == "__main__":
    main()