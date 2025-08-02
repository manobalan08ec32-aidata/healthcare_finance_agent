import streamlit as st
import uuid
from datetime import datetime
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from workflows.langraph_workflow import HealthcareFinanceWorkflow

# Page configuration
st.set_page_config(
    page_title="Healthcare Finance Assistant",
    page_icon="🏥",
    layout="wide"
)

# Initialize session state
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'processing' not in st.session_state:
    st.session_state.processing = False

# Initialize workflow
@st.cache_resource
def initialize_workflow():
    try:
        db_client = DatabricksClient()
        workflow = HealthcareFinanceWorkflow(db_client)
        return workflow
    except Exception as e:
        st.error(f"Failed to initialize: {str(e)}")
        return None

def main():
    workflow = initialize_workflow()
    if not workflow:
        st.stop()
    
    # Display chat messages
    display_messages()
    
    # Chat input at bottom
    display_chat_input(workflow)

def display_messages():
    """Display all chat messages"""
    
    for message in st.session_state.messages:
        if message['type'] == 'user':
            # User message (left side, blue)
            st.markdown(f"""
            <div style="margin: 20px 0;">
                <div style="background-color: #007bff; color: white; padding: 15px; border-radius: 10px; max-width: 80%; display: inline-block;">
                    {message['content']}
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        elif message['type'] == 'assistant':
            # Assistant message (left side, plain)
            st.markdown(f"""
            <div style="margin: 20px 0;">
                <div style="background-color: #f0f0f0; color: black; padding: 15px; border-radius: 10px; max-width: 80%; display: inline-block;">
                    {message['content']}
                </div>
            </div>
            """, unsafe_allow_html=True)

def display_chat_input(workflow):
    """Chat input at bottom"""
    
    # Create input container at bottom
    with st.container():
        st.markdown("---")
        
        # Input form
        with st.form(key="chat_form", clear_on_submit=True):
            col1, col2 = st.columns([6, 1])
            
            with col1:
                user_input = st.text_input(
                    "",
                    placeholder="Ask your question...",
                    disabled=st.session_state.processing
                )
            
            with col2:
                submit = st.form_submit_button(
                    "Send",
                    disabled=st.session_state.processing,
                    type="primary"
                )
            
            if submit and user_input and not st.session_state.processing:
                process_message(user_input, workflow)

def process_message(user_input, workflow):
    """Process user message with workflow streaming"""
    
    # Add user message
    st.session_state.messages.append({
        'type': 'user',
        'content': user_input,
        'timestamp': datetime.now()
    })
    
    # Set processing state
    st.session_state.processing = True
    
    try:
        # Stream workflow execution
        for step_data in workflow.stream_workflow(
            user_question=user_input,
            session_id=st.session_state.session_id,
            user_id="streamlit_user"
        ):
            # Add step output as assistant message
            step_content = format_step_output(step_data)
            if step_content:
                st.session_state.messages.append({
                    'type': 'assistant',
                    'content': step_content,
                    'timestamp': datetime.now()
                })
                
                # Rerun to show new message
                st.rerun()
        
    except Exception as e:
        # Add error message
        st.session_state.messages.append({
            'type': 'assistant',
            'content': f"Error: {str(e)}",
            'timestamp': datetime.now()
        })
    
    finally:
        # Reset processing state
        st.session_state.processing = False
        st.rerun()

def format_step_output(step_data):
    """Format workflow step output for display"""
    
    step = step_data.get('step', {})
    
    # Extract node information
    for node_name, node_state in step.items():
        if node_name == '__start__':
            continue
        
        # Just show the node name and its state
        output = f"**{node_name}:**\n{str(node_state)}"
        return output
    
    return None

if __name__ == "__main__":
    main()