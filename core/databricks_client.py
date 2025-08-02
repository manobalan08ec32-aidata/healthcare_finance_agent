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
    
    # Display chat interface
    display_simple_chat(workflow)

def display_simple_chat(workflow):
    """Simple chat interface"""
    
    # Chat container with fixed height
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
        
        # Process input immediately when form is submitted
        if submit_button and user_input and not st.session_state.processing:
            # Add user message immediately
            add_user_message(user_input)
            
            # Process with workflow
            process_message(user_input, workflow)
            
            # Rerun to show updated chat
            st.rerun()

def add_user_message(message):
    """Add user message to chat immediately"""
    user_message = {
        'type': 'user',
        'content': message,
        'timestamp': datetime.now()
    }
    st.session_state.chat_history.append(user_message)

def process_message(user_input, workflow):
    """Process user message with workflow"""
    
    # Set processing state
    st.session_state.processing = True
    
    # Add "thinking" message
    thinking_message = {
        'type': 'assistant',
        'content': '🤔 Analyzing your question...',
        'timestamp': datetime.now(),
        'processing': True
    }
    st.session_state.chat_history.append(thinking_message)
    
    try:
        # Execute workflow
        result = workflow.run_workflow(
            user_question=user_input,
            session_id=st.session_state.session_id,
            user_id="streamlit_user"
        )
        
        # Remove thinking message
        st.session_state.chat_history.pop()
        
        if result['success']:
            final_state = result['final_state']
            
            # Check for clarification
            if final_state.get('requires_user_input'):
                handle_clarification_needed(final_state)
            else:
                # Add success response
                add_success_response(final_state, result)
        else:
            # Add error response
            add_error_response(result.get('error', 'Unknown error'))
    
    except Exception as e:
        # Remove thinking message
        if st.session_state.chat_history and st.session_state.chat_history[-1].get('processing'):
            st.session_state.chat_history.pop()
        
        # Add error response
        add_error_response(str(e))
    
    finally:
        # Reset processing state
        st.session_state.processing = False

def handle_clarification_needed(final_state):
    """Handle clarification requests"""
    clarification_data = final_state.get('clarification_data', {})
    options = clarification_data.get('options', [])
    
    clarification_msg = {
        'type': 'clarification',
        'content': clarification_data.get('question', 'Which dataset would you prefer?'),
        'options': options,
        'timestamp': datetime.now()
    }
    st.session_state.chat_history.append(clarification_msg)

def add_success_response(final_state, result):
    """Add successful response to chat"""
    
    # Generate response content
    selected_dataset = final_state.get('selected_dataset', 'Unknown')
    confidence = final_state.get('selection_confidence', 0)
    
    if selected_dataset != 'Unknown':
        dataset_name = selected_dataset.split('.')[-1]
        content = f"✅ **Analysis Complete**\n\n"
        content += f"Selected dataset: **{dataset_name}**\n"
        content += f"Confidence: **{confidence:.1%}**\n\n"
        
        phase1_summary = final_state.get('phase1_summary', {})
        if phase1_summary.get('recommendation'):
            content += f"**Next:** {phase1_summary['recommendation']}"
    else:
        content = "❌ I couldn't find a suitable dataset for your question. Try rephrasing it."
    
    response_message = {
        'type': 'assistant',
        'content': content,
        'timestamp': datetime.now(),
        'metadata': {
            'dataset': selected_dataset,
            'confidence': confidence,
            'duration': result.get('total_duration', 0)
        }
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
        
        st.markdown(f"""
        <div style="display: flex; justify-content: flex-start; margin: 10px 0;">
            <div style="background-color: {bg_color}; color: #333; padding: 10px 15px; border-radius: 18px; max-width: 70%; word-wrap: break-word;">
                {content}
                <div style="font-size: 0.8em; opacity: 0.7; margin-top: 5px;">{time_str}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Show metadata if available
        if message.get('metadata'):
            metadata = message['metadata']
            with st.expander("📊 Analysis Details", expanded=False):
                if metadata.get('dataset'):
                    st.write(f"**Dataset:** {metadata['dataset']}")
                if metadata.get('confidence'):
                    st.write(f"**Confidence:** {metadata['confidence']:.1%}")
                if metadata.get('duration'):
                    st.write(f"**Duration:** {metadata['duration']:.1f}s")
    
    elif msg_type == 'clarification':
        # Clarification request
        st.markdown(f"""
        <div style="display: flex; justify-content: flex-start; margin: 10px 0;">
            <div style="background-color: #fff3cd; color: #333; padding: 10px 15px; border-radius: 18px; max-width: 70%; border-left: 4px solid #ffc107;">
                🤔 {content}
                <div style="font-size: 0.8em; opacity: 0.7; margin-top: 5px;">{time_str}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Show options
        options = message.get('options', [])
        if options:
            st.write("**Please choose:**")
            cols = st.columns(len(options))
            
            for i, option in enumerate(options):
                with cols[i]:
                    if st.button(
                        option.get('display_name', f'Option {i+1}'),
                        key=f"option_{message['timestamp']}_{i}"
                    ):
                        handle_option_selected(option)

def handle_option_selected(option):
    """Handle when user selects a clarification option"""
    
    # Add user selection message
    selection_message = {
        'type': 'user',
        'content': f"Selected: {option.get('display_name')}",
        'timestamp': datetime.now()
    }
    st.session_state.chat_history.append(selection_message)
    
    # Add completion message
    completion_message = {
        'type': 'assistant',
        'content': f"✅ Thank you! Proceeding with {option.get('display_name')}...",
        'timestamp': datetime.now()
    }
    st.session_state.chat_history.append(completion_message)
    
    # TODO: Continue workflow with selection
    st.rerun()

# Sidebar with simple controls
with st.sidebar:
    st.header("💬 Chat")
    
    if st.button("🗑️ Clear Chat"):
        st.session_state.chat_history = []
        st.rerun()
    
    st.markdown("---")
    st.markdown("**Examples:**")
    st.markdown("- What is claim revenue for July?")
    st.markdown("- Why are costs higher than forecast?")
    st.markdown("- Show me utilization trends")

if __name__ == "__main__":
    main()