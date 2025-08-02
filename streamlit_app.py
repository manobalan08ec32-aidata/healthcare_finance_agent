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
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for chat
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'workflow_results' not in st.session_state:
    st.session_state.workflow_results = []
if 'current_workflow' not in st.session_state:
    st.session_state.current_workflow = None

# Initialize workflow (cache for performance)
@st.cache_resource
def initialize_workflow():
    """Initialize Databricks client and LangGraph workflow"""
    try:
        db_client = DatabricksClient()
        
        # Test connection
        if not db_client.test_connection():
            st.error("❌ Failed to connect to Databricks")
            return None
        
        workflow = HealthcareFinanceWorkflow(db_client)
        return workflow
    except Exception as e:
        st.error(f"❌ Workflow initialization failed: {str(e)}")
        return None

def main():
    st.title("🏥 Healthcare Finance AI Agent")
    st.markdown("*Chat with your intelligent healthcare finance assistant*")
    
    # Initialize workflow
    workflow = initialize_workflow()
    if not workflow:
        st.stop()
    
    # Create main layout
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Chat interface
        display_chat_interface(workflow)
    
    with col2:
        # Session info and controls
        display_sidebar_info()

def display_chat_interface(workflow):
    """Display the main chat interface"""
    
    # Chat container
    chat_container = st.container()
    
    with chat_container:
        st.subheader("💬 Conversation")
        
        # Display chat history
        if st.session_state.chat_history:
            for i, message in enumerate(st.session_state.chat_history):
                display_chat_message(message, i)
        else:
            # Welcome message
            st.info("👋 Welcome! Ask me about healthcare finance data, claims analysis, or financial variances.")
    
    # Input area (always at bottom)
    st.markdown("---")
    display_chat_input(workflow)

def display_chat_message(message, index):
    """Display a single chat message"""
    
    message_type = message.get('type')
    content = message.get('content')
    timestamp = message.get('timestamp')
    metadata = message.get('metadata', {})
    
    # Format timestamp
    try:
        dt = datetime.fromisoformat(timestamp)
        time_str = dt.strftime("%H:%M:%S")
    except:
        time_str = "Unknown"
    
    if message_type == 'user':
        # User message (right aligned)
        st.markdown(f"""
        <div style="text-align: right; margin: 10px 0;">
            <div style="background-color: #e3f2fd; padding: 10px; border-radius: 10px; display: inline-block; max-width: 80%;">
                <strong>You</strong> <small>({time_str})</small><br>
                {content}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    elif message_type == 'agent':
        # Agent message (left aligned)
        status = metadata.get('status', 'completed')
        dataset = metadata.get('selected_dataset')
        confidence = metadata.get('confidence', 0)
        
        # Status icon
        if status == 'completed':
            status_icon = "✅"
        elif status == 'processing':
            status_icon = "⏳"
        elif status == 'failed':
            status_icon = "❌"
        else:
            status_icon = "🤖"
        
        st.markdown(f"""
        <div style="text-align: left; margin: 10px 0;">
            <div style="background-color: #f5f5f5; padding: 10px; border-radius: 10px; display: inline-block; max-width: 80%;">
                <strong>{status_icon} AI Agent</strong> <small>({time_str})</small><br>
                {content}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Show additional metadata if available
        if dataset or confidence > 0:
            with st.expander(f"📊 Analysis Details - Message {index + 1}"):
                if dataset:
                    st.write(f"**Selected Dataset:** {dataset}")
                if confidence > 0:
                    st.write(f"**Confidence:** {confidence:.1%}")
                if metadata.get('reasoning'):
                    st.write(f"**Reasoning:** {metadata['reasoning']}")
    
    elif message_type == 'clarification':
        # Clarification request (special UI)
        display_clarification_message(message, index)

def display_clarification_message(message, index):
    """Display a clarification request with interactive options"""
    
    content = message.get('content')
    options = message.get('metadata', {}).get('options', [])
    clarification_id = f"clarification_{index}"
    
    st.markdown(f"""
    <div style="text-align: left; margin: 10px 0;">
        <div style="background-color: #fff3cd; padding: 10px; border-radius: 10px; border-left: 4px solid #ffc107;">
            <strong>🤔 AI Agent</strong><br>
            {content}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Display options if still pending
    if not message.get('resolved', False) and options:
        st.write("**Please select an option:**")
        
        for i, option in enumerate(options):
            option_key = f"{clarification_id}_option_{i}"
            
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.write(f"**{option.get('display_name', f'Option {i+1}')}**")
                st.write(option.get('use_case', 'No description'))
            
            with col2:
                if st.button(f"Select", key=option_key):
                    handle_clarification_response(option, index)

def display_chat_input(workflow):
    """Display chat input area"""
    
    # Check if waiting for clarification
    waiting_for_clarification = any(
        msg.get('type') == 'clarification' and not msg.get('resolved', False)
        for msg in st.session_state.chat_history
    )
    
    if waiting_for_clarification:
        st.warning("⏳ Please respond to the clarification request above before asking a new question.")
        return
    
    # Chat input
    col1, col2 = st.columns([4, 1])
    
    with col1:
        user_input = st.text_input(
            "Type your question...",
            placeholder="e.g., Why are Q3 pharmacy claims 15% higher than forecast?",
            key="chat_input"
        )
    
    with col2:
        send_button = st.button("Send 📤", type="primary")
    
    # Process user input
    if send_button and user_input:
        process_user_message(user_input, workflow)
        st.rerun()

def process_user_message(user_message: str, workflow):
    """Process user message and execute workflow"""
    
    # Add user message to chat
    user_chat_message = {
        'type': 'user',
        'content': user_message,
        'timestamp': datetime.now().isoformat(),
        'metadata': {}
    }
    st.session_state.chat_history.append(user_chat_message)
    
    # Add processing message
    processing_message = {
        'type': 'agent',
        'content': "🔄 Analyzing your question...",
        'timestamp': datetime.now().isoformat(),
        'metadata': {'status': 'processing'}
    }
    st.session_state.chat_history.append(processing_message)
    
    try:
        # Execute workflow
        result = workflow.run_workflow(
            user_question=user_message,
            session_id=st.session_state.session_id,
            user_id="streamlit_user"
        )
        
        if result['success']:
            final_state = result['final_state']
            
            # Check if clarification is needed
            if final_state.get('requires_user_input'):
                # Replace processing message with clarification
                st.session_state.chat_history.pop()  # Remove processing message
                
                clarification_data = final_state.get('clarification_data', {})
                clarification_message = {
                    'type': 'clarification',
                    'content': clarification_data.get('question', 'Which option would you prefer?'),
                    'timestamp': datetime.now().isoformat(),
                    'metadata': {
                        'options': clarification_data.get('options', []),
                        'status': 'pending'
                    },
                    'resolved': False
                }
                st.session_state.chat_history.append(clarification_message)
            else:
                # Replace processing message with result
                st.session_state.chat_history.pop()  # Remove processing message
                
                # Generate response content
                response_content = generate_response_content(final_state)
                
                result_message = {
                    'type': 'agent',
                    'content': response_content,
                    'timestamp': datetime.now().isoformat(),
                    'metadata': {
                        'status': 'completed',
                        'selected_dataset': final_state.get('selected_dataset'),
                        'confidence': final_state.get('selection_confidence', 0),
                        'reasoning': final_state.get('selection_reasoning'),
                        'duration': result.get('total_duration', 0)
                    }
                }
                st.session_state.chat_history.append(result_message)
        else:
            # Replace processing message with error
            st.session_state.chat_history.pop()  # Remove processing message
            
            error_message = {
                'type': 'agent',
                'content': f"❌ Sorry, I encountered an error: {result.get('error', 'Unknown error')}",
                'timestamp': datetime.now().isoformat(),
                'metadata': {'status': 'failed'}
            }
            st.session_state.chat_history.append(error_message)
    
    except Exception as e:
        # Replace processing message with error
        st.session_state.chat_history.pop()  # Remove processing message
        
        error_message = {
            'type': 'agent',
            'content': f"❌ Unexpected error: {str(e)}",
            'timestamp': datetime.now().isoformat(),
            'metadata': {'status': 'failed'}
        }
        st.session_state.chat_history.append(error_message)

def handle_clarification_response(selected_option: dict, message_index: int):
    """Handle user's clarification response"""
    
    # Mark clarification as resolved
    st.session_state.chat_history[message_index]['resolved'] = True
    
    # Add user's selection as a message
    selection_message = {
        'type': 'user',
        'content': f"I selected: {selected_option.get('display_name')}",
        'timestamp': datetime.now().isoformat(),
        'metadata': {'selection': selected_option}
    }
    st.session_state.chat_history.append(selection_message)
    
    # Add processing message
    processing_message = {
        'type': 'agent',
        'content': "✅ Thank you! Processing your selection...",
        'timestamp': datetime.now().isoformat(),
        'metadata': {'status': 'processing'}
    }
    st.session_state.chat_history.append(processing_message)
    
    # TODO: Continue workflow with user's clarification
    # This would need integration with the workflow's clarification handling

def generate_response_content(final_state: dict) -> str:
    """Generate chat response content from workflow results"""
    
    selected_dataset = final_state.get('selected_dataset')
    confidence = final_state.get('selection_confidence', 0)
    phase1_summary = final_state.get('phase1_summary', {})
    
    if selected_dataset:
        dataset_name = selected_dataset.split('.')[-1]
        
        response = f"✅ **Analysis Complete!**\n\n"
        response += f"I've analyzed your question and selected the **{dataset_name}** dataset "
        response += f"with {confidence:.1%} confidence.\n\n"
        
        if final_state.get('user_clarified'):
            response += "Thank you for the clarification! "
        
        flow_type = phase1_summary.get('flow_type')
        if flow_type == 'descriptive':
            response += "This appears to be a 'what' question about data analysis."
        elif flow_type == 'analytical':
            response += "This appears to be a 'why' question requiring variance analysis."
        
        recommendation = phase1_summary.get('recommendation')
        if recommendation:
            response += f"\n\n**Next Steps:** {recommendation}"
        
        return response
    else:
        return "❌ I wasn't able to find a suitable dataset for your question. Could you try rephrasing it?"

def display_sidebar_info():
    """Display session info and controls in sidebar"""
    
    st.subheader("📊 Session Info")
    st.write(f"**Session:** `{st.session_state.session_id[:8]}...`")
    st.write(f"**Messages:** {len(st.session_state.chat_history)}")
    
    if st.button("🗑️ Clear Chat"):
        st.session_state.chat_history = []
        st.rerun()
    
    if st.button("🔄 New Session"):
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.chat_history = []
        st.session_state.workflow_results = []
        st.rerun()
    
    # Example questions
    st.subheader("💡 Example Questions")
    example_questions = [
        "What are Q3 pharmacy claims costs?",
        "Why are medical costs 18% higher?",
        "Show me utilization by carrier",
        "Compare Q3 vs Q2 emergency claims"
    ]
    
    for question in example_questions:
        if st.button(question, key=f"example_{hash(question)}"):
            st.session_state.chat_input = question
            st.rerun()
    
    # Chat history export
    if st.session_state.chat_history:
        st.subheader("📁 Export")
        
        chat_data = {
            'session_id': st.session_state.session_id,
            'timestamp': datetime.now().isoformat(),
            'messages': st.session_state.chat_history
        }
        
        st.download_button(
            label="📥 Download Chat History",
            data=json.dumps(chat_data, indent=2),
            file_name=f"chat_history_{st.session_state.session_id[:8]}.json",
            mime="application/json"
        )

if __name__ == "__main__":
    main()