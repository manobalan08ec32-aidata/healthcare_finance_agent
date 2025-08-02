import streamlit as st
import json
import uuid
from datetime import datetime
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from workflows.langraph_workflow import HealthcareFinanceWorkflow

# Page configuration
st.set_page_config(
    page_title="Navigation Controller Test",
    page_icon="🧭",
    layout="wide"
)

# Initialize session state
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if 'questions_history' not in st.session_state:
    st.session_state.questions_history = []
if 'navigation_results' not in st.session_state:
    st.session_state.navigation_results = []

# Initialize workflow
@st.cache_resource
def initialize_workflow():
    """Initialize Healthcare Finance Workflow"""
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
    st.title("🧭 Navigation Controller Testing")
    st.markdown("Testing question rewriting and classification")
    
    # Initialize workflow
    workflow = initialize_workflow()
    if not workflow:
        st.stop()
    
    # Two columns - input and results
    col1, col2 = st.columns([1, 1])
    
    with col1:
        display_input_section(workflow)
    
    with col2:
        display_results_section()

def display_input_section(workflow):
    """Input section for testing"""
    
    st.subheader("📝 Input")
    
    # Question input
    with st.form(key="nav_test_form", clear_on_submit=True):
        user_input = st.text_area(
            "Enter Question:",
            placeholder="e.g., What are Q3 pharmacy claims costs?",
            height=100
        )
        
        submit_button = st.form_submit_button("🧭 Test Navigation", type="primary")
        
        if submit_button and user_input:
            # Just show the input was received
            st.success(f"✅ Question received: {user_input}")
            st.session_state.questions_history.append(user_input)
    
    # Current history
    st.subheader("📚 Questions History")
    if st.session_state.questions_history:
        for i, question in enumerate(st.session_state.questions_history, 1):
            st.write(f"{i}. {question}")
    else:
        st.write("No questions yet")
    
    # Clear button
    if st.button("🗑️ Clear History"):
        st.session_state.questions_history = []
        st.session_state.navigation_results = []
        st.rerun()

def display_results_section():
    """Results section showing navigation output"""
    
    st.subheader("🎯 Navigation Results")
    
    if st.session_state.navigation_results:
        for i, result in enumerate(st.session_state.navigation_results, 1):
            with st.expander(f"Result {i}: {result['original_question'][:50]}...", expanded=(i == len(st.session_state.navigation_results))):
                
                # Original vs Rewritten
                st.markdown("**Original Question:**")
                st.code(result['original_question'])
                
                st.markdown("**Rewritten Question:**")
                st.code(result['rewritten_question'])
                
                # Classification and routing
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Question Type", result['question_type'])
                with col2:
                    st.metric("Next Agent", result['next_agent'])
                
                # Show if question was rewritten
                was_rewritten = result['original_question'] != result['rewritten_question']
                if was_rewritten:
                    st.success("✅ Question was rewritten")
                else:
                    st.info("ℹ️ Question unchanged")
                
                # Timestamp
                st.caption(f"Processed at: {result['timestamp']}")
    else:
        st.write("No navigation results yet. Enter a question to test!")

# Sidebar with info
with st.sidebar:
    st.header("🧭 Navigation Testing")
    
    st.markdown("**Session Info:**")
    st.write(f"ID: `{st.session_state.session_id[:8]}...`")
    st.write(f"Questions: {len(st.session_state.questions_history)}")
    st.write(f"Results: {len(st.session_state.navigation_results)}")
    
    st.markdown("---")
    st.markdown("**Test Examples:**")
    st.markdown("1. What are Q3 pharmacy claims costs?")
    st.markdown("2. Why are they so high?")
    st.markdown("3. Show me the breakdown")
    st.markdown("4. What is driving the increase?")
    
    st.markdown("---")
    st.markdown("**Expected Behavior:**")
    st.markdown("- **'What' questions** → router_agent")
    st.markdown("- **'Why' questions** → root_cause_agent")
    st.markdown("- **Follow-ups** get rewritten")

if __name__ == "__main__":
    main()