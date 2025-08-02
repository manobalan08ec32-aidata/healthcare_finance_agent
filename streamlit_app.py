import streamlit as st
import json
import uuid
import time
from datetime import datetime
from state_schema import AgentState
from databricks_client import DatabricksClient
from langraph_workflow import HealthcareFinanceWorkflow

# Page configuration
st.set_page_config(
    page_title="Healthcare Finance AI Agent",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if 'conversation_history' not in st.session_state:
    st.session_state.conversation_history = []
if 'workflow_results' not in st.session_state:
    st.session_state.workflow_results = []

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

# Main app
def main():
    st.title("🏥 Healthcare Finance AI Agent")
    st.markdown("*LangGraph-powered intelligent analysis of pharmacy claims, costs, and financial variances*")
    
    # Initialize workflow
    workflow = initialize_workflow()
    
    if not workflow:
        st.stop()
    
    # Sidebar for session info and controls
    with st.sidebar:
        st.header("📊 Session Info")
        st.write(f"**Session ID:** `{st.session_state.session_id[:8]}...`")
        st.write(f"**Conversations:** {len(st.session_state.conversation_history)}")
        st.write(f"**Workflow Runs:** {len(st.session_state.workflow_results)}")
        
        if st.button("🔄 Reset Session"):
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state.conversation_history = []
            st.session_state.workflow_results = []
            st.rerun()
        
        # Display recent workflow results
        if st.session_state.workflow_results:
            st.header("🔄 Recent Workflows")
            for i, result in enumerate(st.session_state.workflow_results[-3:]):
                with st.expander(f"Run {i+1}: {result.get('question', 'Unknown')[:25]}..."):
                    st.write(f"**Success:** {'✅' if result.get('success') else '❌'}")
                    st.write(f"**Final Agent:** {result.get('final_agent', 'Unknown')}")
                    st.write(f"**Duration:** {result.get('duration', 'Unknown')}")
                    if result.get('errors'):
                        st.write(f"**Errors:** {len(result['errors'])}")
    
    # Main interface
    st.header("💭 Ask Your Question")
    
    # Example questions
    with st.expander("💡 Example Questions"):
        example_cols = st.columns(3)
        
        with example_cols[0]:
            st.markdown("**Descriptive (What):**")
            st.markdown("- What are Q3 pharmacy claims costs?")
            st.markdown("- Show me member utilization by carrier")
            st.markdown("- What are top procedures by cost?")
        
        with example_cols[1]:
            st.markdown("**Analytical (Why):**")
            st.markdown("- Why are Q3 claims 18% higher?")
            st.markdown("- What caused medical cost variance?")
            st.markdown("- Why are emergency visits increasing?")
        
        with example_cols[2]:
            st.markdown("**Comparative:**")
            st.markdown("- Compare Q3 vs Q2 claims")
            st.markdown("- Show trends vs last year")
            st.markdown("- Actual vs forecast analysis")
    
    # User input
    col1, col2 = st.columns([4, 1])
    
    with col1:
        user_question = st.text_input(
            "Enter your healthcare finance question:",
            placeholder="e.g., Why are pharmacy claims costs 15% higher than forecast this quarter?",
            key="user_input"
        )
    
    with col2:
        st.write("")  # Spacer
        analyze_button = st.button("🔍 Analyze", type="primary")
    
    # Process question with LangGraph workflow
    if analyze_button and user_question:
        process_with_langraph_workflow(user_question, workflow)
    
    # Display workflow execution results
    if st.session_state.workflow_results:
        display_latest_workflow_results()

def process_with_langraph_workflow(user_question: str, workflow: HealthcareFinanceWorkflow):
    """Process user question through LangGraph workflow with streaming"""
    
    start_time = time.time()
    
    # Create containers for streaming updates
    status_container = st.container()
    progress_container = st.container()
    results_container = st.container()
    
    with status_container:
        st.subheader("🔄 LangGraph Workflow Execution")
    
    # Stream workflow execution
    try:
        workflow_steps = []
        current_step = 0
        
        with progress_container:
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Create expandable sections for each agent
            nav_expander = st.expander("🧭 Navigation Controller", expanded=False)
            router_expander = st.expander("🎯 Router Agent", expanded=False)  
            template_expander = st.expander("📋 SQL Template Agent", expanded=False)
            sql_expander = st.expander("💻 SQL Agent", expanded=False)
            variance_expander = st.expander("📊 Variance Detection", expanded=False)
            root_cause_expander = st.expander("🔍 Root Cause Analysis", expanded=False)
            followup_expander = st.expander("❓ Follow-up Generation", expanded=False)
        
        # Execute workflow with streaming
        for step_output in workflow.stream_workflow(
            user_question=user_question,
            session_id=st.session_state.session_id,
            user_id="streamlit_user"
        ):
            workflow_steps.append(step_output)
            current_step += 1
            
            # Update progress
            progress_bar.progress(min(current_step * 0.15, 1.0))
            
            # Process each step
            for node_name, node_state in step_output.items():
                status_text.text(f"Executing: {node_name}")
                
                # Update appropriate expander
                if node_name == "navigation_controller":
                    with nav_expander:
                        display_navigation_results(node_state)
                elif node_name == "router_agent":
                    with router_expander:
                        display_router_results(node_state)
                elif node_name == "sql_template_agent":
                    with template_expander:
                        display_template_results(node_state)
                elif node_name == "sql_agent":
                    with sql_expander:
                        display_sql_results(node_state)
                elif node_name == "variance_detection_agent":
                    with variance_expander:
                        display_variance_results(node_state)
                elif node_name == "root_cause_agent":
                    with root_cause_expander:
                        display_root_cause_results(node_state)
                elif node_name == "follow_up_agent":
                    with followup_expander:
                        display_followup_results(node_state)
                
                # Small delay for visual effect
                time.sleep(0.5)
        
        # Complete progress
        progress_bar.progress(1.0)
        status_text.text("✅ Workflow completed!")
        
        # Get final state
        final_state = workflow_steps[-1] if workflow_steps else {}
        final_node_name = list(final_state.keys())[0] if final_state else "unknown"
        final_node_state = final_state.get(final_node_name, {}) if final_state else {}
        
        # Store workflow result
        end_time = time.time()
        workflow_result = {
            'question': user_question,
            'success': len(final_node_state.get('errors', [])) == 0,
            'final_agent': final_node_state.get('current_agent', 'unknown'),
            'duration': f"{end_time - start_time:.1f}s",
            'steps_executed': len(workflow_steps),
            'errors': final_node_state.get('errors', []),
            'final_state': final_node_state,
            'timestamp': datetime.now().isoformat()
        }
        
        st.session_state.workflow_results.append(workflow_result)
        
        # Display final summary
        with results_container:
            display_workflow_summary(workflow_result)
        
    except Exception as e:
        st.error(f"❌ Workflow execution failed: {str(e)}")
        
        # Store failed result
        workflow_result = {
            'question': user_question,
            'success': False,
            'error': str(e),
            'duration': f"{time.time() - start_time:.1f}s",
            'timestamp': datetime.now().isoformat()
        }
        st.session_state.workflow_results.append(workflow_result)

def display_navigation_results(state: Dict):
    """Display navigation controller results"""
    
    routing_decision = state.get('routing_decision', {})
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Flow Type", routing_decision.get('flow_type', 'Unknown'))
        st.metric("Next Agent", routing_decision.get('next_agent', 'Unknown'))
    
    with col2:
        if routing_decision.get('transition_type'):
            st.metric("Transition", routing_decision['transition_type'])
        if routing_decision.get('confidence'):
            st.metric("Confidence", f"{routing_decision['confidence']:.1%}")
    
    if routing_decision.get('routing_reasoning'):
        st.write("**Reasoning:**")
        st.write(routing_decision['routing_reasoning'])

def display_router_results(state: Dict):
    """Display router agent results"""
    
    col1, col2 = st.columns(2)
    
    with col1:
        if state.get('selected_dataset'):
            st.metric("Selected Dataset", state['selected_dataset'].split('.')import streamlit as st
import json
import uuid
import time
from datetime import datetime
from state_schema import AgentState
from databricks_client import DatabricksClient
from navigation_controller import NavigationController
from router_agent import RouterAgent

# Page configuration
st.set_page_config(
    page_title="Healthcare Finance AI Agent",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if 'conversation_history' not in st.session_state:
    st.session_state.conversation_history = []
if 'current_state' not in st.session_state:
    st.session_state.current_state = None

# Initialize clients (cache for performance)
@st.cache_resource
def initialize_clients():
    """Initialize Databricks client and agents"""
    try:
        db_client = DatabricksClient()
        
        # Test connection
        if not db_client.test_connection():
            st.error("❌ Failed to connect to Databricks")
            return None, None, None
        
        nav_controller = NavigationController(db_client)
        router_agent = RouterAgent(db_client)
        
        return db_client, nav_controller, router_agent
    except Exception as e:
        st.error(f"❌ Initialization failed: {str(e)}")
        return None, None, None

# Main app
def main():
    st.title("🏥 Healthcare Finance AI Agent")
    st.markdown("*Intelligent analysis of pharmacy claims, costs, and financial variances*")
    
    # Initialize clients
    db_client, nav_controller, router_agent = initialize_clients()
    
    if not all([db_client, nav_controller, router_agent]):
        st.stop()
    
    # Sidebar for session info and controls
    with st.sidebar:
        st.header("📊 Session Info")
        st.write(f"**Session ID:** `{st.session_state.session_id[:8]}...`")
        st.write(f"**Conversations:** {len(st.session_state.conversation_history)}")
        
        if st.button("🔄 Reset Session"):
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state.conversation_history = []
            st.session_state.current_state = None
            st.rerun()
        
        # Display conversation history
        if st.session_state.conversation_history:
            st.header("💬 Recent Questions")
            for i, conv in enumerate(st.session_state.conversation_history[-5:]):
                with st.expander(f"Q{i+1}: {conv.get('question', 'Unknown')[:30]}..."):
                    st.write(f"**Type:** {conv.get('question_type', 'N/A')}")
                    st.write(f"**Dataset:** {conv.get('dataset_used', 'N/A')}")
                    st.write(f"**Status:** {'✅' if conv.get('success') else '❌'}")
    
    # Main interface
    st.header("💭 Ask Your Question")
    
    # Example questions
    with st.expander("💡 Example Questions"):
        example_cols = st.columns(2)
        
        with example_cols[0]:
            st.markdown("**What Questions:**")
            st.markdown("- What are Q3 pharmacy claims costs?")
            st.markdown("- Show me member utilization by carrier")
            st.markdown("- What are the top 10 procedures by cost?")
        
        with example_cols[1]:
            st.markdown("**Why Questions:**")
            st.markdown("- Why are Q3 claims 18% higher than forecast?")
            st.markdown("- What caused the variance in medical costs?")
            st.markdown("- Why are emergency visits increasing?")
    
    # User input
    user_question = st.text_input(
        "Enter your healthcare finance question:",
        placeholder="e.g., What are Q3 pharmacy claims costs by carrier?",
        key="user_input"
    )
    
    # Process question
    if st.button("🔍 Analyze", type="primary") and user_question:
        process_user_question(user_question, db_client, nav_controller, router_agent)
    
    # Display current analysis
    if st.session_state.current_state:
        display_analysis_results()

def process_user_question(user_question: str, db_client: DatabricksClient, 
                         nav_controller: NavigationController, router_agent: RouterAgent):
    """Process user question through the agent workflow"""
    
    with st.status("🤔 Processing your question...", expanded=True) as status:
        
        try:
            # 1. Create initial state
            st.write("📝 Creating analysis state...")
            state = AgentState(
                session_id=st.session_state.session_id,
                user_id="streamlit_user",
                user_question=user_question,
                original_question=user_question,
                conversation_history=st.session_state.conversation_history
            )
            
            # 2. Navigation Controller - Route the question
            st.write("🧭 Analyzing question and determining route...")
            routing_decision = nav_controller.route_user_query(state)
            
            # Update state with routing info
            state.update({
                'current_agent': routing_decision['next_agent'],
                'flow_type': routing_decision['flow_type'],
                'transition_type': routing_decision.get('transition_type'),
                'comparison_intent': routing_decision.get('comparison_intent')
            })
            
            # Display routing decision
            with st.expander("🧭 Navigation Analysis"):
                st.json(routing_decision)
            
            # 3. Execute Router Agent (if that's the next agent)
            if routing_decision['next_agent'] == 'router_agent':
                st.write("🎯 Selecting optimal dataset...")
                
                selection_result = router_agent.select_dataset(state)
                
                # Update state with selection
                state.update({
                    'selected_dataset': selection_result['selected_dataset'],
                    'dataset_metadata': selection_result['dataset_metadata'],
                    'selection_reasoning': selection_result['selection_reasoning'],
                    'available_datasets': selection_result['available_datasets']
                })
                
                # Display dataset selection
                with st.expander("🎯 Dataset Selection"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric(
                            "Selected Dataset", 
                            selection_result['selected_dataset'],
                            f"Confidence: {selection_result['confidence']:.2%}"
                        )
                    
                    with col2:
                        st.write("**Reasoning:**")
                        st.write(selection_result['selection_reasoning'])
                    
                    # Show available datasets
                    st.write("**Available Datasets:**")
                    datasets_df = []
                    for ds in selection_result['available_datasets'][:3]:
                        datasets_df.append({
                            'Table': ds.get('table_name'),
                            'Score': ds.get('relevance_score', 0),
                            'Description': ds.get('content', '')[:100] + '...'
                        })
                    st.dataframe(datasets_df, use_container_width=True)
            
            # 4. Save state and update session
            st.session_state.current_state = state
            
            # Add to conversation history
            conversation_entry = {
                'timestamp': datetime.now().isoformat(),
                'question': user_question,
                'question_type': routing_decision['flow_type'],
                'dataset_used': state.get('selected_dataset'),
                'routing_decision': routing_decision['next_agent'],
                'success': True
            }
            st.session_state.conversation_history.append(conversation_entry)
            
            # 5. Determine next steps
            next_steps = determine_next_steps(state, routing_decision)
            st.write(f"✅ Analysis complete. Next: {next_steps}")
            
            status.update(label="✅ Analysis Complete!", state="complete")
            
        except Exception as e:
            st.error(f"❌ Error processing question: {str(e)}")
            status.update(label="❌ Analysis Failed", state="error")

def determine_next_steps(state: AgentState, routing_decision: Dict) -> str:
    """Determine what happens next in the workflow"""
    
    current_agent = routing_decision['next_agent']
    flow_type = routing_decision['flow_type']
    
    if current_agent == 'router_agent' and state.get('selected_dataset'):
        return "SQL Template Agent → SQL Generation"
    elif current_agent == 'variance_detection_agent':
        return "Variance Analysis → Potential Root Cause"
    elif flow_type == 'why':
        return "Root Cause Analysis"
    else:
        return "SQL Generation & Execution"

def display_analysis_results():
    """Display the results of the current analysis"""
    
    state = st.session_state.current_state
    
    st.header("📊 Analysis Results")
    
    # Display state information
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Question Type", state.get('flow_type', 'Unknown'))
    
    with col2:
        st.metric("Current Agent", state.get('current_agent', 'Unknown'))
    
    with col3:
        dataset = state.get('selected_dataset', 'None')
        st.metric("Selected Dataset", dataset.split('.')[-1] if dataset else 'None')
    
    # Dataset Details
    if state.get('dataset_metadata'):
        with st.expander("📋 Dataset Details", expanded=True):
            metadata = state['dataset_metadata']
            
            st.write(f"**Table:** `{metadata.get('table_name')}`")
            st.write(f"**Relevance Score:** {metadata.get('relevance_score', 0)}")
            
            st.write("**Description:**")
            st.write(metadata.get('description', 'No description available'))
            
            # Column information
            columns = metadata.get('columns', {}).get('columns', [])
            if columns:
                st.write("**Key Columns:**")
                col_data = []
                for col in columns[:8]:  # Show first 8 columns
                    col_data.append({
                        'Column': col.get('column_name', 'Unknown'),
                        'Type': col.get('data_type', 'Unknown'),
                        'Description': col.get('description', 'No description')[:60] + '...'
                    })
                st.dataframe(col_data, use_container_width=True)
    
    # Selection Reasoning
    if state.get('selection_reasoning'):
        with st.expander("🤔 Selection Reasoning"):
            st.write(state['selection_reasoning'])
    
    # Next Steps
    st.subheader("🚀 Next Steps")
    
    if state.get('flow_type') == 'what':
        st.info("**Next:** SQL generation and execution to answer your 'what' question.")
        if st.button("▶️ Generate SQL Query"):
            st.success("SQL Agent would be executed next (not implemented in Phase 1)")
    
    elif state.get('flow_type') == 'why':
        st.info("**Next:** Variance detection and potential root cause analysis.")
        if st.button("▶️ Analyze Variance"):
            st.success("Variance Detection Agent would be executed next (not implemented in Phase 1)")
    
    else:
        st.info("**Next:** Continue with appropriate analysis based on question type.")

# Footer
def display_footer():
    st.markdown("---")
    st.markdown(
        """
        **Healthcare Finance AI Agent** - Phase 1 Implementation  
        *Navigation Controller + Router Agent*  
        🔧 *Next: SQL Template Agent, SQL Agent, Variance Detection*
        """
    )

if __name__ == "__main__":
    main()
    display_footer()