from typing import Dict, Any
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from state_schema import AgentState
from databricks_client import DatabricksClient
from llm_navigation_controller import LLMNavigationController
from llm_router_agent import LLMRouterAgent

class HealthcareFinanceWorkflow:
    """LangGraph workflow orchestration for healthcare finance agents"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
        # Initialize agents
        self.nav_controller = LLMNavigationController(databricks_client)
        self.router_agent = LLMRouterAgent(databricks_client)
        
        # Build the graph
        self.workflow = self._build_workflow()
        
        # Compile with memory
        memory = MemorySaver()
        self.app = self.workflow.compile(checkpointer=memory)
    
    def _build_workflow(self) -> StateGraph:
        """Build the LangGraph workflow with nodes and edges"""
        
        # Create the graph
        workflow = StateGraph(AgentState)
        
        # Add nodes (agents)
        workflow.add_node("navigation_controller", self._navigation_controller_node)
        workflow.add_node("router_agent", self._router_agent_node)
        workflow.add_node("sql_template_agent", self._sql_template_agent_node)
        workflow.add_node("sql_agent", self._sql_agent_node)
        workflow.add_node("variance_detection_agent", self._variance_detection_agent_node)
        workflow.add_node("root_cause_agent", self._root_cause_agent_node)
        workflow.add_node("follow_up_agent", self._follow_up_agent_node)
        workflow.add_node("memory_management", self._memory_management_node)
        
        # Set entry point
        workflow.set_entry_point("navigation_controller")
        
        # Add conditional edges based on routing decisions
        workflow.add_conditional_edges(
            "navigation_controller",
            self._route_from_navigation,
            {
                "router_agent": "router_agent",
                "sql_template_agent": "sql_template_agent", 
                "sql_agent": "sql_agent",
                "variance_detection_agent": "variance_detection_agent",
                "memory_management": "memory_management"
            }
        )
        
        # Router agent routes to SQL template agent
        workflow.add_edge("router_agent", "sql_template_agent")
        
        # SQL template agent routes to SQL agent
        workflow.add_edge("sql_template_agent", "sql_agent")
        
        # SQL agent can route to variance detection or follow-up
        workflow.add_conditional_edges(
            "sql_agent",
            self._route_from_sql_agent,
            {
                "variance_detection_agent": "variance_detection_agent",
                "follow_up_agent": "follow_up_agent",
                "END": END
            }
        )
        
        # Variance detection routes to root cause or follow-up
        workflow.add_conditional_edges(
            "variance_detection_agent", 
            self._route_from_variance_detection,
            {
                "root_cause_agent": "root_cause_agent",
                "follow_up_agent": "follow_up_agent",
                "END": END
            }
        )
        
        # Root cause agent routes to follow-up
        workflow.add_edge("root_cause_agent", "follow_up_agent")
        
        # Follow-up agent ends or waits for user input
        workflow.add_conditional_edges(
            "follow_up_agent",
            self._route_from_follow_up,
            {
                "END": END,
                "navigation_controller": "navigation_controller"  # User selects follow-up
            }
        )
        
        # Memory management can end or route back
        workflow.add_conditional_edges(
            "memory_management",
            self._route_from_memory_management,
            {
                "router_agent": "router_agent",
                "END": END
            }
        )
        
        return workflow
    
    # ============ NODE IMPLEMENTATIONS ============
    
    def _navigation_controller_node(self, state: AgentState) -> AgentState:
        """Navigation Controller Node - Routes user queries"""
        
        print(f"🧭 Navigation Controller: Analyzing '{state['user_question']}'")
        
        try:
            routing_decision = self.nav_controller.route_user_query(state)
            
            # Update state with routing information
            state.update({
                'current_agent': 'navigation_controller',
                'previous_agent': state.get('current_agent'),
                'flow_type': routing_decision['flow_type'],
                'transition_type': routing_decision.get('transition_type'),
                'comparison_intent': routing_decision.get('comparison_intent'),
                'routing_decision': routing_decision,
                'errors': []
            })
            
            print(f"  → Routing to: {routing_decision['next_agent']}")
            print(f"  → Flow type: {routing_decision['flow_type']}")
            
            return state
            
        except Exception as e:
            state['errors'].append({
                'agent': 'navigation_controller',
                'error': str(e),
                'timestamp': self._get_timestamp()
            })
            state['last_error'] = str(e)
            return state
    
    def _router_agent_node(self, state: AgentState) -> AgentState:
        """Router Agent Node - Dataset selection"""
        
        print(f"🎯 Router Agent: Selecting dataset for analysis")
        
        try:
            selection_result = self.router_agent.select_dataset(state)
            
            # Update state with dataset selection
            state.update({
                'current_agent': 'router_agent',
                'previous_agent': state.get('current_agent'),
                'selected_dataset': selection_result['selected_dataset'],
                'dataset_metadata': selection_result['dataset_metadata'],
                'selection_reasoning': selection_result['expert_reasoning'],
                'available_datasets': selection_result.get('available_datasets', []),
                'selection_confidence': selection_result.get('final_confidence', 0.8)
            })
            
            print(f"  → Selected: {selection_result['selected_dataset']}")
            print(f"  → Confidence: {selection_result.get('final_confidence', 0.8):.1%}")
            
            return state
            
        except Exception as e:
            state['errors'].append({
                'agent': 'router_agent', 
                'error': str(e),
                'timestamp': self._get_timestamp()
            })
            state['last_error'] = str(e)
            return state
    
    def _sql_template_agent_node(self, state: AgentState) -> AgentState:
        """SQL Template Agent Node - Find existing SQL patterns"""
        
        print(f"📋 SQL Template Agent: Finding SQL patterns")
        
        try:
            # TODO: Implement SQL template search
            # For now, placeholder implementation
            state.update({
                'current_agent': 'sql_template_agent',
                'previous_agent': state.get('current_agent'),
                'sql_templates': [],  # Will be populated when implemented
                'selected_template': None
            })
            
            print(f"  → Templates found: 0 (placeholder - to be implemented)")
            
            return state
            
        except Exception as e:
            state['errors'].append({
                'agent': 'sql_template_agent',
                'error': str(e), 
                'timestamp': self._get_timestamp()
            })
            return state
    
    def _sql_agent_node(self, state: AgentState) -> AgentState:
        """SQL Agent Node - Generate and execute SQL"""
        
        print(f"💻 SQL Agent: Generating and executing SQL")
        
        try:
            # TODO: Implement SQL generation and execution
            # For now, placeholder implementation
            state.update({
                'current_agent': 'sql_agent',
                'previous_agent': state.get('current_agent'),
                'generated_sql': "-- Placeholder SQL to be generated",
                'execution_status': 'pending',
                'query_results': None
            })
            
            print(f"  → SQL generated (placeholder)")
            print(f"  → Execution status: pending")
            
            return state
            
        except Exception as e:
            state['errors'].append({
                'agent': 'sql_agent',
                'error': str(e),
                'timestamp': self._get_timestamp()
            })
            state['execution_status'] = 'failed'
            return state
    
    def _variance_detection_agent_node(self, state: AgentState) -> AgentState:
        """Variance Detection Agent Node - Analyze for significant variances"""
        
        print(f"📊 Variance Detection Agent: Analyzing results")
        
        try:
            # TODO: Implement variance detection logic
            # For now, placeholder implementation
            state.update({
                'current_agent': 'variance_detection_agent',
                'previous_agent': state.get('current_agent'),
                'variance_detected': False,  # Will be calculated when implemented
                'variance_percentage': None,
                'threshold_exceeded': False,
                'requires_root_cause': False
            })
            
            print(f"  → Variance detected: {state['variance_detected']}")
            
            return state
            
        except Exception as e:
            state['errors'].append({
                'agent': 'variance_detection_agent',
                'error': str(e),
                'timestamp': self._get_timestamp()
            })
            return state
    
    def _root_cause_agent_node(self, state: AgentState) -> AgentState:
        """Root Cause Agent Node - Investigate variance causes"""
        
        print(f"🔍 Root Cause Agent: Investigating causes")
        
        try:
            # TODO: Implement root cause analysis
            # For now, placeholder implementation
            state.update({
                'current_agent': 'root_cause_agent',
                'previous_agent': state.get('current_agent'),
                'root_cause_steps': [],
                'root_cause_findings': None,
                'knowledge_graph_used': None
            })
            
            print(f"  → Root cause analysis completed (placeholder)")
            
            return state
            
        except Exception as e:
            state['errors'].append({
                'agent': 'root_cause_agent',
                'error': str(e),
                'timestamp': self._get_timestamp()
            })
            return state
    
    def _follow_up_agent_node(self, state: AgentState) -> AgentState:
        """Follow-up Agent Node - Generate next questions"""
        
        print(f"❓ Follow-up Agent: Generating suggestions")
        
        try:
            # TODO: Implement follow-up question generation
            # For now, placeholder implementation
            state.update({
                'current_agent': 'follow_up_agent',
                'previous_agent': state.get('current_agent'),
                'suggested_questions': [
                    "Would you like to see a breakdown by time period?",
                    "Shall we analyze by different dimensions?",
                    "Do you want to investigate root causes?"
                ],
                'follow_up_context': {}
            })
            
            print(f"  → Generated {len(state['suggested_questions'])} follow-up questions")
            
            return state
            
        except Exception as e:
            state['errors'].append({
                'agent': 'follow_up_agent',
                'error': str(e),
                'timestamp': self._get_timestamp()
            })
            return state
    
    def _memory_management_node(self, state: AgentState) -> AgentState:
        """Memory Management Node - Clean up and preserve insights"""
        
        print(f"🧠 Memory Management: Processing memory actions")
        
        try:
            memory_actions = state.get('routing_decision', {}).get('memory_actions', {})
            memory_action = memory_actions.get('memory_action', 'PRESERVE_ALL')
            
            if memory_action == 'EXTRACT_INSIGHTS_AND_CLEAR':
                # Extract insights before clearing
                insights = self._extract_insights(state)
                state['context_from_previous_query'] = insights
                
                # Clear transient data
                transient_keys = ['query_results', 'sql_attempts', 'root_cause_steps']
                for key in transient_keys:
                    if key in state:
                        del state[key]
                        
            elif memory_action == 'START_FRESH':
                # Clear everything except core session info
                preserve_keys = ['session_id', 'user_id', 'user_question', 'conversation_history', 'user_preferences']
                new_state = {key: state[key] for key in preserve_keys if key in state}
                state.clear()
                state.update(new_state)
            
            state.update({
                'current_agent': 'memory_management',
                'previous_agent': state.get('current_agent')
            })
            
            print(f"  → Memory action: {memory_action}")
            
            return state
            
        except Exception as e:
            state['errors'].append({
                'agent': 'memory_management',
                'error': str(e),
                'timestamp': self._get_timestamp()
            })
            return state
    
    # ============ ROUTING FUNCTIONS ============
    
    def _route_from_navigation(self, state: AgentState) -> str:
        """Route from navigation controller based on decision"""
        
        routing_decision = state.get('routing_decision', {})
        next_agent = routing_decision.get('next_agent', 'router_agent')
        
        print(f"  🔀 Navigation routing: {next_agent}")
        return next_agent
    
    def _route_from_sql_agent(self, state: AgentState) -> str:
        """Route from SQL agent based on execution results and question type"""
        
        execution_status = state.get('execution_status')
        flow_type = state.get('flow_type')
        
        if execution_status == 'failed':
            return 'END'
        elif flow_type == 'analytical' or 'why' in state.get('user_question', '').lower():
            return 'variance_detection_agent'
        else:
            return 'follow_up_agent'
    
    def _route_from_variance_detection(self, state: AgentState) -> str:
        """Route from variance detection based on threshold analysis"""
        
        threshold_exceeded = state.get('threshold_exceeded', False)
        requires_root_cause = state.get('requires_root_cause', False)
        
        if threshold_exceeded or requires_root_cause:
            return 'root_cause_agent'
        else:
            return 'follow_up_agent'
    
    def _route_from_follow_up(self, state: AgentState) -> str:
        """Route from follow-up based on user selection"""
        
        user_selected = state.get('user_selected_followup')
        
        if user_selected:
            # User selected a follow-up question - start new analysis
            state['user_question'] = user_selected
            state['user_selected_followup'] = None
            return 'navigation_controller'
        else:
            return 'END'
    
    def _route_from_memory_management(self, state: AgentState) -> str:
        """Route from memory management"""
        
        routing_decision = state.get('routing_decision', {})
        special_instructions = routing_decision.get('special_instructions', {})
        
        if special_instructions.get('dataset_selection_needed', False):
            return 'router_agent'
        else:
            return 'END'
    
    # ============ UTILITY FUNCTIONS ============
    
    def _extract_insights(self, state: AgentState) -> Dict:
        """Extract insights from current state before clearing"""
        
        return {
            'successful_dataset': state.get('selected_dataset'),
            'query_pattern': state.get('generated_sql', '').split('FROM')[0] if state.get('generated_sql') else None,
            'analysis_approach': state.get('selection_reasoning'),
            'user_flow_preference': state.get('flow_type'),
            'timestamp': self._get_timestamp()
        }
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    # ============ PUBLIC INTERFACE ============
    
    def run_workflow(self, user_question: str, session_id: str, user_id: str = "default_user") -> Dict:
        """Run the complete workflow for a user question"""
        
        # Create initial state
        initial_state = AgentState(
            session_id=session_id,
            user_id=user_id,
            user_question=user_question,
            original_question=user_question,
            messages=[],
            conversation_history=[],
            user_preferences={},
            errors=[],
            retry_count=0
        )
        
        print(f"\n🚀 Starting Healthcare Finance Workflow")
        print(f"Question: {user_question}")
        print(f"Session: {session_id}")
        print("=" * 60)
        
        try:
            # Run the workflow
            config = {"configurable": {"thread_id": session_id}}
            final_state = self.app.invoke(initial_state, config)
            
            print("=" * 60)
            print(f"✅ Workflow completed successfully")
            print(f"Final agent: {final_state.get('current_agent')}")
            print(f"Errors: {len(final_state.get('errors', []))}")
            
            return {
                'success': True,
                'final_state': final_state,
                'session_id': session_id,
                'errors': final_state.get('errors', [])
            }
            
        except Exception as e:
            print(f"❌ Workflow failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'session_id': session_id
            }
    
    def stream_workflow(self, user_question: str, session_id: str, user_id: str = "default_user"):
        """Stream the workflow execution for real-time updates"""
        
        initial_state = AgentState(
            session_id=session_id,
            user_id=user_id, 
            user_question=user_question,
            original_question=user_question,
            messages=[],
            conversation_history=[],
            user_preferences={},
            errors=[],
            retry_count=0
        )
        
        config = {"configurable": {"thread_id": session_id}}
        
        # Stream the workflow execution
        for step in self.app.stream(initial_state, config):
            yield step

# Example usage
if __name__ == "__main__":
    # Initialize workflow
    db_client = DatabricksClient()
    workflow = HealthcareFinanceWorkflow(db_client)
    
    # Test questions
    test_questions = [
        "What are Q3 pharmacy claims costs?",
        "Why are medical claims 18% higher than forecast?",
        "Show me utilization trends by therapeutic class"
    ]
    
    for question in test_questions:
        print(f"\n{'='*80}")
        print(f"TESTING: {question}")
        print('='*80)
        
        result = workflow.run_workflow(
            user_question=question,
            session_id=f"test_{hash(question) % 1000}",
            user_id="test_user"
        )
        
        if result['success']:
            print(f"✅ Success - Final agent: {result['final_state'].get('current_agent')}")
        else:
            print(f"❌ Failed: {result['error']}")
