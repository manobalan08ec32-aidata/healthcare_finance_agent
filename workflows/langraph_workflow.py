import time
from datetime import datetime
from typing import Dict, Any, List
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from core.llm_navigation_controller import LLMNavigationController
from core.llm_router_agent import LLMRouterAgent

class HealthcareFinanceWorkflow:
    """Clean LangGraph workflow - Phase 1: Navigation + Router only"""
    
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
        """Build the LangGraph workflow - Phase 1: Navigation + Router only"""
        
        # Create the graph
        workflow = StateGraph(AgentState)
        
        # Add Phase 1 nodes only
        workflow.add_node("navigation_controller", self._navigation_controller_node)
        workflow.add_node("router_agent", self._router_agent_node)
        workflow.add_node("user_clarification", self._user_clarification_node)
        workflow.add_node("phase1_summary", self._phase1_summary_node)
        
        # Set entry point
        workflow.set_entry_point("navigation_controller")
        
        # Simple Phase 1 flow: Navigation → Router → Summary → END
        workflow.add_conditional_edges(
            "navigation_controller",
            self._route_from_navigation_phase1,
            {
                "router_agent": "router_agent",
                "phase1_summary": "phase1_summary"
            }
        )
        
        # Router agent can route to summary or require user input
        workflow.add_conditional_edges(
            "router_agent",
            self._route_from_router_agent,
            {
                "phase1_summary": "phase1_summary",
                "user_clarification": "user_clarification",
                "END": END
            }
        )
        
        # Add user clarification node for dataset selection
        workflow.add_edge("user_clarification", "phase1_summary")
        
        # Summary node ends the workflow
        workflow.add_edge("phase1_summary", END)
        
        return workflow
    
    # ============ NODE IMPLEMENTATIONS ============
    
    def _navigation_controller_node(self, state: AgentState) -> AgentState:
        """Navigation Controller Node"""
        
        agent_name = "navigation_controller"
        start_time = time.time()
        
        print(f"🧭 Navigation Controller: Analyzing '{state['current_question']}'")
        
        try:
            # Execute navigation controller
            routing_decision = self.nav_controller.route_user_query(state)
            
            # Update state with routing information
            state.update({
                'current_agent': agent_name,
                'previous_agent': state.get('current_agent'),
                'flow_type': routing_decision['flow_type'],
                'transition_type': routing_decision.get('transition_type'),
                'routing_decision': routing_decision
            })
            
            print(f"  → Routing to: {routing_decision['next_agent']}")
            print(f"  → Flow type: {routing_decision['flow_type']}")
            
            return state
            
        except Exception as e:
            print(f"  ❌ Navigation failed: {str(e)}")
            state['errors'].append(f"Navigation error: {str(e)}")
            return state
    
    def _router_agent_node(self, state: AgentState) -> AgentState:
        """Router Agent Node"""
        
        agent_name = "router_agent"
        start_time = time.time()
        
        print(f"🎯 Router Agent: Selecting dataset for analysis")
        
        try:
            # Execute router agent
            selection_result = self.router_agent.select_dataset(state)
            
            # Check if clarification is needed (interrupt workflow)
            if selection_result.get('requires_clarification'):
                print(f"  ❓ Clarification needed from user")
                
                # Set up interrupt data
                state.update({
                    'current_agent': agent_name,
                    'previous_agent': state.get('current_agent'),
                    'requires_user_input': True,
                    'clarification_data': selection_result.get('interrupt_data'),
                    'pending_selection_result': selection_result
                })
                
                return state
            
            # Normal dataset selection completed
            state.update({
                'current_agent': agent_name,
                'previous_agent': state.get('current_agent'),
                'selected_dataset': selection_result['selected_dataset'],
                'dataset_metadata': selection_result['dataset_metadata'],
                'selection_reasoning': selection_result['selection_reasoning'],
                'available_datasets': selection_result.get('available_datasets', []),
                'selection_confidence': selection_result.get('selection_confidence', 0.8),
                'requires_user_input': False
            })
            
            print(f"  → Selected: {selection_result['selected_dataset']}")
            print(f"  → Confidence: {selection_result.get('selection_confidence', 0.8):.1%}")
            
            return state
            
        except Exception as e:
            print(f"  ❌ Dataset selection failed: {str(e)}")
            state['errors'].append(f"Dataset selection error: {str(e)}")
            return state
    
    def _user_clarification_node(self, state: AgentState) -> AgentState:
        """User Clarification Node - Handle dataset selection clarification"""
        
        print(f"❓ User Clarification: Processing user input")
        
        state.update({
            'current_agent': 'user_clarification',
            'previous_agent': state.get('current_agent')
        })
        
        print(f"  → User clarification processed")
        
        return state
    
    def _phase1_summary_node(self, state: AgentState) -> AgentState:
        """Phase 1 Summary Node - Summarize navigation and dataset selection"""
        
        print(f"📋 Phase 1 Summary: Completing analysis")
        
        try:
            # Prepare summary of Phase 1 execution
            routing_decision = state.get('routing_decision', {})
            selected_dataset = state.get('selected_dataset')
            selection_reasoning = state.get('selection_reasoning', '')
            
            summary = {
                'phase': 'Phase 1 Complete',
                'question_analyzed': state.get('current_question'),
                'flow_type': routing_decision.get('flow_type'),
                'routing_confidence': routing_decision.get('confidence', 0.0),
                'selected_dataset': selected_dataset,
                'selection_confidence': state.get('selection_confidence', 0.0),
                'next_phase_ready': bool(selected_dataset),
                'recommendation': self._generate_phase1_recommendation(state)
            }
            
            state.update({
                'current_agent': 'phase1_summary',
                'previous_agent': state.get('current_agent'),
                'phase1_summary': summary,
                'workflow_complete': True
            })
            
            print(f"  ✅ Question Type: {summary['flow_type']}")
            print(f"  ✅ Dataset Selected: {selected_dataset}")
            print(f"  ✅ Ready for Phase 2: {summary['next_phase_ready']}")
            
            return state
            
        except Exception as e:
            print(f"  ❌ Phase 1 summary failed: {str(e)}")
            state['errors'].append(f"Summary error: {str(e)}")
            return state
    
    def _generate_phase1_recommendation(self, state: AgentState) -> str:
        """Generate recommendation for next steps"""
        
        routing_decision = state.get('routing_decision', {})
        flow_type = routing_decision.get('flow_type')
        selected_dataset = state.get('selected_dataset')
        
        if not selected_dataset:
            return "⚠️  Dataset selection failed. Review question or try rephrasing."
        
        if flow_type == 'descriptive':
            return "🎯 Ready for SQL generation to answer your 'what' question."
        elif flow_type == 'analytical':
            return "🔍 Ready for SQL execution and variance analysis for your 'why' question."
        elif flow_type == 'comparative':
            return "📊 Ready for comparison analysis with dynamic time period handling."
        else:
            return "🚀 Ready to proceed with SQL generation and analysis."
    
    # ============ ROUTING FUNCTIONS FOR PHASE 1 ============
    
    def _route_from_navigation_phase1(self, state: AgentState) -> str:
        """Simplified routing from navigation controller for Phase 1"""
        
        routing_decision = state.get('routing_decision', {})
        next_agent = routing_decision.get('next_agent', 'router_agent')
        
        # For Phase 1, we only support router_agent path
        if next_agent == 'router_agent':
            print(f"  🔀 Navigation routing: router_agent")
            return "router_agent"
        else:
            print(f"  🔀 Navigation routing: {next_agent} → router_agent (Phase 1 limitation)")
            return "router_agent"
    
    def _route_from_router_agent(self, state: AgentState) -> str:
        """Route from router agent - handle clarification needs"""
        
        requires_user_input = state.get('requires_user_input', False)
        has_selected_dataset = bool(state.get('selected_dataset'))
        has_errors = len(state.get('errors', [])) > 0
        
        if has_errors:
            print(f"  🔀 Router routing: END (errors occurred)")
            return "END"
        elif requires_user_input:
            print(f"  🔀 Router routing: user_clarification (needs user input)")
            return "user_clarification"
        elif has_selected_dataset:
            print(f"  🔀 Router routing: phase1_summary (dataset selected)")
            return "phase1_summary"
        else:
            print(f"  🔀 Router routing: END (no dataset selected)")
            return "END"
    
    # ============ PUBLIC INTERFACE ============
    
    def run_workflow(self, user_question: str, session_id: str, user_id: str = "default_user") -> Dict:
        """Run the complete workflow"""
        
        start_time = time.time()
        
        # Create initial state
        initial_state = AgentState(
            session_id=session_id,
            user_id=user_id,
            original_question=user_question,
            current_question=user_question
        )
        
        print(f"\n🚀 Starting Healthcare Finance Workflow")
        print(f"Question: {user_question}")
        print(f"Session: {session_id}")
        print("=" * 60)
        
        try:
            # Run the workflow
            config = {"configurable": {"thread_id": session_id}}
            final_state = self.app.invoke(initial_state, config)
            
            # Calculate total processing time
            total_duration = time.time() - start_time
            final_state['total_processing_time'] = total_duration
            
            print("=" * 60)
            print(f"✅ Workflow completed successfully")
            print(f"Final agent: {final_state.get('current_agent')}")
            print(f"Total duration: {total_duration:.2f}s")
            print(f"Errors: {len(final_state.get('errors', []))}")
            
            return {
                'success': True,
                'final_state': final_state,
                'session_id': session_id,
                'total_duration': total_duration,
                'errors': final_state.get('errors', [])
            }
            
        except Exception as e:
            error_duration = time.time() - start_time
            
            print(f"❌ Workflow failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'session_id': session_id,
                'duration': error_duration
            }
    
    def process_user_clarification(self, user_choice: Dict, state: AgentState) -> AgentState:
        """Process user's dataset clarification choice"""
        
        print(f"👤 Processing user clarification: {user_choice}")
        
        try:
            pending_result = state.get('pending_selection_result', {})
            available_datasets = pending_result.get('available_datasets', [])
            
            # Process the user's choice
            final_selection = self.router_agent.process_user_clarification(
                user_choice, available_datasets
            )
            
            # Update state with final selection
            state.update({
                'selected_dataset': final_selection['selected_dataset'],
                'dataset_metadata': final_selection['dataset_metadata'],
                'selection_reasoning': final_selection['selection_reasoning'],
                'selection_confidence': final_selection['selection_confidence'],
                'user_clarified': True,
                'requires_user_input': False,
                'clarification_data': None,
                'pending_selection_result': None
            })
            
            print(f"  ✅ User selected: {final_selection['selected_dataset']}")
            
            return state
            
        except Exception as e:
            print(f"  ❌ Clarification processing failed: {str(e)}")
            state['errors'].append(f"Clarification error: {str(e)}")
            return state
    
    def _generate_final_response(self, state: AgentState) -> str:
        """Generate final response based on workflow outcome"""
        
        if state.get('selected_dataset'):
            dataset_name = state['selected_dataset'].split('.')[-1]
            confidence = state.get('selection_confidence', 0.0)
            
            response = f"Analysis complete! Selected dataset '{dataset_name}' with {confidence:.1%} confidence. "
            
            if state.get('user_clarified'):
                response += "Thank you for the clarification. "
            
            summary = state.get('phase1_summary', {})
            if summary.get('recommendation'):
                response += f"Next step: {summary['recommendation']}"
            
            return response
        else:
            return "Unable to complete analysis. Please try rephrasing your question."

# Example usage
if __name__ == "__main__":
    from core.databricks_client import DatabricksClient
    
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
            if result['final_state'].get('selected_dataset'):
                print(f"📊 Selected dataset: {result['final_state']['selected_dataset']}")
                print(f"🎯 Confidence: {result['final_state'].get('selection_confidence', 0):.1%}")
        else:
            print(f"❌ Failed: {result['error']}")
            
    print("\n" + "="*80)
    print("✅ Workflow testing complete!")