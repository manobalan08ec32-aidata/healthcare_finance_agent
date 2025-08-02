def run_workflow(self, user_question: str, session_id: str, user_id: str = "default_user") -> Dict:
        """Run the complete workflow with enterprise tracking"""
        
        start_time = time.time()
        
        # Create initial state with tracking
        initial_state = AgentState(
            session_id=session_id,
            user_id=user_id,
            original_question=user_question,
            current_question=user_question
        )
        
        # Track user question
        initial_state = self.tracking.track_user_question(initial_state, user_question)
        
        print(f"\n🚀 Starting Healthcare Finance Workflow")
        print(f"Question: {user_question}")
        print(f"Session: {session_id}")
        print("=" * 60)
        
        try:
            # Run the workflow with tracking
            config = {"configurable": {"thread_id": session_id}}
            final_state = self.app.invoke(initial_state, config)
            
            #from typing import Dict, Any
import time
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from state_schema import AgentState
from databricks_client import DatabricksClient
from llm_navigation_controller import LLMNavigationController
from llm_router_agent import LLMRouterAgent
from enterprise_tracking_system import EnterpriseTrackingSystem

class HealthcareFinanceWorkflow:
    """Enterprise LangGraph workflow with comprehensive tracking"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
        # Initialize agents
        self.nav_controller = LLMNavigationController(databricks_client)
        self.router_agent = LLMRouterAgent(databricks_client)
        
        # Initialize enterprise tracking
        self.tracking = EnterpriseTrackingSystem(databricks_client)
        
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
        workflow.add_node("user_clarification", self._user_clarification_node)
        workflow.add_edge("user_clarification", "phase1_summary")
        
        # Summary node ends the workflow
        workflow.add_edge("phase1_summary", END)
        
        return workflow
    
    # ============ NODE IMPLEMENTATIONS ============
    
    def _navigation_controller_node(self, state: AgentState) -> AgentState:
        """Navigation Controller Node with enterprise tracking"""
        
        agent_name = "navigation_controller"
        start_time = time.time()
        
        print(f"🧭 Navigation Controller: Analyzing '{state['current_question']}'")
        
        # Track agent execution start
        state = self.tracking.track_agent_execution(
            state, agent_name, "question_analysis", "started", metadata={
                "question": state['current_question'],
                "question_length": len(state['current_question'])
            }
        )
        
        try:
            # Execute navigation controller
            routing_decision = self.nav_controller.route_user_query(state)
            
            # Track LLM interaction if any
            # (Navigation controller makes LLM calls internally)
            state = self.tracking.track_llm_call(
                state,
                agent_name=agent_name,
                prompt=f"Analyze healthcare finance question: {state['current_question']}",
                response=str(routing_decision),
                duration=time.time() - start_time,
                tokens_used=200  # Estimate - could be actual from API
            )
            
            # Update state with routing information
            state.update({
                'current_agent': agent_name,
                'previous_agent': state.get('current_agent'),
                'flow_type': routing_decision['flow_type'],
                'transition_type': routing_decision.get('transition_type'),
                'comparison_intent': routing_decision.get('comparison_intent')
            })
            
            # Track routing decision
            state = self.tracking.track_workflow_step(
                state, 
                step_name="navigation_analysis",
                step_result="success",
                duration=time.time() - start_time
            )
            
            # Add routing decision to state with proper tracking
            routing_entry = {
                'timestamp': self.tracking._get_timestamp(),
                'decision': routing_decision,
                'confidence': routing_decision.get('confidence', 0.8),
                'next_agent': routing_decision.get('next_agent')
            }
            
            state = {
                **state,
                'routing_decisions': [routing_entry]
            }
            
            # Track successful execution
            state = self.tracking.track_agent_execution(
                state, agent_name, "question_analysis", "success", 
                duration=time.time() - start_time,
                metadata={"next_agent": routing_decision.get('next_agent')}
            )
            
            print(f"  → Routing to: {routing_decision['next_agent']}")
            print(f"  → Flow type: {routing_decision['flow_type']}")
            
            return state
            
        except Exception as e:
            # Track error
            state = self.tracking.track_error(state, agent_name, str(e), "navigation_error")
            state = self.tracking.track_agent_execution(
                state, agent_name, "question_analysis", "failed",
                duration=time.time() - start_time
            )
            
            print(f"  ❌ Navigation failed: {str(e)}")
            return state
    
    def _router_agent_node(self, state: AgentState) -> AgentState:
        """Router Agent Node with comprehensive tracking"""
        
        agent_name = "router_agent"
        start_time = time.time()
        
        print(f"🎯 Router Agent: Selecting dataset for analysis")
        
        # Track agent execution start
        state = self.tracking.track_agent_execution(
            state, agent_name, "dataset_selection", "started"
        )
        
        try:
            # Execute router agent
            selection_result = self.router_agent.select_dataset(state)
            
            # Track vector search operation
            if hasattr(self.router_agent, '_last_search_duration'):
                state = self.tracking.track_vector_search(
                    state,
                    query=state['current_question'],
                    results=selection_result.get('available_datasets', []),
                    duration=getattr(self.router_agent, '_last_search_duration', 0),
                    index_name=self.db_client.VECTOR_TBL_INDEX
                )
            
            # Check if clarification is needed (interrupt workflow)
            if selection_result.get('requires_clarification'):
                print(f"  ❓ Clarification needed from user")
                
                # Track clarification request
                state = self.tracking.track_clarification_request(
                    state,
                    question=selection_result.get('clarification_question', ''),
                    options=selection_result.get('user_options', [])
                )
                
                # Set up interrupt data
                state.update({
                    'current_agent': agent_name,
                    'previous_agent': state.get('current_agent'),
                    'requires_user_input': True,
                    'clarification_data': selection_result.get('interrupt_data'),
                    'pending_selection_result': selection_result
                })
                
                # Track workflow step
                state = self.tracking.track_workflow_step(
                    state,
                    step_name="dataset_selection_clarification",
                    step_result="awaiting_user_input",
                    duration=time.time() - start_time
                )
                
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
            
            # Track dataset selection
            state = self.tracking.track_dataset_selection(
                state,
                selected_dataset=selection_result['selected_dataset'],
                reasoning=selection_result['selection_reasoning'],
                confidence=selection_result.get('selection_confidence', 0.8),
                available_options=selection_result.get('available_datasets', [])
            )
            
            # Track LLM interaction for dataset selection
            state = self.tracking.track_llm_call(
                state,
                agent_name=agent_name,
                prompt=f"Select dataset for: {state['current_question']}",
                response=selection_result['selection_reasoning'],
                duration=time.time() - start_time,
                tokens_used=300  # Estimate
            )
            
            # Track successful execution
            state = self.tracking.track_agent_execution(
                state, agent_name, "dataset_selection", "success",
                duration=time.time() - start_time,
                metadata={
                    "selected_dataset": selection_result['selected_dataset'],
                    "confidence": selection_result.get('selection_confidence', 0.8)
                }
            )
            
            # Track workflow step
            state = self.tracking.track_workflow_step(
                state,
                step_name="dataset_selection",
                step_result="success",
                duration=time.time() - start_time
            )
            
            print(f"  → Selected: {selection_result['selected_dataset']}")
            print(f"  → Confidence: {selection_result.get('selection_confidence', 0.8):.1%}")
            
            return state
            
        except Exception as e:
            # Track error
            state = self.tracking.track_error(state, agent_name, str(e), "dataset_selection_error")
            state = self.tracking.track_agent_execution(
                state, agent_name, "dataset_selection", "failed",
                duration=time.time() - start_time
            )
            
            print(f"  ❌ Dataset selection failed: {str(e)}")
            return state
    
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
            state['errors'].append({
                'agent': 'user_clarification',
                'error': str(e),
                'timestamp': self._get_timestamp()
            })
            return state
    
    # Remove unused nodes for Phase 1
    def _sql_template_agent_node(self, state: AgentState) -> AgentState:
        """Placeholder - Phase 2"""
        return state
    
    def _sql_agent_node(self, state: AgentState) -> AgentState:
        """Placeholder - Phase 2"""
        return state
    
    def _variance_detection_agent_node(self, state: AgentState) -> AgentState:
        """Placeholder - Phase 2"""
        return state
    
    def _root_cause_agent_node(self, state: AgentState) -> AgentState:
        """Placeholder - Phase 3"""
        return state
    
    def _follow_up_agent_node(self, state: AgentState) -> AgentState:
        """Placeholder - Phase 3"""
        return state
    
    def _memory_management_node(self, state: AgentState) -> AgentState:
        """Placeholder - Future"""
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
                'question_analyzed': state.get('user_question'),
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
            state['errors'].append({
                'agent': 'phase1_summary',
                'error': str(e),
                'timestamp': self._get_timestamp()
            })
            return state
    
    def _generate_phase1_recommendation(self, state: AgentState) -> str:
        """Generate recommendation for next steps"""
        
        flow_type = state.get('routing_decision', {}).get('flow_type')
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
    
    # ============ SIMPLIFIED ROUTING FUNCTIONS FOR PHASE 1 ============
    
    def _user_clarification_node(self, state: AgentState) -> AgentState:
        """User Clarification Node - Handle dataset selection clarification"""
        
        print(f"❓ User Clarification: Waiting for user input")
        
        # This node will be called after user provides clarification
        # The actual user input processing should happen before this node is reached
        
        state.update({
            'current_agent': 'user_clarification',
            'previous_agent': state.get('current_agent')
        })
        
        print(f"  → User clarification processed")
        
        return state
    
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
    
    # Remove unused routing functions for Phase 1
    def _route_from_sql_agent(self, state: AgentState) -> str:
        """Placeholder - Phase 2"""
        return 'END'
    
    def _route_from_variance_detection(self, state: AgentState) -> str:
        """Placeholder - Phase 2"""
        return 'END'
    
    def _route_from_follow_up(self, state: AgentState) -> str:
        """Placeholder - Phase 3"""
        return 'END'
    
    def _route_from_memory_management(self, state: AgentState) -> str:
        """Placeholder - Future"""
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
        """Run the complete workflow with enterprise tracking"""
        
        start_time = time.time()
        
        # Create initial state with tracking
        initial_state = AgentState(
            session_id=session_id,
            user_id=user_id,
            original_question=user_question,
            current_question=user_question
        )
        
        # Track user question
        initial_state = self.tracking.track_user_question(initial_state, user_question)
        
        print(f"\n🚀 Starting Healthcare Finance Workflow")
        print(f"Question: {user_question}")
        print(f"Session: {session_id}")
        print("=" * 60)
        
        try:
            # Run the workflow with tracking
            config = {"configurable": {"thread_id": session_id}}
            final_state = self.app.invoke(initial_state, config)
            
            # Calculate total processing time
            total_duration = time.time() - start_time
            final_state['total_processing_time'] = total_duration
            
            # Track final agent response
            final_response = self._generate_final_response(final_state)
            final_state = self.tracking.track_agent_response(
                final_state, "workflow_complete", final_response
            )
            
            # Generate session analytics
            analytics = self.tracking.generate_session_analytics(final_state)
            
            # Persist all tracking data to database
            persistence_success = self.tracking.persist_session_data(final_state)
            
            print("=" * 60)
            print(f"✅ Workflow completed successfully")
            print(f"Final agent: {final_state.get('current_agent')}")
            print(f"Total duration: {total_duration:.2f}s")
            print(f"Errors: {len(final_state.get('error_log', []))}")
            print(f"Data persisted: {'✅' if persistence_success else '❌'}")
            
            return {
                'success': True,
                'final_state': final_state,
                'session_id': session_id,
                'analytics': analytics,
                'total_duration': total_duration,
                'data_persisted': persistence_success,
                'errors': final_state.get('error_log', [])
            }
            
        except Exception as e:
            # Track workflow failure
            error_duration = time.time() - start_time
            error_state = self.tracking.track_error(
                initial_state, "workflow", str(e), "workflow_execution_error"
            )
            
            # Try to persist error data
            try:
                self.tracking.persist_session_data(error_state)
            except:
                pass  # Don't fail on persistence during error handling
            
            print(f"❌ Workflow failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'session_id': session_id,
                'duration': error_duration,
                'error_state': error_state
            }
    
    def process_user_clarification(self, user_choice: Dict, state: AgentState) -> AgentState:
        """Process user's dataset clarification choice with tracking"""
        
        print(f"👤 Processing user clarification: {user_choice}")
        
        start_time = time.time()
        
        try:
            # Track user clarification
            state = self.tracking.track_user_clarification(state, user_choice)
            
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
            
            # Track the final dataset selection after clarification
            state = self.tracking.track_dataset_selection(
                state,
                selected_dataset=final_selection['selected_dataset'],
                reasoning=f"User clarified: {final_selection['selection_reasoning']}",
                confidence=1.0,  # User made the choice
                available_options=available_datasets
            )
            
            # Track workflow step completion
            state = self.tracking.track_workflow_step(
                state,
                step_name="user_clarification_processed",
                step_result="success",
                duration=time.time() - start_time
            )
            
            print(f"  ✅ User selected: {final_selection['selected_dataset']}")
            
            return state
            
        except Exception as e:
            # Track error in clarification processing
            state = self.tracking.track_error(
                state, "user_clarification", str(e), "clarification_processing_error"
            )
            
            print(f"  ❌ Clarification processing failed: {str(e)}")
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
    
    def get_session_analytics(self, session_id: str) -> Dict:
        """Get analytics for a specific session"""
        
        # This would typically load from database
        # For now, return placeholder
        return {
            "session_id": session_id,
            "status": "analytics_available",
            "note": "Full analytics available after workflow completion"
        }
    
    def get_compliance_report(self, session_id: str) -> Dict:
        """Generate compliance report for a session"""
        
        # This would load session data and generate compliance report
        return {
            "session_id": session_id,
            "compliance_status": "compliant",
            "audit_trail_complete": True,
            "data_retention_policy": "followed"
        }
    
    # Utility function for timestamp generation
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
    
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