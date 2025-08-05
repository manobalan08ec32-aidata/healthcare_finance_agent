import time
from datetime import datetime
from typing import Dict, Any, List
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from agents.llm_navigation_controller import LLMNavigationController
from agents.llm_router_agent import LLMRouterAgent
from agents.sql_generator import SQLGeneratorAgent
from langgraph.types import Command, interrupt

class HealthcareFinanceWorkflow:
    """Simplified LangGraph workflow with streaming support"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
        # Initialize agents
        self.nav_controller = LLMNavigationController(databricks_client)
        self.router_agent = LLMRouterAgent(databricks_client)
        self.sql_gen_agent = SQLGeneratorAgent(databricks_client)
        
        # Build the graph
        self.workflow = self._build_workflow()
        
        # Compile with memory
        memory = MemorySaver()
        self.app = self.workflow.compile(checkpointer=memory)
    
    def _build_workflow(self) -> StateGraph:
        """Build the simplified workflow"""
        
        # Create the graph
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("navigation_controller", self._navigation_controller_node)
        workflow.add_node("router_agent", self._router_agent_node)
        workflow.add_node("sql_generator_agent", self._sql_generator_node)
        workflow.add_node("root_cause_agent", self._root_cause_node)
        workflow.add_node("user_clarification", self._user_clarification_node)
        workflow.add_node("workflow_complete", self._workflow_complete_node)
        
        # Set entry point
        workflow.set_entry_point("navigation_controller")
        # Navigation routes to two possible agents based on question type
        workflow.add_conditional_edges(
            "navigation_controller",
            self._route_from_navigation,
            {
                "router_agent": "router_agent",
                "root_cause_agent": "root_cause_agent"
            }
        )
        
        # # Router agent can complete or require clarification
        # workflow.add_conditional_edges(
        #     "router_agent",
        #     self._route_from_router,
        #     {
        #         "workflow_complete": "workflow_complete",
        #         "user_clarification": "user_clarification",
        #         "END": END
        #     }
        # )
        
        # # Other nodes complete the workflow
        workflow.add_edge("router_agent", "sql_generator_agent")
        workflow.add_edge( "sql_generator_agent",END)
        workflow.add_edge("root_cause_agent", END)
        # workflow.add_edge("user_clarification", "workflow_complete")
        # workflow.add_edge("workflow_complete", END)
        
        return workflow
    
    # ============ NODE IMPLEMENTATIONS ============
    
    def _navigation_controller_node(self, state: AgentState) -> AgentState:
        """Navigation Controller: Question rewriting + simple routing"""
        
        print(f"\n🧭 Navigation Controller: Processing question")
        
        try:
            # Process user query
            nav_result = self.nav_controller.process_user_query(state)
            
            # state['current_agent'] = 'navigation_controller'
            state['current_question'] = nav_result['rewritten_question']
            state['question_type'] = nav_result['question_type']
            state['next_agent'] = nav_result['next_agent']
            state['next_agent_disp'] = 'Agent Dataset Identifier'
            state['current_agent'] = 'navigation_controller'
            if 'user_question_history' not in state or state['user_question_history'] is None:
                state['user_question_history'] = []
            state['user_question_history'].append(nav_result['rewritten_question'])

             
            print('state after navigation',state)
            return state
            
        except Exception as e:
            print(f"  ❌ Navigation failed: {str(e)}")
            state['errors'].append(f"Navigation error: {str(e)}")
            state['next_agent'] = 'router_agent'  # Fallback
            return state
    
    def _router_agent_node(self, state: AgentState) -> AgentState:
        """Router Agent: Dataset selection with dynamic interrupt"""
    
        print(f"\n🎯 Router Agent: Selecting dataset")
    
        try:
            # Execute router agent
            selection_result = self.router_agent.select_dataset(state)
            
            
            # Update state with all selection results
            if selection_result.get('dataset_followup_question'):
                # Clarification needed - set state and interrupt
                state['dataset_followup_question'] = selection_result.get('dataset_followup_question')
                state['requires_clarification'] = True
                state['selection_reasoning'] = selection_result.get('selection_reasoning', '')
                state['selected_dataset'] = selection_result.get('selected_dataset')
                state['current_agent'] = 'router_agent'
                
                print(f"🛑 INTERRUPTING: Clarification required: {state['dataset_followup_question']}")
                
                # Dynamic interrupt - this pauses the graph execution and returns the clarification question
                user_clarification = interrupt({
                    "dataset_followup_question": state['dataset_followup_question']
                })
                
                print(f"📝 User provided clarification: {user_clarification}")
                
                # Update current question with clarification
                state['current_question'] = user_clarification
                state['dataset_followup_clarification'] = user_clarification
                
                # Call the fix router function
                try:
                    fixed_result = self.router_agent._fix_router_llm_call(state)
                    
                    # Update state with fixed results
                    if fixed_result.get('selected_dataset'):
                        state['selected_dataset'] = fixed_result.get('selected_dataset')
                        state['requires_clarification'] = False
                        state['dataset_followup_question'] = None
                        state['dataset_metadata'] = selection_result.get('table_kg', {})
                        question_history=state.get('user_question_history', '')
                        actual_question = questions_history[-1] if questions_history else None
                        state['current_question'] =actual_question
                        print(f"✅ Clarification resolved. Selected: {state['selected_dataset']}")
                    else:
                        print("❌ Clarification did not resolve dataset selection")
                        state['errors'].append("Clarification did not resolve dataset selection")
                        
                except Exception as e:
                    print(f"❌ Error in clarification handling: {str(e)}")
                    state['errors'].append(f"Clarification error: {str(e)}")
                    
            else:
                print('router output else clause',selection_result)
                # Clear selection made
                state['selected_dataset'] = selection_result.get('selected_dataset')
                state['dataset_metadata'] = selection_result.get('table_kg', {})
                state['requires_clarification'] = False
                state['dataset_followup_question'] = None
                print(f"✅ Dataset selected: {state.get('selected_dataset', 'Unknown')}")
            
            state['selection_reasoning'] = selection_result.get('selection_reasoning', '')
            state['current_agent'] = 'router_agent'
            state['next_agent'] = 'sql_generator_agent'
            state['next_agent_disp'] = 'SQL Generator and Synthesizer'

            print('state after router',state)
            return state
                    
        except Exception as e:
            print(f"  ❌ Router failed: {str(e)}")
            state['errors'].append(f"Router error: {str(e)}")
            return state
    
    def _sql_generator_node(self, state: AgentState) -> AgentState:
        """SQL Agent: SQL ,dataset and synthesizer generation"""
        
        print(f"\n🎯 SQL Agent: SQL ,dataset and synthesizer generation")
        
        try:
            # Execute router agent
            sql_output = self.sql_gen_agent.generate_and_execute_sql(state)
            
            state['sql_query'] = sql_output.get('sql_query', '')
            state['current_agent'] = 'sql_generator'
            state['query_results'] = sql_output.get('query_results', '')
            state['narrative_response'] = sql_output.get('narrative_response', '')
            # Ensure questions_sql_history is initialized as a list
            if 'questions_sql_history' not in state or state['questions_sql_history'] is None:
                state['questions_sql_history'] = []
            state['questions_sql_history'].append(state.get('current_question'))
            state['questions_sql_history'].append(sql_output.get('sql_query', 'None'))
            print('state after sql load',state)
            return state
                        
        except Exception as e:
            print(f"  ❌ SQL generator failed: {str(e)}")
            state['errors'].append(f"Router error: {str(e)}")
            return state
    
    def _root_cause_node(self, state: AgentState) -> AgentState:
        """Root Cause Agent: Analyze 'why' questions"""
        
        print(f"\n🔍 Root Cause Agent: Analyzing causes")
        
        # Placeholder for Phase 2/3
        state.update({
            'current_agent': 'root_cause_agent',
            'root_cause_analysis': f"Root cause analysis for: {state['current_question']}",
            'variance_detected': True
        })
        
        print(f"  ✅ Root cause analysis completed")
        print(f"  ✅ Question: {state['current_question']}")
        
        return state
    
    def _user_clarification_node(self, state: AgentState) -> AgentState:
        """User Clarification: Handle dataset clarification"""
        
        print(f"\n❓ User Clarification: Processing user choice")
        
        state.update({
            'current_agent': 'user_clarification'
        })
        
        print(f"  ✅ Clarification processed")
        
        return state
    
    def _workflow_complete_node(self, state: AgentState) -> AgentState:
        """Workflow Complete: Final summary"""
        
        print(f"\n✅ Workflow Complete: Summarizing results")
        
        # Create summary
        summary = {
            'question_processed': state['current_question'],
            'question_type': state.get('question_type'),
            'agent_used': state.get('current_agent'),
            'dataset_selected': state.get('selected_dataset'),
            'has_results': bool(state.get('query_results') or state.get('root_cause_analysis')),
            'completion_time': datetime.now().isoformat()
        }
        
        state.update({
            'current_agent': 'workflow_complete',
            'workflow_complete': True,
            'final_summary': summary
        })
        
        print(f"  ✅ Question: {state['current_question']}")
        print(f"  ✅ Type: {state.get('question_type')}")
        print(f"  ✅ Agent: {summary['agent_used']}")
        
        return state
    
    # ============ ROUTING FUNCTIONS ============
    
    def _route_from_navigation(self, state: AgentState) -> str:
        """Route from navigation based on question type"""
        
        question_type = state.get('question_type', 'what')
        next_agent = state.get('next_agent', 'router_agent')
        
        print(f"🔀 Navigation routing debug:")
        print(f"  - question_type: '{question_type}'")
        print(f"  - next_agent: '{next_agent}'")
        print(f"  - next_agent type: {type(next_agent)}")
        
        # Explicit routing with validation
        if next_agent == "root_cause_agent":
            print(f"  ✅ Routing to: root_cause_agent")
            return "root_cause_agent"
        elif next_agent == "router_agent":
            print(f"  ✅ Routing to: router_agent")
            return "router_agent"
        else:
            print(f"  ⚠️ Unknown next_agent '{next_agent}', defaulting to router_agent")
            return "router_agent"
    
    def _route_from_router(self, state: AgentState) -> str:
        """Route from router based on completion state"""
        
        requires_user_input = state.get('requires_user_input', False)
        has_selected_dataset = bool(state.get('selected_dataset'))
        has_errors = len(state.get('errors', [])) > 0
        
        if has_errors:
            print(f"  🔀 Router routing: END (errors)")
            return "END"
        elif requires_user_input:
            print(f"  🔀 Router routing: user_clarification")
            return "user_clarification"
        elif has_selected_dataset:
            print(f"  🔀 Router routing: workflow_complete")
            return "workflow_complete"
        else:
            print(f"  🔀 Router routing: END (no dataset)")
            return "END"
    
    # ============ PUBLIC INTERFACE ============
    
    def run_workflow(self, user_question: str, session_id: str, user_id: str = "default_user") -> Dict:
        """Run the workflow with detailed output"""
        
        start_time = time.time()
        
        # Create initial state
        initial_state = AgentState(
            session_id=session_id,
            user_id=user_id,
            original_question=user_question,
            current_question=user_question
        )
        
        print(f"\n🚀 Starting Healthcare Finance Workflow")
        print(f"Original Question: {user_question}")
        print(f"Session: {session_id}")
        print("=" * 60)
        
        try:
            # Run the workflow with streaming to see each step
            config = {"configurable": {"thread_id": session_id}}
            
            print(f"\n🔍 Streaming workflow execution:")
            final_state = None
            step_count = 0
            
            for step in self.app.stream(initial_state, config):
                step_count += 1
                print(f"\n--- Step {step_count} ---")
                for node_name, node_state in step.items():
                    print(f"📍 Executed node: '{node_name}'")
                    print(f"  Current agent: {node_state.get('current_agent')}")
                    print(f"  Next agent: {node_state.get('next_agent')}")
                    print(f"  Question type: {node_state.get('question_type')}")
                    final_state = node_state
            
            if final_state is None:
                print("❌ No steps executed - workflow might have failed to start")
                return {
                    'success': False,
                    'error': 'No workflow steps executed',
                    'session_id': session_id,
                    'duration': time.time() - start_time
                }
            
            # Calculate total processing time
            total_duration = time.time() - start_time
            final_state['total_processing_time'] = total_duration
            
            print("\n" + "=" * 60)
            print(f"✅ Workflow completed successfully")
            print(f"Total steps: {step_count}")
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
            
            print(f"\n❌ Workflow failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e),
                'session_id': session_id,
                'duration': error_duration
            }
    
    def stream_workflow(self, user_question: str, session_id: str, user_id: str = "default_user"):
        """Stream the workflow execution for real-time updates"""
        
        # Create initial state
        initial_state = AgentState(
            session_id=session_id,
            user_id=user_id,
            original_question=user_question,
            current_question=user_question
        )
        
        config = {"configurable": {"thread_id": session_id}}
        
        # Stream the workflow execution
        for step in self.app.stream(initial_state, config):
            yield {
                'step': step,
                'timestamp': datetime.now().isoformat()
            }
    
    def get_workflow_status(self, session_id: str) -> Dict:
        """Get current workflow status"""
        
        return {
            "session_id": session_id,
            "status": "active",
            "timestamp": datetime.now().isoformat()
        }

# Example usage
if __name__ == "__main__":
    from core.databricks_client import DatabricksClient
    
    db_client = DatabricksClient()
    workflow = HealthcareFinanceWorkflow(db_client)
    
    # Test questions
    test_questions = [
        "What is the claim revenue for the month of july 2025?"
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
            final_state = result['final_state']
            print(f"\n📊 FINAL RESULTS:")
            print(f"  - Question Type: {final_state.get('question_type')}")
            print(f"  - Selected Dataset: {final_state.get('selected_dataset')}")
            print(f"  - Final Agent: {final_state.get('current_agent')}")
        else:
            print(f"❌ Failed: {result['error']}")
            
    print("\n" + "="*80)
    print("✅ Workflow testing complete!")
