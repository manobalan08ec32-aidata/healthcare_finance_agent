import asyncio
import time
from datetime import datetime
from typing import Dict, Any, List
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from agents.llm_navigation_controller import LLMNavigationController

class AsyncHealthcareFinanceWorkflow:
    """Simplified async LangGraph workflow focusing on navigation controller"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
        # Initialize only the navigation controller
        self.nav_controller = LLMNavigationController(databricks_client)

        # Build the simplified graph
        self.workflow = self._build_workflow()
        
        # Compile with checkpointer
        checkpointer = MemorySaver()
        self.app = self.workflow.compile(checkpointer=checkpointer)
    
    def _build_workflow(self) -> StateGraph:
        """Build simplified workflow with just entry router and navigation controller"""
        workflow = StateGraph(AgentState)

        # Add only the two nodes we need
        workflow.add_node("entry_router", self._entry_router_node)
        workflow.add_node("navigation_controller", self._navigation_controller_node)

        # Set entry point
        workflow.set_entry_point("entry_router")

        # Routing from entry_router
        workflow.add_conditional_edges(
            "entry_router",
            self._route_from_entry_router,
            {
                "navigation_controller": "navigation_controller",
                "END": END
            }
        )

        # Navigation controller always ends (for this simplified version)
        workflow.add_edge("navigation_controller", END)

        return workflow
    
    # ============ NODE IMPLEMENTATIONS ============

    async def _entry_router_node(self, state: AgentState) -> AgentState:
        """Entry router node to decide workflow entry point."""
        print("Entry router node: Deciding entry point...")
        
        # Reset error messages
        state['nav_error_msg'] = None
        state['router_error_msg'] = None 
        state['follow_up_error_msg'] = None
        state['sql_gen_error_msg'] = None
        
        # Simple routing logic
        if state.get("requires_dataset_clarification", False):
            state['next_agent'] = ['END']  # Skip for simplified version
            state['next_agent_disp'] = 'Dataset clarification needed'
        elif state.get("is_sql_followup", False):
            state['next_agent'] = ['END']  # Skip for simplified version
            state['next_agent_disp'] = 'SQL followup needed'
        else:
            state['next_agent'] = ['navigation_controller']
            state['next_agent_disp'] = 'Question Validator'

        return state
    
    def _route_from_entry_router(self, state: AgentState) -> str:
        """Route from entry_router to the correct starting node."""
        if state.get("requires_dataset_clarification", False):
            print("Routing entry to: END (dataset clarification)")
            return "END"
        elif state.get("is_sql_followup", False):
            print("Routing entry to: END (SQL followup)")
            return "END"
        else:
            print("Routing entry to: navigation_controller")
            return "navigation_controller"
    
    async def _navigation_controller_node(self, state: AgentState) -> AgentState:
        """Async Navigation Controller node"""
        
        navigation_start_time = datetime.now().isoformat()
        print(f"Navigation Controller starting: {navigation_start_time}")
        
        try:
            # ASYNC CALL - This is the key change!
            nav_result = await self.nav_controller.process_user_query(state)
            
            # Update state with navigation results
            state['current_question'] = nav_result['rewritten_question']
            state['question_type'] = nav_result['question_type']
            state['next_agent'] = nav_result['next_agent']
            state['current_agent'] = 'Question Validator'
            state['nav_error_msg'] = nav_result.get('error_message')  
            state['topic_drift'] = False   
            state['missing_dataset_items'] = False   
            
            # Add retry count tracking
            state['llm_retry_count'] = nav_result.get('llm_retry_count', 0)
            
            # Store pending business question for context preservation
            state['pending_business_question'] = nav_result.get('pending_business_question', '')
            
            # Check if this is a greeting or DML/DDL request
            greeting_response = nav_result.get('greeting_response')
            is_dml_ddl = nav_result.get('is_dml_ddl', False)
            
            if nav_result.get('error', False):
                print(f"Navigation error detected: {nav_result.get('error_message')}")
                state['nav_error_msg'] = nav_result.get('error_message')
                state['next_agent'] = 'END'
                state['next_agent_disp'] = 'Model serving endpoint failed'
                state['requires_domain_clarification'] = False
                state['domain_followup_question'] = None
                print("Navigation failed - ending workflow")
                return state
                
            if greeting_response and greeting_response.strip():
                # This is a greeting or DML/DDL - store response and end workflow
                state['greeting_response'] = greeting_response
                state['is_dml_ddl'] = is_dml_ddl
                state['next_agent'] = 'END'
                state['next_agent_disp'] = 'Greeting response' if not is_dml_ddl else 'DML/DDL not supported'
                state['requires_domain_clarification'] = False
                state['domain_followup_question'] = None
                
                print("Greeting/DML detected - ending workflow")
                return state
            
            # Check if LLM returned a domain follow-up question
            domain_followup_question = nav_result.get('domain_followup_question')
            
            if domain_followup_question and domain_followup_question.strip():
                # LLM wants clarification - stop workflow here
                state['domain_followup_question'] = domain_followup_question
                state['requires_domain_clarification'] = True
                state['rewritten_question'] = nav_result['rewritten_question']
                state['next_agent'] = 'END'
                state['next_agent_disp'] = 'Waiting for domain selection'
                state['greeting_response'] = None
                
                print("Domain clarification required")
            else:
                # No clarification needed - continue with normal flow
                state['domain_followup_question'] = None
                state['requires_domain_clarification'] = False
                state['domain_selection'] = nav_result['domain_selection']
                state['rewritten_question'] = nav_result['rewritten_question']
                state['greeting_response'] = None
                
                # Set next agent display based on question type
                question_type = nav_result['question_type']
                if question_type == "why":
                    state['next_agent_disp'] = 'Drill through Analysis'
                else:
                    state['next_agent_disp'] = 'Agent Dataset Identifier'
                
                # Add ACTUAL BUSINESS QUESTIONS to history
                if 'user_question_history' not in state or state['user_question_history'] is None:
                    state['user_question_history'] = []
                
                # Only add rewritten business questions to history
                rewritten_question = nav_result['rewritten_question']
                if rewritten_question and rewritten_question.strip():
                    state['user_question_history'].append(rewritten_question)
                    print(f"Added business question to history: '{rewritten_question}'")
                else:
                    print("No valid business question to add to history")
            
            print(f"Navigation Controller ending: {datetime.now().isoformat()}")
            return state
            
        except Exception as e:
            print(f"Navigation failed: {str(e)}")
            if 'errors' not in state:
                state['errors'] = []
            state['errors'].append(f"Navigation error: {str(e)}")
            state['next_agent'] = 'END'
            state['nav_error_msg'] = f"Navigation error: {str(e)}"
            return state

    # ============ ASYNC STREAMING METHODS ============
    
    async def astream(self, initial_state: AgentState, config: Dict[str, Any]):
        """Async streaming method for the workflow"""
        async for step_data in self.app.astream(initial_state, config=config):
            yield step_data
    
    async def ainvoke(self, initial_state: AgentState, config: Dict[str, Any]) -> AgentState:
        """Async invoke method for the workflow"""
        return await self.app.ainvoke(initial_state, config=config)
