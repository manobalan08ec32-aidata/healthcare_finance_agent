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
from agents.root_cause_analysis import RootCauseAnalysisAgent
from agents.followup_question import FollowupQuestionAgent
from agents.strategic_planner import StrategicPlanner
from agents.drillthrough_planner import DrillthroughPlanner
from langgraph.types import Command, interrupt
import os     

class HealthcareFinanceWorkflow:
    """Simplified LangGraph workflow with streaming support"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
        # Initialize agents
        self.nav_controller = LLMNavigationController(databricks_client)
        self.router_agent = LLMRouterAgent(databricks_client)
        self.sql_gen_agent = SQLGeneratorAgent(databricks_client)
        self.root_cause_agent = RootCauseAnalysisAgent(databricks_client)
        self.followup_agent = FollowupQuestionAgent(databricks_client)
        self.strategic_planner = StrategicPlanner(self.db_client)
        self.drillthrough_planner = DrillthroughPlanner(self.db_client)

        # Build the graph
        self.workflow = self._build_workflow()
        
        checkpointer = MemorySaver()
    
        # Compile with checkpointer
        self.app = self.workflow.compile(checkpointer=checkpointer)
    
    def _build_workflow(self) -> StateGraph:
        """Build the workflow with a dynamic entry point."""
        workflow = StateGraph(AgentState)

        # Add nodes
        workflow.add_node("entry_router", self._entry_router_node)
        workflow.add_node("navigation_controller", self._navigation_controller_node)
        workflow.add_node("router_agent", self._router_agent_node)
        workflow.add_node("sql_generator_agent", self._sql_generator_node)
        workflow.add_node("followup_question_agent", self._followup_question_node)
        workflow.add_node("root_cause_agent", self._root_cause_node)
        workflow.add_node("strategy_planner_agent", self._strategy_planner_node)
        workflow.add_node("drillthrough_planner_agent", self._drillthrough_planner_node)

        # Set entry point to entry_router
        workflow.set_entry_point("entry_router")

        # Conditional routing from entry_router
        workflow.add_conditional_edges(
            "entry_router",
            self._route_from_entry_router,
            {
                "navigation_controller": "navigation_controller",
                "router_agent":"router_agent",
                "sql_generator_agent": "sql_generator_agent"
            }
        )

        workflow.add_conditional_edges(
            "navigation_controller",
            self._route_from_navigation,
            {
                "router_agent": "router_agent",
                "root_cause_agent": "strategy_planner_agent",
                "END": END
            }
        )
        workflow.add_conditional_edges(
            "router_agent",
            self._route_from_router_agent,
            {
                "navigation_controller":"navigation_controller",
                "END": END,
                "sql_generator_agent": "sql_generator_agent"
            }
        )
        workflow.add_conditional_edges(
            "sql_generator_agent",
            self._route_from_sql_generator,
            {
                "END": END,
                "followup_question_agent": "followup_question_agent"
            }
        )
        workflow.add_edge("followup_question_agent", END)
        workflow.add_edge("strategy_planner_agent", "drillthrough_planner_agent")
        workflow.add_edge("strategy_planner_agent", END)

        return workflow

    
    
    # ============ NODE IMPLEMENTATIONS ============

    def _entry_router_node(self, state: AgentState) -> AgentState:
        """Entry router node to decide workflow entry point."""
        print("🔀 Entry router node: Deciding entry point...")
        state['nav_error_msg']=None
        state['router_error_msg']=None 
        state['follow_up_error_msg']=None
        state['sql_gen_error_msg']=None
        
        if state.get("requires_dataset_clarification", False):
            state['next_agent'] = ['router_agent']
            state['next_agent_disp'] = 'Dataset Identifier'
        if state.get("is_sql_followup", False):
            state['next_agent'] = ['sql_generator_agent']
            state['next_agent_disp'] = 'SQL Generator'
        
        else:
            state['next_agent'] = ['navigation_controller']
            state['next_agent_disp'] = 'Question Validator'

        # No state changes needed, just routing
        return state
    
    def _route_from_entry_router(self, state: AgentState) -> str:
        """Route from entry_router to the correct starting node."""
        if state.get("requires_dataset_clarification", False):
            print("🔀 Routing entry to: router_agent")
            return "router_agent"
        elif state.get("is_sql_followup", False):
            print("🔀 Routing entry to: sql_generator_agent")
            state['next_agent_disp'] = 'SQL Generator'
            return "sql_generator_agent"
        else:
            print("🔀 Routing entry to: navigation_controller")
            state['next_agent_disp'] = 'Question Validator'
            return "navigation_controller"
    
    def _navigation_controller_node(self, state: AgentState) -> AgentState:
        """Navigation Controller: Question rewriting + domain detection with context preservation"""

        navigation_start_time = datetime.now().isoformat()
        
        try:
            # Process user query with context preservation
            nav_result = self.nav_controller.process_user_query(state)  
            # Update state with navigation results
            state['current_question'] = nav_result['rewritten_question']
            state['question_type'] = nav_result['question_type']
            state['next_agent'] = nav_result['next_agent']
            state['current_agent'] = 'Question Validator'
            state['nav_error_msg']=nav_result.get('error_message')  
            state['topic_drift']=False   
            state['missing_dataset_items']=False   
            
            # Add retry count tracking
            state['llm_retry_count'] = nav_result.get('llm_retry_count', 0)
            
            # NEW: Store pending business question for context preservation
            state['pending_business_question'] = nav_result.get('pending_business_question', '')
            
            # Check if this is a greeting or DML/DDL request
            greeting_response = nav_result.get('greeting_response')
            is_dml_ddl = nav_result.get('is_dml_ddl', False)
            
            if nav_result.get('error', False):
                print(f"❌ Navigation error detected: {nav_result.get('error_message')}")
                state['nav_error_msg'] = nav_result.get('error_message')
                state['next_agent'] = 'END'
                state['next_agent_disp'] = 'Model serving endpoint failed'
                state['requires_domain_clarification'] = False
                state['domain_followup_question'] = None
                print(f"🛑 Navigation failed - ending workflow")
                return state
            if greeting_response and greeting_response.strip():
                # This is a greeting or DML/DDL - store response and end workflow
                state['greeting_response'] = greeting_response
                state['is_dml_ddl'] = is_dml_ddl
                state['next_agent'] = 'END'
                state['next_agent_disp'] = 'Greeting response' if not is_dml_ddl else 'DML/DDL not supported'
                state['requires_domain_clarification'] = False
                state['domain_followup_question'] = None
                
                print(f"🛑 Greeting/DML detected - ending workflow",state)
                
                return state
            
            # Check if LLM returned a domain follow-up question
            domain_followup_question = nav_result.get('domain_followup_question')
            
            if domain_followup_question and domain_followup_question.strip():
                # LLM wants clarification - stop workflow here
                state['domain_followup_question'] = domain_followup_question
                state['requires_domain_clarification'] = True
                state['rewritten_question'] = nav_result['rewritten_question']
                state['next_agent'] = 'END'  # Stop the workflow
                state['next_agent_disp'] = 'Waiting for domain selection'
                state['greeting_response'] = None  # Clear any greeting response
                
                print(f"🛑 Domain clarification required")
            else:
                # No clarification needed - continue with normal flow
                state['domain_followup_question'] = None
                state['requires_domain_clarification'] = False
                state['domain_selection'] = nav_result['domain_selection']
                state['rewritten_question'] = nav_result['rewritten_question']
                state['greeting_response'] = None  # Clear any greeting response
                
                # Fix the routing logic here
                question_type = nav_result['question_type']  # Get from nav_result, not undefined variable
                if question_type == "why":
                    state['next_agent_disp'] = 'Drill through Analysis'
                else:
                    state['next_agent_disp'] = 'Agent Dataset Identifier'
                
                # FIXED: Only add ACTUAL BUSINESS QUESTIONS to history
                # Initialize question history if needed
                if 'user_question_history' not in state or state['user_question_history'] is None:
                    state['user_question_history'] = []
                
                # Only add rewritten business questions to history (not greetings, not domain clarifications)
                rewritten_question = nav_result['rewritten_question']
                if rewritten_question and rewritten_question.strip():
                    state['user_question_history'].append(rewritten_question)
                    print(f"✅ Added business question to history: '{rewritten_question}'")
                else:
                    print(f"⚠️ No valid business question to add to history")
            
            print(f"\n🧭 Navigation Controller: ending {datetime.now().isoformat()}")
            
            return state
            
        except Exception as e:
            print(f"  ❌ Navigation failed: {str(e)}")
            if 'errors' not in state:
                state['errors'] = []
            state['errors'].append(f"Navigation error: {str(e)}")
            state['next_agent'] = 'router_agent'  # Fallback
            return state

    def _route_from_navigation(self, state: AgentState) -> str:
        """Route from navigation based on clarification needs, greetings, and question type"""
        
        print(f"🔀 _route_from_navigation received state keys: {list(state.keys())}")
        
        requires_clarification = state.get('requires_domain_clarification', False)
        question_type = state.get('question_type', 'what')
        next_agent = state.get('next_agent', 'router_agent')
        domain_question = state.get('domain_followup_question', 'None')
        greeting_response = state.get('greeting_response')
        is_dml_ddl = state.get('is_dml_ddl', False)
        pending_business_question = state.get('pending_business_question', '')
        nav_error_msg = state.get('nav_error_msg')  # Get error message
        
        print(f"🔀 Navigation routing debug:")
        print(f"  - requires_domain_clarification: {requires_clarification}")
        print(f"  - question_type: '{question_type}'")
        print(f"  - next_agent: '{next_agent}'")
        print(f"  - domain_followup_question exists: {bool(domain_question and domain_question != 'None')}")
        print(f"  - greeting_response exists: {bool(greeting_response)}")
        print(f"  - is_dml_ddl: {is_dml_ddl}")
        print(f"  - pending_business_question: '{pending_business_question}'")
        print(f"  - nav_error_msg: {nav_error_msg}")  # Log error message
        
        # Priority 1: Check for navigation errors
        if nav_error_msg:
            print(f"  🛑 Routing to: END (navigation error: {nav_error_msg})")
            return "END"
        
        # Priority 2: Check for greeting or DML/DDL response
        if greeting_response and greeting_response.strip():
            print(f"  🛑 Routing to: END (greeting/DML response)")
            return "END"
        
        # Priority 3: Check for domain clarification requirement
        if requires_clarification:
            print(f"  🛑 Routing to: END (domain clarification required)")
            print(f"  🛑 Will preserve business question: '{pending_business_question}'")
            return "END"
        
        # Priority 4: Normal workflow routing based on next_agent
        if next_agent == "root_cause_agent":
            print(f"  ✅ Routing to: root_cause_agent")
            return "root_cause_agent"
        elif next_agent == "router_agent":
            print(f"  ✅ Routing to: router_agent")
            return "router_agent"
        elif next_agent == "END":
            # This should only happen if we need to end the workflow
            print(f"  🛑 Routing to: END (explicitly set)")
            return "END"
        else:
            print(f"  ⚠️ Unknown next_agent '{next_agent}', defaulting to router_agent")
            return "router_agent"
    
    
