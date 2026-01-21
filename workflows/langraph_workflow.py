import asyncio
import time
from datetime import datetime
from pytz import timezone
from typing import Dict, Any, List
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from agents.llm_navigation_controller import LLMNavigationController
from agents.llm_router_agent import LLMRouterAgent
from agents.followup_question import FollowupQuestionAgent
from agents.strategic_planner import StrategicPlanner
from agents.drillthrough_planner import DrillthroughPlanner
from agents.narrative_agent import NarrativeAgent
from core.logger import setup_logger, log_with_user_context

# Initialize logger for this module
logger = setup_logger(__name__)

class AsyncHealthcareFinanceWorkflow:
    """Simplified async LangGraph workflow focusing on navigation controller"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
        # Initialize all agents
        self.nav_controller = LLMNavigationController(databricks_client)
        self.router_agent = LLMRouterAgent(databricks_client)
        self.followup_agent = FollowupQuestionAgent(databricks_client)
        self.strategic_planner = StrategicPlanner(self.db_client)
        self.drillthrough_planner = DrillthroughPlanner(self.db_client)
        self.narrative_agent = NarrativeAgent(databricks_client)

        # Build the main workflow (ends at strategy_planner_agent)
        self.workflow = self._build_workflow()
        
        # Build the separate follow-up workflow
        self.followup_workflow = self._build_followup_workflow()
        
        # Build the separate drillthrough workflow
        self.drillthrough_workflow = self._build_drillthrough_workflow()
        
        # Build the separate narrative workflow
        self.narrative_workflow = self._build_narrative_workflow()
        
        # Compile all workflows with checkpointer
        checkpointer = MemorySaver()
        self.app = self.workflow.compile(checkpointer=checkpointer)
        self.followup_app = self.followup_workflow.compile(checkpointer=checkpointer)
        self.drillthrough_app = self.drillthrough_workflow.compile(checkpointer=checkpointer)
        self.narrative_app = self.narrative_workflow.compile(checkpointer=checkpointer)
    
    def _build_workflow(self) -> StateGraph:
        """Build workflow with entry router, navigation controller and router agent"""
        workflow = StateGraph(AgentState)

        # Add nodes
        workflow.add_node("entry_router", self._entry_router_node)
        workflow.add_node("navigation_controller", self._navigation_controller_node)
        workflow.add_node("router_agent", self._router_agent_node)
        workflow.add_node("strategy_planner_agent", self._strategy_planner_node)
        

        # Set entry point
        workflow.set_entry_point("entry_router")

        # Routing from entry_router
        workflow.add_conditional_edges(
            "entry_router",
            self._route_from_entry_router,
            {
                "navigation_controller": "navigation_controller",
                "router_agent": "router_agent"
            }
        )

        # workflow.add_edge("navigation_controller", END)

        # Routing from navigation_controller
        workflow.add_conditional_edges(
            "navigation_controller",
            self._route_from_navigation,
            {
                "router_agent": "router_agent",
                "root_cause_agent": "strategy_planner_agent",
                "END": END
            }
        )

        # ⭐ NEW: Conditional routing from router_agent
        workflow.add_conditional_edges(
            "router_agent",
            self._route_from_router,
            {
                "navigation_controller": "navigation_controller",  # Loop back for new question
                "END": END  # Normal completion or topic drift
            }
        )
        
        # Strategy planner ends here for main workflow - drillthrough handled separately
        workflow.add_edge("strategy_planner_agent", END)

        return workflow
    
    def _build_followup_workflow(self) -> StateGraph:
        """Build separate workflow for follow-up question generation"""
        followup_workflow = StateGraph(AgentState)

        # Add only the follow-up question node
        followup_workflow.add_node("followup_question_agent", self._followup_question_node)
        
        # Set entry point and end
        followup_workflow.set_entry_point("followup_question_agent")
        followup_workflow.add_edge("followup_question_agent", END)

        return followup_workflow
    
    def _build_drillthrough_workflow(self) -> StateGraph:
        """Build separate workflow for drillthrough analysis"""
        drillthrough_workflow = StateGraph(AgentState)

        # Add only the drillthrough planner node
        drillthrough_workflow.add_node("drillthrough_planner_agent", self._drillthrough_planner_node)
        
        # Set entry point and end
        drillthrough_workflow.set_entry_point("drillthrough_planner_agent")
        drillthrough_workflow.add_edge("drillthrough_planner_agent", END)

        return drillthrough_workflow
    
    def _build_narrative_workflow(self) -> StateGraph:
        """Build separate workflow for narrative synthesis"""
        narrative_workflow = StateGraph(AgentState)

        # Add only the narrative agent node
        narrative_workflow.add_node("narrative_agent", self._narrative_agent_node)
        
        # Set entry point and end
        narrative_workflow.set_entry_point("narrative_agent")
        narrative_workflow.add_edge("narrative_agent", END)

        return narrative_workflow
    
    # ============ NODE IMPLEMENTATIONS ============

    async def _entry_router_node(self, state: AgentState) -> AgentState:
        """Entry router node to decide workflow entry point."""
        
        # Reset error messages
        state['nav_error_msg'] = None
        state['router_error_msg'] = None 
        state['follow_up_error_msg'] = None
        state['sql_gen_error_msg'] = None
        state['strategy_planner_err_msg'] = None
        state['user_friendly_message'] = None
        state['domain_selection']=state.get('domain_selection', [])
        
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
            return "router_agent"
        elif state.get("is_sql_followup", False):
            print("Routing entry to: END (SQL followup)")
            return "router_agent"
        else:
            print("Routing entry to: navigation_controller")
            return "navigation_controller"
    
    async def _navigation_controller_node(self, state: AgentState) -> AgentState:
        """Async Navigation Controller node"""
        
        cst = timezone('America/Chicago')
        navigation_start_time = datetime.now(cst).strftime('%Y-%m-%d %H:%M:%S %Z')
        print(f"Navigation Controller starting: {navigation_start_time}")
        
        # Store navigation start timestamp in state
        state['navigation_start_ts'] = navigation_start_time
        
        try:
            # Call navigation controller - ASYNC CALL
            nav_result = await self.nav_controller.process_user_query(state)
            # print('nav_result',nav_result) 
            # Update state with navigation results
            state['current_question'] = nav_result['rewritten_question']
            state['question_type'] = nav_result['question_type']
            state['next_agent'] = nav_result['next_agent']
            state['filter_values'] = nav_result['filter_values']
            state['user_friendly_message'] = nav_result.get('user_friendly_message', 'Considered as a new question')
            state['current_agent'] = 'Question Validator'
            state['nav_error_msg'] = nav_result.get('error_message')  
            state['topic_drift'] = False   
            state['missing_dataset_items'] = False
            state['sql_followup_topic_drift'] = False
            state['sql_followup_but_new_question'] = False
            state['matched_sql'] = None
            state['matched_table_name'] = None
            state['history_question_match'] = None
            state['history_sql_used'] = False
            state['report_found'] = False
            state['report_url'] = None
            state['report_filter'] = None
            state['report_name'] = None
            state['match_type'] = None
            state['report_reason'] = None
            state['sql_history_section']=None
            state['sql_generation_story']=None

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

            state['rewritten_question'] = nav_result['rewritten_question']
            state['greeting_response'] = None
            
            # Set next agent display based on question type
            question_type = nav_result['question_type']
            if question_type == "why":
                state['next_agent_disp'] = 'Drill through Analysis'
            else:
                state['next_agent_disp'] = 'SQL Generator'
            
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

    def _route_from_navigation(self, state: AgentState) -> str:
        """Route from navigation based on clarification needs, greetings, and question type"""
                
        requires_clarification = state.get('requires_domain_clarification', False)
        next_agent = state.get('next_agent', 'router_agent')
        domain_question = state.get('domain_followup_question', 'None')
        greeting_response = state.get('greeting_response')
        is_dml_ddl = state.get('is_dml_ddl', False)
        pending_business_question = state.get('pending_business_question', '')
        nav_error_msg = state.get('nav_error_msg')  # Get error message
        
        print(f"🔀 Navigation routing debug:")
        print(f"  - requires_domain_clarification: {requires_clarification}")
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

    async def _router_agent_node(self, state: AgentState) -> AgentState:
        """Router Agent: Dataset selection with dynamic interrupt and clarification support"""

        print(f"\n🎯 Router Agent: Selecting dataset")
        cst = timezone('America/Chicago')
        router_start_time = datetime.now(cst)  # Keep as aware datetime for duration calculation

        try:
            # Execute router agent - ASYNC CALL
            selection_result = await self.router_agent.select_dataset(state)
            # print('router output',selection_result)
            # ========================================
            # HANDLE SQL FOLLOW-UP VALIDATION FLAGS
            # ========================================
            
            # Check for topic drift
            if selection_result.get('sql_followup_topic_drift', False):
                print(f"⚠️ SQL follow-up topic drift detected")
                state['sql_followup_topic_drift'] = True
                state['sql_followup_but_new_question'] = False
                state['user_message'] = "You seem to be drifting from the follow-up question. Please start a new question."
                state['is_sql_followup'] = False  # Reset follow-up flag
                state['needs_followup'] = False
                state['sql_followup_question'] = None
                
                # Clear previous dataset selection state
                state['requires_dataset_clarification'] = False
                state['dataset_followup_question'] = None

                return state
            
            # Check for new question instead of answer
            if selection_result.get('sql_followup_but_new_question', False):
                print(f"🔄 SQL follow-up detected new question - routing back to navigation")
                state['sql_followup_topic_drift'] = False
                state['sql_followup_but_new_question'] = True
                state['next_agent'] = ['navigation_controller']
               
                
                # Prepare state for new question processing
                state['current_question'] = selection_result.get('detected_new_question', state.get('current_question', ''))
                state['is_sql_followup'] = False  # Reset follow-up flag
                state['needs_followup'] = False
                state['sql_followup_question'] = None
                
                # Clear previous dataset selection state
                state['requires_dataset_clarification'] = False
                state['dataset_followup_question'] = None
                
                return state
            
            # Reset flags if neither condition is met
            state['sql_followup_topic_drift'] = False
            state['sql_followup_but_new_question'] = False

            # ========================================
            # CONTINUE WITH EXISTING LOGIC
            # ========================================

            # 1. If error message exists, handle as before
            if selection_result.get('error'):
                state['router_error_msg'] = selection_result.get('error_message', '')
                return state
            
            # 2. Check if SQL follow-up is needed
            elif selection_result.get('needs_followup', False):
                print(f"❓ SQL follow-up needed - storing question")
                state['sql_followup_question'] = selection_result.get('sql_followup_question', '')
                state['needs_followup'] = True
                state['selected_dataset'] = selection_result.get('selected_dataset', [])
                state['dataset_metadata'] = selection_result.get('dataset_metadata', '')
                state['functional_names'] = selection_result.get('functional_names', [])
                # print('coming inside sql follow up',state)
                return state
            
            # 3. If requires_clarification is True, store follow-up info and return
            elif selection_result.get('requires_clarification', False) and not selection_result.get('topic_drift', False):
                print(f"❓ Dataset clarification needed - preparing follow-up question")
                state['candidate_actual_tables'] = selection_result.get('candidate_actual_tables', [])
                state['dataset_followup_question'] = selection_result.get('dataset_followup_question') or selection_result.get('clarification_question')
                state['functional_names'] = selection_result.get('functional_names', [])
                state['requires_dataset_clarification'] = True
                state['router_error_msg'] = selection_result.get('error_message', '')
                state['filter_metadata_results'] = selection_result.get('filter_metadata_results', '') 
                state['followup_reasoning'] = selection_result.get('selection_reasoning', '')
                return state
                
            elif selection_result.get('missing_dataset_items', False): 
                print(f"❓ dataset missing items")
                state['missing_dataset_items'] = True
                state['user_message'] = selection_result.get('user_message', '')
                return state
                
            elif selection_result.get('topic_drift', False):
                state['topic_drift'] = True
                state['requires_dataset_clarification'] = False
                return state
                
            elif selection_result.get('phi_found', False):
                print(f"❌ PHI/PII information detected")
                state['phi_found'] = True
                state['user_message'] = selection_result.get('user_message', '')
                
                # Remove the last record from user_question_history when PHI is found
                if 'user_question_history' in state and state['user_question_history'] and len(state['user_question_history']) > 0:
                    removed_question = state['user_question_history'].pop()
                    print(f"Removed PHI/PII question from history: '{removed_question}'")
                
                return state

            # 4. SQL generation completed successfully - process results
            else:
                print(f"✅ SQL generation and execution complete")
                
                # Store SQL results and dataset info
                state['sql_result'] = selection_result.get('sql_result', {})
                
                # Add user_question to sql_result
                state['sql_result']['user_question'] = state.get('rewritten_question', '')
                
                # Add title to sql_result if multiple_results is false
                if not state['sql_result'].get('multiple_results', False):
                    state['sql_result']['title'] = state.get('rewritten_question', '')
                
                # NEW: Extract and store SQL generation story
                state['sql_generation_story'] = selection_result.get('sql_result', {}).get('sql_story', '')
                if state['sql_generation_story']:
                    print(f"📖 SQL generation story captured: {state['sql_generation_story'][:100]}...")
                
                state['selected_dataset'] = selection_result.get('selected_dataset', [])
                state['dataset_metadata'] = selection_result.get('dataset_metadata', '')
                state['selection_reasoning'] = selection_result.get('selection_reasoning', '')
                state['functional_names'] = selection_result.get('functional_names', [])
                state['selected_filter_context'] = None
                state['filter_metadata_results'] = None
                state['filter_values'] = None
                state['followup_reasoning'] = None
                
                # Capture history_sql_used flag from router agent result
                state['history_sql_used'] = selection_result.get('history_sql_used', False)
                print(f"📋 History SQL used: {state['history_sql_used']}")

                # Reset flags
                state['dataset_followup_question'] = False
                state['needs_followup'] = False
                state['requires_dataset_clarification'] = False
                state['missing_dataset_items'] = False
                state['phi_found'] = False
                state['is_sql_followup'] = False
                state['dataset_metadata'] = None

                state['next_agent_disp'] = 'SQL Analysis Complete'
                
                cst = timezone('America/Chicago')
                router_end_time = datetime.now(cst)
                state['router_node_end_ts'] = router_end_time.strftime('%Y-%m-%d %H:%M:%S %Z')  # Format for storage
                router_duration = (router_end_time - router_start_time).total_seconds()  # Subtract aware datetimes
                print(f"⏱️ router duration: {router_duration:.3f} seconds")
                # print('router node output:', state)
                return state
            
        except Exception as e:
            print(f"  ❌ Router failed: {str(e)}")
            if 'errors' not in state:
                state['errors'] = []
            state['errors'].append(f"Router error: {str(e)}")
            return state
        
    def _route_from_router(self, state: AgentState) -> str:
        """Route from router_agent based on follow-up response validation"""
        
        sql_followup_topic_drift = state.get('sql_followup_topic_drift', False)
        sql_followup_new_question = state.get('sql_followup_but_new_question', False)
        
        print(f"🔀 Router routing debug:")
        print(f"  - sql_followup_topic_drift: {sql_followup_topic_drift}")
        print(f"  - sql_followup_new_question: {sql_followup_new_question}")
        
        # Priority 1: If new question detected, loop back to navigation
        if sql_followup_new_question:
            print(f"  🔄 Routing to: navigation_controller (new question detected)")
            print('route navigation state info', state)
            return "navigation_controller"
        
        # Priority 2: If topic drift or normal completion, end workflow
        if sql_followup_topic_drift:
            print(f"  🛑 Routing to: END (topic drift detected)")
        else:
            print(f"  ✅ Routing to: END (SQL generation complete)")
        
        return "END"

    # ============ HELPER METHODS ============

    def _update_narrative_history(self, state: AgentState, sql_result: Dict[str, Any]):
        """Update narrative_history with all summary responses from SQL results"""
        
        if not sql_result or not sql_result.get('success', False):
            print("No valid SQL result to extract summaries from")
            return
        
        # Initialize narrative history if it doesn't exist
        if 'narrative_history' not in state or state['narrative_history'] is None:
            state['narrative_history'] = []
        
        # Handle multiple SQL queries
        if sql_result.get('multiple_results', False):
            query_results = sql_result.get('query_results', [])
            for query in query_results:
                summary = query.get('summary', '')
                if summary and summary.strip():
                    # Store each summary as a separate string in the list
                    state['narrative_history'].append(summary.strip())
                    print(f"Added summary to history from: {query.get('title', 'Untitled Query')}")
        
        # Handle single SQL query
        else:
            # Use consistent summary field (narrative agent now stores as summary)
            summary = sql_result.get('summary', '')
            if summary and summary.strip():
                # Store the summary response
                state['narrative_history'].append(summary.strip())
                print(f"Added single summary to history")
        
        print(f"Total narrative history entries: {len(state['narrative_history'])}")


    async def _narrative_agent_node(self, state: AgentState) -> AgentState:
        """Narrative Agent: Generate narrative synthesis from SQL results"""
        
        print(f"\n📝 Narrative Agent: Generating narrative from SQL results")
        
        try:
            # Execute narrative synthesis - ASYNC CALL (includes Power BI matching in parallel)
            narrative_output = await self.narrative_agent.synthesize_narrative(state)
            
            if not narrative_output.get('success', False):
                state['narrative_error_msg'] = narrative_output.get('error', 'Narrative synthesis failed')
                state['current_agent'] = 'narrative_agent'
                state['report_found'] = False
                print(f"  ❌ Narrative synthesis failed: {narrative_output.get('error', 'Unknown error')}")
                return state
            else:
                state['current_agent'] = 'narrative_agent'
                state['narrative_error_msg'] = ''
                state['narrative_complete'] = True
                
                # Store Power BI matching results
                state['report_found'] = narrative_output.get('report_found', False)
                state['report_url'] = narrative_output.get('report_url')
                state['report_filter'] = narrative_output.get('report_filter')
                state['report_name'] = narrative_output.get('report_name')
                state['match_type'] = narrative_output.get('match_type')
                state['report_reason'] = narrative_output.get('report_reason')
                
                if state['report_found']:
                    print(f"  🎯 Power BI Report Found:")
                    print(f"     Report: {state['report_name']}")
                    print(f"     Match: {state['match_type']}")
                    print(f"     URL: {state['report_url']}")
                    print(f"     Filters: {state['report_filter']}")
                else:
                    print(f"  ℹ️ No Power BI report match found")
                
                # Process and store narrative history after successful synthesis
                sql_result = state.get('sql_result', {})
                self._update_narrative_history(state, sql_result)

                
                print(f"  ✅ Narrative synthesis completed successfully")
                
                # Store narrative end timestamp in CST
                cst = timezone('America/Chicago')
                narrative_end_time = datetime.now(cst).strftime('%Y-%m-%d %H:%M:%S %Z')
                state['narrative_end_ts'] = narrative_end_time
            # print('narrative output',state)    
            return state
                        
        except Exception as e:
            print(f"  ❌ Narrative agent failed: {str(e)}")
            if 'errors' not in state:
                state['errors'] = []
            state['errors'].append(f"Narrative agent error: {str(e)}")
            
            # Set error state
            state['narrative_error_msg'] = f"Narrative agent execution failed: {str(e)}"
            state['current_agent'] = 'narrative_agent'
        return state

    
    async def _followup_question_node(self, state: AgentState) -> AgentState:
        """Followup Question Agent: Generate intelligent follow-up questions"""
        
        print(f"\n❓ Followup Question Agent: Generating follow-up questions")
        
        try:
            
            # Execute followup question agent - ASYNC CALL
            followup_output = await self.followup_agent.generate_followup_question(state)
            print('follow-up output',followup_output)
            if followup_output.get('error_message'):
                # Clarification needed - set state and interrupt
                state['sql_gen_error_msg'] = followup_output.get('error_message','')
                return state
            else:

                # Update state with followup results
                state['current_agent'] = 'followup_question_agent'
                state['followup_questions'] = followup_output.get('followup_questions', [])
                state['followup_generation_success'] = followup_output.get('success', False)
                
                # Ensure follow_up_questions_history is initialized as a list
                if 'follow_up_questions_history' not in state or state['follow_up_questions_history'] is None:
                    state['follow_up_questions_history'] = []
                
                # Append the generated followup questions to history
                generated_questions = followup_output.get('followup_questions', [])
                if generated_questions:
                    # Add each followup question to history
                    for question in generated_questions:
                        state['follow_up_questions_history'].append(question)
                    
                    print(f"  ✅ Generated {len(generated_questions)} follow-up questions")
                    print(f"  📝 Follow-up questions: {generated_questions}")
                else:
                    print(f"  ⚠️ No follow-up questions generated")
                
                state['next_agent_disp'] = 'Workflow_Complete'
                
                print(f"  📊 Total follow-up questions in history: {len(state['follow_up_questions_history'])}")
                print(f"  🔍 State after followup generation: followup_questions={len(state.get('followup_questions', []))}")
                return state
                        
        except Exception as e:
            print(f"  ❌ Followup question generation failed: {str(e)}")
            if 'errors' not in state:
                state['errors'] = []
            state['errors'].append(f"Followup question error: {str(e)}")
            
            # Set fallback state
            state['current_agent'] = 'followup_question_agent'
            state['followup_questions'] = []
            state['followup_generation_success'] = False
            state['next_agent_disp'] = 'Root Cause Analysis'
            
            return state
        
    async def _strategy_planner_node(self, state: AgentState) -> AgentState:
    
        print(f"\n🎯 Strategic Planner Agent: Strategic investigation with parallel execution and synthesis")
        
        try:
            # Execute strategic investigation - ASYNC CALL
            strategic_output = await self.strategic_planner.execute_strategic_investigation(state)

            # --- Handle drill-through not available early exit ---
            if not strategic_output.get('success', False) and not strategic_output.get('can_map_to_drillthrough', True):
                state['drillthrough_exists'] = "No drill through knowledge available and please reach out to FDM team and meanwhile you can continue with what questions"
                state['strategy_planner_err_msg'] = strategic_output.get('error', '')
                state['current_agent'] = 'strategy_planner'
                return state

            if strategic_output.get('error'):
                # General error - set state and interrupt
                state['strategy_planner_err_msg'] = strategic_output.get('error', '')
                return state
            else:
                state['current_agent'] = 'strategy_planner'
                state['drillthrough_exists'] = "Available"
                state['strategy_planner_err_msg'] = ''
                state['next_agent_disp'] = 'Generating Drill through Pass 2 Questions'
                
                # Handle strategic results - Following SQL generator pattern exactly
                strategic_results = strategic_output.get('strategic_results', [])
                if strategic_results:

                    print(f"  ✅ Strategic queries executed: {strategic_output.get('total_queries', 0)}")
                    
                    # Store strategic query results - same pattern as multiple SQL results
                    state['multiple_strategic_results'] = True
                    state['strategic_query_results'] = strategic_results  # List of strategic query result objects
                    state['mapped_document'] = strategic_output.get('mapped_document',0) 
                    state['strategic_total_queries'] = strategic_output.get('total_queries', 0)
                    state['strategic_successful_queries'] = strategic_output.get('successful_queries', 0)
                    
                    # NEW: Extract all second_pass_targets from strategic results for next pass
                    first_pass_narrative_results = []
                    for i, result in enumerate(strategic_results, 1):
                        # Extract title and narrative from each result
                        title = result.get('title', f'Strategic Analysis {i}')
                        narrative = result.get('narrative', '')
                        
                        # Create structured entry for LLM consumption
                        if narrative and narrative.strip():
                            structured_entry = f"""Strategic Finding #{i}:
                    Title: {title}
                    Analysis: {narrative}"""
                            first_pass_narrative_results.append(structured_entry)

                    # Join all entries with section separators for LLM readability
                    state['first_pass_narrative_results'] = '\n\n' + '='*50 + '\n\n'.join(first_pass_narrative_results)
                    print(f"  📋 Extracted {len(first_pass_narrative_results)} strategic findings for next phase")
    
                    # For backward compatibility, set narrative_response to first query narrative if available
                    if strategic_results and len(strategic_results) > 0:
                        first_result = strategic_results[0]
                        state['strategic_sql_query'] = first_result.get('sql_query', '')
                        state['narrative_response'] = first_result.get('narrative', '')
                    else:
                        state['strategic_sql_query'] = ''
                        state['narrative_response'] = 'No strategic queries executed successfully'
                    
                    # Store all strategic SQL queries and narratives for history
                    all_strategic_queries = []
                    for result in strategic_results:
                        if result.get('sql_query'):
                            all_strategic_queries.append(result['sql_query'])
                    
                else:
                    print(f"  ⚠️ No strategic results returned")
                    
                    # Handle no results case - same pattern as single SQL result
                    state['multiple_strategic_results'] = False
                    state['strategic_sql_query'] = ''
                    state['strategic_query_results'] = []
                    state['narrative_response'] = 'No strategic analysis results available'
                    all_strategic_queries = []
            
            # Ensure strategic_analysis_history is initialized as a list - same pattern as questions_sql_history
            if 'strategic_analysis_history' not in state or state['strategic_analysis_history'] is None:
                state['strategic_analysis_history'] = []
            
            # Add current question and all strategic SQL queries to history - same pattern
            state['strategic_analysis_history'].append(state.get('current_question'))
            for strategic_query in all_strategic_queries:
                if strategic_query:
                    state['strategic_analysis_history'].append(strategic_query)
            
            return state
                        
        except Exception as e:
            print(f"  ❌ Strategic planner failed: {str(e)}")
            if 'errors' not in state:
                state['errors'] = []
            state['errors'].append(f"Strategic planner error: {str(e)}")
            
            # Set error state
            state['strategy_planner_err_msg'] = f"Strategic planner execution failed: {str(e)}"
            state['drillthrough_exists'] = "Error in processing"
        return state
    
    async def _drillthrough_planner_node(self, state: AgentState) -> AgentState:
    
        print(f"\n🎯 Drillthrough Planner Agent: Operational investigation with parallel execution and synthesis")
        
        try:
            # Execute drillthrough investigation - ASYNC CALL
            drillthrough_output = await self.drillthrough_planner.execute_drillthrough_investigation(state)

            # --- Handle drill-through config not available early exit ---
            if not drillthrough_output.get('success', False):
                state['drillthrough_planner_err_msg'] = drillthrough_output.get('error', '')
                state['current_agent'] = 'drillthrough_planner'
                return state

            if drillthrough_output.get('error'):
                # General error - set state and interrupt
                state['drillthrough_planner_err_msg'] = drillthrough_output.get('error', '')
                return state
            else:
                state['current_agent'] = 'drillthrough_planner'
                state['drillthrough_planner_err_msg'] = ''
                
                # Handle drillthrough results - Following strategic planner pattern exactly
                drillthrough_results = drillthrough_output.get('drillthrough_results', [])
                if drillthrough_results:
                    print(f"  ✅ Drillthrough queries executed: {drillthrough_output.get('total_queries', 0)}")
                    
                    # Store drillthrough query results - same pattern as strategic results
                    state['multiple_drillthrough_results'] = True
                    state['drillthrough_query_results'] = drillthrough_results  # List of drillthrough query result objects
                    
                    
                else:
                    print(f"  ⚠️ No drillthrough results returned")
                    
                    # Handle no results case - same pattern as strategic results
                    state['multiple_drillthrough_results'] = False
                    state['drillthrough_query_results'] = []
            
            return state
                        
        except Exception as e:
            print(f"  ❌ Drillthrough planner failed: {str(e)}")
            if 'errors' not in state:
                state['errors'] = []
            state['errors'].append(f"Drillthrough planner error: {str(e)}")
            
            # Set error state
            state['drillthrough_planner_err_msg'] = f"Drillthrough planner execution failed: {str(e)}"
            state['current_agent'] = 'drillthrough_planner'
        return state

    

    # ============ ASYNC STREAMING METHODS ============
    
    async def astream(self, initial_state: AgentState, config: Dict[str, Any]):
        """Async streaming method for the main workflow (up to router_agent)"""
        async for step_data in self.app.astream(initial_state, config=config):
            yield step_data
    
    async def astream_events(self, initial_state: AgentState, config: Dict[str, Any], version: str = "v2"):
        """Low-level event stream (node_start/node_end/log/error) to enable granular UI updates.

        This exposes LangGraph's event stream so the UI layer can implement Pattern 1
        (placeholder updates per node) without waiting for the full workflow.
        Safe to add alongside existing APIs; callers can opt in.
        """
        # The native astream_events is not working properly, so we'll use the fallback approach
        # that uses astream which provides reliable node completion events
        print("🔧 Using fallback astream approach for reliable events")
        
        async for step in self.app.astream(initial_state, config=config):
            print(f"📦 Astream step: {list(step.keys())}")
            for node_name, node_state in step.items():
                if node_name == '__end__':
                    # print(f"🏁 Workflow end - final state keys: {list(node_state.keys()) if isinstance(node_state, dict) else 'Not a dict'}")
                    yield {"type": "workflow_end", "name": node_name, "data": node_state}
                else:
                    # print(f"✅ Node completed: {node_name} - state keys: {list(node_state.keys()) if isinstance(node_state, dict) else 'Not a dict'}")
                    yield {"type": "node_end", "name": node_name, "data": node_state}
                    
    async def astream_followup(self, state_after_main: AgentState, config: Dict[str, Any]):
        """Async streaming method for the follow-up workflow"""
        async for step_data in self.followup_app.astream(state_after_main, config=config):
            yield step_data
    
    async def astream_drillthrough(self, state_after_strategic: AgentState, config: Dict[str, Any]):
        """Async streaming method for the drillthrough workflow"""
        async for step_data in self.drillthrough_app.astream(state_after_strategic, config=config):
            yield step_data
    
    async def ainvoke(self, initial_state: AgentState, config: Dict[str, Any]) -> AgentState:
        """Async invoke method for the main workflow"""
        return await self.app.ainvoke(initial_state, config=config)
    
    async def ainvoke_followup(self, state_after_main: AgentState, config: Dict[str, Any]) -> AgentState:
        """Async invoke method for the follow-up workflow"""
        return await self.followup_app.ainvoke(state_after_main, config=config)
    
    async def ainvoke_drillthrough(self, state_after_strategic: AgentState, config: Dict[str, Any]) -> AgentState:
        """Async invoke method for the drillthrough workflow"""
        return await self.drillthrough_app.ainvoke(state_after_strategic, config=config)
    
    async def astream_narrative(self, state_with_sql: AgentState, config: Dict[str, Any]):
        """Async streaming method for the narrative workflow"""
        async for step_data in self.narrative_app.astream(state_with_sql, config=config):
            yield step_data
    
    async def ainvoke_narrative(self, state_with_sql: AgentState, config: Dict[str, Any]) -> AgentState:
        """Async invoke method for the narrative workflow"""
        return await self.narrative_app.ainvoke(state_with_sql, config=config)


class HealthcareFinanceWorkflow:
    """Synchronous wrapper for AsyncHealthcareFinanceWorkflow for testing purposes"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.async_workflow = AsyncHealthcareFinanceWorkflow(databricks_client)
    
    def run_workflow(self, user_question: str, session_id: str, user_id: str) -> Dict[str, Any]:
        """Synchronous wrapper to run the async workflow"""
        
        # Create initial state
        initial_state = AgentState(
            user_question=user_question,
            session_id=session_id,
            user_id=user_id,
            current_agent='entry_router',
            timestamp=datetime.now().isoformat(),
            # Initialize all required fields
            question_type=None,
            current_question=user_question,
            next_agent=[],
            next_agent_disp='Starting workflow',
            rewritten_question='',
            domain_selection='PBM Network',
            selected_dataset=[],
            dataset_metadata='',
            selection_reasoning='',
            functional_names=[],
            user_question_history=[],
            questions_sql_history=[],
            # Error handling fields
            nav_error_msg=None,
            router_error_msg=None,
            follow_up_error_msg=None,
            sql_gen_error_msg=None,
            errors=[],
            # Clarification fields
            requires_domain_clarification=False,
            domain_followup_question=None,
            requires_dataset_clarification=False,
            dataset_followup_question=None,
            candidate_actual_tables=[],
            # Follow-up fields
            is_sql_followup=False,
            sql_followup_question=None,
            needs_followup=False,
            # Status fields
            topic_drift=False,
            missing_dataset_items=False,
            phi_found=False,
            user_message=None,
            # Response fields
            greeting_response=None,
            is_dml_ddl=False,
            pending_business_question='',
            # SQL result fields
            sql_result={},
            # Narrative history tracking
            narrative_history=[],
            # Tracking fields
            llm_retry_count=0,
            # Strategic planner fields
            strategy_planner_err_msg=None,
            drillthrough_exists=None,
            multiple_strategic_results=False,
            strategic_query_results=[],
            mapped_document=0,
            strategic_total_queries=0,
            strategic_successful_queries=0,
            first_pass_narrative_results='',
            strategic_sql_query='',
            strategic_analysis_history=[],
            # Drillthrough planner fields
            drillthrough_planner_err_msg=None,
            multiple_drillthrough_results=False,
            drillthrough_query_results=[],
            # Follow-up fields initialization
            followup_questions=[],
            followup_generation_success=False,
            follow_up_questions_history=[],
            narrative_response=None,
            # History tracking
            history_sql_used=False
        )
        
        # Create config with session thread
        config = {
            "configurable": {
                "thread_id": session_id
            }
        }
        
        try:
            # Run the async workflow synchronously
            final_state = asyncio.run(
                self.async_workflow.ainvoke(initial_state, config)
            )
            
            return {
                'success': True,
                'final_state': final_state,
                'error': None
            }
            
        except Exception as e:
            return {
                'success': False,
                'final_state': None,
                'error': str(e)
            }


if __name__ == "__main__":
    from core.databricks_client import DatabricksClient
    
    db_client = DatabricksClient()
    workflow = HealthcareFinanceWorkflow(db_client)
    
    # Test questions
    test_questions = [
        "what is the revenue for august 2025"
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
            print(f"  - Next Agent: {final_state.get('next_agent')}")
            print(f"  - Navigation Error: {final_state.get('nav_error_msg')}")
            print(f"  - Router Error: {final_state.get('router_error_msg')}")
            print(f"  - Domain Clarification: {final_state.get('requires_domain_clarification')}")
            print(f"  - Dataset Clarification: {final_state.get('requires_dataset_clarification')}")
            print(f"  - SQL Follow-up: {final_state.get('needs_followup')}")
            print(f"  - Topic Drift: {final_state.get('topic_drift')}")
            print(f"  - Missing Items: {final_state.get('missing_dataset_items')}")
            
            # Show SQL results if available
            sql_result = final_state.get('sql_result')
            if sql_result and sql_result.get('success'):
                print(f"\n🔍 SQL RESULTS:")
                if sql_result.get('multiple_results'):
                    query_results = sql_result.get('query_results', [])
                    for i, query in enumerate(query_results, 1):
                        print(f"  Query {i}: {query.get('title', 'Untitled')}")
                        print(f"    SQL: {query.get('sql_query', 'N/A')[:100]}...")
                        if query.get('data'):
                            print(f"    Rows: {len(query['data'])}")
                else:
                    print(f"    SQL: {sql_result.get('sql_query', 'N/A')[:100]}...")
                    if sql_result.get('data'):
                        print(f"    Rows: {len(sql_result['data'])}")
            
            # Show follow-up questions if any
            if final_state.get('domain_followup_question'):
                print(f"\n❓ DOMAIN FOLLOW-UP: {final_state.get('domain_followup_question')}")
            if final_state.get('dataset_followup_question'):
                print(f"\n❓ DATASET FOLLOW-UP: {final_state.get('dataset_followup_question')}")
            if final_state.get('sql_followup_question'):
                print(f"\n❓ SQL FOLLOW-UP: {final_state.get('sql_followup_question')}")
                
        else:
            print(f"❌ Failed: {result['error']}")
            
    print("\n" + "="*80)
    print("✅ Workflow testing complete!")
