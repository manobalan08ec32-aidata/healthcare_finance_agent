import asyncio
import time
from datetime import datetime
from typing import Dict, Any, List
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from agents.llm_navigation_controller import LLMNavigationController
from agents.llm_router_agent import LLMRouterAgent
from agents.followup_question import FollowupQuestionAgent

class AsyncHealthcareFinanceWorkflow:
    """Simplified async LangGraph workflow focusing on navigation controller"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
        # Initialize navigation controller and router agent
        self.nav_controller = LLMNavigationController(databricks_client)
        self.router_agent = LLMRouterAgent(databricks_client)
        self.followup_agent = FollowupQuestionAgent(databricks_client)

        # Build the main workflow (ends at router_agent)
        self.workflow = self._build_workflow()
        
        # Build the separate follow-up workflow
        self.followup_workflow = self._build_followup_workflow()
        
        # Compile both workflows with checkpointer
        checkpointer = MemorySaver()
        self.app = self.workflow.compile(checkpointer=checkpointer)
        self.followup_app = self.followup_workflow.compile(checkpointer=checkpointer)
    
    def _build_workflow(self) -> StateGraph:
        """Build workflow with entry router, navigation controller and router agent"""
        workflow = StateGraph(AgentState)

        # Add nodes
        workflow.add_node("entry_router", self._entry_router_node)
        workflow.add_node("navigation_controller", self._navigation_controller_node)
        workflow.add_node("router_agent", self._router_agent_node)
        workflow.add_node("followup_question_agent", self._followup_question_node)
        

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

        # Routing from navigation_controller
        workflow.add_conditional_edges(
            "navigation_controller",
            self._route_from_navigation,
            {
                "router_agent": "router_agent",
                "root_cause_agent": END,
                "END": END
            }
        )

        # Router agent ends here for main workflow - follow-up questions handled separately
        workflow.add_edge("router_agent", END)

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
            return "router_agent"
        elif state.get("is_sql_followup", False):
            print("Routing entry to: END (SQL followup)")
            return "router_agent"
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
        router_start_time = datetime.now().isoformat()

        try:
            # Execute router agent - ASYNC CALL
            selection_result = await self.router_agent.select_dataset(state)
            
            # 1. If error message exists, handle as before
            if selection_result.get('error'):
                state['router_error_msg'] = selection_result.get('error_message', '')
                return state
            
            # 2. Check if SQL follow-up is needed
            elif selection_result.get('needs_followup', False):
                print(f"❓ SQL follow-up needed - storing question")
                state['sql_followup_question'] = selection_result.get('sql_followup_question', '')
                state['selection_reasoning'] = selection_result.get('selection_reasoning', '')
                state['needs_followup'] = True
                state['selected_dataset'] = selection_result.get('selected_dataset', [])
                state['dataset_metadata'] = selection_result.get('dataset_metadata', '')
                state['functional_names'] = selection_result.get('functional_names', [])
                return state
            
            # 3. If requires_clarification is True, store follow-up info and return
            elif selection_result.get('requires_clarification', False) and not selection_result.get('topic_drift', False):
                print(f"❓ Dataset clarification needed - preparing follow-up question")
                state['candidate_actual_tables'] = selection_result.get('candidate_actual_tables', [])
                state['dataset_followup_question'] = selection_result.get('dataset_followup_question') or selection_result.get('clarification_question')
                state['functional_names'] = selection_result.get('functional_names', [])
                state['requires_dataset_clarification'] = True
                # Optionally, store error_message if present
                state['router_error_msg'] = selection_result.get('error_message', '')
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

            # 4. SQL generation completed successfully - process results
            else:
                print(f"✅ SQL generation and execution complete")
                
                # Store SQL results and dataset info
                state['sql_result'] = selection_result.get('sql_result', {})
                state['selected_dataset'] = selection_result.get('selected_dataset', [])
                state['dataset_metadata'] = selection_result.get('dataset_metadata', '')
                state['selection_reasoning'] = selection_result.get('selection_reasoning', '')
                state['functional_names'] = selection_result.get('functional_names', [])
                
                # Reset flags
                state['dataset_followup_question'] = False
                state['needs_followup'] = False
                state['requires_dataset_clarification'] = False
                state['missing_dataset_items'] = False
                
                # Process and store SQL history
                sql_result = selection_result.get('sql_result', {})
                self._update_sql_history(state, sql_result)
                
                state['next_agent_disp'] = 'SQL Analysis Complete'
                
                router_end_time = datetime.now().isoformat()
                router_duration = (datetime.fromisoformat(router_end_time) - datetime.fromisoformat(router_start_time)).total_seconds()
                print(f"⏱️ router duration: {router_duration:.3f} seconds")
                return state
            
        except Exception as e:
            print(f"  ❌ Router failed: {str(e)}")
            if 'errors' not in state:
                state['errors'] = []
            state['errors'].append(f"Router error: {str(e)}")
            return state

    # ============ HELPER METHODS ============
    
    def _update_sql_history(self, state: AgentState, sql_result: Dict[str, Any]):
        """Update questions_sql_history with segregated titles and SQLs"""
        
        if not sql_result or not sql_result.get('success', False):
            print("No valid SQL result to add to history")
            return
        
        # Initialize history if it doesn't exist
        if 'questions_sql_history' not in state or state['questions_sql_history'] is None:
            state['questions_sql_history'] = []
        
        # Handle multiple SQL queries
        if sql_result.get('multiple_results', False):
            query_results = sql_result.get('query_results', [])
            for query in query_results:
                title = query.get('title', 'Untitled Query')
                sql_query = query.get('sql_query', '')
                if sql_query.strip():
                    # Concatenate title and SQL
                    history_entry = f"Title: {title}\nSQL: {sql_query}"
                    state['questions_sql_history'].append(history_entry)
                    print(f"Added to SQL history: {title}")
        
        # Handle single SQL query
        else:
            sql_query = sql_result.get('sql_query', '')
            if sql_query.strip():
                # For single queries, create a generic title or use available info
                title = "Single Query Analysis"
                history_entry = f"Title: {title}\nSQL: {sql_query}"
                state['questions_sql_history'].append(history_entry)
                print(f"Added to SQL history: {title}")
        
        print(f"Total SQL history entries: {len(state['questions_sql_history'])}")

    
    async def _followup_question_node(self, state: AgentState) -> AgentState:
        """Followup Question Agent: Generate intelligent follow-up questions"""
        
        print(f"\n❓ Followup Question Agent: Generating follow-up questions")
        
        try:
            # Add 30-second delay to test if SQL output renders first
            print("⏱️ Waiting 30 seconds before generating follow-up questions...")
            await asyncio.sleep(10)
            print("✅ Wait complete, proceeding with follow-up question generation")
            
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
        if not hasattr(self.app, 'astream_events'):
            # Fallback: synthesize simple node_end events from astream
            async for step in self.app.astream(initial_state, config=config):
                for node_name, node_state in step.items():
                    if node_name == '__end__':
                        yield {"type": "workflow_end", "name": node_name, "data": node_state}
                    else:
                        yield {"type": "node_end", "name": node_name, "data": node_state}
            return

        # Native event streaming: normalize to simple schema {type: node_end|workflow_end, name, data}
        async for raw in self.app.astream_events(initial_state, config=config, version=version):
            rtype = raw.get('type', '')
            data = raw.get('data', {}) or {}

            # Attempt to extract node name & state regardless of native shape
            node_name = raw.get('name') or data.get('name') or data.get('node_name')
            state = data.get('state') if isinstance(data, dict) else None

            # Some LangGraph versions emit per-node state under data['state'], others inline
            if state is None and isinstance(raw.get('state'), dict):
                state = raw.get('state')
            if state is None and isinstance(data, dict):
                # Heuristic: treat entire data as state if it contains known fields
                if any(k in data for k in ("nav_error_msg", "router_error_msg", "sql_result", "followup_questions")):
                    state = data

            # Normalize known event kinds
            if rtype in ("on_node_end", "node_end") and node_name:
                yield {"type": "node_end", "name": node_name, "data": state or {}}
            elif rtype in ("on_graph_end", "workflow_end"):
                # Final aggregated state may live in data['state'] or raw['state']
                final_state = state or {}
                yield {"type": "workflow_end", "name": node_name or '__end__', "data": final_state}
            else:
                # For debugging: surface unexpected events as logs (can be filtered out upstream)
                if rtype.startswith("on_") and node_name and state is not None:
                    # Treat other on_node_* termination events conservatively as node_end if they include state
                    if 'end' in rtype:
                        yield {"type": "node_end", "name": node_name, "data": state}
                    # Otherwise skip (start/progress events not needed for current UI)
                continue
    async def astream_followup(self, state_after_main: AgentState, config: Dict[str, Any]):
        """Async streaming method for the follow-up workflow"""
        async for step_data in self.followup_app.astream(state_after_main, config=config):
            yield step_data
    
    async def ainvoke(self, initial_state: AgentState, config: Dict[str, Any]) -> AgentState:
        """Async invoke method for the main workflow"""
        return await self.app.ainvoke(initial_state, config=config)
    
    async def ainvoke_followup(self, state_after_main: AgentState, config: Dict[str, Any]) -> AgentState:
        """Async invoke method for the follow-up workflow"""
        return await self.followup_app.ainvoke(state_after_main, config=config)


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
            domain_selection=None,
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
            user_message=None,
            # Response fields
            greeting_response=None,
            is_dml_ddl=False,
            pending_business_question='',
            # SQL result fields
            sql_result={},
            # Tracking fields
            llm_retry_count=0
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
        "for PBM, What is the total ingredient fee for the month of july 2025?"
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
