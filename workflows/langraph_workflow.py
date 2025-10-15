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
        return "navigation_controller"
    
    # Priority 2: If topic drift or normal completion, end workflow
    if sql_followup_topic_drift:
        print(f"  🛑 Routing to: END (topic drift detected)")
    else:
        print(f"  ✅ Routing to: END (SQL generation complete)")
    
    return "END"

async def _router_agent_node(self, state: AgentState) -> AgentState:
    """Router Agent: Dataset selection with dynamic interrupt and clarification support"""

    print(f"\n🎯 Router Agent: Selecting dataset")
    router_start_time = datetime.now().isoformat()

    try:
        # Execute router agent - ASYNC CALL
        selection_result = await self.router_agent.select_dataset(state)
        
        # ========================================
        # HANDLE SQL FOLLOW-UP VALIDATION FLAGS
        # ========================================
        
        # Check for topic drift
        if selection_result.get('sql_followup_topic_drift', False):
            print(f"⚠️ SQL follow-up topic drift detected")
            state['sql_followup_topic_drift'] = True
            state['sql_followup_but_new_question'] = False
            state['state_message'] = (
                "You seem to be drifting from the follow-up question. "
                "Please rephrase your response to address the clarification requested, "
                "or start with a new question from scratch."
            )
            state['original_followup_question'] = selection_result.get('original_followup_question', '')
            return state
        
        # Check for new question instead of answer
        if selection_result.get('sql_followup_but_new_question', False):
            print(f"🔄 SQL follow-up detected new question - routing back to navigation")
            state['sql_followup_topic_drift'] = False
            state['sql_followup_but_new_question'] = True
            state['state_message'] = (
                "You've asked a new question instead of providing clarification. "
                "Processing your new question now..."
            )
            
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
            
            state['selected_dataset'] = selection_result.get('selected_dataset', [])
            state['dataset_metadata'] = selection_result.get('dataset_metadata', '')
            state['selection_reasoning'] = selection_result.get('selection_reasoning', '')
            state['functional_names'] = selection_result.get('functional_names', [])
            state['selected_filter_context'] = None
            state['filter_metadata_results'] = None
            state['filter_values'] = None
            state['followup_reasoning'] = None

            # Reset flags
            state['dataset_followup_question'] = False
            state['needs_followup'] = False
            state['requires_dataset_clarification'] = False
            state['missing_dataset_items'] = False
            state['phi_found'] = False
            state['is_sql_followup'] = False
            state['dataset_metadata'] = None

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
```

## Summary of Changes:

✅ **Workflow structure**: Changed router_agent edge from direct to conditional  
✅ **New routing function**: `_route_from_router()` handles three scenarios:
   - New question → Loop back to `navigation_controller`
   - Topic drift → END with message
   - Normal completion → END  
✅ **Router node updated**: Checks flags first, sets appropriate state messages  
✅ **State flags added**: `sql_followup_topic_drift`, `sql_followup_but_new_question`, `state_message`  
✅ **Circular flow enabled**: New questions automatically restart from navigation  

**Flow visualization:**
```
Topic Drift: router_agent → END (with message)
New Question: router_agent → navigation_controller → router_agent (loop)
Success: router_agent → END (SQL complete)
