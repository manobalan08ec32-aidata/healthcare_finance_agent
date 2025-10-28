async def run_streaming_workflow_async(workflow, user_query: str):
    """Stream node events and render incrementally; then run follow-up generation."""
    session_id = st.session_state.session_id
    user_questions_history = [m['content'] for m in st.session_state.messages if m.get('role') == 'user']
    
    # Get domain selection from session state (inherited from main page) and normalize to string
    raw_domain_selection = st.session_state.get('domain_selection', None)
    if isinstance(raw_domain_selection, list):
        print(f"⚠️ domain_selection came as list {raw_domain_selection}; normalizing to first item")
        domain_selection = raw_domain_selection[0] if raw_domain_selection else None
    else:
        domain_selection = raw_domain_selection
    print(f"🔍 Using domain selection (string) in chat workflow: {domain_selection} (type={type(domain_selection).__name__})")
    print(f"🔍 Session state domain_selection raw: {raw_domain_selection}")
    print(f"🔍 Current session_id: {session_id}")
    
    # Check if domain_selection is valid before proceeding
    if not domain_selection or domain_selection.strip() == '':
        st.error("⚠️ Please select a domain from the main page before asking questions.")
        st.info("👉 Go back to the main page and select either 'PBM Network' or 'Optum Pharmacy' domain first.")
        return
    
    initial_state = {
        'original_question': user_query,
        'current_question': user_query,
        'session_id': session_id,
        'user_id': get_authenticated_user(),  # Use authenticated user instead of session-based ID
        'user_questions_history': user_questions_history,
    'domain_selection': domain_selection,  # Pass normalized domain selection to workflow
        'errors': [],
        'session_context': {
            'app_instance_id': session_id,
            'execution_timestamp': datetime.now().isoformat(),
            'conversation_turn': len(user_questions_history)
        }
    }
    config = {"configurable": {"thread_id": session_id}}

    last_state = None
    followup_placeholder = st.empty()
    node_placeholders = {}
    status_region = st.container()

    async def show_progressive_router_status(placeholder, state):
                    """Show progressive status updates for router_agent node"""
                    try:
                        # Initial message (already shown at line 184)
                        await asyncio.sleep(4)  # Wait 4 seconds
                        
                        # Check if we're still processing (placeholder exists)
                        if placeholder and 'router_agent' in node_placeholders:
                            placeholder.info("⚙️ Generating SQL query based on metadata...")
                        
                        await asyncio.sleep(2)  # Wait 2 more seconds
                        
                        # Check again before showing next message
                        if placeholder and 'router_agent' in node_placeholders:
                            placeholder.info("🔄 Executing SQL query...")
                    except Exception as e:
                        print(f"⚠️ Progressive status update error: {e}")
                
    try:
        async for ev in workflow.astream_events(initial_state, config=config):
            et = ev.get('type')
            name = ev.get('name')
            state = ev.get('data', {}) or {}
            
            # Debug logging
            print(f"🎭 UI Event: type={et}, name={name}, state_keys={list(state.keys()) if isinstance(state, dict) else 'None'}")
            
            # Create placeholder when we first see a node (not for workflow end markers)
            if name not in ('__end__', 'workflow_end') and name and name not in node_placeholders:
                node_placeholders[name] = status_region.empty()

                # User-friendly messages for different nodes
                if name == 'navigation_controller':
                    node_placeholders[name].info("🔍 Validating your question...")
                elif name == 'router_agent':
                    node_placeholders[name].info("🎯 Finding the right dataset for your question...")
                    # Start progressive status updates in background
                    asyncio.create_task(show_progressive_router_status(node_placeholders[name], state))
                elif name == 'strategy_planner_agent':
                    node_placeholders[name].info("🧠 Planning strategic analysis...")
                elif name == 'drillthrough_planner_agent':
                    node_placeholders[name].info("🔧 Preparing drillthrough analysis...")
                else:
                    node_placeholders[name].info(f"▶️ {name} running...")
            
            # Handle node completion events
            if et in ('node_end', 'workflow_end'):
                # Only show completion for specific nodes we want to track
                if name in node_placeholders:
                    if name in ['router_agent']:  # Only show completion for important nodes
                        node_placeholders[name].success(f"✅ {name} done")
                    else:
                        # Just clear the placeholder without showing "done" message
                        node_placeholders[name].empty()
                
                # Handle specific node outputs based on actual state content
                if name == 'entry_router':
                    print(f"✅ Entry router completed - routing to next node")
                
                elif name == 'navigation_controller':
                    print(f"🧭 Navigation controller completed - checking outputs...")
                    nav_err = state.get('nav_error_msg')
                    greeting = state.get('greeting_response')
                    user_friendly_msg = state.get('user_friendly_message')

                    if user_friendly_msg:
                        # Show temporarily using a status placeholder, don't add to messages
                        if 'navigation_controller' in node_placeholders:
                            node_placeholders['navigation_controller'].info(f"💬 {user_friendly_msg}")
                        print(f"💬 Displayed user-friendly message (not persisted): {user_friendly_msg}")
                    
                    print(f"   nav_error_msg: {nav_err}")
                    print(f"   greeting_response: {greeting}")
                    
                    if nav_err:
                        # Clear all placeholders silently and show error
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(nav_err, message_type="error")
                        return
                    if greeting:
                        # Clear all placeholders silently and show greeting
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(greeting, message_type="greeting")
                        return
                
                elif name == 'router_agent':
                    print(f"🎯 Router agent completed - checking outputs... ")
                    if state.get('sql_followup_but_new_question', False):
                        print("🔄 New question detected - workflow will continue to navigation_controller")
                        # Clear placeholders but don't break/return - let stream continue to next node
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        # Add a temporary status message
                        st.info("🔄 Processing your new question...")
                        # Don't break or return - just continue to let the stream proceed
                        continue
                    if state.get('router_error_msg'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('router_error_msg'), message_type="error")
                        return
                    if state.get('needs_followup'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('sql_followup_question'), message_type="needs_followup")
                        return
                    if state.get('requires_dataset_clarification') and state.get('dataset_followup_question'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('dataset_followup_question'), message_type="dataset_clarification")
                        return
                    if state.get('missing_dataset_items') and state.get('user_message'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('user_message'), message_type="missing_items")
                        return
                    if state.get('phi_found') and state.get('user_message'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('user_message'), message_type="phi_pii_error")
                        return
                    if state.get('sql_followup_topic_drift') and state.get('user_message'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('user_message'), message_type="error")
                        return
                    
                    sql_result = state.get('sql_result', {})
                    if sql_result and sql_result.get('success'):
                        # Clear node progress indicators
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        
                        # Display selection_reasoning after AI rewritten question if available
                        functional_names = state.get('functional_names', [])
                        if functional_names:
                            add_selection_reasoning_message(functional_names)
                            print(f"✅ Added functional_names display: {functional_names}")
                        
                        # Render SQL results immediately
                        rewritten_question = state.get('rewritten_question', initial_state['current_question'])
                        table_name = state.get('selected_dataset', None)
                        add_sql_result_message(sql_result, rewritten_question, table_name)
                        last_state = state
                        
                        # Mark that SQL results are ready and we need to start narrative generation
                        st.session_state.sql_rendered = True
                        st.session_state.narrative_state = state
                        st.session_state.followup_state = state
                        
                        print("🔄 SQL results rendered - triggering UI update to show results before narrative generation")
                        # Force UI to update by breaking and using session state to continue narrative and follow-up later
                        break
                    else:
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message("No results available or SQL execution failed.", message_type="error")
                        return
                
                elif name == 'strategy_planner_agent':
                    print(f"🧠 Strategic planner completed - checking outputs...")
                    
                    # Check for errors first
                    if state.get('strategy_planner_err_msg'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('strategy_planner_err_msg'), message_type="error")
                        return
                    
                    # Check for strategic results
                    strategic_results = state.get('strategic_query_results', [])
                    if strategic_results:
                        # Clear node progress indicators
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        
                        # Render strategic analysis immediately
                        strategic_reasoning = state.get('strategic_reasoning', '')
                        add_strategic_analysis_message(strategic_results, strategic_reasoning)
                        last_state = state
                        
                        print("🔄 Strategic analysis rendered - checking for drillthrough availability")
                        
                        # Check if drillthrough exists and should be executed
                        drillthrough_exists = state.get('drillthrough_exists', '')
                        if drillthrough_exists == "Available":
                            # Mark strategic analysis as rendered and prepare for drillthrough
                            st.session_state.strategic_rendered = True
                            st.session_state.drillthrough_state = state
                            
                            print("🔄 Strategic analysis complete - drillthrough will be executed separately")
                            # Force UI to update by breaking and using session state to continue drillthrough later
                            break
                        else:
                            # No drillthrough available - this is the end, prepare for follow-ups
                            st.session_state.sql_rendered = True
                            st.session_state.followup_state = state
                            break
                    else:
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message("No strategic analysis results available.", message_type="error")
                        return
                
                elif name == 'drillthrough_planner_agent':
                    print(f"🔧 Drillthrough planner completed - checking outputs...")
                    
                    # Check for errors first
                    if state.get('drillthrough_planner_err_msg'):
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message(state.get('drillthrough_planner_err_msg'), message_type="error")
                        return
                    
                    # Check for drillthrough results
                    drillthrough_results = state.get('drillthrough_query_results', [])
                    if drillthrough_results:
                        # Clear node progress indicators
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        
                        # Render drillthrough analysis immediately
                        drillthrough_reasoning = state.get('drillthrough_reasoning', '')
                        add_drillthrough_analysis_message(drillthrough_results, drillthrough_reasoning)
                        last_state = state
                        
                        print("🔄 Drillthrough analysis rendered - workflow complete")
                        break
                    else:
                        for placeholder in node_placeholders.values():
                            placeholder.empty()
                        add_assistant_message("No drillthrough analysis results available.", message_type="error")
                        return
                
                # Capture final state for follow-up generation
                if et == 'workflow_end':
                    print(f"🏁 Workflow completed - final state captured")
                    # Clear remaining placeholders when workflow ends
                    for placeholder in node_placeholders.values():
                        placeholder.empty()
                    if not last_state:
                        last_state = state
        # Handle follow-up generation based on where we broke out
        if st.session_state.get('sql_rendered', False):
            print("🔄 SQL was rendered - follow-up will be handled in next execution cycle")
            # Don't generate follow-ups here - let the main loop handle it after UI updat
        elif last_state and last_state.get('sql_result', {}).get('success'):
            # This handles the case where workflow completed normally without early break
            try:
                print("🔄 Starting follow-up generation after complete workflow...")
                followup_state = last_state
                follow_res = await workflow.ainvoke_followup(followup_state, config)
                fq = follow_res.get('followup_questions', [])
                if fq:
                    st.session_state.current_followup_questions = fq
                    followup_placeholder.empty()
                    add_assistant_message("💡 **Would you like to explore further? Here are some suggested follow-up questions:**", message_type="followup_questions")
                    print(f"✅ Generated {len(fq)} follow-up questions")
                else:
                    followup_placeholder.empty()
                    print("ℹ️ No follow-up questions generated")
            except Exception as fe:
                followup_placeholder.error(f"Follow-up generation failed: {fe}")
                print(f"❌ Follow-up generation error: {fe}")
        elif last_state:
            print("ℹ️ No SQL results to generate follow-ups for")
        else:
            print("⚠️ No final state captured from workflow")
            
    except Exception as e:
        add_assistant_message(f"Workflow failed: {e}", message_type="error")
        print(f"❌ Workflow error: {e}")
