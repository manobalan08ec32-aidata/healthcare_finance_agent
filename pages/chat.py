async def run_streaming_workflow_async(workflow, user_query: str):
    """Stream node events and render incrementally; then run follow-up generation."""
    session_id = st.session_state.session_id
    user_questions_history = [m['content'] for m in st.session_state.messages if m.get('role') == 'user']
    
    # Get domain selection from session state
    raw_domain_selection = st.session_state.get('domain_selection', None)
    if isinstance(raw_domain_selection, list):
        print(f"⚠️ domain_selection came as list {raw_domain_selection}; normalizing to first item")
        domain_selection = raw_domain_selection[0] if raw_domain_selection else None
    else:
        domain_selection = raw_domain_selection
    print(f"🔍 Using domain selection (string) in chat workflow: {domain_selection} (type={type(domain_selection).__name__})")
    
    if not domain_selection or domain_selection.strip() == '':
        st.error("⚠️ Please select a domain from the main page before asking questions.")
        st.info("👉 Go back to the main page and select either 'PBM Network' or 'Optum Pharmacy' domain first.")
        return
    
    initial_state = {
        'original_question': user_query,
        'current_question': user_query,
        'session_id': session_id,
        'user_id': get_authenticated_user(),
        'user_questions_history': user_questions_history,
        'domain_selection': domain_selection,
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
    
    # Single status display and spinner
    status_display = st.empty()
    spinner_placeholder = st.empty()
    
    # Track which node is currently being shown in status
    current_status_node = None

    def show_spinner():
        """Show a persistent spinner indicator"""
        spinner_placeholder.markdown("""
            <div style="display: flex; align-items: center; padding: 8px 12px; background-color: #f0f8ff; border-radius: 6px; margin-bottom: 10px;">
                <div class="spinner" style="
                    border: 3px solid #f3f3f3;
                    border-top: 3px solid #3498db;
                    border-radius: 50%;
                    width: 20px;
                    height: 20px;
                    animation: spin 1s linear infinite;
                    margin-right: 10px;
                "></div>
                <span style="color: #2c3e50; font-weight: 500;">Processing your request...</span>
            </div>
            <style>
                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }
            </style>
        """, unsafe_allow_html=True)
    
    def hide_spinner():
        """Hide the spinner indicator"""
        spinner_placeholder.empty()
    
    def update_status_for_node(node_name: str):
        """Update status display for the given node that is STARTING"""
        nonlocal current_status_node
        
        if node_name == current_status_node:
            return  # Already showing this node's status
        
        current_status_node = node_name
        print(f"🎨 STATUS UPDATE: Showing status for {node_name}")
        
        if node_name == 'entry_router':
            status_display.info("⏳ Routing your question...")
        elif node_name == 'navigation_controller':
            status_display.info("⏳ Validating your question...")
        elif node_name == 'router_agent':
            status_display.info("⏳ Finding the right dataset for your question...")
            # Start progressive updates
            asyncio.create_task(show_progressive_router_status())
        elif node_name == 'strategy_planner_agent':
            status_display.info("⏳ Planning strategic analysis...")
        elif node_name == 'drillthrough_planner_agent':
            status_display.info("⏳ Preparing drillthrough analysis...")
        else:
            status_display.info(f"⏳ Processing: {node_name}...")

    async def show_progressive_router_status():
        """Show progressive status updates for router_agent node"""
        try:
            await asyncio.sleep(4)
            if current_status_node == 'router_agent':  # Still on router
                status_display.info("⏳ Generating SQL query based on metadata...")
                print(f"🔄 Router status update: Generating SQL (T+4s)")
            
            await asyncio.sleep(2)
            if current_status_node == 'router_agent':  # Still on router
                status_display.info("⏳ Executing SQL query...")
                print(f"🔄 Router status update: Executing SQL (T+6s)")
        except Exception as e:
            print(f"⚠️ Progressive status update error: {e}")
    
    # Show spinner at start
    show_spinner()
    
    # Show initial status - workflow always starts with entry_router
    update_status_for_node('entry_router')
    
    print("\n" + "="*80)
    print("🎬 WORKFLOW STARTING - Event stream beginning")
    print("="*80 + "\n")
                
    try:
        async for ev in workflow.astream_events(initial_state, config=config):
            et = ev.get('type')
            name = ev.get('name')
            state = ev.get('data', {}) or {}
            
            print(f"📦 Event: type={et}, name={name}")
            
            # ✅ KEY FIX: When a node ENDS, show status for the NEXT node that's about to start
            if et == 'node_end':
                print(f"✅ Node completed: {name}")
                
                # Determine what the next node will be based on routing logic
                # and show its status IMMEDIATELY (before it starts processing)
                
                if name == 'entry_router':
                    # After entry_router, next is usually navigation_controller
                    # (unless there's dataset clarification or SQL followup)
                    if state.get('requires_dataset_clarification') or state.get('is_sql_followup'):
                        update_status_for_node('router_agent')
                    else:
                        update_status_for_node('navigation_controller')
                
                elif name == 'navigation_controller':
                    # Check navigation outputs
                    nav_err = state.get('nav_error_msg')
                    greeting = state.get('greeting_response')
                    user_friendly_msg = state.get('user_friendly_message')

                    if user_friendly_msg:
                        status_display.info(f"💬 {user_friendly_msg}")
                        print(f"💬 Displayed user-friendly message: {user_friendly_msg}")
                        await asyncio.sleep(2)
                    
                    if nav_err:
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(nav_err, message_type="error")
                        return
                    if greeting:
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(greeting, message_type="greeting")
                        return
                    
                    # After navigation, next is usually router_agent or END
                    question_type = state.get('question_type')
                    if question_type == 'root_cause':
                        update_status_for_node('strategy_planner_agent')
                    else:
                        update_status_for_node('router_agent')
                
                elif name == 'router_agent':
                    print(f"🎯 Router agent completed - checking outputs...")
                    
                    # Show success briefly before checking results
                    status_display.success("✅ Query execution complete")
                    await asyncio.sleep(0.5)
                    
                    # Check for various router outcomes
                    if state.get('sql_followup_but_new_question', False):
                        print("🔄 New question detected - will loop back to navigation")
                        update_status_for_node('navigation_controller')
                        continue
                    
                    if state.get('router_error_msg'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('router_error_msg'), message_type="error")
                        return
                    if state.get('needs_followup'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('sql_followup_question'), message_type="needs_followup")
                        return
                    if state.get('requires_dataset_clarification') and state.get('dataset_followup_question'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('dataset_followup_question'), message_type="dataset_clarification")
                        return
                    if state.get('missing_dataset_items') and state.get('user_message'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('user_message'), message_type="missing_items")
                        return
                    if state.get('phi_found') and state.get('user_message'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('user_message'), message_type="phi_pii_error")
                        return
                    if state.get('sql_followup_topic_drift') and state.get('user_message'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('user_message'), message_type="error")
                        return
                    
                    sql_result = state.get('sql_result', {})
                    if sql_result and sql_result.get('success'):
                        hide_spinner()
                        status_display.empty()
                        
                        functional_names = state.get('functional_names', [])
                        if functional_names:
                            add_selection_reasoning_message(functional_names)
                            print(f"✅ Added functional_names display: {functional_names}")
                        
                        rewritten_question = state.get('rewritten_question', initial_state['current_question'])
                        table_name = state.get('selected_dataset', None)
                        add_sql_result_message(sql_result, rewritten_question, table_name)
                        last_state = state
                        
                        st.session_state.sql_rendered = True
                        st.session_state.narrative_state = state
                        st.session_state.followup_state = state
                        
                        print("🔄 SQL results rendered - breaking")
                        break
                    else:
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message("No results available or SQL execution failed.", message_type="error")
                        return
                
                elif name == 'strategy_planner_agent':
                    print(f"🧠 Strategic planner completed")
                    
                    if state.get('strategy_planner_err_msg'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('strategy_planner_err_msg'), message_type="error")
                        return
                    
                    strategic_results = state.get('strategic_query_results', [])
                    if strategic_results:
                        hide_spinner()
                        status_display.empty()
                        
                        strategic_reasoning = state.get('strategic_reasoning', '')
                        add_strategic_analysis_message(strategic_results, strategic_reasoning)
                        last_state = state
                        
                        drillthrough_exists = state.get('drillthrough_exists', '')
                        if drillthrough_exists == "Available":
                            st.session_state.strategic_rendered = True
                            st.session_state.drillthrough_state = state
                            break
                        else:
                            st.session_state.sql_rendered = True
                            st.session_state.followup_state = state
                            break
                    else:
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message("No strategic analysis results available.", message_type="error")
                        return
                
                elif name == 'drillthrough_planner_agent':
                    print(f"🔧 Drillthrough planner completed")
                    
                    if state.get('drillthrough_planner_err_msg'):
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message(state.get('drillthrough_planner_err_msg'), message_type="error")
                        return
                    
                    drillthrough_results = state.get('drillthrough_query_results', [])
                    if drillthrough_results:
                        hide_spinner()
                        status_display.empty()
                        
                        drillthrough_reasoning = state.get('drillthrough_reasoning', '')
                        add_drillthrough_analysis_message(drillthrough_results, drillthrough_reasoning)
                        last_state = state
                        break
                    else:
                        hide_spinner()
                        status_display.empty()
                        add_assistant_message("No drillthrough analysis results available.", message_type="error")
                        return
            
            elif et == 'workflow_end':
                print(f"🏁 Workflow completed")
                hide_spinner()
                status_display.empty()
                if not last_state:
                    last_state = state

        hide_spinner()
        
        # Handle follow-up generation
        if st.session_state.get('sql_rendered', False):
            print("🔄 SQL was rendered - follow-up will be handled in next execution cycle")
        elif last_state and last_state.get('sql_result', {}).get('success'):
            try:
                print("🔄 Starting follow-up generation")
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
        hide_spinner()
        status_display.empty()
        add_assistant_message(f"Workflow failed: {e}", message_type="error")
        print(f"❌ Workflow error: {e}")
