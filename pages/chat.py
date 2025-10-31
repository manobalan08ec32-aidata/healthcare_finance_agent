def start_processing(user_query: str):
    """Start processing user query - with proper message management"""
    print(f"🎯 Starting processing for: {user_query}")
    
    # Get question type and prepend to workflow query
    question_type = st.session_state.get("question_type_selection", "Follow-up")
    display_query = user_query
    workflow_query = f"{question_type.lower()} - {user_query}"
    print(f"🚀 Prepended workflow query: {workflow_query}")
    
    # 1. Mark all existing SQL results as historical/non-interactive
    for msg in st.session_state.messages:
        if msg.get('message_type') == 'sql_result':
            # This ensures feedback buttons are hidden for old results
            msg['historical'] = True 
            print(f"🕰️ Marked SQL result message as historical")
        
        # 2. MARK FOLLOW-UP INTRO MESSAGE AS HISTORICAL
        # This prevents the follow-up message from re-rendering the buttons in a disabled state
        if msg.get('message_type') == 'followup_questions':
            msg['historical'] = True
            print("🕰️ Marked old follow-up intro message as historical")
    
    # 3. CLEAR INTERACTIVE FOLLOW-UP BUTTONS (MOST CRITICAL STEP)
    # The buttons render based on this list, so clearing it hides the buttons on re-run.
    if hasattr(st.session_state, 'current_followup_questions'):
        if st.session_state.current_followup_questions:
            print("🗑️ Clearing interactive follow-up questions list due to new user input")
            st.session_state.current_followup_questions = []
            
    # Remove the visible "Would you like to explore further?" message from chat history
    # This addresses the case where the user types a question instead of clicking a button.
    # We re-check the messages list *after* marking them as historical/non-interactive,
    # ensuring the last message is the one we want to remove.
    if (st.session_state.messages and 
        st.session_state.messages[-1].get('message_type') == 'followup_questions'):
        st.session_state.messages.pop()
        print("🗑️ Removed follow-up intro message from chat history")
        
    # Add user message to history (use the clean, original query for display)
    st.session_state.messages.append({
        "role": "user",
        "content": display_query
    })
    
    # IMPORTANT: Mark where this conversation starts so we can manage responses properly
    st.session_state.current_conversation_start = len(st.session_state.messages) - 1
    
    # Set processing state and reset narrative state
    st.session_state.current_query = workflow_query
    st.session_state.processing = True
    st.session_state.workflow_started = False
    st.session_state.narrative_rendered = False
    st.session_state.narrative_state = None


def main():
    """Main Streamlit application with async workflow integration and real-time chat history"""
    
    # Safety check: Ensure user came from main.py with proper authentication
    initialize_session_state()
    authenticated_user = get_authenticated_user()
    
    if authenticated_user == 'unknown_user':
        st.error("⚠️ Please start from the main page to ensure proper authentication.")
        st.info("👉 Click the button below to go to the main page and select your team.")
        if st.button("Go to Main Page"):
            st.switch_page("main.py")
        st.stop()

    # Add enhanced CSS styling
    add_message_type_css()

    with st.sidebar:
        st.markdown("### Navigation")
        
        # Custom styling for the back button
        st.markdown("""
        <style>
        .back-button {
            background-color: white;
            color: #1e3a8a;
            border: 1px solid #1e3a8a;
            padding: 8px 16px;
            border-radius: 4px;
            text-decoration: none;
            font-weight: 500;
            display: inline-block;
            width: 100%;
            text-align: center;
            margin: 10px 0;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
        }
        .back-button:hover {
            background-color: #f0f9ff;
        }
        
        /* Sidebar session history buttons - compact and small */
        [data-testid="stSidebar"] .stButton > button[key*="history_"] {
            text-align: left !important;
            background-color: #f8f9fa !important;
            border: 1px solid #e0e0e0 !important;
            padding: 4px 6px !important;
            margin: 2px 0 !important;
            border-radius: 4px !important;
            font-size: 10px !important;
            line-height: 1.2 !important;
            width: 100% !important;
            height: auto !important;
            min-height: 24px !important;
            white-space: nowrap !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        
        [data-testid="stSidebar"] .stButton > button[key*="history_"]:hover {
            background-color: #e9ecef !important;
            border-color: #1e3a8a !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
        if st.button("Back to Main Page", key="back_to_main"):
            st.switch_page("main.py")
    
    try:
        workflow = get_session_workflow()
        
        if workflow is None:
            st.error("Failed to initialize async workflow. Please refresh the page.")
            return
        
        # Show spinner in main area while fetching session history
        if 'cached_history' not in st.session_state:
            # Show spinner in main chat area while fetching history
            spinner_placeholder = st.empty()
            with spinner_placeholder:
                with st.spinner("🔄 Loading application and fetching session history..."):
                    # Fetch history in background while showing spinner
                    with st.sidebar:
                        st.markdown("---")
                        render_sidebar_history()
            # Clear the spinner once history is loaded
            spinner_placeholder.empty()
        else:
            # History already cached, just render sidebar
            with st.sidebar:
                st.markdown("---")
                render_sidebar_history()
        
        st.markdown("""
        <div style="margin-top: -20px; margin-bottom: 10px;">
            <h2 style="color: #1e3a8a; font-weight: 600; font-size: 1.8rem; margin-bottom: 0;">Finance Analytics Assistant</h2>
        </div>
        """, unsafe_allow_html=True)
        
        # Display current domain selection if available
        if st.session_state.get('domain_selection'):
            domain_display = ", ".join(st.session_state.domain_selection)
            st.markdown(f"""
            <div style="background-color: #e3f2fd; padding: 8px 12px; border-radius: 6px; margin-bottom: 10px; color: #1e3a8a; font-size: 0.9rem;">
                🏢 <strong>Selected Team:</strong> {domain_display}
            </div>
            """, unsafe_allow_html=True)
        
        # Show overview of last session if available (only for fresh sessions)
        render_last_session_overview()
        
        st.markdown("---")
        
        # Chat container for messages
        chat_container = st.container()
        
        with chat_container:
            st.markdown('<div class="chat-container">', unsafe_allow_html=True)
            
            # Render all chat messages (including real-time updates)
            for idx, message in enumerate(st.session_state.messages):
                render_chat_message_enhanced(message, idx)
            
            # Render follow-up questions if they exist
            render_persistent_followup_questions()
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Processing indicator and streaming workflow execution
        if st.session_state.processing:
            # with st.spinner("Running SQL Generation..."):
            run_streaming_workflow(workflow, st.session_state.current_query)
            st.session_state.processing = False
            st.session_state.workflow_started = False
            st.rerun()
        
        # Handle narrative generation after SQL results have been rendered
        elif st.session_state.get('sql_rendered', False) and not st.session_state.get('narrative_rendered', False):
            narrative_state = st.session_state.get('narrative_state')
            if narrative_state:
                with st.spinner("Generating Insights..."):
                    try:
                        print("📝 Generating narrative after SQL display...")
                        
                        # Run narrative generation
                        async def generate_narrative():
                            session_id = st.session_state.session_id
                            config = {"configurable": {"thread_id": session_id}}
                            return await workflow.ainvoke_narrative(narrative_state, config)
                        
                        narrative_res = asyncio.run(generate_narrative())
                        
                        # Add diagnostic logging to understand the response structure
                        print("🔍 Narrative response keys:", list(narrative_res.keys()) if isinstance(narrative_res, dict) else "Not a dict")
                        print("🔍 narrative_complete in response:", narrative_res.get('narrative_complete', 'NOT_FOUND'))
                        
                        # The narrative_complete flag should be in the returned state, not the agent's return value
                        narrative_complete_flag = narrative_res.get('narrative_complete', False)
                        
                        # Get the updated sql_result from the returned state (narrative agent modifies it in-place)
                        updated_sql_result = narrative_res.get('sql_result', {})
                        print("🔍 Updated sql_result has narrative:", bool(updated_sql_result.get('narrative')))
                        
                        if narrative_complete_flag:
                            # Update the existing SQL result message with narrative
                            if updated_sql_result:
                                # Find and update the last SQL result message
                                for i in range(len(st.session_state.messages) - 1, -1, -1):
                                    msg = st.session_state.messages[i]
                                    if msg.get('message_type') == 'sql_result':
                                        msg['sql_result'] = updated_sql_result
                                        break
                            else:
                                # Fallback: try to locate sql_result from narrative_state if not returned explicitly
                                updated_sql_result = (st.session_state.get('narrative_state') or {}).get('sql_result', {})
                            
                            # Fire and forget insert of tracking row (after we have narrative)
                            if updated_sql_result:
                                try:
                                    asyncio.run(_insert_session_tracking_row(updated_sql_result))
                                except RuntimeError:
                                    # If already in an event loop (unlikely here due to asyncio.run wrapping), schedule task
                                    loop = asyncio.get_event_loop()
                                    loop.create_task(_insert_session_tracking_row(updated_sql_result))
                            
                            print("✅ Narrative generation completed")
                        else:
                            print("⚠️ Narrative generation completed with issues - checking if narrative exists anyway...")
                            # Even if flag is missing, check if narrative was actually added
                            if updated_sql_result and updated_sql_result.get('narrative'):
                                print("🔄 Found narrative content despite missing flag - updating message and inserting")
                                # Update the message anyway
                                for i in range(len(st.session_state.messages) - 1, -1, -1):
                                    msg = st.session_state.messages[i]
                                    if msg.get('message_type') == 'sql_result':
                                        msg['sql_result'] = updated_sql_result
                                        break
                                
                                # Insert tracking row
                                try:
                                    asyncio.run(_insert_session_tracking_row(updated_sql_result))
                                except RuntimeError:
                                    loop = asyncio.get_event_loop()
                                    loop.create_task(_insert_session_tracking_row(updated_sql_result))
                            else:
                                print("❌ No narrative content found in sql_result")
                            
                    except Exception as ne:
                        st.error(f"Narrative generation failed: {ne}")
                        print(f"❌ Narrative generation error: {ne}")
                    
                    # Mark narrative as rendered and prepare for follow-up
                    st.session_state.narrative_rendered = True
                    st.session_state.narrative_state = None
                    st.rerun()
        
        # Handle follow-up generation after narrative has been rendered
        elif st.session_state.get('narrative_rendered', False):
            followup_state = st.session_state.get('followup_state')
            if followup_state:
                with st.spinner("Generating follow-up questions..."):
                    try:
                        print("🔄 Generating follow-up questions after narrative completion...")
                        
                        # Run follow-up generation
                        async def generate_followups():
                            session_id = st.session_state.session_id
                            config = {"configurable": {"thread_id": session_id}}
                            return await workflow.ainvoke_followup(followup_state, config)
                        
                        follow_res = asyncio.run(generate_followups())
                        fq = follow_res.get('followup_questions', [])
                        
                        if fq:
                            st.session_state.current_followup_questions = fq
                            add_assistant_message("💡 **Would you like to explore further? Here are some suggested follow-up questions:**", message_type="followup_questions")
                            print(f"✅ Generated {len(fq)} follow-up questions")
                        else:
                            print("ℹ️ No follow-up questions generated")
                            
                    except Exception as fe:
                        st.error(f"Follow-up generation failed: {fe}")
                        print(f"❌ Follow-up generation error: {fe}")
                    
                    # Clean up session state
                    st.session_state.sql_rendered = False
                    st.session_state.narrative_rendered = False
                    st.session_state.followup_state = None
                    st.rerun()
        
        # Handle drillthrough execution after strategic analysis has been rendered
        elif st.session_state.get('strategic_rendered', False):
            drillthrough_state = st.session_state.get('drillthrough_state')
            if drillthrough_state:
                with st.spinner("Running Drillthrough Analysis..."):
                    try:
                        print("🔄 Running drillthrough analysis after strategic display...")
                        
                        # Run drillthrough analysis
                        async def execute_drillthrough():
                            session_id = st.session_state.session_id
                            config = {"configurable": {"thread_id": session_id}}
                            return await workflow.ainvoke_drillthrough(drillthrough_state, config)
                        
                        drillthrough_res = asyncio.run(execute_drillthrough())
                        
                        # Extract drillthrough results
                        drillthrough_results = drillthrough_res.get('drillthrough_query_results', [])
                        drillthrough_reasoning = drillthrough_res.get('drillthrough_reasoning', '')
                        
                        if drillthrough_results:
                            add_drillthrough_analysis_message(drillthrough_results, drillthrough_reasoning)
                            print(f"✅ Drillthrough analysis complete: {len(drillthrough_results)} analyses")
                            print("🏁 Analysis workflow complete - no follow-up questions will be generated")
                        else:
                            print("ℹ️ No drillthrough results generated")
                            add_assistant_message("No drillthrough analysis results available.", message_type="error")
                        
                    except Exception as e:
                        print(f"❌ Drillthrough execution failed: {e}")
                        add_assistant_message(f"Drillthrough analysis failed: {e}", message_type="error")
                    
                    # Clear the flag to prevent re-running
                    st.session_state.strategic_rendered = False
                    st.session_state.drillthrough_state = None
                    st.rerun()

        # --- NEW CODE: Radio Button for Question Context ---
        # This will appear just above the chat input bar
        st.markdown("<div style='margin-top: 15px; margin-bottom: -5px;'>", unsafe_allow_html=True)
        question_type = st.radio(
            "Select question context:",
            ("Follow-up", "New Question"),
            index=0,  # Default to "Follow-up"
            horizontal=True,
            key="question_type_radio"
        )
        st.markdown("</div>", unsafe_allow_html=True)

        # Store the selection in session state so start_processing can access it
        st.session_state.question_type_selection = question_type
        # --- END NEW CODE ---
        
        # Chat input at the bottom
        if prompt := st.chat_input("Ask a question...", disabled=st.session_state.processing):
            # Immediately clear follow-up questions when new input is detected
            # if hasattr(st.session_state, 'current_followup_questions'):
            #     st.session_state.current_followup_questions = []
            start_processing(prompt)
            st.rerun()
        
    except Exception as e:
        st.error(f"Application Error: {str(e)}")
        st.session_state.processing = False
        st.session_state.workflow_started = False
    finally:
        # Ensure cleanup on any app termination
        if st.session_state.get('db_client'):
            pass  # Cleanup will be handled by atexit


def render_chat_message_enhanced(message, message_idx):
    """Enhanced chat message rendering with message type awareness"""
    
    role = message.get('role', 'user')
    content = message.get('content', '')
    message_type = message.get('message_type', 'standard')
    timestamp = message.get('timestamp', '')
    is_historical = message.get('historical', False)  # Check if this is a historical message
    
    if role == 'user':
        # Use custom sky blue background for user messages with icon
        # Properly format the content to handle newlines and special characters
        safe_content_html = convert_text_to_safe_html(content)
        st.markdown(f"""
        <div class="user-message">
            <div style="display: flex; align-items: flex-start; gap: 8px;">
                <div style="flex-shrink: 0; width: 32px; height: 32px; background-color: #4f9cf9; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: white; font-weight: 600; font-size: 14px;">
                    👤
                </div>
                <div class="user-message-content">
                    {safe_content_html}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    elif role == 'assistant':
        # Handle SQL result messages specially
        if message_type == "sql_result":
            sql_result = message.get('sql_result')
            rewritten_question = message.get('rewritten_question')
            table_name = message.get('table_name')
            if sql_result:
                print(f"🔍 Rendering SQL result: type={type(sql_result)}")
                if isinstance(sql_result, str):
                    print(f"    ⚠️ SQL result is string, not dict: {sql_result[:100]}...")
                    # Try to parse as JSON if it's a string
                    try:
                        sql_result = json.loads(sql_result)
                        print(f"    ✅ Successfully parsed string to dict")
                    except json.JSONDecodeError as e:
                        print(f"    ❌ Failed to parse SQL result string as JSON: {e}")
                        st.error("Error: Could not parse SQL result data")
                        return
                # Feedback is only shown if the message is NOT historical
                render_sql_results(sql_result, rewritten_question, show_feedback=not is_historical,message_idx=message_idx, table_name=table_name)
            return
        
        # Handle strategic analysis messages
        elif message_type == "strategic_analysis":
            strategic_results = message.get('strategic_results')
            strategic_reasoning = message.get('strategic_reasoning')
            if strategic_results:
                render_strategic_analysis(strategic_results, strategic_reasoning, show_feedback=not is_historical)
            return
        
        # Handle drillthrough analysis messages
        elif message_type == "drillthrough_analysis":
            drillthrough_results = message.get('drillthrough_results')
            drillthrough_reasoning = message.get('drillthrough_reasoning')
            if drillthrough_results:
                render_drillthrough_analysis(drillthrough_results, drillthrough_reasoning, show_feedback=not is_historical)
            return
        
        # Handle selection reasoning messages - use sky blue background like last session story
        elif message_type == "selection_reasoning":
            # content is now a list of functional_names
            if isinstance(content, list):
                datasets_text = ", ".join(content)
            else:
                datasets_text = str(content)
            
            st.markdown(f"""
            <div style="background-color: #f0f8ff; border: 1px solid #4a90e2; border-radius: 8px; padding: 12px; margin: 12px 0; max-width: 420px;">
                <div style="display: flex; align-items: center;">
                    <span style="font-size: 1rem; margin-right: 8px;">📊</span>
                    <strong style="color: #1e3a8a; font-size: 0.95rem;">Selected Datasets: {datasets_text}</strong>
                </div>
            </div>
            """, unsafe_allow_html=True)
            return

        # NEW CODE: Suppress historical followup_questions message
        elif message_type == "followup_questions":
            if is_historical:
                # If the user started a new question, the old follow-up message is marked historical. 
                # We skip rendering it to prevent the empty, greyed-out space.
                return 

        # Use consistent warm gold background for all system messages with icon
        safe_content_html = convert_text_to_safe_html(content)
        st.markdown(f"""
        <div class="assistant-message">
            <div style="display: flex; align-items: flex-start; gap: 8px;">
                <div style="flex-shrink: 0; width: 32px; height: 32px; background-color: #ff6b35; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: white; font-weight: 600; font-size: 14px;">
                    🤖
                </div>
                <div class="assistant-message-content">
                    {safe_content_html}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)



