# ===================================================================
# MODIFIED FUNCTIONS FOR chat-2.py
# ===================================================================

# ===================================================================
# 1. MODIFICATION IN run_streaming_workflow_async function
#    Replace lines 269-273 with this code:
# ===================================================================

# OLD CODE (lines 269-273):
# # Display selection_reasoning after AI rewritten question if available
# selection_reasoning = state.get('selection_reasoning', '')
# if selection_reasoning and selection_reasoning.strip():
#     add_selection_reasoning_message(selection_reasoning)
#     print(f"✅ Added selection reasoning display")

# NEW CODE:
# Display functional_names instead of selection_reasoning
functional_names = state.get('functional_names', [])
if functional_names:
    add_selection_reasoning_message(functional_names)
    print(f"✅ Added functional_names display: {functional_names}")


# ===================================================================
# 2. MODIFIED add_selection_reasoning_message function
#    Replace the entire function (lines 1103-1115)
# ===================================================================

def add_selection_reasoning_message(functional_names):
    """
    Add functional_names message to chat history for proper rendering
    functional_names: list of strings representing selected datasets
    """
    message = {
        "role": "assistant",
        "content": functional_names,  # Store as list instead of string
        "message_type": "selection_reasoning",
        "timestamp": datetime.now().isoformat()
    }
    
    st.session_state.messages.append(message)
    print(f"✅ Added functional_names message (total messages now: {len(st.session_state.messages)})")


# ===================================================================
# 3. MODIFIED render_chat_message_enhanced function
#    Replace the selection_reasoning section (lines 2636-2649)
# ===================================================================

# OLD CODE (lines 2636-2649):
# elif message_type == "selection_reasoning":
#     safe_content_html = convert_text_to_safe_html(content)
#     st.markdown(f"""
#     <div style="background-color: #f0f8ff; border: 1px solid #4a90e2; border-radius: 8px; padding: 16px; margin: 16px 0;">
#         <div style="display: flex; align-items: center; margin-bottom: 12px;">
#             <span style="font-size: 1.2rem; margin-right: 8px;">🧠</span>
#             <strong style="color: #1e3a8a; font-size: 1.1rem;">Dataset Selection Reasoning</strong>
#         </div>
#         <div style="color: #1e3a8a; line-height: 1.6; font-weight: 350;">
#             {safe_content_html}
#         </div>
#     </div>
#     """, unsafe_allow_html=True)
#     return

# NEW CODE:
elif message_type == "selection_reasoning":
    # content is now a list of functional_names
    if isinstance(content, list):
        datasets_text = ", ".join(content)
    else:
        datasets_text = str(content)
    
    st.markdown(f"""
    <div style="background-color: #f0f8ff; border: 1px solid #4a90e2; border-radius: 8px; padding: 16px; margin: 16px 0;">
        <div style="display: flex; align-items: center;">
            <span style="font-size: 1.2rem; margin-right: 8px;">📊</span>
            <strong style="color: #1e3a8a; font-size: 1.1rem;">Selected Datasets: {datasets_text}</strong>
        </div>
    </div>
    """, unsafe_allow_html=True)
    return


# ===================================================================
# 4. MODIFIED render_feedback_section function
#    Replace the entire function (lines 687-785)
# ===================================================================

def render_feedback_section(title, sql_query, data, narrative, user_question=None, message_idx=None, table_name=None):
    """Render feedback section with thumbs up/down buttons - only for latest SQL result"""
    
    # Only show feedback for the most recent SQL result
    if message_idx is not None:
        total_messages = len(st.session_state.messages)
        is_latest_sql_result = (message_idx == total_messages - 1)
        
        if not is_latest_sql_result:
            # Don't show feedback for old messages
            print(f"🔇 Skipping feedback for historical message (idx={message_idx})")
            return
    
    # Create a unique key for this feedback section using multiple factors
    # Include session_id, message count, and content hash for uniqueness
    session_id = st.session_state.get('session_id', 'default')
    if message_idx is not None:
        feedback_key = f"feedback_{session_id}_msg_{message_idx}"
    else:
        # Fallback for edge cases
        timestamp = int(time.time() * 1000)
        feedback_key = f"feedback_{session_id}_{timestamp}"
    
    print(f"🔑 Generated feedback key: {feedback_key}")
    
    # Show feedback buttons if not already submitted
    if not st.session_state.get(f"{feedback_key}_submitted", False):
        st.markdown("---")
        st.markdown("**Was this helpful?** ")
        
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("👍 Yes, helpful", key=f"{feedback_key}_thumbs_up"):
                # Insert positive feedback - use user_question if available, otherwise fallback
                rewritten_question = user_question or st.session_state.get('current_query', title)
                
                sql_result_dict = {
                    'title': title,
                    'sql_query': sql_query,
                    'query_results': data,
                    'narrative': narrative,
                    'user_question': user_question  # Include user_question in sql_result
                }
                
                # Run feedback insertion
                success = asyncio.run(_insert_feedback_row(
                    rewritten_question, sql_query, True, table_name
                ))
                
                if success:
                    st.session_state[f"{feedback_key}_submitted"] = True
                    st.success("Thank you for your feedback! 👍")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to save feedback")
        
        with col2:
            if st.button("👎 Needs improvement", key=f"{feedback_key}_thumbs_down"):
                st.session_state[f"{feedback_key}_show_form"] = True
                st.rerun()
        
        # Show feedback form for thumbs down
        if st.session_state.get(f"{feedback_key}_show_form", False):
            with st.form(key=f"{feedback_key}_form"):
                feedback_text = st.text_area(
                    "How can we improve this response?",
                    placeholder="Please describe what could be better...",
                    height=100
                )
                
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.form_submit_button("Submit Feedback"):
                        # Insert negative feedback - use user_question if available, otherwise fallback
                        rewritten_question = user_question or st.session_state.get('current_query', title)
                        
                        sql_result_dict = {
                            'title': title,
                            'sql_query': sql_query,
                            'query_results': data,
                            'narrative': narrative,
                            'user_question': user_question  # Include user_question in sql_result
                        }
                        
                        success = asyncio.run(_insert_feedback_row(
                            rewritten_question, sql_query, False, table_name, feedback_text
                        ))
                        
                        if success:
                            st.session_state[f"{feedback_key}_submitted"] = True
                            st.session_state[f"{feedback_key}_show_form"] = False
                            st.success("Thank you for your feedback! We'll work on improving.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Failed to save feedback")
                
                with col2:
                    if st.form_submit_button("Cancel"):
                        st.session_state[f"{feedback_key}_show_form"] = False
                        st.rerun()
    else:
        # Show feedback submitted message
        st.markdown("---")
        st.success("✅ Thank you for your feedback!")


# ===================================================================
# 5. MODIFIED start_processing function
#    Replace the entire function (lines 1799-1828)
# ===================================================================

def start_processing(user_query: str):
    """Start processing user query - with proper message management"""
    print(f"🎯 Starting processing for: {user_query}")
    
    # Mark all existing SQL results as historical before adding new question
    for msg in st.session_state.messages:
        if msg.get('message_type') == 'sql_result':
            msg['historical'] = True
            print(f"🕰️ Marked SQL result message as historical")
    
    # Clear follow-up questions when user types a new question
    if hasattr(st.session_state, 'current_followup_questions') and st.session_state.current_followup_questions:
        print("🗑️ Clearing follow-up questions due to new user input")
        st.session_state.current_followup_questions = []
        
        # Also remove the "Would you like to explore further?" message from chat history if it exists
        if (st.session_state.messages and 
            st.session_state.messages[-1].get('message_type') == 'followup_questions'):
            st.session_state.messages.pop()
            print("🗑️ Removed follow-up intro message from chat history")
    
    # Add user message to history
    st.session_state.messages.append({
        "role": "user",
        "content": user_query
    })
    
    # IMPORTANT: Mark where this conversation starts so we can manage responses properly
    st.session_state.current_conversation_start = len(st.session_state.messages) - 1
    
    # Set processing state and reset narrative state
    st.session_state.current_query = user_query
    st.session_state.processing = True
    st.session_state.workflow_started = False
    st.session_state.narrative_rendered = False
    st.session_state.narrative_state = None


# ===================================================================
# SUMMARY OF CHANGES:
# ===================================================================
# 
# 1. In run_streaming_workflow_async (line ~270):
#    - Changed from using 'selection_reasoning' to 'functional_names'
#    - Now expects functional_names as a list from state
#
# 2. add_selection_reasoning_message function:
#    - Now accepts functional_names (list) instead of selection_reasoning (string)
#    - Stores the list directly in message content
#
# 3. render_chat_message_enhanced function (selection_reasoning section):
#    - Now handles functional_names as a list
#    - Displays as comma-separated string: "Selected Datasets: dataset1, dataset2, dataset3"
#    - Changed icon from 🧠 to 📊
#    - Simplified layout (removed extra div)
#
# 4. render_feedback_section function:
#    - Added check to only show feedback for latest SQL result
#    - Historical messages (not the latest) won't show feedback buttons
#
# 5. start_processing function:
#    - Added logic to mark all existing SQL results as 'historical' before processing new question
#    - This ensures feedback only appears on the current question's results
#
# ===================================================================

# ===================================================================
# MODIFIED FUNCTIONS FOR TIMESTAMP-BASED SESSION HISTORY IN SIDEBAR
# ===================================================================

# ===================================================================
# 1. MODIFIED _fetch_session_history function
#    Replace the entire function (lines 1178-1240)
# ===================================================================

async def _fetch_session_history():
    """Fetch last 5 sessions for the user from fdmbotsession_tracking table.
    Returns list of sessions with session_id and insert_ts (timestamp).
    """
    try:
        db_client = st.session_state.get('db_client')
        if not db_client:
            print("⚠️ No db_client in session; skipping history fetch")
            return []
        
        user_id = get_authenticated_user()
        
        # Modified query to fetch both session_id and max timestamp
        fetch_sql = f"""
        SELECT session_id, MAX(insert_ts) as last_activity
        FROM prd_optumrx_orxfdmprdsa.rag.fdmbotsession_tracking
        WHERE user_id = '{user_id}'
        GROUP BY session_id 
        ORDER BY MAX(insert_ts) DESC
        LIMIT 5
        """
        
        print("🕐 Fetching session history for user:", user_id)
        print("🔍 SQL Query:", fetch_sql)

        result = await db_client.execute_sql_async_audit(fetch_sql)

        print(f"🔍 Database result type: {type(result)}")
        print(f"🔍 Database result: {result}")
        
        # Handle different response formats from database client
        sessions = []
        
        if isinstance(result, list):
            # Direct list response
            sessions = result
            print(f"✅ Direct list response with {len(sessions)} sessions")
        elif isinstance(result, dict):
            if result.get('success'):
                sessions = result.get('data', [])
                print(f"✅ Dict response with success=True, {len(sessions)} sessions")
            else:
                print("⚠️ Dict response with success=False")
                print(f"🔍 Error details: {result}")
                return []
        else:
            print(f"⚠️ Unexpected result type: {type(result)}")
            return []
        
        # Debug each session
        for i, session in enumerate(sessions):
            if isinstance(session, dict):
                session_id = session.get('session_id', 'NO_ID')
                last_activity = session.get('last_activity', 'NO_TIMESTAMP')
                print(f"  Session {i+1}: {session_id} | Last Activity: {last_activity}")
            else:
                print(f"  Session {i+1}: Unexpected session type: {type(session)} - {session}")
        
        return sessions
            
    except Exception as e:
        print(f"⚠️ Session history fetch failed: {e}")
        import traceback
        print(f"🔍 Full traceback: {traceback.format_exc()}")
        return []


# ===================================================================
# 2. MODIFIED render_sidebar_history function
#    Replace the entire function (lines 1688-1763)
# ===================================================================

def render_sidebar_history():
    """Render session history in the sidebar with clickable timestamps."""
    
    st.markdown("### 📚 Recent Sessions")
    
    # Check if we have a database client first
    db_client = st.session_state.get('db_client')
    if not db_client:
        st.info("Database not connected")
        return
    
    # Auto-fetch history on load
    if 'cached_history' not in st.session_state:
        try:
            print("🔄 Auto-fetching history from database...")
            history = asyncio.run(_fetch_session_history())
            st.session_state.cached_history = history
            print(f"📦 Cached {len(history)} sessions")
        except Exception as e:
            print(f"❌ Failed to fetch history: {e}")
            st.session_state.cached_history = []
    
    history = st.session_state.get('cached_history', [])
    print(f"🗂️ Displaying {len(history)} cached sessions")
    
    if not history:
        st.info("No previous sessions found")
        return
    
    # Display historical sessions by timestamp (ordered DESC)
    for idx, session_item in enumerate(history):
        # Handle dict responses from database (should have session_id and last_activity)
        if isinstance(session_item, dict):
            session_id = session_item.get('session_id', f'unknown_{idx}')
            last_activity = session_item.get('last_activity', 'Unknown Time')
            
            # Format timestamp for display
            try:
                # Parse timestamp and format it nicely
                from datetime import datetime
                if isinstance(last_activity, str):
                    # Try parsing ISO format timestamp
                    dt = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
                    display_timestamp = dt.strftime("%b %d, %Y %I:%M %p")
                else:
                    # If it's already a datetime object
                    display_timestamp = last_activity.strftime("%b %d, %Y %I:%M %p")
            except Exception as e:
                print(f"⚠️ Failed to format timestamp: {e}")
                display_timestamp = str(last_activity)
            
            display_text = f"🕒 {display_timestamp}"
            
        elif isinstance(session_item, str):
            # Fallback for string-only response (shouldn't happen with new query)
            session_id = session_item
            display_text = f"Session: {session_id[:12]}..."
            
        else:
            print(f"⚠️ Unexpected session type: {type(session_item)} - {session_item}")
            continue
        
        # Create a clickable button for each session with timestamp display
        if st.button(
            display_text, 
            key=f"history_{idx}_{session_id}",
            help=f"Load session from {display_text}\nSession ID: {session_id}",
            use_container_width=True
        ):
            # Clear current chat and load historical session
            # Preserve domain_selection before clearing everything
            current_domain_selection = st.session_state.get('domain_selection')
            
            st.session_state.messages = []
            st.session_state.sql_rendered = False
            st.session_state.narrative_rendered = False
            st.session_state.processing = False
            st.session_state.session_overview_shown = True  # Don't show overview when loading historical session
            
            # Set session ID to the historical session ID to continue the conversation thread
            st.session_state.session_id = session_id
            
            # Restore domain_selection so new questions work properly
            if current_domain_selection:
                st.session_state.domain_selection = current_domain_selection
                print(f"🔄 Session ID updated to historical session: {session_id}, preserved domain_selection: {current_domain_selection}")
            else:
                # If no domain selection exists, this might cause "Domain Not Found" error
                print(f"⚠️ Session ID updated to historical session: {session_id}, no domain_selection to preserve - this may cause errors!")
                print(f"💡 User may need to go back to main page to select domain again")
            
            # Render the complete historical session
            render_historical_session_by_id(session_id)
            st.rerun()


# ===================================================================
# SUMMARY OF CHANGES:
# ===================================================================
# 
# 1. _fetch_session_history function:
#    - Modified SQL query to SELECT both session_id and MAX(insert_ts) as last_activity
#    - Still ordered by timestamp DESC (most recent first)
#    - Returns dict with both session_id and last_activity for each session
#
# 2. render_sidebar_history function:
#    - Changed to display formatted timestamp instead of session_id
#    - Timestamp format: "Dec 25, 2024 03:45 PM" (Month Day, Year Hour:Min AM/PM)
#    - When user clicks on timestamp button, it still uses session_id internally
#    - Button tooltip shows both timestamp and session_id for reference
#    - Sessions are shown in descending timestamp order (most recent first)
#
# How it works:
# - User sees: "🕒 Jan 15, 2025 02:30 PM"
# - User clicks on it
# - Backend loads session using the session_id associated with that timestamp
# - All historical messages for that session_id are loaded
#
# ===================================================================
