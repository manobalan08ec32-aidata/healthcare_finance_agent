# ===================================================================
# FIXED render_feedback_section FUNCTION
# Replace lines 689-795 in chat.py
# ===================================================================

def render_feedback_section(title, sql_query, data, narrative, user_question=None, message_idx=None, table_name=None):
    """Render feedback section with thumbs up/down buttons - only for current question"""
    
    # NOTE: We trust the show_feedback parameter passed from render_single_sql_result
    # The historical marking is already handled in start_processing and checked before calling this function
    # No need to check if this is the "last message" because follow-up questions may be added after SQL results
    
    # Create a unique key for this feedback section using multiple factors
    # Include session_id and message_idx for uniqueness
    session_id = st.session_state.get('session_id', 'default')
    if message_idx is not None:
        feedback_key = f"feedback_{session_id}_msg_{message_idx}"
    else:
        # Fallback for edge cases
        timestamp = int(time.time() * 1000)
        feedback_key = f"feedback_{session_id}_{timestamp}"
    
    print(f"🔑 Generated feedback key: {feedback_key} | Showing feedback for message_idx={message_idx}")
    
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
# EXPLANATION OF THE FIX:
# ===================================================================
# 
# REMOVED THE INCORRECT CHECK (lines 692-700 in original):
#     if message_idx is not None:
#         total_messages = len(st.session_state.messages)
#         is_latest_sql_result = (message_idx == total_messages - 1)
#         
#         if not is_latest_sql_result:
#             print(f"🔇 Skipping feedback for historical message (idx={message_idx})")
#             return
# 
# WHY THIS CHECK WAS WRONG:
# 1. After SQL results are rendered, follow-up questions are added to messages
# 2. So SQL result at index 7 is no longer at index 8 (last position)
# 3. The check fails even though the SQL result is the CURRENT one (not historical)
# 4. This causes feedback to be skipped incorrectly
# 
# WHY THE FIX WORKS:
# 1. The show_feedback parameter is already correctly set in render_chat_message_enhanced
# 2. It uses: show_feedback=not is_historical
# 3. start_processing marks old SQL results as historical=True when new question starts
# 4. So only the current question's SQL result has show_feedback=True
# 5. We don't need an additional check - just trust the parameter!
# 
# FLOW:
# New Question Asked
#   → start_processing() marks all previous SQL results as historical=True
#   → New SQL result added WITHOUT historical flag
#   → render_chat_message_enhanced checks: show_feedback=not is_historical
#   → Only current SQL result has show_feedback=True
#   → render_feedback_section is called (only for current result)
#   → Feedback buttons displayed ✅
# 
# ===================================================================


# ===================================================================
# FIXED render_sidebar_history FUNCTION - SINGLE LINE TIMESTAMPS
# Replace lines 1702-1794 in chat.py
# ===================================================================

def render_sidebar_history():
    """Render session history in the sidebar with clickable timestamps (single line)."""
    
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
            
            # Format timestamp for display - COMPACT FORMAT FOR SINGLE LINE
            try:
                # Parse timestamp and format it nicely
                from datetime import datetime
                if isinstance(last_activity, str):
                    # Try parsing ISO format timestamp
                    dt = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
                    # OPTION 1: Compact format without year - "Jan 15 2:30PM"
                    display_timestamp = dt.strftime("%b %d %I:%M%p")
                    
                    # OPTION 2: Even more compact with slashes - "1/15 2:30PM" (uncomment to use)
                    # display_timestamp = dt.strftime("%m/%d %I:%M%p")
                    
                    # OPTION 3: 24-hour format, no year - "Jan 15 14:30" (uncomment to use)
                    # display_timestamp = dt.strftime("%b %d %H:%M")
                else:
                    # If it's already a datetime object
                    display_timestamp = last_activity.strftime("%b %d %I:%M%p")
            except Exception as e:
                print(f"⚠️ Failed to format timestamp: {e}")
                display_timestamp = str(last_activity)
            
            # Add emoji with compact timestamp
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
            help=f"Load session from {display_timestamp}\nSession ID: {session_id}",
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
# ENHANCED CSS FOR SIDEBAR BUTTONS (OPTIONAL - ALREADY EXISTS)
# If the buttons still wrap, ensure this CSS is in your main() function
# This should already be at lines 2301-2317
# ===================================================================

"""
/* Sidebar session history buttons - compact and single line */
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
"""


# ===================================================================
# TIMESTAMP FORMAT OPTIONS:
# ===================================================================
# 
# Choose one of these formats by uncommenting in lines 51-58:
# 
# OPTION 1 (DEFAULT): "%b %d %I:%M%p"
# Result: "Jan 15 2:30PM"
# - Pros: Human readable, includes AM/PM
# - Cons: Slightly longer with month name
# 
# OPTION 2: "%m/%d %I:%M%p"  
# Result: "1/15 2:30PM"
# - Pros: Most compact
# - Cons: Numeric month less readable
# 
# OPTION 3: "%b %d %H:%M"
# Result: "Jan 15 14:30"
# - Pros: No AM/PM, clean 24-hour format
# - Cons: Some users prefer 12-hour time
# 
# OPTION 4 (If you need year): "%m/%d/%y %H:%M"
# Result: "1/15/25 14:30"
# - Pros: Includes year, very compact
# - Cons: All numeric, less readable
# 
# ===================================================================
# 
# KEY CHANGES:
# 1. Changed timestamp format from "%b %d, %Y %I:%M %p" to "%b %d %I:%M%p"
#    Old: "Jan 15, 2025 02:30 PM" (wraps to 2 lines)
#    New: "Jan 15 2:30PM" (fits on 1 line)
# 
# 2. Removed spaces around AM/PM (%I:%M%p instead of %I:%M %p)
#    Saves 1 character
# 
# 3. Removed year and comma - most sessions are recent anyway
# 
# 4. CSS already ensures single line with:
#    - white-space: nowrap (prevents wrapping)
#    - text-overflow: ellipsis (adds ... if too long)
#    - overflow: hidden (clips overflow)
# 
# RESULT: Timestamps now fit on a single line in the 220px sidebar!
# 
# ===================================================================
