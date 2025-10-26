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
