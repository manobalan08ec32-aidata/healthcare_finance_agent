# ===================================================================
# FIX FOR DUPLICATE FEEDBACK KEYS IN MULTIPLE SQL RESULTS
# ===================================================================
# 
# PROBLEM: When a single question generates multiple SQL queries, all 
# feedback buttons get the same key (e.g., feedback_xxx_msg_2_thumbs_up),
# causing Streamlit's "duplicate key" error.
#
# SOLUTION: Add a sub_index parameter to differentiate between multiple
# SQL results within the same message.
# ===================================================================

# ===================================================================
# 1. MODIFIED render_sql_results function
#    Replace lines 792-825 in chat-2.py
# ===================================================================

def render_sql_results(sql_result, rewritten_question=None, show_feedback=True, message_idx=None, table_name=None):
    """Render SQL results with title, expandable SQL query, data table, and narrative"""
    
    print(f"📊 render_sql_results called with: type={type(sql_result)}, show_feedback={show_feedback}")
    
    # Ensure sql_result is a dictionary
    if not isinstance(sql_result, dict):
        print(f"❌ SQL result is not a dict: {type(sql_result)} - {str(sql_result)[:100]}...")
        st.error(f"Error: Invalid SQL result format (expected dict, got {type(sql_result).__name__})")
        return
    
    # Get user_question from sql_result for feedback
    user_question = sql_result.get('user_question')
    
    # Check if multiple results
    if sql_result.get('multiple_results', False):
        query_results = sql_result.get('query_results', [])
        for i, result in enumerate(query_results):
            title = result.get('title', f'Query {i+1}')
            sql_query = result.get('sql_query', '')
            data = result.get('data', [])
            narrative = result.get('narrative', '')
            
            # ⭐ Pass sub_index (i) to differentiate multiple SQL results
            render_single_sql_result(
                title, sql_query, data, narrative, 
                user_question, show_feedback, message_idx, 
                table_name, sub_index=i
            )
    else:
        # Single result - use rewritten_question if available, otherwise default
        title = rewritten_question if rewritten_question else "Analysis Results"
        sql_query = sql_result.get('sql_query', '')
        data = sql_result.get('query_results', [])
        narrative = sql_result.get('narrative', '')
        
        # ⭐ No sub_index needed for single result (defaults to None)
        render_single_sql_result(
            title, sql_query, data, narrative, 
            user_question, show_feedback, message_idx, 
            table_name, sub_index=None
        )


# ===================================================================
# 2. MODIFIED render_single_sql_result function
#    Replace lines 828-884 in chat-2.py
# ===================================================================

def render_single_sql_result(title, sql_query, data, narrative, user_question=None, show_feedback=True, message_idx=None, table_name=None, sub_index=None):
    """Render a single SQL result with warm gold background for title and narrative"""
    
    # Title with custom narrative-content styling
    st.markdown(f"""
    <div class="narrative-content">
        <strong> 🤖 AI Rewritten Question:</strong> {title}
    </div>
    """, unsafe_allow_html=True)
    
    # SQL Query in collapsible section
    if sql_query:
        # Use HTML details/summary instead of st.expander to avoid Azure rendering issues
        st.markdown(f"""
        <details style="margin: 10px 0; border: 1px solid #e1e5e9; border-radius: 6px; padding: 0;">
            <summary style="background-color: #f8f9fa; padding: 8px 12px; cursor: pointer; border-radius: 6px 6px 0 0; font-weight: 500; color: #495057;">
                View SQL Query
            </summary>
            <div style="padding: 12px; background-color: #f8f9fa; border-radius: 0 0 6px 6px;">
                <pre style="background-color: #2d3748; color: #e2e8f0; padding: 12px; border-radius: 4px; overflow-x: auto; font-family: 'Courier New', monospace; font-size: 12px; line-height: 1.4; margin: 0;"><code>{sql_query}</code></pre>
            </div>
        </details>
        """, unsafe_allow_html=True)
    
    # Data table
    if data:
        formatted_df = format_sql_data_for_streamlit(data)
        row_count = len(formatted_df) if hasattr(formatted_df, 'shape') else 0
        if row_count > 5000:
            st.warning("⚠️ SQL query output exceeds more than 5000 rows. Please rephrase your query to reduce the result size.")
        elif not formatted_df.empty:
            st.dataframe(
                formatted_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No data to display")
    
    # Narrative with custom styling
    if narrative:
        safe_narrative_html = convert_text_to_safe_html(narrative)
        st.markdown(f"""
        <div class="narrative-content">
            <div style="font-weight: 600; margin-bottom: 8px; display: flex; align-items: center;">
                <span style="margin-right: 8px;">💡</span>
                Key Insights
            </div>
            <div>
                {safe_narrative_html}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Add feedback buttons after narrative (only for current session)
        if show_feedback:
            # ⭐ Pass sub_index to render_feedback_section
            render_feedback_section(title, sql_query, data, narrative, user_question, message_idx, table_name, sub_index)


# ===================================================================
# 3. MODIFIED render_feedback_section function
#    Replace lines 689-790 in chat-2.py
# ===================================================================

def render_feedback_section(title, sql_query, data, narrative, user_question=None, message_idx=None, table_name=None, sub_index=None):
    """Render feedback section with thumbs up/down buttons - only for current question"""
    
    # NOTE: We trust the show_feedback parameter passed from render_single_sql_result
    # The historical marking is already handled in start_processing and checked before calling this function
    
    # Create a unique key for this feedback section
    # Include session_id, message_idx, AND sub_index for uniqueness
    session_id = st.session_state.get('session_id', 'default')
    
    if message_idx is not None:
        if sub_index is not None:
            # Multiple SQL results in same message - include sub_index
            feedback_key = f"feedback_{session_id}_msg_{message_idx}_sub_{sub_index}"
        else:
            # Single SQL result in message
            feedback_key = f"feedback_{session_id}_msg_{message_idx}"
    else:
        # Fallback for edge cases
        timestamp = int(time.time() * 1000)
        feedback_key = f"feedback_{session_id}_{timestamp}"
    
    print(f"🔑 Generated feedback key: {feedback_key} | message_idx={message_idx} | sub_index={sub_index}")
    
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
# SUMMARY OF CHANGES:
# ===================================================================
#
# 1. render_sql_results:
#    - Added sub_index=i when calling render_single_sql_result in the loop
#    - This passes 0, 1, 2, etc. for each SQL result in multiple_results
#
# 2. render_single_sql_result:
#    - Added sub_index=None parameter (default for single results)
#    - Passes sub_index to render_feedback_section
#
# 3. render_feedback_section:
#    - Added sub_index=None parameter
#    - Modified feedback_key generation to include sub_index when present
#    - Key format:
#      * Single SQL: "feedback_{session_id}_msg_{message_idx}"
#      * Multiple SQL: "feedback_{session_id}_msg_{message_idx}_sub_{sub_index}"
#
# RESULT:
# - Single question, single SQL: feedback_xxx_msg_2
# - Single question, multiple SQLs:
#   * SQL 1: feedback_xxx_msg_2_sub_0
#   * SQL 2: feedback_xxx_msg_2_sub_1
#   * SQL 3: feedback_xxx_msg_2_sub_2
#
# This ensures all feedback buttons have unique keys! ✅
#
# ===================================================================
