def render_sql_results(sql_result, rewritten_question=None, show_feedback=True, message_idx=None):
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
            
            # ⭐ Pass message_idx here
            render_single_sql_result(title, sql_query, data, narrative, user_question, show_feedback, message_idx)
    else:
        # Single result - use rewritten_question if available, otherwise default
        title = rewritten_question if rewritten_question else "Analysis Results"
        sql_query = sql_result.get('sql_query', '')
        data = sql_result.get('query_results', [])
        narrative = sql_result.get('narrative', '')
        
        # ⭐ Pass message_idx here
        render_single_sql_result(title, sql_query, data, narrative, user_question, show_feedback, message_idx)
