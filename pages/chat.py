try:
        workflow = get_session_workflow()
        
        if workflow is None:
            st.error("Failed to initialize async workflow. Please refresh the page.")
            return

        # --- NEW CODE: Reset radio button to default "Follow-up" after processing ---
        # Check if processing just finished (st.session_state.processing is False) 
        # and the question type was set to 'New Question'
        if (not st.session_state.get('processing', True) and 
            st.session_state.get('question_type_selection') == 'New Question'):
            
            # This ensures the default selection is "Follow-up" for the next query.
            # We explicitly update the key used by the st.radio component.
            st.session_state.question_type_radio = 'Follow-up'
            st.session_state.question_type_selection = 'Follow-up'
            print("🔄 Resetting question_type to 'Follow-up' for next turn.")
        # --- END NEW CODE ---
        
        # Show spinner in main area while fetching session history
        # ... (rest of main function follows)

# ... (previous elif st.session_state.get('strategic_rendered', False): block ends) ...

        # --- UPDATED CODE: Radio Button for Question Context with MINIMAL SPACING ---
        st.markdown("""
        <div style="margin-top: 10px; margin-bottom: -15px; font-weight: 500;">
            Select Question Context:
        </div>
        """, unsafe_allow_html=True)
        
        question_type = st.radio(
            "", # Label removed and placed in markdown above for finer control
            ("Follow-up", "New Question"),
            index=0,  # Default to "Follow-up"
            horizontal=True,
            key="question_type_radio"
        )

        # Store the selection in session state so start_processing can access it
        st.session_state.question_type_selection = question_type
        # --- END UPDATED CODE ---
        
        # Chat input at the bottom
        if prompt := st.chat_input("Ask a question...", disabled=st.session_state.processing):
        # ... (rest of main function follows)

def render_persistent_followup_questions():
    """Render followup questions as simple styled buttons - left aligned like before"""
    
    # --- CRITICAL FIX: DO NOT RENDER BUTTONS WHILE PROCESSING ---
    # The condition is now explicit: Only show buttons if the app is NOT processing 
    # and the list of questions is NOT empty.
    if (st.session_state.get('processing', False)):
        return # Skip rendering entirely if processing is active

    # Don't show follow-up questions if the list is empty
    if (hasattr(st.session_state, 'current_followup_questions') and 
        st.session_state.current_followup_questions):

        # Display the follow-up questions with simple Streamlit buttons
        for idx, question in enumerate(st.session_state.current_followup_questions):
            # Create more unique key using session_id, timestamp, and question content
            session_id = st.session_state.get('session_id', 'default')
            message_count = len(st.session_state.get('messages', []))
            question_hash = abs(hash(question))
            button_key = f"followup_{session_id}_{message_count}_{idx}_{question_hash}"
            
            if st.button(
                question, 
                key=button_key,
                help=f"Click to explore: {question}",
                use_container_width=True
            ):
                print(f"🔄 Follow-up question clicked: {question}")
                # Clear the follow-up questions to hide buttons
                st.session_state.current_followup_questions = []
                
                # Remove the "Would you like to explore further?" message from chat history
                if (st.session_state.messages and 
                    st.session_state.messages[-1].get('message_type') == 'followup_questions'):
                    st.session_state.messages.pop()
                    print("🗑️ Removed follow-up intro message from chat history")
                
                # Start processing the selected question
                start_processing(question)
                st.rerun()
