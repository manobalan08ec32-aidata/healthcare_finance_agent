# ... (previous elif st.session_state.get('strategic_rendered', False): block ends) ...

        # --- FIX 2 (FINAL): Radio Button for Question Context with MINIMAL SPACING ---
        # We now use CSS to specifically reduce the margin of the radio component itself.
        st.markdown("""
        <style>
            /* Target the specific element containing the radio button options to reduce top margin */
            div[data-testid="stForm"] > div > div > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) {
                margin-top: 0px !important; 
                padding-top: 0px !important;
            }

            /* This targets the container holding the radio options to pull it up */
            div[data-testid="stForm"] > div > div > div:nth-child(2) > div:nth-child(2) {
                margin-top: -15px !important;
                padding-top: 0px !important;
            }
            
            /* Target the entire radio button container and reduce padding/margin */
            .stRadio {
                margin-bottom: -15px !important; 
            }
        </style>
        """, unsafe_allow_html=True)

        question_type = st.radio(
            "Select Question Context:", # Use this as the main label for tight integration
            ("Follow-up", "New Question"),
            index=0,  # Default to "Follow-up"
            horizontal=True,
            key="question_type_radio"
        )

        # Store the selection in session state so start_processing can access it
        st.session_state.question_type_selection = question_type
        # --- END FIX 2 (FINAL) ---
        
        # Chat input at the bottom
        if prompt := st.chat_input("Ask a question...", disabled=st.session_state.processing):
        # ... (rest of main function follows)
