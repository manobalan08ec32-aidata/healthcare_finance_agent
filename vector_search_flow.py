# Custom CSS for chat interface (Your original CSS is preserved)
st.markdown("""
<style>
    /* Remove default Streamlit padding and margins */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        padding-left: 1rem;
        padding-right: 1rem;
        max-width: 100%;
    }
    
    /* Hide Streamlit header and footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Chat container styling */
    .chat-container {
        height: calc(100vh - 150px);
        overflow-y: auto;
        padding: 1rem;
        margin-bottom: 100px;
    }
    
    /* Chat input container - Fixed at bottom */
    .chat-input-container {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        background-color: white;
        border-top: 1px solid #e0e0e0;
        padding: 1rem;
        z-index: 1000;
        box-shadow: 0 -2px 10px rgba(0,0,0,0.1);
    }
    
    /* User message styling - Left aligned, full width */
    .user-message {
        display: flex;
        justify-content: flex-start;
        margin: 1rem 0;
    }
    
    /* MODIFICATION 1: User message background is now white with a border */
    .user-message-content {
        background-color: #ffffff;
        color: #333333;
        border: 1px solid #e0e0e0;
        padding: 12px 16px;
        border-radius: 18px;
        max-width: 95%;
        word-wrap: break-word;
    }
    
    /* Assistant message styling - Left aligned, full width */
    .assistant-message {
        display: flex;
        justify-content: flex-start;
        margin: 1rem 0;
    }
    .assistant-message-content {
        background-color: #f1f3f4;
        color: #333;
        padding: 12px 16px;
        border-radius: 18px;
        max-width: 95%;
        word-wrap: break-word;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
        font-size: 15px;
        line-height: 1.6;
        font-style: normal !important;
        font-weight: normal !important;
    }
    
    /* ... (rest of your CSS is preserved) ... */

    .followup-buttons-container {
        display: flex !important;
        flex-direction: column !important; /* Stack buttons vertically */
        gap: 8px !important;
        align-items: stretch !important; /* Make buttons same width */
        justify-content: flex-start !important;
        max-width: 95% !important;
        margin: 0.5rem 0 !important;
        padding: 0 !important;
    }

    /* MODIFICATION 2: Button styles updated for full width and text wrapping */
    .stButton > button {
        background-color: white !important;
        color: #007bff !important;
        border: 1px solid #007bff !important;
        border-radius: 8px !important;
        padding: 10px 16px !important;
        margin: 0 !important;
        width: 100% !important; /* Make button take full width of its container */
        text-align: left !important; /* Align text to the left */
        font-size: 14px !important;
        line-height: 1.4 !important;
        transition: all 0.2s ease !important;
        height: auto !important; /* Allow button to grow vertically */
        min-height: 40px !important;
        white-space: normal !important; /* Allow text to wrap */
        word-wrap: break-word !important;
    }

    .stButton > button:hover {
        background-color: #007bff !important;
        color: white !important;
        box-shadow: 0 1px 3px rgba(0,123,255,0.2) !important;
        transform: translateY(-0.5px) !important;
    }

    .stButton > button:focus {
        background-color: #007bff !important;
        color: white !important;
        box-shadow: 0 0 0 2px rgba(0,123,255,0.25) !importan
    }
</style>
""", unsafe_allow_html=True)
