import streamlit as st
import os

# Page configuration
st.set_page_config(
    page_title="FAQ - FDM Agent Portal",
    page_icon="❓",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS with Inter font and consistent styling - REMOVED WARM WHITE BACKGROUND
st.markdown("""
<style>
    /* Import Inter font from Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Remove default Streamlit padding and margins */
    .block-container {
        padding-top: 0.5rem !important;
        padding-bottom: 1rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 1400px !important;
    }
    
    /* Hide Streamlit header and footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Header section - MATCHING metadata.py structure */
    .header-section {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1rem;
        padding: 0.5rem 0;
        width: 100%;
    }
    
    /* Title container - CENTERED with negative margin like metadata.py */
    .title-container {
        flex: 1;
        text-align: center;
        margin-left: -120px; /* Same as metadata.py */
    }
    
    /* REDUCED: Main title size */
    .main-title {
        font-size: 1.8rem !important; /* Reduced from 2.5rem */
        font-weight: 700 !important;
        color: #002677;
        margin: 0;
        line-height: 1.2;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
    }
    
    /* Back button container */
    .back-button-header {
        flex: 0 0 auto;
    }
    
    /* FAQ content styling - REMOVED BACKGROUND, BORDER, PADDING */
    .faq-content {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif;
        line-height: 1.6;
        margin: 1rem 0;
    }
    
    /* REDUCED: Main title styling inside content */
    .faq-content h1 {
        color: #002677 !important;
        font-size: 1.4rem !important; /* Reduced from 1.8rem */
        font-weight: 700 !important;
        margin-bottom: 1.5rem !important;
        text-align: center !important;
        font-family: inherit !important;
        padding: 0 !important;
        background: none !important;
        border: none !important;
    }
    
    /* REDUCED: Section headings */
    .faq-content h2 {
        color: #002677 !important;
        font-size: 1.1rem !important; /* Reduced from 1.4rem */
        font-weight: 600 !important;
        margin-top: 1.5rem !important; /* Reduced from 2rem */
        margin-bottom: 0.8rem !important; /* Reduced from 1rem */
        font-family: inherit !important;
    }
    
    .faq-content h3 {
        color: #002677 !important;
        font-size: 1.0rem !important; /* Reduced from 1.2rem */
        font-weight: 600 !important;
        margin-top: 1.2rem !important; /* Reduced from 1.5rem */
        margin-bottom: 0.6rem !important; /* Reduced from 0.8rem */
        font-family: inherit !important;
    }
    
    /* Keep other text black */
    .faq-content p {
        color: #333;
        margin-bottom: 0.8rem; /* Reduced from 1rem */
        font-family: inherit;
        font-size: 0.95rem; /* Slightly smaller */
    }
    
    .faq-content ul, .faq-content ol {
        margin-bottom: 0.8rem; /* Reduced from 1rem */
        padding-left: 1.5rem;
    }
    
    .faq-content li {
        margin-bottom: 0.4rem; /* Reduced from 0.5rem */
        color: #333;
        font-family: inherit;
        font-size: 0.95rem; /* Slightly smaller */
    }
    
    .faq-content strong {
        font-weight: 700;
        color: #1a1a1a;
        font-family: inherit;
    }
    
    .faq-content code {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 4px;
        padding: 0.2rem 0.4rem;
        font-size: 0.9rem;
        color: #d63384;
        font-family: 'Courier New', monospace;
    }
    
    .faq-content hr {
        border: none;
        border-top: 2px solid #e0e0e0;
        margin: 2rem 0;
    }
    
    /* UPDATED: Back button styling - DARK BLUE matching metadata.py */
    .stButton > button {
        background: linear-gradient(45deg, #007bff, #0056b3) !important;
        color: white !important;
        border: none !important;
        border-radius: 25px !important;
        padding: 0.5rem 1.5rem !important;
        font-size: 0.9rem !important;
        font-weight: 500 !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 6px 20px rgba(0,123,255,0.3) !important;
        cursor: pointer !important;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    .stButton > button:hover {
        background: linear-gradient(45deg, #0056b3, #004085) !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 25px rgba(0,123,255,0.4) !important;
    }
    
    .stButton > button:active {
        transform: translateY(-1px) !important;
    }
</style>
""", unsafe_allow_html=True)

def load_faq_content():
    """Load FAQ content from markdown file"""
    try:
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        faq_path = os.path.join(current_dir, "assets", "faq.md")
        
        if os.path.exists(faq_path):
            with open(faq_path, 'r', encoding='utf-8') as file:
                return file.read()
        else:
            return "FAQ content not found. Please check if the faq.md file exists in the assets folder."
    except Exception as e:
        return f"Error loading FAQ content: {str(e)}"

def main():
    """FAQ page with content and navigation - MATCHING metadata.py structure"""
    
    # Header section with logo, title, and back button - SAME STRUCTURE AS metadata.py
    st.markdown('<div class="header-section">', unsafe_allow_html=True)
    
    col_logo, col_title, col_back = st.columns([1, 4, 1])
    
    with col_logo:
        # Logo
        try:
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            logo_path = os.path.join(current_dir, "assets", "optumrx_logo.png")
            
            if os.path.exists(logo_path):
                st.image(logo_path, width=120)
            else:
                st.markdown("🏥 **Logo**")
        except Exception as e:
            st.markdown("🏥 **OptumRx**")
    
    with col_title:
        st.markdown("""
        <div class="title-container">
            <div class="main-title">
                FAQ - Best Practices Guide
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col_back:
        if st.button("⬅️ Back to Main Page", key="back_to_main", help="Return to main page"):
            st.switch_page("main.py")
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Load and display FAQ content - NO BACKGROUND BOX
    faq_content = load_faq_content()
    
    # Display the markdown content directly without background container
    if "Error loading" not in faq_content:
        st.markdown(f'<div class="faq-content">{faq_content}</div>', unsafe_allow_html=True)
    else:
        st.error(faq_content)

if __name__ == "__main__":
    main()
