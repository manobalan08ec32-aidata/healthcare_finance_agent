import streamlit as st
import os

# Page configuration
st.set_page_config(
    page_title="FDM Agent Portal",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS with knowledge base styling
st.markdown("""
<style>
    /* Remove default Streamlit padding and margins */
    .block-container {
        padding-top: 0.5rem !important;
        padding-bottom: 1rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 1200px !important;
    }
    
    /* Hide Streamlit header and footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Header section with logo and title on same line */
    .header-section {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1.5rem;
        padding: 0.5rem 0;
        width: 100%;
    }
    
    .logo-container {
        flex: 0 0 auto;
    }
    
    .title-container {
        flex: 1;
        text-align: center;
        margin-left: -120px; /* Offset logo width for perfect centering */
    }
    
    /* Title styling - same line as logo */
    .main-title {
        font-size: 2.5rem;
        font-weight: 700 !important;
        color: #333;
        margin: 0;
        line-height: 1.2;
    }
    
    /* Knowledge base content styling */
    .knowledge-section {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 2rem;
        margin: 0 auto 2rem auto;
        border-left: 4px solid #007bff;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        width: 100%;
        max-height: 70vh;
        overflow-y: auto;
        
        /* Scrollbar styling */
        scrollbar-width: thick;
        scrollbar-color: #007bff #e0e0e0;
    }
    
    .knowledge-section::-webkit-scrollbar {
        width: 12px;
    }
    
    .knowledge-section::-webkit-scrollbar-track {
        background: #e0e0e0;
        border-radius: 6px;
    }
    
    .knowledge-section::-webkit-scrollbar-thumb {
        background: #007bff;
        border-radius: 6px;
    }
    
    .knowledge-section::-webkit-scrollbar-thumb:hover {
        background: #0056b3;
    }
    
    .knowledge-title {
        font-size: 1.8rem;
        font-weight: 600;
        color: #007bff;
        margin-bottom: 1rem;
        border-bottom: 2px solid #007bff;
        padding-bottom: 0.5rem;
        text-align: center;
    }
    
    /* Content styling */
    .knowledge-section h2 {
        font-size: 1.4rem;
        font-weight: 600;
        color: #007bff;
        margin: 2rem 0 1rem 0;
        border-bottom: 2px solid #007bff;
        padding-bottom: 0.5rem;
    }
    
    .knowledge-section h3 {
        font-size: 1.2rem;
        font-weight: 600;
        color: #0056b3;
        margin: 1.5rem 0 0.8rem 0;
    }
    
    .knowledge-section p {
        margin: 0.8rem 0;
        line-height: 1.6;
        color: #555;
    }
    
    .knowledge-section strong {
        font-weight: 700;
        color: #333;
    }
    
    .knowledge-section ul {
        margin: 1rem 0;
        padding-left: 1.5rem;
    }
    
    .knowledge-section li {
        margin: 0.5rem 0;
        line-height: 1.6;
        color: #555;
    }
    
    .knowledge-section hr {
        border: none;
        border-top: 2px solid #e0e0e0;
        margin: 2rem 0;
    }
    
    /* Important note styling */
    .important-note {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 1.5rem;
    }
    
    .important-note h3 {
        color: #856404 !important;
        margin-top: 0 !important;
    }
    
    .important-note p {
        color: #856404 !important;
        margin-bottom: 0;
    }
    
    /* Button section */
    .button-section {
        text-align: center;
        padding: 2rem 0 1rem 0;
    }
    
    /* Button styling */
    .stButton > button {
        background-color: #007bff !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 1rem 3rem !important;
        font-size: 1.2rem !important;
        font-weight: 500 !important;
        width: 250px !important;
        height: 65px !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 4px 12px rgba(0,123,255,0.3) !important;
    }
    
    .stButton > button:hover {
        background-color: #0056b3 !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 16px rgba(0,123,255,0.4) !important;
    }
    
    /* Remove extra spacing from Streamlit columns */
    .stColumn {
        padding: 0 !important;
    }
    
    /* Image styling */
    .stImage {
        margin: 0 !important;
    }
</style>
""", unsafe_allow_html=True)

def get_knowledge_base_content():
    """Return the organized knowledge base content - simplified"""
    return """
    <div class="important-note">
        <h3>Important Note</h3>
        <p>The agent's knowledge is limited to the below attributes and metrics. For additional data needs, contact the FDMTeam.</p>
    </div>
    
    <p>The agent supports Pharmacy and PBM teams using two datasets for <strong>Specialty</strong>, <strong>Home Delivery</strong>, and <strong>PBM</strong> segments.</p>
    
    <h2>📊 Available Datasets</h2>
    <h3>1. Actuals vs Forecast Analysis Dataset</h3>
    <h3>2. Pharmacy Claim Transaction Dataset</h3>
    
    <h2>🏷️ Available Attributes</h2>
    <p><strong>Business Context:</strong> Line of Business, Product Category, Client/Carrier/Account/Group, State/Region, Time periods (Date/Year/Month/Quarter)</p>
    <p><strong>Pharmacy:</strong> Dispensing Location, National Provider Identifier (NPI)</p>
    <p><strong>Drug:</strong> Medication Name, Therapeutic Class, Brand/Generic Classification, GPI, NDC, Manufacturer</p>
    <p><strong>Claims:</strong> Claim Identifiers, Submission Dates, Status (Paid/Reversed/Rejected)</p>
    
    <h2>📈 Available Metrics</h2>
    <p><strong>Utilization:</strong> Total Prescriptions, Adjusted Counts, 30-day/90-day Fills, Total Membership</p>
    <p><strong>Financial:</strong> Revenue, COGS (after reclassification), SG&A (after reclassification), WAC, AWP, IOI</p>
    <p><strong>Derived:</strong> Revenue per Prescription, Generic Dispense Rate (GDR), Variance Analysis, Mix Shift Analysis</p>
    """

def main():
    """Main landing page with logo/title, knowledge base, and chat button"""
    
    # Header section with logo and title on same horizontal line
    st.markdown('<div class="header-section">', unsafe_allow_html=True)
    
    # Create columns for logo and title - same line
    col1, col2 = st.columns([1, 6])
    
    with col1:
        # Logo
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            logo_path = os.path.join(current_dir, "assets", "optumrx_logo.png")
            
            if os.path.exists(logo_path):
                st.image(logo_path, width=120)
            else:
                st.markdown("🏥 **Logo**")
        except Exception as e:
            st.markdown("🏥 **OptumRx**")
    
    with col2:
        # Title - horizontally aligned with logo
        st.markdown("""
        <div class="title-container">
            <div class="main-title">
                Talk to FDM Agent
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Knowledge base section
    knowledge_content = get_knowledge_base_content()
    
    st.markdown(f"""
    <div class="knowledge-section">
        <div class="knowledge-title">
            Agent Knowledge Summary for Finance Users
        </div>
        {knowledge_content}
    </div>
    """, unsafe_allow_html=True)
    
    # Button section at bottom
    st.markdown('<div class="button-section">', unsafe_allow_html=True)
    
    # Center the button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Start Chat", key="start_chat"):
            # Navigate to chat page in pages directory
            st.switch_page("pages/chat.py")
    
    st.markdown('</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()
