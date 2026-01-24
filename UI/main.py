import streamlit as st
import os

# Page configuration
st.set_page_config(
    page_title="FDM Agent Portal",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

def get_azure_user_info():
    """
    Extract user information from Azure App Service authentication headers
    Returns a dictionary with user information
    """
    try:
        # Azure App Service injects user info in these headers when authentication is enabled
        headers = st.context.headers if hasattr(st.context, 'headers') else {}
        
        # DEBUG: Print all available headers
        print(f"🔍 DEBUG: Available headers: {list(headers.keys()) if headers else 'None'}")
        
        # Try different header names that Azure App Service uses
        user_email = None
        user_name = None
        
        # Common Azure App Service authentication headers
        possible_email_headers = [
            'X-MS-CLIENT-PRINCIPAL-NAME',  # Usually contains email
            'X-MS-CLIENT-PRINCIPAL-ID',    # User ID
            'X-MS-TOKEN-AAD-ID-TOKEN',     # JWT token (needs decoding)
        ]
        
        possible_name_headers = [
            'X-MS-CLIENT-PRINCIPAL-NAME',
            'X-MS-CLIENT-DISPLAY-NAME'
        ]
        
        # Try to get email from headers
        for header in possible_email_headers:
            if header in headers:
                user_email = headers[header]
                print(f"✅ Found email in header '{header}': {user_email}")
                break
        
        # Try to get name from headers
        for header in possible_name_headers:
            if header in headers:
                user_name = headers[header]
                print(f"✅ Found name in header '{header}': {user_name}")
                break
        
        # Fallback: try Streamlit's request headers directly
        if not user_email:
            print("⚠️ Email not found in st.context.headers, trying server request headers...")
            try:
                import streamlit.web.server.server as server
                session = server.Server.get_current()._get_session_info()
                if session and hasattr(session, 'request'):
                    request_headers = session.request.headers
                    print(f"🔍 DEBUG: Request headers: {list(request_headers.keys()) if request_headers else 'None'}")
                    for header in possible_email_headers:
                        if header in request_headers:
                            user_email = request_headers[header]
                            print(f"✅ Found email in request header '{header}': {user_email}")
                            break
            except Exception as e:
                print(f"⚠️ Could not access server request headers: {e}")
        
        result = {
            'email': user_email or 'Unknown User',
            'name': user_name or user_email or 'Unknown User',
            'authenticated': user_email is not None
        }
        
        print(f"🔍 DEBUG: Final user_info result: {result}")
        return result
        
    except Exception as e:
        print(f"❌ Error getting Azure user info: {e}")
        import traceback
        print(f"🔍 Full traceback: {traceback.format_exc()}")
        return {
            'email': 'Unknown User',
            'name': 'Unknown User', 
            'authenticated': False,
            'error': str(e)
        }

# CSS with new clean design using Optum colors
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    /* Remove default Streamlit padding and margins */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 1rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 900px !important;
        background-color: #FAF8F2 !important;
    }
    
    body, .stApp {
        background-color: #FAF8F2 !important;
    }
    
    /* Hide Streamlit header and footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Header section */
    .header-section {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1.5rem;
        padding: 0.5rem 0;
        width: 100%;
    }
    
    /* Auth status styling */
    .auth-status {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 13px;
    }
    
    .auth-status.authenticated {
        color: #38A169;
    }
    
    .auth-status.warning {
        color: #D74120;
    }
    
    /* Welcome Banner - Clean cyan gradient */
    .welcome-banner {
        background: linear-gradient(135deg, #D9F6FA 0%, #FFFFFF 100%);
        border: 1px solid #D9F6FA;
        border-radius: 12px;
        padding: 28px 32px;
        text-align: center;
        margin-bottom: 28px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
    }
    
    .welcome-title {
        font-size: 24px;
        font-weight: 700;
        color: #D74120;
        margin-bottom: 8px;
    }
    
    .welcome-subtitle {
        font-size: 15px;
        color: #4A5568;
        line-height: 1.5;
        margin: 0;
    }
    
    /* Team Selection Section */
    .section-label {
        font-size: 15px;
        font-weight: 500;
        color: #2D3748;
        margin-bottom: 12px;
    }
    
    /* Team cards container */
    .team-cards-container {
        display: flex;
        gap: 16px;
        margin-bottom: 20px;
    }
    
    /* Style Streamlit radio buttons as cards */
    .stRadio > div {
        display: flex !important;
        flex-direction: row !important;
        gap: 16px !important;
    }
    
    .stRadio > div > label {
        flex: 1 !important;
        padding: 16px 20px !important;
        border: 2px solid #EDE8E0 !important;
        border-radius: 10px !important;
        cursor: pointer !important;
        transition: all 0.2s ease !important;
        background: #FFFFFF !important;
        display: flex !important;
        align-items: center !important;
        gap: 12px !important;
        margin: 0 !important;
    }
    
    .stRadio > div > label:hover {
        border-color: #FFD1AB !important;
        background: #FFFAF7 !important;
    }
    
    .stRadio > div > label[data-checked="true"],
    .stRadio > div > label:has(input:checked) {
        border-color: #FF612B !important;
        background: #FFFAF7 !important;
        box-shadow: 0 0 0 3px rgba(255, 97, 43, 0.12) !important;
    }
    
    .stRadio > div > label > div:first-child {
        /* Radio button circle */
        width: 20px !important;
        height: 20px !important;
        border: 2px solid #CBD5E0 !important;
        border-radius: 50% !important;
    }
    
    .stRadio > div > label[data-checked="true"] > div:first-child,
    .stRadio > div > label:has(input:checked) > div:first-child {
        border-color: #FF612B !important;
        background: #FF612B !important;
    }
    
    .stRadio > div > label > div:last-child {
        font-size: 15px !important;
        font-weight: 600 !important;
        color: #2D3748 !important;
    }
    
    /* Note Box */
    .note-box {
        background: linear-gradient(135deg, #D9F6FA 0%, #FFFFFF 100%);
        border-left: 4px solid #FF612B;
        border-radius: 0 8px 8px 0;
        padding: 14px 18px;
        margin: 16px 0 24px 0;
    }
    
    .note-title {
        font-size: 13px;
        font-weight: 600;
        color: #D74120;
        margin-bottom: 8px;
    }
    
    .note-content {
        font-size: 13px;
        color: #4A5568;
        line-height: 1.6;
    }
    
    .note-content ul {
        margin: 8px 0 0 20px;
        padding: 0;
    }
    
    .note-content li {
        margin-bottom: 6px;
    }
    
    /* Go to Chat Button - Single centered button */
    .chat-button-container {
        text-align: center;
        margin: 24px 0 32px 0;
    }
    
    .chat-button-container .stButton > button {
        background: linear-gradient(135deg, #D74120 0%, #FF612B 100%) !important;
        color: #FFFFFF !important;
        border: none !important;
        border-radius: 10px !important;
        padding: 14px 48px !important;
        font-size: 15px !important;
        font-weight: 600 !important;
        cursor: pointer !important;
        transition: all 0.2s ease !important;
        box-shadow: 0 4px 14px rgba(215, 65, 32, 0.25) !important;
        min-width: 180px !important;
    }
    
    .chat-button-container .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 20px rgba(215, 65, 32, 0.35) !important;
    }
    
    /* Disabled state */
    .chat-button-disabled .stButton > button {
        background: #A0AEC0 !important;
        cursor: not-allowed !important;
        box-shadow: none !important;
    }
    
    .chat-button-disabled .stButton > button:hover {
        transform: none !important;
        box-shadow: none !important;
    }
    
    /* Divider */
    .section-divider {
        height: 1px;
        background: #EDE8E0;
        margin: 32px 0;
    }
    
    /* Resources Section */
    .resources-section {
        margin-bottom: 32px;
    }
    
    .resources-title {
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: #718096;
        margin-bottom: 16px;
    }
    
    /* Hyperlink style for resources */
    .resource-links {
        display: flex;
        gap: 40px;
        justify-content: flex-start;
    }
    
    .resource-link {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        font-size: 14px;
        color: #D74120;
        text-decoration: none;
        font-weight: 500;
        padding: 8px 0;
        border-bottom: 1px solid transparent;
        transition: all 0.2s ease;
        cursor: pointer;
    }
    
    .resource-link:hover {
        color: #FF612B;
        border-bottom-color: #FF612B;
    }
    
    .resource-link svg {
        width: 18px;
        height: 18px;
    }
    
    /* Make Streamlit buttons look like hyperlinks */
    .hyperlink-button .stButton > button {
        background: none !important;
        border: none !important;
        color: #D74120 !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        padding: 8px 0 !important;
        box-shadow: none !important;
        text-decoration: none !important;
        border-bottom: 1px solid transparent !important;
        border-radius: 0 !important;
        min-width: auto !important;
        width: auto !important;
    }
    
    .hyperlink-button .stButton > button:hover {
        color: #FF612B !important;
        border-bottom-color: #FF612B !important;
        background: none !important;
        transform: none !important;
        box-shadow: none !important;
    }
    
    /* Feature Cards */
    .feature-cards {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 16px;
        margin-top: 24px;
    }
    
    .feature-card {
        background: #FFFFFF;
        border: 1px solid #EDE8E0;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        transition: all 0.2s ease;
    }
    
    .feature-card:hover {
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.06);
        transform: translateY(-2px);
    }
    
    .feature-icon {
        width: 40px;
        height: 40px;
        margin: 0 auto 12px;
        border-radius: 10px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 20px;
    }
    
    .feature-icon.cyan {
        background: #D9F6FA;
    }
    
    .feature-icon.orange {
        background: #FFD1AB;
    }
    
    .feature-icon.gray {
        background: #EDE8E0;
    }
    
    .feature-title {
        font-size: 13px;
        font-weight: 600;
        color: #2D3748;
        margin-bottom: 4px;
    }
    
    .feature-desc {
        font-size: 12px;
        color: #718096;
        line-height: 1.4;
    }
    
    /* Footer */
    .footer {
        text-align: center;
        padding: 20px 0;
        border-top: 1px solid #EDE8E0;
        margin-top: 40px;
    }
    
    .footer-text {
        font-size: 12px;
        color: #A0AEC0;
    }
    
    /* Hide radio label */
    .stRadio > label {
        display: none !important;
    }
    
    /* Image styling */
    .stImage {
        margin: 0 !important;
    }
    
    /* Column padding adjustment */
    [data-testid="column"] {
        padding: 0 !important;
    }
</style>
""", unsafe_allow_html=True)

def get_knowledge_base_content():
    """Return the organized knowledge base content - by dataset (kept but hidden)"""
    return """
    <div class="important-note">
        <h3>Important Note</h3>
        <p>The agent's knowledge is limited to the below attributes and metrics. For additional data needs, contact the FDMTeam.</p>
    </div>
    
    <h2>📊 Dataset 1: Actuals vs Forecast Analysis</h2>
    <p><strong>Attributes:</strong> Line of Business, Product Category, State/Region, Time periods (Date/Year/Month/Quarter), Forecast scenarios (8+4, 2+10, 5+7), Budget plans (BUDGET, GAAP)</p>
    <p><strong>Metrics:</strong> Total Prescriptions, Adjusted Counts, 30-day/90-day Fills, Revenue, COGS (after reclassification), SG&A (after reclassification), IOI, Total Membership, Variance Analysis</p>
    
    <h2>📊 Dataset 2: PBM & Pharmacy Claim Transaction</h2>
    <p><strong>Attributes:</strong> Line of Business, Client/Carrier/Account/Group, Dispensing Location, NPI, Medication Name, Therapeutic Class, Brand/Generic, GPI, NDC, Manufacturer, Claim ID, Submission Date, Status,Client Type,Pharmacy Type,Member Age group,State (Paid/Reversed/Rejected)</p>
    <p><strong>Metrics:</strong> Total Prescriptions, Adjusted Counts, 30-day/90-day Fills, Revenue, COGS, WAC, AWP, Revenue per Prescription, Generic Dispense Rate (GDR)</p>
    """

def main():
    """Main landing page with logo and welcome section"""
    
    # Get Azure user information
    user_info = get_azure_user_info()
    
    # Initialize session state for team selection
    if 'selected_team' not in st.session_state:
        st.session_state.selected_team = None
    
    # Store user info in session state for use in other pages
    st.session_state.user_info = user_info
    
    # Header section with logo and auth status
    col1, col2 = st.columns([1, 4])
    
    with col1:
        # Logo
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            logo_path = os.path.join(current_dir, "assets", "optumrx_logo.png")
            
            if os.path.exists(logo_path):
                st.image(logo_path, width=120)
            else:
                st.markdown('<span style="font-size: 22px; font-weight: 700; color: #FF612B;">Optum</span><span style="font-size: 22px; color: #2D3748;">Rx</span>', unsafe_allow_html=True)
        except Exception as e:
            st.markdown('<span style="font-size: 22px; font-weight: 700; color: #FF612B;">Optum</span><span style="font-size: 22px; color: #2D3748;">Rx</span>', unsafe_allow_html=True)
    
    with col2:
        # User info aligned to the right
        if user_info['authenticated']:
            st.markdown(f"""
            <div style="text-align: right; padding-top: 10px;">
                <span style="color: #38A169; font-size: 13px; display: inline-flex; align-items: center; gap: 8px;">
                    <span style="width: 8px; height: 8px; background: #38A169; border-radius: 50%; display: inline-block;"></span>
                    👤 Logged in as: <strong>{user_info['email']}</strong>
                </span>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="text-align: right; padding-top: 10px;">
                <span style="color: #D74120; font-size: 13px;">
                    ⚠️ Authentication status unknown
                </span>
            </div>
            """, unsafe_allow_html=True)
    
    # Welcome Banner
    st.markdown("""
    <div class="welcome-banner">
        <div class="welcome-title">Welcome to Finance Data Mart Analytics Assistant</div>
        <p class="welcome-subtitle">Your DANA-powered assistant for healthcare finance data analysis and insights</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Team selection section
    st.markdown('<p class="section-label">Please select which team you belong to:</p>', unsafe_allow_html=True)
    
    # Radio button for team selection
    team_options = ["PBM Network", "Optum Pharmacy"]
    
    selected_team = st.radio(
        "Team Selection",
        options=team_options,
        index=None,
        key="team_radio",
        horizontal=True,
        label_visibility="hidden"
    )
    
    # Update session state when there's a selection
    if selected_team:
        st.session_state.selected_team = selected_team
    
    # Note about datasets - only show after team selection
    if selected_team:
        st.markdown("""
        <div class="note-box">
            <div class="note-title">📌 Note</div>
            <div class="note-content">
                <ul>
                    <li>Ledger datasets contain a combination of PBM, HDP, and Specialty information. Both teams will have access to the entire Ledger datasets.</li>
                    <li>If you want to see individual product category level data (PBM, HDP, or Specialty), you can always add the filtering while prompting (e.g., "Show me revenue for PBM" or "Compare actuals vs forecast for Specialty").</li>
                </ul>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Go to Chat Button - centered
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.session_state.selected_team:
            st.markdown('<div class="chat-button-container">', unsafe_allow_html=True)
            if st.button("💬 Go to Chat", key="start_chat", use_container_width=True):
                # Store selected team and user info in session state before switching pages
                st.session_state.domain_selection = [st.session_state.selected_team]
                st.session_state.authenticated_user = user_info.get('email', 'Unknown User')
                st.session_state.authenticated_user_name = user_info.get('name', 'Unknown User')
                print(f"🔍 DEBUG: Setting session_state.authenticated_user to: {st.session_state.authenticated_user}")
                print(f"🔍 DEBUG: Setting session_state.authenticated_user_name to: {st.session_state.authenticated_user_name}")
                st.switch_page("pages/chat.py")
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="chat-button-disabled">', unsafe_allow_html=True)
            st.button("💬 Go to Chat", key="start_chat_disabled", disabled=True, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Divider
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Resources Section - Horizontal hyperlinks
    st.markdown('<p class="resources-title">Resources</p>', unsafe_allow_html=True)
    
    # Create horizontal layout for hyperlinks
    link_col1, link_col2, link_col3 = st.columns([1, 1, 2])
    
    with link_col1:
        st.markdown('<div class="hyperlink-button">', unsafe_allow_html=True)
        if st.button("📊 Available Datasets", key="view_metadata"):
            if st.session_state.selected_team:
                st.session_state.domain_selection = [st.session_state.selected_team]
            st.session_state.authenticated_user = user_info.get('email', 'Unknown User')
            st.session_state.authenticated_user_name = user_info.get('name', 'Unknown User')
            st.switch_page("pages/metadata.py")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with link_col2:
        st.markdown('<div class="hyperlink-button">', unsafe_allow_html=True)
        if st.button("❓ FAQ", key="view_faq"):
            st.switch_page("pages/faq.py")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Feature Cards
    st.markdown("""
    <div class="feature-cards">
        <div class="feature-card">
            <div class="feature-icon cyan">📦</div>
            <div class="feature-title">Multiple Datasets</div>
            <div class="feature-desc">Claims, billing, ledger & more</div>
        </div>
        <div class="feature-card">
            <div class="feature-icon orange">⭐</div>
            <div class="feature-title">AI-Powered</div>
            <div class="feature-desc">Natural language to SQL</div>
        </div>
        <div class="feature-card">
            <div class="feature-icon gray">🔒</div>
            <div class="feature-title">Secure</div>
            <div class="feature-desc">Enterprise-grade security</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("""
    <div class="footer">
        <p class="footer-text">DANA v2.4 • Powered by Claude AI</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Knowledge base content is kept in the function but not displayed
    # knowledge_content = get_knowledge_base_content()  # Available but hidden

if __name__ == "__main__":
    main()
