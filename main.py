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

# CSS with updated button styling for three buttons
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif !important;
    }
    
    /* Remove default Streamlit padding and margins */
    .block-container {
        padding-top: 0.5rem !important;
        padding-bottom: 1rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 1200px !important;
        background-color: #FAFAF8 !important;
    }
    
    body {
        background-color: #FAFAF8 !important;
    }
    
    /* Hide Streamlit header and footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Header section with logo only - no title */
    .header-section {
        display: flex;
        align-items: center;
        justify-content: flex-start;
        margin-bottom: 1rem;
        padding: 0.5rem 0;
        width: 100%;
    }
    
    .logo-container {
        flex: 0 0 auto;
    }
    
    /* Welcome section styling - refined cyan card to match chat style */
    .welcome-section {
        text-align: center;
        margin: 0.5rem 0;
        padding: 1.5rem 2rem;
        background: linear-gradient(135deg, #D9F6FA 0%, #FFFFFF 100%);
        border: 1px solid #D9F6FA;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    }
    
    .welcome-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: #D74120;
        margin-bottom: 0.3rem;
    }
    
    .welcome-subtitle {
        font-size: 1rem;
        color: #333;
        margin-bottom: 0.5rem;
        line-height: 1.3;
    }
    
    /* Team selection section */
    .team-selection-section {
        text-align: center;
        margin: 1.5rem 0;
        padding: 1.5rem 2rem;
        background: #FAFAF8;
        border-radius: 12px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        border-left: 4px solid #D74120;
    }
    
    .team-selection-title {
        font-size: 1.3rem;
        font-weight: 600;
        color: #333;
        margin-bottom: 1rem;
    }
    
    .team-note {
        font-size: 0.9rem;
        color: #333;
        background: linear-gradient(135deg, #D9F6FA 0%, #FFFFFF 100%);
        padding: 0.8rem;
        border-radius: 8px;
        margin-top: 1rem;
        text-align: left;
        line-height: 1.4;
        border-left: 4px solid #FF612B;
    }
    
    /* Button section - MOVED UP MORE */
    .button-section {
        text-align: center;
        padding: 0.5rem 0;
        margin-top: 0.5rem;
    }
    
    /* Button container for side-by-side layout - UPDATED FOR THREE BUTTONS */
    .button-container {
        display: flex;
        justify-content: center;
        gap: 1.5rem;
        margin-top: 1rem;
    }
    
    /* Individual button styling - ADJUSTED FOR THREE BUTTONS */
    .stButton > button {
        background: linear-gradient(45deg, #007bff, #0056b3) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 1rem 1.5rem !important;
        font-size: 1rem !important;
        font-weight: 600 !important;
        width: 180px !important;
        height: 65px !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 6px 20px rgba(0,123,255,0.3) !important;
        cursor: pointer !important;
    }
    
    .stButton > button:hover {
        background: linear-gradient(45deg, #0056b3, #004085) !important;
        transform: translateY(-3px) !important;
        box-shadow: 0 8px 25px rgba(0,123,255,0.4) !important;
    }
    
    .stButton > button:active {
        transform: translateY(-1px) !important;
    }
    
    /* Chat button - Optum orange gradient */
    .chat-button .stButton > button {
        background: linear-gradient(45deg, #D74120, #E85C3F) !important;
        color: #FFFFFF !important;
        box-shadow: 0 6px 20px rgba(215, 65, 32, 0.3) !important;
    }
    
    .chat-button .stButton > button:hover {
        background: linear-gradient(45deg, #B83419, #D74120) !important;
        color: #FFFFFF !important;
        box-shadow: 0 8px 25px rgba(215, 65, 32, 0.4) !important;
    }
    
    /* Disabled chat button */
    .chat-button-disabled .stButton > button {
        background: #6c757d !important;
        color: #ffffff !important;
        cursor: not-allowed !important;
        box-shadow: 0 6px 20px rgba(108,117,125,0.2) !important;
    }
    
    .chat-button-disabled .stButton > button:hover {
        background: #6c757d !important;
        transform: none !important;
        box-shadow: 0 6px 20px rgba(108,117,125,0.2) !important;
    }
    
    /* Metadata button - Blue gradient */
    .metadata-button .stButton > button {
        background: linear-gradient(45deg, #2196F3, #1976D2) !important;
        box-shadow: 0 6px 20px rgba(33, 150, 243, 0.3) !important;
    }
    
    .metadata-button .stButton > button:hover {
        background: linear-gradient(45deg, #1976D2, #1565C0) !important;
        box-shadow: 0 8px 25px rgba(33, 150, 243, 0.4) !important;
    }
    
    /* FAQ button - Green gradient */
    .faq-button .stButton > button {
        background: linear-gradient(45deg, #28a745, #20c997) !important;
        box-shadow: 0 6px 20px rgba(40, 167, 69, 0.3) !important;
    }
    
    .faq-button .stButton > button:hover {
        background: linear-gradient(45deg, #20c997, #17a2b8) !important;
        box-shadow: 0 8px 25px rgba(40, 167, 69, 0.4) !important;
    }
    
    /* Remove extra spacing from Streamlit columns */
    .stColumn {
        padding: 0 !important;
    }
    
    /* Image styling */
    .stImage {
        margin: 0 !important;
    }
    
    /* Button descriptions */
    .button-description {
        margin-top: 0.5rem;
        font-size: 0.9rem;
        color: #6c757d;
        text-align: center;
    }
    
    /* Reduce spacing between radio buttons and title */
    .stRadio {
        margin-top: 0 !important;
    }
    
    .stRadio > div {
        margin-top: 0 !important;
        padding-top: 0 !important;
    }
    
    /* Optum-styled radio buttons */
    .stRadio > label {
        font-size: 1rem !important;
        font-weight: 500 !important;
        color: #333 !important;
    }
    
    .stRadio > div > label > div {
        font-size: 1rem !important;
        color: #333 !important;
    }
    
    .stRadio [data-baseweb="radio"] {
        accent-color: #D74120 !important;
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
    
    # Header section with logo only (no title)
    st.markdown('<div class="header-section">', unsafe_allow_html=True)
    
    # Logo aligned to the left, user info on the right
    col1, col2 = st.columns([1, 5])
    
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
        # User info aligned to the right
        if user_info['authenticated']:
            st.markdown(f"""
            <div style="text-align: right; padding-top: 20px;">
                <span style="color: #002677; font-size: 0.9rem;">
                    👤 Logged in as: <strong>{user_info['email']}</strong>
                </span>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="text-align: right; padding-top: 20px;">
                <span style="color: #dc3545; font-size: 0.9rem;">
                    ⚠️ Authentication status unknown
                </span>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Welcome section (gray box) - REDUCED VERTICAL SIZE AND MOVED UP
    st.markdown("""
    <div class="welcome-section">
        <div class="welcome-title">
            Welcome to Finance Data Mart Analytics Assistant 
        </div>
        <div class="welcome-subtitle">
            Your DANA-powered assistant for healthcare finance data analysis and insights
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Team selection section - left aligned
    st.markdown('<div style="margin: 0.8rem 0 0.2rem 0;">', unsafe_allow_html=True)
    st.markdown('<p style="color: #333; margin-bottom: 0.2rem; font-size: 1.1rem; font-weight: 500;">Please select which team you belong to:</p>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Radio button for team selection - left aligned
    team_options = ["PBM Network", "Optum Pharmacy"]
    
    # Radio buttons left aligned without columns
    selected_team = st.radio(
        "Team Selection",
        options=team_options,
        index=None,
        key="team_radio",
        horizontal=True,
        label_visibility="hidden",
        on_change=None
    )
    
    # Update session state only when there's a selection
    if selected_team:
        st.session_state.selected_team = selected_team
    
    # Note about datasets - only show after team selection, with larger font
    if selected_team:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #D9F6FA 0%, #FFFFFF 100%); padding: 1rem; border-radius: 8px; margin: 0.5rem 0; color: #333; font-size: 1.0rem; line-height: 1.4; text-align: left; border-left: 4px solid #FF612B;">
            <strong style="color: #D74120;">📌 Note:</strong>
            <ul style="margin-left: 1.2em; margin-top: 0.5rem;">
                <li>Ledger datasets contain a combination of PBM, HDP, and Specialty information. Both teams will have access to the entire Ledger datasets.</li>
                <li>If you want to see individual product category level data (PBM, HDP, or Specialty), you can always add the filtering while prompting (e.g., "Show me revenue for PBM" or "Compare actuals vs forecast for Specialty").</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Button section with three options - moved up
    st.markdown('<div class="button-section" style="margin-top: 0.5rem;">', unsafe_allow_html=True)
    
    # Create columns for three buttons
    col1, col2, col3 = st.columns([1, 3, 1])
    
    with col2:
        # Create three sub-columns for the buttons
        btn_col1, btn_col2, btn_col3 = st.columns(3)
        
        with btn_col1:
            # Start Chat button - only enabled if team is selected
            if st.session_state.selected_team:
                st.markdown('<div class="chat-button">', unsafe_allow_html=True)
                if st.button("💬 Start Chat", key="start_chat"):
                    # Store selected team and user info in session state before switching pages
                    st.session_state.domain_selection = [st.session_state.selected_team]
                    # Pass user info to chat page
                    st.session_state.authenticated_user = user_info.get('email', 'Unknown User')
                    st.session_state.authenticated_user_name = user_info.get('name', 'Unknown User')
                    # DEBUG: Log what we're storing
                    print(f"🔍 DEBUG: Setting session_state.authenticated_user to: {st.session_state.authenticated_user}")
                    print(f"🔍 DEBUG: Setting session_state.authenticated_user_name to: {st.session_state.authenticated_user_name}")
                    st.switch_page("pages/chat.py")
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="chat-button-disabled">', unsafe_allow_html=True)
                st.button("💬 Start Chat", key="start_chat_disabled", disabled=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            st.markdown('<div class="button-description"></div>', unsafe_allow_html=True)

        with btn_col2:
            # Available Datasets button - only enabled if team is selected
            if st.session_state.selected_team:
                st.markdown('<div class="metadata-button">', unsafe_allow_html=True)
                if st.button("📊 Available Datasets", key="view_metadata"):
                    # Store selected team and user info in session state before switching pages
                    st.session_state.domain_selection = [st.session_state.selected_team]
                    # Pass user info to metadata page
                    st.session_state.authenticated_user = user_info.get('email', 'Unknown User')
                    st.session_state.authenticated_user_name = user_info.get('name', 'Unknown User')
                    # DEBUG: Log what we're storing
                    print(f"🔍 DEBUG: Setting session_state.authenticated_user to: {st.session_state.authenticated_user}")
                    print(f"🔍 DEBUG: Setting session_state.authenticated_user_name to: {st.session_state.authenticated_user_name}")
                    st.switch_page("pages/metadata.py")
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="chat-button-disabled">', unsafe_allow_html=True)
                st.button("📊 Available Datasets", key="view_metadata_disabled", disabled=True)
                st.markdown('</div>', unsafe_allow_html=True)
            st.markdown('<div class="button-description"></div>', unsafe_allow_html=True)
        
        with btn_col3:
            st.markdown('<div class="faq-button">', unsafe_allow_html=True)
            if st.button("❓ FAQ", key="view_faq"):
                st.switch_page("pages/faq.py")
            st.markdown('</div>', unsafe_allow_html=True)
            st.markdown('<div class="button-description"></div>', unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Knowledge base content is kept in the function but not displayed
    # knowledge_content = get_knowledge_base_content()  # Available but hidden

if __name__ == "__main__":
    main()
