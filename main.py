
import streamlit as st
import os

# Page configuration
st.set_page_config(
    page_title="FDM Agent Portal",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Simple CSS - FIXED ALIGNMENT ISSUES
st.markdown("""
<style>
    /* Remove default Streamlit padding and margins */
    .block-container {
        padding-top: 0.5rem !important;
        padding-bottom: 1rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 1200px !important;
        height: 100vh !important;  /* Full viewport height */
    }
    
    /* Hide Streamlit header and footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* FIXED: Header section with logo and title on same line */
    .header-section {
        display: flex;
        align-items: center;  /* Center align vertically */
        justify-content: space-between;  /* Space between logo and title */
        margin-bottom: 1rem;
        padding: 0.5rem 0;
        width: 100%;
    }
    
    .logo-container {
        flex: 0 0 auto;  /* Don't grow or shrink */
    }
    
    .title-container {
        flex: 1;
        text-align: center;  /* Center the title */
        margin-left: -120px;  /* Offset logo width for perfect centering */
    }
    
    /* FIXED: Title styling - same line as logo */
    .main-title {
        font-size: 2.5rem;
        font-weight: 700 !important;      
        color: #333;
        margin: 0;
        line-height: 1.2;
    }
    
    /* FIXED: Content section - expanded horizontally */
    .content-section {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        margin: 0 auto 1rem auto;  /* Less bottom margin */
        border-left: 3px solid #007bff;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        width: 100%;                /* Expanded to 98% width */
        height: calc(100vh - 235px); /* Full height minus space for title and button */
        max-height: calc(100vh - 235px);
        overflow: hidden;          /* Hide outer overflow */
        display: flex;
        flex-direction: column;
    }
    
    .content-title {
        font-size: 1.5rem;
        font-weight: 600;
        color: #333;
        margin-bottom: 1rem;
        border-bottom: 2px solid #007bff;
        padding-bottom: 0.5rem;
        text-align: center;
        flex-shrink: 0;  /* Don't shrink the title */
    }
    
    /* FIXED: Content text with very visible scrollbar and markdown styling */
    .content-text {
        font-size: 1rem;
        line-height: 1.6;
        color: #555;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
        text-align: left;
        overflow-y: auto;          /* Enable vertical scrolling */
        flex: 1;                   /* Take remaining space */
        padding-right: 1rem;       /* Space for scrollbar */
        
        /* VERY VISIBLE SCROLLBAR STYLING */
        scrollbar-width: thick;     /* Firefox - thick scrollbar */
        scrollbar-color: #007bff #e0e0e0;  /* Firefox - blue thumb, gray track */
    }
    
    /* Markdown styling within content */
    .content-text h2 {
        font-size: 1.4rem;
        font-weight: 600;
        color: #007bff;
        margin: 1.5rem 0 1rem 0;
        border-bottom: 2px solid #007bff;
        padding-bottom: 0.5rem;
    }
    
    .content-text h3 {
        font-size: 1.2rem;
        font-weight: 600;
        color: #0056b3;
        margin: 1.5rem 0 0.8rem 0;
    }
    
    .content-text p {
        margin: 0.8rem 0;
        line-height: 1.6;
    }
    
    .content-text strong {
        font-weight: 700;
        color: #333;
    }
    
    .content-text ul {
        margin: 1rem 0;
        padding-left: 1.5rem;
    }
    
    .content-text li {
        margin: 0.5rem 0;
        line-height: 1.6;
    }
    
    .content-text hr {
        border: none;
        border-top: 2px solid #e0e0e0;
        margin: 2rem 0;
    }
    
    /* VERY VISIBLE SCROLLBAR FOR WEBKIT BROWSERS (Chrome, Safari, Edge) */
    .content-text::-webkit-scrollbar {
        width: 16px !important;     /* Wide scrollbar */
    }
    
    .content-text::-webkit-scrollbar-track {
        background: #e0e0e0 !important;     /* Gray track */
        border-radius: 8px !important;
    }
    
    .content-text::-webkit-scrollbar-thumb {
        background: #007bff !important;     /* Blue thumb */
        border-radius: 8px !important;
        border: 2px solid #e0e0e0 !important;  /* Border for contrast */
    }
    
    .content-text::-webkit-scrollbar-thumb:hover {
        background: #0056b3 !important;     /* Darker blue on hover */
    }
    
    /* Button section - fixed at bottom */
    .button-section {
        text-align: center;
        padding: 1rem 0;           /* Reduced padding */
        margin-top: auto;          /* Push to bottom */
    }
    
    /* Simple button styling */
    .stButton > button {
        background-color: #007bff !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.75rem 2rem !important;
        font-size: 1.1rem !important;
        font-weight: 500 !important;
        width: 200px !important;
        height: 55px !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 4px 12px rgba(0,123,255,0.3) !important;
    }
    
    .stButton > button:hover {
        background-color: #0056b3 !important;
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 16px rgba(0,123,255,0.4) !important;
    }
    
    /* FIXED: Remove extra spacing from Streamlit columns */
    .stColumn {
        padding: 0 !important;
    }
    
    /* FIXED: Image styling */
    .stImage {
        margin: 0 !important;
    }
    
    /* ENSURE EVERYTHING FITS ON PAGE */
    .main-container {
        height: 100vh;
        display: flex;
        flex-direction: column;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

def get_hardcoded_content():
    """Return hardcoded markdown content as HTML"""
    return """
<h3>Important Note</h3>
<p>The agent's knowledge is limited to the below datasets, attributes, and metrics. For additional data needs or to expand its capabilities, please contact the FDMTeam.</p>
<hr>
<p>The current agent supports Pharmacy and PBM teams by answering "what" and "drill-through" questions using two foundational datasets built for <strong>Specialty</strong>, <strong>Home Delivery</strong>, and <strong>PBM</strong> segments.</p>
<hr>
<h3>1. Actuals vs Forecast Analysis Dataset</h3>

<p>
An analytics-ready dataset for comparing actual results, forecast scenarios (8+4, 2+10, 5+7), and budget plans (BUDGET, GAAP) across key pharmacy and healthcare performance metrics. The measures span prescription counts (total, adjusted, 30-day, 90-day), revenue, cost of goods sold (COGS) after reclassification, SG&A after reclassification, IOI, and total membership. Each record contains a single value for the selected measure, enabling flexible aggregation and comparison. Analysis can be segmented by line of business (e.g., Community & State, Employer & Individual, Medicare & Retirement, Optum, External), product category and its sub-categories, state or region, and standard time periods (date, year, month, quarter). This table supports use cases such as variance analysis between actuals and forecast/budget, mix shift tracking by LOB or product category, and trend reporting where the same measure is evaluated across multiple timeframes.
</p>

<hr>

<h3>2. Pharmacy Claim Transaction Dataset</h3>

<p>
A detailed claim-level dataset capturing pharmacy benefit manager (PBM) activity, with one record per submitted claim. It includes claim identifiers for tracking individual transactions, submission dates for time-based analysis, and claim status indicators showing whether a claim was paid, reversed, or rejected (with paid and reversed used for net calculations). Business context is provided through line of business, client, carrier, account, and group details. Pharmacy-related fields capture the dispensing location name and national provider identifier (NPI). Drug-related fields include the medication name, therapeutic class, brand/generic classification, generic product identifier (GPI), national drug code (NDC), and manufacturer name. Core utilization metrics track total prescriptions, adjusted counts, and 30-day / 90-day fills. Financial metrics include revenue, expenses/COGS, wholesale acquisition cost (WAC), and average wholesale price (AWP), with derived indicators like revenue per prescription and generic dispense rate (GDR). Typical analyses include prescription volume and mix trends, brand vs generic comparisons, LOB and client performance, geographic patterns, and financial performance by drug or pharmacy.</p>
"""


def main():
    """Main landing page with horizontally aligned logo/title, expanded card, and visible button"""
    
    # FIXED: Header section with logo and title on same horizontal line
    st.markdown('<div class="header-section">', unsafe_allow_html=True)
    
    # Create columns for logo and title - same line
    col1, col2 = st.columns([1, 6])  # Adjusted proportions
    
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
    
    # Content section with hardcoded content - properly formatted markdown
    content_html = get_hardcoded_content()
    
    st.markdown(f"""
    <div class="content-section">
        <div class="content-title">
            Agent Knowledge Summary for Finance Users
        </div>
        <div class="content-text">
            {content_html}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Button section - visible at bottom
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
