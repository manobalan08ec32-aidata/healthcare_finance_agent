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
    return """<p>The current agent supports Pharmacy and PBM teams by answering "what" and "drill-through" questions using two foundational datasets built for <strong>Specialty</strong>, <strong>Home Delivery</strong>, and <strong>PBM</strong> segments.</p>
<hr>
<h3>1. Actuals vs Forecast Analysis Dataset</h3>

<p>Provides financial insights for analyzing actuals, forecasts, and budgets across time and business segments. Supports trend, mix, and variance analysis.</p>

<p><strong>Attributes:</strong><br>
Financial Ledger Type, Metric Category, Business Segment, Product Category, Product Subcategory Level 1, Product Subcategory Level 2, Transaction Date</p>

<p><strong>Metrics:</strong><br>
Financial or Volume Value</p>

<p><strong>Details:</strong></p>
<ul>
    <li><strong>Financial Ledger Type:</strong> Includes GAAP (Actuals), BUDGET, and forecast types like 8+4, 2+10, 5+7. Used to distinguish financial perspectives for variance analysis.</li>
    <li><strong>Metric Category:</strong> Covers Unadjusted Scripts, Adjusted Scripts, Revenues, COGS Post Reclass, SG&A Post Reclass, IOI, Total Membership. Always group by this field for accurate aggregations.</li>
    <li><strong>Financial or Volume Value:</strong> Raw values tied to each metric type. Must be filtered or grouped appropriately to avoid misaggregation.</li>
</ul>

<hr>

<h3>2. Pharmacy Claim Transaction Dataset</h3>

<p>Provides claim-level granularity for utilization and financial analysis at the drug, pharmacy, and client level. Supports detailed breakdowns and derived metrics.</p>

<p><strong>Attributes:</strong><br>
Product Category, Claim Number, Claim Sequence, Claim Status, Business Segment, Business Segment Description, Carrier ID, Account ID, Group ID, Claim Submission Date, Pharmacy Name, Pharmacy NPI, Client ID, Client Name, Drug Name, Therapy Class, Brand/Generic Indicator, Drug Classification Code, Drug Manufacturer, State Code, Member Date of Birth, Member Gender, Client Type, Pharmacy Type</p>

<p><strong>Metrics:</strong><br>
Unadjusted Script Count, 90-Day Script Count, 30-Day Script Count, Adjusted Script Count, Revenue Amount, Expense Amount, Wholesale Acquisition Cost, Average Wholesale Price, Revenue per Script, Generic Dispense Rate</p>

<p><strong>Details:</strong></p>
<ul>
    <li><strong>Claim Status:</strong> Use Paid and Reversed to reflect net activity.</li>
    <li><strong>Therapy Class:</strong> Includes categories like GLP-1 Receptor Agonists, Oncology, SGLT-2 Inhibitors. Use LIKE operator for broader class queries (e.g., GLP-1).</li>
    <li><strong>Revenue per Script:</strong> Derived as Revenue Amount ÷ Unadjusted Script Count.</li>
    <li><strong>Generic Dispense Rate:</strong> Percentage of generic scripts based on Brand/Generic Indicator.</li>
</ul>

<hr>

<h3>Important Note</h3>

<p>The agent's knowledge is limited to the above datasets, attributes, and metrics. For additional data needs or to expand its capabilities, please contact the FDMTeam.</p>"""

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
