import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import asyncio
import json
from typing import Dict, List, Optional, Literal

# Configure Streamlit page
st.set_page_config(
    page_title="Healthcare Finance Drill-Through",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
def initialize_session_state():
    """Initialize all session state variables"""
    if 'investigation_state' not in st.session_state:
        st.session_state.investigation_state = {
            'question': "Why did revenue drop 15% for LOB Commercial?",
            'explored_path': [],
            'remaining_by_level': {
                'level1': ['client', 'carrier'],
                'level2': ['drug', 'therapy_class', 'state', 'brand_generic', 'member'],
                'level3': ['formulary']
            },
            'analysis_scope': 'overall',  # 'overall' or 'client_specific'
            'selected_client': None,
            'current_context': None,
            'metrics': {'revenue': -15, 'volume': -8, 'gdr': 5},
            'chat_history': []
        }
    
    if 'investigation_sessions' not in st.session_state:
        st.session_state.investigation_sessions = {}

# Sample healthcare finance data
@st.cache_data
def load_sample_data():
    """Load sample healthcare finance data"""
    
    # Top revenue contributors
    top_contributors = [
        {'name': 'Aetna Better Health', 'revenue': 45000000, 'change': -18, 'volume': '2.1M', 'impact': 'High', 'reason': 'Largest contributor with significant drop'},
        {'name': 'Molina Healthcare', 'revenue': 38000000, 'change': -22, 'volume': '1.8M', 'impact': 'High', 'reason': 'Second largest, steeper decline'},
        {'name': 'Centene Corporation', 'revenue': 31000000, 'change': -8, 'volume': '1.5M', 'impact': 'Medium', 'reason': 'Stable, smaller decline'}
    ]
    
    # Biggest decliners
    biggest_decliners = [
        {'name': 'UnitedHealth Group', 'revenue': 28000000, 'change': -35, 'volume': '1.2M', 'impact': 'Critical', 'reason': 'Steepest decline, needs investigation'},
        {'name': 'Anthem BCBS', 'revenue': 22000000, 'change': -28, 'volume': '980K', 'impact': 'High', 'reason': 'Major decline in key market'},
        {'name': 'Kaiser Permanente', 'revenue': 19000000, 'change': -31, 'volume': '850K', 'impact': 'High', 'reason': 'Unusual pattern for stable client'}
    ]
    
    # Outliers
    outliers = [
        {'name': 'Humana Inc', 'revenue': 15000000, 'change': 12, 'volume': '680K', 'impact': 'Positive', 'reason': 'Only major client with growth'},
        {'name': 'WellCare Health', 'revenue': 12000000, 'change': -45, 'volume': '520K', 'impact': 'Critical', 'reason': 'Catastrophic decline'}
    ]
    
    # Dimension data for drill-through
    dimension_data = {
        'carrier': [
            {'name': 'Network A', 'revenue_change': -30, 'volume_change': -18, 'contribution': '60%'},
            {'name': 'Network B', 'revenue_change': 5, 'volume_change': 2, 'contribution': '40%'}
        ],
        'drug': [
            {'name': 'Lipitor', 'revenue_change': -35, 'volume_change': -20, 'contribution': '15%'},
            {'name': 'Metformin', 'revenue_change': -10, 'volume_change': -5, 'contribution': '12%'},
            {'name': 'Synthroid', 'revenue_change': 8, 'volume_change': 3, 'contribution': '10%'}
        ],
        'state': [
            {'name': 'California', 'revenue_change': -22, 'volume_change': -15, 'contribution': '25%'},
            {'name': 'Texas', 'revenue_change': -12, 'volume_change': -8, 'contribution': '20%'},
            {'name': 'New York', 'revenue_change': -5, 'volume_change': -2, 'contribution': '18%'}
        ],
        'therapy_class': [
            {'name': 'Cardiovascular', 'revenue_change': -25, 'volume_change': -12, 'contribution': '22%'},
            {'name': 'Diabetes', 'revenue_change': -18, 'volume_change': -10, 'contribution': '20%'},
            {'name': 'Mental Health', 'revenue_change': -8, 'volume_change': -3, 'contribution': '15%'}
        ]
    }
    
    return {
        'top_contributors': top_contributors,
        'biggest_decliners': biggest_decliners,
        'outliers': outliers,
        'dimension_data': dimension_data
    }

def format_currency(amount):
    """Format currency for display"""
    if amount >= 1000000:
        return f"${amount/1000000:.1f}M"
    elif amount >= 1000:
        return f"${amount/1000:.0f}K"
    else:
        return f"${amount:.0f}"

# LLM Integration (Simulated)
async def simulate_llm_analysis(question: str, context: Dict) -> Dict:
    """Simulate LLM analysis - replace with actual LangGraph call"""
    await asyncio.sleep(1)  # Simulate processing time
    
    return {
        'recommendations': ['client', 'drug', 'carrier'],
        'reasoning': f"Based on the {context.get('metrics', {}).get('revenue', 0)}% revenue drop, I recommend investigating these dimensions first.",
        'priority_scores': {'client': 0.9, 'drug': 0.8, 'carrier': 0.7}
    }

def get_dimension_suggestions(current_path: List, scope: str, selected_client: Optional[Dict]) -> List[Dict]:
    """Get available dimension suggestions based on current state"""
    
    if len(current_path) == 0:
        base_suggestions = [
            {'dimension': 'client', 'level': 1, 'reason': 'Identify problematic clients', 'priority': 'high', 'type': 'start'},
            {'dimension': 'carrier', 'level': 1, 'reason': 'Analyze network performance', 'priority': 'medium', 'type': 'start'},
            {'dimension': 'drug', 'level': 2, 'reason': 'Cross-cutting drug analysis', 'priority': 'high', 'type': 'cross_cutting'}
        ]
    else:
        # Generate suggestions based on current path
        last_dimension = current_path[-1]['dimension']
        
        if last_dimension == 'client' and scope == 'client_specific':
            base_suggestions = [
                {'dimension': 'carrier', 'level': 2, 'reason': f'Analyze {selected_client["name"]} carrier networks', 'priority': 'high', 'type': 'drill_down'},
                {'dimension': 'drug', 'level': 2, 'reason': f'{selected_client["name"]} drug preferences', 'priority': 'high', 'type': 'drill_down'},
                {'dimension': 'state', 'level': 2, 'reason': f'{selected_client["name"]} geographic distribution', 'priority': 'medium', 'type': 'lateral'}
            ]
        else:
            base_suggestions = [
                {'dimension': 'therapy_class', 'level': 2, 'reason': 'Drill into therapy categories', 'priority': 'high', 'type': 'drill_down'},
                {'dimension': 'state', 'level': 2, 'reason': 'Geographic analysis', 'priority': 'medium', 'type': 'lateral'}
            ]
    
    return base_suggestions

def display_client_filtering():
    """Display the smart client filtering interface"""
    st.header("🎯 Smart Client Selection")
    
    data = load_sample_data()
    
    # LLM Analysis Header
    st.info("🤖 **LLM Analysis**: From your 2,000+ clients, here are the most investigation-worthy based on your revenue drop question.")
    
    # Create tabs for different client categories
    tab1, tab2, tab3, tab4 = st.tabs(["💰 Top Contributors", "📉 Biggest Decliners", "🔍 Outliers", "🔎 Search/Overall"])
    
    with tab1:
        st.subheader("Top Revenue Contributors (High Impact)")
        for i, client in enumerate(data['top_contributors']):
            col1, col2, col3 = st.columns([3, 1, 1])
            
            with col1:
                st.write(f"**{client['name']}**")
                st.caption(client['reason'])
            
            with col2:
                st.metric("Revenue", format_currency(client['revenue']), f"{client['change']}%")
            
            with col3:
                if st.button(f"Select", key=f"top_{i}", type="primary"):
                    st.session_state.investigation_state['selected_client'] = client
                    st.session_state.investigation_state['analysis_scope'] = 'client_specific'
                    st.session_state.investigation_state['chat_history'].append({
                        'type': 'user',
                        'message': f"Selected {client['name']} for analysis",
                        'timestamp': datetime.now()
                    })
                    st.rerun()
    
    with tab2:
        st.subheader("Biggest Revenue Decliners (Critical)")
        for i, client in enumerate(data['biggest_decliners']):
            col1, col2, col3 = st.columns([3, 1, 1])
            
            with col1:
                st.write(f"**{client['name']}**")
                st.caption(f"🚨 {client['reason']}")
            
            with col2:
                st.metric("Revenue", format_currency(client['revenue']), f"{client['change']}%")
            
            with col3:
                if st.button(f"Select", key=f"decline_{i}", type="primary"):
                    st.session_state.investigation_state['selected_client'] = client
                    st.session_state.investigation_state['analysis_scope'] = 'client_specific'
                    st.session_state.investigation_state['chat_history'].append({
                        'type': 'user',
                        'message': f"Selected {client['name']} for critical analysis",
                        'timestamp': datetime.now()
                    })
                    st.rerun()
    
    with tab3:
        st.subheader("Interesting Outliers")
        for i, client in enumerate(data['outliers']):
            col1, col2, col3 = st.columns([3, 1, 1])
            
            with col1:
                st.write(f"**{client['name']}**")
                st.caption(f"{'📈' if client['change'] > 0 else '📉'} {client['reason']}")
            
            with col2:
                st.metric("Revenue", format_currency(client['revenue']), f"{client['change']}%")
            
            with col3:
                if st.button(f"Select", key=f"outlier_{i}", type="primary"):
                    st.session_state.investigation_state['selected_client'] = client
                    st.session_state.investigation_state['analysis_scope'] = 'client_specific'
                    st.session_state.investigation_state['chat_history'].append({
                        'type': 'user',
                        'message': f"Selected outlier {client['name']} for pattern analysis",
                        'timestamp': datetime.now()
                    })
                    st.rerun()
    
    with tab4:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔍 Search Specific Client")
            search_term = st.text_input("Client name or ID", placeholder="Enter client name...")
            if st.button("Search Client Database"):
                st.info("🔍 Searching 2,000+ clients... (Feature to be implemented)")
        
        with col2:
            st.subheader("📊 Continue Overall Analysis")
            st.write("Analyze aggregate patterns across all clients without focusing on specific ones.")
            if st.button("Skip Client Selection", type="secondary"):
                st.session_state.investigation_state['analysis_scope'] = 'overall'
                st.session_state.investigation_state['selected_client'] = None
                st.session_state.investigation_state['chat_history'].append({
                    'type': 'system',
                    'message': 'Continuing with overall analysis across all clients',
                    'timestamp': datetime.now()
                })
                st.rerun()

def display_dimension_analysis():
    """Display dimension analysis interface"""
    state = st.session_state.investigation_state
    
    st.header("📊 Drill-Through Analysis")
    
    # Context indicator
    if state['analysis_scope'] == 'client_specific' and state['selected_client']:
        st.success(f"🎯 **Analysis Scope**: {state['selected_client']['name']} ({state['selected_client']['change']}% change)")
    else:
        st.info("🌐 **Analysis Scope**: Overall aggregate across all clients")
    
    # Get dimension suggestions
    suggestions = get_dimension_suggestions(
        state['explored_path'], 
        state['analysis_scope'], 
        state['selected_client']
    )
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Available Drill-Down Dimensions")
        
        for i, suggestion in enumerate(suggestions):
            with st.container():
                col_a, col_b, col_c = st.columns([3, 1, 1])
                
                with col_a:
                    priority_color = {"high": "🔴", "medium": "🟡", "low": "🟢"}
                    type_emoji = {"start": "🚀", "drill_down": "🔽", "lateral": "↔️", "cross_cutting": "🔄"}
                    
                    st.write(f"**{suggestion['dimension'].replace('_', ' ').title()}** {priority_color[suggestion['priority']]} {type_emoji.get(suggestion['type'], '')}")
                    st.caption(suggestion['reason'])
                
                with col_b:
                    st.write(f"Level {suggestion['level']}")
                    st.write(f"{suggestion['priority']} priority")
                
                with col_c:
                    if st.button("Investigate", key=f"dim_{i}"):
                        # Add to exploration path
                        state['explored_path'].append({
                            'dimension': suggestion['dimension'],
                            'level': suggestion['level'],
                            'timestamp': datetime.now(),
                            'selected': None
                        })
                        
                        # Add to chat history
                        state['chat_history'].append({
                            'type': 'user',
                            'message': f"Investigating {suggestion['dimension']} dimension",
                            'timestamp': datetime.now()
                        })
                        
                        st.rerun()
        
        # Show dimension data if one is being investigated
        if state['explored_path']:
            current_dim = state['explored_path'][-1]['dimension']
            st.subheader(f"📈 {current_dim.title()} Analysis Results")
            
            data = load_sample_data()
            if current_dim in data['dimension_data']:
                dim_data = data['dimension_data'][current_dim]
                
                # Create DataFrame for display
                df = pd.DataFrame(dim_data)
                
                # Display as interactive table
                st.dataframe(
                    df,
                    column_config={
                        'name': st.column_config.TextColumn('Name'),
                        'revenue_change': st.column_config.NumberColumn('Revenue Change (%)', format="%.1f%%"),
                        'volume_change': st.column_config.NumberColumn('Volume Change (%)', format="%.1f%%"),
                        'contribution': st.column_config.TextColumn('Contribution')
                    },
                    use_container_width=True
                )
                
                # Create visualization
                fig = px.bar(
                    df, 
                    x='name', 
                    y='revenue_change',
                    color='revenue_change',
                    color_continuous_scale='RdYlGn_r',
                    title=f'{current_dim.title()} Revenue Performance',
                    labels={'revenue_change': 'Revenue Change (%)', 'name': current_dim.title()}
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Investigation path
        st.subheader("🔍 Investigation Path")
        
        if not state['explored_path']:
            st.write("No dimensions explored yet")
        else:
            for i, step in enumerate(state['explored_path']):
                st.write(f"**{i+1}.** {step['dimension']} (Level {step['level']})")
                if step.get('selected'):
                    st.write(f"   → Selected: {step['selected']['name']}")
        
        # Reset button
        if st.button("🔄 Reset Investigation", type="secondary"):
            st.session_state.investigation_state['explored_path'] = []
            st.session_state.investigation_state['chat_history'] = []
            st.session_state.investigation_state['analysis_scope'] = 'overall'
            st.session_state.investigation_state['selected_client'] = None
            st.rerun()

def display_chat_history():
    """Display chat history in sidebar"""
    st.sidebar.header("💬 Investigation Log")
    
    chat_history = st.session_state.investigation_state.get('chat_history', [])
    
    if not chat_history:
        st.sidebar.write("Start your investigation...")
    else:
        for msg in chat_history[-10:]:  # Show last 10 messages
            if msg['type'] == 'user':
                st.sidebar.info(f"👤 {msg['message']}")
            else:
                st.sidebar.success(f"🤖 {msg['message']}")

def display_metrics_dashboard():
    """Display key metrics dashboard"""
    st.sidebar.header("📊 Key Metrics")
    
    metrics = st.session_state.investigation_state['metrics']
    
    col1, col2, col3 = st.sidebar.columns(3)
    with col1:
        st.metric("Revenue", "LOB Commercial", f"{metrics['revenue']}%")
    with col2:
        st.metric("Volume", "Scripts", f"{metrics['volume']}%")
    with col3:
        st.metric("GDR", "Generic %", f"+{metrics['gdr']}%")

def main():
    """Main Streamlit application"""
    initialize_session_state()
    
    st.title("🏥 Healthcare Finance Drill-Through System")
    st.caption("LLM-powered investigation with smart client filtering and context-aware analysis")
    
    # Sidebar
    display_metrics_dashboard()
    display_chat_history()
    
    # Main content area
    state = st.session_state.investigation_state
    
    # Display current question
    st.info(f"**Current Question**: {state['question']}")
    
    # Navigation logic
    if state['analysis_scope'] == 'overall' and not state['selected_client'] and not state['explored_path']:
        # Show client filtering
        display_client_filtering()
    else:
        # Show dimension analysis
        display_dimension_analysis()
        
        # Option to change scope
        with st.expander("🔄 Change Analysis Scope"):
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🌐 Switch to Overall Analysis"):
                    state['analysis_scope'] = 'overall'
                    state['selected_client'] = None
                    st.rerun()
            with col2:
                if st.button("🎯 Select Different Client"):
                    state['analysis_scope'] = 'overall'
                    state['selected_client'] = None
                    state['explored_path'] = []
                    st.rerun()
    
    # Debug information (can be removed in production)
    with st.expander("🔧 Debug Information"):
        st.json(st.session_state.investigation_state)

if __name__ == "__main__":
    main()
