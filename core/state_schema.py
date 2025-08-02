from typing import TypedDict, List, Dict, Any, Optional
from langchain_core.messages import BaseMessage
import datetime

class AgentState(TypedDict):
    """Complete state schema for healthcare finance agentic workflow"""
    
    # Session & User Context
    session_id: str
    user_id: str
    messages: List[BaseMessage]
    user_question: str
    original_question: str  # Keep track of initial question
    
    # Navigation & Flow Control
    current_agent: Optional[str]
    previous_agent: Optional[str]
    flow_type: Optional[str]  # 'what', 'why', 'comparison', 'trend'
    transition_type: Optional[str]  # 'what_to_why', 'why_to_what', 'context_break'
    
    # Dataset Selection (Router Agent)
    selected_dataset: Optional[str]
    dataset_metadata: Optional[Dict]
    selection_reasoning: Optional[str]
    available_datasets: List[Dict]
    
    # SQL Template & Generation
    sql_templates: List[Dict]
    selected_template: Optional[Dict]
    generated_sql: Optional[str]
    sql_attempts: List[Dict]  # Track retries
    final_sql: Optional[str]
    
    # Query Execution
    query_results: Optional[Any]
    execution_status: str  # 'pending', 'success', 'failed', 'retry_needed'
    execution_time: Optional[float]
    result_summary: Optional[Dict]
    
    # Variance Analysis
    variance_detected: bool
    variance_percentage: Optional[float]
    variance_category: Optional[str]  # 'critical', 'significant', 'moderate', 'low'
    threshold_exceeded: bool
    variance_details: Optional[Dict]
    
    # Root Cause Analysis
    requires_root_cause: bool
    user_acknowledged_root_cause: Optional[bool]
    root_cause_context: Optional[Dict]
    root_cause_steps: List[Dict]
    root_cause_findings: Optional[Dict]
    knowledge_graph_used: Optional[str]
    fallback_analysis: bool
    
    # Follow-up & Suggestions
    suggested_questions: List[str]
    follow_up_context: Optional[Dict]
    user_selected_followup: Optional[str]
    
    # Memory & Learning
    conversation_history: List[Dict]
    user_preferences: Dict
    successful_patterns: List[Dict]
    context_from_previous_query: Optional[Dict]
    
    # Error Handling
    errors: List[Dict]
    retry_count: int
    last_error: Optional[str]
    
    # Comparison Intent (Dynamic Query Adaptation)
    comparison_intent: Optional[Dict]
    time_periods: Optional[Dict]
    comparison_type: Optional[str]  # 'QoQ', 'MoM', 'YoY'
    
    # System Metadata
    timestamp: str
    processing_time: Optional[float]
    tokens_used: Optional[int]
    
    def __init__(self, **kwargs):
        # Set defaults for required fields
        defaults = {
            'session_id': '',
            'user_id': '',
            'messages': [],
            'user_question': '',
            'original_question': '',
            'current_agent': None,
            'previous_agent': None,
            'flow_type': None,
            'transition_type': None,
            'selected_dataset': None,
            'dataset_metadata': None,
            'selection_reasoning': None,
            'available_datasets': [],
            'sql_templates': [],
            'selected_template': None,
            'generated_sql': None,
            'sql_attempts': [],
            'final_sql': None,
            'query_results': None,
            'execution_status': 'pending',
            'execution_time': None,
            'result_summary': None,
            'variance_detected': False,
            'variance_percentage': None,
            'variance_category': None,
            'threshold_exceeded': False,
            'variance_details': None,
            'requires_root_cause': False,
            'user_acknowledged_root_cause': None,
            'root_cause_context': None,
            'root_cause_steps': [],
            'root_cause_findings': None,
            'knowledge_graph_used': None,
            'fallback_analysis': False,
            'suggested_questions': [],
            'follow_up_context': None,
            'user_selected_followup': None,
            'conversation_history': [],
            'user_preferences': {},
            'successful_patterns': [],
            'context_from_previous_query': None,
            'errors': [],
            'retry_count': 0,
            'last_error': None,
            'comparison_intent': None,
            'time_periods': None,
            'comparison_type': None,
            'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'processing_time': None,
            'tokens_used': None
        }
        
        # Update with provided kwargs
        for key, value in kwargs.items():
            if key in defaults:
                defaults[key] = value
        
        super().__init__(**defaults)