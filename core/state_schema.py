from typing import TypedDict, List, Dict, Any, Optional
import datetime

class AgentState(TypedDict):
    """Simplified state schema for core workflow - no enterprise tracking"""
    
    # ============ CORE SESSION DATA ============
    session_id: str
    user_id: str
    original_question: str
    current_question: str
    
    # ============ CURRENT STATE ============
    current_agent: Optional[str]
    previous_agent: Optional[str]
    flow_type: Optional[str]
    transition_type: Optional[str]
    
    # ============ NAVIGATION CONTROLLER STATE ============
    question_type: Optional[str]  # "what" or "why"
    next_agent: Optional[str]     # "router_agent", "sql_generator_agent", "root_cause_agent"
    routing_decision: Optional[Dict]
    
    # ============ DATASET SELECTION ============
    selected_dataset: Optional[str]
    dataset_metadata: Optional[Dict]
    selection_reasoning: Optional[str]
    available_datasets: List[Dict]
    selection_confidence: Optional[float]
    metadata_context: Optional[Dict]  # For storing last table context
    pending_selection_result: Optional[Dict]
    
    # ============ SQL & EXECUTION (PHASE 2) ============
    sql_templates: List[Dict]
    selected_template: Optional[Dict]
    generated_sql: Optional[str]
    query_results: Optional[Any]
    execution_status: str
    
    # ============ VARIANCE & ROOT CAUSE (PHASE 2/3) ============
    variance_detected: bool
    variance_details: List[Dict]
    root_cause_steps: List[Dict]
    follow_up_questions: List[Dict]
    
    # ============ USER INTERACTION ============
    requires_user_input: bool
    clarification_data: Optional[Dict]
    user_preferences: Dict
    
    # ============ WORKFLOW STATE ============
    phase1_summary: Optional[Dict]
    workflow_complete: bool
    errors: List[str]
    final_summary: Optional[Dict]  # Added for workflow completion
    
    # ============ ANALYSIS RESULTS ============
    root_cause_analysis: Optional[str]  # For root cause agent results
    
    # ============ SYSTEM METADATA ============
    session_start_time: str
    last_update_time: str
    total_processing_time: Optional[float]
    
    def __init__(self, **kwargs):
        """Initialize state with basic defaults"""
        defaults = {
            # Core required fields
            'session_id': '',
            'user_id': '',
            'original_question': '',
            'current_question': '',
            
            # Current state
            'current_agent': None,
            'previous_agent': None,
            'flow_type': None,
            'transition_type': None,
            
            # Navigation state
            'question_type': None,
            'next_agent': None,
            'routing_decision': None,
            
            # Dataset selection
            'selected_dataset': None,
            'dataset_metadata': None,
            'selection_reasoning': None,
            'available_datasets': [],
            'selection_confidence': None,
            'metadata_context': None,
            'pending_selection_result': None,
            'sql_templates': [],
            'selected_template': None,
            'generated_sql': None,
            'query_results': None,
            'execution_status': 'pending',
            'variance_detected': False,
            'variance_details': [],
            'root_cause_steps': [],
            'follow_up_questions': [],
            'requires_user_input': False,
            'clarification_data': None,
            'user_preferences': {},
            'phase1_summary': None,
            'workflow_complete': False,
            'errors': [],
            'final_summary': None,
            'root_cause_analysis': None,
            
            # System metadata
            'session_start_time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'last_update_time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'total_processing_time': None
        }
        
        # Update with provided kwargs
        for key, value in kwargs.items():
            if key in defaults:
                defaults[key] = value
        
        super().__init__(**defaults)