from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langchain_core.messages import BaseMessage
import datetime
from operator import add

def append_to_list(existing: List, new_item: Any) -> List:
    """Reducer function to append new items to a list"""
    if existing is None:
        existing = []
    if isinstance(new_item, list):
        return existing + new_item
    return existing + [new_item]

def merge_dicts(existing: Dict, new_dict: Dict) -> Dict:
    """Reducer function to merge dictionaries"""
    if existing is None:
        existing = {}
    return {**existing, **new_dict}

class AgentState(TypedDict):
    """Enterprise-grade state schema with comprehensive tracking and proper LangGraph reducers"""
    
    # ============ CORE SESSION DATA ============
    session_id: str
    user_id: str
    original_question: str
    current_question: str
    
    # ============ COMPREHENSIVE CONVERSATION TRACKING ============
    # Using Annotated with reducers to accumulate history
    conversation_history: Annotated[List[Dict], append_to_list]
    llm_interactions: Annotated[List[Dict], append_to_list]
    user_interactions: Annotated[List[Dict], append_to_list]
    agent_executions: Annotated[List[Dict], append_to_list]
    error_log: Annotated[List[Dict], append_to_list]
    
    # ============ WORKFLOW EXECUTION TRACKING ============
    workflow_steps: Annotated[List[Dict], append_to_list]
    routing_decisions: Annotated[List[Dict], append_to_list]
    dataset_selections: Annotated[List[Dict], append_to_list]
    performance_metrics: Annotated[Dict, merge_dicts]
    
    # ============ CURRENT STATE (OVERRIDABLE) ============
    current_agent: Optional[str]
    previous_agent: Optional[str]
    flow_type: Optional[str]
    transition_type: Optional[str]
    
    # ============ DATASET SELECTION ============
    selected_dataset: Optional[str]
    dataset_metadata: Optional[Dict]
    selection_reasoning: Optional[str]
    available_datasets: List[Dict]
    selection_confidence: Optional[float]
    
    # ============ VECTOR SEARCH TRACKING ============
    vector_searches: Annotated[List[Dict], append_to_list]
    search_results_history: Annotated[List[Dict], append_to_list]
    
    # ============ SQL & EXECUTION (PHASE 2) ============
    sql_templates: List[Dict]
    selected_template: Optional[Dict]
    generated_sql: Optional[str]
    sql_attempts: Annotated[List[Dict], append_to_list]
    query_results: Optional[Any]
    execution_status: str
    
    # ============ VARIANCE & ROOT CAUSE (PHASE 2/3) ============
    variance_detected: bool
    variance_details: Annotated[List[Dict], append_to_list]
    root_cause_steps: Annotated[List[Dict], append_to_list]
    follow_up_questions: Annotated[List[Dict], append_to_list]
    
    # ============ USER INTERACTION TRACKING ============
    requires_user_input: bool
    clarification_requests: Annotated[List[Dict], append_to_list]
    user_clarifications: Annotated[List[Dict], append_to_list]
    user_preferences: Annotated[Dict, merge_dicts]
    
    # ============ ENTERPRISE TRACKING ============
    audit_trail: Annotated[List[Dict], append_to_list]
    compliance_log: Annotated[List[Dict], append_to_list]
    performance_tracking: Annotated[List[Dict], append_to_list]
    token_usage: Annotated[List[Dict], append_to_list]
    
    # ============ SYSTEM METADATA ============
    session_start_time: str
    last_update_time: str
    total_processing_time: Optional[float]
    
    def __init__(self, **kwargs):
        """Initialize state with enterprise defaults"""
        defaults = {
            # Core required fields
            'session_id': '',
            'user_id': '',
            'original_question': '',
            'current_question': '',
            
            # Tracked lists (will accumulate)
            'conversation_history': [],
            'llm_interactions': [],
            'user_interactions': [],
            'agent_executions': [],
            'error_log': [],
            'workflow_steps': [],
            'routing_decisions': [],
            'dataset_selections': [],
            'vector_searches': [],
            'search_results_history': [],
            'sql_attempts': [],
            'variance_details': [],
            'root_cause_steps': [],
            'follow_up_questions': [],
            'clarification_requests': [],
            'user_clarifications': [],
            'audit_trail': [],
            'compliance_log': [],
            'performance_tracking': [],
            'token_usage': [],
            
            # Tracked dicts (will merge)
            'performance_metrics': {},
            'user_preferences': {},
            
            # Current state (overridable)
            'current_agent': None,
            'previous_agent': None,
            'flow_type': None,
            'transition_type': None,
            'selected_dataset': None,
            'dataset_metadata': None,
            'selection_reasoning': None,
            'available_datasets': [],
            'selection_confidence': None,
            'sql_templates': [],
            'selected_template': None,
            'generated_sql': None,
            'query_results': None,
            'execution_status': 'pending',
            'variance_detected': False,
            'requires_user_input': False,
            
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

# ============ TRACKING UTILITY FUNCTIONS ============

def create_conversation_entry(user_question: str, agent_response: str, metadata: Dict = None) -> Dict:
    """Create standardized conversation history entry"""
    return {
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'user_question': user_question,
        'agent_response': agent_response,
        'metadata': metadata or {},
        'entry_type': 'conversation'
    }

def create_llm_interaction_entry(agent_name: str, prompt: str, response: str, 
                               tokens_used: int = None, duration: float = None) -> Dict:
    """Create standardized LLM interaction entry"""
    return {
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'agent_name': agent_name,
        'prompt': prompt,
        'response': response,
        'tokens_used': tokens_used,
        'duration_seconds': duration,
        'entry_type': 'llm_interaction'
    }

def create_user_interaction_entry(interaction_type: str, data: Dict) -> Dict:
    """Create standardized user interaction entry"""
    return {
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'interaction_type': interaction_type,  # 'question', 'clarification', 'selection'
        'data': data,
        'entry_type': 'user_interaction'
    }

def create_agent_execution_entry(agent_name: str, action: str, result: str, 
                                duration: float = None, metadata: Dict = None) -> Dict:
    """Create standardized agent execution entry"""
    return {
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'agent_name': agent_name,
        'action': action,
        'result': result,
        'duration_seconds': duration,
        'metadata': metadata or {},
        'entry_type': 'agent_execution'
    }

def create_audit_entry(action: str, user_id: str, details: Dict) -> Dict:
    """Create standardized audit trail entry"""
    return {
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'action': action,
        'user_id': user_id,
        'details': details,
        'entry_type': 'audit'
    }

def create_performance_entry(operation: str, duration: float, success: bool, metadata: Dict = None) -> Dict:
    """Create standardized performance tracking entry"""
    return {
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'operation': operation,
        'duration_seconds': duration,
        'success': success,
        'metadata': metadata or {},
        'entry_type': 'performance'
    }