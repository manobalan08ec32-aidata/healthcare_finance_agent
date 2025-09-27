from typing import Dict, List, Optional, Any
from typing_extensions import TypedDict

class AgentState(TypedDict):
    """Minimal state schema for entry router and navigation controller nodes"""
    
    # ============ CORE REQUIRED FIELDS ============
    
    # Input questions
    original_question: str              # Initial user input
    current_question: str               # Current question being processed
    
    # Session identification (REQUIRED)
    session_id: str                     # Unique session identifier
    user_id: str                        # User identifier
    
    # ============ ENTRY ROUTER FIELDS ============
    
    # Routing flags for entry router logic
    requires_dataset_clarification: Optional[bool]     # Default: False
    is_sql_followup: Optional[bool]                   # Default: False
    
    # ============ NAVIGATION CONTROLLER FIELDS ============
    
    # Navigation outputs
    rewritten_question: Optional[str]               # Processed/rewritten question
    question_type: Optional[str]                    # "what" or "why"
    next_agent: Optional[str]                       # Next node to execute
    next_agent_disp: Optional[str]                  # Display name for next agent
    current_agent: Optional[str]                    # Current processing agent
    
    # Domain handling
    domain_selection: Optional[List[str]]           # Selected product categories
    requires_domain_clarification: Optional[bool]   # Default: False
    domain_followup_question: Optional[str]         # Domain clarification message
    pending_business_question: Optional[str]        # Stored business question during domain clarification
    
    # Context and history
    user_question_history: Optional[List[str]]      # Previous questions for context
    context_type: Optional[str]                     # "new_independent", "true_followup", etc.
    inherited_context: Optional[str]                # Context inherited from previous questions
    
    # Response handling
    greeting_response: Optional[str]                # Greeting/capability responses
    is_dml_ddl: Optional[bool]                     # Data modification request flag
    
    # Error handling
    nav_error_msg: Optional[str]                   # Navigation-specific errors
    errors: Optional[List[str]]                    # General error list
    llm_retry_count: Optional[int]                 # LLM retry tracking
    
    # ============ LEGACY/COMPATIBILITY FIELDS ============
    
    # Keep these for compatibility with existing Streamlit code
    user_questions_history: Optional[List[str]]    # Alternative history field name
    session_context: Optional[Dict[str, Any]]      # Session metadata
    
    # Workflow state flags
    topic_drift: Optional[bool]                    # Default: False
    missing_dataset_items: Optional[bool]          # Default: False
    
    # Additional error fields (for other nodes when added back)
    router_error_msg: Optional[str]
    follow_up_error_msg: Optional[str]
    sql_gen_error_msg: Optional[str]
