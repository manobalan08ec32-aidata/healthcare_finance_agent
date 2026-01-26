from typing import Dict, List, Optional, Any
from typing_extensions import TypedDict

class AgentState(TypedDict):
    """Minimal state schema for entry router and navigation controller nodes"""
    
    # ============ CORE REQUIRED FIELDS ============
    
    # Input questions
    user_question: str                  # Initial user input (matches workflow initialization)
    current_question: str               # Current question being processed
    
    # Session identification (REQUIRED)
    session_id: str                     # Unique session identifier
    user_id: str                        # User identifier
    user_email: Optional[str]           # User email for logging context
    
    # ============ ENTRY ROUTER FIELDS ============
    
    # Routing flags for entry router logic
    requires_dataset_clarification: Optional[bool]     # Default: False
    is_sql_followup: Optional[bool]                   # Default: False
    
    # ============ NAVIGATION CONTROLLER FIELDS ============
    
    # Navigation outputs
    rewritten_question: Optional[str]               # Processed/rewritten question
    question_type: Optional[str]                    # "what" or "why"
    next_agent: Optional[List[str]]                 # Next node(s) to execute (can be list)
    next_agent_disp: Optional[str]                  # Display name for next agent
    current_agent: Optional[str]                    # Current processing agent
    
    # Domain handling
    domain_selection: Optional[str]           # Selected product categories
    requires_domain_clarification: Optional[bool]   # Default: False
    domain_followup_question: Optional[str]         # Domain clarification message
    pending_business_question: Optional[str]        # Stored business question during domain clarification
    
    # Context and history
    user_question_history: Optional[List[str]]      # Previous questions for context
    context_type: Optional[str]                     # "new_independent", "true_followup", etc.
    inherited_context: Optional[str]                # Context inherited from previous questions
    conversation_memory: Optional[Dict[str, Any]]   # Memory structure: {'dimensions': {}, 'analysis_context': {}}
    
    # Response handling
    greeting_response: Optional[str]                # Greeting/capability responses
    is_dml_ddl: Optional[bool]                     # Data modification request flag
    
    # Error handling
    nav_error_msg: Optional[str]                   # Navigation-specific errors
    errors: Optional[List[str]]                    # General error list
    llm_retry_count: Optional[int]                 # LLM retry tracking
    filter_values:Optional[List[str]]
    filter_metadata_results:Optional[List[str]]    # Filter metadata search results
    selected_filter_context:Optional[str]
    user_friendly_message: Optional[str]        # User-friendly message for navigation issues
    
    # LLM reasoning outputs (for debugging/transparency)
    dataset_selection_reasoning_stream: Optional[str]  # LLM reasoning before JSON in dataset selection
    sql_assessment_reasoning_stream: Optional[str]     # LLM assessment reasoning before SQL JSON
    
    # ============ ROUTER AGENT FIELDS ============
    
    # Column index for filter value search
    column_index: Optional[Dict[str, Any]]         # Column index loaded from Unity Catalog Volume (for filter search)
    
    # Dataset selection and metadata
    selected_dataset: Optional[List[str]]          # Selected table names for analysis
    dataset_metadata: Optional[str]                # Metadata for selected datasets
    functional_names: Optional[List[str]]          # User-friendly dataset names
    user_selected_datasets: Optional[List[str]]    # Datasets selected by user from UI dropdown
    available_datasets: Optional[List[Dict[str, str]]]  # All datasets available for current domain
    
    # Dataset clarification handling
    requires_dataset_clarification: Optional[bool] # Dataset clarification needed
    dataset_followup_question: Optional[str]       # Dataset clarification message
    candidate_actual_tables: Optional[List[str]]   # Candidate tables for selection
    
    # Dataset selection results
    selection_reasoning: Optional[str]             # Reasoning for dataset selection
    followup_reasoning:Optional[str]
    user_message: Optional[str]                    # Message for missing dataset items
    
    matched_sql:Optional[str]
    history_question_match:Optional[str]
    matched_table_name:Optional[str]
    history_sql_used:Optional[bool]
    reasoning_context:Optional[str]

    # ============ LEGACY/COMPATIBILITY FIELDS ============
    
    # Keep these for compatibility with existing Streamlit code
    user_questions_history: Optional[List[str]]    # Alternative history field name
    session_context: Optional[Dict[str, Any]]      # Session metadata
    
    # Workflow state flags
    topic_drift: Optional[bool]                    # Default: False
    missing_dataset_items: Optional[bool]          # Default: False
    phi_found: Optional[bool]                      # Default: False, indicates PHI/PII detected
    plan_approval_exists_flg: Optional[bool]       # Default: False, indicates SQL plan approval content exists
    
    # ============ SQL GENERATION FIELDS ============
    
    # SQL follow-up handling
    sql_followup_question: Optional[str]           # SQL clarification message
    needs_followup: Optional[bool]                 # SQL needs follow-up flag
    dataset_change_requested: Optional[bool]       # Flag indicating user requested dataset change during SQL followup
    
    # SQL history and results
    questions_sql_history: Optional[List[str]]     # SQL queries with titles history
    sql_result: Optional[Dict[str, Any]]           # SQL execution results
    narrative_history: Optional[List[str]]         # History of all narrative responses
    sql_generation_story: Optional[str]            # Business-friendly explanation of SQL generation (2-3 lines)
    
    # Narrative generation status
    narrative_complete: Optional[bool]             # Flag indicating narrative generation completion
    narrative_error_msg: Optional[str]             # Narrative-specific errors

    # Chart generation results (from narrative agent)
    chart_spec: Optional[Dict[str, Any]]           # Chart specification for visualization
    
    # Power BI Report Matching (from narrative agent)
    report_found: Optional[bool]                   # Whether a matching Power BI report was found
    report_url: Optional[str]                      # Power BI report URL
    report_filter: Optional[str]                   # Filters to apply to the report
    report_name: Optional[str]                     # Name of the matched Power BI report
    tab_name: Optional[str]                        # Tab/page name in the report
    match_type: Optional[str]                      # Match quality: EXACT, PARTIAL, APPROXIMATE
    report_reason: Optional[str]                   # Explanation of why this report was matched
    unsupported_filters: Optional[str]             # Filters that cannot be applied automatically
    report_warnings: Optional[List[str]]           # Warnings about report matching
    
    # Additional error fields (for other nodes when added back)
    router_error_msg: Optional[str]
    follow_up_error_msg: Optional[str]
    sql_gen_error_msg: Optional[str]
    sql_followup_topic_drift: Optional[bool]
    sql_followup_but_new_question: Optional[bool]
    sql_drift_message: Optional[str]
    feedback_similarity_score: Optional[int]
    feedback_match_confidence: Optional[str]
    sql_history_section:Optional[str]
    
    
    # ============ FOLLOW-UP QUESTION FIELDS ============
    
    # Follow-up question generation results
    followup_questions: Optional[List[str]]           # Generated follow-up questions
    followup_generation_success: Optional[bool]       # Success flag for follow-up generation
    follow_up_questions_history: Optional[List[str]]  # History of all follow-up questions
    
    # Narrative response for follow-up context
    narrative_response: Optional[str]                 # LLM narrative response for context
    
    # ============ STRATEGIC PLANNER FIELDS ============
    
    # Strategic planner status and control
    strategy_planner_err_msg: Optional[str]           # Strategic planner error messages
    drillthrough_exists: Optional[str]                # Availability status of drillthrough analysis
    
    # Strategic analysis results
    multiple_strategic_results: Optional[bool]        # Flag for multiple strategic query results
    strategic_query_results: Optional[List[Dict[str, Any]]]  # List of strategic query result objects
    mapped_document: Optional[int]                    # Number of mapped documents
    strategic_total_queries: Optional[int]            # Total strategic queries executed
    strategic_successful_queries: Optional[int]       # Number of successful strategic queries
            # User-friendly message for strategic analysis
    
    # Strategic analysis context and history
    first_pass_narrative_results: Optional[str]       # Structured narrative results from first pass
    strategic_sql_query: Optional[str]                # Primary strategic SQL query (for compatibility)
    strategic_analysis_history: Optional[List[str]]   # History of strategic analysis queries
    
    # ============ DRILLTHROUGH PLANNER FIELDS ============
    
    # Drillthrough planner status and control
    drillthrough_planner_err_msg: Optional[str]       # Drillthrough planner error messages
    
    # Drillthrough analysis results
    multiple_drillthrough_results: Optional[bool]     # Flag for multiple drillthrough query results
    drillthrough_query_results: Optional[List[Dict[str, Any]]]  # List of drillthrough query result objects
    
    # ============ TIMESTAMP FIELD ============

    # Workflow timing
    timestamp: Optional[str]                          # ISO timestamp for workflow start
    navigation_start_ts: Optional[str]                # ISO timestamp when navigation controller starts
    router_node_end_ts: Optional[str]                 # ISO timestamp when router node completes
    narrative_end_ts: Optional[str]                   # ISO timestamp when narrative agent node completes

    # ============ REFLECTION AGENT FIELDS ============

    # Reflection mode activation
    is_reflection_mode: Optional[bool]                # True when in correction flow
    is_reflection_handling: Optional[bool]            # True when handling reflection follow-up (routes directly to reflection_agent)
    reflection_phase: Optional[str]                   # "diagnosis" | "input_collection" | "plan_review" | "execution"

    # Previous context being corrected
    previous_sql_for_correction: Optional[str]        # The SQL that was wrong
    previous_question_for_correction: Optional[str]   # Original question that produced wrong answer
    previous_table_used: Optional[List[str]]          # Table(s) that were used in wrong answer

    # Diagnostic results
    reflection_diagnosis: Optional[Dict[str, Any]]    # Full diagnosis output from LLM
    identified_issue_type: Optional[str]              # "wrong_dataset" | "wrong_filter" | "wrong_calculation" | "missing_column"
    correction_path: Optional[str]                    # "DATASET_CHANGE" | "FILTER_FIX" | "STRUCTURE_FIX" | "NEED_CLARIFICATION"

    # User input collection
    user_correction_feedback: Optional[str]           # User's original correction feedback (e.g., "that's wrong")
    user_correction_intent: Optional[str]             # What user wants fixed (after clarification)
    user_dataset_preference: Optional[str]            # If user specifies different dataset

    # Available datasets for correction (loaded from metadata)
    available_datasets_for_correction: Optional[List[Dict[str, Any]]]  # Datasets available for user to choose

    # Plan review
    reflection_plan: Optional[str]                    # Generated correction plan (human-readable)
    reflection_plan_approved: Optional[bool]          # True if user approved the plan
    reflection_follow_up_question: Optional[str]      # Question to ask user (diagnosis or clarification)

    # Reflection execution results
    reflection_corrected_sql: Optional[str]           # The corrected SQL after reflection
    reflection_error_msg: Optional[str]               # Reflection-specific errors
    reflection_followup_retry_count: Optional[int]    # Track retries for topic drift in follow-up
    reflection_plan_approval_retry_count: Optional[int]  # Track retries for topic drift in plan approval
