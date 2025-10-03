import asyncio
import json
import time
from typing import Dict, List, Optional, Any
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class LLMNavigationController:
    """Streamlined async navigation controller with consolidated LLM calls"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
    
    async def process_user_query(self, state: AgentState) -> Dict[str, any]:
        """Main entry point: Smart consolidated async system"""
        
        current_question = state.get('current_question', state.get('original_question', ''))
        requires_domain_clarification = state.get('requires_domain_clarification', False)
        pending_business_question = state.get('pending_business_question', '')
        existing_domain_selection = state.get('domain_selection', [])
        total_retry_count = state.get('llm_retry_count', 0)
        
        print(f"Navigation Input - Current: '{current_question}'")
        print(f"Navigation Input - Pending: '{pending_business_question}'")
        print(f"Navigation Input - Requires Clarification: {requires_domain_clarification}")
        print(f"Navigation Input - Existing Domain: {existing_domain_selection}")
        
        if requires_domain_clarification:
            # Consolidated: domain response analysis + question processing in one call
            return await self._handle_domain_and_classify(
                current_question, pending_business_question, 
                existing_domain_selection, total_retry_count, state
            )
        else:
            # Consolidated: input analysis + question processing in one call  
            return await self._analyze_and_process_input(
                current_question, existing_domain_selection, total_retry_count, state
            )
    
    async def _analyze_and_process_input(self, current_question: str, existing_domain_selection: List[str], 
                                   total_retry_count: int, state: AgentState) -> Dict[str, any]:
    """CONSOLIDATED: Analyze input type + process business questions in single LLM call"""
    
    questions_history = state.get('user_question_history', [])
    history_context = questions_history[-1:] if questions_history else []
    
    # SINGLE COMPREHENSIVE PROMPT - Simplified without team selection logic
    comprehensive_prompt = f"""You are a healthcare finance analytics assistant.

    SYSTEM KNOWLEDGE:
    This chatbot analyzes healthcare finance data including claims, ledgers, payments, member data, provider data, and pharmacy data. It supports financial reporting, variance analysis, trends, and performance metrics.

    NOW ANALYZE THIS USER INPUT:
    User Input: "{current_question}"
    Previous Question History: {history_context}

    === TASK 1: CLASSIFY INPUT TYPE ===

    Classify the user input into one of these categories:

    1. **GREETING** - Simple greetings, capability questions, general chat
    Examples: "Hi", "Hello", "What can you do?", "Help me", "Good morning"

    2. **DML/DDL** - Data modification requests (not supported)
    Examples: "INSERT data", "UPDATE table", "DELETE records", "CREATE table", "DROP column"

    3. **BUSINESS_QUESTION** - Questions about healthcare finance data, analytics, claims, ledgers, payments
    Examples: "Show me claims data", "Revenue analysis", "Payment trends", "Ledger reconciliation", "Member costs"

    **BUSINESS QUESTION VALIDATION:**
    ✅ VALID: Healthcare finance questions about claims, ledgers, payments, members, providers, pharmacy
    ❌ INVALID: Non-healthcare topics like weather, sports, retail, manufacturing

    === TASK 2: IF VALID BUSINESS QUESTION, APPLY REWRITE AND CLASSIFY LOGIC ===

    IF this is a valid business question, also perform question rewriting and classification:

    ## QUESTION COMPLETENESS ANALYSIS

    BEFORE applying any rewriting, determine if the current question needs history context:

    ### Independence Check Rules:
    1) **Self-Contained Question Indicators** (DO NOT use history):
    - Contains specific metrics ("claim revenue", "top 10 drugs", "total sales")
    - Contains specific timeframes ("July 2025", "Q3 2024", "last month")
    - Contains specific dimensions/filters ("line of business C&S", "North region", "product category")
    - Has complete business context with clear what/when/where components
    - Question is asking about a completely different topic/metric than previous questions

    2) **Incomplete Question Indicators** (USE history):
    - Contains pronouns: "that", "it", "those", "this", "same"
    - Vague references: "why is that", "show me more", "what about trends", "explain that"
    - Missing critical context: "I need expenses" (missing timeframe/filters)
    - Direct follow-ups: "drill down", "show details", "break it down"

    ## REWRITE THE QUESTION BASED ON HISTORY

    ### Rewriting Rules (apply only if question is incomplete):

    1) **Follow-up Handling**:
    - If the current question is a follow-up (e.g., "why is that", "what about", "show me more", "I need expenses"), rewrite it using the most recent relevant item in the previous questions context.
    - Replace pronouns with specific references from history
    - Inherit timeframes, filters, and dimensional context when missing

    2) **New Topic Handling**:
    - If it is a new topic or already complete, treat it as standalone and do not import unrelated history.

    3) **Context Preservation**:
    - Always preserve specific time periods (Q3, January, last month) from history when current question lacks timeframe
    - Always preserve geographic/dimensional filters (North region, by product) when relevant
    - Maintain business terminology exactly as used in history
    - If multiple previous questions exist, use the most recent relevant one

    4) **Temporal Enhancement**:
    - If a month is mentioned without a year, add 2025.
    - Do not modify if a year already exists or if phrasing is like "next January".
    - Recognize month names and common abbreviations (e.g., Jan, Feb, Mar).

    5) **Quality Requirements**:
    - Keep grammar natural, preserve important keywords, and make it sufficient for SQL generation.
    - Rewritten question must be self-contained (readable without history)
    - Length must be 15–500 characters.
    - Trim extra spaces and ensure the first letter is capitalized.

    ## QUESTION TYPE CLASSIFICATION

    - **"what"**: Data requests, facts, numbers, reports, trends, quantities
    - Includes: "what", "how much", "how many", "which", "when", "where"
    - **"why"**: Explanations, causes, drivers, root cause analysis, drill-through analysis  
    - Includes: "why", "how did", "what caused", "what's driving"
    - Follow-ups inherit the type unless explicitly asking for explanations.

    === TASK 3: GENERATE RESPONSE MESSAGE ===

    Based on input type:

    - **GREETING** - Simple greetings, capability questions, general chat, or questions about what information/datasets are available.
        -Examples: "Hi", "Hello", "What can you do?", "Help me", "Good morning", 
        -"What information do you have about claims?", 
        -"What data is available for claims?", 
        -"Specifically within claims, what information you have?"

    - **DML/DDL**: Polite refusal explaining you only analyze data (2-3 lines)
    - **VALID BUSINESS_QUESTION**: Empty string "" (will be processed further)
    - **INVALID BUSINESS_QUESTION**: Helpful redirect to your capabilities (2-3 lines)

    === EXAMPLES FOR CLAUDE ===

    Input: "Hi,what can you do,how are you" 
    → input_type="greeting", valid=false, response="Hello! I'm your healthcare finance analytics assistant..."
    
    Input: "Specifically within claims, what information you have"
    → input_type="greeting", valid=false, response="Here is the information I have about claims: ..."

    Input: "INSERT new data"
    → input_type="dml_ddl", valid=false, response="I can only analyze data, not modify it..."

    Input: "Show me revenue"
    → input_type="business_question", valid=true, response="", rewritten="Show me revenue", question_type="what"

    Input: "Show me PBM data for July"
    → input_type="business_question", valid=true, response="", rewritten="Show me PBM data for July 2025", question_type="what"

    Input: "Tell me about the weather"
    → input_type="business_question", valid=false, response="I specialize in healthcare finance analytics. Please ask about claims, ledgers, payments, or other healthcare data."

    The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.
    {{
        "input_type": "greeting|dml_ddl|business_question",
        "is_valid_business_question": true or false,
        "response_message": "",
        "context_type": "new_independent|true_followup|filter_refinement|metric_expansion",
        "inherited_context": "specific context inherited from previous question or 'none' if independent",
        "rewritten_question": "complete rewritten question with appropriate context inheritance",
        "question_type": "what|why",
        "used_history": true or false
    }}

    Important: Return ONLY valid JSON. No additional text, markdown, or formatting."""
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            llm_response = await self.db_client.call_claude_api_endpoint_async([
                {"role": "user", "content": comprehensive_prompt}
            ])
            
            print("Comprehensive LLM response:", llm_response)
            
            # Try to parse as JSON, if it fails, treat as greeting message
            try:
                response_json = json.loads(llm_response)
                
                input_type = response_json.get('input_type', 'business_question')
                is_valid_business_question = response_json.get('is_valid_business_question', False)
                response_message = response_json.get('response_message', '')
                rewritten_question = response_json.get('rewritten_question', '')
                question_type = response_json.get('question_type', 'what')
                
            except json.JSONDecodeError as json_error:
                print(f"LLM response is not valid JSON, treating as greeting: {json_error}")
                # Treat non-JSON response as a greeting message
                input_type = 'greeting'
                is_valid_business_question = False
                response_message = llm_response.strip()  # Use the raw response as greeting message
                rewritten_question = ''
                question_type = 'what'
            
            total_retry_count += retry_count
            
            # Handle non-business inputs
            if input_type in ['greeting', 'dml_ddl', 'invalid_business']:
                return {
                    'rewritten_question': current_question,
                    'question_type': 'what',
                    'next_agent': 'END',
                    'next_agent_disp': f'{input_type.replace("_", " ").title()} response',
                    'requires_domain_clarification': False,
                    'domain_followup_question': None,
                    'domain_selection': existing_domain_selection,
                    'greeting_response': response_message,
                    'is_dml_ddl': input_type == 'dml_ddl',
                    'llm_retry_count': total_retry_count,
                    'pending_business_question': ''
                }
            
            # Handle valid business questions
            if input_type == 'business_question' and is_valid_business_question:
                
                # Process business question with existing domain selection
                if rewritten_question:
                    next_agent = "router_agent" if question_type == "what" else "root_cause_agent"
                    
                    return {
                        'rewritten_question': rewritten_question,
                        'question_type': question_type,
                        'context_type': response_json.get('context_type', 'new_independent'),
                        'inherited_context': response_json.get('inherited_context', ''),
                        'next_agent': next_agent,
                        'next_agent_disp': next_agent.replace('_', ' ').title(),
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': existing_domain_selection,
                        'llm_retry_count': total_retry_count,
                        'pending_business_question': ''
                    }
            
            # Fallback - treat as invalid business question
            return {
                'rewritten_question': current_question,
                'question_type': 'what',
                'next_agent': 'END',
                'next_agent_disp': 'Invalid business question',
                'requires_domain_clarification': False,
                'domain_followup_question': None,
                'domain_selection': existing_domain_selection,
                'greeting_response': "I specialize in healthcare finance analytics. Please ask about claims, ledgers, payments, or other healthcare data.",
                'llm_retry_count': total_retry_count,
                'pending_business_question': ''
            }
                    
        except Exception as e:
            retry_count += 1
            print(f"Comprehensive analysis attempt {retry_count} failed: {str(e)}")
            
            if retry_count < max_retries:
                print(f"Retrying comprehensive analysis... ({retry_count}/{max_retries})")
                await asyncio.sleep(2 ** retry_count)
                continue
            else:
                print(f"All comprehensive analysis retries failed: {str(e)}")
                return {
                    'rewritten_question': current_question,
                    'question_type': 'what',
                    'next_agent': 'END',
                    'next_agent_disp': 'Model serving endpoint failed',
                    'requires_domain_clarification': False,
                    'domain_followup_question': None,
                    'domain_selection': existing_domain_selection,
                    'greeting_response': "Model serving endpoint failed. Please try again after some time.",
                    'llm_retry_count': total_retry_count + retry_count,
                    'pending_business_question': '',
                    'error': True,
                    'error_message': f"Model serving endpoint failed after {max_retries} attempts"
                }

    async def _handle_domain_and_classify(self, domain_response: str, pending_business_question: str, 
                                    existing_domain_selection: List[str], total_retry_count: int, 
                                    state: AgentState) -> Dict[str, any]:
    """CONSOLIDATED: Team parsing + question rewriting in single LLM call"""
    
    questions_history = state.get('user_question_history', [])
    history_context = questions_history[-1:] if questions_history else []
    
    # SINGLE COMPREHENSIVE TEAM + CLASSIFY PROMPT
    domain_classify_prompt = f"""Parse a user's team selection response and provide intelligent feedback for invalid responses. If valid, immediately rewrite the business question with complete context.

User Team Response: "{domain_response}"
Pending Business Question: "{pending_business_question}"
Previous Question History: {history_context}

Available Teams:
- PBM Network
- Pharmacy (IRIS)

TASK 1 - PARSING RULES FOR CLAUDE:

1. **Valid Selection Patterns (CASE INSENSITIVE):**

NUMBER-BASED SELECTIONS:
- "1" → ["PBM Network"]
- "2" → ["Pharmacy (IRIS)"]

NAME-BASED SELECTIONS:
- "pbm network" OR "pbm" → ["PBM Network"]
- "pharmacy" OR "iris" OR "pharmacy iris" → ["Pharmacy (IRIS)"]

**CRITICAL TEAM DETECTION EXAMPLES:**
- "1" → valid=true, teams=["PBM Network"]
- "2" → valid=true, teams=["Pharmacy (IRIS)"]
- "PBM Network" → valid=true, teams=["PBM Network"]
- "PBM" → valid=true, teams=["PBM Network"]
- "Pharmacy" → valid=true, teams=["Pharmacy (IRIS)"]
- "IRIS" → valid=true, teams=["Pharmacy (IRIS)"]
- "I'm from PBM Network" → valid=true, teams=["PBM Network"]

2. **Invalid Response Types:**
- "confused": "I don't know", "maybe", "not sure", unclear responses
- "new_question": User asks new question instead of selecting
- "invalid_input": Gibberish, unrelated text
- "empty": Empty or very short responses

3. **Smart Response Generation:**
Generate helpful, context-aware follow-up messages for invalid responses.

TASK 2 - IF VALID TEAM, REWRITE BUSINESS QUESTION:

Apply question rewriting rules to the pending business question:

## QUESTION COMPLETENESS ANALYSIS

### Independence Check Rules:
1) **Self-Contained Question Indicators** (DO NOT use history):
- Contains specific metrics ("claim revenue", "top 10 drugs", "total sales")
- Contains specific timeframes ("July 2025", "Q3 2024", "last month")
- Contains specific dimensions/filters ("line of business C&S", "North region", "product category")
- Has complete business context with clear what/when/where components
- Question is asking about a completely different topic/metric than previous questions

2) **Incomplete Question Indicators** (USE history):
- Contains pronouns: "that", "it", "those", "this", "same"
- Vague references: "why is that", "show me more", "what about trends", "explain that"
- Missing critical context: "I need expenses" (missing timeframe/filters)
- Direct follow-ups: "drill down", "show details", "break it down"

## REWRITE THE QUESTION BASED ON HISTORY

### Rewriting Rules (apply only if question is incomplete):

1) **Follow-up Handling**:
- If the current question is a follow-up (e.g., "why is that", "what about", "show me more", "I need expenses"), rewrite it using the most recent relevant item in the previous questions context.
- Replace pronouns with specific references from history
- Inherit timeframes, filters, and dimensional context when missing

2) **New Topic Handling**:
- If it is a new topic or already complete, treat it as standalone and do not import unrelated history.

3) **Context Preservation**:
- Always preserve specific time periods (Q3, January, last month) from history when current question lacks timeframe
- Always preserve geographic/dimensional filters (North region, by product) when relevant
- Maintain business terminology exactly as used in history
- If multiple previous questions exist, use the most recent relevant one

4) **Temporal Enhancement**:
- If a month is mentioned without a year, add 2025.
- Do not modify if a year already exists or if phrasing is like "next January".
- Recognize month names and common abbreviations (e.g., Jan, Feb, Mar).

5) **NO Team Appending**:
- DO NOT append team information to the rewritten question
- Team context is stored separately and not added to the question text
- Keep the rewritten question focused on the business query only

6) **Quality Requirements**:
- Keep grammar natural, preserve important keywords, and make it sufficient for SQL generation.
- Rewritten question must be self-contained (readable without history)
- Length must be 15–500 characters.
- Trim extra spaces and ensure the first letter is capitalized.

## QUESTION TYPE CLASSIFICATION

- **"what"**: Data requests, facts, numbers, reports, trends, quantities
- Includes: "what", "how much", "how many", "which", "when", "where"
- **"why"**: Explanations, causes, drivers, root cause analysis, drill-through analysis  
- Includes: "why", "how did", "what caused", "what's driving"
- Follow-ups inherit the type unless explicitly asking for explanations.

RESPONSE EXAMPLES:
- "1" → valid=true, teams=["PBM Network"], error_type=null, rewritten="Show me revenue"
- "PBM Network" → valid=true, teams=["PBM Network"], error_type=null, rewritten="Show me revenue"
- "2" → valid=true, teams=["Pharmacy (IRIS)"], error_type=null, rewritten="Show me revenue"
- "I don't know" → valid=false, teams=[], error_type="confused"
- "What is revenue?" → valid=false, teams=[], error_type="new_question"

The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.
{{
    "valid_team_selection": true,
    "selected_teams": ["PBM Network"],
    "error_type": null,
    "smart_followup_message": null,
    "rewritten_question": "Show me revenue",
    "question_type": "what",
    "context_type": "new_independent",
    "inherited_context": "none"
}}

Important: Return ONLY valid JSON. No additional text, markdown, or formatting."""
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            llm_response = await self.db_client.call_claude_api_endpoint_async([
                {"role": "user", "content": domain_classify_prompt}
            ])
            
            print("Team + Classify response:", llm_response)
            
            # Try to parse as JSON, if it fails, treat as invalid team selection
            try:
                response_json = json.loads(llm_response)
                
                valid_team_selection = response_json.get('valid_team_selection', False)
                selected_teams = response_json.get('selected_teams', [])
                error_type = response_json.get('error_type')
                smart_followup_message = response_json.get('smart_followup_message')
                rewritten_question = response_json.get('rewritten_question', '')
                question_type = response_json.get('question_type', 'what')
                
            except json.JSONDecodeError as json_error:
                print(f"Team classify response is not valid JSON, treating as invalid: {json_error}")
                # Treat non-JSON response as invalid team selection
                valid_team_selection = False
                selected_teams = []
                error_type = "invalid_input"
                smart_followup_message = llm_response.strip() if llm_response.strip() else None
                rewritten_question = ''
                question_type = 'what'
            
            total_retry_count += retry_count
            
            if valid_team_selection and selected_teams and rewritten_question:
                # Success - team selected and question rewritten
                next_agent = "router_agent" if question_type == "what" else "root_cause_agent"
                
                return {
                    'rewritten_question': rewritten_question,
                    'question_type': question_type,
                    'context_type': response_json.get('context_type', 'new_independent'),
                    'inherited_context': response_json.get('inherited_context', ''),
                    'next_agent': next_agent,
                    'next_agent_disp': next_agent.replace('_', ' ').title(),
                    'requires_domain_clarification': False,
                    'domain_followup_question': None,
                    'domain_selection': selected_teams,
                    'llm_retry_count': total_retry_count,
                    'pending_business_question': ''
                }
            else:
                # Invalid team selection - ask again
                followup_message = smart_followup_message or self._get_default_domain_followup()
                
                return {
                    'rewritten_question': pending_business_question or domain_response,
                    'question_type': 'what',
                    'next_agent': 'END',
                    'next_agent_disp': 'Waiting for team selection',
                    'requires_domain_clarification': True,
                    'domain_followup_question': followup_message,
                    'domain_selection': None,
                    'llm_retry_count': total_retry_count,
                    'pending_business_question': pending_business_question
                }
                
        except Exception as e:
            retry_count += 1
            print(f"Team + classify attempt {retry_count} failed: {str(e)}")
            
            if retry_count < max_retries:
                print(f"Retrying team + classify... ({retry_count}/{max_retries})")
                await asyncio.sleep(2 ** retry_count)
                continue
            else:
                print(f"All team + classify retries failed: {str(e)}")
                return {
                    'rewritten_question': pending_business_question or domain_response,
                    'question_type': 'what',
                    'next_agent': 'END',
                    'next_agent_disp': 'Model serving endpoint failed',
                    'requires_domain_clarification': False,
                    'domain_followup_question': None,
                    'domain_selection': None,
                    'greeting_response': "Model serving endpoint failed. Please try again after some time.",
                    'llm_retry_count': total_retry_count + retry_count,
                    'pending_business_question': pending_business_question,
                    'error': True,
                    'error_message': f"Model serving endpoint failed after {max_retries} attempts"
                }


    def _get_default_domain_followup(self) -> str:
    """Default team clarification message"""
    return """Please select which team you belong to:

1. **PBM Network**
2. **Pharmacy (IRIS)**

**Note:** Ledger datasets contain a combination of PBM, HDP, and Specialty information. Both teams will have access to the entire datasets. If you want to see individual product category level data (PBM, HDP, or Specialty), you can always add the filtering while prompting (e.g., "Show me revenue for PBM" or "Claims data for Specialty")."""
