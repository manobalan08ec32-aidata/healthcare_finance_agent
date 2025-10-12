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
        existing_domain_selection = state.get('domain_selection', [])
        total_retry_count = state.get('llm_retry_count', 0)
        
        print(f"Navigation Input - Current: '{current_question}'")
        print(f"Navigation Input - Existing Domain: {existing_domain_selection}")
        

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
        This chatbot analyzes healthcare finance data including claims, ledgers, payments, member data, provider data, and pharmacy data. It supports comprehensive healthcare analytics including:
        - Drug analysis: top drugs by revenue/volume, drug performance, drug costs, pharmaceutical trends
        - Therapy class analysis: therapy class performance, therapeutic area metrics, specialty vs generic analysis
        - Claims analytics: claim volumes, claim costs, utilization patterns, medical vs pharmacy claims
        - Financial reporting: revenue analysis, expense tracking, budget vs actual, forecast analysis, invoices,all kind of finance metrics
        - Member analytics: member costs, demographics, utilization, risk analysis
        - Pharmacy analytics: dispensing patterns, formulary compliance, specialty pharmacy metrics

        
        IMPORTANT: Questions about drugs, therapy classes, pharmaceuticals, medications are CORE healthcare finance topics and should ALWAYS be classified as valid business questions.

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
        Examples: "Show me claims data", "Revenue analysis", "Payment trends", "Ledger reconciliation", "Member costs", "Top 10 drugs by revenue", "Therapy class performance", "Drug utilization analysis"

        **BUSINESS QUESTION VALIDATION:**
        ✅ VALID: Healthcare finance questions about claims, ledgers, payments, members, providers, pharmacy, drugs, therapy classes, medications, pharmaceuticals, medical costs, utilization, revenue analysis, financial metrics
        ❌ INVALID: Non-healthcare topics like weather, sports, retail, manufacturing, general technology, entertainment

        **CRITICAL DRUG/THERAPY CLASSIFICATION RULES:**
        - ANY question mentioning "drug", "drugs", "medication", "therapy", "therapeutic", "pharmaceutical" is ALWAYS a valid business question
        - Questions about "top drugs", "drug performance", "therapy class", "drug revenue" are core healthcare finance topics
        - Never classify drug-related questions as invalid or greeting - they are business questions requiring data analysis

        === TASK 2: EXTRACT FILTER VALUES ===

        **FILTER VALUES EXTRACTION RULES:**
        
        1. **Extract Potential Filter Values**: From the user input, identify terms that could be filter values for database queries
        2. **Multi-word Terms**: If the user mentions a multi-word phrase (e.g., "covid vaccine"), keep it as a single filter value: ["covid vaccine"]. If the user mentions multiple single-word terms (e.g., "diabetes, asthma"), keep them as separate values: ["diabetes", "asthma"].
        3. **EXCLUSION Rules - DO NOT INCLUDE**:
           - **Common Filter Terms** (case-insensitive): "HDP", "Home Delivery", "SP", "Specialty", "Mail","PBM","Claim Fee","Claim Cost","Activity Fee"
           - **Time/Date Terms**: Any time periods, dates, months, quarters, years, temporal references
           - **Time Examples to EXCLUDE**: "January", "Feb", "Q1", "Q2", "2024", "2025", "July", "last month", "this year", "quarter", "yearly", "monthly", "daily"
           - **Generic Terms**: "revenue", "cost", "expense", "data", "analysis", "report", "top", "bottom", "show", "get"
        4. **INCLUDE Business Terms**: Include terms that could be:
           - Product names, drug names, medication names
           - Therapy classes, therapeutic areas, medical conditions
           - Geographic locations, regions, states, countries
           - Business segments, client names, company names
           - Disease areas, medical specialties
           - Specific brand names, pharmaceutical companies
           - Channel types (excluding the excluded ones above)

        **Examples of Filter Value Extraction:**
        - Input: "Show me revenue for diabetes drugs in California for July 2025" → filter_values: ["diabetes", "California"] (July and 2025 excluded)
        - Input: "Top 10 covid vaccine sales in Q1" → filter_values: ["covid vaccine"] (Q1 excluded)
        - Input: "HDP claims for cardiology this month" → filter_values: ["cardiology"] (HDP and "this month" excluded)

        === TASK 3: IF VALID BUSINESS QUESTION, APPLY REWRITE AND CLASSIFY LOGIC ===

        IF this is a valid business question, also perform question rewriting and classification:

        ## SQL VALIDATION REQUEST DETECTION

        **CRITICAL SPECIAL HANDLING**: Before processing any question, check if this is a SQL validation request:

        **SQL Validation Keywords**: "validate", "check", "verify", "correct", "fix", "wrong", "incorrect result", "not getting correct", "query is wrong", "SQL is wrong", "result doesn't look right"

        **SQL Validation Logic**:
        If the current input contains SQL validation keywords:
        1. **Use the LAST question from history** as the base question (not the validation request itself)
        2. **Add validation context** to indicate this is a validation/correction request
        3. **Rewritten question format**: "[Last question from history] - VALIDATION REQUEST: [current validation input]"
        4. **Example**: 
           - Last question: "What is the revenue for Specialty for July 2025"
           - Current input: "could you please validate the last query. i am not getting correct result"
           - Rewritten: "What is the revenue for Specialty for July 2025 - VALIDATION REQUEST: could you please validate the last query. i am not getting correct result"

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

        5) **Professional Question Structure**:
        - CRITICAL: Always structure rewritten questions as proper business queries
        - Transform informal requests into professional question format
        - Use patterns: "What is the [metric] for [filters] for [timeframe]"
        - Examples of transformations:
          * "I need expense" → "What is the expense for [inherited context]"
          * "show me costs" → "What are the costs for [inherited context]"
          * "get revenue" → "What is the revenue for [inherited context]"
          * "I want claims data" → "What is the claims data for [inherited context]"

        6) **Quality Requirements**:
        - Keep grammar natural, preserve important keywords, and make it sufficient for SQL generation.
        - Rewritten question must be self-contained (readable without history)
        - Must be a well-formed business question, not a command or incomplete phrase
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

        Input: "Show me revenue for last 6 months"
        → input_type="business_question", valid=true, response="", rewritten="What is the revenue for last 6 months", question_type="what", filter_values=[]

        Input: "Show me PBM data for July"
        → input_type="business_question", valid=true, response="", rewritten="What is the PBM data for July 2025", question_type="what", filter_values=[]

        Input: "Tell me about the weather"
        → input_type="business_question", valid=false, response="I specialize in healthcare finance analytics. Please ask about claims, ledgers, payments, drugs, therapy classes, or other healthcare data."

        **CRITICAL FILTER VALUES EXAMPLES:**
        
        Input: "Diabetes medications revenue in Texas for 2024"
        → filter_values=["diabetes", "medications", "Texas"] (2024 excluded as time period)
        
        Input: "Humira sales performance in Q3"
        → filter_values=["Humira"] (Q3 excluded as time period)

        **CRITICAL FOLLOW-UP EXAMPLES:**
        
        Previous: "What is the revenue for Specialty for July 2025"
        Input: "I need expense" 
        → rewritten="What is the expense for Specialty for July 2025"

        Previous: "What is the ledger data for Commercial for June 2025"
        Input: "show me forecast"
        → rewritten="What is the forecast for Commercial for June 2025"

        **CRITICAL SQL VALIDATION EXAMPLES:**
        
        Previous: "What is the revenue for Specialty for July 2025"
        Input: "could you please validate the last query. i am not getting correct result"
        → rewritten="What is the revenue for Specialty for July 2025 - VALIDATION REQUEST: could you please validate the last query. i am not getting correct result"

        Previous: "Show me expense and gross margin for HDP for July 2025"  
        Input: "the query is wrong"
        → rewritten="Show me expense and gross margin for HDP for July 2025 - VALIDATION REQUEST: the query is wrong"

        The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.
        {{
            "input_type": "greeting|dml_ddl|business_question",
            "is_valid_business_question": true or false,
            "response_message": "",
            "context_type": "new_independent|true_followup|filter_refinement|metric_expansion",
            "inherited_context": "specific context inherited from previous question or 'none' if independent",
            "rewritten_question": "complete rewritten question with appropriate context inheritance",
            "question_type": "what|why",
            "used_history": true or false,
            "filter_values": ["array", "of", "extracted", "filter", "terms"]
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
                    filter_values = response_json.get('filter_values', [])
                    
                except json.JSONDecodeError as json_error:
                    print(f"LLM response is not valid JSON, treating as greeting: {json_error}")
                    # Treat non-JSON response as a greeting message
                    input_type = 'greeting'
                    is_valid_business_question = False
                    response_message = llm_response.strip()  # Use the raw response as greeting message
                    rewritten_question = ''
                    question_type = 'what'
                    filter_values = []
                
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
                        'pending_business_question': '',
                        'filter_values': []
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
                            'pending_business_question': '',
                            'filter_values': filter_values
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
                    'pending_business_question': '',
                    'filter_values': filter_values if 'filter_values' in locals() else []
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
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts",
                        'filter_values': []
                    }
                
