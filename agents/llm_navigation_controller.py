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
        
        # SINGLE COMPREHENSIVE PROMPT - Your complete unified prompt + rewrite logic
        comprehensive_prompt = f"""You are a healthcare finance analytics assistant specialized in pharmacy and healthcare performance data analysis.

        SYSTEM KNOWLEDGE - WHAT THIS CHATBOT IS BUILT FOR:

        ## Dataset 1: Actuals vs Forecast Analysis
        **Attributes:** Line of Business, Product Category, State/Region, Time periods (Date/Year/Month/Quarter), Forecast scenarios (8+4, 2+10, 5+7), Budget plans (BUDGET, GAAP)  
        **Metrics:** Total Prescriptions, Adjusted Counts, 30-day/90-day Fills, Revenue, COGS (after reclassification), SG&A (after reclassification), IOI, Total Membership, Variance Analysis

        ## Dataset 2: PBM & Pharmacy Claim Transaction
        **Attributes:** Line of Business, Client/Carrier/Account/Group, Dispensing Location, NPI, Medication Name, Therapeutic Class, Brand/Generic, GPI, NDC, Manufacturer, Claim ID, Submission Date, Status, Client Type, Pharmacy Type, Member Age group, State (Paid/Reversed/Rejected)  
        **Metrics:** Total Prescriptions, Adjusted Counts, 30-day/90-day Fills, Revenue, COGS, WAC, AWP, Revenue per Prescription, Generic Dispense Rate (GDR)

        **Supported Analysis Types:**
        - Variance analysis (actual vs forecast/budget)
        - Mix shift tracking by LOB or product category
        - Trend reporting across timeframes
        - Prescription volume analysis
        - Revenue and cost analysis
        - Membership analytics

        **Available Product Categories:**
        - Home Delivery (HDP) - Home delivery pharmacy services
        - Specialty (SP) - Specialty pharmacy services  
        - PBM - Pharmacy Benefit Management services

        NOW ANALYZE THIS USER INPUT:
        User Input: "{current_question}"
        Existing Domain Context: {existing_domain_selection if existing_domain_selection else "None"}
        Previous Question History: {history_context}

        === TASK 1: CLASSIFY INPUT TYPE ===

        Classify the user input into one of these categories:

        1. **GREETING** - Simple greetings, capability questions, general chat
        Examples: "Hi", "Hello", "What can you do?", "Help me", "Good morning"

        2. **DML/DDL** - Data modification requests (not supported)
        Examples: "INSERT data", "UPDATE table", "DELETE records", "CREATE table", "DROP column"

        3. **BUSINESS_QUESTION** - Questions about data, analytics, healthcare finance
        Examples: "Show me revenue", "Prescription counts", "Cost analysis", "Performance metrics"

        BUSINESS QUESTION VALIDATION RULES:
        ✅ VALID: Healthcare/pharmacy related queries about metrics, trends, analysis
        ✅ VALID: Vague but analytics-related: "show me data", "performance metrics"
        ❌ INVALID: Completely unrelated topics: "weather", "sports", "personal advice"

        === TASK 2: EXTRACT DOMAIN CONTEXT ===

        For VALID business questions only, extract product category mentions:

        **Domain Detection Rules - CASE INSENSITIVE MATCHING:**
        - Look for exact text matches (case insensitive): "specialty", "home delivery", "pbm"
        - Look for abbreviations: "hdp", "sp" 
        - Look for specific phrases: "home delivery pharmacy", "specialty pharmacy", "pharmacy benefit management"
        - Multiple domains can be detected in one input

        **Exact Domain Mapping (IMPORTANT - Match these exactly):**
        - If input contains "home delivery" OR "hdp" (case insensitive) → include "Home Delivery" in domains
        - If input contains "specialty" OR "sp" (case insensitive) → include "Specialty" in domains  
        - If input contains "pbm" (case insensitive) → include "PBM" in domains
        - If input contains "all" OR "all categories" (case insensitive) → domains = ["Home Delivery", "Specialty", "PBM"]

        **Domain Found Logic:**
        - If ANY domain explicitly mentioned → domain_found = true, detected_domains = [list of found domains]
        - If NO domain mentioned but valid business question → domain_found = false, detected_domains = []

        **CRITICAL: PBM Detection Examples:**
        - "Show me PBM data" → domain_found = true, detected_domains = ["PBM"]
        - "PBM revenue" → domain_found = true, detected_domains = ["PBM"] 
        - "pbm costs" → domain_found = true, detected_domains = ["PBM"]
        - "What is PBM performance?" → domain_found = true, detected_domains = ["PBM"]

        === TASK 3: IF VALID BUSINESS QUESTION, APPLY REWRITE AND CLASSIFY LOGIC ===

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

        5) **Domain Appending**:
        - If detected domains from step 2 OR existing domain context is available, append at the end of the rewritten question.
        - If the rewritten question ends with "?", insert the domain text before the "?".
        - Ensure a single space before the domain text and avoid double punctuation.

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

        === TASK 4: GENERATE RESPONSE MESSAGE ===

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
        → input_type="greeting", valid=false, domain_found=false, domains=[], response="Hello! I'm your healthcare finance analytics assistant..."
        
        Input: "Specifically within claims, what information you have"
        → input_type="greeting", valid=false, domain_found=false, domains=[], response="Here is the information I have about claims: ..."

        Input: "INSERT new data"
        → input_type="dml_ddl", valid=false, domain_found=false, domains=[], response="I can only analyze data, not modify it..."

        Input: "Show me revenue"
        → input_type="business_question", valid=true, domain_found=false, domains=[], response="", needs_clarification=true

        Input: "PBM revenue trends"
        → input_type="business_question", valid=true, domain_found=true, domains=["PBM"], response="", rewritten="PBM revenue trends", question_type="what"

        Input: "Specialty and HDP costs"
        → input_type="business_question", valid=true, domain_found=true, domains=["Specialty", "Home Delivery"], response="", rewritten="Specialty and HDP costs", question_type="what"

        Input: "Tell me about the weather"
        → input_type="business_question", valid=false, domain_found=false, domains=[], response="I specialize in healthcare finance analytics..."

        The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.
        {{
            "input_type": "greeting|dml_ddl|business_question",
            "is_valid_business_question": true,
            "domain_found": true,
            "detected_domains": ["PBM"],
            "response_message": "",
            "needs_domain_clarification": false,
            "context_type": "new_independent|true_followup|filter_refinement|metric_expansion",
            "inherited_context": "specific context inherited from previous question or 'none' if independent",
            "rewritten_question": "complete rewritten question with appropriate context inheritance",
            "question_type": "what|why",
            "used_history": true
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
                response_json = json.loads(llm_response)
                
                input_type = response_json.get('input_type', 'business_question')
                is_valid_business_question = response_json.get('is_valid_business_question', False)
                response_message = response_json.get('response_message', '')
                domain_found = response_json.get('domain_found', False)
                detected_domains = response_json.get('detected_domains', [])
                needs_domain_clarification = response_json.get('needs_domain_clarification', False)
                rewritten_question = response_json.get('rewritten_question', '')
                question_type = response_json.get('question_type', 'what')
                
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
                        'domain_selection': None,
                        'greeting_response': response_message,
                        'is_dml_ddl': input_type == 'dml_ddl',
                        'llm_retry_count': total_retry_count,
                        'pending_business_question': ''
                    }
                
                # Handle valid business questions
                if input_type == 'business_question' and is_valid_business_question:
                    
                    # Determine final domain selection
                    final_domain_selection = None
                    
                    if domain_found and detected_domains:
                        final_domain_selection = detected_domains
                    elif existing_domain_selection:
                        final_domain_selection = existing_domain_selection
                    elif needs_domain_clarification:
                        # Need domain clarification
                        followup_question = """I understand you're asking about healthcare finance data. To provide the most accurate analysis, please specify which product category you're interested in:

1. **Home Delivery (HDP)** - Home delivery pharmacy services
2. **Specialty** - Specialty pharmacy services  
3. **PBM** - Pharmacy Benefit Management services

You can choose individual categories (e.g., 'PBM'), combinations (e.g., 'HDP and Specialty'), or 'ALL' for comprehensive analysis."""
                        
                        return {
                            'rewritten_question': current_question,
                            'question_type': 'what',
                            'next_agent': 'END',
                            'next_agent_disp': 'Waiting for domain selection',
                            'requires_domain_clarification': True,
                            'domain_followup_question': followup_question,
                            'domain_selection': None,
                            'llm_retry_count': total_retry_count,
                            'pending_business_question': current_question
                        }
                    
                    # Process complete business question
                    if final_domain_selection and rewritten_question:
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
                            'domain_selection': final_domain_selection,
                            'llm_retry_count': total_retry_count,
                            'pending_business_question': ''
                        }
                
                # Fallback for any other cases
                return await self._simple_fallback_response(current_question, existing_domain_selection, total_retry_count)
                    
            except Exception as e:
                retry_count += 1
                print(f"Comprehensive analysis attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"Retrying comprehensive analysis... ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 ** retry_count)  # Async sleep!
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
                        'domain_selection': None,
                        'greeting_response': "Model serving endpoint failed. Please try again after some time.",
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'error': True,
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts"
                    }

    async def _handle_domain_and_classify(self, domain_response: str, pending_business_question: str, 
                                        existing_domain_selection: List[str], total_retry_count: int, 
                                        state: AgentState) -> Dict[str, any]:
        """CONSOLIDATED: Domain parsing + question rewriting in single LLM call"""
        
        questions_history = state.get('user_question_history', [])
        history_context = questions_history[-1:] if questions_history else []
        
        # SINGLE COMPREHENSIVE DOMAIN + CLASSIFY PROMPT - Your complete domain prompt + rewrite logic  
        domain_classify_prompt = f"""Parse a user's domain selection response and provide intelligent feedback for invalid responses. If valid, immediately rewrite the business question with complete context.

User Domain Response: "{domain_response}"
Pending Business Question: "{pending_business_question}"
Previous Question History: {history_context}

Available Categories:
- Home Delivery (HDP)
- Specialty  
- PBM

TASK 1 - PARSING RULES FOR CLAUDE:

1. **Valid Selection Patterns (CASE INSENSITIVE):**

NUMBER-BASED SELECTIONS:
- "1" → ["Home Delivery"]
- "2" → ["Specialty"] 
- "3" → ["PBM"]
- "4" OR "all" OR "ALL" → ["Home Delivery", "Specialty", "PBM"]

NAME-BASED SELECTIONS:
- "home delivery" OR "hdp" → ["Home Delivery"]
- "specialty" OR "sp" → ["Specialty"]
- "pbm" OR "PBM" → ["PBM"] 
- "all" OR "ALL" → ["Home Delivery", "Specialty", "PBM"]

COMBINATION SELECTIONS:
- "1 and 2" → ["Home Delivery", "Specialty"]
- "1,2" → ["Home Delivery", "Specialty"] 
- "home delivery and specialty" → ["Home Delivery", "Specialty"]
- "PBM and Specialty" → ["PBM", "Specialty"]
- "2 and 3" → ["Specialty", "PBM"]
- "1, 2, 3" → ["Home Delivery", "Specialty", "PBM"]

**CRITICAL PBM DETECTION EXAMPLES:**
- "3" → valid=true, domains=["PBM"]
- "PBM" → valid=true, domains=["PBM"]
- "pbm" → valid=true, domains=["PBM"]
- "I choose PBM" → valid=true, domains=["PBM"]
- "3 - PBM" → valid=true, domains=["PBM"]

2. **Invalid Response Types:**
- "confused": "I don't know", "maybe", "not sure", unclear responses
- "new_question": User asks new question instead of selecting
- "invalid_input": Gibberish, unrelated text
- "empty": Empty or very short responses

3. **Smart Response Generation:**
Generate helpful, context-aware follow-up messages for invalid responses.

TASK 2 - IF VALID DOMAIN, REWRITE BUSINESS QUESTION:

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

5) **Domain Appending**:
- Append the selected domain context at the end of the rewritten question.
- If the rewritten question ends with "?", insert the domain text before the "?".
- Ensure a single space before the domain text and avoid double punctuation.

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
- "3" → valid=true, domains=["PBM"], error_type=null
- "PBM" → valid=true, domains=["PBM"], error_type=null  
- "pbm" → valid=true, domains=["PBM"], error_type=null
- "I don't know" → valid=false, domains=[], error_type="confused"
- "What is revenue?" → valid=false, domains=[], error_type="new_question"

The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.
{{
    "valid_domain_selection": true,
    "selected_domains": ["PBM"],
    "error_type": null,
    "smart_followup_message": null,
    "rewritten_question": "Show me revenue for the product category PBM",
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
                
                print("Domain + Classify response:", llm_response)
                response_json = json.loads(llm_response)
                
                valid_domain_selection = response_json.get('valid_domain_selection', False)
                selected_domains = response_json.get('selected_domains', [])
                error_type = response_json.get('error_type')
                smart_followup_message = response_json.get('smart_followup_message')
                rewritten_question = response_json.get('rewritten_question', '')
                question_type = response_json.get('question_type', 'what')
                
                total_retry_count += retry_count
                
                if valid_domain_selection and selected_domains and rewritten_question:
                    # Success - domain selected and question rewritten
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
                        'domain_selection': selected_domains,
                        'llm_retry_count': total_retry_count,
                        'pending_business_question': ''
                    }
                else:
                    # Invalid domain selection - ask again
                    followup_message = smart_followup_message or self._get_default_domain_followup()
                    
                    return {
                        'rewritten_question': pending_business_question or domain_response,
                        'question_type': 'what',
                        'next_agent': 'END',
                        'next_agent_disp': 'Waiting for domain selection',
                        'requires_domain_clarification': True,
                        'domain_followup_question': followup_message,
                        'domain_selection': None,
                        'llm_retry_count': total_retry_count,
                        'pending_business_question': pending_business_question
                    }
                    
            except Exception as e:
                retry_count += 1
                print(f"Domain + classify attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"Retrying domain + classify... ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    print(f"All domain + classify retries failed: {str(e)}")
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

    async def _simple_fallback_response(self, current_question: str, existing_domain_selection: List[str], 
                                      total_retry_count: int) -> Dict[str, any]:
        """Simple fallback for edge cases - no LLM calls"""
        
        # Simple heuristic-based processing
        question_lower = current_question.lower()
        question_type = "why" if any(word in question_lower for word in ['why', 'cause', 'reason']) else "what"
        
        rewritten_question = current_question
        if existing_domain_selection:
            domain_str = ", ".join(existing_domain_selection)
            rewritten_question = f"{current_question} for product category {domain_str}"
        
        next_agent = "router_agent" if question_type == "what" else "root_cause_agent"
        
        return {
            'rewritten_question': rewritten_question,
            'question_type': question_type,
            'next_agent': next_agent,
            'next_agent_disp': next_agent.replace('_', ' ').title(),
            'requires_domain_clarification': False,
            'domain_followup_question': None,
            'domain_selection': existing_domain_selection,
            'llm_retry_count': total_retry_count,
            'pending_business_question': ''
        }

    def _get_default_domain_followup(self) -> str:
        """Default domain clarification message"""
        return """I need to understand which product category you're asking about. Please select one or more:

1. **Home Delivery (HDP)** - Home delivery pharmacy services
2. **Specialty** - Specialty pharmacy services  
3. **PBM** - Pharmacy Benefit Management services
4. **ALL** - All three categories

You can choose individual categories (e.g., 'PBM'), combinations (e.g., 'HDP and Specialty'), or 'ALL' for all categories."""
