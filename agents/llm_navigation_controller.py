from typing import Dict, List, Optional
import json
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class LLMNavigationController:
    """Refactored navigation controller: greeting validation + domain detection + question processing"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
    
    def process_user_query(self, state: AgentState) -> Dict[str, any]:
        """Main entry point: Smart 2-LLM-call system with context preservation"""
        
        current_question = state.get('current_question', state.get('original_question', ''))
        requires_domain_clarification = state.get('requires_domain_clarification', False)
        pending_business_question = state.get('pending_business_question', '')
        existing_domain_selection = state.get('domain_selection', [])
        total_retry_count = state.get('llm_retry_count', 0)
        
        print(f"🔍 Navigation Input - Current: '{current_question}'")
        print(f"🔍 Navigation Input - Pending: '{pending_business_question}'")
        print(f"🔍 Navigation Input - Requires Clarification: {requires_domain_clarification}")
        print(f"🔍 Navigation Input - Existing Domain: {existing_domain_selection}")
        
        if requires_domain_clarification:
            # We're in domain clarification mode - this should be a domain response
            return self._handle_domain_clarification_response(current_question, pending_business_question, existing_domain_selection, total_retry_count, state)
        else:
            # This is a new user input - analyze everything in one LLM call
            return self._analyze_user_input_unified(current_question, existing_domain_selection, total_retry_count, state)
    
    def _analyze_user_input_unified(self, current_question: str, existing_domain_selection: List[str], total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """LLM CALL 1: Unified analysis with system knowledge and clearer decision logic"""
        
        unified_prompt = f"""
            You are a healthcare finance analytics assistant specialized in pharmacy and healthcare performance data analysis.

            SYSTEM KNOWLEDGE - WHAT THIS CHATBOT IS BUILT FOR:

            **Primary Datasets:**
            1. **Ledger Dataset** - Analytics-ready dataset for comparing actual results, forecast scenarios (8+4, 2+10, 5+7), and budget plans (BUDGET, GAAP) across key pharmacy and healthcare performance metrics. Measures include prescription counts (total, adjusted, 30-day, 90-day), revenue, cost of goods sold (COGS) after reclassification, SG&A after reclassification, IOI, and total membership. Analysis can be segmented by line of business (Community & State, Employer & Individual, Medicare & Retirement, Optum, External), product category and sub-categories, state/region, and time periods.

            2. **Pharmacy/PBM Claims Dataset** - Analytics-ready dataset for pharmacy benefit management and claims analysis with the same metric structure as Ledger Dataset, supporting variance analysis, mix shift tracking, and trend reporting.

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

            **Domain Detection Rules:**
            - Look for explicit mentions: "specialty revenue", "HDP costs", "PBM data", "home delivery"
            - Look for abbreviations: "hdp", "sp", "pbm" 
            - Case insensitive matching
            - Multiple domains can be detected

            **Domain Mapping:**
            - "Home Delivery", "HDP", "home delivery" → "Home Delivery"
            - "Specialty", "SP", "specialty" → "Specialty"  
            - "PBM", "pbm" → "PBM"
            - "ALL", "all categories" → ["Home Delivery", "Specialty", "PBM"]

            **Domain Found Logic:**
            - If ANY domain explicitly mentioned → domain_found = true
            - If NO domain mentioned but valid business question → domain_found = false (need clarification)

            === TASK 3: GENERATE RESPONSE MESSAGE ===

            Based on input type:

            - **GREETING**: Friendly welcome explaining your capabilities (2-3 lines)
            - **DML/DDL**: Polite refusal explaining you only analyze data (2-3 lines)
            - **VALID BUSINESS_QUESTION**: Empty string "" (will be processed further)
            - **INVALID BUSINESS_QUESTION**: Helpful redirect to your capabilities (2-3 lines)

            === EXAMPLES ===

            Input: "Hi" 
            → input_type="greeting", valid=false, domain_found=false, domains=[], response="Hello! I'm your healthcare finance analytics assistant..."

            Input: "What can you do?"
            → input_type="greeting", valid=false, domain_found=false, domains=[], response="I can help analyze pharmacy data..."

            Input: "INSERT new data"
            → input_type="dml_ddl", valid=false, domain_found=false, domains=[], response="I can only analyze data, not modify it..."

            Input: "Show me revenue"
            → input_type="business_question", valid=true, domain_found=false, domains=[], response=""

            Input: "PBM revenue trends"
            → input_type="business_question", valid=true, domain_found=true, domains=["PBM"], response=""

            Input: "Specialty and HDP costs"
            → input_type="business_question", valid=true, domain_found=true, domains=["Specialty", "Home Delivery"], response=""

            Input: "Tell me about the weather"
            → input_type="business_question", valid=false, domain_found=false, domains=[], response="I specialize in healthcare finance analytics..."

            RESPONSE FORMAT:
            The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.
            {{
                "input_type": "greeting|dml_ddl|business_question",
                "is_valid_business_question": true/false,
                "domain_found": true/false,
                "detected_domains": ["list of domains"] or [],
                "response_message": "appropriate response or empty string (2-3 lines)"
            }}

            """
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_sonnet_3_api([
                    {"role": "user", "content": unified_prompt}
                ])
                print("llm response -1",llm_response)
                response_json = json.loads(llm_response)
                input_type = response_json.get('input_type', 'business_question')
                is_valid_business_question = response_json.get('is_valid_business_question', False)
                domain_found = response_json.get('domain_found', False)
                detected_domains = response_json.get('detected_domains', [])
                response_message = response_json.get('response_message', '')
                
                total_retry_count += retry_count
                
                # Handle greeting or DML/DDL
                if input_type in ['greeting', 'dml_ddl']:
                    return {
                        'rewritten_question': current_question,
                        'question_type': 'what',
                        'next_agent': 'END',
                        'next_agent_disp': 'Greeting response' if input_type == 'greeting' else 'DML/DDL not supported',
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': None,
                        'greeting_response': response_message,
                        'is_dml_ddl': input_type == 'dml_ddl',
                        'llm_retry_count': total_retry_count,
                        'pending_business_question': ''
                    }
                
                # Handle invalid business question
                if input_type == 'business_question' and not is_valid_business_question:
                    return {
                        'rewritten_question': current_question,
                        'question_type': 'what',
                        'next_agent': 'END',
                        'next_agent_disp': 'Invalid question - helpful redirect',
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': None,
                        'greeting_response': response_message,
                        'is_dml_ddl': False,
                        'llm_retry_count': total_retry_count,
                        'pending_business_question': ''
                    }
                
                # Handle valid business question
                if input_type == 'business_question' and is_valid_business_question:
                    
                    # Determine final domain selection
                    final_domain_selection = None
                    needs_clarification = False
                    
                    if domain_found and detected_domains:
                        # Domain found in question
                        final_domain_selection = detected_domains
                    elif existing_domain_selection:
                        # Use existing domain
                        final_domain_selection = existing_domain_selection
                    else:
                        # Need domain clarification
                        needs_clarification = True
                    
                    if needs_clarification:
                        # Store business question and ask for domain
                        followup_question = """I understand you're asking about healthcare finance data. To provide the most accurate analysis, please specify which product category you're interested in:

                        1. **Home Delivery (HDP)** - Home delivery pharmacy services
                        2. **Specialty ** - Specialty pharmacy services  
                        3. **PBM** - Pharmacy Benefit Management services

                        You can choose individual categories (e.g., 'PBM'), combinations (e.g., 'HDP and Specialty),  for comprehensive analysis."""
                        
                        return {
                            'rewritten_question': current_question,
                            'question_type': 'what',
                            'next_agent': 'END',
                            'next_agent_disp': 'Waiting for domain selection',
                            'requires_domain_clarification': True,
                            'domain_followup_question': followup_question,
                            'domain_selection': None,
                            'llm_retry_count': total_retry_count,
                            'pending_business_question': current_question  # STORE business question
                        }
                    else:
                        # Process business question with domain - LLM CALL 2
                        return self._rewrite_and_classify_question(current_question, final_domain_selection, total_retry_count, state)
                
                # Fallback - treat as invalid business question
                return {
                    'rewritten_question': current_question,
                    'question_type': 'what',
                    'next_agent': 'END',
                    'next_agent_disp': 'Invalid question - helpful redirect',
                    'requires_domain_clarification': False,
                    'domain_followup_question': None,
                    'domain_selection': None,
                    'greeting_response': "I specialize in healthcare finance analytics. I can help you analyze prescription counts, revenue, costs, membership data, and performance metrics across Home Delivery, Specialty, and PBM product categories. What would you like to analyze?",
                    'is_dml_ddl': False,
                    'llm_retry_count': total_retry_count,
                    'pending_business_question': ''
                }
                    
            except Exception as e:
                retry_count += 1
                print(f"❌ Unified analysis attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying unified analysis... ({retry_count}/{max_retries})")
                    import time
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    # REMOVE FALLBACK - Return error instead
                    print(f"❌ All unified analysis retries failed: {str(e)}")
                    return {
                        'rewritten_question': current_question,
                        'question_type': 'what',
                        'next_agent': 'END',
                        'next_agent_disp': 'Model serving endpoint failed',
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': None,
                        'greeting_response': "Model serving endpoint failed. Please try again after some time.",
                        'is_dml_ddl': False,
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'error': True,
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts"
                    }


    def _handle_domain_clarification_response(self, domain_response: str, pending_business_question: str, existing_domain_selection: List[str], total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """Handle domain clarification response using the same unified analysis"""
                
        # Use the same unified analysis to parse domain response
        unified_result = self._analyze_domain_response_unified(domain_response, total_retry_count)
        total_retry_count += unified_result.get('retry_count', 0)
        
        if unified_result['valid_domain_selection']:
            # Valid domain selection - process pending business question
            business_question = pending_business_question or domain_response
            domain_selection = unified_result['selected_domains']
            
            # LLM CALL 2: Process business question with domain
            return self._rewrite_and_classify_question(business_question, domain_selection, total_retry_count, state)
        else:
            # Invalid domain selection - ask again with smart message
            smart_followup = unified_result.get('smart_followup_message', self._get_default_domain_followup())
            
            return {
                'rewritten_question': pending_business_question or domain_response,
                'question_type': 'what',
                'next_agent': 'END',
                'next_agent_disp': 'Waiting for domain selection',
                'requires_domain_clarification': True,
                'domain_followup_question': smart_followup,
                'domain_selection': None,
                'llm_retry_count': total_retry_count,
                'pending_business_question': pending_business_question
            }

    def _analyze_domain_response_unified(self, domain_response: str, total_retry_count: int) -> Dict[str, any]:
        """Analyze domain clarification response with smart error handling"""
        
        domain_parse_prompt = f"""
        Parse a user's domain selection response and provide intelligent feedback for invalid responses.

        User Response: "{domain_response}"

        Available Categories:
        - Home Delivery (HDP)
        - Specialty
        - PBM

        PARSING RULES:
        1. Valid Selections:
        - Numbers: "1"→Home Delivery, "2"→Specialty, "3"→PBM, "4"→All
        - Names: "Home Delivery", "Specialty", "PBM", "ALL"
        - Combinations: "1 and 2", "Specialty and Home Delivery"

        2. Invalid Response Types:
        - "confused": "I don't know", "maybe", "not sure", unclear responses
        - "new_question": User asks new question instead of selecting
        - "invalid_input": Gibberish, unrelated text
        - "empty": Empty or very short responses

        3. Smart Response Generation:
        Generate helpful, context-aware follow-up messages for invalid responses.

        EXAMPLES:
        - "1" → valid=true, domains=["Home Delivery"]
        - "PBM" → valid=true, domains=["PBM"]
        - "I don't know" → valid=false, error_type="confused"
        - "What is revenue?" → valid=false, error_type="new_question"

        RESPONSE FORMAT:
        The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.
        {{
            "valid_domain_selection": true/false,
            "selected_domains": ["list"] or [],
            "error_type": "confused|new_question|invalid_input|empty" or null,
            "smart_followup_message": "helpful message for invalid responses" or null
        }}

        """
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_sonnet_3_api([
                    {"role": "user", "content": domain_parse_prompt}
                ])
                print("response-2",llm_response)
                response_json = json.loads(llm_response)
                return {
                    'valid_domain_selection': response_json.get('valid_domain_selection', False),
                    'selected_domains': response_json.get('selected_domains', []),
                    'error_type': response_json.get('error_type'),
                    'smart_followup_message': response_json.get('smart_followup_message'),
                    'retry_count': retry_count
                }
                    
            except Exception as e:
                retry_count += 1
                print(f"❌ Domain response analysis attempt {retry_count} failed: {str(e)}")
                
                if self._is_retryable_error(e) and retry_count < max_retries:
                    print(f"🔄 Retrying domain response analysis... ({retry_count}/{max_retries})")
                    import time
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    # REMOVE FALLBACK - Return error instead
                    print(f"❌ All domain response analysis retries failed: {str(e)}")
                    return {
                        'valid_domain_selection': False,
                        'selected_domains': [],
                        'error_type': 'model_endpoint_failed',
                        'smart_followup_message': "Model serving endpoint failed. Please try again after some time.",
                        'retry_count': retry_count,
                        'error': True,
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }
                
    def _rewrite_and_classify_question(self, business_question: str, domain_selection: List[str], total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """LLM CALL 2: Rewrite question + classify type with smart context inheritance"""
        
        questions_history = state.get('user_question_history', [])
        history_context = questions_history[-1:] if questions_history else []
        domain_text = ""
        if domain_selection:
            if len(domain_selection) == 1:
                domain_text = f" for the product category {domain_selection[0]}"
            elif len(domain_selection) == 2:
                domain_text = f" for the product categories {domain_selection[0]} and {domain_selection[1]}"
            elif len(domain_selection) == 3:
                domain_text = " for the product categories Home Delivery, Specialty, and PBM"
            else:
                domain_text = f" for the product categories {', '.join(domain_selection[:-1])}, and {domain_selection[-1]}"
        # history_context = "\n".join(last_two)
                
        print(f'📚 History context: {history_context}')
        
        rewrite_classify_prompt = f"""
               Your job is to **rewrite and classify the user's question**. Follow the rules strictly.

                Current Question: "{business_question}"
                Domain Context (append exactly as-is if needed): "{domain_text}"
                Previous Question History: "{history_context}" (if empty, treat as first question)

                ## TASK 1 - QUESTION COMPLETENESS ANALYSIS

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

                ## TASK 2 - REWRITE THE QUESTION BASED ON HISTORY

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
                - If Domain Context to Append is non-empty, append it at the end of the rewritten question.
                - If the rewritten question ends with "?", insert the domain text before the "?".
                - Ensure a single space before the domain text and avoid double punctuation.

                6) **Quality Requirements**:
                - Keep grammar natural, preserve important keywords, and make it sufficient for SQL generation.
                - Rewritten question must be self-contained (readable without history)
                - Length must be 15–500 characters.
                - Trim extra spaces and ensure the first letter is capitalized.

                ### Rewriting Examples:
                - Current: "I need expenses" + History: "What was Q3 2025 revenue for North region?" → "What were the expenses for North region in Q3 2025?"
                - Current: "what about trends" + History: "How much did we sell in January?" → "What are the sales trends for January 2025?"
                - Current: "why is that happening" + History: "top 10 drugs for July 2025" → "Why are those top 10 drugs performing that way in July 2025?"

                ### Independence Examples:
                - Current: "what is the claim revenue for line of business C&S for July" → Keep as-is (already complete)
                - Current: "what are the top 10 drugs for the month of July 2025" → Keep as-is (already complete)

                ## TASK 3 - QUESTION TYPE CLASSIFICATION

                - **"what"**: Data requests, facts, numbers, reports, trends, quantities
                - Includes: "what", "how much", "how many", "which", "when", "where"
                - **"why"**: Explanations, causes, drivers, root cause analysis, drill-through analysis  
                - Includes: "why", "how did", "what caused", "what's driving"
                - Follow-ups inherit the type unless explicitly asking for explanations.

                ## RESPONSE FORMAT

                The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.


                {{
                "context_type": "new_independent|true_followup|filter_refinement|metric_expansion",
                "inherited_context": "specific context inherited from previous question or 'none' if independent",
                "rewritten_question": "complete rewritten question with appropriate context inheritance",
                "question_type": "what|why",
                "used_history": true/false
                }}
                """

        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_sonnet_3_api([
                    {"role": "user", "content": rewrite_classify_prompt}
                ])
                print("response-3",llm_response)
                response_json = json.loads(llm_response)
                context_type = response_json.get('context_type', 'new_independent')
                inherited_context = response_json.get('inherited_context', '')
                rewritten_question = response_json.get('rewritten_question', '').strip()
                question_type = response_json.get('question_type', 'what').lower()                
                total_retry_count += retry_count
                
                print(f"✅ Original question: '{business_question}'")
                print(f"✅ Context type: {context_type}")
                print(f"✅ Inherited context: {inherited_context}")
                print(f"✅ Domain context: {domain_selection}")
                print(f"✅ Rewritten question: '{rewritten_question}'")
                print(f"✅ Question type: {question_type}")
                
                # Validate response
                if rewritten_question and question_type in ['what', 'why']:
                    next_agent = "router_agent" if question_type == "what" else "root_cause_agent"
                                    
                    return {
                        'rewritten_question': rewritten_question,
                        'question_type': question_type,
                        'context_type': context_type,
                        'inherited_context': inherited_context,  # Add for debugging
                        'next_agent': next_agent,
                        'next_agent_disp': next_agent.replace('_', ' ').title(),
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': domain_selection,
                        'llm_retry_count': total_retry_count,
                        'pending_business_question': ''
                    }
                else:
                    return self._fallback_rewrite_and_classify(business_question, domain_selection, total_retry_count, state)
                    
            except Exception as e:
                retry_count += 1
                print(f"❌ Rewrite and classify attempt {retry_count} failed: {str(e)}")
                
                if self._is_retryable_error(e) and retry_count < max_retries:
                    print(f"🔄 Retrying rewrite and classify... ({retry_count}/{max_retries})")
                    import time
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    print(f"❌ All rewrite and classify retries failed: {str(e)}")
                    return {
                        'rewritten_question': business_question,
                        'question_type': 'what',
                        'next_agent': 'END',
                        'next_agent_disp': 'Model serving endpoint failed',
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': domain_selection,
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'error': True,
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }


    def _fallback_domain_response_analysis(self, domain_response: str, retry_count: int) -> Dict[str, any]:
        """Fallback domain response analysis"""
        
        user_lower = domain_response.lower().strip()
        selected_categories = []
        
        # Simple parsing
        if "all" in user_lower or "4" in domain_response:
            selected_categories = ["Home Delivery", "Specialty", "PBM"]
        else:
            if "1" in domain_response or "home delivery" in user_lower or "hdp" in user_lower:
                selected_categories.append("Home Delivery")
            if "2" in domain_response or "specialty" in user_lower or "sp" in user_lower:
                selected_categories.append("Specialty")
            if "3" in domain_response or "pbm" in user_lower:
                selected_categories.append("PBM")
        
        if selected_categories:
            return {
                'valid_domain_selection': True,
                'selected_domains': selected_categories,
                'error_type': None,
                'smart_followup_message': None,
                'retry_count': retry_count
            }
        else:
            return {
                'valid_domain_selection': False,
                'selected_domains': [],
                'error_type': 'invalid_input',
                'smart_followup_message': self._get_default_domain_followup(),
                'retry_count': retry_count
            }

    def _fallback_rewrite_and_classify(self, business_question: str, domain_selection: List[str], total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """Fallback for question rewriting and classification with domain context"""
        
        # Simple question type classification
        question_lower = business_question.lower()
        why_indicators = ['why', 'what caused', 'reason for', 'cause of', 'root cause']
        question_type = "why" if any(indicator in question_lower for indicator in why_indicators) else "what"
        
        # Check if domain is already mentioned in the question
        domain_keywords = ['home delivery', 'hdp', 'specialty', 'sp', 'pbm']
        has_domain_mentioned = any(keyword in question_lower for keyword in domain_keywords)
        
        # Append domain context if not already mentioned
        rewritten_question = business_question
        if not has_domain_mentioned and domain_selection:
            domain_str = ", ".join(domain_selection)
            rewritten_question = f"{business_question} for product category {domain_str}"
        
        print(f"🔧 Fallback rewrite:")
        print(f"   Original: '{business_question}'")
        print(f"   Domain mentioned: {has_domain_mentioned}")
        print(f"   Domain context: {domain_selection}")
        print(f"   Rewritten: '{rewritten_question}'")
        
        next_agent = "router_agent" if question_type == "what" else "root_cause_agent"
        
        return {
            'rewritten_question': rewritten_question,
            'question_type': question_type,
            'next_agent': next_agent,
            'next_agent_disp': next_agent.replace('_', ' ').title(),
            'requires_domain_clarification': False,
            'domain_followup_question': None,
            'domain_selection': domain_selection,
            'llm_retry_count': total_retry_count,
            'pending_business_question': ''
        }
    def _get_default_domain_followup(self) -> str:
        """Default domain clarification message"""
        return """I need to understand which product category you're asking about. Please select one or more:

        1. **Home Delivery (HDP)** - Home delivery pharmacy services
        2. **Specialty (SP)** - Specialty pharmacy services  
        3. **PBM** - Pharmacy Benefit Management services
        4. **ALL** - All three categories

        You can choose individual categories (e.g., 'Home Delivery'), combinations (e.g., 'Home Delivery and Specialty'), or 'ALL' for all categories."""
