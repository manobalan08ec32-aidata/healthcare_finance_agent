import asyncio
import json
import time
from typing import Dict, List, Optional, Any
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class LLMNavigationController:
    """Single-prompt navigation controller with complete analysis, rewriting, and filter extraction"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
    
    async def process_user_query(self, state: AgentState) -> Dict[str, any]:
        """
        Main entry point: Single-step LLM processing
        
        Args:
            state: Agent state containing question and history
        """
        
        current_question = state.get('current_question', state.get('original_question', ''))
        existing_domain_selection = state.get('domain_selection', [])
        total_retry_count = state.get('llm_retry_count', 0)
        
        print(f"Navigation Input - Current: '{current_question}'")
        print(f"Navigation Input - Existing Domain: {existing_domain_selection}")
        
        # Single-step processing: Analyze → Rewrite → Extract in one call
        return await self._single_step_processing(
            current_question, existing_domain_selection, total_retry_count, state
        )
    
    async def _single_step_processing(self, current_question: str, existing_domain_selection: List[str], 
                                     total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """Single-step LLM processing: Analyze → Rewrite → Extract in one prompt"""
        
        print("Starting single-step processing...")
        questions_history = state.get('user_question_history', [])
        previous_question = state.get('rewritten_question', '') 
        history_context = questions_history[-2:] if questions_history else []
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # ═══════════════════════════════════════════════════════════════
                # SINGLE STEP: COMBINED PROMPT
                # ═══════════════════════════════════════════════════════════════
                
                prompt = self._build_combined_prompt(
                    current_question, 
                    previous_question,
                    history_context
                )
                
                response = await self.db_client.call_claude_api_endpoint_async([
                    {"role": "user", "content": prompt}
                ])
                
                print("LLM Response:", response)
                
                try:
                    result = json.loads(response)
                except json.JSONDecodeError as json_error:
                    print(f"LLM response is not valid JSON: {json_error}")
                    # Treat as greeting
                    result = {
                        'analysis': {
                            'input_type': 'greeting',
                            'is_valid_business_question': False,
                            'response_message': response.strip()
                        }
                    }
                
                # Print analysis for debugging
                if 'analysis' in result:
                    print("="*60)
                    print("ANALYSIS:")
                    print(f"  Detected Prefix: {result['analysis'].get('detected_prefix', 'none')}")
                    print(f"  Clean Question: {result['analysis'].get('clean_question', '')}")
                    print(f"  Decision: {result['analysis'].get('context_decision', '')}")
                    print(f"  Reasoning: {result['analysis'].get('reasoning', '')}")
                    print("="*60)
                
                # Print rewrite for debugging
                if 'rewrite' in result:
                    print("REWRITE:")
                    print(f"  Rewritten: {result['rewrite'].get('rewritten_question', '')}")
                    print(f"  User Message: {result['rewrite'].get('user_message', '')}")
                    print("="*60)
                
                # Print filters for debugging
                if 'filters' in result:
                    print("FILTERS:")
                    print(f"  Extracted: {result['filters'].get('filter_values', [])}")
                    print("="*60)
                
                # Handle non-business questions (greeting, DML, invalid)
                analysis = result.get('analysis', {})
                input_type = analysis.get('input_type', 'business_question')
                is_valid_business_question = analysis.get('is_valid_business_question', False)
                response_message = analysis.get('response_message', '')
                
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
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'filter_values': [],
                        'user_friendly_message': response_message
                    }
                
                # Handle business questions
                if input_type == 'business_question' and is_valid_business_question:
                    
                    rewrite = result.get('rewrite', {})
                    filters = result.get('filters', {})
                    
                    rewritten_question = rewrite.get('rewritten_question', current_question)
                    question_type = rewrite.get('question_type', 'what')
                    user_message = rewrite.get('user_message', '')
                    filter_values = filters.get('filter_values', [])
                    
                    # Determine next agent based on question type
                    next_agent = "router_agent" if question_type == "what" else "root_cause_agent"
                    
                    # Return complete result
                    return {
                        'rewritten_question': rewritten_question,
                        'question_type': question_type,
                        'context_type': analysis.get('context_decision', 'NEW'),
                        'inherited_context': user_message,
                        'next_agent': next_agent,
                        'next_agent_disp': next_agent.replace('_', ' ').title(),
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': existing_domain_selection,
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'filter_values': filter_values,
                        'user_friendly_message': user_message,
                        'decision_from_analysis': analysis.get('context_decision', ''),
                        'detected_prefix': analysis.get('detected_prefix', 'none'),
                        'clean_question': analysis.get('clean_question', current_question),
                        'reasoning': analysis.get('reasoning', '')
                    }
                
                # Fallback - invalid business question
                return {
                    'rewritten_question': current_question,
                    'question_type': 'what',
                    'next_agent': 'END',
                    'next_agent_disp': 'Invalid business question',
                    'requires_domain_clarification': False,
                    'domain_followup_question': None,
                    'domain_selection': existing_domain_selection,
                    'greeting_response': "I specialize in healthcare finance analytics. Please ask about claims, ledgers, payments, or other healthcare data.",
                    'llm_retry_count': total_retry_count + retry_count,
                    'pending_business_question': '',
                    'filter_values': [],
                    'user_friendly_message': "I specialize in healthcare finance analytics."
                }
                        
            except Exception as e:
                retry_count += 1
                print(f"Single-step processing attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"Retrying... ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    print(f"All retries failed: {str(e)}")
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
                        'filter_values': [],
                        'user_friendly_message': "Service temporarily unavailable."
                    }
    
    def _build_combined_prompt(self, current_question: str, previous_question: str,
                               history_context: List) -> str:
        """
        Combined Prompt: Analyze → Rewrite → Extract in single call
        """
        
        # Get current year dynamically
        from datetime import datetime
        current_year = datetime.now().year
        
        # Special filters that are important for context inheritance
        special_filters = ["PBM", "HDP", "Specialty", "Mail", "Retail", "8+4", "5+7", "9+3", "10+2"]
        
        prompt = f"""You are a healthcare finance analytics assistant that analyzes questions, rewrites them with proper context, and extracts filter values.

════════════════════════════════════════════════════════════
INPUT INFORMATION
════════════════════════════════════════════════════════════
User Input: "{current_question}"
Previous Question: "{previous_question if previous_question else 'None'}"
History: {history_context}
Current Year: {current_year}

Special Context Filters (important for inheritance): {special_filters}

════════════════════════════════════════════════════════════
SECTION 1: ANALYZE & CLASSIFY
════════════════════════════════════════════════════════════

**Step 1: Detect and Strip Prefix**

Check if user input starts with any of these prefixes:
- "new question -", "new question:", "new question ", "NEW:" → PREFIX: "new question"
- "follow-up -", "follow up -", "followup -", "follow-up:", "FOLLOW-UP:" → PREFIX: "follow-up"
- "validation", "wrong", "fix", "incorrect" → PREFIX: "validation"

If prefix found:
- Store it in detected_prefix
- Strip it from the question to get clean_question
- Use prefix as PRIMARY signal for decision

If no prefix: detected_prefix = "none", clean_question = user input

**Prefix Examples:**
- "follow-up - show me expense" → prefix: "follow-up", clean: "show me expense"
- "new question - revenue for PBM" → prefix: "new question", clean: "revenue for PBM"
- "show me expense" (no prefix) → prefix: "none", clean: "show me expense"

**Step 2: Classify Input Type**

Classify the CLEAN question into ONE type:

**GREETING** - Greetings, capability questions, general chat
Examples: "Hi", "Hello", "What can you do?", "Help me"

**DML/DDL** - Data modification (not supported)
Examples: "INSERT", "UPDATE", "DELETE", "CREATE table"

**BUSINESS_QUESTION** - Healthcare finance queries
Valid topics: Claims, pharmacy, drugs, therapy classes, revenue, expenses, payments, utilization, actuals, forecasts, finance questions
Invalid: Weather, sports, retail (non-healthcare), non-healthcare topics

**Step 3: Component Detection (For business questions only)**

Analyze the CLEAN question for components:

**Metric** - What's being measured
Examples: revenue, claims, expenses, volume, actuals, forecast, cost, script count

**Filters** - "for X" pattern → Specific entities/values (check against special_filters too)
Examples:
- "for PBM" → filters: ["PBM"]
- "for Specialty" → filters: ["Specialty"]
- "for diabetes" → filters: ["diabetes"]
- "for carrier MDOVA" → filters: ["carrier MDOVA"]

**Attributes** - "by Y" pattern → Grouping dimensions
Examples:
- "by line of business" → attributes: ["line of business"]
- "by carrier" → attributes: ["carrier"]
- "by therapy class" → attributes: ["therapy class"]

**Time Period**
- Full: "August 2025", "Q3 2025", "July 2025" → time_is_partial: false
- Partial: "August", "Q3", "September" → time_is_partial: true

**Signals**
- Pronouns: "that", "it", "this", "those"
- Continuation verbs: "compare", "show me", "breakdown"

**Step 4: Make Decision**

**Priority 1: Detected Prefix (HIGHEST)**
IF detected_prefix == "new question" → Decision: NEW
IF detected_prefix == "follow-up" → Decision: FOLLOW_UP
IF detected_prefix == "validation" → Decision: VALIDATION

**Priority 2: Automatic Detection (if no prefix)**
1. Has validation keywords in clean question → VALIDATION
2. No previous question exists → NEW
3. Has pronouns ("that", "it", "this") → FOLLOW_UP
4. Otherwise, compare current vs previous:
   - If previous question exists and current is missing ANY component that previous had → FOLLOW_UP
   - If current question is self-contained or no previous exists → NEW

**How FOLLOW_UP Inheritance Works:**
For FOLLOW_UP questions, inherit WHATEVER components are missing from current that existed in previous.

Valid question structures:
- metric + filters + time
- metric + attributes + time
- metric + filters + attributes + time
- metric + attributes (e.g., "revenue by line of business")
- metric + filters (e.g., "revenue for PBM")

**Inheritance Logic:**
Compare what current has vs what previous had, then inherit the missing components.

**Decision Examples (Inline - Use these patterns):**

Ex1: "follow-up - show me expense" | Prev: "revenue for PBM by line of business for July 2025"
→ Prefix: "follow-up" → FOLLOW_UP | Current: metric only | Prev had: filters+attributes+time → inherit all three

Ex2: "follow-up - show me expense" | Prev: "revenue for PBM for July 2025"
→ Prefix: "follow-up" → FOLLOW_UP | Current: metric only | Prev had: filters+time (no attributes) → inherit filters+time

Ex3: "follow-up - show me expense" | Prev: "revenue by line of business for July 2025"
→ Prefix: "follow-up" → FOLLOW_UP | Current: metric only | Prev had: attributes+time (no filters) → inherit attributes+time

Ex4: "new question - revenue by line of business" | Prev: any
→ Prefix: "new question" → NEW | Has metric+attributes, valid structure → use as-is, add year if time missing

Ex5: "show me expense" (no prefix) | Prev: "revenue for PBM for July 2025"
→ No prefix, auto-detect | Prev had filters+time, current missing both → FOLLOW_UP

Ex6: "actuals for PBM for September" (no prefix) | Prev: "revenue by LOB for Q3 2025"
→ No prefix, auto-detect | Has metric+filters+partial time → FOLLOW_UP (add year)

Ex7: "validation - revenue was wrong" | Prev: any
→ Prefix: "validation" → VALIDATION

Ex8: "why is that high" (no prefix) | Prev: any
→ No prefix, has pronoun "that" → FOLLOW_UP (inherit all)

**Step 5: Extract Components from Previous Question**

If previous_question exists, analyze it to extract ALL components:
- Previous metric (what was being measured)
- Previous filters (look for "for X" patterns like "for PBM", "for Specialty")
- Previous attributes (look for "by Y" patterns like "by line of business", "by carrier")
- Previous time (look for time periods like "July 2025", "Q3 2025")

These will be used for inheritance if current question is FOLLOW_UP and is missing any of these components.

**Step 6: Write Reasoning**

Clearly explain:
1. Which prefix was detected (if any)
2. What components are in clean_question
3. What components are missing
4. What should be inherited from previous (be specific: "Should inherit ['PBM'] and 'July 2025'")
5. Why this decision was made

════════════════════════════════════════════════════════════
SECTION 2: REWRITE QUESTION
════════════════════════════════════════════════════════════

Now use your analysis from Section 1 to rewrite the question.

**CRITICAL: Read your own reasoning - it tells you exactly what to do**

**IF NEW:**
→ Use clean_question components as-is
→ Format professionally: "What is [metric] for [filters] for [time]"
→ If time_is_partial, add current year: "for [time] {current_year}"
→ user_message: "" (empty string)

**IF FOLLOW_UP:**
→ Start with clean_question components
→ For each component that is null/empty/[] in current question:
  - Extract that component from previous_question string
  - Example: "What is revenue for PBM by line of business for July 2025" 
    → extract filters ["PBM"], attributes ["by line of business"], time "July 2025"
  - Use the extracted value to fill the missing component in current question
→ If time_is_partial is true, add {current_year}
→ Format: "What is [metric] for [filters] [by attributes] for [time]"
  - Include "by [attributes]" only if attributes exist
  - Include "for [filters]" only if filters exist
→ Create user_message explaining what was inherited:
  - Be specific: "I'm using PBM, line of business grouping, and July 2025 from your last question."
  - If only year added: "Added {current_year} as the year."
  - List all inherited components clearly

**IF VALIDATION:**
→ Format: "[Previous Question] - VALIDATION REQUEST: [clean_question]"
→ user_message: "This is a validation request for the previous answer."

**Question Type:**
- If clean_question starts with "why", "how come", "explain" → question_type: "why"
- Otherwise → question_type: "what"

**IMPORTANT:**
- Preserve forecast cycles exactly: "actuals vs forecast 8+4" → keep "8+4"
- Use specific values in user_message, not generic statements
- Always capitalize time periods: "July 2025" not "july 2025"

════════════════════════════════════════════════════════════
SECTION 3: EXTRACT FILTER VALUES
════════════════════════════════════════════════════════════

**CRITICAL: Extract filter values from the REWRITTEN question (after inheritance)**

**GENERIC EXTRACTION LOGIC:**

For every word/phrase in the rewritten question, apply these checks in order:

**Step 1: Does it have an attribute label?**
Attribute labels: carrier, invoice #, invoice number, claim #, claim number, member ID, provider ID, client, account
→ If YES: EXCLUDE (LLM will handle in SQL)

**Step 2: Is it a pure number (digits only)?**
Examples: 2025, 123, 456
→ If YES: EXCLUDE

**Step 3: Is it in the exclusion list?**
- **Common terms**: PBM, HDP, Home Delivery, SP, Specialty, Mail, Retail, Claim Fee, Claim Cost, Activity Fee
- **Attribute/Dimension names**: therapy class, line of business, LOB, carrier, geography, region, state, channel, plan type, member type, provider type, facility type, drug class, drug category
- **Metric modifiers**: unadjusted, adjusted, normalized, raw, calculated, derived, per script, per claim, per member, per capita, average, total, net, gross
- **Forecast cycles**: 8+4, 5+7, 9+3, 10+2, any pattern like "N+M" (these are part of metric)
- **Time/Date**: months (January-December), quarters (Q1-Q4), years (2024, 2025), dates, week, month, year
- **Generic words**: revenue, cost, expense, data, analysis, total, overall, what, is, for, the, by, breakdown, trend, trends, volume, count, script, scripts, claim, claims, compare, versus, vs
- **Metrics**: revenue, billed amount, claims count, expense, volume, actuals, forecast, script count, claim count, amount
- **Grouping keywords**: by, breakdown, group, grouped, aggregate, aggregated, across, each, per
- **Question words**: what, why, how, when, where, who
→ If in ANY category: EXCLUDE

**Step 4: Does it contain letters?**
→ If YES and passed above checks: EXTRACT ✅

**Multi-word handling:**
- Keep phrases together: "covid vaccine" → ["covid vaccine"]
- Multiple terms: "diabetes, asthma" → ["diabetes", "asthma"]
- Hyphenated: "covid-19" → ["covid-19"]

**FILTER EXTRACTION EXAMPLES:**

Example 1: Simple Filter Extraction
Rewritten: "What is the revenue for MPDOVA for September {current_year}"
→ "MPDOVA": no attribute ✓, not pure number ✓, not in exclusion ✓, has letters ✓ → EXTRACT ✅
→ filter_values: ["MPDOVA"]

Example 2: Attribute Label Exclusion
Rewritten: "What is revenue for covid vaccines for carrier MDOVA for Q3"
→ "covid vaccines": passes all checks → EXTRACT ✅
→ "MDOVA": has attribute label "carrier" → EXCLUDE
→ filter_values: ["covid vaccines"]

Example 3: Multiple Exclusions
Rewritten: "What is unadjusted script count by therapy class for diabetes for PBM for July {current_year}"
→ "unadjusted": metric modifier → EXCLUDE
→ "script": generic word → EXCLUDE
→ "count": generic word → EXCLUDE
→ "therapy class": attribute name → EXCLUDE
→ "diabetes": passes all checks → EXTRACT ✅
→ "PBM": common term → EXCLUDE
→ filter_values: ["diabetes"]

Example 4: All Exclusions (No Filters)
Rewritten: "What is revenue for PBM by line of business for July {current_year}"
→ "PBM": common term → EXCLUDE
→ "line of business": attribute name → EXCLUDE
→ filter_values: []

Example 5: Drug Names
Rewritten: "What is revenue for humira for Specialty for Q3 2025"
→ "humira": passes all checks → EXTRACT ✅
→ "Specialty": common term → EXCLUDE
→ filter_values: ["humira"]

════════════════════════════════════════════════════════════
OUTPUT FORMAT
════════════════════════════════════════════════════════════

You MUST output valid JSON in this EXACT structure. Use proper JSON formatting with double quotes.
Do NOT include markdown formatting like ```json.
Do NOT include any text before or after the JSON.

**IMPORTANT: The components you detect should be explained in the reasoning, not as separate JSON fields.**

{{
    "analysis": {{
        "detected_prefix": "new question|follow-up|validation|none",
        "clean_question": "the question after removing prefix",
        "input_type": "greeting|dml_ddl|business_question",
        "is_valid_business_question": true|false,
        "response_message": "message if greeting/dml, empty otherwise",
        "context_decision": "NEW|FOLLOW_UP|VALIDATION",
        "reasoning": "Comprehensive explanation covering: 1) Prefix detected (if any), 2) What components found in current question (metric, filters, attributes, time), 3) What's missing from current, 4) What should be inherited from previous question with specific values like 'Should inherit PBM and July 2025', 5) Why this decision was made"
    }},
    "rewrite": {{
        "rewritten_question": "complete rewritten question with full context and proper capitalization",
        "question_type": "what|why",
        "user_message": "explanation of what was inherited or added, empty string if nothing"
    }},
    "filters": {{
        "filter_values": ["array", "of", "extracted", "filter", "values"]
    }}
}}

════════════════════════════════════════════════════════════
OUTPUT FORMAT EXAMPLE
════════════════════════════════════════════════════════════

Input: "follow-up - show me the expense" | Prev: "What is revenue for PBM by line of business for July 2025"
{{
    "analysis": {{
        "detected_prefix": "follow-up",
        "clean_question": "show me the expense",
        "input_type": "business_question",
        "is_valid_business_question": true,
        "response_message": "",
        "context_decision": "FOLLOW_UP",
        "reasoning": "Prefix 'follow-up' detected. Clean has metric 'expense' only. Previous had filters (PBM), attributes (by line of business), and time (July 2025). Should inherit all three missing components."
    }},
    "rewrite": {{
        "rewritten_question": "What is the expense for PBM by line of business for July 2025",
        "question_type": "what",
        "user_message": "I'm using PBM, line of business grouping, and July 2025 from your last question."
    }},
    "filters": {{"filter_values": []}}
}}

"""
        return prompt
