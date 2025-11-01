import asyncio
import json
import time
from typing import Dict, List, Optional, Any
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class LLMNavigationController:
    """Two-prompt navigation controller with enhanced accuracy and transparency"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
    
    async def process_user_query(self, state: AgentState) -> Dict[str, any]:
        """
        Main entry point: Two-step LLM processing
        
        Args:
            state: Agent state containing question and history
            user_input_type: User's explicit choice from UI - "follow-up" or "new-question"
        """
        
        current_question = state.get('current_question', state.get('original_question', ''))
        existing_domain_selection = state.get('domain_selection', [])
        total_retry_count = state.get('llm_retry_count', 0)
        
        print(f"Navigation Input - Current: '{current_question}'")
        print(f"Navigation Input - Existing Domain: {existing_domain_selection}")
        
        # Two-step processing: Decision → Rewriting
        return await self._two_step_processing(
            current_question, existing_domain_selection, total_retry_count, state
        )
    
    async def _two_step_processing(self, current_question: str, existing_domain_selection: List[str], 
                                   total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """Two-step LLM processing: PROMPT 1 (Decision) → PROMPT 2 (Rewriting)"""
        
        print("coming here ")
        questions_history = state.get('user_question_history', [])
        previous_question = state.get('rewritten_question', '') 
        history_context = questions_history[-2:] if questions_history else []
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # ═══════════════════════════════════════════════════════════════
                # STEP 1: CALL PROMPT 1 - DECISION MAKER
                # ═══════════════════════════════════════════════════════════════
                
                prompt_1 = self._build_prompt_1_decision_maker(
                    current_question, 
                    previous_question, 
                    history_context
                )
                # print("PROMPT 1 ", prompt_1)
                decision_response = await self.db_client.call_claude_api_endpoint_async([
                    {"role": "user", "content": prompt_1}
                ])
                
                print("PROMPT 1 Response:", decision_response)
                
                try:
                    decision_json = json.loads(decision_response)
                except json.JSONDecodeError as json_error:
                    print(f"PROMPT 1 response is not valid JSON: {json_error}")
                    # Treat as greeting
                    decision_json = {
                        'input_type': 'greeting',
                        'is_valid_business_question': False,
                        'response_message': decision_response.strip()
                    }
                
                # Handle non-business questions (greeting, DML, invalid)
                input_type = decision_json.get('input_type', 'business_question')
                is_valid_business_question = decision_json.get('is_valid_business_question', False)
                response_message = decision_json.get('response_message', '')
                
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
                
                # ═══════════════════════════════════════════════════════════════
                # STEP 2: CALL PROMPT 2 - REWRITER
                # ═══════════════════════════════════════════════════════════════
                
                if input_type == 'business_question' and is_valid_business_question:
                    
                    prompt_2 = self._build_prompt_2_rewriter(
                        current_question,
                        previous_question,
                        decision_json
                    )
                    
                    # print('decision prompt:', prompt_2)
                    rewrite_response = await self.db_client.call_claude_api_endpoint_async([
                        {"role": "user", "content": prompt_2}
                    ])
                    
                    print("PROMPT 2 Response:", rewrite_response)
                    
                    try:
                        final_json = json.loads(rewrite_response)
                    except json.JSONDecodeError as json_error:
                        print(f"PROMPT 2 response is not valid JSON: {json_error}")
                        # Fallback
                        final_json = {
                            'rewritten_question': current_question,
                            'filter_values': [],
                            'question_type': 'what',
                            'user_message': ''
                        }
                    
                    # Get user message directly from LLM
                    user_message = final_json.get('user_message', '')
                    
                    # Determine next agent based on question type
                    question_type = final_json.get('question_type', 'what')
                    next_agent = "router_agent" if question_type == "what" else "root_cause_agent"
                    
                    # Return complete result with transparency
                    return {
                        'rewritten_question': final_json.get('rewritten_question', current_question),
                        'question_type': question_type,
                        'context_type': decision_json.get('context_decision', 'new_independent'),
                        'inherited_context': user_message,  # Simple string from LLM
                        'next_agent': next_agent,
                        'next_agent_disp': next_agent.replace('_', ' ').title(),
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': existing_domain_selection,
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'filter_values': final_json.get('filter_values', []),
                        'user_friendly_message': user_message,  # For UI display
                        'decision_from_prompt1': decision_json.get('context_decision', '')  # For debugging
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
                print(f"Two-step processing attempt {retry_count} failed: {str(e)}")
                
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
    
    def _build_prompt_1_decision_maker(self, current_question: str, previous_question: str, 
                                       history_context: List[str]) -> str:
        """
        PROMPT 1: Decision Maker
        Job: Classify input type and decide NEW vs FOLLOW-UP (no rewriting yet)
        """
        
        prompt = f"""You are a healthcare finance analytics assistant.

════════════════════════════════════════════════════════════
NOW ANALYZE THIS USER INPUT:
════════════════════════════════════════════════════════════
User Input: "{current_question}"
Previous Question: "{previous_question if previous_question else 'None'}"
Previous Questions History: {history_context}

════════════════════════════════════════════════════════════
SECTION 1: INPUT CLASSIFICATION
════════════════════════════════════════════════════════════

Classify into ONE type:

**GREETING** - Greetings, capability questions, general chat
Examples: "Hi", "Hello", "What can you do?", "Help me"

**DML/DDL** - Data modification (not supported)
Examples: "INSERT", "UPDATE", "DELETE", "CREATE table"

**BUSINESS_QUESTION** - Healthcare finance queries
Valid topics: Claims, pharmacy, drugs, therapy classes, revenue, expenses, payments, utilization, actuals, forecasts
Invalid: Weather, sports, retail, non-healthcare topics

════════════════════════════════════════════════════════════
SECTION 2: COMPONENT DETECTION (For business questions)
════════════════════════════════════════════════════════════

Identify components in CURRENT question using these patterns:

**Metric** - What's being measured
Examples: revenue, claims, expenses, volume, actuals, forecast, cost

**Filters** - "for X" pattern → Specific entities/values
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
- Full: "August 2025", "Q3 2025" → time_is_partial: false
- Partial: "August", "Q3" → time_is_partial: true

**Signals**
- Pronouns: "that", "it", "this", "those"
- Continuation: "compare", "show me", "breakdown"
- Explicit commands: "NEW:", "FOLLOW-UP:" at start

════════════════════════════════════════════════════════════
SECTION 3: DECISION LOGIC
════════════════════════════════════════════════════════════

**Priority 1: User's Explicit UI Choice (HIGHEST PRIORITY)**

IF User Input contains "new question" → Decision: NEW (respect user's explicit choice)
IF User Input contains "follow-up" → Decision: FOLLOW_UP (respect user's explicit choice)

**Priority 2: User Command in Text** (Check second)
IF current starts with "New Question:" → Decision: NEW
IF current starts with "Follow up question:" → Decision: FOLLOW_UP

**Priority 3: Automatic Detection** (Only if no explicit choice above)

1. **Validation keywords** ("validate", "wrong", "fix") → VALIDATION

2. **No previous question** → NEW

3. **Has pronouns** ("that", "it", "this", "those") → FOLLOW-UP

4. **Complete question check:**
   IF current has ALL of: metric + filters + time with year → NEW
   
   **CRITICAL:** Having attributes alone does NOT make a question complete.
   Filters must be present for a question to be considered complete.
   
   Examples of COMPLETE (has filters):
   - "revenue for PBM for Q3 2025" → has metric + filters + time → NEW
   - "actuals for PBM for September 2025" → has metric + filters + time → NEW
   - "actuals vs forecast 8+4 for Specialty for Q3 2025" → has metric + filters + time → NEW
   
   Examples of INCOMPLETE (no filters):
   - "revenue by line of business for 2025" → has metric + attributes + time, but NO filters → FOLLOW-UP
   - "claims by carrier for Q3 2025" → has metric + attributes + time, but NO filters → FOLLOW-UP
   
   Why FOLLOW-UP? Without filters, the question needs context from previous (which entity/channel to analyze).

5. **Incomplete question check:**
   IF current is MISSING any of: metric, filters, or time → FOLLOW-UP
   
   Examples of INCOMPLETE:
   - "compare actuals for September" → has metric + time, but MISSING filters → FOLLOW-UP
   - "for covid vaccines" → MISSING metric, MISSING time → FOLLOW-UP
   - "show me by line of business" → has attributes, but MISSING metric + filters + time → FOLLOW-UP
   
   Why FOLLOW-UP? Missing components need to be inherited from previous.

6. **Otherwise** → NEW

**Key principle:** 
- Complete questions require: metric + filters + time (with year)
- Attributes alone don't make it complete
- Missing filters = FOLLOW-UP (needs inheritance)

**IMPORTANT for REASONING:**
When writing the reasoning field, clearly state:
- If user made an explicit choice, mention it first
- What components are present in current question
- What components are missing
- What needs to be inherited from previous (be specific: "Should inherit PBM and July 2025")
- Why this decision was made

════════════════════════════════════════════════════════════
EXAMPLES
════════════════════════════════════════════════════════════

Example 1: User Explicit Choice - Follow-up
User's Choice: "follow-up"
Current: "show me the expense"
Previous: "revenue for PBM for July 2025"
→ Decision: FOLLOW_UP (user explicitly selected follow-up)
→ metric: "expense", filters: [], time: null
→ Reasoning: "User explicitly selected follow-up. Current has metric 'expense' but missing filters and time. Should inherit PBM and July 2025 from previous question."

Example 2: User Explicit Choice - New Question
User's Choice: "new-question"
Current: "revenue for Specialty"
Previous: "revenue for PBM for Q3 2025"
→ Decision: NEW (user explicitly selected new question)
→ metric: "revenue", filters: ["Specialty"], time: null (partial)
→ Reasoning: "User explicitly selected new question. Current has metric and filters but time is incomplete. Will be treated as new independent question."

Example 3: Complete New Question (Has Filters)
User's Choice: "follow-up" (default)
Current: "revenue for PBM for Q3 2025"
→ metric: "revenue", filters: ["PBM"], time: "Q3 2025"
→ Has metric + filters + time (all present) → Decision: NEW
→ Reasoning: "Complete question with metric, filters, and time. No inheritance needed."

Example 4: Complete Question with Filters (Overrides User Choice if Complete)
User's Choice: "follow-up"
Current: "actuals for PBM for September 2025"
→ metric: "actuals", filters: ["PBM"], time: "September 2025"
→ Has metric + filters + time (all present) → Decision: NEW
→ Reasoning: "User selected follow-up, but current question is complete with metric, filters, and time. Treating as NEW since it's self-contained."

Example 5: Has Attributes but Missing Filters (FOLLOW-UP!)
User's Choice: "follow-up"
Current: "revenue by line of business for 2025"
Previous: "revenue for PBM for Q3 2025"
→ metric: "revenue", filters: [], attributes: ["line of business"], time: "2025"
→ Has metric + attributes + time, but MISSING filters → Decision: FOLLOW_UP
→ Reasoning: "User selected follow-up. Current has metric and attributes but missing filters. Should inherit PBM from previous question."

Example 6: Missing Filters (No Attributes)
User's Choice: "follow-up"
Current: "compare actuals for September"
Previous: "revenue for PBM for Q3 2025"
→ metric: "actuals", filters: [], time: "September" (partial)
→ Has metric + time, but MISSING filters → Decision: FOLLOW_UP
→ Reasoning: "User selected follow-up. Current has metric and partial time but missing filters. Should inherit PBM from previous question."

Example 7: Forecast Cycle Preserved
User's Choice: "follow-up"
Current: "actuals vs forecast 8+4 for Specialty for Q3 2025"
→ metric: "actuals vs forecast 8+4", filters: ["Specialty"], time: "Q3 2025"
→ Has metric (with cycle) + filters + time → Decision: NEW
→ Reasoning: "User selected follow-up, but question is complete with metric (including 8+4 cycle), filters, and time. Treating as NEW."

Example 8: Pronoun Reference
User's Choice: "follow-up"
Current: "why is that high"
→ Has pronoun "that" → Decision: FOLLOW_UP
→ Reasoning: "User selected follow-up. Current has pronoun 'that' which requires context. Should inherit all components (metric, filters, time) from previous question."

Example 9: User Command in Text
User's Choice: "follow-up"
Current: "NEW: revenue for Specialty"
→ Starts with "NEW:" → Decision: NEW (respect command)
→ Reasoning: "User text starts with 'NEW:' command, overriding UI choice. Current specifies revenue for Specialty but missing time."

════════════════════════════════════════════════════════════
OUTPUT FORMAT (JSON ONLY - NO MARKDOWN)
════════════════════════════════════════════════════════════

- The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. 
- The response MUST not start with ```json and end with ```.

{{
    "input_type": "greeting|dml_ddl|business_question",
    "is_valid_business_question": true|false,
    "response_message": "message if greeting/dml, empty otherwise",
    "is_validation": true|false,
    "context_decision": "NEW|FOLLOW_UP|VALIDATION",
    "components_current": {{
        "metric": "value or null",
        "filters": ["list"],
        "attributes": ["list"],
        "time": "value or null",
        "time_is_partial": true|false,
        "signals": {{
            "has_pronouns": true|false,
            "has_continuation_verbs": true|false
        }}
    }},
    "components_previous": {{
        "metric": "value or null",
        "filters": ["list"],
        "attributes": ["list"],
        "time": "value or null"
    }},
    "reasoning": "Clear explanation mentioning: 1) User's choice if applicable, 2) What's present in current, 3) What's missing, 4) What to inherit (be specific), 5) Why this decision"
}}

"""
        return prompt
    
    def _build_prompt_2_rewriter(self, current_question: str, previous_question: str, 
                                 decision_json: dict) -> str:
        """
        PROMPT 2: Rewriter
        Job: Execute rewriting based on PROMPT 1's analysis + Extract filters
        """
        
        # Get current year dynamically
        from datetime import datetime
        current_year = datetime.now().year
        
        prompt = f"""You are a question rewriter and filter extractor.

════════════════════════════════════════════════════════════
INPUT INFORMATION
════════════════════════════════════════════════════════════
Current Question: "{current_question}"
Previous Question: "{previous_question if previous_question else 'None'}"
Current Year: {current_year}

**ANALYSIS FROM PROMPT 1:**
{json.dumps(decision_json, indent=2)}

**REASONING FROM PROMPT 1:**
{decision_json.get('reasoning', 'No reasoning provided')}

════════════════════════════════════════════════════════════
SECTION 1: QUESTION REWRITING
════════════════════════════════════════════════════════════

Your job is to rewrite the question based on the analysis and reasoning from PROMPT 1.

**CRITICAL: USE THE REASONING ABOVE AS YOUR PRIMARY GUIDE**

The reasoning tells you:
1. What the user's intent was (explicit choice or auto-detected)
2. Which components are present in the current question
3. Which components are missing
4. Exactly what to inherit from the previous question (with specific values)

**STEP 1: Read and Parse the Reasoning**
Example reasoning: "User explicitly selected follow-up. Current has metric 'expense' but missing filters and time. Should inherit PBM and July 2025 from previous."

Parse this to understand:
- Decision basis: "User explicitly selected follow-up"
- What's in current: "metric 'expense'"
- What's missing: "filters and time"
- What to inherit: "PBM and July 2025"

**STEP 2: Apply Rewriting Based on Context Decision**

Context Decision: {decision_json.get('context_decision', 'NEW')}

**IF NEW:**
→ Use the current question components as-is
→ Format professionally: "What is [metric] for [filters] for [time]"
→ NO inheritance from previous
→ user_message: "" (empty string)

**IF VALIDATION:**
→ Format: "[Previous Question] - VALIDATION REQUEST: [current input]"
→ user_message: "This is a validation request."

**IF FOLLOW_UP:**
→ **PRIMARY INSTRUCTION: Follow the reasoning exactly**
→ The reasoning tells you what to inherit - look for phrases like:
  - "Should inherit [specific values]"
  - "Missing [component]"
  - "Needs context from previous"
→ Build the rewritten question using:
  1. **Reasoning as guide**: If reasoning says "inherit PBM", look at components_previous.filters for PBM
  2. **Components as data source**: Use components_current for what's present, components_previous for what's missing
  3. **Time handling**: If time_is_partial is true, add {current_year}
  4. **Format**: "What is [metric] for [filters] [by attributes] for [time]"
→ Create user_message by extracting what was inherited from the reasoning

**STEP 3: Create User Message**
- Extract the specific values mentioned in reasoning for inheritance
- Format naturally: "I'm using [specific values] from your last question."
- If time was added: Include "Added {current_year} as the year."
- If nothing inherited: empty string ""

**IMPORTANT NOTES:**
- **ALWAYS reference the reasoning first** - it contains the exact inheritance instructions
- Preserve forecast cycles exactly: "actuals vs forecast 8+4" → keep "8+4" as part of metric
- Use specific values from reasoning, not generic statements
- Always use professional format: "What is..." for what questions, "Why is..." for why questions

════════════════════════════════════════════════════════════
REWRITING EXAMPLES (Using Reasoning)
════════════════════════════════════════════════════════════

Example 1: NEW Decision
Decision: NEW
Current: "revenue for PBM for Q3 2025"
Reasoning: "Complete question with all components"

**Using Reasoning:**
- Reasoning says "complete question" → no inheritance needed
- Use current components as-is

→ Rewritten: "What is revenue for PBM for Q3 2025"
→ user_message: ""

Example 2: FOLLOW_UP - Missing Filters and Time (YOUR KEY EXAMPLE)
Decision: FOLLOW_UP
Current: "show me the expense"
Previous: "revenue for PBM for July 2025"
Reasoning: "User explicitly selected follow-up. Current has metric 'expense' but missing filters and time. Should inherit PBM and July 2025 from previous."
components_current: {{"metric": "expense", "filters": [], "time": null}}
components_previous: {{"metric": "revenue", "filters": ["PBM"], "time": "July 2025"}}

**Using Reasoning:**
Step 1: Parse reasoning
- User choice: "explicitly selected follow-up"
- What's present: "metric 'expense'"
- What's missing: "filters and time"
- What to inherit: "PBM and July 2025"

Step 2: Build rewritten question
- Metric from current: "expense"
- Filters to inherit: "PBM" (reasoning says so, confirmed in components_previous.filters)
- Time to inherit: "July 2025" (reasoning says so, confirmed in components_previous.time)

Step 3: Create user message
- Extract inherited values from reasoning: "PBM and July 2025"

→ Rewritten: "What is the expense for PBM for July 2025"
→ user_message: "I'm using PBM and July 2025 from your last question."

Example 3: FOLLOW_UP - Partial Time Only
Decision: FOLLOW_UP
Current: "actuals for PBM for September"
Previous: "revenue for PBM for Q3 2025"
Reasoning: "User selected follow-up. Current has metric, filters, but time is partial (missing year)."
components_current: {{"metric": "actuals", "filters": ["PBM"], "time": "September", "time_is_partial": true}}

**Using Reasoning:**
Step 1: Parse reasoning
- What's present: "metric, filters"
- What's partial: "time (missing year)"

Step 2: Build rewritten question
- Metric from current: "actuals"
- Filters from current: ["PBM"]
- Time from current + year: "September {current_year}"

Step 3: Create user message
- Only year was added, no inheritance

→ Rewritten: "What is actuals for PBM for September {current_year}"
→ user_message: "Added {current_year} as the year."

Example 4: FOLLOW_UP - Missing Filters, Keep Attributes
Decision: FOLLOW_UP
Current: "revenue by line of business for 2025"
Previous: "revenue for PBM for Q3 2025"
Reasoning: "User selected follow-up. Current has attributes but missing filters. Should inherit PBM from previous."
components_current: {{"metric": "revenue", "filters": [], "attributes": ["line of business"], "time": "2025"}}
components_previous: {{"filters": ["PBM"]}}

**Using Reasoning:**
Step 1: Parse reasoning
- What's present: "attributes"
- What's missing: "filters"
- What to inherit: "PBM"

Step 2: Build rewritten question
- Metric from current: "revenue"
- Filters to inherit: ["PBM"] (reasoning explicitly says "inherit PBM")
- Attributes from current: ["line of business"]
- Time from current: "2025"

Step 3: Create user message
- Extract inherited value: "PBM"

→ Rewritten: "What is revenue for PBM by line of business for 2025"
→ user_message: "I'm using PBM from your last question."

Example 5: FOLLOW_UP - Pronoun (Inherit All)
Decision: FOLLOW_UP
Current: "why is that high"
Previous: "revenue for carrier MDOVA for Q3"
Reasoning: "User selected follow-up. Current has pronoun 'that' which requires context. Should inherit all components (metric, filters, time) from previous question."
components_current: {{"metric": null, "filters": [], "time": null, "signals": {{"has_pronouns": true}}}}
components_previous: {{"metric": "revenue", "filters": ["carrier MDOVA"], "time": "Q3"}}

**Using Reasoning:**
Step 1: Parse reasoning
- What's missing: Everything (pronoun reference)
- What to inherit: "all components (metric, filters, time)"

Step 2: Build rewritten question
- Metric to inherit: "revenue"
- Filters to inherit: ["carrier MDOVA"]
- Time to inherit: "Q3"

Step 3: Create user message
- Extract all inherited values

→ Rewritten: "Why is the revenue for carrier MDOVA for Q3 high"
→ user_message: "I'm using revenue, carrier MDOVA, and Q3 from your last question."

Example 6: FOLLOW_UP - Forecast Cycle Preserved
Decision: FOLLOW_UP
Current: "compare actuals vs forecast 8+4 for September"
Previous: "actuals vs forecast 8+4 for Specialty for Q3"
Reasoning: "User selected follow-up. Current has metric with cycle but missing filters. Should inherit Specialty. Time is partial."
components_current: {{"metric": "actuals vs forecast 8+4", "filters": [], "time": "September", "time_is_partial": true}}
components_previous: {{"filters": ["Specialty"]}}

**Using Reasoning:**
Step 1: Parse reasoning
- What's present: "metric with cycle"
- What's missing: "filters"
- What to inherit: "Specialty"
- Time status: "partial"

Step 2: Build rewritten question
- Metric from current: "actuals vs forecast 8+4" (preserve cycle)
- Filters to inherit: ["Specialty"] (reasoning says so)
- Time from current + year: "September {current_year}"

Step 3: Create user message
- Inherited: "Specialty"
- Added: year

→ Rewritten: "Compare actuals vs forecast 8+4 for Specialty for September {current_year}"
→ user_message: "I'm using Specialty from your last question. Added {current_year} as the year."

════════════════════════════════════════════════════════════
SECTION 2: FILTER VALUES EXTRACTION
════════════════════════════════════════════════════════════

**CRITICAL: Extract filter values from the REWRITTEN question (after inheritance)**

**GENERIC EXTRACTION LOGIC:**

For every word/phrase in the rewritten question, apply these checks:

**Step 1: Does it have an attribute label?**
Attribute labels: carrier, invoice #, invoice number, claim #, claim number, member ID, provider ID, client, account
→ If YES: EXCLUDE (LLM will handle in SQL)

**Step 2: Is it a pure number (digits only)?**
→ If YES: EXCLUDE

**Step 3: Is it in the exclusion list?**
- **Common terms**: PBM, HDP, Home Delivery, SP, Specialty, Mail, Retail, Claim Fee, Claim Cost, Activity Fee
- **Attribute/Dimension names**: therapy class, line of business, LOB, carrier, geography, region, state, channel, plan type, member type, provider type, facility type, drug class, drug category
- **Metric modifiers**: unadjusted, adjusted, normalized, raw, calculated, derived, per script, per claim, per member, per capita, average, total, net, gross
- **Forecast cycles**: 8+4, 5+7, 9+3, 10+2, any pattern like "N+M" (these are part of metric)
- **Time/Date**: months (January-December), quarters (Q1-Q4), years (2024, 2025), dates
- **Generic words**: revenue, cost, expense, data, analysis, total, overall, what, is, for, the, by, breakdown, trend, trends, volume, count, script, scripts, claim, claims
- **Metrics**: revenue, billed amount, claims count, expense, volume, actuals, forecast, script count, claim count, amount
- **Grouping keywords**: by, breakdown, group, grouped, aggregate, aggregated, across, each, per
→ If in ANY category: EXCLUDE

**Step 4: Does it contain letters?**
→ If YES and passed above checks: EXTRACT ✅

**Multi-word handling:**
Keep phrases together: "covid vaccine" → ["covid vaccine"]
Multiple terms: "diabetes, asthma" → ["diabetes", "asthma"]

**FILTER EXTRACTION EXAMPLES:**

Example 1: Simple Filter Extraction
Rewritten: "What is the revenue for MPDOVA for September {current_year}"
→ "MPDOVA": no attribute ✓, not pure number ✓, not in exclusion ✓, has letters ✓ → EXTRACT ✅
→ filter_values: ["MPDOVA"]

Example 2: Attribute Label Exclusion + Filter Extraction
Rewritten: "What is revenue for covid vaccines for carrier MDOVA for Q3"
→ "covid vaccines": no attribute ✓, not pure number ✓, not in exclusion ✓, has letters ✓ → EXTRACT ✅
→ "MDOVA": has attribute "carrier" → EXCLUDE
→ filter_values: ["covid vaccines"]

Example 3: Multiple Exclusion Types + Filter Extraction
Rewritten: "What is unadjusted script count by therapy class for diabetes for PBM for July {current_year}"
→ "unadjusted": in exclusion list (metric modifiers) → EXCLUDE
→ "script": in exclusion list (generic words) → EXCLUDE
→ "count": in exclusion list (generic words) → EXCLUDE
→ "by": in exclusion list (grouping keywords) → EXCLUDE
→ "therapy class": in exclusion list (attribute names) → EXCLUDE
→ "diabetes": passes all checks → EXTRACT ✅
→ "PBM": in exclusion list (common terms) → EXCLUDE
→ filter_values: ["diabetes"]

Example 4: All Exclusions (No Filter Values)
Rewritten: "What is revenue for PBM by line of business for July {current_year}"
→ "PBM": in exclusion list (common terms) → EXCLUDE
→ "by": in exclusion list (grouping keywords) → EXCLUDE
→ "line of business": in exclusion list (attribute names) → EXCLUDE
→ filter_values: []

════════════════════════════════════════════════════════════
OUTPUT FORMAT
════════════════════════════════════════════════════════════

- The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. 
- The response MUST not start with ```json and end with ```.

{{
    "rewritten_question": "complete rewritten question with full context",
    "question_type": "what|why",
    "filter_values": ["array", "of", "extracted", "filter", "values"],
    "user_message": "simple statement about what was inherited or completed - empty string if nothing to say"
}}
"""
        return prompt
