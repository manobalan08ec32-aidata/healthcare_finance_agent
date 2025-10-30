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
        """Main entry point: Two-step LLM processing"""
        
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
                                       history_context: List) -> str:
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

**User Command Override** (Check first!)
IF current starts with "NEW:" → Decision: NEW (respect user)
IF current starts with "FOLLOW-UP:" → Decision: FOLLOW-UP (respect user)

**Otherwise, apply this logic IN ORDER:**

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

════════════════════════════════════════════════════════════
EXAMPLES
════════════════════════════════════════════════════════════

Example 1: Complete New Question (Has Filters)
Current: "revenue for PBM for Q3 2025"
→ metric: "revenue", filters: ["PBM"], time: "Q3 2025"
→ Has metric + filters + time (all present) → Decision: NEW

Example 2: Complete Question with Filters (Your Key Case)
Previous: "revenue by line of business for PBM for Q3 2025"
Current: "actuals for PBM for September 2025"
→ metric: "actuals", filters: ["PBM"], time: "September 2025"
→ Has metric + filters + time (all present) → Decision: NEW

Example 3: Has Attributes but Missing Filters (FOLLOW-UP!)
Current: "revenue by line of business for 2025"
Previous: "revenue for PBM for Q3 2025"
→ metric: "revenue", filters: [], attributes: ["line of business"], time: "2025"
→ Has metric + attributes + time, but MISSING filters → Decision: FOLLOW-UP
→ Note: Will inherit "PBM" from previous

Example 4: Missing Filters (No Attributes)
Current: "compare actuals for September"
Previous: "revenue for PBM for Q3 2025"
→ metric: "actuals", filters: [], time: "September" (partial)
→ Has metric + time, but MISSING filters → Decision: FOLLOW-UP

Example 5: Forecast Cycle Preserved
Current: "actuals vs forecast 8+4 for Specialty for Q3 2025"
→ metric: "actuals vs forecast 8+4", filters: ["Specialty"], time: "Q3 2025"
→ Has metric (with cycle) + filters + time → Decision: NEW
→ Note: "8+4" is part of the metric

Example 6: Pronoun Reference
Current: "why is that high"
→ Has pronoun "that" → Decision: FOLLOW-UP

Example 7: User Command
Current: "NEW: revenue for Specialty"
→ Starts with "NEW:" → Decision: NEW (respect command)

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
    "reasoning": "brief explanation"
}}

"""
        return prompt
    
    def _build_prompt_2_rewriter(self, current_question: str, previous_question: str, 
                                 decision_json: dict) -> str:
        """
        PROMPT 2: Rewriter
        Job: Execute rewriting based on Prompt 1's decision + Extract filters
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

Decision from PROMPT 1:
{json.dumps(decision_json, indent=2)}

Current Year: {current_year}

════════════════════════════════════════════════════════════
SECTION 1: EXECUTION RULES
════════════════════════════════════════════════════════════

Based on context_decision: {decision_json.get('context_decision', 'NEW')}

**IF NEW:**
→ Use current question as-is
→ Format professionally: "What is [metric] for [filters] for [time]"
→ No inheritance

**IF VALIDATION:**
→ Format: "[Previous Question] - VALIDATION REQUEST: [current input]"

**IF FOLLOW-UP:**
→ Apply inheritance rules below

**IMPORTANT: Metric Preservation**
When the metric includes forecast cycles or modifiers, preserve them exactly:
- "actuals vs forecast 8+4" → Keep "8+4" as part of metric
- "actuals vs forecast 5+7" → Keep "5+7" as part of metric
- These cycles are part of the metric specification, not filters

════════════════════════════════════════════════════════════
SECTION 2: INHERITANCE RULES (FOLLOW-UP only)
════════════════════════════════════════════════════════════

Apply these 5 core rules:

**Rule 1: Fill What's Missing**
If component is missing (null/empty) in current → inherit from previous
- Missing metric → inherit previous metric
- Missing filters → inherit previous filters
- Missing attributes → inherit previous attributes
- Missing time → inherit previous time

**Rule 2: Complete Question → Don't Inherit Attributes (CRITICAL)**

If current has ALL of these: metric + filters + time
→ DON'T inherit attributes from previous
→ User provided complete specification, don't add grouping dimensions

If current has metric + attributes + time BUT NO filters:
→ This is NOT complete
→ DO inherit filters from previous
→ Keep the attributes that are in current

Examples:
- Previous: "revenue by line of business for PBM for Q3"
           ↑ has attribute "by line of business", filter "PBM"
- Current: "actuals for PBM for September"
           ↑ has metric + filters + time (complete!)
- Result: "actuals for PBM for September"
          ↑ NO "by line of business" inherited
- Reason: Current is complete with filters, don't add attributes

BUT if current has attributes but NO filters:
- Previous: "revenue for PBM for Q3"
- Current: "revenue by line of business for 2025"
           ↑ has metric + attributes + time, but NO filters
- Result: "revenue for PBM by line of business for 2025"
          ↑ Inherited "PBM", kept "by line of business"
- Reason: Current missing filters, so inherit them and keep attributes

**Rule 3: Add Current Year for Partial Time**
If time_is_partial = true → add {current_year}
- "August" → "August {current_year}"
- "Q3" → "Q3 {current_year}"

**Rule 4: Pronouns Inherit Everything**
If has_pronouns = true → inherit ALL components
- "why is that high" → inherit metric + filters + attributes + time

**Rule 5: Current Always Wins**
If current has a component → use current's value (don't override)
- Current says "for Specialty", previous had "for PBM" → use Specialty

════════════════════════════════════════════════════════════
SECTION 3: FILTER VALUES EXTRACTION
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
EXAMPLES
════════════════════════════════════════════════════════════

Example 1: Year Defaulting
Decision: FOLLOW-UP, time_is_partial: true
Current: "actuals for PBM for August"
→ Rewritten: "What is actuals for PBM for August {current_year}"
→ user_message: "Added {current_year} as the year."

Example 2: Missing Filters (Inherit Filters)
Decision: FOLLOW-UP
Current: "compare actuals for September"
Previous: "revenue for PBM for Q3 {current_year}"
→ Current MISSING filters
→ Rewritten: "Compare actuals for PBM for September {current_year}"
→ user_message: "I'm using PBM from your last question. Added {current_year} as the year."

Example 3: Complete Question - Don't Inherit Attributes (YOUR KEY CASE)
Decision: FOLLOW-UP (because partial time)
Current: "actuals for PBM for September"
         ↑ has metric + filters + time (complete structure!)
Previous: "revenue by line of business for PBM for Q3"
         ↑ had attribute "by line of business"

Analysis:
- Current has: metric ("actuals") + filters ("PBM") + time ("September")
- Even though time is partial, current has COMPLETE structure with filters
- Rule 2 applies: Don't inherit attributes from previous
- Rule 3 applies: Add year for partial time

→ Rewritten: "What is actuals for PBM for September {current_year}"
             ↑ NO "by line of business" inherited!
→ user_message: "Added {current_year} as the year."

Example 4: Has Attributes but Missing Filters (Inherit Filters, Keep Attributes)
Decision: FOLLOW-UP
Current: "revenue by line of business for 2025"
         ↑ has attributes but MISSING filters
Previous: "revenue for PBM for Q3 {current_year}"
         ↑ had filter "PBM"

Analysis:
- Current has: metric + attributes + time, but NO filters
- Rule 1 applies: Inherit missing filters from previous
- Keep attributes from current

→ Rewritten: "What is revenue for PBM by line of business for 2025"
             ↑ Inherited "PBM", kept "by line of business"
→ user_message: "I'm using PBM from your last question."

Example 5: Forecast Cycle Preserved
Decision: FOLLOW-UP
Current: "compare actuals vs forecast 8+4 for September"
Previous: "actuals vs forecast 8+4 for Specialty for Q3"
→ Metric includes "8+4" cycle (preserved)
→ Missing filters, inherit "Specialty"
→ Rewritten: "Compare actuals vs forecast 8+4 for Specialty for September {current_year}"
→ user_message: "I'm using Specialty from your last question. Added {current_year} as the year."

Example 6: Pronoun (Inherit All)
Decision: FOLLOW-UP, has_pronouns: true
Current: "why is that high"
Previous: "revenue for carrier MDOVA for Q3"
→ Rewritten: "Why is the revenue for carrier MDOVA for Q3 high"
→ user_message: "I'm using revenue, carrier MDOVA, Q3 from your last question."

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
