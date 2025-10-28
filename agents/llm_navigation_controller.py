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
        
        questions_history = state.get('user_question_history', [])
        previous_question = questions_history[-1]['rewritten_question'] if questions_history else ""
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
                
                print("Calling PROMPT 1 (Decision Maker)...")
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
                    
                    print("Calling PROMPT 2 (Rewriter)...")
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
SECTION 1: SYSTEM KNOWLEDGE
════════════════════════════════════════════════════════════

This system analyzes healthcare finance data including:
- **Claims & Ledgers**: claim volumes, costs, utilization, reconciliation, billing, invoices
- **Pharmacy & Drugs**: drug analysis (revenue/volume/performance), therapy classes, medications, pharmaceutical trends, services
- **Financial Metrics**: revenue, expenses, payments, budgets, forecasts, invoices, operating expenses

**Important**: Drug, therapy class, medication, and pharmaceutical questions are CORE healthcare finance topics.

════════════════════════════════════════════════════════════
SECTION 2: SQL VALIDATION DETECTION
════════════════════════════════════════════════════════════

**Check if this is a SQL validation request:**

Validation keywords: "validate", "check", "verify", "correct", "fix", "wrong", "incorrect result", "not getting correct", "query is wrong", "SQL is wrong", "result doesn't look right"

**If validation keywords detected:**
→ Set is_validation = true
→ This will be formatted as: "[Previous Question] - VALIDATION REQUEST: [current input]"

════════════════════════════════════════════════════════════
SECTION 3: INPUT CLASSIFICATION
════════════════════════════════════════════════════════════

Classify user input into ONE category:

**GREETING** - Greetings, capability questions, general chat, dataset availability questions
Examples: "Hi", "Hello", "What can you do?", "Help me", "What data do you have about claims?"

**DML/DDL** - Data modification requests (NOT supported)
Examples: "INSERT data", "UPDATE table", "DELETE records", "CREATE table", "DROP column"

**BUSINESS_QUESTION** - Healthcare finance queries
Must be VALIDATED for healthcare relevance:

✅ **VALID**: Claims, ledgers, payments, members, providers, pharmacy, drugs, medications, therapy classes, pharmaceuticals, revenue, expenses, invoices, medical costs, utilization, financial metrics, actuals, forecasts

❌ **INVALID**: Weather, sports, retail, manufacturing, general technology, entertainment, non-healthcare topics

════════════════════════════════════════════════════════════
SECTION 4: COMPONENT DETECTION & DECISION
════════════════════════════════════════════════════════════

**Your job: Detect components and decide NEW vs FOLLOW-UP (don't rewrite yet)**

### Step 1: Analyze CURRENT question components

Identify what CURRENT question explicitly mentions (these are EXAMPLES - recognize similar concepts):

- **Metric** (any measurable value): 
  Examples: revenue, expense, claims, billed amount, cost, volume, payments, utilization, actuals, forecast, margin, profit, loss, rate, count, total, spend, amount, fee, percentage
  → Recognize ANY financial/operational metric, not just these examples

- **Scope** (who/what/where the analysis is about):
  Examples: carrier names (carrier XYZ), geography (California, North region), Line of Business (Commercial, Medicare, Medicaid), departments, provider types, service categories, plan types, segments
  → Recognize ANY grouping/filtering dimension, not just these examples

- **Time Period**: 
  - Full time: "August 2025", "Q3 2025", "July 2024" (month/quarter/year together)
  - Partial time: "August" (month only), "Q3" (quarter only) ← Mark time_is_partial = true
  → Recognize ANY time reference (dates, periods, ranges)

- **Filters** (specific items being analyzed):
  Examples: drug names (covid vaccine, Humira), therapy classes (diabetes, asthma), categories, procedure codes, diagnosis codes, NDC codes, member IDs, claim types
  → Recognize ANY specific filter value, not just these examples

- **Signals** (words indicating user's intent):
  - Pronouns: "that", "it", "those", "this", "these"
  - Continuation verbs: "compare", "show me", "what about", "for", "breakdown", "drill"
  - Aggregate words: "overall", "as a whole", "all carriers", "total", "across the board"
  - Validation words: "validate", "wrong", "incorrect", "fix", "check"
  → Recognize similar words with same intent

### Step 2: Analyze PREVIOUS question components (if exists)

Identify same components from previous question.

### Step 3: Make Decision (NEW vs FOLLOW-UP)

**Decision Logic:**

IF validation keywords detected → context_decision = "VALIDATION"

ELSE IF no previous question → context_decision = "NEW"

ELSE IF current has pronouns ("that", "it", "this", "those") → context_decision = "FOLLOW-UP"

ELSE IF current has ALL THREE components (metric + scope + time with year):
→ context_decision = "NEW" (complete question, starting fresh)

ELSE IF current has continuation verbs ("compare", "for", "show me") AND missing components:
→ context_decision = "FOLLOW-UP" (continuing previous analysis)

ELSE IF current missing one or more components (metric, scope, or time):
→ context_decision = "FOLLOW-UP" (needs to inherit missing parts)

ELSE IF current semantically related to previous:
→ context_decision = "FOLLOW-UP"

ELSE → context_decision = "NEW"

════════════════════════════════════════════════════════════
DECISION EXAMPLES
════════════════════════════════════════════════════════════

Example 1: Complete New Question
Previous: "What is revenue for carrier MDOVA for Q3"
Current: "Show me claims for carrier BCBS for July 2025"
→ Has metric (claims) + scope (BCBS) + time (July 2025) = COMPLETE
→ Decision: NEW

Example 2: Follow-up with Pronoun
Previous: "What is revenue for carrier MDOVA for Q3"
Current: "why is that high"
→ Has pronoun "that"
→ Decision: FOLLOW-UP

Example 3: Missing Components
Previous: "What is revenue for carrier MDOVA for Q3"
Current: "for covid vaccines"
→ Missing metric, missing scope, missing time
→ Decision: FOLLOW-UP

Example 4: Continuation Verb with Missing Scope
Previous: "What is actuals vs forecast for PBM for August"
Current: "compare actuals vs forecast for September"
→ Has continuation verb "compare"
→ Has metric (actuals vs forecast) + time (September)
→ Missing scope (PBM not mentioned)
→ Missing year (September without year)
→ Decision: FOLLOW-UP

Example 5: Validation Request
Previous: "What is revenue for Specialty for July 2025"
Current: "validate the last query, not getting correct result"
→ Has validation keywords
→ Decision: VALIDATION

════════════════════════════════════════════════════════════
OUTPUT FORMAT (JSON ONLY - NO MARKDOWN)
════════════════════════════════════════════════════════════

Return ONLY valid JSON. Do NOT include ```json``` or any markdown.

{{
    "input_type": "greeting|dml_ddl|business_question",
    "is_valid_business_question": true|false,
    "response_message": "message if greeting/dml, empty string otherwise",
    "is_validation": true|false,
    "context_decision": "NEW|FOLLOW_UP|VALIDATION",
    "components_current": {{
        "metric": "value or null",
        "scope": "value or null",
        "time": "value or null",
        "time_is_partial": true|false,
        "filters": ["list", "of", "filters"],
        "signals": {{
            "has_pronouns": true|false,
            "has_continuation_verbs": true|false,
            "has_aggregate_words": true|false,
            "has_validation_words": true|false
        }}
    }},
    "components_previous": {{
        "metric": "value or null",
        "scope": "value or null",
        "time": "value or null",
        "filters": ["list"]
    }},
    "reasoning": "brief explanation of decision"
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
SECTION 4: EXECUTE CONTEXT INHERITANCE
════════════════════════════════════════════════════════════

Based on context_decision: {decision_json.get('context_decision', 'NEW')}

**IF context_decision = "NEW":**
→ Use current question as-is (properly formatted)
→ No inheritance from previous
→ Format professionally: "What is the [metric] for [scope] for [time]"

**IF context_decision = "VALIDATION":**
→ Format: "[Previous Question] - VALIDATION REQUEST: [current question]"
→ Keep everything from previous question

**IF context_decision = "FOLLOW_UP":**
→ Apply inheritance rules below ↓

### FOLLOW-UP INHERITANCE RULES (Apply in order):

**Rule 1: Current Always Wins**
If current mentions a component → use current's value (don't override with previous)
Example: Current says "carrier ABC", don't use previous "carrier XYZ"

**Rule 2: Fill Only What's Missing**
Look at what current lacks and fill from previous:
- Missing metric → fill from previous
- Missing scope → fill from previous
- Missing time → fill from previous
- Missing filters → inherit from previous (unless aggregate signal detected)

**Rule 3: Component Replacement**
If current mentions same TYPE but different value → REPLACE
- Previous: "carrier XYZ", Current: "carrier ABC" → Use ABC
- Previous: "Q1", Current: "Q2" → Use Q2
- Previous: "diabetes drugs", Current: "asthma drugs" → Use asthma

**Rule 4: Year Defaulting (CRITICAL for fixing year problem)**
If time_is_partial = true in current components:
→ Add current year ({current_year}) automatically
Examples:
- "August" → "August {current_year}"
- "Q3" → "Q3 {current_year}"
- "September" → "September {current_year}"
→ Track this in user_message: "Added {current_year} as the year."

**Rule 5: Pronouns Fill Everything**
If has_pronouns = true:
→ Inherit ALL components (metric + scope + time + filters)
Example: "why is that high" → inherit everything

**Rule 6: Continuation Verbs Inherit Context**
If has_continuation_verbs = true ("compare", "for", "show me"):
→ This is continuing previous analysis
→ Inherit missing components (especially scope and time)
Example: "compare actuals vs forecast for September" 
→ If previous had "PBM" scope, inherit it
→ If "September" has no year, add current year ({current_year})

**Rule 7: Breakdown Pattern - Metric vs Filter**
If pattern is "[X] breakdown" or "breakdown of [X]":

Check if X is a METRIC:
- Metrics: revenue, expense, claims, billed amount, cost, volume, payments, utilization, actuals, forecast
- If X is metric → use X as metric, fill scope/time from previous
- If X is NOT metric (drug name, category) → keep previous metric, add X as filter

**Rule 8: Aggregate Signals Clear Filters**
If has_aggregate_words = true ("overall", "all carriers", "as a whole", "total"):
→ Fill metric + time from previous
→ DROP all scope filters and specific item filters
→ User wants big picture without filters

### Edge Cases:

**Same Metric, Different Time:**
If current has same metric as previous but different time period:
→ Replace time
→ Inherit scope if missing
Example: 
- Previous: "actuals vs forecast for PBM for August"
- Current: "compare actuals vs forecast for September"
→ Result: "Compare actuals vs forecast for PBM for September 2025"
→ Inherited: scope (PBM)
→ Auto-completed: year (2025)

**Partial Overlap:**
If current has some components but not all:
→ Fill what's missing from previous
Example:
- Previous: "revenue for carrier MDOVA for Q3"
- Current: "for covid vaccines"
→ Result: "What is revenue for covid vaccines for carrier MDOVA for Q3"
→ Inherited: metric (revenue), scope (MDOVA), time (Q3)

### Construct Final Rewritten Question:

Combine current + inherited components → complete professional question
Pattern: "What is the [metric] for [filters] for [scope] for [timeframe]"
OR for why questions: "Why is the [metric] for [filters] for [scope] for [timeframe] [condition]"

Transform informal to formal:
- "I need expense" → "What is the expense..."
- "show me revenue" → "What is the revenue..."

Proper grammar, capitalize first letter, trim spaces.

════════════════════════════════════════════════════════════
SECTION 6: FILTER VALUES EXTRACTION
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
- **Common terms**: PBM, HDP, Home Delivery, SP, Specialty, Mail, Claim Fee, Claim Cost, Activity Fee
- **Time/Date**: months (January-December), quarters (Q1-Q4), years (2024, 2025), dates
- **Generic words**: revenue, cost, expense, data, analysis, total, overall, what, is, for, the
- **Metrics**: revenue, billed amount, claims count, expense, volume, actuals, forecast
→ If in ANY category: EXCLUDE

**Step 4: Does it contain letters?**
→ If YES and passed above checks: EXTRACT ✅

**Multi-word handling:**
Keep phrases together: "covid vaccine" → ["covid vaccine"]
Multiple terms: "diabetes, asthma" → ["diabetes", "asthma"]

**EXAMPLES:**

Example 1:
Rewritten: "What is the revenue for MPDOVA for September {current_year}"
→ "MPDOVA": no attribute ✓, not pure number ✓, not in exclusion ✓, has letters ✓ → EXTRACT ✅
→ filter_values: ["MPDOVA"]

Example 2:
Rewritten: "What is actuals vs forecast for PBM for September {current_year}"
→ "PBM": in exclusion list (common terms) → EXCLUDE
→ filter_values: []

Example 3:
Rewritten: "What is revenue for covid vaccines for carrier MDOVA for Q3"
→ "covid vaccines": no attribute ✓, not pure number ✓, not in exclusion ✓, has letters ✓ → EXTRACT ✅
→ "MDOVA": has attribute "carrier" → EXCLUDE
→ filter_values: ["covid vaccines"]

Example 4:
Rewritten: "What is claim number 12345 for cardiology for HDP for July {current_year}"
→ "12345": has attribute "claim number" → EXCLUDE
→ "cardiology": passes all checks → EXTRACT ✅
→ "HDP": in exclusion list (common terms) → EXCLUDE
→ filter_values: ["cardiology"]

════════════════════════════════════════════════════════════
REWRITING EXAMPLES
════════════════════════════════════════════════════════════

Example 1: Year Defaulting
Decision: FOLLOW_UP, time_is_partial = true
Current: "actuals vs forecast for PBM for August"
Previous: None
→ Rewritten: "What is actuals vs forecast for PBM for August {current_year}"
→ User message: "Added {current_year} as the year."

Example 2: Missing Scope with Continuation Verb
Decision: FOLLOW_UP
Current: "compare actuals vs forecast for September"
Previous: "What is actuals vs forecast for PBM for August {current_year}"
Components: metric same, scope missing, time different (partial)
→ Rewritten: "Compare actuals vs forecast for PBM for September {current_year}"
→ User message: "I'm using PBM from your last question. Added {current_year} as the year."

Example 3: Drill-down with Filter
Decision: FOLLOW_UP
Current: "for covid vaccines"
Previous: "What is revenue for carrier MDOVA for Q3"
→ Rewritten: "What is revenue for covid vaccines for carrier MDOVA for Q3"
→ User message: "I'm using revenue, carrier MDOVA, Q3 from your last question."
→ filter_values: ["covid vaccines"]

Example 4: Pronoun Reference
Decision: FOLLOW_UP, has_pronouns = true
Current: "why is that high"
Previous: "What is revenue for carrier MDOVA for Q3"
→ Rewritten: "Why is the revenue for carrier MDOVA for Q3 high"
→ User message: "I'm using revenue, carrier MDOVA, Q3 from your last question."
→ question_type: "why"

Example 5: New Complete Question
Decision: NEW
Current: "Show me claims for carrier BCBS for July {current_year}"
→ Rewritten: "What are the claims for carrier BCBS for July {current_year}"
→ User message: ""
→ filter_values: []

Example 6: Validation Request
Decision: VALIDATION
Current: "validate the query, results look wrong"
Previous: "What is revenue for Specialty for July {current_year}"
→ Rewritten: "What is revenue for Specialty for July {current_year} - VALIDATION REQUEST: validate the query, results look wrong"
→ User message: "Validating previous query."

════════════════════════════════════════════════════════════
OUTPUT FORMAT
════════════════════════════════════════════════════════════

Return valid JSON without markdown formatting.

{{
    "rewritten_question": "complete rewritten question with full context",
    "question_type": "what|why",
    "filter_values": ["array", "of", "extracted", "filter", "values"],
    "user_message": "simple statement about what was inherited or completed - empty string if nothing to say"
}}
"""
        return prompt
