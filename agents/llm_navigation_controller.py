import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from core.logger import setup_logger, log_with_user_context

# Initialize logger for this module
logger = setup_logger(__name__)

class LLMNavigationController:
    """Single-prompt navigation controller with complete analysis, rewriting, and filter extraction"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
    
    def _log(self, level: str, message: str, state: AgentState = None, **extra):
        """Helper method to log with user context from state"""
        user_email = state.get('user_email') if state else None
        session_id = state.get('session_id') if state else None
        log_with_user_context(logger, level, f"[Navigation] {message}", user_email, session_id, **extra)
    
    def _calculate_forecast_cycle(self) -> str:
        """
        Calculate current forecast cycle based on month:
        - Feb to May: 2+10
        - Jun to Aug: 5+7
        - Sep to Oct: 8+4
        - Nov to Jan: 9+3
        """
        from datetime import datetime
        current_month = datetime.now().month
        
        if 2 <= current_month <= 5:  # February to May
            return "2+10"
        elif 6 <= current_month <= 8:  # June to August
            return "5+7"
        elif 9 <= current_month <= 11:  # September to October
            return "8+4"
        else:  # November (11), December (12), January (1)
            return "9+3"
    
    async def process_user_query(self, state: AgentState) -> Dict[str, any]:
        """
        Main entry point: Single-step LLM processing
        
        Args:
            state: Agent state containing question and history
        """
        
        current_question = state.get('current_question', state.get('original_question', ''))
        existing_domain_selection = state.get('domain_selection', [])
        total_retry_count = state.get('llm_retry_count', 0)
        
        self._log('info', "Processing user query", state, question=current_question[:100])
        
        # Single-step processing: Analyze → Rewrite → Extract in one call
        return await self._single_step_processing(
            current_question, existing_domain_selection, total_retry_count, state
        )
    
    async def _single_step_processing(self, current_question: str, existing_domain_selection: List[str], 
                                     total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """Single-step LLM processing: Analyze → Rewrite → Extract in one prompt"""
        
        self._log('info', "Starting question analysis and rewriting")
        questions_history = state.get('user_question_history', [])
        previous_question = state.get('rewritten_question', '') 
        conversation_memory = state.get('conversation_memory', {
            'dimensions': {},
            'analysis_context': {
                'current_analysis_type': None,
                'analysis_history': []
            }
        })
        history_context = questions_history[-2:] if questions_history else []
        
        # Calculate current forecast cycle based on current month
        current_forecast_cycle = self._calculate_forecast_cycle()
        self._log('info', "Calculated forecast cycle", state, cycle=current_forecast_cycle)
        
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
                    history_context,
                    current_forecast_cycle,
                    conversation_memory
                )
                # print('question prompt', prompt)
                print("Current Timestamp before question validator call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=1000,
                    temperature=0.1,  # Deterministic rewriting
                    top_p=0.7 , # Focused sampling
                    system_prompt="You are a healthcare finance analytics agent that analyzes questions, rewrites them with proper context, and extracts filter values."
                )
                print("Current Timestamp after Question validator call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print("LLM Response:", response)
                
                # Log LLM output - actual response truncated to 500 chars for safety
                self._log('info', "LLM response received from navigation validator", state,
                         llm_response=response,
                         retry_attempt=retry_count)
                
                # Check if LLM cannot answer - trigger retry
                if "Sorry, the model cannot answer this question" in response:
                    retry_count += 1
                    print(f"⚠️ LLM cannot answer - retrying ({retry_count}/{max_retries})")
                    if retry_count < max_retries:
                        await asyncio.sleep(2 ** retry_count)
                        continue
                    else:
                        print(f"Failed after {max_retries} retries - returning Optum Gateway error message")
                        error_message = "Sorry, the Optum Gateway is rejecting your request. Please try after 5 Minutes."
                        return {
                            'rewritten_question': current_question,
                            'question_type': 'what',
                            'next_agent': 'END',
                            'next_agent_disp': 'Optum Gateway temporarily unavailable',
                            'requires_domain_clarification': False,
                            'domain_followup_question': None,
                            'domain_selection': existing_domain_selection,
                            'greeting_response': error_message,
                            'llm_retry_count': total_retry_count + retry_count,
                            'pending_business_question': '',
                            'error': True,
                            'error_message': error_message,
                            'filter_values': [],
                            'user_friendly_message': error_message
                        }
                
                # Strip markdown code blocks if present
                cleaned_response = response.strip()
                if cleaned_response.startswith("```json"):
                    cleaned_response = cleaned_response[7:]  # Remove ```json
                if cleaned_response.startswith("```"):
                    cleaned_response = cleaned_response[3:]  # Remove ```
                if cleaned_response.endswith("```"):
                    cleaned_response = cleaned_response[:-3]  # Remove trailing ```
                cleaned_response = cleaned_response.strip()
                
                try:
                    result = json.loads(cleaned_response)
                except json.JSONDecodeError as json_error:
                    print(f"LLM response is not valid JSON: {json_error}")
                    print(f"Cleaned response: {cleaned_response[:200]}")
                    # Treat as greeting
                    result = {
                        'analysis': {
                            'input_type': 'greeting',
                            'is_valid_business_question': False,
                            'response_message': response.strip()
                        }
                    }
                
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
                               history_context: List, current_forecast_cycle: str, 
                               conversation_memory: Dict = None) -> str:
        """
        Combined Prompt: Analyze → Rewrite → Extract in single call
        """
        
        # Get current year and month dynamically
        from datetime import datetime
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        current_month_name = now.strftime("%B")
        
        # Calculate previous year for month context
        previous_year = current_year - 1
        
        # Special filters that are important for context inheritance
        special_filters = ["PBM", "HDP", "Specialty", "Mail", "Retail", "8+4", "5+7", "9+3", "10+2", "2+10"]
        
        # Initialize conversation memory if not provided
        if conversation_memory is None:
            conversation_memory = {
                'dimensions': {},
                'analysis_context': {
                    'current_analysis_type': None,
                    'analysis_history': []
                }
            }
        
        # Build memory context for prompt - keep as JSON for dimension key extraction
        memory_dimensions = conversation_memory.get('dimensions', {})
        memory_context_json = ""
        
        if memory_dimensions:
            memory_context_json = f"\n**CONVERSATION MEMORY (Recent Analysis Context)**:\n```json\n{json.dumps(memory_dimensions)}\n```\n"
            print('memory_context_json', memory_context_json)

        # Combine all context into a single prompt
        prompt = f"""
You are a healthcare finance analytics agent that analyzes questions, rewrites them with proper context, and extracts filter values.

====================================================
INPUTS
====================================================
Current Input: "{current_question}"
Previous Question: "{previous_question if previous_question else 'None'}"
History: {history_context}
Current Date: {current_month_name} {current_year} (Month {current_month} of {current_year})
Current Year: {current_year}
Previous Year: {previous_year}
Forecast Cycle: {current_forecast_cycle}
Memory: {memory_context_json if memory_dimensions else 'None'}

====================================================
CRITICAL: SMART YEAR ASSIGNMENT FOR PARTIAL DATES
====================================================

**WHEN USER MENTIONS ONLY MONTH/QUARTER WITHOUT YEAR:**

**Rule:** Compare the mentioned month with current month ({current_month_name}):
- If mentioned month is BEFORE or SAME as current month → Use CURRENT year ({current_year})
- If mentioned month is AFTER current month → Use PREVIOUS year ({previous_year})

**Month Ordering:** January(1) < February(2) < March(3) < April(4) < May(5) < June(6) < July(7) < August(8) < September(9) < October(10) < November(11) < December(12)

**Examples (assuming current date is {current_month_name} {current_year}):**
- User asks "December" → December (month 12) is {"BEFORE" if 12 < current_month else "AFTER" if 12 > current_month else "SAME AS"} {current_month_name} (month {current_month}) → Use {previous_year if 12 > current_month else current_year} ✓
- User asks "January" → January (month 1) is {"BEFORE" if 1 < current_month else "AFTER" if 1 > current_month else "SAME AS"} {current_month_name} (month {current_month}) → Use {previous_year if 1 > current_month else current_year} ✓
- User asks "{current_month_name}" → Same as current month → Use {current_year} ✓

**Quarter Handling:**
- Q1 (Jan-Mar): Use {previous_year if 3 < current_month else current_year}
- Q2 (Apr-Jun): Use {previous_year if 6 < current_month else current_year}
- Q3 (Jul-Sep): Use {previous_year if 9 < current_month else current_year}
- Q4 (Oct-Dec): Use {previous_year if 12 < current_month else current_year}

**CRITICAL:** When user says just "December" in {current_month_name}, they mean the MOST RECENT December, which is December {previous_year if current_month < 12 else current_year}!

====================================================
CRITICAL: FILTER INHERITANCE & SUBSTITUTION RULES
====================================================

**DOMAIN FILTERS** (PBM, HDP, SP, Specialty, Mail, PBM Retail, Home Delivery):
1. Current HAS domain + Previous HAS domain → REPLACE (use current only)
2. Current HAS domain + Previous NO domain → USE current only
3. Current NO domain + Previous HAS domain → INHERIT previous domain
4. Current NO domain + Previous NO domain → Keep as-is (no domain)

**ENTITY FILTERS** (Therapy classes, drugs, carriers, clients):
1. Current HAS entity + Previous HAS entity:
   - WITH "and/include/also/both" → ACCUMULATE both
   - WITHOUT those keywords → REPLACE (use current only)
2. Current HAS entity + Previous NO entity → USE current only
3. Current NO entity + Previous HAS entity → DO NOT inherit entities
4. Current NO entity + Previous NO entity → Keep as-is (no entities)

**EXAMPLES**:
- Prev: "PBM" → Curr: "HDP" → Final: "HDP" (domain replacement)
- Prev: "PBM" → Curr: "GLP-1" → Final: "PBM for GLP-1" (inherit domain)
- Prev: "GLP-1" → Curr: "Wegovy" → Final: "Wegovy" (entity replacement)
- Prev: "PBM for GLP-1" → Curr: "Ozempic" → Final: "PBM for Ozempic" (inherit domain, replace entity)

====================================================
PROCESSING RULES - APPLY SEQUENTIALLY
====================================================

**RULE 1: PREFIX DETECTION & STRIPPING**
Check for and strip these prefixes:
- "new question", "NEW:" → detected_prefix="new question", strip it
- "follow-up", "followup", "FOLLOW-UP:" → detected_prefix="follow-up", strip it  
- "validation", "wrong", "fix", "incorrect" → detected_prefix="validation", strip it
- None found → detected_prefix="none", clean_question=original

**RULE 2: INPUT CLASSIFICATION**
Classify clean_question into:

A. GREETING: Hi, hello, what can you do, help
   → Return: input_type="greeting", response_message="I help with healthcare analytics"

B. DML/DDL: INSERT, UPDATE, DELETE, CREATE, DROP
   → Return: input_type="dml_ddl", response_message="Data modification not supported"

C. BUSINESS_QUESTION: Contains ANY healthcare/finance term:
   - Metrics: revenue, claims, expenses, cost, scripts, forecast, actuals, billed amount
   - Entities: drugs, therapies, carriers, clients, pharmacies, GLP-1, Wegovy
   - Analysis: increase, decrease, variance, trend, breakdown, by, for
   → Return: input_type="business_question", is_valid=true

D. INVALID: Everything else
   → Return: is_valid=false, response_message="Please ask about healthcare finance"

**RULE 3: CONTEXT DECISION (Business Questions Only)**

Priority Order (STOP at first match):
1. IF detected_prefix="new question" → NEW
2. IF detected_prefix="follow-up" → FOLLOW_UP  
3. IF detected_prefix="validation" → VALIDATION
4. IF no previous_question → NEW
5. IF has pronouns (that, it, this, those) → FOLLOW_UP
6. IF missing component that previous had → FOLLOW_UP
7. ELSE → NEW

⚠️ **VALIDATION/CORRECTION CONTEXT ANALYSIS (CRITICAL)**:
When detected_prefix="validation" OR user provides hints/corrections:
- ALWAYS analyze BOTH previous_question AND current_question together
- Previous question provides BASE CONTEXT (what was asked originally)
- Current question provides CORRECTION/HINT/VALIDATION (what needs to change)
- User hints include: "use table X", "show as side-by-side", "correct the SQL", "fix this", "wrong result"
- User validations include: "no, I meant...", "actually...", "incorrect", "that's wrong"

EXAMPLES OF VALIDATION/HINT PATTERNS:
✓ Prev: "revenue for Q3" → Curr: "use Claims table instead of Ledger" → VALIDATION context
✓ Prev: "revenue by month" → Curr: "show months side by side" → VALIDATION (SQL structure hint)
✓ Prev: "cost for GLP-1" → Curr: "no, I meant specialty pharmacy only" → VALIDATION (filter correction)
✓ Prev: "scripts for July" → Curr: "wrong, get from Billing table" → VALIDATION (table hint)

**RULE 4: COMPONENT EXTRACTION**

Extract from BOTH current and previous questions:
- METRIC: revenue, cost, expense, scripts, claims, forecast, actuals
- FILTERS: "for X" patterns (PBM, HDP, Specialty, drug names, etc.)
- ATTRIBUTES: "by Y" patterns (carrier, therapy class, LOB)
- TIME: Q3 2025, July 2025, etc. (partial if no year)
- **USER HINTS**: table names, structure preferences (side-by-side, stacked), specific columns

**RULE 5: COMPONENT COMPARISON & INHERITANCE**

Extract components from BOTH current and previous, then apply:

**INTENT PATTERNS** (guide how to apply inheritance):

**Continuation Signals** → INHERIT missing components
- "what about...", "how about..." → Exploring same context
- "also show...", "and the..." → Adding to analysis

**Shift Signals** → REPLACE filters/entities
- "instead of...", "but for...", "now for..." → Explicit replacement

**Validation/Correction Signals** → COMBINE previous + current
- "use table X", "correct the SQL", "show as...", "fix this" → Append to previous
- "no, I meant...", "actually...", "incorrect" → Correction to previous
- "wrong result", "that's wrong" → Validation failure indicator

**Drill-Down Signals** → INHERIT base + ADD specificity
- "specifically...", "particularly...", "within that..." → Adding detail

**COMPONENT HANDLING:**
- **Metrics**: Current has → use current | Current missing → inherit from previous
- **Domain Filters** (PBM, HDP, etc.): Apply 4-case domain inheritance rules above
- **Entity Filters** (drugs, therapies, etc.): Apply 4-case entity inheritance rules above
- **Attributes** (by carrier, by LOB, for each X): Current has → ADD to previous | Current missing → inherit
- **User Hints** (table names, structure): ALWAYS preserve and append to previous question
- **Time**: Current has → use current | **Current missing → ALWAYS inherit from previous (MANDATORY for follow-ups)**

⚠️ TIME INHERITANCE IS MANDATORY: For follow-up questions without time, copy the EXACT time reference from previous (month, quarter, year).

**CRITICAL - Breakdown/Attribute Requests**:
When current says "for each X", "by X", "breakdown by X" - this is ADDING an attribute, NOT replacing domain:
- Prev: "revenue for PBM for July" → Curr: "for each LOB" → Final: "revenue for PBM for each LOB for July" ✓
- INHERIT domain if missing from current (use case #3 from domain rules)

**RULE 6: METRIC INHERITANCE (CRITICAL)**

IF question contains growth/decline terms WITHOUT preceding metric:
- Terms: decline, growth, increase, decrease, trending, rising, falling
- Check: Is there a metric BEFORE the term?
  * "revenue decline" → HAS metric ✓
  * "the decline" → NO metric ✗
- Action: Inherit metric from previous question, place BEFORE the term

**RULE 7: MEMORY DIMENSION TAGGING**

IF memory exists AND value found in memory:
1. Extract dimension KEY from memory (not value)
2. Add dimension prefix: "for [dimension_key] [value]"

Example with memory {{"client_id": ["57760"]}}:
- User: "revenue for 57760"
- Tag: "revenue for client_id 57760"

**RULE 8: FORECAST CYCLE RULES**

Apply to rewritten question:
- "forecast" alone → Add: "forecast {current_forecast_cycle}"
- "actuals vs forecast" → Add: "actuals vs forecast {current_forecast_cycle}"
- Cycle alone (8+4) → Add: "forecast 8+4"
- Both present → Keep as-is

**RULE 9: BUILD REWRITTEN QUESTION**

Based on context_decision:

NEW:
- Use current components + apply Rules 6-8
- **Add year to partial dates using SMART YEAR ASSIGNMENT rules above**
- If month/quarter has no year → Compare with current month and assign correct year
- Examples:
  * User: "December" (asked in {current_month_name}) → "December {previous_year if current_month < 12 else current_year}"
  * User: "Q4" (asked in {current_month_name}) → "Q4 {previous_year if current_month <= 12 and current_month not in [10,11,12] else current_year}"

FOLLOW_UP:
- Start with current components
- Check domain inheritance: If current NO domain but previous HAS domain → Insert "for [DOMAIN]" 
- Check entity inheritance: Apply entity 4-case rules (do NOT auto-inherit entities)
- Inherit missing components from previous:
  * Metrics: Current missing → inherit from previous
  * Attributes: Current missing → inherit from previous
  * **Time: Current missing → ALWAYS inherit from previous** (Q3 2025, July 2025, etc.)
- Apply Rules 6-8
- user_message: "Using [inherited items] from previous question"

⚠️ TIME INHERITANCE: If current question has NO time reference (no month, quarter, year), ALWAYS copy the complete time reference from previous question.

Examples:
- Prev: "revenue for PBM for July" → Curr: "for each LOB" → Final: "What is revenue for PBM for each line of business for July 2025" ✓
- Prev: "revenue for PBM" → Curr: "volume for GLP-1" → Final: "What is volume for PBM for GLP-1" ✓
- Prev: "revenue for Q3 2025" → Curr: "what about HDP" → Final: "What is revenue for HDP for Q3 2025" ✓

VALIDATION/Correction handling:
- **CRITICAL: Analyze BOTH previous AND current together to understand full context**
- Format: "[Previous Question] - [User's correction/validation/hint]"
- Keep previous question EXACTLY as-is, do NOT rewrite it
- Append the user's current input as the correction/validation/hint point
- Preserve ALL user hints (table names, structure preferences, corrections)
- user_message: "Applying validation/correction/hint to previous question"
- Examples: 
  * Prev: "What is revenue for PBM for Q3 2025 vs Q4 2025"
  * Curr: "correct the sql to show months side by side"
  * Result: "What is revenue for PBM for Q3 2025 vs Q4 2025 - correct the sql to show months side by side"
  
  * Prev: "What is revenue for GLP-1 drugs for July 2025"
  * Curr: "use Claims table instead of Ledger"
  * Result: "What is revenue for GLP-1 drugs for July 2025 - use Claims table instead of Ledger"

**RULE 10: FILTER VALUE EXTRACTION**

⚠️ CRITICAL: Extract filter values from the FINAL REWRITTEN question (after all inheritance applied)

From REWRITTEN question (including inherited filters), extract filter values:

**WHAT TO EXTRACT:**
- The actual entity VALUE after "for" or "by"
- Drug names, therapy names, carrier codes, client names
- Special codes like PBM, HDP, 8+4, forecast cycles

**IMPORTANT**: If you inherited domain (e.g., PBM) in RULE 9, it MUST appear in both:
1. The rewritten_question text
2. The filter_values array

**STEP 1: Preserve Compound Terms**
Keep multi-word terms together matching these patterns:
[word] fee → "admin fee", "EGWP fee", "dispensing fee", "network fee"
[word] rate → "generic rate", "discount rate"
[word] pharmacy → "specialty pharmacy", "retail pharmacy"
[word] delivery → "home delivery", "mail delivery"

**STEP 2: Extract These Values**
Domains: PBM, HDP, SP, Specialty, Mail, Retail, Home Delivery
Forecast cycles: 8+4, 4+8, 0+12
Drug names, therapy classes, carrier codes, client codes
Compound fee types from Step 1

**STEP 3: Strip Dimension Prefixes AND Suffixes**
Prefixes to strip:
- "for drug name Wegovy" → "Wegovy"
- "for therapy class GLP-1" → "GLP-1"
- "for carrier MDOVA" → "MDOVA"

Suffixes to strip:
- "GLP-1 drugs" → "GLP-1"
- "Wegovy medication" → "Wegovy"
- "diabetes therapy" → "diabetes"
- "Mounjaro drug" → "Mounjaro"

**STEP 4: Never Extract**
Dimension labels: therapy class, drug name, carrier, client, LOB, line of business
Pure standalone metrics: revenue, cost, expense, volume, scripts, claims (BUT KEEP compound fees like "admin fee")
Operators: by, for, breakdown, versus, vs
Time: Q1-Q4, months, years
Pure numbers without context

====================================================
QUICK REFERENCE EXAMPLES
====================================================

NEW: "new question - What is PBM revenue Q3 2025?"
→ Strip prefix, components complete → "What is revenue for PBM for Q3 2025"
→ Filters: ["PBM"]

FOLLOW_UP + SUBSTITUTION: "what about HDP" (Prev: "PBM revenue Q3")
→ Intent: domain switch, inherit metric/time → "What is revenue for HDP for Q3 2025"
→ Filters: ["HDP"] (replaced PBM)

DRILL-DOWN: "specifically Wegovy" (Prev: "GLP-1 revenue")
→ Intent: drill-down, inherit metric → "What is revenue for Wegovy"
→ Filters: ["Wegovy"] (replaced GLP-1 with more specific)

METRIC INHERITANCE: "show the decline" (Prev: "revenue for PBM")
→ No metric before "decline" → "show the revenue decline for PBM"
→ Filters: ["PBM"]

====================================================
OUTPUT FORMAT
====================================================
**CRITICAL REQUIREMENTS:**
1. Return ONLY valid JSON - no markdown, no code blocks, no extra text
2. Do NOT wrap in ```json or ``` 

{{
  "analysis": {{
    "detected_prefix": "new question|follow-up|validation|none",
    "input_type": "greeting|dml_ddl|business_question",
    "is_valid_business_question": true|false,
    "response_message": "for non-business questions only",
    "context_decision": "NEW|FOLLOW_UP|VALIDATION"
  }},
  "rewrite": {{
    "rewritten_question": "complete question with context",
    "question_type": "what|why",
    "user_message": "what was inherited/added (empty if nothing)"
  }},
  "filters": {{
    "filter_values": ["ONLY", "VALUES", "NO", "ATTRIBUTE", "NAMES"]
  }}
}}

**WRONG FORMAT (DO NOT USE):**
```json
{{ ... }}
```

"""
        return prompt
