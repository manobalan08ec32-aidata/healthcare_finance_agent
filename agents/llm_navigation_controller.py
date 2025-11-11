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
        conversation_memory = state.get('conversation_memory', {
            'dimensions': {},
            'analysis_context': {
                'current_analysis_type': None,
                'analysis_history': []
            }
        })
        history_context = questions_history[-2:] if questions_history else []
        print('conversation_memory',conversation_memory)
        # Calculate current forecast cycle based on current month
        current_forecast_cycle = self._calculate_forecast_cycle()
        print(f"📊 Current forecast cycle: {current_forecast_cycle}")
        
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
                response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=3000,
                    temperature=0.0,  # Deterministic rewriting
                    top_p=0.1  # Focused sampling
                )
                
                print("LLM Response:", response)
                
                # Check if LLM cannot answer - trigger retry
                if "Sorry, the model cannot answer this question" in response:
                    retry_count += 1
                    print(f"⚠️ LLM cannot answer - retrying ({retry_count}/{max_retries})")
                    if retry_count < max_retries:
                        await asyncio.sleep(2 ** retry_count)
                        continue
                    else:
                        print(f"Failed after {max_retries} retries - returning error response")
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
        
        # Get current year dynamically
        from datetime import datetime
        current_year = datetime.now().year
        
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
        
        # Build memory context for prompt
        memory_dimensions = conversation_memory.get('dimensions', {})
        memory_context_str = ""
        
        if memory_dimensions:
            memory_context_str = "\n**CONVERSATION MEMORY (Recent Analysis Context)**:\n"
            for dim_key, values in memory_dimensions.items():
                # Show dimension and its values (full list, not just first element)
                values_str = str(values) if isinstance(values, list) else str([values])
                memory_context_str += f"- {dim_key}: {values_str}\n"
            memory_context_str += "\n**MEMORY USAGE RULE**: If user mentions a value that exists in memory (case-insensitive, fuzzy match), ADD the dimension prefix when rewriting.\n"
            memory_context_str += "**CRITICAL**: Use the EXACT dimension key name from memory (e.g., 'client_name', 'client_id', 'drug_name'). Do NOT convert or simplify.\n"
            memory_context_str += "**Example**: Memory has client_name: ['BCBSM', ...] and user says 'BCBSM' → Rewrite as 'for client_name BCBSM' (NOT 'for client BCBSM')\n"
            memory_context_str += "**Priority**: If value appears in MULTIPLE dimensions, use the LATEST (last) dimension.\n"
            memory_context_str += "**Fuzzy Match**: Case-insensitive matching for values (e.g., 'wegovy' matches 'WEGOVY'), but use EXACT case from memory.\n\n"
            print('memory_context_str',memory_context_str)
        prompt = f"""⚠️⚠️⚠️ CRITICAL ROLE - READ THIS FIRST ⚠️⚠️⚠️

You are a QUESTION REWRITER and ANALYZER - NOT an assistant that answers questions.
Your ONLY job is to:
1. Analyze the user's question
2. Rewrite it with proper context
3. Extract filter values

You do NOT answer business questions. You do NOT provide data or insights.
You ONLY rewrite questions into a complete format.

Think of yourself as: analyze(question) → rewrite(question) → extract(filters) → JSON output

⚠️ REMEMBER: You are NOT answering the question. You are ONLY rewriting it.

════════════════════════════════════════════════════════════
INPUT INFORMATION
════════════════════════════════════════════════════════════
User Input: "{current_question}"
Previous Question: "{previous_question if previous_question else 'None'}"
History: {history_context}
Current Year: {current_year}
Current Forecast Cycle: {current_forecast_cycle}

Special Context Filters (important for inheritance): {special_filters}

{memory_context_str}

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
⚠️ **IMPORTANT: If the question mentions ANY of these, it's a VALID business question:**
- **Metrics**: revenue, claims, expenses, cost, volume, actuals, forecast, script count, utilization, payments, script, prescription, billed amount
- **Healthcare entities**: drugs, medications, therapy classes (GLP-1, SGLT-2, etc.), carriers, clients, pharmacies, NDC, drug names (Wegovy, Ozempic, etc.)
- **Pharmacy terms**: PBM, HDP, Specialty, Mail, Retail, Home Delivery, pharmacy channel
- **Finance terms**: increase, decrease, decline, growth, variance, comparison, trend, breakdown
- **Time comparisons**: Q3 2025 vs Q3 2024, year-over-year, month-over-month, quarterly

✅ **Valid Examples:**
- "what SGLT-2 Inhibitors drugs the PBM revenue increase" → VALID (drug class + PBM + revenue)
- "show me actuals vs forecast 8+4 revenue for Q3 2025" → VALID (drug name + revenue + time)

❌ **Invalid Examples:**
- "What's the weather today?" → INVALID (not healthcare/finance)
- "Show me sports scores" → INVALID (not healthcare/finance)
- "Calculate 2+2" → INVALID (not business related)

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

***Decision Examples (Use these patterns):**

**Real Conversation Chain from Production:**

Ex1: Current: "new question - What is PBM revenue for Q3 2025 compared to Q3 2024"
→ Prefix: "new question" → NEW | Has: metric+filters+time (complete) → Use as-is

Ex2: Current: "follow-up - What therapies contributed to increase" | Prev: "PBM revenue Q3 2025 vs Q3 2024"
→ Prefix: "follow-up" → FOLLOW_UP | "increase" missing metric → inherit "revenue"
→ Missing: filters (PBM), time → inherit both
→ Rewrite: "What therapies contributed to revenue increase for PBM for Q3 2025 vs Q3 2024"

Ex3: Current: "follow-up - what GLP-1 drugs drove the increase, include adjusted scripts" | Prev: "revenue increase for PBM Q3 2025 vs Q3 2024"
→ Prefix: "follow-up" → FOLLOW_UP | "increase" missing metric → inherit "revenue"
→ Current adds: GLP-1 | Inherits: PBM, time

Ex4: Current: "follow-up - compare drug Wegovy decline" | Prev: "revenue increase for GLP-1 for PBM Q3 2025 vs Q3 2024"
→ Prefix: "follow-up" → FOLLOW_UP | Change: "increase" → "decline", missing metric → inherit "revenue"
→ Current: Wegovy replaces GLP-1 | Inherits: PBM, time

Ex5: Current: "follow-up - What lines of business contributed to the decline" | Prev: "revenue decline for Wegovy for PBM Q3 2025 vs Q3 2024"
→ Prefix: "follow-up" → FOLLOW_UP | "decline" missing metric → inherit "revenue"
→ Current adds: attributes (lines of business) | Inherits: Wegovy, PBM, time

Ex6: Current: "follow-up - What Clients in External LOB contributed to the decline" | Prev: "revenue decline for Wegovy for PBM Q3 2025 vs Q3 2024"
→ Prefix: "follow-up" → FOLLOW_UP | "decline" missing metric → inherit "revenue"
→ Current adds: attributes (Clients), filter (External LOB) | Inherits: Wegovy, PBM, time

**Edge Cases:**

Ex7: Current: "validation - revenue was wrong" | Prev: any
→ Prefix: "validation" → VALIDATION

Ex8: Current: "why is that high" (no prefix) | Prev: "revenue for PBM for Q3 2025"
→ No prefix, has pronoun "that" → FOLLOW_UP (inherit all: revenue, PBM, Q3 2025)

Ex9: Current: "actuals for PBM for September" (no prefix) | Prev: "revenue by LOB for Q3 2025"
→ No prefix, auto-detect | Has metric+filters+partial time → FOLLOW_UP (add year)

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
2. What components are in clean_question (metric, filters, attributes, time)
3. **MANDATORY METRIC CHECK**: Does question contain growth/decline term (decline, growth, increase, decrease)? If YES, is there a metric BEFORE it? If NO metric, state which metric will be inherited from previous question.
4. What other components should be inherited from previous (be specific: "Should inherit ['PBM'] and 'July 2025'")
5. Why this decision was made

**Reasoning Example for Metric Inheritance:**
"Question contains 'decline' but no metric before it ('the decline in Wegovy'). Previous question had 'revenue decline'. Will inherit 'revenue' and rewrite as 'revenue decline in Wegovy'."

════════════════════════════════════════════════════════════
SECTION 2: REWRITE QUESTION
════════════════════════════════════════════════════════════

Now use your analysis from Section 1 to rewrite the question.

**CRITICAL: Read your own reasoning - it tells you exactly what to do**

**STEP 0: METRIC INHERITANCE (Apply FIRST - MANDATORY CHECK)**

⚠️ **CRITICAL: ALWAYS check if metric is missing when growth/decline terms present**

**Detection Logic:**
1. Does question contain: decline, growth, increase, decrease, trending, rising, falling?
2. Does question ALREADY have a metric BEFORE the growth/decline term?
   - Check for: revenue, volume, expense, script count, claims, cost, actuals, forecast
   - Examples WITH metric: "revenue decline", "expense growth", "script count increase" ✅
   - Examples WITHOUT metric: "the decline", "decline in Wegovy", "most by the decline" ❌

**If growth/decline term found BUT no metric before it:**
- Extract metric from PREVIOUS QUESTION
- Inject metric IMMEDIATELY BEFORE the growth/decline term
- Format: "[metric] [growth/decline term]"

**Examples:**
- Previous: "revenue for PBM" → User: "show decline" → Rewritten: "show revenue decline"
- Previous: "revenue decline for Wegovy" → User: "decline in Wegovy" → Rewritten: "revenue decline in Wegovy"
- Previous: "expense by LOB" → User: "impacted by the decline" → Rewritten: "impacted by the expense decline"

**STEP 1: MEMORY-BASED DIMENSION DETECTION (Apply After Metric Inheritance)**

**⚠️ CRITICAL: Check conversation memory BEFORE rewriting**

If conversation memory exists:

1. **Extract values mentioned in question** (entity names, drug names, client codes, multi-word phrases)
   - Case-insensitive, fuzzy matching

2. **Check if each value exists in memory dimensions**
   - If found: Identify dimension key
   - If in MULTIPLE dimensions: Use LATEST (last) dimension
   - **CRITICAL: Use the EXACT dimension key name from memory (do NOT convert/simplify)**
     * If memory has `client_id` → Use "client_id" (NOT "client")
     * If memory has `client_name` → Use "client_name" (NOT "client")
     * If memory has `carrier_id` → Use "carrier_id" (NOT "carrier")
     * If memory has `drug_name` → Use "drug_name" (NOT "drug")
   - Add dimension prefix with EXACT key name: "for [exact_dimension_key] [value]"
   - Mark as DIMENSION_TAGGED (exclude from filters)

3. **If NOT found in memory**: Keep as-is (will be extracted as filter)

**EXAMPLES:**

Memory: {{"client_id": ["57760", "57096"]}}
User: "revenue for 57760"
→ Rewritten: "What is revenue for client_id 57760" (use exact key "client_id")

Memory: {{"client_name": ["BCBSM", "HARVARD PILGRIM"]}}
User: "revenue for BCBSM"
→ Rewritten: "What is revenue for client_name BCBSM" (use exact key "client_name")

Memory: {{"drug_name": ["WEGOVY", "OZEMPIC"]}}
User: "revenue for Wegovy and diabetes"
→ Rewritten: "revenue for drug_name WEGOVY and diabetes" (use exact key "drug_name", exact value "WEGOVY")

**STEP 2: Apply Forecast Cycle Rules (if applicable)**

**Available cycles:** 2+10, 5+7, 8+4, 9+3 | **Current cycle:** {current_forecast_cycle}

**Rules:**
1. "forecast" WITHOUT cycle → Add current cycle: "forecast {current_forecast_cycle}"
2. Cycle pattern (8+4,5+7,2+10,9+3,10+2,4+8) WITHOUT "forecast" → Prepend "forecast": "forecast [cycle]"
3. BOTH present → Use as-is

**STEP 2: Build Rewritten Question**

**STEP 3: Build Rewritten Question**

**IF NEW:**
→ Use clean_question components as-is
→ Apply metric inheritance (Step 0) + forecast cycle rules (Step 2) + memory dimension detection (Step 1)
→ Format: "What is [metric] for [filters] for [time]"
→ If time_is_partial, add current year: "for [time] {current_year}"
→ user_message: "" (empty unless forecast cycle added)

**IF FOLLOW_UP:**
→ Start with clean_question components
→ Apply metric inheritance (Step 0) + forecast cycle rules (Step 2) + memory dimension detection (Step 1)
→ For missing components, extract from previous_question
→ If time_is_partial, add {current_year}
→ Create user_message: "I'm using [specific inherited components] from your last question."

**IF VALIDATION:**
→ Format: "[Previous Question] - VALIDATION REQUEST: [clean_question]"
→ user_message: "This is a validation request for the previous answer."

**Question Type:**
- "why", "how come", "explain" → question_type: "why"
- Otherwise → question_type: "what"

════════════════════════════════════════════════════════════
SECTION 3: EXTRACT FILTER VALUES
════════════════════════════════════════════════════════════

**CRITICAL: Extract filter values from REWRITTEN question (after inheritance and memory dimension tagging)**

**⚠️ IMPORTANT: ALWAYS extract the actual value, even if it has a dimension prefix**

**EXTRACTION RULES (Apply in order):**

1. **Dimension Prefixes** → **EXTRACT the VALUE only** ✅
   - "for drug name Wegovy" → EXTRACT "Wegovy" ✅
   - "for client BCBS" → EXTRACT "BCBS" ✅
   - "for therapy class GLP-1" → EXTRACT "GLP-1" ✅
   - "for carrier MDOVA" → EXTRACT "MDOVA" ✅
   - Strip the dimension prefix, keep only the value

2. **Strip Suffixes** → Remove: drug(s), medication(s), class(es), category/categories, type(s), group(s), name(s), therapy/therapies
   - "GLP-1 drug" → Strip "drug" → "GLP-1" ✅
   - "Wegovy medication" → Strip "medication" → "Wegovy" ✅
   - Exception: If ONLY suffix word → EXCLUDE ❌

3. **Pure Numbers** → EXCLUDE ❌
   - "invoice # 12345" → EXCLUDE ❌ (pure numbers)

4. **Exclusion List** → EXCLUDE if matches:
   - Common: PBM, HDP, Home Delivery, Specialty, Mail, Retail, Claim Fee, Activity Fee
   - Dimension names (not values): therapy class, line of business, LOB, carrier, geography, region, channel, drug name, drug, client (only when standalone)
   - Modifiers: unadjusted, normalized, per script, average, total, net, gross
   - Time: months, quarters, years, dates
   - Generic: revenue, cost, expense, data, what, is, for, by, breakdown, volume, count
   - Metrics: billed amount, claims count, script count
   - Keywords: by, breakdown, group, compare, versus

5. **Contains Letters?** → If passed above checks, EXTRACT ✅

**KEY PRINCIPLE: Extract the actual entity VALUE, strip dimension labels and suffixes**

**EXAMPLES:**

Rewritten: "What lines of business contributed to the decline for drug name Wegovy between Q3 2025 vs Q3 2024?"
→ "drug name Wegovy" → Extract "Wegovy" → filter_values: ["Wegovy"]

Rewritten: "What is revenue for diabetes?"
→ "diabetes" → Extract "diabetes" → filter_values: ["diabetes"]

Rewritten: "What is revenue for drug name Wegovy and diabetes?"
→ "drug name Wegovy" → Extract "Wegovy", "diabetes" → Extract "diabetes"
→ filter_values: ["Wegovy", "diabetes"]

Rewritten: "What is revenue for client BCBS for drug name Ozempic?"
→ "client BCBS" → Extract "BCBS", "drug name Ozempic" → Extract "Ozempic"
→ filter_values: ["BCBS", "Ozempic"]

Rewritten: "What is revenue for External LOB and carrier MDOVA for July 2025?"
→ "carrier MDOVA" ,External LOB → Extract "MDOVA","External"
→ filter_values: ["MDOVA","External"]

Rewritten: "What is actuals vs forecast 8+4 for diabetes for Q3 2025"
→ "8+4" → Extract "8+4", "diabetes" → Extract "diabetes"
→ filter_values: ["8+4", "diabetes"]

Rewritten: "What is revenue for GLP-1 drug for July 2025"
→ "GLP-1 drug" → Strip "drug" → Extract "GLP-1"
→ filter_values: ["GLP-1"]

════════════════════════════════════════════════════════════
OUTPUT FORMAT - PURE JSON ONLY
════════════════════════════════════════════════════════════
**CRITICAL REQUIREMENTS:**
1. Return ONLY valid JSON - no markdown, no code blocks, no extra text
2. Do NOT wrap in ```json or ``` 
3. Start directly with {{ and end with }}
4. No explanatory text before or after the JSON

**CORRECT FORMAT:**
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
        "filter_values": ["array", "of", "extracted", "filter", "values", "without", "attribute", "labels"]
    }}
}}

**WRONG FORMAT (DO NOT USE):**
```json
{{ ... }}
```

"""
        return prompt
