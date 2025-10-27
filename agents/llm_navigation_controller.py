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
        history_context = questions_history[-2:] if questions_history else []
        
        # SINGLE COMPREHENSIVE PROMPT - Simplified without team selection logic
        comprehensive_prompt = f"""You are a healthcare finance analytics assistant.

════════════════════════════════════
NOW ANALYZE THIS USER INPUT:
════════════════════════════════════
User Input: "{current_question}"

Previous Questions History: {history_context}

════════════════════════════════════
SECTION 1: SYSTEM KNOWLEDGE
════════════════════════════════════

This system analyzes healthcare finance data including:
- **Claims & Ledgers**: claim volumes, costs, utilization, reconciliation,billing,invoices
- **Pharmacy & Drugs**: drug analysis (revenue/volume/performance), therapy classes, medications, pharmaceutical trends,services
- **Financial Metrics**: revenue, expenses, payments, budgets, forecasts, invoices,operating expenses

**Important**: Drug, therapy class, medication, and pharmaceutical questions are CORE healthcare finance topics.

═══════════════════════════════════════════════════════════════════
SECTION 2: SQL VALIDATION DETECTION (CHECK FIRST)
═══════════════════════════════════════════════════════════════════

**Before processing, check if this is a SQL validation request:**

Validation keywords: "validate", "check", "verify", "correct", "fix", "wrong", "incorrect result", "not getting correct", "query is wrong", "SQL is wrong", "result doesn't look right"

**If validation keywords detected:**
1. Use LAST question from history as base question
2. Append validation context to indicate correction request
3. Format: "[Last question] - VALIDATION REQUEST: [current input]"

Example:
- Previous: "What is revenue for Specialty for July 2025"
- Current: "validate the last query, not getting correct result"
- Rewritten: "What is revenue for Specialty for July 2025 - VALIDATION REQUEST: validate the last query, not getting correct result"

═══════════════════════════════════════════════════════════════════
SECTION 3: INPUT CLASSIFICATION
═══════════════════════════════════════════════════════════════════

Classify user input into ONE category:

**GREETING** - Greetings, capability questions, general chat, dataset availability questions
Examples: "Hi", "Hello", "What can you do?", "Help me", "What data do you have about claims?", "What information is available?"

**DML/DDL** - Data modification requests (NOT supported)
Examples: "INSERT data", "UPDATE table", "DELETE records", "CREATE table", "DROP column"

**BUSINESS_QUESTION** - Healthcare finance queries
Must be VALIDATED for healthcare relevance:

✅ **VALID**: Claims, ledgers, payments, members, providers, pharmacy, drugs, medications, therapy classes, pharmaceuticals, revenue, expenses, invoices, medical costs, utilization, financial metrics

❌ **INVALID**: Weather, sports, retail, manufacturing, general technology, entertainment, non-healthcare topics

═══════════════════════════════════════════════════════════════════
SECTION 4: CONTEXT INHERITANCE (FILL WHAT'S MISSING)
═══════════════════════════════════════════════════════════════════

### Step 1: Extract Components from Questions

**From CURRENT question, identify what it explicitly mentions:**
- **Metric**: revenue, expense, claims, billed amount, cost, volume, payments, utilization
- **Carrier/Entity**: carrier names (carrier XYZ, carrier ABC), clients, partners, accounts
- **Time Period**: quarters (Q1, Q2, Q3, Q4), months (July, August), years (2024, 2025)
- **Geography**: regions, states, countries (North region, California, Texas)
- **Line of Business**: Commercial, Medicare, Medicaid, segments
- **Filters**: drug names (covid vaccine, Humira), therapy classes, categories, specific items
- **Aggregate Signals**: "as a whole", "overall", "total", "top N", "all carriers", "all customers"

**From HISTORY question (last rewritten question), identify:**
- Same components as above

### Step 2: Determine Question Type

**NEW QUESTION (no inheritance):**
- Completely different topic with no semantic overlap (weather, sports, non-healthcare)
- Current has ALL required components = COMPLETE question:
  - Has metric + scope (carrier/geography/LOB) + time = complete
  - Example: "What is claims data for carrier ABC for Q2 2025" (has everything)
- No relationship to history

**FOLLOW-UP QUESTION (fill missing components):**
- Current is missing one or more components (metric, carrier, time, etc.)
- Semantically related to history (same domain, continuation)
- Implicit continuation of previous analysis

### Step 3: Fill Missing Components

**IF NEW QUESTION:**
→ Use current question as-is, no inheritance, no filling

**IF FOLLOW-UP QUESTION:**
→ Fill missing components from history using these rules:

**Filling Rules:**
1. **Current Always Wins**: If current mentions a component, use current's value (don't override with history)
   - Example: Current says "carrier ABC", don't use history's "carrier XYZ"

2. **Fill Only What's Missing**: Look at what current lacks and fill from history
   - Missing metric → fill from history
   - Missing carrier → fill from history
   - Missing time → fill from history
   - Missing filter → fill from history

3. **Component Replacement**: If current mentions same TYPE of component but different value, REPLACE it
   - History: "carrier XYZ", Current: "carrier ABC" → Use ABC (replace)
   - History: "Q1", Current: "Q2" → Use Q2 (replace)

4. **Pronouns Fill Everything**: If current has pronouns ("that", "it", "those", "this"), fill ALL components from history
   - "why is that high" → inherit everything (metric + carrier + time + filter)

5. **"X breakdown" Pattern - Metric vs Filter Detection:**
 
 If current has pattern "[X] breakdown" or "breakdown of [X]":
 
 **Step A: Check if X is a METRIC**
 
 Recognized Metrics (these ARE metrics):
 - revenue, expense, claims, billed amount, cost, volume, payments, utilization, claims count, spend, amount, fee, payment amount, total cost
 
 **Step B: Apply Logic**
 
 IF X is a recognized METRIC:
 → Use X as the metric (replace previous metric)
 → Fill missing carrier/time/geography from history
 
 IF X is NOT a recognized METRIC (drug name, therapy class, category, entity):
 → KEEP previous metric from history
 → ADD X as a new filter
 → Fill missing carrier/time/geography from history

### Step 4: Handle Edge Cases

**Edge Case 1: Strong Aggregate Signals Override Scope**

If current has strong aggregate signals:
- "overall", "all carriers", "all customers", "across the board", "unfiltered", "complete", "entire", "grand total"

**Action:**
→ Fill ONLY metric + time (if missing)
→ DROP all scope filters (carriers, geography, LOB)
→ DROP specific item filters (drugs, categories)

**Why:** These signals mean user wants the BIG PICTURE without filters

**Example:**
History: "What is expense for carrier XYZ for Q3"
Current: "as a whole"
→ Strong aggregate signal detected
→ Fill: metric (expense), time (Q3)
→ Drop: carrier (XYZ)
→ Result: "What is the expense as a whole for Q3"

**Edge Case 2: Complete Questions Don't Fill**

If current has ALL THREE core components:
- Has metric (revenue, expense, claims, etc.)
- Has scope (carrier/geography/LOB)
- Has time (Q1, July, 2025)

**Action:**
→ Treat as NEW QUESTION
→ NO filling from history

**Why:** If user provides complete context, they're starting fresh

**Example:**
History: "What is expense for carrier XYZ"
Current: "claims data for carrier ABC for Q2 2025"
→ Complete question detected (has metric + carrier + time)
→ No filling needed
→ Result: "What is claims data for carrier ABC for Q2 2025"



### Step 5: Construct Rewritten Question

Combine current components + filled components → complete rewritten question

**Professional Structure:**
- Pattern: "What is the [metric] for [filters] for [scope] for [timeframe]"
- Transform informal to formal: "I need expense" → "What is the expense..."
- Proper grammar, capitalized first letter, trimmed spaces

═══════════════════════════════════════════════════════════════════
SECTION 5: QUESTION REWRITING (APPLY CONTEXT INHERITANCE)
═══════════════════════════════════════════════════════════════════

### Determine if Question Needs History

**Self-Contained** (DON'T use history - treat as NEW):
- Contains specific metric + scope + timeframe = COMPLETE
- Completely different topic from previous questions
- No semantic relationship to history

**Incomplete** (USE history - FOLLOW-UP):
- Contains pronouns: "that", "it", "those", "this"
- Vague references: "why is that", "show me more", "what about"
- Missing critical components: metric, carrier, time, or filter
- Direct follow-ups: "for carrier ABC", "for Q2", "as a whole"

### Rewriting Process

1. **Extract components** from current and history (Section 5, Step 1)
2. **Determine question type** - NEW or FOLLOW-UP (Section 5, Step 2)
3. **Apply filling logic** if FOLLOW-UP (Section 5, Step 3)
4. **Check edge cases** (Section 5, Step 4)
5. **Build rewritten question** (Section 5, Step 5)

**Temporal Enhancement:**
- If month mentioned without year → add current year (2025 for now, update annually)
- If "next [month]" → add next year
- If "last [month]" → determine based on context
- Don't modify if year already present

**Quality Requirements:**
- Length: 15-500 characters
- Self-contained (readable without history)
- Well-formed business question (not command or fragment)
- Proper grammar, capitalized first letter, trimmed spaces

### Examples

**Example 1 - Fill Missing Components with Replacement:**
History: "What is expense for carrier XYZ for Q3"
Current: "for carrier ABC for Q4"
→ Type: FOLLOW-UP (missing metric)
→ Current has: carrier=ABC (replaces XYZ), time=Q4 (replaces Q3)
→ Missing: metric
→ Fill: metric=expense
→ Result: "What is expense for carrier ABC for Q4"

**Example 2 - Strong Aggregate Signal (Edge Case 1):**
History: "What is expense for carrier XYZ for Q3"
Current: "as a whole"
→ Type: FOLLOW-UP with strong aggregate signal
→ Current has: aggregate signal "as a whole"
→ Missing: metric, time
→ Fill: metric=expense, time=Q3
→ Drop: carrier=XYZ (aggregate signal overrides scope)
→ Result: "What is expense as a whole for Q3"

**Example 3 - Complete Question (Edge Case 2):**
History: "What is expense for carrier XYZ"
Current: "claims data for carrier ABC for Q2 2025"
→ Type: NEW QUESTION (has metric + carrier + time = complete)
→ No filling needed
→ Result: "What is claims data for carrier ABC for Q2 2025"

**Example 4 - Pronoun (Fill Everything):**
History: "What is billed amount for covid vaccines for carrier MDOVA"
Current: "why is that high"
→ Type: FOLLOW-UP with pronoun
→ Current has: pronoun "that"
→ Fill: everything (metric + filter + carrier)
→ Result: "Why is billed amount for covid vaccines for carrier MDOVA high"

═══════════════════════════════════════════════════════════════════
SECTION 6: FILTER VALUES EXTRACTION (FROM REWRITTEN QUESTION)
═══════════════════════════════════════════════════════════════════

**IMPORTANT:** Extract filter values from the REWRITTEN question (after applying context inheritance), not from the current user input.

**GENERIC EXTRACTION LOGIC:**

For every word/phrase in the rewritten question, apply these checks in order:

**Step 1: Does it have an attribute label?**
- Attribute labels: carrier, invoice #, invoice number, claim #, claim number, member ID, member number, patient ID, provider ID, client, account, transaction ID, reference number, contract number, policy number
- If YES → EXCLUDE (LLM will handle it in SQL)
- If NO → Go to Step 2

**Step 2: Is it a pure number (digits only)?**
- Check if value contains ONLY digits: 0-9
- If YES → EXCLUDE
- If NO → Go to Step 3

**Step 3: Is it in the exclusion list?**
- Check against exclusion categories below
- If YES → EXCLUDE
- If NO → Go to Step 4

**Step 4: Does it contain letters (string/alphanumeric)?**
- Check if value contains at least one letter (a-z, A-Z)
- If YES → **EXTRACT IT** ✅
- If NO → EXCLUDE

**EXCLUSION CATEGORIES (Step 3):**
- **Common terms**:"PBM", "HDP", "Home Delivery", "SP", "Specialty", "Mail", "Claim Fee", "Claim Cost", "Activity Fee"
- **Time/Date**: months (January, February, July, etc.), quarters (Q1, Q2, Q3, Q4), years, dates, temporal words (last, next, monthly, quarterly, yearly)
- **Generic words**: revenue, cost, expense, data, analysis, report, top, bottom, show, get, total, overall, what, is, the, for, in, of, and, or
- **Metrics**: revenue, billed amount, claims count, expense, volume, cost, amount, fee, payment

**Multi-word handling:**
- Keep phrases together: "covid vaccine" → ["covid vaccine"]
- Multiple terms: Separate → "diabetes, asthma" as ["diabetes", "asthma"]

**EDGE CASES:**

**Multiple values with same attribute:**
"carrier MPDOVA and BCBS" → both have attribute "carrier" → exclude both

**Mixed standalone and attributed:**
"diabetes for member ID 12345" → "diabetes" passes all checks ✅ | "12345" has attribute + pure number ❌

**EXAMPLES:**

Example 1:
Rewritten: "What is the revenue amount for MPDOVA for September 2025"
→ "MPDOVA": no attribute ✓, not pure number ✓, not in exclusion list ✓, has letters ✓ → **EXTRACT** ✅
→ "September 2025": time/date term → EXCLUDE
→ filter_values: ["MPDOVA"]

Example 2:
Rewritten: "What is invoice #76273623 for diabetes drugs in California for carrier BCBS"
→ "76273623": has attribute "invoice #" → EXCLUDE
→ "diabetes": no attribute ✓, not pure number ✓, not in exclusion list ✓, has letters ✓ → **EXTRACT** ✅
→ "California": no attribute ✓, not pure number ✓, not in exclusion list ✓, has letters ✓ → **EXTRACT** ✅
→ "BCBS": has attribute "carrier" → EXCLUDE
→ filter_values: ["diabetes", "California"]

Example 3:
Rewritten: "What is claim number 982967283632 for cardiology for HDP for July 2025"
→ "982967283632": has attribute "claim number" → EXCLUDE
→ "cardiology": no attribute ✓, not pure number ✓, not in exclusion list ✓, has letters ✓ → **EXTRACT** ✅
→ "HDP": common term → EXCLUDE
→ "July 2025": time/date term → EXCLUDE
→ filter_values: ["cardiology"]

═══════════════════════════════════════════════════
SECTION 7: RESPONSE MESSAGE GENERATION
═══════════════════════════════════════════════════

Based on input classification:

**GREETING**: Friendly introduction to capabilities (2-3 sentences)
Example: "Hello! I'm your healthcare finance analytics assistant. I can help you analyze claims, ledgers, payments, drug performance, therapy classes, revenue, expenses, and other healthcare finance data. What would you like to explore?"

**DML/DDL**: Polite refusal (2-3 sentences)
Example: "I can only analyze data, not modify it. I cannot perform INSERT, UPDATE, DELETE, CREATE, or DROP operations. Please ask me questions about your healthcare finance data instead."

**VALID BUSINESS_QUESTION**: Empty string "" (will be processed further)

**INVALID BUSINESS_QUESTION**: Helpful redirect (2-3 sentences)
Example: "I specialize in healthcare finance analytics. Please ask about claims, ledgers, payments, drugs, therapy classes, members, providers, or other healthcare finance data."

═══════════════════════════════════════════════
SECTION 8: CONSOLIDATED EXAMPLES
═══════════════════════════════════════════════

**GREETING EXAMPLE:**

Input: "Hi, what can you do?"
→ input_type="greeting", valid=false, response="Hello! I'm your healthcare finance analytics assistant..."

**DML/DDL EXAMPLE:**

Input: "INSERT new data into table"
→ input_type="dml_ddl", valid=false, response="I can only analyze data, not modify it..."

**BUSINESS QUESTION EXAMPLE:**

Input: "Show me revenue for last 6 months"
→ input_type="business_question", valid=true, response="", rewritten="What is the revenue for last 6 months", question_type="what", filter_values=[]

**CONTEXT INHERITANCE EXAMPLES:**

Example 1 - Drill-Down:
Previous: "What is revenue for carrier MDOVA for Q3"
Current: "covid vaccine breakdown"
→ Intent: DRILL-DOWN (specific breakdown)
→ Type: WHAT
→ Result: "What is covid vaccine breakdown for carrier MDOVA for Q3"

Example 2 - Aggregate:
Previous: "What is covid vaccine data for carrier MDOVA for Q3"
Current: "show me top 10 drugs"
→ Intent: AGGREGATE (drop covid vaccine filter)
→ Type: WHAT
→ Result: "What are the top 10 drugs for carrier MDOVA for Q3"

Example 3 - New Focus:
Previous: "What is drug A for carrier MDOVA"
Current: "what about drug B"
→ Intent: NEW FOCUS (replace drug A with drug B)
→ Type: WHAT
→ Result: "What is drug B for carrier MDOVA"

Example 4 - Why Question with Context:
Previous: "What is revenue for carrier MDOVA for Q3"
Current: "why is that high"
→ Intent: DRILL-DOWN (pronoun "that")
→ Type: WHY (explicit "why")
→ Result: "Why is the revenue for carrier MDOVA for Q3 high"

**SQL VALIDATION EXAMPLES:**

Previous: "What is revenue for Specialty for July 2025"
Current: "validate the last query, not getting correct result"
→ rewritten="What is revenue for Specialty for July 2025 - VALIDATION REQUEST: validate the last query, not getting correct result"

Previous: "Show expense and gross margin for HDP for July 2025"
Current: "the query is wrong"
→ rewritten="Show expense and gross margin for HDP for July 2025 - VALIDATION REQUEST: the query is wrong"

**FILTER EXTRACTION EXAMPLE:**

Input: "for covid vaccines"
Previous: "What is revenue for carrier MDOVA for Q3"
Rewritten: "What is the revenue for covid vaccines for carrier MDOVA for Q3"
→ filter_values: ["covid vaccines", "MDOVA"]

═══════════════════════════════════════════════════════════════════
SECTION 1: OUTPUT FORMAT (Strictly JSON Format and dont stream reasoning)
═══════════════════════════════════════════════════════════════════

RESPONSE FORMAT MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```
CRITICAL - Do not show any reasoning and only provide the JSON response.

{{
    "input_type": "greeting|dml_ddl|business_question",
    "is_valid_business_question": true|false,
    "response_message": "",
    "context_type": "new_independent|true_followup|filter_refinement|metric_expansion",
    "inherited_context": "specific context inherited or 'none'",
    "rewritten_question": "complete rewritten question with context",
    "question_type": "what|why",
    "used_history": true|false,
    "filter_values": ["array", "of", "extracted", "filter", "terms"]
}}

"""
        
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
                
