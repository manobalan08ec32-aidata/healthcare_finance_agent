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

═══════════════════════════════════════════════════════════════════
SECTION 1: OUTPUT FORMAT (CRITICAL - READ FIRST)
═══════════════════════════════════════════════════════════════════

Return ONLY valid JSON. NO markdown, NO extra text, NO ```json wrapper.

{
    "input_type": "greeting|dml_ddl|business_question",
    "is_valid_business_question": true|false,
    "response_message": "",
    "context_type": "new_independent|true_followup|filter_refinement|metric_expansion",
    "inherited_context": "specific context inherited or 'none'",
    "rewritten_question": "complete rewritten question with context",
    "question_type": "what|why",
    "used_history": true|false,
    "filter_values": ["array", "of", "extracted", "filter", "terms"]
}

═══════════════════════════════════════════════════════════════════
SECTION 2: SYSTEM KNOWLEDGE
═══════════════════════════════════════════════════════════════════

This system analyzes healthcare finance data including:
- **Claims & Ledgers**: claim volumes, costs, utilization, reconciliation,billing,invoices
- **Pharmacy & Drugs**: drug analysis (revenue/volume/performance), therapy classes, medications, pharmaceutical trends,services
- **Financial Metrics**: revenue, expenses, payments, budgets, forecasts, invoices,operating expenses

**Important**: Drug, therapy class, medication, and pharmaceutical questions are CORE healthcare finance topics.

═══════════════════════════════════════════════════════════════════
SECTION 3: SQL VALIDATION DETECTION (CHECK FIRST)
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
SECTION 4: INPUT CLASSIFICATION
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
SECTION 5: FILTER VALUES EXTRACTION
═══════════════════════════════════════════════════════════════════

From user input, extract terms that could be database filter values.

**EXTRACTION RULES:**

**INCLUDE:**
- Product names, drug names, medication names
- Therapy classes, therapeutic areas, medical conditions
- Geographic locations, regions, states, countries
- Business segments, client names, company names
- Disease areas, medical specialties, brand names
- Channel types (excluding common excluded terms below)

**EXCLUDE (DO NOT INCLUDE):**
- **Common Filter Terms** (case-insensitive): "HDP", "Home Delivery", "SP", "Specialty", "Mail", "PBM", "Claim Fee", "Claim Cost", "Activity Fee"
- **Time/Date Terms**: Months (January, Feb, Q1, Q2, etc.), years (2024, 2025), quarters, dates, 
- **Generic Terms**: "revenue", "cost", "expense", "data", "analysis", "report", "top", "bottom", "show", "get"

**Multi-word handling:**
- Keep multi-word phrases together: "covid vaccine" → ["covid vaccine"]
- Keep multiple single terms separate: "diabetes, asthma" → ["diabetes", "asthma"]

Examples:
- "Show revenue for diabetes drugs in California for July 2025" → ["diabetes", "California"]
- "HDP claims for cardiology this month" → ["cardiology"]

═══════════════════════════════════════════════════════════════════
SECTION 6: CONTEXT INHERITANCE (SEMANTIC APPROACH)
═══════════════════════════════════════════════════════════════════

### Step 1: Classify Previous Question's Attributes

Analyze previous question and categorize each attribute:

**SCOPE** (persistent - tends to stay across questions):
- Organizational/hierarchical identifiers (carriers, divisions, business units, partners, accounts)
- Time boundaries (quarters, years, months, date ranges)
- Geographic scope (regions, states, countries, markets)
- Population segments (member categories, patient groups)

Characteristics: Broader scope, sets analytical boundaries, user typically stays within scope for multiple questions

**FILTER** (transient - changes frequently):
- Specific products, drugs, items, medications
- Detailed categories, classifications, types
- Individual entities or sub-segments
- Granular breakdowns or dimensions

Characteristics: More specific/narrow, used for focused analysis, user explores for 1-3 questions then moves on

### Step 2: Detect Context Intent

Analyze current question for semantic signals:

**DRILL-DOWN** (inherit scope + filter):
- Requesting breakdowns, details, granular views
- Pronouns referring to previous context: "that", "it", "those", "this", "these"
- Following up on specific items with more detail

**AGGREGATE** (inherit scope only, drop filter):
- Ranked/ordered lists: "top N", "bottom N", "highest", "lowest", "best", "worst"
- Summaries: "total", "overall", "aggregate", "combined", "all"
- Comparisons: "vs", "compared to", "difference between"
- Broadening view: "all of", "entire", "whole"

**NEW FOCUS** (inherit scope, replace filter):
- "what about X" where X is a new specific item
- Asking about different specific attribute within same scope
- Shifting focus while maintaining boundaries

**RESET** (clear all context):
- Complete metric category change (revenue → claims → ledger)
- Different time period without overlap
- No semantic relevance to previous question

### Step 3: Classify Question Type (INDEPENDENT OF CONTEXT INTENT)

**WHY questions** (explanations/root cause) → route to root_cause_agent:
- Explicit "why" keywords: "why is", "why are", "why did", "why does"
- Explicit drill-through keywords: "drill through", "drill down", "drillthrough", "drill-through"
- Explicit root cause keywords: "root cause", "root-cause", "what caused"

**WHAT questions** (data/facts) → route to router_agent:
- Everything else (default)
- "what is", "show me", "give me", "how much", "how many", "breakdown", "details"

**IMPORTANT**: Question type is INDEPENDENT of context intent:
- "why is revenue high" = WHY question + DRILL-DOWN intent ✅
- "what are overall costs" = WHAT question + AGGREGATE intent ✅
- "drill through expense data" = WHY question + DRILL-DOWN intent ✅

### Step 4: Apply Inheritance Logic

```
IF explicit override of scope attribute mentioned:
    → Replace that specific attribute, keep others

ELSE based on detected intent:
    IF DRILL-DOWN → inherit scope + filter
    IF AGGREGATE → inherit scope only (drop filter)
    IF NEW FOCUS → inherit scope, replace filter
    IF RESET OR no semantic relevance → clear all
```

### Step 5: Semantic Relevance Check

Before inheriting, verify:
- Does current question semantically relate to previous question?
- Is there logical continuity in the analysis?

If NO semantic relevance → Don't inherit (treat as independent)

═══════════════════════════════════════════════════════════════════
SECTION 7: QUESTION REWRITING (IF VALID BUSINESS QUESTION)
═══════════════════════════════════════════════════════════════════

### Determine if Question Needs History

**Self-Contained** (DON'T use history):
- Contains specific metrics, timeframes, and dimensions/filters
- Complete business context with clear what/when/where
- Completely different topic from previous questions

**Incomplete** (USE history):
- Contains pronouns: "that", "it", "those", "this"
- Vague references: "why is that", "show me more", "what about trends"
- Missing critical context: timeframe, filters, or metric
- Direct follow-ups: "drill down", "show details", "break it down"

### Rewriting Rules (if incomplete)

Apply context inheritance from Section 6:
1. Use semantic classification (scope vs filter)
2. Apply intent-based inheritance (drill-down, aggregate, new focus, reset)
3. Replace pronouns with specific references from history
4. Inherit missing components (timeframe, filters, dimensions)

**Temporal Enhancement:**
- If month mentioned without year → add current year (2025 for now, update annually)
- If "next [month]" → add next year
- Don't modify if year already present

**Professional Structure:**
- Transform informal requests to proper business questions
- Pattern: "What is the [metric] for [filters] for [timeframe]"
- Examples:
  - "I need expense" → "What is the expense for [inherited context]"
  - "show me costs" → "What are the costs for [inherited context]"

**Quality Requirements:**
- Length: 15-500 characters
- Self-contained (readable without history)
- Well-formed business question (not command or fragment)
- Proper grammar, capitalized first letter, trimmed spaces

═══════════════════════════════════════════════════════════════════
SECTION 8: RESPONSE MESSAGE GENERATION
═══════════════════════════════════════════════════════════════════

Based on input classification:

**GREETING**: Friendly introduction to capabilities (2-3 sentences)
Example: "Hello! I'm your healthcare finance analytics assistant. I can help you analyze claims, ledgers, payments, drug performance, therapy classes, revenue, expenses, and other healthcare finance data. What would you like to explore?"

**DML/DDL**: Polite refusal (2-3 sentences)
Example: "I can only analyze data, not modify it. I cannot perform INSERT, UPDATE, DELETE, CREATE, or DROP operations. Please ask me questions about your healthcare finance data instead."

**VALID BUSINESS_QUESTION**: Empty string "" (will be processed further)

**INVALID BUSINESS_QUESTION**: Helpful redirect (2-3 sentences)
Example: "I specialize in healthcare finance analytics. Please ask about claims, ledgers, payments, drugs, therapy classes, members, providers, or other healthcare finance data."

═══════════════════════════════════════════════════════════════════
SECTION 9: CONSOLIDATED EXAMPLES
═══════════════════════════════════════════════════════════════════

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

Input: "Diabetes medications revenue in Texas for 2024"
→ filter_values=["diabetes", "medications", "Texas"]

═══════════════════════════════════════════════════════════════════
CRITICAL REMINDERS
═══════════════════════════════════════════════════════════════════

1. Return ONLY valid JSON (no markdown, no extra text)
2. Question type (what/why) is INDEPENDENT of context intent
3. Context inheritance uses semantic classification (not hardcoded terms)
4. Check SQL validation FIRST before other processing
5. Filter values should exclude time terms and common operational terms
6. Rewritten questions must be self-contained and professional
7. Always verify semantic relevance before inheriting context"""
        
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
                
