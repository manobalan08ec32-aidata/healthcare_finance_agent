"""
Navigation Controller Prompts

Contains all hardcoded prompts used by the LLMNavigationController.
These are imported and formatted with dynamic values in the agent file.

STRUCTURE:
- CLASSIFICATION prompts: Lightweight prefix detection and input classification
- REWRITE prompts: Full question rewriting with context inheritance
- Reusable rule blocks: Shared rules between prompts
"""

# ═
# SYSTEM PROMPTS
# ═

# Original system prompt (kept for backward compatibility)
NAVIGATION_SYSTEM_PROMPT = "You are a healthcare finance analytics agent that analyzes questions, rewrites them with proper context, and extracts filter values."

# Classification system prompt (lightweight)
NAVIGATION_CLASSIFICATION_SYSTEM_PROMPT = """You are a healthcare finance question classifier. Your role is to quickly classify user input and detect special prefixes."""

# Rewrite system prompt
NAVIGATION_REWRITE_SYSTEM_PROMPT = """You are a healthcare finance question rewriter that applies context inheritance and filter extraction rules."""


# ═
# REUSABLE RULE BLOCKS
# These are extracted from the original combined prompt for reuse
# ═

SMART_YEAR_RULES = """
**WHEN USER MENTIONS ONLY MONTH/QUARTER WITHOUT YEAR:**

**Rule:** Compare the mentioned month with current month ({current_month_name}):
- If mentioned month is BEFORE or SAME as current month → Use CURRENT year ({current_year})
- If mentioned month is AFTER current month → Use PREVIOUS year ({previous_year})

**Month Ordering:** January(1) < February(2) < March(3) < April(4) < May(5) < June(6) < July(7) < August(8) < September(9) < October(10) < November(11) < December(12)

**Examples (assuming current date is {current_month_name} {current_year}):**
- User asks "December" → December (month 12) is {december_comparison} {current_month_name} (month {current_month}) → Use {december_year} ✓
- User asks "January" → January (month 1) is {january_comparison} {current_month_name} (month {current_month}) → Use {january_year} ✓
- User asks "{current_month_name}" → Same as {current_month} → Use {current_year} ✓

**Quarter Handling:**
- Q1 (Jan-Mar): Use {q1_year}
- Q2 (Apr-Jun): Use {q2_year}
- Q3 (Jul-Sep): Use {q3_year}
- Q4 (Oct-Dec): Use {q4_year}

**CRITICAL:** When user says just "December" in {current_month_name}, they mean the MOST RECENT December, which is December {most_recent_december_year}
"""

FILTER_INHERITANCE_RULES = """
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
"""

COMPONENT_EXTRACTION_RULES = """
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
"""

METRIC_INHERITANCE_RULES = """
**RULE 6: METRIC INHERITANCE (CRITICAL)**

IF question contains growth/decline terms WITHOUT preceding metric:
- Terms: decline, growth, increase, decrease, trending, rising, falling
- Check: Is there a metric BEFORE the term?
  * "revenue decline" → HAS metric ✓
  * "the decline" → NO metric ✗
- Action: Inherit metric from previous question, place BEFORE the term
"""

FORECAST_CYCLE_RULES = """
**RULE 7: FORECAST CYCLE RULES**

Apply to rewritten question:
- "forecast" alone → Add: "forecast {current_forecast_cycle}"
- "actuals vs forecast" → Add: "actuals vs forecast {current_forecast_cycle}"
- Cycle alone (8+4) → Add: "forecast 8+4"
- Both present → Keep as-is
"""

REWRITTEN_QUESTION_RULES = """
**RULE 8: BUILD REWRITTEN QUESTION**

Based on context_decision:

NEW:
- Use current components + apply Rules 6-8
- **Add year to partial dates using SMART YEAR ASSIGNMENT rules above**
- If month/quarter has no year → Compare with current month and assign correct year

FOLLOW_UP:
- Start with current components
- Check domain inheritance: If current NO domain but previous HAS domain → Insert "for [DOMAIN]"
- Check entity inheritance: Apply entity 4-case rules (do NOT auto-inherit entities)
- Inherit missing components from previous:
  * Metrics: Current missing → inherit from previous
  * Attributes: Current missing → inherit from previous
  * **Time: Current missing → ALWAYS inherit from previous** (Q3 2025, July 2025, etc.)
- Apply Rules 6-7
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
"""

FILTER_EXTRACTION_RULES = """
**RULE 9: FILTER VALUE EXTRACTION**

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
"""


# ═
# PROMPT 1: CLASSIFICATION (Lightweight - ~150 tokens output)
# ═

NAVIGATION_CLASSIFICATION_PROMPT = """
Classify the user input and detect any special prefixes.

CURRENT INPUT: "{current_question}"
PREVIOUS QUESTION: "{previous_question}"


STEP 1: PREFIX DETECTION

Check for and strip these prefixes (case-insensitive):
- "new question", "NEW:" → detected_prefix="new_question"
- "follow-up", "followup", "FOLLOW-UP:" → detected_prefix="followup"
- "wrong", "incorrect", "fix", "correct this", "that's not right", "that's wrong", "the answer is wrong" → detected_prefix="reflection"
- None found → detected_prefix="none"


STEP 2: INPUT CLASSIFICATION

Classify the clean_question (after prefix stripped) into:

A. GREETING: Hi, hello, what can you do, help, hey
   → input_type="greeting", response_message="I help with healthcare finance analytics. Ask me about revenue, claims, costs, or other financial metrics."

B. DML_DDL: INSERT, UPDATE, DELETE, CREATE, DROP, ALTER
   → input_type="dml_ddl", response_message="Data modification operations are not supported. Please ask analytical questions instead."

C. BUSINESS_QUESTION: Contains ANY healthcare/finance term:
   - Metrics: revenue, claims, expenses, cost, scripts, forecast, actuals, billed amount, margin
   - Entities: drugs, therapies, carriers, clients, pharmacies, GLP-1, Wegovy, Ozempic
   - Analysis: increase, decrease, variance, trend, breakdown, by, for, compare
   → input_type="business_question", is_valid_business_question=true

D. INVALID: Everything else
   → input_type="invalid", is_valid_business_question=false, response_message="I specialize in healthcare finance analytics. Please ask about claims, revenue, costs, or other financial metrics."


STEP 3: CONTEXT DECISION (Business Questions Only)

Priority order (STOP at first match):
1. IF detected_prefix="new_question" → NEW
2. IF detected_prefix="reflection" → REFLECTION
3. IF detected_prefix="followup" → FOLLOW_UP
4. IF no previous_question or previous_question="None" → NEW
5. IF has pronouns referring to previous (that, it, this, those, the) AND previous_question exists → FOLLOW_UP
6. IF question seems incomplete without context AND previous_question exists → FOLLOW_UP
7. ELSE → NEW


OUTPUT (JSON only, no markdown, no code blocks):


{{
  "detected_prefix": "new_question|followup|reflection|none",
  "clean_question": "question with prefix stripped",
  "input_type": "greeting|dml_ddl|business_question|invalid",
  "is_valid_business_question": true|false,
  "context_decision": "NEW|FOLLOW_UP|REFLECTION",
  "response_message": "only for non-business questions"
}}
"""


# ═
# PROMPT 2: REWRITE (Full question rewriting - ~500 tokens output)
# Only called for valid business questions with context_decision = NEW or FOLLOW_UP
# ═

NAVIGATION_REWRITE_PROMPT = """
Rewrite the business question with proper context and extract filters.


INPUTS

Current Question: "{current_question}"
Previous Question: "{previous_question}"
Context Decision: "{context_decision}"
History: {history_context}
Current Date: {current_month_name} {current_year} (Month {current_month} of {current_year})
Current Year: {current_year}
Previous Year: {previous_year}
Forecast Cycle: {current_forecast_cycle}
Memory: {memory_context}


SMART YEAR ASSIGNMENT FOR PARTIAL DATES:

{smart_year_rules}

FILTER INHERITANCE & SUBSTITUTION RULES:

{filter_inheritance_rules}

COMPONENT EXTRACTION & INHERITANCE:

{component_extraction_rules}

METRIC INHERITANCE:

{metric_inheritance_rules}

FORECAST CYCLE RULES:

{forecast_cycle_rules}

BUILD REWRITTEN QUESTION:

{rewritten_question_rules}

FILTER VALUE EXTRACTION:

{filter_extraction_rules}

OUTPUT (JSON only, no markdown, no code blocks):


{{
  "rewritten_question": "complete question with all context applied",
  "question_type": "what|why",
  "user_message": "what was inherited/added from previous (empty string if nothing)",
  "filter_values": ["ONLY", "ACTUAL", "VALUES", "NO", "LABELS"]
}}

QUICK REFERENCE EXAMPLES:

NEW: "new question - What is PBM revenue Q3 2025?"
→ Strip prefix, components complete → "What is revenue for PBM for Q3 2025"
→ filter_values: ["PBM"]

FOLLOW_UP + SUBSTITUTION: "what about HDP" (Prev: "PBM revenue Q3")
→ Intent: domain switch, inherit metric/time → "What is revenue for HDP for Q3 2025"
→ filter_values: ["HDP"] (replaced PBM)

DRILL-DOWN: "specifically Wegovy" (Prev: "GLP-1 revenue")
→ Intent: drill-down, inherit metric → "What is revenue for Wegovy"
→ filter_values: ["Wegovy"] (replaced GLP-1 with more specific)

METRIC INHERITANCE: "show the decline" (Prev: "revenue for PBM")
→ No metric before "decline" → "show the revenue decline for PBM"
→ filter_values: ["PBM"]
"""

