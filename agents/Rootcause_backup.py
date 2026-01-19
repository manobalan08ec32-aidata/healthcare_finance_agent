selection_prompt = f"""
You are analyzing a database schema to route SQL queries to the correct table(s).

CORE PRINCIPLES

1. ALL-OR-NOTHING: A table is valid ONLY if it has ALL required metrics + ALL required attributes + ALL required filter columns. Partial matches are INVALID.

2. NO TRADE-OFFS: Never pick "closest match". If no table has all components → status="missing_items".
   WRONG: "Ledger has membership but no Carrier ID, picking anyway"
   CORRECT: "No table has both → missing_items"

3. ONE FOLLOW-UP CHANCE: You have exactly ONE opportunity to ask user for clarification. Use ONLY when:
   - Multiple tables FULLY qualify and need user preference
   - Filter value maps to multiple columns (genuine ambiguity)
   - User's question contains multiple sub-questions needing different tables
   NEVER hallucinate or guess. If genuinely ambiguous → ASK. If no table qualifies → missing_items.

4. COMPLEMENTARY MULTI-TABLE: Select multiple tables when:
   - User asks multiple questions where each needs a different table (e.g., "show revenue from claims and script count from ledger")
   - Tables can be joined using relationships defined in metadata (if join keys available)
   - User explicitly requests data from multiple sources
   This is NOT a fallback for missing components - each table must independently qualify for its portion.

5. METRIC CONTEXT: When users request "revenue per script" or "cost per claim", they ask which TABLE contains the data. SQL generation handles calculations.

INPUTS

QUESTION: {user_question}
EXTRACTED FILTER VALUES: {filter_values}
FILTER METADATA: {filter_metadata_text}
AVAILABLE DATASETS:
{search_results}

STEP 1: CHECK USER EXPLICIT TABLE HINTS

If user explicitly specifies tables, RESPECT their choice even if suboptimal:
- "from table A and table B" / "compare X vs Y" → Select BOTH tables
- "using [table_name]" / "fetch from [table_name]" → Select that table
- "get X from table A, Y from table B" → Select BOTH tables A and B

Decision logic:
- User mentions multiple tables → Return ALL mentioned (status="success", multi_table_mode="user_requested")
- Single table sufficient but user wants multiple → RESPECT user's choice
- User specifies table lacking required columns → status="needs_disambiguation", explain limitation

If no explicit hint → Continue to Step 2.

STEP 2: EXTRACT REQUIRED COMPONENTS

A. METRICS (values to aggregate):
- Direct: revenue, expense, cost, cogs, membership, scripts, volume, margin, billed amount, allowed amount
- Fuzzy:
  * "scripts/prescriptions" → unadjusted_scripts, adjusted_scripts, 30_day_scripts, 90_day_scripts
  * "cost/costs" → expense, cogs, cost per script
  * "margin" → gross margin, Gross Margin %, margin per script
  * "billing/billed" → billed amount, revenue from billing
- Skip calculations (handled downstream): variance, growth, change, percentage, performance

B. ATTRIBUTES (GROUP BY dimensions):
- Triggered by: "by [X]", "per [X]", "for each [X]", "top N [X]"
- Direct: Client Name, Drug Name, Therapy Class, Carrier ID, Client ID, Line of Business
- Fuzzy:
  * "therapy/therapies" → Therapy Class
  * "client/clients" → Client Name, Client ID
  * "carrier" → Carrier ID
  * "drug/medication" → Drug Name
  * "pharmacy/pharmacies" → Pharmacy Name, Pharmacy NPI
  * "lob/line of business" → Line of Business
  * "invoice" → Invoice Number, Invoice Date
  * "member/patient" → Member ID (PHI check required)
- BLOCK substitutions: Product Category ≠ Drug, ingredient_fee ≠ expense

C. FILTER COLUMNS (WHERE clause):
- Triggered by: "for [value]", specific values in question
- Identify COLUMN NAME required, not just value:
  * "for carrier MPDOVA" → filter_column: Carrier ID
  * "for client Acme Corp" → filter_column: Client Name
  * "for drug Humira" → filter_column: Drug Name
  * "for vaccines" (therapy class value) → filter_column: Therapy Class
- CRITICAL: Filter columns are REQUIRED - table MUST have them in "attrs" list
- Skip universal filters (exist in all tables): external, SP, PBM, HDP, optum, mail, specialty, home delivery, PBM retail

STEP 3: CHECK DIRECT TABLE KEYWORD MATCH

Check for explicit table references:
- "ledger" → actuals_vs_forecast table
- "claims" or "claim" → claim_transaction table
- "billing" → billing_extract table

If keyword found AND table passes all validations → status="success"
If keyword found BUT table fails validations → Continue to full evaluation
If no keyword → Continue to Step 4

STEP 4: VALIDATE EACH TABLE (ALL-OR-NOTHING)

For each table, apply checks IN ORDER. Must pass ALL to be valid.

CHECK 1 - PHI/PII Security:
- Question requests columns in "phi_cols"? → status="phi_found", STOP

CHECK 2 - "not_useful_for" Elimination:
- Question matches any item in "not_useful_for"? → ELIMINATE
- Check BOTH breakdown AND filter contexts:
  * Filter "for carrier X" + "carrier analysis" in not_useful_for → ELIMINATE
  * Filter "for drug X" + "drug/therapy analysis" in not_useful_for → ELIMINATE
  * Breakdown "by therapy class" + "drug/therapy analysis" in not_useful_for → ELIMINATE
  * Filter "for vaccines" (therapy value) + "drug/therapy analysis" in not_useful_for → ELIMINATE
- CRITICAL: Any question with drug/therapy/vaccine/medication filters OR breakdowns → Check for "drug/therapy analysis"
- Examples:
  * Q: "top 10 clients by expense" + Ledger not_useful_for:["client expense/margin alone"] → ELIMINATE Ledger
  * Q: "top 10 drugs by revenue" + Ledger not_useful_for:["drug/therapy analysis"] → ELIMINATE Ledger
  * Q: "script count for vaccines" + Ledger not_useful_for:["drug/therapy analysis"] → ELIMINATE Ledger

CHECK 3 - Metrics Validation:
- Table's "metrics" contains ALL required metrics? → If missing any → ELIMINATE

CHECK 4 - Attributes Validation:
- Table's "attrs" contains ALL required attributes?
- DO NOT assume attributes not explicitly listed in "attrs"
- Must have 100% match - no partial matching
- If missing any → ELIMINATE
- Example:
  * Q: "revenue by therapy class, drug name"
  * Required: ["Therapy Class", "Drug Name"]
  * Ledger attrs: ["Ledger", "Mail Service", "Home Delivery", "Specialty", "Line of Business", "Transaction Date", "Year", "Month", "Quarter", "Product Category", "Product Category Level 1", "Product Category Level 2", "Client ID", "Client Name"]
  * Ledger does NOT contain "Therapy Class" or "Drug Name" → ELIMINATE Ledger ❌
  * Claims attrs: ["Claim Number", "Submit Date", "Client ID", "Client Name", "Pharmacy NPI", "Drug Name", "Therapy Class", ...]
  * Claims CONTAINS both → KEEP Claims ✅

CHECK 5 - Filter Columns Validation:
- Table's "attrs" contains ALL required filter columns?
- If missing any → ELIMINATE
- Example:
  * Q: "membership for carrier MPDOVA"
  * Required filter column: Carrier ID
  * Ledger attrs: [..., "Client ID", "Client Name"] → NO "Carrier ID" → ELIMINATE Ledger ❌
  * Claims attrs: [..., "Carrier ID", ...] → KEEP Claims ✅

STEP 5: HANDLE FILTER VALUE DISAMBIGUATION

When filter values exist in metadata:
- Value in SINGLE column → Auto-select that column
- Value in MULTIPLE columns, one COMPLETE match → Auto-select that column
- Value in MULTIPLE columns, multiple matches → status="needs_disambiguation"
- Example: "covid vaccine" in [drug_name, therapy_class_name] → needs_disambiguation

STEP 6: CHECK FOR MULTI-QUESTION OR JOIN SCENARIOS

Before final decision, check if question contains multiple sub-questions:
- Q: "show claims revenue and billing amount by client" → Two metrics from different tables
- Q: "compare claims data with billing data" → Explicit comparison request

If multiple sub-questions detected:
- Validate each sub-question against appropriate table
- Each table must fully qualify for its portion (ALL-OR-NOTHING still applies per table)
- If join keys exist in metadata between tables → Can combine results
- Return status="success" with multi_table_mode="complementary"

If single question → Continue to Step 7

STEP 7: DECIDE BASED ON VALID TABLES

ONE table passed:
→ status="success"

MULTIPLE tables passed (for same question):
→ Apply tie-breakers:
  1. Check "high_level" field: prefer high_level=true for summary queries
  2. Check "useful_for" for best match
  3. Still tied → status="needs_disambiguation", use your ONE follow-up chance
→ Example: Both Claims and Ledger have [revenue, Client Name], Ledger has high_level=true → SELECT Ledger

ZERO tables passed:
→ status="missing_items"
→ Explain what's missing and suggest alternatives
→ NEVER hallucinate or pick "closest match"

EXAMPLES

Example 1: Tie-breaker with high_level
Q: "Revenue by client or line of business"
- Extracted: metrics=[revenue], attributes=[client name, line of business], filter_columns=[]
- Validation: Ledger PASSED, Claims PASSED (both have revenue + client name + lob)
- Decision: Multiple passed → Ledger has high_level=true → SELECT Ledger

Example 2: Drug/therapy eliminates Ledger
Q: "Script count and revenue by drug name and therapy class"
- Extracted: metrics=[scripts, revenue], attributes=[drug name, therapy class], filter_columns=[]
- Validation: Ledger ELIMINATED (not_useful_for "drug/therapy analysis", missing attrs), Claims PASSED
- Decision: SELECT Claims - only table with required attributes

Example 3: Filter triggers not_useful_for
Q: "script count for vaccines"
- Extracted: metrics=[scripts], attributes=[], filter_columns=[Therapy Class]
- Validation: Ledger ELIMINATED (not_useful_for "drug/therapy analysis"), Claims PASSED
- Decision: SELECT Claims - Ledger blocked for therapy filtering

Example 4: Missing filter column → missing_items
Q: "membership count for carrier MPDOVA"
- Extracted: metrics=[membership], attributes=[], filter_columns=[Carrier ID]
- Validation:
  * Ledger: membership ✓, Carrier ID ✗, not_useful_for "carrier analysis" ✗ → ELIMINATED
  * Claims: membership ✗ → ELIMINATED
  * Billing: membership ✗ → ELIMINATED
- Decision: ZERO passed → status="missing_items"
- Message: "'membership' only exists in Ledger, but Ledger lacks Carrier ID. Try: 'membership by client' or 'script count for carrier MPDOVA'."

Example 5: Multi-question complementary
Q: "show claims revenue by client and also billing amount by client"
- Sub-question 1: "claims revenue by client" → Claims table (has revenue, Client Name) ✓
- Sub-question 2: "billing amount by client" → Billing table (has billed amount, Client Description) ✓
- Decision: Both tables qualify for their portions → status="success", multi_table_mode="complementary"

OUTPUT FORMAT

REASONING (3-5 lines):
- Extracted: metrics=[...], attributes=[...], filter_columns=[...]
- Eliminated: [Table: reason], [Table: reason]
- Selected: [Table] - [brief reason]

JSON in <json> tags:

SUCCESS:
{{
  "status": "success",
  "final_actual_tables": ["table_name"],
  "functional_names": ["friendly_name"],
  "selection_reasoning": "Brief explanation",
  "high_level_table_selected": true/false,
  "multi_table_mode": null / "complementary" / "user_requested",
  "selected_filter_context": "column: X, values: [Y]" or null
}}

NEEDS_DISAMBIGUATION:
{{
  "status": "needs_disambiguation",
  "tables_identified_for_clarification": ["table_1", "table_2"],
  "functional_table_name_identified_for_clarification": ["name_1", "name_2"],
  "requires_clarification": true,
  "clarification_question": "Which table would you prefer: [name_1] or [name_2]?"
}}

MISSING_ITEMS:
{{
  "status": "missing_items",
  "user_message": "Explanation of gap and suggested alternatives"
}}

PHI_FOUND:
{{
  "status": "phi_found",
  "user_message": "This query requests protected health information."
}}
"""
