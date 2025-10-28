def _build_prompt_2_rewriter(self, current_question: str, previous_question: str, 
                             decision_json: dict) -> str:
    """
    PROMPT 2: Rewriter
    Job: Execute rewriting based on PROMPT 1's decision
    """
    
    from datetime import datetime
    current_year = datetime.now().year
    
    prompt = f"""You are a question rewriter.

════════════════════════════════════════════════════════════
INPUT
════════════════════════════════════════════════════════════
Current: "{current_question}"
Previous: "{previous_question if previous_question else 'None'}"

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

Examples:
- Previous: "revenue by line of business for PBM for Q3"
           ↑ has attribute "by line of business"
- Current: "actuals for PBM for September"
           ↑ has metric + filters + time (complete!)
- Result: "actuals for PBM for September"
          ↑ NO "by line of business" inherited
- Reason: Current is complete with filters, don't add attributes

BUT if current ONLY has metric + time (no filters):
- Current: "actuals for September"
           ↑ has metric + time, but MISSING filters
- Then inherit filters (and potentially attributes if relevant)

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

Example 2: Missing Filters (Inherit)
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

Example 4: Missing Filters and Has Attributes (Different Case)
Decision: FOLLOW-UP
Current: "show me by line of business for September"
         ↑ has attributes but MISSING filters
Previous: "revenue for PBM for Q3"
→ Current has attributes but missing filters
→ Inherit filters, keep attributes
→ Rewritten: "What is revenue for PBM by line of business for September {current_year}"
→ user_message: "I'm using PBM from your last question. Added {current_year} as the year."

Example 5: Pronoun (Inherit All)
Decision: FOLLOW-UP, has_pronouns: true
Current: "why is that high"
Previous: "revenue for carrier MDOVA for Q3"
→ Rewritten: "Why is the revenue for carrier MDOVA for Q3 high"
→ user_message: "I'm using revenue, carrier MDOVA, Q3 from your last question."

════════════════════════════════════════════════════════════
OUTPUT FORMAT (JSON ONLY)
════════════════════════════════════════════════════════════

Return valid JSON without markdown:

{{
    "rewritten_question": "complete rewritten question",
    "question_type": "what|why",
    "filter_values": ["extracted", "values"],
    "user_message": "what was inherited/added - empty if nothing"
}}
"""
    return prompt


    def _build_prompt_1_decision_maker(self, current_question: str, previous_question: str, 
                                   history_context: List) -> str:
    """
    PROMPT 1: Decision Maker
    Job: Classify input + Extract components + Decide NEW vs FOLLOW-UP
    """
    
    prompt = f"""You are a healthcare finance analytics assistant.

════════════════════════════════════════════════════════════
CURRENT INPUT
════════════════════════════════════════════════════════════
Current: "{current_question}"
Previous: "{previous_question if previous_question else 'None'}"

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
   IF current has ALL of: metric + (filters OR attributes) + time with year → NEW
   
   Examples of COMPLETE:
   - "revenue for PBM for Q3 2025" → has metric + filters + time → NEW
   - "actuals for PBM for September 2025" → has metric + filters + time → NEW
   - "claims by carrier for Q3 2025" → has metric + attributes + time → NEW
   
   Why NEW? User provided all necessary components, this is a standalone question.

5. **Incomplete question check:**
   IF current is MISSING any of: metric, filters/attributes, or time → FOLLOW-UP
   
   Examples of INCOMPLETE:
   - "compare actuals for September" → has metric + time, but MISSING filters → FOLLOW-UP
   - "for covid vaccines" → MISSING metric, MISSING time → FOLLOW-UP
   - "show me by line of business" → MISSING metric, MISSING time → FOLLOW-UP
   
   Why FOLLOW-UP? Missing components need to be inherited from previous.

6. **Otherwise** → NEW

**Key principle:** 
- Complete questions (metric + filters/attributes + time) = NEW
- Incomplete questions (missing any component) = FOLLOW-UP
- Pronouns always = FOLLOW-UP

════════════════════════════════════════════════════════════
EXAMPLES
════════════════════════════════════════════════════════════

Example 1: Complete New Question (Has All Components)
Current: "revenue for PBM for Q3 2025"
→ metric: "revenue", filters: ["PBM"], time: "Q3 2025"
→ Has metric + filters + time (all present) → Decision: NEW

Example 2: Complete Question with Filters (Your Key Case)
Previous: "revenue by line of business for PBM for Q3 2025"
Current: "actuals for PBM for September 2025"
→ metric: "actuals", filters: ["PBM"], time: "September 2025"
→ Has metric + filters + time (all present) → Decision: NEW
→ Note: Even though previous had "by line of business", current is complete standalone question

Example 3: Missing Filters
Current: "compare actuals for September"
Previous: "revenue for PBM for Q3 2025"
→ metric: "actuals", filters: [], time: "September" (partial)
→ Has metric + time, but MISSING filters → Decision: FOLLOW-UP

Example 4: Missing Time and Filters
Current: "for covid vaccines"
→ metric: null, filters: ["covid vaccines"], time: null
→ MISSING metric and time → Decision: FOLLOW-UP

Example 5: Pronoun Reference
Current: "why is that high"
→ Has pronoun "that" → Decision: FOLLOW-UP

Example 6: User Command
Current: "NEW: revenue for Specialty"
→ Starts with "NEW:" → Decision: NEW (respect command)

════════════════════════════════════════════════════════════
OUTPUT FORMAT (JSON ONLY)
════════════════════════════════════════════════════════════

Return valid JSON without markdown:

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
