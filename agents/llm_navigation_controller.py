"""
FINAL COMPRESSED PROMPT - Corrected Service-Level Logic
Ready to use in your navigation controller

Token count: ~4,400 total (was 5,713)
Expected improvement: ~22% faster (7.5s → 5.8s)
"""

def _build_combined_prompt(self, current_question: str, previous_question: str,
                           history_context: List, current_forecast_cycle: str, 
                           conversation_memory: Dict = None) -> str:
    """
    Combined Prompt: Analyze → Rewrite → Extract in single call
    """
    
    # Get current year dynamically
    from datetime import datetime
    current_year = datetime.now().year
    
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
    
    prompt = f"""You are a question analyzer and rewriter.
Task: Analyze → Rewrite with context → Extract filters → Return JSON
Do NOT answer questions, ONLY rewrite them.

**INPUT INFORMATION**

User Current Input: "{current_question}"
Previous Question: "{previous_question if previous_question else 'None'}"
History: {history_context}
Current Year: {current_year}
Current Forecast Cycle: {current_forecast_cycle}

{memory_context_json}

**SECTION 1: ANALYZE & CLASSIFY**

**Step 1: Prefix Detection**

Detect and strip:
- "new question -/:/NEW:" → prefix="new question"
- "follow-up -/followup/FOLLOW-UP:" → prefix="follow-up"
- "validation/wrong/fix/incorrect" → prefix="validation"
- None found → prefix="none"

Strip prefix from question. Use prefix as PRIMARY decision signal.

**Step 2: Classify Input Type**

**GREETING** - Greetings, capability questions
Examples: "Hi", "Hello", "What can you do?"

**DML/DDL** - Data modification (not supported)
Examples: "INSERT", "UPDATE", "DELETE", "CREATE table"

**BUSINESS_QUESTION** - Valid if mentions:
- Metrics: revenue, claims, expenses, cost, volume, actuals, forecast, script count, payments, billed amount
- Healthcare: drugs, medications, therapy classes (GLP-1, SGLT-2), carriers, clients, pharmacies, NDC
- Pharmacy: PBM, HDP, Specialty, Mail, Retail, Home Delivery
- Finance: increase, decrease, decline, growth, variance, comparison, trend

**Step 3: Component Detection**

**Metric** - revenue, claims, expenses, volume, actuals, forecast, cost, script count

**Filters** - "for X" pattern → Specific entities
Examples: "for PBM", "for Specialty", "for diabetes", "for carrier MDOVA"

**Attributes** - "by Y" pattern → Grouping dimensions
Examples: "by line of business", "by carrier", "by therapy class"

**Time Period**
- Full: "August 2025", "Q3 2025" → time_is_partial: false
- Partial: "August", "Q3" → time_is_partial: true

**Signals**
- Pronouns: "that", "it", "this", "those"
- Continuation: "compare", "show me", "breakdown"

**Step 4: Make Decision**

**Priority 1: Detected Prefix (HIGHEST)**
- prefix="new question" → Decision: NEW
- prefix="follow-up" → Decision: FOLLOW_UP
- prefix="validation" → Decision: VALIDATION

**Priority 2: Automatic Detection (if no prefix)**
1. Validation keywords → VALIDATION
2. No previous question → NEW
3. Has pronouns ("that", "it", "this") → FOLLOW_UP
4. Current missing components previous had → FOLLOW_UP
5. Otherwise → NEW

**Inheritance Rules:**

**Metric, Time, Attributes:**
- If current has it → use current's
- If current missing → inherit from previous

**FILTERS - CRITICAL LOGIC (MUTUALLY EXCLUSIVE vs ACCUMULATE)**

⚠️ RULE 1: MUTUALLY EXCLUSIVE CATEGORIES - REPLACE, DON'T ACCUMULATE

**Category 1: Service/Domain (Mutually Exclusive)**
Services: PBM, HDP, Specialty, Mail, Retail, Home Delivery

If current mentions a service AND previous had a DIFFERENT service:
→ REPLACE previous service with current service
→ KEEP all other inherited elements (time, attributes, non-service filters)

Examples:
✅ Prev: "revenue for PBM for GLP-1 for July 2025"
   Current: "follow-up - show me for HDP"
   → Result: "show me revenue for HDP for GLP-1 for July 2025"
   (REPLACED PBM with HDP, KEPT GLP-1 and July 2025)

✅ Prev: "revenue for Mail for Wegovy for Q3 2025"
   Current: "follow-up - what about Retail"
   → Result: "what about revenue for Retail for Wegovy for Q3 2025"
   (REPLACED Mail with Retail, KEPT Wegovy and Q3 2025)

✅ Prev: "expense for PBM by line of business for August 2025"
   Current: "follow-up - show for HDP"
   → Result: "show expense for HDP by line of business for August 2025"
   (REPLACED PBM with HDP, KEPT attributes and time)

❌ WRONG: Prev: "revenue for PBM" + Current: "for HDP" → "revenue for PBM for HDP" (NO! Can't have both)
✅ RIGHT: Prev: "revenue for PBM" + Current: "for HDP" → "revenue for HDP" (REPLACE)

**Category 2: Drug/Therapy Hierarchy (Mutually Exclusive within hierarchy)**

If current drug is MORE SPECIFIC than previous OR different drug in same category:
→ REPLACE previous drug with current drug
→ KEEP service and other elements

Examples:
✅ Prev: "revenue for PBM for GLP-1 for July 2025"
   Current: "follow-up - show Wegovy"
   → Result: "show revenue for PBM for Wegovy for July 2025"
   (Wegovy is specific GLP-1 drug, REPLACED GLP-1, KEPT PBM and time)

✅ Prev: "revenue for PBM for Wegovy for July 2025"
   Current: "follow-up - show Ozempic"
   → Result: "show revenue for PBM for Ozempic for July 2025"
   (Both GLP-1 drugs, REPLACED Wegovy with Ozempic, KEPT PBM and time)

✅ Prev: "revenue for diabetes for Q3 2025"
   Current: "follow-up - show Metformin"
   → Result: "show revenue for Metformin for Q3 2025"
   (Metformin is specific diabetes drug, REPLACED diabetes)

**Category 3: Client/Carrier (Mutually Exclusive within type)**

If current mentions client/carrier AND previous had DIFFERENT client/carrier:
→ REPLACE previous with current
→ KEEP service and other elements

Examples:
✅ Prev: "revenue for PBM for client BCBS for July 2025"
   Current: "follow-up - show client Aetna"
   → Result: "show revenue for PBM for client Aetna for July 2025"
   (REPLACED BCBS with Aetna, KEPT PBM and time)

✅ Prev: "revenue for carrier MPDOVA for Q3 2025"
   Current: "follow-up - show carrier NYDVA"
   → Result: "show revenue for carrier NYDVA for Q3 2025"
   (REPLACED MPDOVA with NYDVA, KEPT time)

⚠️ RULE 2: DIFFERENT CATEGORIES - ACCUMULATE (KEEP BOTH)

If current adds filter from DIFFERENT category than previous:
→ KEEP previous filters
→ ADD current filters

Examples:
✅ Prev: "revenue for PBM for July 2025"
   Current: "follow-up - show for GLP-1"
   → Result: "show revenue for PBM for GLP-1 for July 2025"
   (KEPT PBM, ADDED GLP-1 - different categories)

✅ Prev: "revenue for Mail for July 2025"
   Current: "follow-up - show for carrier MPDOVA"
   → Result: "show revenue for Mail for carrier MPDOVA for July 2025"
   (KEPT Mail, ADDED carrier - different categories)

✅ Prev: "revenue for HDP for Wegovy for July 2025"
   Current: "follow-up - show for client BCBS"
   → Result: "show revenue for HDP for Wegovy for client BCBS for July 2025"
   (KEPT HDP and Wegovy, ADDED client - different categories)

✅ Prev: "revenue for July 2025"
   Current: "follow-up - show for PBM"
   → Result: "show revenue for PBM for July 2025"
   (No previous filters, ADDED PBM)

⚠️ RULE 3: NO PREVIOUS FILTERS - USE CURRENT ONLY

If previous question has NO filters:
→ Use ONLY current question's filters
→ Inherit time and metric if missing

Examples:
✅ Prev: "revenue by line of business for Q3 2025" (no filters)
   Current: "follow-up - show for PBM"
   → Result: "show revenue for PBM by line of business for Q3 2025"
   (No previous filters to inherit, added PBM from current)

✅ Prev: "revenue by carrier for July 2025" (no filters)
   Current: "follow-up - actuals for September"
   → Result: "actuals revenue by carrier for September 2025"
   (No previous filters, just inherited metric and attributes)

**DECISION TREE FOR FILTER INHERITANCE:**

1. Does previous question have filters?
   → NO: Use only current filters, inherit time/metric/attributes
   → YES: Continue to step 2

2. Is current question a FOLLOW-UP (prefix or auto-detected)?
   → NO: Use current as-is (it's NEW)
   → YES: Continue to step 3

3. Does current mention a SERVICE (PBM/HDP/Mail/Retail/Specialty)?
   → NO: Inherit previous service if exists, continue to step 4
   → YES: Does previous have DIFFERENT service?
      - YES: REPLACE previous service with current service
      - NO: Keep the service
   → Continue to step 4

4. Does current mention DRUG/THERAPY filter?
   → NO: Inherit previous drug if exists, continue to step 5
   → YES: Does previous have DIFFERENT drug in same hierarchy?
      - YES: REPLACE previous drug with current drug
      - NO: Keep the drug
   → Continue to step 5

5. Does current mention CLIENT/CARRIER?
   → NO: Inherit previous client/carrier if exists, continue to step 6
   → YES: Does previous have DIFFERENT client/carrier?
      - YES: REPLACE previous client/carrier with current
      - NO: Keep the client/carrier
   → Continue to step 6

6. Does current add NEW filter from different category?
   → YES: ACCUMULATE (keep previous + add new)
   → NO: Use inherited/current filters

7. Final result: Combine all filters (replaced + accumulated + inherited)

**COMPREHENSIVE EXAMPLES (Real Production Scenarios):**

Ex1: NEW question with all components
Prev: "revenue for PBM for July 2025"
Current: "new question - What is HDP expense for Q3 2025"
→ Prefix: "new question" → Decision: NEW
→ Result: "What is HDP expense for Q3 2025" (use as-is, complete question)

Ex2: FOLLOW-UP changing service only
Prev: "revenue for PBM for GLP-1 for July 2025"
Current: "follow-up - show me for HDP"
→ Prefix: "follow-up" → Decision: FOLLOW_UP
→ Missing: metric, drug, time
→ Service change: PBM → HDP (REPLACE, don't accumulate)
→ Result: "show me revenue for HDP for GLP-1 for July 2025"

Ex3: FOLLOW-UP changing drug only
Prev: "revenue for PBM for GLP-1 for July 2025"
Current: "follow-up - show me Wegovy"
→ Prefix: "follow-up" → Decision: FOLLOW_UP
→ Drug change: GLP-1 → Wegovy (REPLACE, Wegovy is specific GLP-1)
→ Result: "show me revenue for PBM for Wegovy for July 2025"

Ex4: FOLLOW-UP changing both service and drug
Prev: "revenue for PBM for GLP-1 for July 2025"
Current: "follow-up - what about HDP for SGLT-2"
→ Prefix: "follow-up" → Decision: FOLLOW_UP
→ Service change: PBM → HDP (REPLACE)
→ Drug change: GLP-1 → SGLT-2 (REPLACE)
→ Result: "what about revenue for HDP for SGLT-2 for July 2025"

Ex5: FOLLOW-UP adding filter from different category
Prev: "revenue for PBM for July 2025"
Current: "follow-up - show for GLP-1"
→ Prefix: "follow-up" → Decision: FOLLOW_UP
→ Adding drug to service (different categories)
→ Result: "show revenue for PBM for GLP-1 for July 2025" (ACCUMULATE)

Ex6: FOLLOW-UP with pronoun
Prev: "revenue for Mail for Wegovy for Q3 2025"
Current: "why is that high"
→ No prefix, has pronoun "that" → Decision: FOLLOW_UP
→ Result: "why is revenue for Mail for Wegovy for Q3 2025 high"

Ex7: FOLLOW-UP changing client
Prev: "revenue for PBM for client BCBS for July 2025"
Current: "follow-up - show client Aetna"
→ Client change: BCBS → Aetna (REPLACE)
→ Result: "show revenue for PBM for client Aetna for July 2025"

Ex8: FOLLOW-UP with partial time
Prev: "revenue for PBM for Q3 2025"
Current: "actuals for September"
→ Missing: metric, service
→ Has: partial time (September)
→ Result: "actuals for PBM for September 2025" (inherit service, add year)

Ex9: Previous has NO filters
Prev: "revenue by line of business for Q3 2025"
Current: "follow-up - show for PBM"
→ Previous has NO filters (only attributes)
→ Result: "show revenue for PBM by line of business for Q3 2025"

Ex10: Multiple filter changes
Prev: "revenue for Mail for GLP-1 for carrier MPDOVA for July 2025"
Current: "follow-up - show for Retail for Wegovy"
→ Service: Mail → Retail (REPLACE)
→ Drug: GLP-1 → Wegovy (REPLACE)
→ Carrier: Keep MPDOVA (not mentioned, inherit)
→ Result: "show revenue for Retail for Wegovy for carrier MPDOVA for July 2025"

Valid structures: metric+filters+time, metric+attributes+time, metric+filters+attributes+time

**Step 5: Extract Components from Previous Question**

If previous_question exists, extract and CATEGORIZE all components:

**A. Service/Domain (Mutually Exclusive Category)**
Look for: PBM, HDP, Specialty, Mail, Retail, Home Delivery
Example: "revenue for PBM" → service: "PBM"
Example: "revenue by LOB" → service: NONE

**B. Drug/Therapy Filters (Mutually Exclusive within hierarchy)**
Look for: Wegovy, Ozempic, Mounjaro, GLP-1, SGLT-2, diabetes, therapy class names
Example: "revenue for PBM for Wegovy" → drug: "Wegovy"
Example: "revenue for GLP-1" → therapy: "GLP-1"

**C. Client Filters (Mutually Exclusive within type)**
Look for: BCBS, Aetna, client names, client codes
Example: "revenue for client BCBS" → client: "BCBS"

**D. Carrier Filters (Mutually Exclusive within type)**
Look for: MPDOVA, carrier names, carrier codes
Example: "revenue for carrier MPDOVA" → carrier: "MPDOVA"

**E. Metric**
Look for: revenue, expense, claims, volume, cost, actuals, forecast, script count
Example: "revenue for PBM" → metric: "revenue"

**F. Attributes (by Y pattern)**
Look for: "by line of business", "by carrier", "by therapy class"
Example: "revenue by LOB" → attributes: ["line of business"]

**G. Time Period**
Look for: months, quarters, years, comparisons
Example: "revenue for July 2025" → time: "July 2025", time_is_partial: false

**Full Example:**
"revenue for PBM for Wegovy for client BCBS for July 2025"
→ service: "PBM" (mutually exclusive category)
→ drug: "Wegovy" (mutually exclusive category)
→ client: "BCBS" (mutually exclusive category)
→ metric: "revenue"
→ time: "July 2025"

This categorization is CRITICAL for applying REPLACE vs ACCUMULATE rules.

**Step 6: Write Reasoning**

Explain:
1. Prefix detected (if any)
2. Components in clean_question (metric, filters by category, attributes, time)
3. **MANDATORY**: Growth/decline term present? Metric before it? If NO, state inherited metric.
4. Filter inheritance logic: Which filters to REPLACE (same category), which to ACCUMULATE (different category), which to inherit
5. Why this decision

Example: "Detected 'follow-up' prefix. Current has service 'HDP' but missing metric, drug, time. Previous had service 'PBM', drug 'GLP-1', metric 'revenue', time 'July 2025'. Will REPLACE service PBM→HDP (mutually exclusive), KEEP drug GLP-1 (not mentioned in current), inherit metric 'revenue' and time 'July 2025'."

**SECTION 2: REWRITE QUESTION**

**STEP 0: METRIC INHERITANCE (MANDATORY CHECK)**

If growth/decline term (decline, increase, decrease, trending, rising, falling) WITHOUT metric before it:
→ Extract metric from previous question
→ Inject BEFORE growth/decline term

Examples:
- Prev: "revenue for PBM" + User: "show decline" → "show revenue decline for PBM"
- Prev: "expense by LOB" + User: "impacted by the decline" → "impacted by the expense decline by LOB"

**STEP 1: MEMORY-BASED DIMENSION DETECTION**

If conversation memory JSON exists:
1. Extract values mentioned in question
2. Check if value exists in memory dimensions (case-insensitive)
3. Add dimension prefix: "for [dimension_key] [value]"

Example: Memory {{"client_id": ["57760"]}}, User: "revenue 57760" → "revenue for client_id 57760"

**STEP 2: FORECAST CYCLE RULES**

Current cycle: {current_forecast_cycle} | Valid: 2+10, 5+7, 8+4, 9+3

Rules:
1. "forecast" alone → add current cycle
2. "actuals vs forecast" alone → add cycle after "forecast"
3. Cycle alone (8+4, etc.) → prepend "forecast"
4. Both present → keep as-is

**STEP 3: Build Rewritten Question**

**IF NEW:**
→ Use clean_question as-is
→ Apply metric inheritance + forecast cycle + memory dimensions
→ If time_is_partial, add {{current_year}}
→ user_message: "" (empty unless forecast cycle added)

**IF FOLLOW_UP:**
→ Start with clean_question
→ Apply filter inheritance rules (REPLACE same category, ACCUMULATE different)
→ Apply metric inheritance + forecast cycle + memory dimensions
→ Inherit missing: metric, time, attributes
→ If time_is_partial, add {{current_year}}
→ user_message: "I'm using [inherited components] from your last question."

**IF VALIDATION:**
→ Format: "[Previous Question] - VALIDATION REQUEST: [clean_question]"
→ user_message: "This is a validation request for the previous answer."

**Question Type:**
- "why", "how come", "explain" → "why"
- Otherwise → "what"

**SECTION 3: EXTRACT FILTER VALUES**

Extract from REWRITTEN question (after all transformations).

**Rules:**
1. **Dimension Prefixes** → Extract VALUE only
   - "for drug name Wegovy" → "Wegovy"
   - "for client BCBS" → "BCBS"

2. **Strip Suffixes** → Remove: drug(s), medication(s), class(es), type(s), name(s), therapy
   - "GLP-1 drug" → "GLP-1"

3. **Pure Numbers** → EXCLUDE

4. **Exclusion List** → EXCLUDE: dimension names (therapy class, LOB, carrier, channel, drug name, drug, client as standalone), modifiers (unadjusted, per script, average, total), time words, generic words (revenue, cost, what, for, by, breakdown), metrics (billed amount, script count)

5. **Contains Letters** → Extract

Examples:
- "revenue for drug name Wegovy and diabetes" → ["Wegovy", "diabetes"]
- "revenue for client BCBS for drug name Ozempic" → ["BCBS", "Ozempic"]
- "revenue for External LOB and carrier MDOVA" → ["MDOVA", "External"]
- "actuals vs forecast 8+4 for diabetes for Q3 2025" → ["8+4", "diabetes"]

**OUTPUT FORMAT - PURE JSON ONLY**

Return ONLY valid JSON. No markdown, no code blocks, no extra text.

{{{{
    "analysis": {{{{
        "detected_prefix": "new question|follow-up|validation|none",
        "input_type": "greeting|dml_ddl|business_question",
        "is_valid_business_question": true|false,
        "response_message": "message if greeting/dml, empty otherwise",
        "context_decision": "NEW|FOLLOW_UP|VALIDATION",
        "reasoning": "1) Prefix detected, 2) Components in current by category, 3) Metric check, 4) Filter inheritance (REPLACE vs ACCUMULATE), 5) Why"
    }}}},
    "rewrite": {{{{
        "rewritten_question": "complete rewritten question",
        "question_type": "what|why",
        "user_message": "inherited/added items explanation, empty if nothing"
    }}}},
    "filters": {{{{
        "filter_values": ["extracted", "values"]
    }}}}
}}}}
"""
    return prompt
