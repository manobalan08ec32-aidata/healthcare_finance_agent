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

For each component type, compare current vs previous:

**Metric:**
- If current has metric → use current's metric
- If current missing metric → inherit previous metric

**Time:**
- If current has time → use current's time
- If current missing time → inherit previous time

**Attributes:**
- If current has attributes → use current's attributes
- If current missing attributes → inherit previous attributes

**Filters (SPECIAL RULE - ACCUMULATE by default):**

⚠️ **CRITICAL: ALWAYS preserve these filters from previous question:**
- PBM, HDP, Home Delivery, Specialty, Mail, Retail
- These are SCOPE-DEFINING filters and MUST carry forward to all follow-ups
- Never drop these, even if current question has other filters

**General Filter Inheritance:**
- If current has NO filters → inherit ALL previous filters
- If current has filters AND previous has filters → **KEEP BOTH** (accumulate)
  - Exception: Replace only if current filter is MORE SPECIFIC version of previous filter
    * "GLP-1" → "Wegovy" (Wegovy replaces GLP-1, both are drug-related)
    * "diabetes" → "Metformin" (Metformin replaces diabetes, both are drug-related)
  - Otherwise: KEEP BOTH (different categories)
    * "Mail" + "MPDOVA" → KEEP BOTH (channel + carrier)
    * "Mail" + "Mounjaro" → KEEP BOTH (channel + drug)
    * "PBM" + "GLP-1" → KEEP BOTH (domain + therapy class)

**Quick Filter Check:**
- Is it PBM/HDP/Home Delivery/Specialty/Mail/Retail? → ALWAYS keep
- Are they same category (both drugs, both carriers)? → Replace with more specific
- Are they different categories? → Keep both (accumulate)

Valid question structures:
- metric + filters + time
- metric + attributes + time
- metric + filters + attributes + time
- metric + attributes (e.g., "revenue by line of business")
- metric + filters (e.g., "revenue for PBM")
