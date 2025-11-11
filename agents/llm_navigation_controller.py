**Filters (SPECIAL RULE - ACCUMULATE by default):**

⚠️ **CRITICAL: Scope-Defining Filter Preservation**
IF previous question contained ANY of these filters:
- PBM, HDP, Home Delivery, Specialty, Mail, Retail

THEN those specific filters MUST be inherited in follow-up:
- Only inherit the ones that ACTUALLY EXISTED in previous question
- Do NOT add these if they weren't in previous question
- These are SCOPE-DEFINING filters and must carry forward when present

**General Filter Inheritance:**
- If current has NO filters → inherit ALL previous filters (only what actually existed)
- If current has filters AND previous has filters → **KEEP BOTH** (accumulate)
  - Exception: Replace only if current filter is MORE SPECIFIC version of previous filter
    * "GLP-1" → "Wegovy" (Wegovy replaces GLP-1, both are drug-related)
    * "diabetes" → "Metformin" (Metformin replaces diabetes, both are drug-related)
  - Otherwise: KEEP BOTH (different categories)
    * "Mail" + "MPDOVA" → KEEP BOTH (channel + carrier)
    * "Mail" + "Mounjaro" → KEEP BOTH (channel + drug)
    * "PBM" + "GLP-1" → KEEP BOTH (domain + therapy class)

**Quick Filter Check:**
- Was it (PBM/HDP/Home Delivery/Specialty/Mail/Retail) in previous? → MUST keep it
- Are they same category (both drugs, both carriers)? → Replace with more specific
- Are they different categories? → Keep both (accumulate)
