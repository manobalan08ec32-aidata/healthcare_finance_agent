async def _llm_dataset_selection(self, search_results: List[Dict], state: AgentState, filter_metadata_results: List[str] = None) -> Dict:
    """Enhanced LLM selection with validation, disambiguation handling, filter-based selection, and historical learning"""
    
    user_question = state.get('current_question', state.get('original_question', ''))
    filter_values = state.get('filter_values', [])
    
    # NEW: Get feedback match for historical learning (3 lines)
    feedback_match = state.get('feedback_match_result', {})
    has_history = feedback_match.get('status') == 'match_found'
    matched_table = feedback_match.get('table_name', '') if has_history else ''
    
    # Format filter metadata results for the prompt
    filter_metadata_text = ""
    if filter_metadata_results:
        filter_metadata_text = "\n**FILTER METADATA FOUND:**\n"
        for result in filter_metadata_results:
            filter_metadata_text += f"- {result}\n"
    else:
        filter_metadata_text = "\n**FILTER METADATA:** No specific filter values found in metadata.\n"
    
    # NEW: Build history hint (only if match exists) (5 lines)
    history_hint = ""
    if has_history:
        history_hint = f"""
**HISTORICAL HINT**: A similar question previously used table: {matched_table}
- If this table is valid and you're deciding between multiple options, prefer it.
- If it's not in search results or doesn't meet requirements, ignore this hint.
"""

    selection_prompt = f"""
You are a Dataset Identifier Agent. You have FIVE sequential tasks to complete.

CURRENT QUESTION: {user_question}

EXTRACTED COLUMNS WITH FILTER VALUES: {filter_values}
{filter_metadata_text}

AVAILABLE DATASETS (JSON FORMAT): 

{search_results}

A. **PHI/PII SECURITY CHECK**:
- First, examine each dataset's "PHI_PII_Columns" field (if present)
- Analyze the user's question to identify if they are requesting any PHI/PII information
- PHI/PII includes: SSN, member IDs, personal identifiers, patient names, addresses, etc.
- Check if the user's question mentions or implies access to columns listed in "PHI_PII_Columns"
- If PHI/PII columns are requested, IMMEDIATELY return phi_found status (do not proceed to other checks)

B. **METRICS & ATTRIBUTES CHECK**:
- Extract requested metrics/measures and attributes/dimensions
- Apply smart mapping with these rules:

**TIER 1 - Direct Matches**: Exact column names
**TIER 2 - Standard Healthcare Mapping**: 
    * "therapies" → "therapy_class_name"
    * "scripts" → "unadjusted_scripts/adjusted_scripts"  
    * "drugs" → "drug_name"
    * "clients" → "client_id/client_name"

**TIER 3 - Mathematical Operations**: 
    * "variance/variances" → calculated from existing metrics over time periods
    * "growth/change" → period-over-period calculations
    * "percentage/rate" → ratio calculations

**TIER 4 - Skip Common Filter Values**: 
    * Skip validation for: "external", "internal", "retail", "mail order", "commercial", "medicare", "brand", "generic"
    * These appear to be filter values, not missing attributes

**BLOCK - Creative Substitutions**:
    * Do NOT map unrelated terms (e.g., "ingredient fee" ≠ "expense")
    * Do NOT assume domain knowledge not in metadata

- Only mark as missing if NO reasonable Tier 1-3 mapping exists

**NEW - EXPLICIT ATTRIBUTE DETECTION**:
- Scan the user's question for explicit attribute keywords(examples below):
    * "carrier" → carrier_name, carrier_id
    * "drug" → drug_name, drug_id
    * "pharmacy" → pharmacy_name, pharmacy_id
    * "therapy" → therapy_class_name, therapy_id
    * "client" → client_name, client_id
    * "manufacturer" → drug_manufctr_nm, manufacturer_name
- Flag as "explicit_attribute_mentioned = true/false"
- Store mapped column names for later filter disambiguation check
- This detection is case-insensitive and looks for these keywords anywhere in the question

B1. **TABLE-BY-TABLE EVALUATION**:
**CRITICAL: Evaluate EACH table separately to avoid mixing information**

For EACH dataset in AVAILABLE DATASETS, perform this evaluation:

**STEP 1 - Extract Table Information:**
- Table name: [full table name]
- Functional name: [functional table name]
- This table's metrics: [list from metadata]
- This table's attributes: [list from metadata]
- This table's not_useful_for: [EXACT copy from THIS table only]

**STEP 2 - Check User's Requirements Against THIS Table:**
- Does THIS table have the user's requested metrics? [YES/NO - list what's found/missing]
- Does THIS table have the user's requested attributes? [YES/NO - list what's found/missing]
- Does user's query match ANY pattern in THIS table's not_useful_for? [YES/NO - explain if match found]

**STEP 3 - Decision for THIS Table:**
- If not_useful_for matches user query → ELIMINATE (do not consider this table)
- If missing required metrics/attributes → INCOMPLETE (note what's missing)
- If has all requirements and no conflicts → COMPLETE (candidate for selection)

**STEP 4 - Move to Next Table:**
Repeat Steps 1-3 for the next table. Do NOT mix information between tables.

**FINAL RESULT:**
- Eliminated tables: [list with reasons]
- Incomplete tables: [list with what's missing]
- Complete tables: [list - these are your candidates]

If multiple complete tables exist, proceed to Section C for tie-breaking.
If no complete tables, check if complementary analysis needed (Section D).

C. **KEYWORD & SUITABILITY ANALYSIS**:
- **KEYWORD MATCHING**: Look for domain keywords that indicate preferences:
* "claim/claims" → indicates claim_transaction dataset relevance
* "forecast/budget" → indicates actuals_vs_forecast dataset relevance  
* "ledger" → indicates actuals_vs_forecast dataset relevance

- **CRITICAL: SUITABILITY VALIDATION (HARD CONSTRAINTS)**:
* **BLOCKING RULE**: If a dataset's "not_useful_for" field contains keywords/patterns that match the user's question, IMMEDIATELY EXCLUDE that dataset regardless of other factors
* **POSITIVE VALIDATION**: Check if user's question aligns with "useful_for" field patterns
* **Example Applications**:
- User asks "top 10 clients by expense" → Ledger has "not_useful_for": ["client level expense"] → EXCLUDE ledger table completely
- User asks "claim-level analysis" → Claims has "useful_for": ["claim-level financial analysis"] → PREFER claims table
* **PRECEDENCE**: not_useful_for OVERRIDES metrics/attributes availability - even if a table has the columns, exclude it if explicitly marked as not suitable

- Verify time_grains match user needs (daily vs monthly vs quarterly)
- Note: Keywords indicate relevance but suitability constraints are MANDATORY

D. **COMPLEMENTARY ANALYSIS CHECK**:
- **PURPOSE**: Identify if multiple datasets together provide more complete analysis than any single dataset
- **LOOK FOR THESE PATTERNS**:
* Primary metric in one dataset + dimensional attributes in another (e.g., "ledger revenue" + "therapy breakdown")
* Different analytical perspectives on same business question (e.g., actuals view + claims view)
* One dataset provides core data, another provides breakdown/segmentation
* Cross-dataset comparison needs (e.g., budget vs actual vs claims)
* **BREAKDOWN ANALYSIS**: When question asks for metric breakdown by dimensions not available in the primary dataset

- **EVALUATION CRITERIA**:
* Single dataset with ALL metrics + attributes → SELECT IT
* No single complete dataset → SELECT MULTIPLE if complementary
* Primary metric in A + breakdown dimension in B → SELECT BOTH

**KEY EXAMPLES**:
- "top 10 drugs by revenue" → Claims table (has revenue + drug_name) NOT Ledger (missing drug_name)
- "total revenue" → Ledger table (high_level_table tie-breaker when both have revenue)
- "ledger revenue breakdown by drug" → Both tables (complementary: ledger revenue + claims drug_name)

**CLARIFICATION vs COMPLEMENTARY**:
- Ask clarification when: Same data available in multiple datasets with different contexts OR multiple columns in same table
- Select multiple when: Different but compatible data needed from each dataset for complete analysis

F. **FINAL DECISION LOGIC**:

{history_hint}

- **STEP 1**: Use results from B1 (TABLE-BY-TABLE EVALUATION)
- **STEP 2**: Decision based on B1's complete tables list:

* **IF 0 complete tables**: Check if complementary analysis needed (Section D)
  - If complementary datasets can provide coverage → SELECT MULTIPLE
  - Otherwise → status=missing_items

* **IF 1 complete table**: SELECT IT → Proceed to filter disambiguation below

* **IF 2+ complete tables**: Apply tie-breakers IN ORDER:
  1. **Historical preference** (NEW - if hint provided above):
     - Check if historical table is in your complete tables list
     - If YES → SELECT IT (it was successful for similar question before)
     - If NO or not applicable → Proceed to next tie-breaker
  
  2. **Keyword preference**: "claim/claims" → Claims table | "billing/invoice" → Billing table | "ledger/forecast/budget" → Ledger table
  
  3. **High-level table priority** (only for high-level queries: total/overall/summary, NOT breakdown/top X/distribution)
  
  4. **Still tied?** → status=needs_disambiguation

* **SMART FILTER DISAMBIGUATION CHECK**:
**STEP 3A**: Check if filter values exist in multiple columns
**STEP 3B**: Determine if follow-up is needed using this SIMPLIFIED logic:

1. **Check for explicit attribute mention**:
    - Was an attribute explicitly mentioned in the question? (from Section B detection)
    - Examples: "Carrier", "drug", "pharmacy", "therapy", "client", "manufacturer", "plan"

2. **Count matching columns in selected dataset**:
    - From filter metadata, identify all columns containing the filter value
    - Filter to only columns that exist in the selected dataset's attributes
    - Store as: matching_columns_count

3. **SIMPLE DECISION TREE**:
    - IF explicit_attribute_mentioned = true → NO FOLLOW-UP (trust user's specification)
    - IF explicit_attribute_mentioned = false:
        * IF matching_columns_count = 1 → NO FOLLOW-UP (obvious choice)
        * IF matching_columns_count > 1 → RETURN "needs_disambiguation"

**Examples**:
- "Carrier MPDOVA billed amount" + MPDOVA in [carrier_name, carrier_id, plan_name] → ✓ NO follow-up (user said "Carrier")
- "MPDOVA billed amount" + MPDOVA in [carrier_name] only → ✓ NO follow-up (only 1 match)
- "covid vaccine billed amount" + covid vaccine in [drug_name, pharmacy_name, therapy_class_name] → ❌ ASK which column (no explicit attribute, 3 matches)

**HIGH LEVEL TABLE PRIORITY RULE** (ONLY APPLIES DURING TIES):
- **CRITICAL**: High-level table priority is ONLY used as a tie-breaker when multiple datasets have ALL required metrics AND attributes
- **PRIMARY RULE**: ALWAYS validate that dataset has both required metrics AND required attributes FIRST
- **HIGH LEVEL QUESTION INDICATORS**: Questions asking for summary metrics, totals, aggregates, or general overviews without specific breakdowns
- **Examples of HIGH LEVEL**: "total revenue", "overall costs", "summary metrics", "high-level view", "aggregate performance", "what is the revenue", "show me costs"  
- **Examples of NOT HIGH LEVEL**: "revenue breakdown by therapy", "costs by client", "detailed analysis", "revenue by drug category", "performance by region", "top drugs by revenue", "top clients by cost"
- **VALIDATION FIRST RULE**: 
* Step 1: Check if dataset has required metrics (revenue, cost, etc.)
* Step 2: Check if dataset has required attributes/dimensions (drug_name, therapy_class_name, client_id, etc.)
* Step 3: ONLY if multiple datasets pass Steps 1 & 2, then check "high_level_table": "True" as tie-breaker
- **NEVER OVERRIDE RULE**: Never select high_level_table if it's missing required attributes, even for "high-level" questions

==============================
DECISION CRITERIA
==============================

**PHI_FOUND** IF:
- User question requests or implies access to PHI/PII columns
- Any columns mentioned in "PHI_PII_Columns" are being requested
- Must be checked FIRST before other validations

**PROCEED** (SELECT DATASET) IF:
- **STANDARD PATH**: Dataset passes suitability validation (not blocked by "not_useful_for" field) AND all requested metrics/attributes have Tier 1-3 matches AND clear selection
- Single dataset meets all requirements after suitability and coverage checks
- Complementary datasets identified for complete coverage after all validations

**MISSING_ITEMS** IF:
- Required metrics/attributes don't have Tier 1-3 matches in any dataset
- No suitable alternatives available after all validation steps

**REQUEST_FOLLOW_UP** IF:
- **PRIORITY 1 - DATASET AMBIGUITY**: 
* Multiple datasets with conflicting contexts AND no clear preference from traditional validation
* ALWAYS ask user to specify which table/dataset to use FIRST

- **PRIORITY 2 - SMART FILTER COLUMN AMBIGUITY**: 
* User did NOT explicitly mention an attribute (e.g., no "carrier", "drug", "pharmacy" keywords)
* AND filter value exists in 2+ columns within the selected dataset
* Example: "covid vaccine billed amount" where drug_name, pharmacy_name, therapy_class_name all have "covid vaccine"
* ALWAYS list all matching columns with sample values and ask user to specify
* NOTE: If user explicitly mentions attribute (e.g., "Carrier MPDOVA"), NO follow-up needed regardless of multiple matches

==============================
ASSESSMENT FORMAT (BRIEF)
==============================

**ASSESSMENT**: A:✓(no PHI) B:✓(metrics found) B1:✓(table eval done) B-ATTR:✓(explicit attr OR single column) C:✓(suitability passed) D:✓(complementary) F:✓(clear selection)
**DECISION**: PROCEED - [One sentence reasoning]

Keep assessment ultra-brief:
- Use checkmarks (✓) or X marks (❌) with 10 words max explanation in parentheses
- **NEW B1**: ✓ means table-by-table evaluation completed successfully
- **NEW B-ATTR**: ✓ means explicit attribute mentioned OR only 1 column match (no ambiguity), ❌ means no explicit attr AND multiple columns (disambiguation needed)
- **CRITICAL for C (suitability)**: ✓ means no "not_useful_for" conflicts, ❌ means blocked by suitability constraints
- Each area gets: "A:✓(brief reason)" or "A:❌(brief issue)"
- Decision reasoning maximum 15 words
- No detailed explanations or bullet points in assessment
- Save detailed analysis for JSON selection_reasoning field

=======================
RESPONSE FORMAT
=======================

IMPORTANT: Keep assessment ultra-brief (1-2 lines max), then output ONLY the JSON wrapped in <json> tags.

"status": "phi_found" | "success" | "missing_items" | "needs_disambiguation",
"final_actual_tables": ["table_name_1","table_name2"] if status = success else [],
"functional_names": ["functional_name"] if status = success else [],
"tables_identified_for_clarification": ["table_1", "table_2"] if status = needs_disambiguation else [],
"functional_table_name_identified_for_clarification": ["functional_name_1", "functional_name_2"] if status = needs_disambiguation else [],
"requires_clarification": true if status = needs_disambiguation else false,
"selection_reasoning": "2-3 lines max explanation",
"high_level_table_selected": true/false if status = success else null,
"user_message": "message to user" if status = phi_found or missing_items else null,
"clarification_question": "question to user" if status = needs_disambiguation else null,
"selected_filter_context": "col name - [actual_column_name], sample values [all values from filter extract]" if column selected from filter context else null

**FIELD POPULATION RULES FOR needs_disambiguation STATUS**:
- tables_identified_for_clarification: ALWAYS populate when status = needs_disambiguation
* If PRIORITY 1 (dataset ambiguity): List all candidate tables that need disambiguation
* If PRIORITY 2 (column ambiguity): List the single selected table where column disambiguation is needed

"""

    max_retries = 1
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # print("Sending selection prompt to LLM...",selection_prompt)
            llm_response = await self.db_client.call_claude_api_endpoint_async([
                {"role": "user", "content": selection_prompt}
            ])
            
            print("Raw LLM response:", llm_response)
            
            # Extract JSON from the response using the existing helper method
            json_content = self._extract_json_from_response(llm_response)
            
            selection_result = json.loads(json_content)
            
            # Handle different status types
            status = selection_result.get('status', 'success')
            
            if status == "missing_items":
                print(f"❌ Missing items found: {selection_result.get('missing_items')}")
                return {
                    'final_actual_tables': [],
                    'functional_names': [],
                    'requires_clarification': False,
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'missing_items': selection_result.get('missing_items', {}),
                    'user_message': selection_result.get('user_message', ''),
                    'error_message': '',
                    'status': 'missing_items',
                    'selected_filter_context': ''
                }
            
            elif status in ("needs_disambiguation", "needs_clarification"):
                print(f"❓ Clarification needed - preparing follow-up question")
                return {
                    'final_actual_tables': selection_result.get('final_actual_tables', []),
                    'functional_names': selection_result.get('functional_table_name_identified_for_clarification', []),
                    'requires_clarification': True,
                    'clarification_question': selection_result.get('clarification_question'),
                    'candidate_actual_tables': selection_result.get('tables_identified_for_clarification', []),
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'missing_items': selection_result.get('missing_items', {}),
                    'error_message': '',
                    'selected_filter_context': selection_result.get('selected_filter_context', '')
                }
            
            elif status == "phi_found":
                print(f"❌ PHI/PII information detected: {selection_result.get('selection_reasoning', '')}")
                return {
                    'final_actual_tables': [],
                    'functional_names': [],
                    'requires_clarification': False,
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'user_message': selection_result.get('user_message', ''),
                    'error_message': '',
                    'status': 'phi_found',
                     'selected_filter_context': ''
                }
            
            else:  # status == "success"
                high_level_selected = selection_result.get('high_level_table_selected', False)
                if has_history and matched_table in selection_result.get('final_actual_tables', []):
                    print(f"✅ Dataset selection complete (Historical match used): {selection_result.get('functional_names')}")
                elif high_level_selected:
                    print(f"✅ Dataset selection complete (High-level table prioritized): {selection_result.get('functional_names')}")
                else:
                    print(f"✅ Dataset selection complete: {selection_result.get('functional_names')}")
                return {
                    'final_actual_tables': selection_result.get('final_actual_tables', []),
                    'functional_names': selection_result.get('functional_names', []),
                    'requires_clarification': False,
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'missing_items': selection_result.get('missing_items', {}),
                    'error_message': '',
                    'high_level_table_selected': high_level_selected,
                    'selected_filter_context': selection_result.get('selected_filter_context', '')
                }
                    
        except Exception as e:
            retry_count += 1
            print(f"⚠ Dataset selection attempt {retry_count} failed: {str(e)}")
            
            if retry_count < max_retries:
                print(f"🔄 Retrying... ({retry_count}/{max_retries})")
                await asyncio.sleep(2 ** retry_count)
                continue
            else:
                return {
                    'final_actual_tables': [],
                    'functional_names': [],
                    'requires_clarification': False,
                    'selection_reasoning': 'Dataset selection failed',
                    'missing_items': {'metrics': [], 'attributes': []},
                    'error': True,
                    'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}",
                    'selected_filter_context': ''
                }


async def _assess_and_generate_sql_async(self, context: Dict, state: Dict) -> Dict[str, Any]:
    """SQL generation with optional historical learning context"""
    
    current_question = context.get('current_question', '')
    dataset_metadata = context.get('dataset_metadata', '')
    join_clause = state.get('join_clause', '')
    selected_filter_context = context.get('selected_filter_context')
    
    # NEW: History context for learning
    matched_sql = state.get('matched_sql', '')
    history_question_match = state.get('history_question_match', '')
    matched_table_name = state.get('matched_table_name', '')
    
    # Check if history exists and is relevant
    has_history = bool(matched_sql and history_question_match and matched_table_name)
    
    # Check if we have multiple tables
    selected_datasets = state.get('selected_dataset', [])

    # Define mandatory column mapping
    mandatory_column_mapping = {
        "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast": [
            "Ledger"
        ]
    }
    
    # Extract mandatory columns based on selected datasets
    mandatory_columns_info = []
    if isinstance(selected_datasets, list):
        for dataset in selected_datasets:
            if dataset in mandatory_column_mapping:
                mandatory_columns = mandatory_column_mapping[dataset]
                for col in mandatory_columns:
                    mandatory_columns_info.append(f"Table {dataset}: {col} (MANDATORY)")
            else:
                mandatory_columns_info.append(f"Table {dataset}: Not Applicable")
    
    # Format mandatory columns for prompt
    mandatory_columns_text = "\n".join(mandatory_columns_info) if mandatory_columns_info else "No mandatory columns required"
    
    # Format selected filter context for prompt
    filter_context_text = ""
    if selected_filter_context:
        filter_context_text = f"""
SELECTED FILTER CONTEXT Available for SQL generation if the filter values exactly matches:
     {selected_filter_context}

"""
    
    has_multiple_tables = len(selected_datasets) > 1 if isinstance(selected_datasets, list) else False

    # NEW: Build conditional history context
    history_section = ""
    check_5_text = "**CHECK 5: Historical SQL availability**: N/A (no historical reference)"
    
    if has_history:
        check_5_text = "**CHECK 5: Historical SQL availability**: ✓ Available (using as learning template)"
        history_section = f"""
=== HISTORICAL SUCCESSFUL SQL (LEARNING REFERENCE) ===

A similar question was successfully answered with this SQL:
- Previous Question: "{history_question_match}"
- Table Used: {matched_table_name}

<historical_sql>
{matched_sql}
</historical_sql>

**CRITICAL - HOW TO USE THIS REFERENCE:**

✅ LEARN FROM (Structure & Logic):
1. **Query Structure**: 
   - Observe GROUP BY strategy (dimensions used)
   - Study CASE WHEN patterns (side-by-side columns)
   - Note aggregation logic (SUM, COUNT, AVG placement)
   - Review calculation methods (ROUND, NULLIF usage)

2. **Column Selection**:
   - See which business dimensions are included
   - Understand metric aggregations used
   - Notice naming conventions (e.g., august_revenue_amount)

3. **Best Practices**:
   - UPPER() for case-insensitive filters
   - ROUND(x, 0) for amounts, ROUND(x, 3) for percentages
   - NULLIF for division safety
   - Clean, descriptive column aliases

❌ DO NOT COPY DIRECTLY (Adapt These):
1. **Filter Values**: 
   - Historical may have <parameter> placeholders or specific values
   - ALWAYS extract filters from CURRENT question
   - Example: Historical has carrier_id = 'MPDOVA' → Use carrier_id from current question

2. **Date/Time Values**:
   - Historical may have specific dates/periods
   - ALWAYS use dates from CURRENT question
   - Example: Historical has "year = 2024" → Use year from current question

3. **Entity Names**:
   - Client names, carrier IDs, product categories, etc.
   - ALWAYS use entities from CURRENT question

⚠️ MANDATORY VALIDATIONS:
1. **Add Mandatory Filters**: 
   - Check MANDATORY FILTER COLUMNS section above
   - Historical SQL may not have these (different requirements)
   - YOU MUST ADD any mandatory filters listed

2. **Verify Column Availability**:
   - Confirm all columns exist in AVAILABLE METADATA
   - If historical column missing, use equivalent from metadata

3. **Apply Current Filters**:
   - Use FILTER VALUES EXTRACTED section (marked ✓Valid)
   - Apply filters from CURRENT question, not historical

4. **Update Time Logic**:
   - Match time structure to CURRENT question
   - Monthly trend? YoY comparison? Date range? Use current requirement

**ADAPTATION PRIORITY:**
Content from CURRENT question > Historical structure > Metadata defaults

This is a LEARNING TEMPLATE, not a query to copy. Generate ADAPTED SQL for current question.

====================================================

"""

    assessment_prompt = f"""
You are a highly skilled Healthcare Finance SQL analyst. You have TWO sequential tasks to complete.

CURRENT QUESTION: {current_question}
MULTIPLE TABLES AVAILABLE: {has_multiple_tables}
JOIN INFORMATION: {join_clause if join_clause else "No join clause provided"}
MANDATORY FILTER COLUMNS: {mandatory_columns_text}

FILTER VALUES EXTRACTED:
{filter_context_text}

AVAILABLE METADATA: {dataset_metadata}

==============================
PRE-ASSESSMENT VALIDATION
==============================

Before starting Task 1, perform these mandatory checks:

**CHECK 1: Extract ALL user-mentioned terms**
Identify every attribute, metric, filter, and dimension term in the question.
List: [term1, term2, term3...]

**CHECK 2: Validate against metadata**
For EACH term, check if it maps to columns in AVAILABLE METADATA:
- Exact match: "carrier_id" → carrier_id → ✓ Found (carrier_id)
- Fuzzy match: "carrier" → carrier_id, "state" → state_name → ✓ Found (column_name). Note carrier is not client_id
- No match: "xyz" with no similar column → ❌ Not Found
- Multiple matches: "region" could be state/territory/district → ⚠️ Ambiguous (col1, col2)

Mark: ✓ Found (col_name) | ❌ Not Found | ⚠️ Ambiguous (col1, col2)

**CHECK 3: Filter context validation**
Check if user's question has a filter value WITHOUT an attribute name (e.g., "MPDOVA" but not "carrier_id MPDOVA").
If yes, check FILTER VALUES EXTRACTED:
  a) Does the filter value EXACTLY match (not partial) what's in the user's question?
  b) Does the column name exist in AVAILABLE METADATA?
- If BOTH pass → ✓Valid (use this column for filtering)
- If ONLY partial match → ❌Mark for follow-up
- If exact match but column not in metadata → ❌Mark for follow-up
- If filter value not mentioned in question → Skip (don't use this filter)

**CHECK 4: Clarification rules validation**
Check if selected dataset has "clarification_rules" field in metadata.
If present, evaluate user's question against each rule:
- Does question trigger any rule? → ❌ Rule triggered (needs clarification)
- No rules triggered? → ✓ No rules apply

Output: ✓ No rules | ❌ Rule: [brief rule description]

{check_5_text}

**Output Format:**
Terms: [list]
Validation: term1(✓col_name) | term2(❌not found) | term3(⚠️col1,col2)
Filter Context: ✓Valid (column_name) | ❌Partial match | ❌Column missing | N/A
Clarification Rules: [status from CHECK 4]
Historical SQL: [status from CHECK 5]

==============================
TASK 1: STRICT ASSESSMENT
==============================

Analyze clarity using STRICT criteria. Each area must pass for SQL generation.

**A. TEMPORAL SCOPE**
If question mentions specific dates/periods:
- Past dates (before Oct 2025) → ✓ Valid
- Current/recent dates (2025 year-to-date) → ✓ Valid  
- Near-future dates (within 12 months) → ✓ Valid (forecast context)
- Far-future dates (beyond Nov 2026) → ❌ Clarify intent

**B. METRIC DEFINITIONS** - Calculation Method Clarity
Scope: Only numeric metrics requiring aggregation/calculation
✓ = All metrics have clear, standard calculation methods (SUM/COUNT/AVG/MAX/MIN)
❌ = Any metric requires custom formula not specified OR calculation method ambiguous
⚠️ = Metric exists but needs confirmation
N/A = No metrics/calculations needed

**C. BUSINESS CONTEXT**
✓ = Filtering criteria clear AND grouping dimensions explicit
❌ = Missing critical context ("top" by what?, "compare" to what?, "by region" which level?)
⚠️ = Partially clear but confirmation recommended

**D. FORMULA & CALCULATION REQUIREMENTS**
✓ = Standard SQL aggregations sufficient
❌ = Requires custom formulas without clear definition
N/A = No calculations needed

**E. METADATA MAPPING** - Column Existence Validation
✓ = ALL terms from CHECK 2 are ✓ (found with exact or fuzzy match)
❌ = ANY term from CHECK 2 is ❌ (not found) or ⚠️ (ambiguous)

Use CHECK 2 validation results directly. No additional examples needed.

**F. QUERY STRATEGY**
✓ = Clear if single/multi query or join needed
❌ = Multi-table approach unclear

**G. DATASET CLARIFICATION RULES**
✓ = No clarification rules triggered OR rules don't apply to question
❌ = Clarification rule triggered (rule indicates missing specification or unsupported request)

Use CHECK 4 validation result directly.

==============================
ASSESSMENT OUTPUT FORMAT
==============================

**PRE-VALIDATION:**
Terms: [list]
Validation: [statuses]
Filter Context: [status]
Clarification Rules: [status]
Historical SQL: [status]

**ASSESSMENT**: 
A: ✓/❌/N/A (max 5 words)
B: ✓/❌/⚠️/N/A (max 5 words)
C: ✓/❌/⚠️ (max 5 words)
D: ✓/❌/N/A (max 5 words)
E: ✓/❌ (list failed mappings if any)
F: ✓/❌ (max 5 words)
G: ✓/❌ (rule description if triggered)

**DECISION**: PROCEED | FOLLOW-UP

==============================
STRICT DECISION CRITERIA
==============================

**MUST PROCEED only if:**
ALL areas (A, B, C, D, E, F, G) = ✓ or N/A with NO ❌ and NO blocking ⚠️

**MUST FOLLOW-UP if:**
ANY single area = ❌ OR any ⚠️ that affects SQL accuracy

**Critical Rule: ONE failure = STOP. Do not generate SQL with any uncertainty.**

==============================
FOLLOW-UP GENERATION
==============================

**Priority Order:** G (Clarification Rules) → E (Metadata) → B (Metrics) → C (Context) → A/D/F

Address ALL missing/unclear items from the assessment. List issues in priority order.

<followup>
I need clarification to generate accurate SQL:

**[What's Missing/Unclear]**: [Describe the issue - e.g., "Dataset requires forecast cycle" or "Cannot find column 'state_code'"]
- **Please specify**: [What you need from user]
- **Available options** (if applicable): [List from metadata]

[Additional issues as needed - cover all failures from A-G]

*You can also ask a different question if you prefer.*
</followup>

{history_section}

==============================================
TASK 2: HIGH-QUALITY DATABRICKS SQL GENERATION 
==============================================

(Only execute if Task 1 DECISION says "PROCEED")

**CORE SQL GENERATION RULES:**

[YOU WILL PASTE YOUR SQL GENERATION RULES HERE]

==============================
OUTPUT FORMATS
==============================

Return ONLY the result in XML tags with no additional text.

**SINGLE SQL QUERY:**
<sql>
[Your complete SQL query]
</sql>

**MULTIPLE SQL QUERIES:**
<multiple_sql>
<query1_title>[Title - max 8 words]</query1_title>
<query1>[SQL query]</query1>
<query2_title>[Title - max 8 words]</query2_title>
<query2>[SQL query]</query2>
</multiple_sql>

FOR ANY FOLLOW-UP SITUATION:
<followup>
I need clarification to generate accurate SQL:

**[Specific issue from unclear area]**: [Direct question in one sentence]
- Available data: [specific column names from metadata]
- Suggested approach: [concrete calculation option]

**[Second issue if needed]**: [Second direct question in one sentence only if multiple areas unclear]
- Available data: [relevant columns]
- Alternative: [another option]

Please clarify these points.
</followup>


==============================
EXECUTION INSTRUCTION
==============================

1. Complete PRE-VALIDATION (extract and validate all terms + check clarification rules + check historical SQL)
2. Complete TASK 1 strict assessment (A-G with clear marks)
3. Apply STRICT decision: ANY ❌ or blocking ⚠️ = FOLLOW-UP
4. If PROCEED: Execute TASK 2 with SQL generation (learn from historical SQL if available)
5. If FOLLOW-UP: Ask targeted questions (max 2, prioritize G → E → B → C)

**Show your work**: Display pre-validation, assessment, then SQL or follow-up.
**Remember**: ONE failure = STOP.
"""

    for attempt in range(self.max_retries):
        try:
            llm_response = await self.db_client.call_claude_api_endpoint_async([
                {"role": "user", "content": assessment_prompt}
            ])
            print('sql llm response', llm_response)
            
            # Extract SQL or follow-up questions
            # Check for multiple SQL queries first
            multiple_sql_match = re.search(r'<multiple_sql>(.*?)</multiple_sql>', llm_response, re.DOTALL)
            if multiple_sql_match:
                multiple_content = multiple_sql_match.group(1).strip()
                
                # Extract individual queries with titles
                query_matches = re.findall(r'<query(\d+)_title>(.*?)</query\1_title>.*?<query\1>(.*?)</query\1>', multiple_content, re.DOTALL)
                if query_matches:
                    sql_queries = []
                    query_titles = []
                    for i, (query_num, title, query) in enumerate(query_matches):
                        cleaned_query = query.strip().replace('`', '')
                        cleaned_title = title.strip()
                        if cleaned_query and cleaned_title:
                            sql_queries.append(cleaned_query)
                            query_titles.append(cleaned_title)
                    
                    if sql_queries:
                        return {
                            'success': True,
                            'multiple_sql': True,
                            'sql_queries': sql_queries,
                            'query_titles': query_titles,
                            'query_count': len(sql_queries),
                            'used_history_asset': has_history  # NEW FIELD
                        }
                
                raise ValueError("Empty or invalid multiple SQL queries in XML response")
            
            # Check for single SQL query
            sql_match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
            if sql_match:
                sql_query = sql_match.group(1).strip()
                sql_query = sql_query.replace('`', '')  # Remove backticks
                
                if not sql_query:
                    raise ValueError("Empty SQL query in XML response")
                
                return {
                    'success': True,
                    'multiple_sql': False,
                    'sql_query': sql_query,
                    'used_history_asset': has_history  # NEW FIELD
                }
            
            # Check for follow-up questions
            followup_match = re.search(r'<followup>(.*?)</followup>', llm_response, re.DOTALL)
            if followup_match:
                followup_text = followup_match.group(1).strip()
                
                if not followup_text:
                    raise ValueError("Empty follow-up questions in XML response")
                
                # Set is_sql_followup to True when asking follow-up questions
                state['is_sql_followup'] = True
                
                return {
                    'success': True,
                    'needs_followup': True,
                    'sql_followup_questions': followup_text,
                    'used_history_asset': False  # NEW FIELD - Not applicable for follow-up
                }
            
            # Neither SQL nor follow-up found
            raise ValueError("No SQL or follow-up questions found in response")
        
        except Exception as e:
            print(f"❌ SQL assessment attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < self.max_retries - 1:
                print(f"🔄 Retrying SQL assessment... (Attempt {attempt + 1}/{self.max_retries})")
                await asyncio.sleep(2 ** attempt)
    
    return {
        'success': False,
        'error': f"SQL assessment failed after {self.max_retries} attempts due to Model errors",
        'used_history_asset': False  # NEW FIELD
    }
