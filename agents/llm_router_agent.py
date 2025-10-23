async def _llm_dataset_selection(self, search_results: List[Dict], state: AgentState, filter_metadata_results: List[str] = None) -> Dict:
        """Enhanced LLM selection with validation, disambiguation handling, and filter-based selection"""
        
        user_question = state.get('current_question', state.get('original_question', ''))
        filter_values = state.get('filter_values', [])
        
        # Format filter metadata results for the prompt
        filter_metadata_text = ""
        if filter_metadata_results:
            filter_metadata_text = "\n**FILTER METADATA FOUND:**\n"
            for result in filter_metadata_results:
                filter_metadata_text += f"- {result}\n"
        else:
            filter_metadata_text = "\n**FILTER METADATA:** No specific filter values found in metadata.\n"

        selection_prompt = f"""
            You are a Dataset Identifier Agent. You have FIVE sequential tasks to complete.

CURRENT QUESTION: {user_question}

EXTRACTED COLUMNS WITH FILTER VALUES: {filter_values}
{filter_metadata_text}

AVAILABLE DATASETS: {search_results}

A. **PHI/PII SECURITY CHECK**:
- Examine each dataset's "PHI_PII_Columns" field
- Check if user's question requests PHI/PII (SSN, member IDs, personal identifiers, patient names, addresses, etc.)
- If PHI/PII columns requested, IMMEDIATELY return phi_found status (do not proceed)

B. **METRICS & ATTRIBUTES CHECK**:
- Extract requested metrics/measures and attributes/dimensions
- Apply smart mapping:

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

**EXPLICIT ATTRIBUTE DETECTION**:
- Scan user's question for explicit attribute keywords: "carrier", "drug", "pharmacy", "therapy", "client", "manufacturer" (case-insensitive)
- Flag as "explicit_attribute_mentioned = true/false"
- Store mapped column names for later filter disambiguation check

C. **KEYWORD & SUITABILITY ANALYSIS**:
- **KEYWORD MATCHING**: Domain keywords indicate preferences:
* "claim/claims" → claim_transaction dataset
* "forecast/budget" → actuals_vs_forecast dataset  
* "ledger" → actuals_vs_forecast dataset

- **SUITABILITY VALIDATION (HARD CONSTRAINTS)**:
* **BLOCKING**: "not_useful_for" match → IMMEDIATELY EXCLUDE dataset (overrides all other factors)
* **POSITIVE**: "useful_for" match → PREFER dataset
* Example: "top 10 clients by expense" → Ledger "not_useful_for: client level expense" → EXCLUDE

- Verify time_grains match user needs (daily vs monthly vs quarterly)

D. **COMPLEMENTARY ANALYSIS CHECK**:
- Single dataset with ALL metrics + attributes → SELECT IT
- No single complete → SELECT MULTIPLE if: metric in A + breakdown dimension in B, or cross-dataset comparison needed
- Example: "ledger revenue breakdown by drug" → Both (ledger revenue + claims drug_name)
- Ask clarification only when: Same data in multiple datasets with conflicting contexts

E. **DATASET CLARIFICATION RULES CHECK**:
- After identifying candidate dataset(s), check if any have "clarification_rules" field
- Evaluate user's question against each rule (rules are plain text business logic)
- If ANY rule applies to the user's question → STOP and return "needs_clarification" status
- Common patterns: missing specification (forecast cycle), unsupported granularity (client-level)
- Only proceed to dataset selection if NO rules are triggered

F. **FINAL DECISION LOGIC**:
- **STEP 1**: Check results from sections A through E
- **STEP 2**: MANDATORY Decision order:
* **FIRST**: Apply suitability constraints - eliminate "not_useful_for" matches
* **SECOND**: Validate complete coverage (metrics + attributes) on remaining datasets
* **THIRD**: Single complete dataset → CHECK clarification rules (Section E) → SELECT if no rules triggered

* **SMART FILTER DISAMBIGUATION CHECK**:
**STEP 3A**: Check if filter values exist in multiple columns
**STEP 3B**: Determine if follow-up is needed using this logic:

1. **Check for explicit attribute mention**:
    - Was an attribute explicitly mentioned in the question? (from Section B detection)
    - Examples: "Carrier", "drug", "pharmacy", "therapy", "client", "manufacturer"

2. **Count matching columns in selected dataset**:
    - From filter metadata, identify all columns containing the filter value
    - Filter to only columns that exist in the selected dataset's attributes
    - Store as: matching_columns_count

3. **DECISION TREE**:
    - IF explicit_attribute_mentioned = true → NO FOLLOW-UP (trust user's specification)
    - IF explicit_attribute_mentioned = false:
        * IF matching_columns_count = 1 → NO FOLLOW-UP (obvious choice)
        * IF matching_columns_count > 1 → RETURN "needs_disambiguation"

**Examples**:
- "Carrier MPDOVA billed amount" + MPDOVA in [carrier_name, carrier_id, plan_name] → ✓ NO follow-up (user said "Carrier")
- "MPDOVA billed amount" + MPDOVA in [carrier_name] only → ✓ NO follow-up (only 1 match)
- "covid vaccine billed amount" + covid vaccine in [drug_name, pharmacy_name, therapy_class_name] → ❌ ASK which column (no explicit attribute, 3 matches)

* **FOURTH**: No single complete → SELECT MULTIPLE if complementary
* **FIFTH**: Multiple complete → Use tie-breakers (keywords, high_level_table)
* **SIXTH**: Still tied → RETURN "needs_disambiguation"
* **LAST**: No coverage OR unresolvable ambiguity → Report as missing items or request clarification

**HIGH LEVEL TABLE TIE-BREAKER** (only when multiple datasets have ALL required metrics + attributes):
- High-level questions ("total revenue", "overall costs") → prefer "high_level_table": "True"
- Breakdown questions ("revenue by drug", "costs by client") → prefer granular tables
- NEVER use as tie-breaker if dataset missing required attributes

==============================
DECISION CRITERIA
==============================

**PHI_FOUND** IF:
- User question requests or implies access to PHI/PII columns
- Must be checked FIRST before other validations

**NEEDS_CLARIFICATION** IF:
- Dataset identified successfully BUT its "clarification_rules" are triggered by user's question
- Rule indicates missing specification or unsupported request

**PROCEED** (SELECT DATASET) IF:
- Dataset passes suitability validation AND all metrics/attributes have Tier 1-3 matches AND clear selection AND no clarification rules triggered
- Single dataset meets all requirements after all checks
- Complementary datasets identified for complete coverage

**MISSING_ITEMS** IF:
- Required metrics/attributes don't have Tier 1-3 matches in any dataset
- No suitable alternatives available

**NEEDS_DISAMBIGUATION** IF:
- **PRIORITY 1**: Multiple datasets, no clear preference → ask which table
- **PRIORITY 2**: Filter value in 2+ columns, no explicit attribute mentioned → ask which column

==============================
ASSESSMENT FORMAT (BRIEF)
==============================

**ASSESSMENT**: A:✓(no PHI) B:✓(metrics found) B-ATTR:✓(explicit attr OR single column) C:✓(suitability passed) D:✓(complementary) E:✓(no rules triggered) F:✓(clear selection)
**DECISION**: PROCEED - [One sentence reasoning]

Keep assessment ultra-brief:
- Use checkmarks (✓) or X marks (❌) with 10 words max explanation in parentheses
- **E**: ✓ means no clarification rules triggered, ❌ means rule applies
- **B-ATTR**: ✓ means explicit attribute OR only 1 column match, ❌ means no explicit attr AND multiple columns
- **C (suitability)**: ✓ means no "not_useful_for" conflicts, ❌ means blocked by suitability
- Each area gets: "A:✓(brief reason)" or "A:❌(brief issue)"
- Decision reasoning maximum 15 words
- Save detailed analysis for JSON selection_reasoning field

=======================
RESPONSE FORMAT
=======================

IMPORTANT: Keep assessment ultra-brief (1-2 lines max), then output ONLY the JSON wrapped in <json> tags.

"status": "phi_found" | "success" | "missing_items" | "needs_disambiguation" | "needs_clarification",
"final_actual_tables": ["table_name_1","table_name2"] if status = success else [],
"functional_names": ["functional_name"] if status = success else [],
"tables_identified_for_clarification": ["table_1", "table_2"] if status = needs_disambiguation OR needs_clarification else [],
"requires_clarification": true if status = needs_disambiguation OR needs_clarification else false,
"selection_reasoning": "2-3 lines max explanation",
"high_level_table_selected": true/false if status = success else null,
"user_message": "message to user" if status = phi_found or missing_items else null,
"clarification_question": "question to user" if status = needs_disambiguation OR needs_clarification else null,
"selected_filter_context": "col name - [actual_column_name], sample values [all values from filter extract]" if column selected from filter context else null

**FIELD POPULATION RULES**:
- needs_disambiguation: populate tables_identified_for_clarification with all candidate tables (Priority 1) or single table (Priority 2)
- needs_clarification: populate tables_identified_for_clarification with rule-triggering table, set requires_clarification=true, final_actual_tables=[]

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
                        'functional_names': selection_result.get('functional_names', []),
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
                    if high_level_selected:
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
- Fuzzy match: "carrier" → carrier_id, "state" → state_name → ✓ Found (column_name)
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

**Output Format:**
Terms: [list]
Validation: term1(✓col_name) | term2(❌not found) | term3(⚠️col1,col2)
Filter Context: ✓Valid (column_name) | ❌Partial match | ❌Column missing | N/A

==============================
TASK 1: STRICT ASSESSMENT
==============================

Analyze clarity using STRICT criteria. Each area must pass for SQL generation.

**A. TEMPORAL SCOPE**
✓ = Time period clearly specified
❌ = Time period needed but missing
N/A = No time dimension needed

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

==============================
ASSESSMENT OUTPUT FORMAT
==============================

**PRE-VALIDATION:**
Terms: [list]
Validation: [statuses]
Filter Context: [status]

**ASSESSMENT**: 
A: ✓/❌/N/A (max 5 words)
B: ✓/❌/⚠️/N/A (max 5 words)
C: ✓/❌/⚠️ (max 5 words)
D: ✓/❌/N/A (max 5 words)
E: ✓/❌ (list failed mappings if any)
F: ✓/❌ (max 5 words)

**DECISION**: PROCEED | FOLLOW-UP

==============================
STRICT DECISION CRITERIA
==============================

**MUST PROCEED only if:**
ALL areas (A, B, C, D, E, F) = ✓ or N/A with NO ❌ and NO blocking ⚠️

**MUST FOLLOW-UP if:**
ANY single area = ❌ OR any ⚠️ that affects SQL accuracy

**Critical Rule: ONE failure = STOP. Do not generate SQL with any uncertainty.**

==============================
FOLLOW-UP GENERATION
==============================

**Priority Order:** E (Metadata) → B (Metrics) → C (Context) → A/D/F
**Maximum 2 questions** - pick most critical blocking issues

**Generic Format for ANY failure:**
<followup>
I need clarification to generate accurate SQL:

**[What's Missing/Unclear]**: [Specific term or concept that failed]
- Available in metadata: [list actual column names or options]
- Please clarify: [what user needs to specify]

[Second issue only if needed - max 2 total]
</followup>

**Example:**
**Missing Column**: I cannot find "state_code" in the available data.
- Available in metadata: state_name, state_abbr
- Please clarify: Which column should I use?

==============================================
TASK 2: HIGH-QUALITY DATABRICKS SQL GENERATION 
==============================================

(Only execute if Task 1 DECISION says "PROCEED")

**CORE SQL GENERATION RULES:**

==============================================
TASK 2: HIGH-QUALITY DATABRICKS SQL GENERATION 
==============================================

(Only execute if Task 1 says "PROCEED")

**CORE SQL GENERATION RULES:**

1. MANDATORY FILTERS - ALWAYS APPLY
- Review MANDATORY FILTER COLUMNS section - any marked MANDATORY must be in WHERE clause

2. FILTER VALUES EXTRACTED - USE VALIDATED FILTERS
**Rule**: If PRE-VALIDATION marked Filter Context as ✓Valid (column_name):
- Apply exact match filter: WHERE UPPER(column_name) = UPPER('VALUE')
- For multiple values use IN: WHERE UPPER(column_name) IN (UPPER('VAL1'), UPPER('VAL2'))

The validation was already done in CHECK 3. Only use filters marked as ✓Valid

3. CALCULATED FORMULAS HANDLING (CRITICAL)
**When calculating derived metrics (Gross Margin, Cost %, Margin %, etc.), DO NOT group by metric_type:**

CORRECT PATTERN:
```sql
SELECT 
    ledger, year, month,  -- Business dimensions only
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount_or_count ELSE 0 END) AS revenues,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS Post Reclass') THEN amount_or_count ELSE 0 END) AS expense_cogs,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount_or_count ELSE 0 END) - 
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS Post Reclass') THEN amount_or_count ELSE 0 END) AS gross_margin
FROM table
WHERE conditions AND UPPER(metric_type) IN (UPPER('Revenues'), UPPER('COGS Post Reclass'))
GROUP BY ledger, year, month  -- Group by dimensions, NOT metric_type
```

WRONG PATTERN:
```sql
GROUP BY ledger, metric_type  -- Creates separate rows per metric_type, breaks formulas
```

**Only group by metric_type when user explicitly asks to see individual metric types as separate rows.**

4. METRICS & AGGREGATIONS
- Always use appropriate aggregation functions for numeric metrics: SUM, COUNT, AVG, MAX, MIN
- Even with specific entity filters (invoice #123, member ID 456), always aggregate unless user asks for "line items" or "individual records"
- Include time dimensions (month, quarter, year) when relevant to question
- Use business-friendly dimension names (therapeutic_class, service_type, age_group, state_name)

5. SELECT CLAUSE STRATEGY

**Simple Aggregates (no breakdown requested):**
- Show only the aggregated metric and essential time dimensions if specified
- Example: "What is total revenue?" → SELECT SUM(revenue) AS total_revenue
- Do NOT include unnecessary business dimensions or filter columns

**Calculations & Breakdowns (analysis BY dimensions):**
- Include ALL columns used in WHERE, GROUP BY, and calculations when relevant to question
- For calculations, show all components for transparency:
  * Percentage: Include numerator + denominator + percentage
  * Variance: Include original values + variance
  * Ratios: Include both components + ratio
- Example: "Cost per member by state" → SELECT state_name, total_cost, member_count, cost_per_member

6. MULTI-TABLE JOIN SYNTAX (when applicable)
- Use provided join clause exactly as specified
- Qualify all columns with table aliases
- Include all necessary tables in FROM/JOIN clauses
- Only join if question requires related data together; otherwise use separate queries

7. ATTRIBUTE-ONLY QUERIES
- If question asks only about attributes (age, name, type) without metrics, return relevant columns without aggregation

8. STRING FILTERING - CASE INSENSITIVE
- Always use UPPER() on both sides for text/string comparisons
- Example: WHERE UPPER(product_category) = UPPER('Specialty')

9. TOP N/BOTTOM N QUERIES WITH CONTEXT
-Show requested top/bottom N records with their individual values
-CRITICAL: Include the overall total as an additional COLUMN in each row (not as a separate row)
-Calculate and show percentage contribution: (individual value / overall total) × 100
Overall totals logic:
    -✅ Include overall total column for summable metrics: revenue, cost, expense, amount, count, volume, scripts, quantity, spend
    -❌ Exclude overall total column for derived metrics: margin %, ratios, rates, per-unit calculations, averages
-Use subquery in SELECT to show overall total alongside each individual record
-Column structure: [dimension] | [individual_value] | [overall_total] | [percentage_contribution]
-ALWAYS filter out blank/null records: WHERE column_name NOT IN ('-', 'BL')

10. COMPARISON QUERIES - SIDE-BY-SIDE FORMAT
- When comparing two related metrics (actual vs forecast, budget vs actual), use side-by-side columns
- For time-based comparisons (month-over-month, year-over-year), display time periods as adjacent columns with clear month/period names
- Example: Display "January_Revenue", "February_Revenue", "March_Revenue" side by side for easy comparison
- Include variance/difference columns when comparing metrics
- Prevents users from manually comparing separate rows

11. DATABRICKS SQL COMPATIBILITY
- Standard SQL functions: SUM, COUNT, AVG, MAX, MIN
- Date functions: date_trunc(), year(), month(), quarter()
- Conditional logic: CASE WHEN
- CTEs: WITH clauses for complex logic

12. FORMATTING & ORDERING
- Show whole numbers for metrics, round percentages to 4 decimal places
- Use ORDER BY only for date columns in descending order
- Use meaningful, business-relevant column names aligned with user's question

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

**FOLLOW-UP REQUEST:**
<followup>
[Use format above]
</followup>

==============================
EXECUTION INSTRUCTION
==============================

1. Complete PRE-VALIDATION (extract and validate all terms)
2. Complete TASK 1 strict assessment (A-F with clear marks)
3. Apply STRICT decision: ANY ❌ or blocking ⚠️ = FOLLOW-UP
4. If PROCEED: Execute TASK 2 with SQL generation
5. If FOLLOW-UP: Ask targeted questions (max 2, prioritize E → B → C)

**Show your work**: Display pre-validation, assessment, then SQL or follow-up.
**Remember**: ONE failure = STOP.
"""

