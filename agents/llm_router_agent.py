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

**IMPORTANT**: If you successfully use this historical SQL as a template, set history_sql_used = true in your response. If you generate SQL from scratch without using historical patterns, set history_sql_used = false.

====================================================

"""
    else:
        history_section = """
=== HISTORICAL SQL ===
Not available

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

{history_section}

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
<history_sql_used>[true or false]</history_sql_used>

**MULTIPLE SQL QUERIES:**
<multiple_sql>
<query1_title>[Title - max 8 words]</query1_title>
<query1>[SQL query]</query1>
<query2_title>[Title - max 8 words]</query2_title>
<query2>[SQL query]</query2>
</multiple_sql>
<history_sql_used>[true or false]</history_sql_used>

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

**HISTORY_SQL_USED FLAG RULES:**
- If historical SQL was available AND you used its structure/patterns → true
- If historical SQL was available BUT you generated from scratch → false
- If historical SQL was not available → false


==============================
EXECUTION INSTRUCTION
==============================

1. Complete PRE-VALIDATION (extract and validate all terms + check clarification rules + check historical SQL)
2. Complete TASK 1 strict assessment (A-G with clear marks)
3. Apply STRICT decision: ANY ❌ or blocking ⚠️ = FOLLOW-UP
4. If PROCEED: Execute TASK 2 with SQL generation (learn from historical SQL if available)
5. If FOLLOW-UP: Ask targeted questions (max 2, prioritize G → E → B → C)
6. Always include history_sql_used flag in output (true/false)

**Show your work**: Display pre-validation, assessment, then SQL or follow-up.
**Remember**: ONE failure = STOP.
"""

    for attempt in range(self.max_retries):
        try:
            llm_response = await self.db_client.call_claude_api_endpoint_async([
                {"role": "user", "content": assessment_prompt}
            ])
            print('sql llm response', llm_response)
            
            # NEW: Extract history_sql_used flag
            history_sql_used = False
            history_used_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', llm_response, re.IGNORECASE | re.DOTALL)
            if history_used_match:
                history_sql_used = history_used_match.group(1).strip().lower() == 'true'
            
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
                            'used_history_asset': has_history,
                            'history_sql_used': history_sql_used  # NEW: LLM's flag
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
                    'used_history_asset': has_history,
                    'history_sql_used': history_sql_used  # NEW: LLM's flag
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
                    'used_history_asset': False,
                    'history_sql_used': False  # NEW: Not applicable for follow-up
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
        'used_history_asset': False,
        'history_sql_used': False  # NEW FIELD
    }
