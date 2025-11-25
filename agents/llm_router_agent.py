async def _assess_and_generate_sql_async(self, context: Dict, state: Dict) -> Dict[str, Any]:
        """SQL generation with optional historical learning context"""
        
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        join_clause = state.get('join_clause', '')
        selected_filter_context = context.get('selected_filter_context')
        
        # Get selected datasets for table filtering
        selected_datasets = state.get('selected_dataset', [])
        
        # NEW: Search for historical SQL feedback BEFORE generating SQL
        # This ensures we only search for SQL from the selected dataset(s)
        print(f"🔍 Searching feedback SQL embeddings for selected dataset(s): {selected_datasets}")
        feedback_results = await self.db_client.search_feedback_sql_embeddings(current_question, table_names=selected_datasets)
        
        # Process feedback results with LLM selection
        matched_sql = ''
        history_question_match = ''
        matched_table_name = ''
        
        if feedback_results:
            print(f"🤖 Analyzing {len(feedback_results)} feedback SQL candidates from selected dataset(s)...")
            feedback_selection_result = await self.db_client._llm_feedback_selection(feedback_results, state)
            
            if feedback_selection_result.get('status') == 'match_found':
                # Extract seq_id from the LLM selection result
                matched_seq_id = feedback_selection_result.get('seq_id')
                
                # Filter feedback results to find the matching record
                matched_record = None
                for result in feedback_results:
                    if result.get('seq_id') == matched_seq_id:
                        matched_record = result
                        break
                
                if matched_record:
                    # Extract matched results
                    history_question_match = matched_record.get('user_question', '')
                    matched_sql = matched_record.get('sql_query', '')
                    matched_table_name = matched_record.get('table_name', '')
                    
                    # Store in state for consistency
                    state['history_question_match'] = history_question_match
                    state['matched_sql'] = matched_sql
                    state['matched_table_name'] = matched_table_name
                    
                    print(f"✅ Feedback match found from {matched_table_name}")
                    print(f"   Matched question: {history_question_match[:100]}...")
                else:
                    print(f"⚠️ Matched seq_id {matched_seq_id} not found in results")
            else:
                print(f"ℹ️ No suitable feedback SQL match found (status: {feedback_selection_result.get('status')})")
        else:
            print(f"ℹ️ No feedback SQL embeddings found for selected dataset(s)")
        
        # Check if history exists and is relevant
        has_history = bool(matched_sql and history_question_match and matched_table_name)
        
        # Check if we have multiple tables
        selected_datasets = state.get('selected_dataset', [])

        # Define mandatory column mapping
        mandatory_column_mapping = {
            "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast": [
                "Ledger"
            ],"prd_optumrx_orxfdmprdsa.rag.pbm_claims": [
                "product_category='PBM'"
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
            check_5_text = "**CHECK 5: Historical SQL availability**: ✓ Available (learning template)"
            history_section = f"""
=== HISTORICAL SQL REFERENCE ===
Previous: "{history_question_match}" | Table: {matched_table_name}

<historical_sql>
{matched_sql}
</historical_sql>

**STEP 1: QUESTION ALIGNMENT CHECK**
Current asks for: [Interpret current question's grouping needs]
Historical had: [Observe historical GROUP BY dimensions]

**INHERIT DECISIONS:**
A. **DIMENSIONS/GROUP BY**: 
   ✅ INHERIT if: Questions ask for same analysis type (e.g., both ask "by product category")
   ❌ DON'T INHERIT if: Different analysis requested (e.g., current asks "by carrier" vs historical "by product")
   
B. **STRUCTURAL PATTERNS** (ALWAYS PRESERVE):
   - Side-by-side comparisons (CASE WHEN for periods)
   - UNION/UNION ALL patterns (detail + OVERALL_TOTAL rows)
   - CTE/subquery architecture
   - Calculation methods (ROUND, NULLIF, percentages)
   - Window functions structure

**STEP 2: SELECTIVE LEARNING**

✅ **ALWAYS LEARN** (Universal Patterns):
- CASE WHEN for side-by-side columns (august_revenue, september_revenue)
- Aggregation placement (SUM/COUNT/AVG)
- Division safety (NULLIF patterns)
- UPPER() for case-insensitive filters
- ROUND(amounts,0), ROUND(percentages,3)

⚡ **CONDITIONALLY INHERIT** (Check Alignment First):
- GROUP BY dimensions → Only if questions ask same granularity
- SELECT columns → Match if same analysis type
- JOIN patterns → Use if same tables needed

❌ **NEVER COPY** (Always From Current):
- Filter values (dates, carrier_id, entities)
- <parameter> placeholders → use actual values
- Specific time periods → use current question's dates

**STEP 3: VALIDATION**
1. Verify columns exist in AVAILABLE METADATA
2. Add MANDATORY FILTER COLUMNS if missing
3. Apply ✓Valid filters from current question

**DECISION LOGIC:**
IF current_question_pattern == historical_pattern:
    → Inherit dimensions + structure
    → Set history_sql_used = true
ELSE:
    → Learn only structural patterns (CASE WHEN, UNION)
    → Build GROUP BY from current question
    → Set history_sql_used = partial

**CRITICAL**: Structural patterns (side-by-side) are proven UX patterns - preserve these even when dimensions differ.

    """
        else:
            history_section = """
    === HISTORICAL SQL ===
    Not available

    """
        assessment_prompt = f"""
⚠️ IMPORTANT CONTEXT - READ THIS FIRST ⚠️
You are a Databricks SQL code generator. Your task is to analyze user questions and generate SQL queries.


CURRENT QUESTION: {current_question}
AVAILABLE METADATA: {dataset_metadata}
MANDATORY FILTER COLUMNS: {mandatory_columns_text}
FILTER VALUES EXTRACTED: {filter_context_text}
JOIN INFORMATION: {join_clause if join_clause else "No joins needed"}

{history_section} 

PHASE 1: PRE-VALIDATION PIPELINE

Execute these checks sequentially. Document each finding.

**CHECK 1: Term Extraction & Mapping**
Extract EVERY business term from the question and map to metadata:

Examples:
- "revenue by carrier for August" → Extract: [revenue, carrier, August]
- "top 5 products with highest margin" → Extract: [products, margin, top 5]
- "MPDOVA claims in Q3" → Extract: [MPDOVA, claims, Q3]

For each term, validate against AVAILABLE METADATA:
- Exact match: "carrier" → carrier_id → ✓Found(carrier_id)
- Fuzzy match: "revenue" → revenue_amount → ✓Found(revenue_amount)  
- No match: "profitability" → (no profit columns) → ✗Not Found
- Ambiguous: "amount" → (revenue_amount, expense_amount) → ⚠Ambiguous(revenue_amount, expense_amount)

Output:
Terms: [revenue, carrier, August]
Mapping: revenue(✓revenue_amount) | carrier(✓carrier_id) | August(✓month)

**CHECK 2: Mandatory Filter Validation** ⚠️ CRITICAL
Mandatory filters MUST be in WHERE clause or query fails.

Check: {mandatory_columns_text}
- If "ledger: MANDATORY" → SQL MUST have "WHERE ledger = ..."
- If "data_source: MANDATORY" → SQL MUST have "WHERE data_source = ..."

Status: ✓Ready (will add to WHERE) | ✗STOP (mandatory column not in metadata)

**CHECK 3: Filter Context Validation**
When user mentions values without attributes (e.g., "MPDOVA", "2024", "Specialty"):

Process:
1. Is value in user's question? → If no, skip this filter
2. Is it in FILTER VALUES EXTRACTED with exact match? → Check
3. Does the mapped column exist in metadata? → Verify

Examples:
- Question has "MPDOVA", Filter shows "carrier_id: MPDOVA", carrier_id in metadata → ✓Use(carrier_id='MPDOVA')

Status: ✓Valid([column=value pairs]) | ✗Invalid | N/A

**CHECK 4: Clarity Assessment**

A. **Temporal Clarity**
- Specific dates (past/present/future): "August 2024", "Q3 2025", "Jan 2026" → ✓Clear
- Relative dates: "last month", "YTD", "next quarter" → ✓Clear
- Vague: "recently", "a while ago", "soon" → ✗Unclear

B. **Metric Clarity**
- Standard aggregations: "total revenue", "average cost" → ✓Clear(SUM/AVG)
- Known calculations: "margin = revenue - cost" → ✓Clear
- Undefined: "efficiency score", "performance index" → ✗Unclear formula

C. **Grouping/Filtering Clarity**
- Explicit: "by product category and month" → ✓Clear
- Vague: "top products" (by what metric?) → ✗Unclear
- Missing: "compare regions" (which regions?) → ✗Unclear

D. **Multiple Value Intent**
When user lists multiple specific values (HDP, SP or drug1, drug2):
- Multiple values mentioned → Need breakdown by each → ✓Show individually
- "total for HDP and SP" → Aggregate together → ✓Clear intent
- "compare HDP vs SP" → Show side-by-side → ✓Clear intent

Examples:
- "revenue for HDP, SP" → SELECT category, SUM(revenue) GROUP BY category
- "total revenue for HDP and SP" → WHERE category IN ('HDP','SP') [no GROUP BY category]
- "HDP vs SP comparison" → Separate columns for each

**CHECK 5: {check_5_text} **

==========================
VALIDATION DECISION GATE
==========================
⚠️ OUTPUT STARTS HERE - DO NOT SHOW PRE-VALIDATION PIPELINE ⚠️

**VALIDATION OUTPUT:**
□ CHECK 1 Mapping: (✓Revenues) | External LOB(✓line_of_business) | HDP(✓product_category)| July 2025(✓month, year)
□ CHECK 2 Mandatory: [✓Ready | ✗STOP]
□ CHECK 3 Filters: [✓Valid | ✗Invalid | N/A]
□ CHECK 4 Clarity: A[✓/✗] B[✓/✗] C[✓/✗]
□ CHECK 5 Rules: [✓history sql used | ✗ history sql not used]

**DECISION LOGIC:**
- ALL checks ✓ or N/A → PROCEED TO SQL GENERATION
- ANY check ✗ or blocking ⚠ → GENERATE FOLLOW-UP
- ONE failure = STOP (Do not attempt SQL with uncertainty)

⚠️ After outputting the decision above, immediately proceed to <sql>, <multiple_sql>, or <followup> tags.
⚠️ Do NOT output any additional explanation or detailed assessment.

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

PHASE 2: SQL GENERATION RULES 

⚠️ ONLY PROCEED HERE IF ALL VALIDATION PASSED

**PRIORITY 0: MANDATORY REQUIREMENTS** (Violation = Query Failure)

M1. **Mandatory Filters** ⚠️⚠️⚠️ CRITICAL
MUST include EVERY mandatory filter in WHERE clause:
```sql
-- Example: If ledger is MANDATORY:
WHERE ledger = 'GAAP'
  AND [other conditions]
```

M2. **Validated Filter Values**
Use ONLY filters marked ✓Valid in CHECK 3:
```sql
WHERE UPPER(carrier_id) = UPPER('MPDOVA')  -- Only if validated
```

M3. **CALCULATED FORMULAS HANDLING (CRITICAL)-Metric Type Grouping Rule for Calculations**
When calculating derived metrics (Gross Margin, Cost %, Margin %), DO NOT group by metric_type:
```sql
-- ✓ CORRECT (for calculations):
SELECT 
    ledger, year, month,  -- Business dimensions only
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) AS revenues,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS cogs,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) - 
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS gross_margin
FROM table
WHERE UPPER(metric_type) IN (UPPER('Revenues'), UPPER('COGS'))
GROUP BY ledger, year, month  -- NOT metric_type

-- ✗ WRONG (breaks calculations):
GROUP BY ledger, metric_type  -- Creates separate rows per metric_type
```
**Only group by metric_type when user explicitly asks to see individual metric types as separate rows.**

**PRIORITY 1: STRUCTURAL PATTERNS**

S1. **Historical SQL Patterns** (if available)
Follow structured approach from history_section - inherit if aligned, learn patterns if not.

S2. **Aggregation Requirements**
Always aggregate metrics unless "line items" explicitly requested.

S3. **Component Display Rule**
ALWAYS show source components for ANY calculation:
```sql
-- For "cost per member by state":
SELECT 
  state_name,
  SUM(total_cost) as total_cost,      -- Component 1
  COUNT(member_id) as member_count,   -- Component 2  
  ROUND(SUM(total_cost) / NULLIF(COUNT(member_id), 0), 2) as cost_per_member  -- Result
FROM table
GROUP BY state_name
```

**PRIORITY 2: QUERY PATTERNS**

P1. **Top N Pattern**
```sql
-- "Top 5 carriers"
SELECT carrier_id, 
       SUM(amount) as amount,
       (SELECT SUM(amount) FROM table) as total,
       ROUND(SUM(amount)/(SELECT SUM(amount) FROM table)*100, 3) as pct
FROM table
WHERE [mandatory filters]
GROUP BY carrier_id
ORDER BY amount DESC LIMIT 5
```

P2. **Time Comparison Pattern**
```sql
-- "Compare Q3 months"
SELECT product,
       SUM(CASE WHEN month = 7 THEN revenue END) as jul_revenue,
       SUM(CASE WHEN month = 8 THEN revenue END) as aug_revenue,
       SUM(CASE WHEN month = 9 THEN revenue END) as sep_revenue
FROM table
WHERE quarter = 3 AND [mandatory filters]
GROUP BY product
```

P3. **Multi-Table Pattern**
```sql
-- When JOIN needed
SELECT t1.dim, SUM(t1.metric) as m1, SUM(t2.metric) as m2
FROM table1 t1
{join_clause}  -- Use provided join
WHERE t1.mandatory_col = value
GROUP BY t1.dim
```

**PRIORITY 3: FORMATTING STANDARDS**

F1. **String Comparison** - Always case-insensitive:
```sql
WHERE UPPER(column) = UPPER('value')
```

F2. **Numeric Formatting**:
- Amounts: ROUND(x, 0) AS name_amount
- Percentages: ROUND(x, 3) AS name_percent
- Division safety: NULLIF(denominator, 0)

PHASE 3: OUTPUT GENERATION & VALIDATION

**PRE-OUTPUT CHECKLIST:** ⚠️ MUST VERIFY
□ All mandatory filters present in WHERE clause?
□ All validated filters applied with UPPER()?
□ Source components shown for calculations?

**OUTPUT FORMAT:**

For SINGLE query:
<sql>
[Complete SQL with all mandatory filters]
</sql>
<history_sql_used>[true if used historical patterns | false]</history_sql_used>

For MULTIPLE queries:
<multiple_sql>
<query1_title>[Title - max 8 words]</query1_title>
<query1>[SQL]</query1>
<query2_title>[Title]</query2_title>  
<query2>[SQL]</query2>
</multiple_sql>
<history_sql_used>[true | false]</history_sql_used>

**HISTORY_SQL_USED FLAG RULES:**
- If historical SQL was available AND you used its structure/patterns → true
- If historical SQL was available BUT you generated from scratch → false
- If historical SQL was not available → false

FINAL EXECUTION INSTRUCTION

1. Complete ALL Phase 1 validation checks - show your work
2. Make DECISION: PROCEED or FOLLOW-UP
3. If FOLLOW-UP: Generate clarification questions
4. If PROCEED: Apply SQL rules in priority order (M→S→P→F)
5. Validate final databricks SQL contains ALL mandatory requirements
6. Output SQL with history_sql_used flag

⚠️ REMEMBER: You generate SQL CODE only, not business answers.
⚠️ CRITICAL: Every mandatory filter MUST be in WHERE clause.
"""

        for attempt in range(self.max_retries):
            try:
                print('sql llm prompt', assessment_prompt)
                print("Current Timestamp before SQL writer:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": assessment_prompt}],
                    max_tokens=3000,
                    temperature=0.0,  # Deterministic for SQL generation
                    top_p=0.1,
                    system_prompt="You are a Databricks SQL code generator. Your role is to generate syntactically correct SQL queries based on database metadata and user questions. When users request metrics like 'revenue per script' or 'cost per member', they are asking you to generate SQL that calculates these values - not asking you to perform the calculations yourself. You output SQL code wrapped in XML tags for downstream execution systems."
                )
            
                print('sql llm response', llm_response)
                print("Current Timestamp after SQL write call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                
                # Extract assessment reasoning stream before XML tags
                assessment_reasoning = ""
                # Find first XML tag (could be <sql>, <multiple_sql>, or <followup>)
                xml_patterns = ['<sql>', '<multiple_sql>', '<followup>']
                first_xml_pos = len(llm_response)
                for pattern in xml_patterns:
                    pos = llm_response.find(pattern)
                    if pos != -1 and pos < first_xml_pos:
                        first_xml_pos = pos
                
                if first_xml_pos > 0 and first_xml_pos < len(llm_response):
                    assessment_reasoning = llm_response[:first_xml_pos].strip()
                    print(f"📝 Captured SQL assessment reasoning stream ({len(assessment_reasoning)} chars)")
                
                # Store assessment reasoning in state
                state['sql_assessment_reasoning_stream'] = assessment_reasoning
                
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
