system_prompt = """
You are a SQL query matching expert. Your task is to select the SINGLE most relevant historical question from the candidates provided, or return NO_MATCH if none are suitable.

=== CORE MATCHING PRINCIPLES ===

1. SYNONYMS & VARIATIONS (Treat as identical):
   - "revenue" = "network revenue" = "product revenue" = "network product revenue"
   - "script count" = "volume" = "script volume" = "unadjusted scripts"
   - "Home Delivery" = "HDP" = "Mail"
   - "Specialty" = "SP"
   - These are THE SAME THING - match them as exact

2. DATES (Completely ignore):
   - "July 2025" vs "August 2024" → IGNORE, focus on pattern
   - "Q3 2025" vs "Q2 2024" → IGNORE, focus on comparison structure
   - "Jan-Sep 2025" vs "Apr-Jun 2024" → IGNORE, focus on date range pattern
   - What MATTERS: Is it a single period? Date range? Period comparison (YoY/QoQ/MoM)?

3. FILTER VALUES (Different values are OK):
   - "Specialty" vs "Home Delivery" → SAME PATTERN (product_category filter)
   - "client MDOVA" vs "client PDIND" → SAME PATTERN (client_id filter)
   - "GLP-1" vs "Oncology" → SAME PATTERN (therapy_class filter)
   - Different filter VALUES are fine, same filter TYPE is what matters

4. EXTRA METRICS IN SELECT (OK):
   - History: "script count, revenue for carrier MPDOVA"
   - Current: "script count for carrier MPDOVA"
   - → MATCH! Extra metrics in history are harmless, just ignore them

5. ENTITY OVER-SPECIFICATION (REJECT):
   - History: "revenue for client MDOVA"
   - Current: "revenue"
   - → REJECT! History has entity filter that current doesn't mention
   - Risk: Will confuse LLM to add unwanted filters

6. MISSING DIMENSIONS (REJECT):
   - History: "revenue"
   - Current: "revenue by line of business"
   - → REJECT! History lacks the GROUP BY structure needed
   - Risk: Won't help with dimensional breakdown

7. EXTRA DIMENSIONS IN HISTORY (REJECT):
   - History: "revenue by month"
   - Current: "revenue"
   - → REJECT! History has GROUP BY that current doesn't want
   - Risk: Will confuse LLM to add unwanted grouping

=== DECISION PROCESS ===

STEP 1: Check table compatibility
- Must be same table_name
- If different → REJECT immediately

STEP 2: Check metric compatibility
- Must have at least one overlapping metric (accounting for synonyms)
- "revenue" matches "network revenue" ✅
- "script count" matches "volume" ✅
- "revenue" does NOT match "script count" ❌

STEP 3: Check for over-specification
- Does history have entity filters (client, carrier, specific categories) that current doesn't mention?
- If YES → REJECT
- Example: History="revenue for PBM for E&I", Current="revenue for PBM" → REJECT

STEP 4: Check for missing dimensions
- Does current need GROUP BY dimensions (by drug, by client, by month) that history doesn't have?
- If YES → REJECT
- Example: History="revenue", Current="revenue by drug name" → REJECT

STEP 5: Check for extra dimensions
- Does history have GROUP BY dimensions that current doesn't mention?
- If YES → REJECT
- Example: History="revenue by month", Current="revenue" → REJECT

STEP 6: Pattern matching (for remaining candidates)
Look for BEST match based on:
A. EXACT pattern match (highest priority):
   - Same calculation type (variance, comparison, split, ratio)
   - Same comparison pattern (YoY, QoQ, MoM, vs previous)
   - Same dimensions (by drug_name, by client, etc.)
   - Same structure (side-by-side vs rows)

B. PARTIAL pattern match (if no exact):
   - Same metric and structure
   - Different filter values OK
   - Similar aggregation approach

C. If multiple good matches:
   - Prefer more recent (higher seq_id)
   - Prefer same filter categories
   - Prefer similar complexity

STEP 7: Final validation
Ask: "Will this history help or confuse the LLM?"
- If history has extra unwanted filters → CONFUSE → REJECT
- If history lacks needed structure → CONFUSE → REJECT  
- If pattern clearly transferable → HELP → ACCEPT

=== EXAMPLES ===

Example 1 - SYNONYM MATCH:
Current: "what is network revenue for PBM"
History: "what is revenue for PBM"
Decision: SELECT ✅
Reason: "network revenue" = "revenue" (synonym), same table, same filter, exact match

Example 2 - EXTRA METRIC OK:
Current: "script count for carrier MPDOVA"
History: "script count, revenue for carrier MPDOVA"  
Decision: SELECT ✅
Reason: Same entity filter, same dimensions, extra metric (revenue) in history is harmless

Example 3 - OVER-SPECIFICATION REJECT:
Current: "revenue for PBM"
History: "revenue for PBM for client MDOVA"
Decision: REJECT ❌
Reason: History has client filter that current doesn't mention - will confuse LLM

Example 4 - MISSING DIMENSION REJECT:
Current: "revenue by line of business"
History: "revenue"
Decision: REJECT ❌
Reason: History lacks GROUP BY dimension that current needs

Example 5 - EXTRA DIMENSION REJECT:
Current: "revenue for carrier MPDOVA"
History: "revenue for carrier MPDOVA by month"
Decision: REJECT ❌
Reason: History has GROUP BY month that current doesn't want

Example 6 - FILTER VALUE CHANGE OK:
Current: "script count for Specialty by drug name Q3 2025"
History: "script count for Home Delivery by drug name Q2 2024"
Decision: SELECT ✅
Reason: Same pattern (by drug_name, single quarter), different filter values are fine

Example 7 - PATTERN MATCH:
Current: "volume and revenue for GLP-1 by drug name Q3 2025 vs Q3 2024"
History: "volume and revenue for Oncology by drug name Q2 2025 vs Q2 2024"
Decision: SELECT ✅
Reason: Exact same structure (YoY comparison, by drug_name, volume+revenue), only filter value differs

Example 8 - NO MATCH:
Current: "top 10 clients with highest variance"
Candidates: 
  - "revenue by client"
  - "variance by therapy class"
Decision: NO_MATCH ❌
Reason: First lacks variance logic and top N, second lacks client dimension

=== OUTPUT FORMAT ===

Return valid JSON only (no markdown, no extra text):
{
  "selected_seq_id": <number> or null,
  "history_question_matched": "<exact question text>" or null,
  "table_name": "<table name>" or null,
  "reason": "<detailed explanation of why selected or why NO_MATCH>",
  "decision": "SELECTED" or "NO_MATCH",
  "pattern_match_level": "EXACT" or "PARTIAL" or "NONE"
}

Be conservative. If uncertain, return NO_MATCH. Better to have no match than wrong match.

=== CANDIDATES ===
{candidates_json}

=== CURRENT QUESTION ===
{current_question}

Analyze step-by-step and make your decision.
"""


-- ================================================================
-- FEEDBACK TRACKING - INSERT STATEMENTS REPOSITORY
-- ================================================================
-- Session ID: 28f6f8e3-5889-4e5b-aba9-58da52ad4791
-- User ID: sivakumm@optum.com
-- Table: prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking
-- ================================================================

-- QUESTION 1: What is the network product revenue or network revenue or revenue for PBM?
-- LEARNING: Added product_category and product_sub_category_lvl_1 to SELECT and GROUP BY
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id,
    user_id,
    user_question,
    state_info,
    positive_feedback,
    feedback,
    insert_ts,
    table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791',
    'sivakumm@optum.com',
    'what is the network product revenue or network revenue or revenue for PBM?',
    'SELECT product_category, product_sub_category_lvl_1, year, month, ROUND(SUM(amount_or_count), 0) AS revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) = UPPER(''Revenues'') AND UPPER(product_category) = UPPER(''PBM'') GROUP BY product_category, product_sub_category_lvl_1, year, month ORDER BY year DESC, month DESC',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ================================================================
-- END OF QUESTION 1
-- ================================================================

-- QUESTION 2: What is the actuals vs forecast 8+4 for July 2025 for PBM?
-- LEARNING: 
--   - Use side-by-side format for actuals vs forecast comparisons
--   - Use 8+4 as default forecast cycle when not specified (LLM will change if user specifies different cycle)
--   - GROUP BY metric_type for actuals vs forecast comparisons to show all metrics separately
--   - Continue including product_category and product_sub_category_lvl_1 from Q1 learning
--   - Normalize question to include specific cycle "forecast 8+4" for clarity
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id,
    user_id,
    user_question,
    state_info,
    positive_feedback,
    feedback,
    insert_ts,
    table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791',
    'sivakumm@optum.com',
    'what is the actuals vs forecast 8+4 for July 2025 for PBM',
    'SELECT product_category, product_sub_category_lvl_1, metric_type, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER(''GAAP'') THEN amount_or_count ELSE 0 END), 0) AS actuals_amount, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER(''8+4'') THEN amount_or_count ELSE 0 END), 0) AS forecast_8_4_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 7 AND UPPER(product_category) = UPPER(''PBM'') AND UPPER(ledger) IN (UPPER(''GAAP''), UPPER(''8+4'')) GROUP BY product_category, product_sub_category_lvl_1, metric_type ORDER BY metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ================================================================
-- END OF QUESTION 2
-- ================================================================

-- QUESTION 3: What is the PBM revenue by month Jan-Sep 2025?
-- LEARNING: 
--   - Use side-by-side columns for month-over-month comparisons (each month as separate column)
--   - Default to GAAP ledger for revenue/actuals queries
--   - Use month BETWEEN for date ranges
--   - Continue including product_category and product_sub_category_lvl_1 from Q1 learning
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id,
    user_id,
    user_question,
    state_info,
    positive_feedback,
    feedback,
    insert_ts,
    table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791',
    'sivakumm@optum.com',
    'what is the PBM revenue by month jan-sep 2025 or what is the network product revenue from jan-sep 2025 for PBM',
    'SELECT product_category, product_sub_category_lvl_1, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_revenue_amount, ROUND(SUM(CASE WHEN month = 2 THEN amount_or_count ELSE 0 END), 0) AS february_revenue_amount, ROUND(SUM(CASE WHEN month = 3 THEN amount_or_count ELSE 0 END), 0) AS march_revenue_amount, ROUND(SUM(CASE WHEN month = 4 THEN amount_or_count ELSE 0 END), 0) AS april_revenue_amount, ROUND(SUM(CASE WHEN month = 5 THEN amount_or_count ELSE 0 END), 0) AS may_revenue_amount, ROUND(SUM(CASE WHEN month = 6 THEN amount_or_count ELSE 0 END), 0) AS june_revenue_amount, ROUND(SUM(CASE WHEN month = 7 THEN amount_or_count ELSE 0 END), 0) AS july_revenue_amount, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_revenue_amount, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) = UPPER(''Revenues'') AND UPPER(product_category) = UPPER(''PBM'') GROUP BY product_category, product_sub_category_lvl_1',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ================================================================
-- END OF QUESTION 3
-- ================================================================

-- QUESTION 4: Compare PBM revenue for Q3 2025 vs Q3 2024
-- LEARNING: 
--   - Use side-by-side columns for year-over-year comparisons (q3_2024, q3_2025)
--   - Include variance columns (absolute and percentage) for comparison queries
--   - Use quarter column for quarterly comparisons
--   - Use NULLIF to prevent division by zero in percentage calculations
--   - Continue including product_category and product_sub_category_lvl_1 from Q1 learning
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id,
    user_id,
    user_question,
    state_info,
    positive_feedback,
    feedback,
    insert_ts,
    table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791',
    'sivakumm@optum.com',
    'compare PBM revenue or PBM network revenue or Network product revenue for Q3 2025 VS q3 2024',
    'SELECT product_category, product_sub_category_lvl_1, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) AS q3_2024_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) AS q3_2025_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) AS variance_amount, ROUND((SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = ''Q3'' AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) = UPPER(''Revenues'') AND UPPER(product_category) = UPPER(''PBM'') GROUP BY product_category, product_sub_category_lvl_1',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ================================================================
-- END OF QUESTION 4
-- ================================================================

-- QUESTION 5: What is the script counts trends for Specialty from Jan to September 2025?
-- LEARNING: Script counts = Unadjusted Scripts metric type
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id,
    user_id,
    user_question,
    state_info,
    positive_feedback,
    feedback,
    insert_ts,
    table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791',
    'sivakumm@optum.com',
    'what is the script counts trends for Speciality from jan to September 2025',
    'SELECT product_category, product_sub_category_lvl_1, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_script_count, ROUND(SUM(CASE WHEN month = 2 THEN amount_or_count ELSE 0 END), 0) AS february_script_count, ROUND(SUM(CASE WHEN month = 3 THEN amount_or_count ELSE 0 END), 0) AS march_script_count, ROUND(SUM(CASE WHEN month = 4 THEN amount_or_count ELSE 0 END), 0) AS april_script_count, ROUND(SUM(CASE WHEN month = 5 THEN amount_or_count ELSE 0 END), 0) AS may_script_count, ROUND(SUM(CASE WHEN month = 6 THEN amount_or_count ELSE 0 END), 0) AS june_script_count, ROUND(SUM(CASE WHEN month = 7 THEN amount_or_count ELSE 0 END), 0) AS july_script_count, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_script_count, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_script_count FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) = UPPER(''Unadjusted Scripts'') AND UPPER(product_category) = UPPER(''Specialty'') GROUP BY product_category, product_sub_category_lvl_1',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ================================================================
-- END OF QUESTION 5
-- ================================================================

-- QUESTION 6: What is the revenue for each line of business for July 2025?
-- LEARNING: LOB queries use line_of_business + product_category (not product_sub_category_lvl_1)
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'what is the network product revenue or revenue for each line of business for July 2025',
    'SELECT line_of_business, product_category, ROUND(SUM(amount_or_count), 0) AS revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 7 AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) = UPPER(''Revenues'') GROUP BY line_of_business, product_category ORDER BY line_of_business, product_category',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- QUESTION 7: What is the revenue for each line of business from Jan to Sept 2025?
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'what is the revenue for each line of business from jan to sept 2025',
    'SELECT line_of_business, product_category, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_revenue_amount, ROUND(SUM(CASE WHEN month = 2 THEN amount_or_count ELSE 0 END), 0) AS february_revenue_amount, ROUND(SUM(CASE WHEN month = 3 THEN amount_or_count ELSE 0 END), 0) AS march_revenue_amount, ROUND(SUM(CASE WHEN month = 4 THEN amount_or_count ELSE 0 END), 0) AS april_revenue_amount, ROUND(SUM(CASE WHEN month = 5 THEN amount_or_count ELSE 0 END), 0) AS may_revenue_amount, ROUND(SUM(CASE WHEN month = 6 THEN amount_or_count ELSE 0 END), 0) AS june_revenue_amount, ROUND(SUM(CASE WHEN month = 7 THEN amount_or_count ELSE 0 END), 0) AS july_revenue_amount, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_revenue_amount, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) = UPPER(''Revenues'') GROUP BY line_of_business, product_category ORDER BY line_of_business, product_category',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- QUESTION 8: Compare revenue for each line of business Q3 2025 vs Q3 2024
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'compare revenue for each line of business Q3 2025 vs Q3 2024',
    'SELECT line_of_business, product_category, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) AS q3_2024_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) AS q3_2025_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) AS variance_amount, ROUND((SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = ''Q3'' AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) = UPPER(''Revenues'') GROUP BY line_of_business, product_category ORDER BY line_of_business, product_category',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ================================================================
-- END OF QUESTIONS 6-8
-- ================================================================

-- QUESTION 9: Show adjusted and unadjusted script count for Home Delivery from Jan-Sept 2025
-- LEARNING: Script queries include both Adjusted Scripts and Unadjusted Scripts with metric_type in GROUP BY
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me adjusted script and unadjusted script count for Home Delivery or HDP from jan - September 2025',
    'SELECT product_category, product_sub_category_lvl_1, metric_type, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_count, ROUND(SUM(CASE WHEN month = 2 THEN amount_or_count ELSE 0 END), 0) AS february_count, ROUND(SUM(CASE WHEN month = 3 THEN amount_or_count ELSE 0 END), 0) AS march_count, ROUND(SUM(CASE WHEN month = 4 THEN amount_or_count ELSE 0 END), 0) AS april_count, ROUND(SUM(CASE WHEN month = 5 THEN amount_or_count ELSE 0 END), 0) AS may_count, ROUND(SUM(CASE WHEN month = 6 THEN amount_or_count ELSE 0 END), 0) AS june_count, ROUND(SUM(CASE WHEN month = 7 THEN amount_or_count ELSE 0 END), 0) AS july_count, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_count, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_count FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) IN (UPPER(''Adjusted Scripts''), UPPER(''Unadjusted Scripts'')) AND UPPER(product_category) = UPPER(''Home Delivery'') GROUP BY product_category, product_sub_category_lvl_1, metric_type ORDER BY metric_type',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- QUESTION 10: Scripts current month vs previous month (Sep 2025 vs Aug 2025)
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me adjusted and unadjusted script count for Home Delivery current month vs previous month',
    'SELECT product_category, product_sub_category_lvl_1, metric_type, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_2025_count, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_count, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS variance_count, ROUND((SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month IN (8, 9) AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) IN (UPPER(''Adjusted Scripts''), UPPER(''Unadjusted Scripts'')) AND UPPER(product_category) = UPPER(''Home Delivery'') GROUP BY product_category, product_sub_category_lvl_1, metric_type ORDER BY metric_type',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- QUESTION 11: Scripts current month this year vs last year (Sep 2025 vs Sep 2024)
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me adjusted and unadjusted script count for Home Delivery current month this year vs last year this month',
    'SELECT product_category, product_sub_category_lvl_1, metric_type, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2024_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS variance_count, ROUND((SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND month = 9 AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) IN (UPPER(''Adjusted Scripts''), UPPER(''Unadjusted Scripts'')) AND UPPER(product_category) = UPPER(''Home Delivery'') GROUP BY product_category, product_sub_category_lvl_1, metric_type ORDER BY metric_type',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- QUESTION 12: Scripts current quarter this year vs last year (Q3 2025 vs Q3 2024)
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me adjusted and unadjusted script count for Home Delivery current quarter this year vs last year this quarter',
    'SELECT product_category, product_sub_category_lvl_1, metric_type, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) AS q3_2024_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) AS q3_2025_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) AS variance_count, ROUND((SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = ''Q3'' AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) IN (UPPER(''Adjusted Scripts''), UPPER(''Unadjusted Scripts'')) AND UPPER(product_category) = UPPER(''Home Delivery'') GROUP BY product_category, product_sub_category_lvl_1, metric_type ORDER BY metric_type',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ================================================================
-- END OF QUESTIONS 9-12
-- ================================================================

-- QUESTION 13: What is monthly revenue from Jan through Sep 2025 for network revenue (high-level)?
-- LEARNING: High-level queries use only product_category in GROUP BY (exclude product_sub_category_lvl_1)
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'What is monthly revenue from Jan through Sep 2025 for network revenue',
    'SELECT product_category, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_revenue_amount, ROUND(SUM(CASE WHEN month = 2 THEN amount_or_count ELSE 0 END), 0) AS february_revenue_amount, ROUND(SUM(CASE WHEN month = 3 THEN amount_or_count ELSE 0 END), 0) AS march_revenue_amount, ROUND(SUM(CASE WHEN month = 4 THEN amount_or_count ELSE 0 END), 0) AS april_revenue_amount, ROUND(SUM(CASE WHEN month = 5 THEN amount_or_count ELSE 0 END), 0) AS may_revenue_amount, ROUND(SUM(CASE WHEN month = 6 THEN amount_or_count ELSE 0 END), 0) AS june_revenue_amount, ROUND(SUM(CASE WHEN month = 7 THEN amount_or_count ELSE 0 END), 0) AS july_revenue_amount, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_revenue_amount, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER(''GAAP'') AND UPPER(metric_type) = UPPER(''Revenues'') GROUP BY product_category ORDER BY product_category',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ================================================================
-- END OF QUESTION 13
-- ================================================================

-- ================================================================
-- NEW TABLE: prd_optumrx_orxfdmprdsa.rag.pbm_claims
-- ================================================================

-- QUESTION 14: Show script counts for PBM for current month vs previous month for client MDOVA
-- LEARNING: 
--   - Script counts = unadjusted_script_count column
--   - Always filter claim_status_code IN ('P', 'X') for net activity (Paid and Reversed)
--   - Always include client_id and client_name in SELECT for client queries
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me the script counts for PBM for current month vs previous month for client MDOVA',
    'SELECT client_id, client_name, ROUND(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) AS august_2025_script_count, ROUND(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0) AS september_2025_script_count, ROUND(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) AS variance_count, ROUND((SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year = 2025 AND month IN (8, 9) AND UPPER(product_category) = UPPER(''PBM'') AND UPPER(client_id) = UPPER(''MDOVA'') AND claim_status_code IN (''P'', ''X'') GROUP BY client_id, client_name',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- QUESTION 15: Show script counts for current month vs previous month for clients with variance > 10%
-- LEARNING: Use HAVING clause with ABS() for variance threshold filtering
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me the script counts for current month vs previous month for the clients which has variance greater than 10%',
    'SELECT client_id, client_name, ROUND(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) AS august_2025_script_count, ROUND(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0) AS september_2025_script_count, ROUND(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) AS variance_count, ROUND((SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year = 2025 AND month IN (8, 9) AND UPPER(product_category) = UPPER(''PBM'') AND claim_status_code IN (''P'', ''X'') GROUP BY client_id, client_name HAVING ABS((SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) * 100) > 10 ORDER BY variance_percent DESC',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- ================================================================
-- END OF QUESTIONS 14-15
-- ================================================================

-- QUESTION 16: Compare volume and rate for GLP-1s by drug name for Q3 2025 vs Q3 2024
-- LEARNING: 
--   - Use LIKE '%GLP-1%' for therapy_class_name when user asks generically about GLP-1
--   - Volume = unadjusted_script_count
--   - Rate/Revenue per script = revenue_amt / unadjusted_script_count
--   - Show source components (revenue, volume) when calculating rate
--   - Include therapy_class_name + drug_name for drug-level analysis
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'Compare volume and rate for GLP-1s or GLP-1s by drug name for Q3 2025 vs Q3 2024',
    'SELECT therapy_class_name, drug_name, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2024_volume_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) AS q3_2024_revenue_amount, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0), 3) AS q3_2024_rate, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2025_volume_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) AS q3_2025_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0), 3) AS q3_2025_rate, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) AS volume_variance_count, ROUND((SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0)) - (SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0)), 3) AS rate_variance FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year IN (2024, 2025) AND quarter = ''Q3'' AND UPPER(therapy_class_name) LIKE UPPER(''%GLP-1%'') AND claim_status_code IN (''P'', ''X'') GROUP BY therapy_class_name, drug_name ORDER BY therapy_class_name, drug_name',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- QUESTION 17: What is rate/revenue per script for GLP-1s by drug name for Jan-Sept 2025
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'What is rate/script or revenue per script for GLP-1s by drug name for Jan-Sept 2025',
    'SELECT therapy_class_name, drug_name, ROUND(SUM(CASE WHEN month = 1 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 1 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS january_rate, ROUND(SUM(CASE WHEN month = 2 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 2 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS february_rate, ROUND(SUM(CASE WHEN month = 3 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 3 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS march_rate, ROUND(SUM(CASE WHEN month = 4 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 4 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS april_rate, ROUND(SUM(CASE WHEN month = 5 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 5 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS may_rate, ROUND(SUM(CASE WHEN month = 6 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 6 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS june_rate, ROUND(SUM(CASE WHEN month = 7 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 7 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS july_rate, ROUND(SUM(CASE WHEN month = 8 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS august_rate, ROUND(SUM(CASE WHEN month = 9 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS september_rate FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(therapy_class_name) LIKE UPPER(''%GLP-1%'') AND claim_status_code IN (''P'', ''X'') GROUP BY therapy_class_name, drug_name ORDER BY therapy_class_name, drug_name',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- QUESTION 18: What is volume and revenue for GLP-1s by drug name for PBM for Q3 2025 vs Q3 2024
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'What is volume and revenue for GLP-1s by drug name for PBM for Q3 2025 vs Q3 2024',
    'SELECT therapy_class_name, drug_name, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2024_volume_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) AS q3_2024_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2025_volume_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) AS q3_2025_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) AS volume_variance_count, ROUND((SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) * 100, 3) AS volume_variance_percent, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) AS revenue_variance_amount, ROUND((SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) * 100, 3) AS revenue_variance_percent FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year IN (2024, 2025) AND quarter = ''Q3'' AND UPPER(product_category) = UPPER(''PBM'') AND UPPER(therapy_class_name) LIKE UPPER(''%GLP-1%'') AND claim_status_code IN (''P'', ''X'') GROUP BY therapy_class_name, drug_name ORDER BY therapy_class_name, drug_name',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- ================================================================
-- END OF QUESTIONS 16-18
-- ================================================================

-- QUESTION 19: What therapies contributed to rate increase for PBM from April to June 2025?
-- LEARNING: 
--   - For "contributed to increase" queries, use HAVING clause to filter only increases
--   - Order by volume DESC when user asks for "high volume ones first"
--   - Show both periods' volume, revenue, and rate for full context
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'What therapies contributed to rate increase for PBM from April to June 2025',
    'SELECT therapy_class_name, ROUND(SUM(CASE WHEN month = 4 THEN unadjusted_script_count ELSE 0 END), 0) AS april_2025_volume_count, ROUND(SUM(CASE WHEN month = 4 THEN revenue_amt ELSE 0 END), 0) AS april_2025_revenue_amount, ROUND(SUM(CASE WHEN month = 4 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 4 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS april_2025_rate, ROUND(SUM(CASE WHEN month = 6 THEN unadjusted_script_count ELSE 0 END), 0) AS june_2025_volume_count, ROUND(SUM(CASE WHEN month = 6 THEN revenue_amt ELSE 0 END), 0) AS june_2025_revenue_amount, ROUND(SUM(CASE WHEN month = 6 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 6 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS june_2025_rate, ROUND((SUM(CASE WHEN month = 6 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 6 THEN unadjusted_script_count ELSE 0 END), 0)) - (SUM(CASE WHEN month = 4 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 4 THEN unadjusted_script_count ELSE 0 END), 0)), 3) AS rate_variance FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year = 2025 AND month IN (4, 6) AND UPPER(product_category) = UPPER(''PBM'') AND claim_status_code IN (''P'', ''X'') GROUP BY therapy_class_name HAVING (SUM(CASE WHEN month = 6 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 6 THEN unadjusted_script_count ELSE 0 END), 0)) > (SUM(CASE WHEN month = 4 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 4 THEN unadjusted_script_count ELSE 0 END), 0)) ORDER BY june_2025_volume_count DESC',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- ================================================================
-- END OF QUESTION 19
-- ================================================================

-- QUESTION 20: Volume and revenue for GLP-1s for PBM split between obesity vs diabetes Q3 2025 vs Q3 2024
-- LEARNING: 
--   - For category splits (obesity vs diabetes, brand vs generic), use CASE statements for side-by-side columns
--   - GLP-1 Anti-Obesity Agents = obesity, GLP-1 Receptor Agonists = diabetes
--   - Do NOT use GROUP BY when user asks to "split between" categories - use CASE instead
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'What is volume and revenue for GLP-1s for PBM split between obesity versus diabetes for Q3 2025 vs Q3 2024',
    'SELECT ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' AND UPPER(therapy_class_name) = UPPER(''GLP-1 Anti-Obesity Agents'') THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2024_obesity_volume_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' AND UPPER(therapy_class_name) = UPPER(''GLP-1 Anti-Obesity Agents'') THEN revenue_amt ELSE 0 END), 0) AS q3_2024_obesity_revenue_amount, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' AND UPPER(therapy_class_name) = UPPER(''GLP-1 Receptor Agonists'') THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2024_diabetes_volume_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' AND UPPER(therapy_class_name) = UPPER(''GLP-1 Receptor Agonists'') THEN revenue_amt ELSE 0 END), 0) AS q3_2024_diabetes_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' AND UPPER(therapy_class_name) = UPPER(''GLP-1 Anti-Obesity Agents'') THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2025_obesity_volume_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' AND UPPER(therapy_class_name) = UPPER(''GLP-1 Anti-Obesity Agents'') THEN revenue_amt ELSE 0 END), 0) AS q3_2025_obesity_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' AND UPPER(therapy_class_name) = UPPER(''GLP-1 Receptor Agonists'') THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2025_diabetes_volume_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' AND UPPER(therapy_class_name) = UPPER(''GLP-1 Receptor Agonists'') THEN revenue_amt ELSE 0 END), 0) AS q3_2025_diabetes_revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year IN (2024, 2025) AND quarter = ''Q3'' AND UPPER(product_category) = UPPER(''PBM'') AND UPPER(therapy_class_name) IN (UPPER(''GLP-1 Anti-Obesity Agents''), UPPER(''GLP-1 Receptor Agonists'')) AND claim_status_code IN (''P'', ''X'')',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- QUESTION 21: Volume and revenue for Thrombolytic Enzymes for PBM by drug name for April to June 2025
-- LEARNING: For specific therapy class (not generic like GLP-1), use exact match with = operator
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'What is the volume and revenue for Thrombolytic Enzymes for PBM by drug name for April to June 2025',
    'SELECT therapy_class_name, drug_name, ROUND(SUM(CASE WHEN month = 4 THEN unadjusted_script_count ELSE 0 END), 0) AS april_2025_volume_count, ROUND(SUM(CASE WHEN month = 4 THEN revenue_amt ELSE 0 END), 0) AS april_2025_revenue_amount, ROUND(SUM(CASE WHEN month = 5 THEN unadjusted_script_count ELSE 0 END), 0) AS may_2025_volume_count, ROUND(SUM(CASE WHEN month = 5 THEN revenue_amt ELSE 0 END), 0) AS may_2025_revenue_amount, ROUND(SUM(CASE WHEN month = 6 THEN unadjusted_script_count ELSE 0 END), 0) AS june_2025_volume_count, ROUND(SUM(CASE WHEN month = 6 THEN revenue_amt ELSE 0 END), 0) AS june_2025_revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year = 2025 AND month BETWEEN 4 AND 6 AND UPPER(product_category) = UPPER(''PBM'') AND UPPER(therapy_class_name) = UPPER(''Thrombolytic Enzymes'') AND claim_status_code IN (''P'', ''X'') GROUP BY therapy_class_name, drug_name ORDER BY drug_name',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- QUESTION 22: Volume and revenue for GLP-1s by drug name for PBM for Q3 2025 vs Q3 2024
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'What is volume and revenue for GLP-1s by drug name for PBM for Q3 2025 vs Q3 2024',
    'SELECT therapy_class_name, drug_name, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2024_volume_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) AS q3_2024_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) AS q3_2025_volume_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) AS q3_2025_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) AS volume_variance_count, ROUND((SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN unadjusted_script_count ELSE 0 END), 0) * 100, 3) AS volume_variance_percent, ROUND(SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) AS revenue_variance_amount, ROUND((SUM(CASE WHEN year = 2025 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = ''Q3'' THEN revenue_amt ELSE 0 END), 0) * 100, 3) AS revenue_variance_percent FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year IN (2024, 2025) AND quarter = ''Q3'' AND UPPER(product_category) = UPPER(''PBM'') AND UPPER(therapy_class_name) LIKE UPPER(''%GLP-1%'') AND claim_status_code IN (''P'', ''X'') GROUP BY therapy_class_name, drug_name ORDER BY therapy_class_name, drug_name',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- ================================================================
-- END OF QUESTIONS 20-22
-- ================================================================

-- QUESTION 23: What is revenue for MPDOVA for PBM by month Jan to Sep 2025
-- LEARNING: 
--   - carrier_id is different from client_id - use appropriate column based on identifier type
--   - For single entity trends over time, use simple GROUP BY month (not CASE statements)
--   - Use CASE statements only for side-by-side comparisons (periods, categories)
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'What is revenue for MPDOVA for PBM revenue by month jan to sep 2025',
    'SELECT carrier_id, year, month, ROUND(SUM(revenue_amt), 0) AS revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(product_category) = UPPER(''PBM'') AND UPPER(carrier_id) = UPPER(''MPDOVA'') AND claim_status_code IN (''P'', ''X'') GROUP BY carrier_id, year, month ORDER BY year, month',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- ================================================================
-- END OF QUESTION 23
-- ================================================================

-- QUESTION 24: For GLP-1 drugs, what are the top 10 clients with highest rate variance, volume variance and mix variance
-- LEARNING: 
--   - Rate variance = (Prior Rate - Current Rate) × Current Volume
--   - Volume variance = (Prior Volume - Current Volume) × Current Rate
--   - Mix variance = (Prior Revenue - Current Revenue - Rate Variance - Volume Variance)
--   - Show all three variance types in same query with source components
--   - Use ABS() in ORDER BY to get highest variance regardless of positive/negative
--   - Use LIMIT 10 for top N queries
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'for GLP-1 drugs, what are the top 10 clients with highest rate variance, volume variance and mix variance',
    'SELECT client_id, client_name, ROUND(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) AS august_2025_volume_count, ROUND(SUM(CASE WHEN month = 8 THEN revenue_amt ELSE 0 END), 0) AS august_2025_revenue_amount, ROUND(SUM(CASE WHEN month = 8 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS august_2025_rate, ROUND(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0) AS september_2025_volume_count, ROUND(SUM(CASE WHEN month = 9 THEN revenue_amt ELSE 0 END), 0) AS september_2025_revenue_amount, ROUND(SUM(CASE WHEN month = 9 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0), 3) AS september_2025_rate, ROUND((SUM(CASE WHEN month = 8 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) - SUM(CASE WHEN month = 9 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0)) * SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0) AS rate_variance_amount, ROUND((SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END)) * (SUM(CASE WHEN month = 9 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0)), 0) AS volume_variance_amount, ROUND((SUM(CASE WHEN month = 8 THEN revenue_amt ELSE 0 END) - SUM(CASE WHEN month = 9 THEN revenue_amt ELSE 0 END)) - ((SUM(CASE WHEN month = 8 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END), 0) - SUM(CASE WHEN month = 9 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0)) * SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END)) - ((SUM(CASE WHEN month = 8 THEN unadjusted_script_count ELSE 0 END) - SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END)) * (SUM(CASE WHEN month = 9 THEN revenue_amt ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 9 THEN unadjusted_script_count ELSE 0 END), 0))), 0) AS mix_variance_amount FROM prd_optumrx_orxfdmprdsa.rag.pbm_claims WHERE year = 2025 AND month IN (8, 9) AND UPPER(therapy_class_name) LIKE UPPER(''%GLP-1%'') AND claim_status_code IN (''P'', ''X'') GROUP BY client_id, client_name ORDER BY ABS(rate_variance_amount) DESC LIMIT 10',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.pbm_claims'
);

-- ================================================================
-- END OF QUESTION 24
-- ================================================================
