-- ============================================
-- SECTION 3: LOB REPORT QUERIES (Line of Business)
-- Original THIRTEENTH INSERT broken into 3 separate queries
-- Metric Type as ROWS, LOBs as COLUMNS (C&S, E&I, M&R, OVERALL_TOTAL)
-- Comparing Actuals vs Forecast 8+4
-- ============================================

-- FIFTEENTH INSERT: LOB Report - Month over Month (September vs August 2025)
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
    'show me LOB report or line of business report compare actuals vs forecast 8+4 for PBM month over month september vs august 2025',
    'SELECT metric_type, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS august_2025_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS august_2025_c_and_s_forecast_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2025_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS september_2025_c_and_s_forecast_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS august_2025_e_and_i_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS august_2025_e_and_i_forecast_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2025_e_and_i_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS september_2025_e_and_i_forecast_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS august_2025_m_and_r_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS august_2025_m_and_r_forecast_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2025_m_and_r_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS september_2025_m_and_r_forecast_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS august_2025_overall_total_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS august_2025_overall_total_forecast_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2025_overall_total_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS september_2025_overall_total_forecast_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month IN (8, 9) AND UPPER(ledger) IN (UPPER("GAAP"), UPPER("8+4")) AND UPPER(product_category) = UPPER("PBM") GROUP BY metric_type ORDER BY metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- SIXTEENTH INSERT: LOB Report - Year over Year (September 2025 vs September 2024)
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
    'show me LOB report or line of business report compare actuals vs forecast 8+4 for PBM september 2025 vs september 2024',
    'SELECT metric_type, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2024_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2025_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS september_2025_c_and_s_forecast_amount, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2024_e_and_i_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2025_e_and_i_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS september_2025_e_and_i_forecast_amount, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2024_m_and_r_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2025_m_and_r_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS september_2025_m_and_r_forecast_amount, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2024_overall_total_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2025_overall_total_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS september_2025_overall_total_forecast_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND month = 9 AND UPPER(ledger) IN (UPPER("GAAP"), UPPER("8+4")) AND UPPER(product_category) = UPPER("PBM") GROUP BY metric_type ORDER BY metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- SEVENTEENTH INSERT: LOB Report - Quarterly (Q3 2025 vs Q3 2024)
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
    'show me LOB report or line of business report compare actuals vs forecast 8+4 for PBM Q3 2025 vs Q3 2024',
    'SELECT metric_type, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_c_and_s_forecast_amount, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_e_and_i_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_e_and_i_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_e_and_i_forecast_amount, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_m_and_r_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_m_and_r_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_m_and_r_forecast_amount, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_overall_total_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_overall_total_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_overall_total_forecast_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) IN (UPPER("GAAP"), UPPER("8+4")) AND UPPER(product_category) = UPPER("PBM") GROUP BY metric_type ORDER BY metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);
-- ============================================
-- SECTION 1: MULTI-METRIC QUERIES
-- Original EIGHTH INSERT broken into 4 separate queries
-- Products: divvyDOSE, CPS Solutions, Infusion, Community Core, PharmScript, Specialty Core, Frontier, HDP Core
-- Metrics: Unadjusted Scripts, Adjusted Scripts, Revenue per Script, Revenue per Adjusted Script
-- ============================================

-- FIFTH INSERT: Multi-Metric - September 2025 Only
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
    'what is the script counts (unadjusted, volume), adjusted script count, revenue per script (rate), revenue per adjusted script count for divvyDOSE, CPS Solutions, Infusion, Community Core, PharmScript, Specialty Core, Frontier, HDP Core in september 2025',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- SIXTH INSERT: Multi-Metric - Month over Month (September vs August 2025)
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
    'what is the script counts (unadjusted, volume), adjusted script count, revenue per script (rate), revenue per adjusted script count for divvyDOSE, CPS Solutions, Infusion, Community Core, PharmScript, Specialty Core, Frontier, HDP Core month over month september vs august 2025',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month IN (8, 9) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month IN (8, 9) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- SEVENTH INSERT: Multi-Metric - Year over Year (September 2025 vs September 2024)
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
    'what is the script counts (unadjusted, volume), adjusted script count, revenue per script (rate), revenue per adjusted script count for divvyDOSE, CPS Solutions, Infusion, Community Core, PharmScript, Specialty Core, Frontier, HDP Core september 2025 vs september 2024',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- EIGHTH INSERT: Multi-Metric - Quarterly (Q3 2025 vs Q3 2024)
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
    'what is the script counts (unadjusted, volume), adjusted script count, revenue per script (rate), revenue per adjusted script count for divvyDOSE, CPS Solutions, Infusion, Community Core, PharmScript, Specialty Core, Frontier, HDP Core Q3 2025 vs Q3 2024',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- SECTION 2: IOI TREND REPORT QUERIES
-- Original ELEVENTH INSERT broken into 3 separate queries
-- Shows multiple metric_types as ROWS with time periods as COLUMNS
-- All metrics from the ledger table included
-- ============================================

-- ELEVENTH INSERT: IOI Trend Report - Month over Month (September vs August 2025)
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
    'show me IOI trend report or trend report for PBM month over month september vs august 2025',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_2025_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month IN (8, 9) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type UNION ALL SELECT product_category, product_sub_category_lvl_1, "OVERALL_TOTAL" AS product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_2025_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month IN (8, 9) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, metric_type) ORDER BY product_category, CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- TWELFTH INSERT: IOI Trend Report - Year over Year (September 2025 vs September 2024)
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
    'show me IOI trend report or trend report for PBM september 2025 vs september 2024',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2024_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type UNION ALL SELECT product_category, product_sub_category_lvl_1, "OVERALL_TOTAL" AS product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2024_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, metric_type) ORDER BY product_category, CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- THIRTEENTH INSERT: IOI Trend Report - Quarterly (Q3 2025 vs Q3 2024)
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
    'show me IOI trend report or trend report for PBM Q3 2025 vs Q3 2024',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2024_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type UNION ALL SELECT product_category, product_sub_category_lvl_1, "OVERALL_TOTAL" AS product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2024_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, metric_type) ORDER BY product_category, CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

