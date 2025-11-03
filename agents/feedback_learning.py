-- UNION Query: Granular + Overall Total for PBM Revenue
-- This query returns both sub-category breakdown AND overall total in one result set

SELECT * FROM (
  SELECT 
    product_category, 
    product_sub_category_lvl_2, 
    year, 
    month, 
    ROUND(SUM(amount_or_count), 0) AS revenue_amount 
  FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast 
  WHERE UPPER(ledger) = UPPER("GAAP") 
    AND UPPER(metric_type) = UPPER("Revenues") 
    AND UPPER(product_category) = UPPER("PBM") 
  GROUP BY product_category, product_sub_category_lvl_2, year, month
  
  UNION ALL
  
  SELECT 
    product_category, 
    "OVERALL_TOTAL" AS product_sub_category_lvl_2, 
    year, 
    month, 
    ROUND(SUM(amount_or_count), 0) AS revenue_amount 
  FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast 
  WHERE UPPER(ledger) = UPPER("GAAP") 
    AND UPPER(metric_type) = UPPER("Revenues") 
    AND UPPER(product_category) = UPPER("PBM") 
  GROUP BY product_category, year, month
) 
ORDER BY year DESC, month DESC, 
  CASE WHEN product_sub_category_lvl_2 = 'OVERALL_TOTAL' THEN 0 ELSE 1 END,
  product_sub_category_lvl_2;


-- ============================================
-- INSERT STATEMENT FOR FEEDBACK TRACKING
-- ============================================

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
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, year, month, ROUND(SUM(amount_or_count), 0) AS revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_2, year, month UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, year, month, ROUND(SUM(amount_or_count), 0) AS revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, year, month) ORDER BY year DESC, month DESC, CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- SECOND INSERT: Monthly Pivoted Revenue (Jan-Sep 2025)
-- ============================================

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
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_revenue_amount, ...[months 2-9: february, march, april, may, june, july, august, september]... FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_revenue_amount, ...[months 2-9: february, march, april, may, june, july, august, september]... FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- THIRD INSERT: Q3 2025 vs Q3 2024 Comparison
-- ============================================

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
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2024_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2025_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS variance_amount, ROUND((SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2024_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2025_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS variance_amount, ROUND((SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- FOURTH INSERT: Multi-Metric Query - Scripts, Revenue per Script for Specialty (Jan-March 2025)
-- ============================================

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
    'what is the script counts (unadjusted, volume) trends, adjusted script count, revenue per script (rate), revenue per adjusted script count trends for Specialty from jan to march 2025',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS january_unadjusted_script_count, ...[months 2-3: february_unadjusted_script_count, march_unadjusted_script_count]..., ROUND(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS january_adjusted_script_count, ...[months 2-3: february_adjusted_script_count, march_adjusted_script_count]..., ROUND(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS january_revenue_per_script, ...[months 2-3: february_revenue_per_script, march_revenue_per_script]..., ROUND(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS january_revenue_per_adjusted_script, ...[months 2-3: february_revenue_per_adjusted_script, march_revenue_per_adjusted_script]... FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 3 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_category) = UPPER("Specialty") GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS january_unadjusted_script_count, ...[months 2-3: february_unadjusted_script_count, march_unadjusted_script_count]..., ROUND(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS january_adjusted_script_count, ...[months 2-3: february_adjusted_script_count, march_adjusted_script_count]..., ROUND(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS january_revenue_per_script, ...[months 2-3: february_revenue_per_script, march_revenue_per_script]..., ROUND(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN month = 1 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS january_revenue_per_adjusted_script, ...[months 2-3: february_revenue_per_adjusted_script, march_revenue_per_adjusted_script]... FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 3 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_category) = UPPER("Specialty") GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- FIFTH INSERT: Single Month Multi-Metric (September 2025)
-- ============================================

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
    'what is the script counts (unadjusted, volume), adjusted script count, revenue per script (rate), revenue per adjusted script count for Specialty in september 2025',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS unadjusted_script_count, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS adjusted_script_count, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS revenue_per_script, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_category) = UPPER("Specialty") GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS unadjusted_script_count, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS adjusted_script_count, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS revenue_per_script, ROUND(SUM(CASE WHEN UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_category) = UPPER("Specialty") GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- SIXTH INSERT: Q3 2025 vs Q3 2024 Multi-Metric Comparison
-- ============================================

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
    'compare script counts (unadjusted, volume), adjusted script count, revenue per script (rate), revenue per adjusted script count for Specialty Q3 2025 vs Q3 2024',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_category) = UPPER("Specialty") GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_category) = UPPER("Specialty") GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- SEVENTH INSERT: Comprehensive Multi-Metric Multi-Time Comparison for Multiple Services
-- Includes: MoM (Sep vs Aug 2025), YoY (Sep 2025 vs Sep 2024), Q3 2025 vs Q3 2024
-- Services: Admin Fees, Emisar GPO, Retail Other, Retail, Mfr Discount, Workers Comp, Prior Auth
-- ============================================

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
    'what is the script counts (unadjusted, volume), adjusted script count, revenue per script (rate), revenue per adjusted script count for Admin Fees, Emisar GPO, Retail Other, Retail, Mfr Discount, Workers Comp, Prior Auth in september 2025 or month over month (september vs august 2025) or current month this year vs last year (september 2025 vs september 2024) or Q3 2025 vs Q3 2024',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE ((year = 2025 AND month IN (8, 9)) OR (year = 2024 AND month = 9) OR (year IN (2024, 2025) AND quarter = "Q3")) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("Admin Fees"), UPPER("Emisar GPO"), UPPER("Retail Other"), UPPER("Retail"), UPPER("Mfr Discount"), UPPER("Workers Comp"), UPPER("Prior Auth")) GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE ((year = 2025 AND month IN (8, 9)) OR (year = 2024 AND month = 9) OR (year IN (2024, 2025) AND quarter = "Q3")) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("Admin Fees"), UPPER("Emisar GPO"), UPPER("Retail Other"), UPPER("Retail"), UPPER("Mfr Discount"), UPPER("Workers Comp"), UPPER("Prior Auth")) GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- EIGHTH INSERT: Comprehensive Multi-Metric Multi-Time Comparison for Second Set of Services
-- Includes: MoM (Sep vs Aug 2025), YoY (Sep 2025 vs Sep 2024), Q3 2025 vs Q3 2024
-- Services: divvyDOSE, CPS Solutions, Infusion, Community Core, PharmScript, Specialty Core, Frontier, HDP Core
-- ============================================

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
    'what is the script counts (unadjusted, volume), adjusted script count, revenue per script (rate), revenue per adjusted script count for divvyDOSE, CPS Solutions, Infusion, Community Core, PharmScript, Specialty Core, Frontier, HDP Core in september 2025 or month over month (september vs august 2025) or current month this year vs last year (september 2025 vs september 2024) or Q3 2025 vs Q3 2024',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE ((year = 2025 AND month IN (8, 9)) OR (year = 2024 AND month = 9) OR (year IN (2024, 2025) AND quarter = "Q3")) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category, product_sub_category_lvl_2 UNION ALL SELECT product_category, "OVERALL_TOTAL" AS product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_unadjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS august_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS september_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_adjusted_script_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS august_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2025_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS september_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2024_revenue_per_adjusted_script, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS q3_2025_revenue_per_adjusted_script FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE ((year = 2025 AND month IN (8, 9)) OR (year = 2024 AND month = 9) OR (year IN (2024, 2025) AND quarter = "Q3")) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Unadjusted Scripts"), UPPER("Adjusted Scripts"), UPPER("Revenues")) AND UPPER(product_sub_category_lvl_2) IN (UPPER("divvyDOSE"), UPPER("CPS Solutions"), UPPER("Infusion"), UPPER("Community Core"), UPPER("PharmScript"), UPPER("Specialty Core"), UPPER("Frontier"), UPPER("HDP Core")) GROUP BY product_category) ORDER BY CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- IOI TREND REPORT QUERIES
-- Shows multiple metric_types as ROWS with time periods as COLUMNS
-- ============================================

-- NINTH INSERT: IOI Trend Report - Jan to Mar 2025 (Monthly)
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
    'show me IOI trend report or trend report for jan to march 2025 for PBM',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_amount, ...[months 2-3: february_amount, march_amount]... FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 3 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type UNION ALL SELECT product_category, product_sub_category_lvl_1, "OVERALL_TOTAL" AS product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_amount, ...[months 2-3: february_amount, march_amount]... FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 3 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, metric_type) ORDER BY product_category, CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- TENTH INSERT: IOI Trend Report - Single Month (September 2025)
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
    'show me IOI trend report or trend report for september 2025 for PBM',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type, ROUND(SUM(amount_or_count), 0) AS september_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type UNION ALL SELECT product_category, product_sub_category_lvl_1, "OVERALL_TOTAL" AS product_sub_category_lvl_2, metric_type, ROUND(SUM(amount_or_count), 0) AS september_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, metric_type) ORDER BY product_category, CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ELEVENTH INSERT: IOI Trend Report - Comprehensive (MoM + YoY + Quarterly)
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
    'show me IOI trend report or trend report for PBM month over month (september vs august 2025) or current month this year vs last year (september 2025 vs september 2024) or Q3 2025 vs Q3 2024',
    'SELECT * FROM (SELECT product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_2025_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_amount, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2024_amount, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2024_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE ((year = 2025 AND month IN (8, 9)) OR (year = 2024 AND month = 9) OR (year IN (2024, 2025) AND quarter = "Q3")) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type UNION ALL SELECT product_category, product_sub_category_lvl_1, "OVERALL_TOTAL" AS product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_2025_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_amount, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2024_amount, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2024_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2025_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE ((year = 2025 AND month IN (8, 9)) OR (year = 2024 AND month = 9) OR (year IN (2024, 2025) AND quarter = "Q3")) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_1, metric_type) ORDER BY product_category, CASE WHEN product_sub_category_lvl_2 = ''OVERALL_TOTAL'' THEN 0 ELSE 1 END, product_sub_category_lvl_1, product_sub_category_lvl_2, metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- LOB REPORT QUERIES (Line of Business)
-- OPTION 2: Metric Type as ROWS, LOBs as COLUMNS
-- Actuals vs Forecast 8+4 comparison
-- ============================================

-- TWELFTH INSERT: LOB Report - Single Month (September 2025)
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
    'show me LOB report or line of business report compare actuals vs forecast 8+4 for september 2025 for PBM',
    'SELECT metric_type, ROUND(SUM(CASE WHEN UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS c_and_s_actuals_amount, ROUND(SUM(CASE WHEN UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS c_and_s_forecast_8_plus_4_amount, ROUND(SUM(CASE WHEN UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS c_and_s_variance_amount, ROUND((SUM(CASE WHEN UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS c_and_s_variance_percent, ROUND(SUM(CASE WHEN UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS e_and_i_actuals_amount, ROUND(SUM(CASE WHEN UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS e_and_i_forecast_8_plus_4_amount, ROUND(SUM(CASE WHEN UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS e_and_i_variance_amount, ROUND((SUM(CASE WHEN UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN UPPER(line_of_business) = UPPER("E&I") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS e_and_i_variance_percent, ROUND(SUM(CASE WHEN UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS m_and_r_actuals_amount, ROUND(SUM(CASE WHEN UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS m_and_r_forecast_8_plus_4_amount, ROUND(SUM(CASE WHEN UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS m_and_r_variance_amount, ROUND((SUM(CASE WHEN UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN UPPER(line_of_business) = UPPER("M&R") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS m_and_r_variance_percent, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS overall_total_actuals_amount, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS overall_total_forecast_8_plus_4_amount, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS overall_total_variance_amount, ROUND((SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS overall_total_variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) IN (UPPER("GAAP"), UPPER("8+4")) AND UPPER(product_category) = UPPER("PBM") GROUP BY metric_type ORDER BY metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- THIRTEENTH INSERT: LOB Report - Comprehensive (MoM + YoY + Quarterly) Actuals vs Forecast 8+4
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
    'show me LOB report or line of business report compare actuals vs forecast 8+4 for PBM month over month (september vs august 2025) or september 2025 vs september 2024 or Q3 2025 vs Q3 2024',
    'SELECT metric_type, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS august_2025_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 8 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS august_2025_c_and_s_forecast_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2025_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS september_2025_c_and_s_forecast_amount, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS september_2024_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2024_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_c_and_s_actuals_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" AND UPPER(line_of_business) = UPPER("C&S") AND UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS q3_2025_c_and_s_forecast_amount, ...[Similar patterns for E&I, M&R, and OVERALL_TOTAL]... FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE ((year = 2025 AND month IN (8, 9)) OR (year = 2024 AND month = 9) OR (year IN (2024, 2025) AND quarter = "Q3")) AND UPPER(ledger) IN (UPPER("GAAP"), UPPER("8+4")) AND UPPER(product_category) = UPPER("PBM") GROUP BY metric_type ORDER BY metric_type',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- ============================================
-- KEY METRICS REPORT (III METRICS)
-- Calculated metrics only - PBM level
-- No product hierarchy breakdown
-- ============================================

-- FOURTEENTH INSERT: Key Metrics Report - Actuals vs Forecast 8+4 (September 2025)
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
    'show me key metrics report or III metrics report compare actuals vs forecast 8+4 for september 2025 for PBM',
    'SELECT "Revenue Per Script_Unadjusted" AS metric_name, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS actuals_value, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS forecast_8_plus_4_value, ROUND((SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0)) - (SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0)), 3) AS variance_value, ROUND(((SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0)) - (SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0))) / NULLIF((SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0)), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) IN (UPPER("GAAP"), UPPER("8+4")) AND UPPER(product_category) = UPPER("PBM") AND UPPER(metric_type) IN (UPPER("Revenues"), UPPER("Unadjusted Scripts")) UNION ALL SELECT "Revenue Per Script_Adjusted" AS metric_name, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS actuals_value, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS forecast_8_plus_4_value, ROUND((SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0)) - (SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0)), 3) AS variance_value, ROUND(((SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0)) - (SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0))) / NULLIF((SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0)), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) IN (UPPER("GAAP"), UPPER("8+4")) AND UPPER(product_category) = UPPER("PBM") AND UPPER(metric_type) IN (UPPER("Revenues"), UPPER("Adjusted Scripts")) UNION ALL ...[Similar UNION ALL for remaining 17 calculated metrics: Cost Per Script Unadjusted/Adjusted, Cost%, Gross Margin%, IOI Per Script Unadjusted/Adjusted, IOI%, OP Cost Per Script, OP Exp Per Script, Operating Expense%, Utilization PMPM, SP Capture%]...',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- FIFTEENTH INSERT: Key Metrics Report - Forecast 5+7 vs Forecast 8+4 (September 2025)
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
    'show me key metrics report or III metrics report compare forecast 5+7 vs forecast 8+4 for september 2025 for PBM',
    'SELECT "Revenue Per Script_Unadjusted" AS metric_name, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS forecast_5_plus_7_value, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS forecast_8_plus_4_value, ROUND((SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0)) - (SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0)), 3) AS variance_value, ROUND(((SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0)) - (SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0))) / NULLIF((SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") THEN amount_or_count ELSE 0 END), 0)), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) IN (UPPER("5+7"), UPPER("8+4")) AND UPPER(product_category) = UPPER("PBM") AND UPPER(metric_type) IN (UPPER("Revenues"), UPPER("Unadjusted Scripts")) UNION ALL SELECT "Revenue Per Script_Adjusted" AS metric_name, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS forecast_5_plus_7_value, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0), 3) AS forecast_8_plus_4_value, ROUND((SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0)) - (SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0)), 3) AS variance_value, ROUND(((SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0)) - (SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0))) / NULLIF((SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Revenues") THEN amount_or_count ELSE 0 END) / NULLIF(SUM(CASE WHEN UPPER(ledger) = UPPER("5+7") AND UPPER(metric_type) = UPPER("Adjusted Scripts") THEN amount_or_count ELSE 0 END), 0)), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 9 AND UPPER(ledger) IN (UPPER("5+7"), UPPER("8+4")) AND UPPER(product_category) = UPPER("PBM") AND UPPER(metric_type) IN (UPPER("Revenues"), UPPER("Adjusted Scripts")) UNION ALL ...[Similar UNION ALL for remaining 17 calculated metrics: Cost Per Script Unadjusted/Adjusted, Cost%, Gross Margin%, IOI Per Script Unadjusted/Adjusted, IOI%, OP Cost Per Script, OP Exp Per Script, Operating Expense%, Utilization PMPM, SP Capture%]...',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);
