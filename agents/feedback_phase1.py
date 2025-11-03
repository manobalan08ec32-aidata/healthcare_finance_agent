%sql
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
    'SELECT product_category, product_sub_category_lvl_2, year, month, ROUND(SUM(amount_or_count), 0) AS revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_2, year, month ORDER BY year DESC, month DESC',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);
%sql
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
    'SELECT product_category, product_sub_category_lvl_2, metric_type, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("GAAP") THEN amount_or_count ELSE 0 END), 0) AS actuals_amount, ROUND(SUM(CASE WHEN UPPER(ledger) = UPPER("8+4") THEN amount_or_count ELSE 0 END), 0) AS forecast_8_4_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 7 AND UPPER(product_category) = UPPER("PBM") AND UPPER(ledger) IN (UPPER("GAAP"), UPPER("8+4")) GROUP BY product_category, product_sub_category_lvl_2, metric_type ORDER BY product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

%sql
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
    'SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_revenue_amount, ROUND(SUM(CASE WHEN month = 2 THEN amount_or_count ELSE 0 END), 0) AS february_revenue_amount, ROUND(SUM(CASE WHEN month = 3 THEN amount_or_count ELSE 0 END), 0) AS march_revenue_amount, ROUND(SUM(CASE WHEN month = 4 THEN amount_or_count ELSE 0 END), 0) AS april_revenue_amount, ROUND(SUM(CASE WHEN month = 5 THEN amount_or_count ELSE 0 END), 0) AS may_revenue_amount, ROUND(SUM(CASE WHEN month = 6 THEN amount_or_count ELSE 0 END), 0) AS june_revenue_amount, ROUND(SUM(CASE WHEN month = 7 THEN amount_or_count ELSE 0 END), 0) AS july_revenue_amount, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_revenue_amount, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
)
%sql
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
    'SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2024_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2025_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS variance_amount, ROUND((SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") AND UPPER(product_category) = UPPER("PBM") GROUP BY product_category, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);
%sql
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
    'SELECT product_category, product_sub_category_lvl_2, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_script_count, ROUND(SUM(CASE WHEN month = 2 THEN amount_or_count ELSE 0 END), 0) AS february_script_count, ROUND(SUM(CASE WHEN month = 3 THEN amount_or_count ELSE 0 END), 0) AS march_script_count, ROUND(SUM(CASE WHEN month = 4 THEN amount_or_count ELSE 0 END), 0) AS april_script_count, ROUND(SUM(CASE WHEN month = 5 THEN amount_or_count ELSE 0 END), 0) AS may_script_count, ROUND(SUM(CASE WHEN month = 6 THEN amount_or_count ELSE 0 END), 0) AS june_script_count, ROUND(SUM(CASE WHEN month = 7 THEN amount_or_count ELSE 0 END), 0) AS july_script_count, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_script_count, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_script_count FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Unadjusted Scripts") AND UPPER(product_category) = UPPER("Specialty") GROUP BY product_category, product_sub_category_lvl_2',
    true,
    NULL,
    current_timestamp(),
    'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

%sql
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'what is the network product revenue or revenue for each line of business for July 2025',
    'SELECT line_of_business, ROUND(SUM(amount_or_count), 0) AS revenue_amount FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month = 7 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") GROUP BY line_of_business ORDER BY line_of_business',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- QUESTION 7: What is the revenue for each line of business from Jan to Sept 2025?
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'what is the revenue for each line of business from jan to sept 2025 for PBM',
    'SELECT product_category, line_of_business, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS jan_rev_amt, ROUND(SUM(CASE WHEN month = 2 THEN amount_or_count ELSE 0 END), 0) AS february_rev_amt, ROUND(SUM(CASE WHEN month = 3 THEN amount_or_count ELSE 0 END), 0) AS march_rev_amt, ROUND(SUM(CASE WHEN month = 4 THEN amount_or_count ELSE 0 END), 0) AS april_rev_amt, ROUND(SUM(CASE WHEN month = 5 THEN amount_or_count ELSE 0 END), 0) AS may_rev_amt, ROUND(SUM(CASE WHEN month = 6 THEN amount_or_count ELSE 0 END), 0) AS june_rev_amt, ROUND(SUM(CASE WHEN month = 7 THEN amount_or_count ELSE 0 END), 0) AS july_rev_amt, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_rev_amt, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_rev_amt FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") and product_category="PBM" GROUP BY line_of_business, product_category ORDER BY line_of_business, product_category, product_category',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

%sql
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'compare revenue for each line of business Q3 2025 vs Q3 2024',
    'SELECT line_of_business, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2024_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2025_revenue_amount, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS variance_amount, ROUND((SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) = UPPER("Revenues") GROUP BY line_of_business ORDER BY line_of_business',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);


INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me adjusted script and unadjusted script count for Home Delivery or HDP from jan - September 2025',
    'SELECT product_category, product_sub_category_lvl_1, metric_type, ROUND(SUM(CASE WHEN month = 1 THEN amount_or_count ELSE 0 END), 0) AS january_count, ROUND(SUM(CASE WHEN month = 2 THEN amount_or_count ELSE 0 END), 0) AS february_count, ROUND(SUM(CASE WHEN month = 3 THEN amount_or_count ELSE 0 END), 0) AS march_count, ROUND(SUM(CASE WHEN month = 4 THEN amount_or_count ELSE 0 END), 0) AS april_count, ROUND(SUM(CASE WHEN month = 5 THEN amount_or_count ELSE 0 END), 0) AS may_count, ROUND(SUM(CASE WHEN month = 6 THEN amount_or_count ELSE 0 END), 0) AS june_count, ROUND(SUM(CASE WHEN month = 7 THEN amount_or_count ELSE 0 END), 0) AS july_count, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_count, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_count FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month BETWEEN 1 AND 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Adjusted Scripts"), UPPER("Unadjusted Scripts")) AND UPPER(product_category) = UPPER("Home Delivery") GROUP BY product_category, product_sub_category_lvl_1, metric_type ORDER BY metric_type',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- QUESTION 10: Scripts current month vs previous month (Sep 2025 vs Aug 2025)
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me adjusted and unadjusted script count for Home Delivery current month vs previous month',
    'SELECT product_category, product_sub_category_lvl_1, metric_type, ROUND(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS august_2025_count, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_count, ROUND(SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) AS variance_count, ROUND((SUM(CASE WHEN month = 9 THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN month = 8 THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year = 2025 AND month IN (8, 9) AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Adjusted Scripts"), UPPER("Unadjusted Scripts")) AND UPPER(product_category) = UPPER("Home Delivery") GROUP BY product_category, product_sub_category_lvl_1, metric_type ORDER BY metric_type',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

-- QUESTION 11: Scripts current month this year vs last year (Sep 2025 vs Sep 2024)
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me adjusted and unadjusted script count for Home Delivery current month this year vs last year this month',
    'SELECT product_category, product_sub_category_lvl_1, metric_type, ROUND(SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2024_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS september_2025_count, ROUND(SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) AS variance_count, ROUND((SUM(CASE WHEN year = 2025 AND month = 9 THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND month = 9 THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND month = 9 AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Adjusted Scripts"), UPPER("Unadjusted Scripts")) AND UPPER(product_category) = UPPER("Home Delivery") GROUP BY product_category, product_sub_category_lvl_1, metric_type ORDER BY metric_type',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

%sql
INSERT INTO prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking (
    session_id, user_id, user_question, state_info, positive_feedback, feedback, insert_ts, table_name
)
VALUES (
    '28f6f8e3-5889-4e5b-aba9-58da52ad4791', 'sivakumm@optum.com',
    'show me adjusted and unadjusted script count for (Home Delivery or Mail or HDP) current quarter this year vs last year this quarter',
    'SELECT product_category, product_sub_category_lvl_1, metric_type, ROUND(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2024_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS q3_2025_count, ROUND(SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) AS variance_count, ROUND((SUM(CASE WHEN year = 2025 AND quarter = "Q3" THEN amount_or_count ELSE 0 END) - SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END)) / NULLIF(SUM(CASE WHEN year = 2024 AND quarter = "Q3" THEN amount_or_count ELSE 0 END), 0) * 100, 3) AS variance_percent FROM prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast WHERE year IN (2024, 2025) AND quarter = "Q3" AND UPPER(ledger) = UPPER("GAAP") AND UPPER(metric_type) IN (UPPER("Adjusted Scripts"), UPPER("Unadjusted Scripts")) AND UPPER(product_category) = UPPER("Home Delivery") GROUP BY product_category, product_sub_category_lvl_1, metric_type ORDER BY metric_type',
    true, NULL, current_timestamp(), 'prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast'
);

