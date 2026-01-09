📝 Query preview:
        SELECT
            column_name,
            value_lower,
            CASE WHEN value_lower = 'egwp fee' THEN 1 WHEN value_lower LIKE 'egwp fee%' THEN 2 WHEN value_lower LIKE '%egwp fee%' THEN 3 WHEN value_lower RLIKE '(^|[^a-z])egwp(s)?([^a-z]|$)' AND value_lower RLIKE '(^|[^a-z])fee(s)?([^a-z]|$)' THEN 4 ELSE 5 END as tier
        FROM prd_optumrx_orxfdmprdsa.rag.column_values_index
        WHERE value_lower = 'egwp fee' OR value_lower LIKE 'egwp fee%' OR value_lower LIKE '%egwp fee%'...
SQL execution attempt 1/3
Databricks response keys: dict_keys(['statement_id', 'status'])
Initial query state: PENDING
Query still running, polling for results (timeout: 300s)
🔄 Polling for results of statement 01f0ed6b-7d4f-14ee-b09b-9dbef44a3036 (max 300s)
  ⏱️ Elapsed: 0.0s, checking status...
    Status: SUCCEEDED
  Query completed successfully after 0.0s
❌ Error in search_column_values: '<=' not supported between instances of 'str' and 'int'
Traceback (most recent call last):
  File "C:\Users\msivaku1\Documents\AgentBot-Async\core\databricks_client.py", line 2134, in search_column_values
    rows = [r for r in rows if r["tier"] <= 4]
                               ^^^^^^^^^^^^^^
TypeError: '<=' not supported between instances of 'str' and 'int'
