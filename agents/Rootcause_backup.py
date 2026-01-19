📝 Query preview:
        SELECT
            column_name,
            table_name,
            value_lower,
            CASE WHEN value_lower = 'mpdova' THEN 1 WHEN value_lower LIKE 'mpdova%' THEN 2 ELSE 7 END as tier
        FROM prd_optumrx_orxfdmprdsa.rag.column_values_index
        WHERE value_lower = 'mpdova' OR value_lower LIKE 'mpdova%'
        ORDER BY tier, column_name, value_lower
        ...
SQL execution attempt 1/3
Databricks response keys: dict_keys(['statement_id', 'status'])
Initial query state: PENDING
Query still running, polling for results (timeout: 300s)
🔄 Polling for results of statement 01f0f529-e7bb-17b8-9437-8eeea2c45697 (max 300s)
  ⏱️ Elapsed: 0.0s, checking status...
    Status: SUCCEEDED
  Query completed successfully after 0.0s
✅ Found 3 total matches before tier filtering
❌ Error in search_column_values: 'DatabricksClient' object has no attribute '_apply_tier_group_filter'
Traceback (most recent call last):
  File "C:\Users\msivaku1\Documents\AgentBot-Async\core\databricks_client.py", line 2195, in search_column_values
    rows = self._apply_tier_group_filter(rows)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'DatabricksClient' object has no attribute '_apply_tier_group_filter'
📊 Found 0 filter metadata matches
filter
 No specific filter values found in metadata.
