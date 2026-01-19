from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, collect_list, concat_ws, array_distinct, flatten, array_sort
import logging

def extract_distinct_column_values(spark, schema_name, final_table_name):
    """
    Extract distinct values for each column across multiple tables and store in final table.
    Handles different actual column names across tables, converts to lowercase, and deduplicates.
    Unity Catalog compatible - uses DataFrame operations only (no RDD).
    
    Args:
        spark: SparkSession
        schema_name: Schema name (e.g., 'prd_optumrx_orxfdmprdsa.rag')
        final_table_name: Final table to store results (e.g., 'prd_optumrx_orxfdmprdsa.rag.column_distinct_values')
    """
    
    # Column mapping: standardized_name -> {table_name: actual_column_name}
    column_mapping = {
        'carrier_id': {
            'claim_billing': 'carrier_id',
            'pbm_claims': 'carrier_id'
        },
        'account_id': {
            'claim_billing': 'account_id',
            'pbm_claims': 'account_id'
        },
        'group_id': {
            'claim_billing': 'group_id',
            'pbm_claims': 'group_id'
        },
        'client_id': {
            'claim_billing': 'client_id',
            'pbm_claims': 'client_id',
            'ledger_actual_vs_forecast': 'ora_client_id'
        },
        'client_description': {
            'claim_billing': 'client_description',
            'pbm_claims': 'client_name'
           
        },
        'oracle_prod_code': {
            'claim_billing': 'oracle_prod_code'
        },
        'orcl_prod_desc': {
            'claim_billing': 'orcl_prod_desc'
        },
        'actvty_category_cd': {
            'claim_billing': 'actvty_category_cd'
        },
        'actvty_cat_desc': {
            'claim_billing': 'actvty_cat_desc'
        },
        'blng_entty_cd': {
            'claim_billing': 'blng_entty_cd'
        },
        'blng_entty_name': {
            'claim_billing': 'blng_entty_name'
        },
        'line_of_business': {
            'claim_billing': 'line_of_business_name',
            'pbm_claims': 'line_of_business',
            'ledger_actual_vs_forecast': 'line_of_business'
        },
        'product_sub_category_lvl_1': {
            'ledger_actual_vs_forecast': 'product_sub_category_lvl_1'
        },
        'product_sub_category_lvl_2': {
            'ledger_actual_vs_forecast': 'product_sub_category_lvl_2'
        },
        'pharmacy_name': {
            'pbm_claims': 'pharmacy_name'
        },
        'drug_name': {
            'claim_billing': 'drug_name',
            'pbm_claims': 'drug_name'
        },
        'therapy_class_name': {
            'claim_billing': 'therapy_class_name',
            'pbm_claims': 'therapy_class_name'
        },
        'drg_lbl_nm': {
            'claim_billing': 'drg_lbl_nm',
            'pbm_claims': 'drg_lbl_nm'
        },
        'client_type': {
            'pbm_claims': 'client_type'
        }
    }
    
    # Create final table if not exists
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {final_table_name} (
        column_name STRING,
        distinct_values STRING
    )
    """
    spark.sql(create_table_query)
    logging.info(f"Final table {final_table_name} ready")
    
    results = []
    
    # Process each standardized column
    for std_column_name, table_columns in column_mapping.items():
        logging.info(f"Processing column: {std_column_name}")
        
        # Build UNION query for all tables containing this column
        union_queries = []
        for table_name, actual_col_name in table_columns.items():
            query = f"""
                SELECT DISTINCT LOWER(CAST({actual_col_name} AS STRING)) as value 
                FROM {schema_name}.{table_name} 
                WHERE {actual_col_name} IS NOT NULL 
                  AND TRIM(CAST({actual_col_name} AS STRING)) != ''
            """
            union_queries.append(query)
        
        # Combine all queries with UNION
        full_query = " UNION ".join(union_queries)
        
        try:
            # Execute query and get distinct values as DataFrame
            df = spark.sql(full_query)
            
            # Collect values using DataFrame operations (no RDD)
            df_sorted = df.orderBy("value")
            distinct_values_list = [row.value for row in df_sorted.collect()]
            
            # Join with comma
            distinct_values_str = ','.join(distinct_values_list)
            
            results.append((std_column_name, distinct_values_str))
            
            logging.info(f"  ✓ {std_column_name}: {len(distinct_values_list)} distinct values")
            
        except Exception as e:
            logging.error(f"  ✗ Error processing {std_column_name}: {str(e)}")
            results.append((std_column_name, f"ERROR: {str(e)}"))
    
    # Insert results into final table
    if results:
        # Create DataFrame from results
        results_df = spark.createDataFrame(results, ["column_name", "distinct_values"])
        
        # Truncate and insert
        spark.sql(f"TRUNCATE TABLE {final_table_name}")
        results_df.write.mode("append").saveAsTable(final_table_name)
        
        logging.info(f"✓ Successfully inserted {len(results)} rows into {final_table_name}")
    
    return results


spark = SparkSession.builder.getOrCreate()
results = extract_distinct_column_values(
    spark=spark,
    schema_name='prd_optumrx_orxfdmprdsa.rag',
    final_table_name='prd_optumrx_orxfdmprdsa.rag.column_distinct_values'
)

%sql
DROP TABLE IF EXISTS prd_optumrx_orxfdmprdsa.rag.column_values_index;
-- Create pre-exploded index table
CREATE TABLE prd_optumrx_orxfdmprdsa.rag.column_values_index AS
SELECT 
    column_name,
    LOWER(TRIM(value)) as value_lower
FROM prd_optumrx_orxfdmprdsa.rag.column_distinct_values
LATERAL VIEW EXPLODE(SPLIT(distinct_values, ',')) AS value
WHERE TRIM(value) IS NOT NULL 
  AND TRIM(value) != '';
  -- Optimize for search performance
OPTIMIZE prd_optumrx_orxfdmprdsa.rag.column_values_index ZORDER BY (column_name, value_lower);
===================================================================================================

async def search_column_values(
        self,
        search_terms: List[str],
        max_columns: int = 7,
        max_values_per_column: int = 5
    ) -> List[str]:
        """
        Search column values with regex word boundary matching.
        
        Single word: Simple LIKE
        Multi-word: 
            - Tier 1: Exact match
            - Tier 2: Prefix match
            - Tier 3: Contains full phrase
            - Tier 4: ALL words with boundary
            - Tier 5: FIRST N-1 words (fallback, only if Tier 1-4 empty)
            - Tier 6: FIRST 3 words (fallback, only if Tier 1-5 empty)
        """
        
        if not search_terms:
            return []
        
        # Escape single quotes for SQL
        def escape_sql(val):
            return val.replace("'", "''")
        
        # Build word boundary regex pattern for a word
        def word_boundary_pattern(word):
            """
            Creates regex pattern for word with boundary.
            Allows optional 's' for plurals.
            """
            escaped = escape_sql(word)
            return f"(^|[^a-z]){escaped}(s)?($|[^a-z0-9])"
        
        # Build combined RLIKE condition for list of words
        def build_rlike_condition(words):
            conditions = [f"value_lower RLIKE '{word_boundary_pattern(w)}'" for w in words]
            return " AND ".join(conditions)
        
        # Process search terms
        search_items = []
        
        for term in search_terms:
            term_lower = term.strip().lower()
            if not term_lower:
                continue
            
            words = term_lower.split()
            is_multi_word = len(words) > 1
            
            search_items.append({
                "phrase": term_lower,
                "words": words,
                "is_multi_word": is_multi_word,
                "word_count": len(words)
            })
        
        if not search_items:
            return []
        
        print(f"🔍 Search items: {[s['phrase'] for s in search_items]}")
        
        # Build SQL conditions for WHERE clause
        all_conditions = []
        
        for item in search_items:
            phrase = item["phrase"]
            words = item["words"]
            is_multi_word = item["is_multi_word"]
            word_count = item["word_count"]
            phrase_escaped = escape_sql(phrase)
            
            if is_multi_word:
                # Exact, prefix, contains
                all_conditions.append(f"value_lower = '{phrase_escaped}'")
                all_conditions.append(f"value_lower LIKE '{phrase_escaped}%'")
                all_conditions.append(f"value_lower LIKE '%{phrase_escaped}%'")
                
                # Tier 4: All words
                all_conditions.append(f"({build_rlike_condition(words)})")
                
                # Tier 5: First N-1 words (for 3+ words)
                if word_count >= 3:
                    first_n_minus_1 = words[:word_count - 1]
                    all_conditions.append(f"({build_rlike_condition(first_n_minus_1)})")
                
                # Tier 6: First 3 words (for 5+ words)
                if word_count >= 5:
                    first_3 = words[:3]
                    all_conditions.append(f"({build_rlike_condition(first_3)})")
            else:
                # Single word: exact, prefix, contains
                all_conditions.append(f"value_lower = '{phrase_escaped}'")
                all_conditions.append(f"value_lower LIKE '{phrase_escaped}%'")
                all_conditions.append(f"value_lower LIKE '%{phrase_escaped}%'")
        
        where_clause = " OR ".join(all_conditions)
        
        # Build tier CASE statement
        tier_cases = []
        
        for item in search_items:
            phrase = item["phrase"]
            words = item["words"]
            is_multi_word = item["is_multi_word"]
            word_count = item["word_count"]
            phrase_escaped = escape_sql(phrase)
            
            # Tier 1: Exact
            tier_cases.append(f"WHEN value_lower = '{phrase_escaped}' THEN 1")
            
            # Tier 2: Prefix
            tier_cases.append(f"WHEN value_lower LIKE '{phrase_escaped}%' THEN 2")
            
            # Tier 3: Contains full phrase
            tier_cases.append(f"WHEN value_lower LIKE '%{phrase_escaped}%' THEN 3")
            
            if is_multi_word:
                # Tier 4: All words with boundary
                tier_cases.append(f"WHEN {build_rlike_condition(words)} THEN 4")
                
                # Tier 5: First N-1 words (for 3+ words)
                if word_count >= 3:
                    first_n_minus_1 = words[:word_count - 1]
                    tier_cases.append(f"WHEN {build_rlike_condition(first_n_minus_1)} THEN 5")
                
                # Tier 6: First 3 words (for 5+ words)
                if word_count >= 5:
                    first_3 = words[:3]
                    tier_cases.append(f"WHEN {build_rlike_condition(first_3)} THEN 6")
        
        tier_case_statement = "CASE " + " ".join(tier_cases) + " ELSE 7 END"
        
        # Build query
        query = f"""
        SELECT 
            column_name,
            value_lower,
            {tier_case_statement} as tier
        FROM prd_optumrx_orxfdmprdsa.rag.column_values_index
        WHERE {where_clause}
        ORDER BY tier, column_name, value_lower
        """
        
        print(f"🔍 Executing search query...")
        print(f"📝 Query preview: {query[:500]}...")
        
        try:
            # Execute query
            result_data = await self.execute_sql_async_audit(query)
            
            # Convert to list
            if hasattr(result_data, 'collect'):
                rows = [row.asDict() for row in result_data.collect()]
            elif isinstance(result_data, list):
                rows = result_data
            else:
                print(f"❌ Unexpected result type: {type(result_data)}")
                return []
            
            if not rows:
                print("❌ No matches found")
                return []
            
            # Convert tier to int (may come as string from SQL)
            for row in rows:
                row["tier"] = int(row["tier"])
            
            # Filter out Tier 7 (no match)
            rows = [r for r in rows if r["tier"] <= 6]
            
            if not rows:
                print("❌ No valid matches found")
                return []
            
            print(f"✅ Found {len(rows)} total matches before tier filtering")
            
            # Apply exclusive tier group logic
            rows = self._apply_tier_group_filter(rows)
            
            if not rows:
                print("❌ No matches after tier group filtering")
                return []
            
            print(f"✅ Found {len(rows)} matches after tier filtering")
            
            # Group by column
            column_data = {}
            
            for row in rows:
                col = row["column_name"]
                val = row["value_lower"]
                tier = row["tier"]
                
                if col not in column_data:
                    column_data[col] = {"best_tier": tier, "values": []}
                
                # Update best tier
                if tier < column_data[col]["best_tier"]:
                    column_data[col]["best_tier"] = tier
                
                # Add value if not duplicate
                existing_values = [v[0] for v in column_data[col]["values"]]
                if val not in existing_values:
                    column_data[col]["values"].append((val, tier))
            
            # Sort columns by best tier, then column name
            sorted_columns = sorted(
                column_data.items(),
                key=lambda x: (x[1]["best_tier"], x[0])
            )
            
            # Format output
            results = []
            for col_name, col_info in sorted_columns[:max_columns]:
                # Sort values by tier, then alphabetically
                sorted_values = sorted(col_info["values"], key=lambda x: (x[1], x[0]))
                
                # Take top N values
                top_values = [v[0] for v in sorted_values[:max_values_per_column]]
                values_str = ", ".join(top_values)
                
                results.append(f"{col_name} - {values_str}")
                print(f"   Tier {col_info['best_tier']}: {col_name} → {top_values}")
            
            print(f"✅ Search complete: {len(results)} columns")
            return results
            
        except Exception as e:
            print(f"❌ Error in search_column_values: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    
    def _apply_tier_group_filter(self, rows: List[dict]) -> List[dict]:
        """
        Apply exclusive tier group filtering.
        
        Groups:
            - Primary (Tier 1-4): Always return if any match exists
            - Fallback 1 (Tier 5): Only if Tier 1-4 are ALL empty
            - Fallback 2 (Tier 6): Only if Tier 1-5 are ALL empty
        """
        
        # Separate rows by tier group
        primary_rows = [r for r in rows if r["tier"] <= 4]
        fallback_1_rows = [r for r in rows if r["tier"] == 5]
        fallback_2_rows = [r for r in rows if r["tier"] == 6]
        
        print(f"   📊 Tier groups: Primary(1-4)={len(primary_rows)}, Fallback1(5)={len(fallback_1_rows)}, Fallback2(6)={len(fallback_2_rows)}")
        
        # Return based on priority
        if primary_rows:
            print(f"   ✅ Returning Primary tier (1-4) results")
            return primary_rows
        
        if fallback_1_rows:
            print(f"   ✅ Returning Fallback 1 tier (5) results")
            return fallback_1_rows
        
        if fallback_2_rows:
            print(f"   ✅ Returning Fallback 2 tier (6) results")
            return fallback_2_rows
        
        return []

