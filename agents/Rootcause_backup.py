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
            'pbm_claims': 'client_name',
            'ledger_actual_vs_forecast': 'ora_client_description'
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


# Usage example:
# spark = SparkSession.builder.getOrCreate()
# results = extract_distinct_column_values(
#     spark=spark,
#     schema_name='prd_optumrx_orxfdmprdsa.rag',
#     final_table_name='prd_optumrx_orxfdmprdsa.rag.column_distinct_values'
# )
