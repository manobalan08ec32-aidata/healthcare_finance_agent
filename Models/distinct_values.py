# ============================================================================
# PART 1: BACKEND JOB - Data Collection (Run this as scheduled job in Databricks)
# ============================================================================

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------
# Configuration - Define your tables and columns

table_column_config = {
    "table_A": ["drug_name", "category", "description"],
    "table_B": ["product_name", "type"],
    "table_C": ["item_name"]
}

# COMMAND ----------
# Function to get distinct values for a table-column combination

def get_distinct_values_for_column(table_name, column_name):
    """
    Gets all distinct values from a specific column and returns them comma-separated
    """
    try:
        query = f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL"
        df = spark.sql(query)
        
        # Collect distinct values and join with comma
        distinct_values = df.select(column_name).rdd.flatMap(lambda x: x).collect()
        distinct_values_str = ", ".join([str(val) for val in distinct_values])
        
        return (table_name, column_name, distinct_values_str)
    except Exception as e:
        print(f"Error processing {table_name}.{column_name}: {str(e)}")
        return (table_name, column_name, "")

# COMMAND ----------
# Process all tables and columns to create the distinct values table

print("Starting data collection...")
distinct_values_data = []

for table_name, columns in table_column_config.items():
    print(f"Processing table: {table_name}")
    for column_name in columns:
        result = get_distinct_values_for_column(table_name, column_name)
        distinct_values_data.append(result)
        print(f"  ✓ Column: {column_name}")

# Create schema for the distinct values table
schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("distinct_values", StringType(), True)
])

# Create DataFrame
distinct_values_df = spark.createDataFrame(distinct_values_data, schema=schema)

# Save to a table
distinct_values_df.write.mode("overwrite").saveAsTable("distinct_values_metadata")

print("=" * 80)
print("✓ BACKEND JOB COMPLETE: distinct_values_metadata table created")
print("=" * 80)
display(distinct_values_df)

# COMMAND ----------


# ============================================================================
# PART 2: STREAMLIT INTEGRATION - SQL-Based Filtering
# ============================================================================

"""
This section provides the SQL query and Python function for Streamlit integration.
The SQL approach is optimized for performance and scalability.
"""

# COMMAND ----------
# OPTION A: Pure SQL Query (Copy this to use in Streamlit)

"""
-- SQL Query Template for Streamlit
-- This uses LIKE for case-insensitive partial matching

SELECT 
    table_name,
    column_name,
    distinct_values,
    matched_values
FROM (
    SELECT 
        table_name,
        column_name,
        distinct_values,
        -- Filter and extract only matched values
        array_join(
            filter(
                split(distinct_values, ','),
                value -> (
                    lower(trim(value)) LIKE '%covid%' OR 
                    lower(trim(value)) LIKE '%glp-1%'
                )
            ),
            ', '
        ) AS matched_values
    FROM distinct_values_metadata
)
WHERE matched_values != '';
"""

# COMMAND ----------
# OPTION B: Python Function for Streamlit (OPTIMIZED - Single Table Scan)

def search_metadata_sql(filter_list, spark_session=None):
    """
    Searches the distinct_values_metadata table using Spark SQL.
    OPTIMIZED: Single table scan with regex pattern matching.
    
    Args:
        filter_list: List of strings to search for (e.g., ['covid', 'glp-1'])
        spark_session: Spark session object (optional, uses global spark if not provided)
    
    Returns:
        DataFrame with columns: table_name, column_name, matched_values
    
    Example:
        results = search_metadata_sql(['covid', 'diabetes'])
        results.show()
    """
    
    if spark_session is None:
        spark_session = spark
    
    # Build a single regex pattern for all filter terms
    # Pattern: (term1|term2|term3) - matches any of the terms
    escaped_terms = [term.replace('\\', '\\\\').replace('|', '\\|').replace('(', '\\(').replace(')', '\\)') 
                     for term in filter_list]
    regex_pattern = '|'.join(escaped_terms)
    
    # OPTIMIZED SQL: Single table scan with regex
    query = f"""
    WITH exploded_values AS (
        SELECT 
            table_name,
            column_name,
            trim(value) AS individual_value
        FROM distinct_values_metadata
        LATERAL VIEW explode(split(distinct_values, ',')) AS value
    ),
    filtered_values AS (
        SELECT 
            table_name,
            column_name,
            individual_value
        FROM exploded_values
        WHERE lower(individual_value) RLIKE '(?i)({regex_pattern})'
    )
    SELECT 
        table_name,
        column_name,
        concat_ws(', ', collect_list(individual_value)) AS matched_values
    FROM filtered_values
    GROUP BY table_name, column_name
    ORDER BY table_name, column_name
    """
    
    result_df = spark_session.sql(query)
    return result_df


# OPTION C: Alternative High-Performance Approach (For very large datasets)

def search_metadata_sql_exploded(filter_list, spark_session=None):
    """
    Alternative approach: Pre-explode and cache for multiple searches.
    Best for scenarios where you'll run multiple searches in a session.
    
    Args:
        filter_list: List of strings to search for
        spark_session: Spark session object
    
    Returns:
        DataFrame with matched results
    """
    
    if spark_session is None:
        spark_session = spark
    
    # Check if exploded view exists, if not create it
    try:
        spark_session.sql("SELECT 1 FROM distinct_values_exploded LIMIT 1")
    except:
        # Create and cache exploded view (run once per session)
        spark_session.sql("""
            CREATE OR REPLACE TEMP VIEW distinct_values_exploded AS
            SELECT 
                table_name,
                column_name,
                trim(value) AS individual_value
            FROM distinct_values_metadata
            LATERAL VIEW explode(split(distinct_values, ',')) AS value
        """)
        spark_session.sql("CACHE TABLE distinct_values_exploded")
    
    # Build regex pattern
    escaped_terms = [term.replace('\\', '\\\\').replace('|', '\\|') for term in filter_list]
    regex_pattern = '|'.join(escaped_terms)
    
    # Fast query on cached exploded data
    query = f"""
    SELECT 
        table_name,
        column_name,
        concat_ws(', ', collect_list(individual_value)) AS matched_values
    FROM distinct_values_exploded
    WHERE lower(individual_value) RLIKE '(?i)({regex_pattern})'
    GROUP BY table_name, column_name
    ORDER BY table_name, column_name
    """
    
    result_df = spark_session.sql(query)
    return result_df

# COMMAND ----------
# Test the optimized function with sample filters

test_filters = ['covid', 'glp-1', 'diabetes']
print(f"Testing OPTIMIZED approach with filters: {test_filters}")
print("=" * 80)

# Test Option B (recommended for most cases)
result = search_metadata_sql(test_filters)
print("Results using single regex scan:")
display(result)

print("\n" + "=" * 80)

# Test Option C (for multiple searches in same session)
result_cached = search_metadata_sql_exploded(test_filters)
print("Results using cached exploded view (faster for multiple searches):")
display(result_cached)

# COMMAND ----------


# ============================================================================
# STREAMLIT INTEGRATION CODE
# ============================================================================

"""
Copy this code to your Streamlit application:
"""

# streamlit_app.py
"""
import streamlit as st
from pyspark.sql import SparkSession
import re

# Initialize Spark session (configure based on your setup)
@st.cache_resource
def get_spark_session():
    return SparkSession.builder \\
        .appName("Streamlit Metadata Search") \\
        .config("spark.sql.warehouse.dir", "/path/to/warehouse") \\
        .getOrCreate()

spark = get_spark_session()

# OPTIMIZED search function
def search_metadata(filter_list):
    '''
    Single table scan with regex pattern matching - OPTIMIZED
    '''
    # Build regex pattern from filter list
    escaped_terms = [re.escape(term) for term in filter_list]
    regex_pattern = '|'.join(escaped_terms)
    
    query = f'''
    WITH exploded_values AS (
        SELECT 
            table_name,
            column_name,
            trim(value) AS individual_value
        FROM distinct_values_metadata
        LATERAL VIEW explode(split(distinct_values, ',')) AS value
    ),
    filtered_values AS (
        SELECT 
            table_name,
            column_name,
            individual_value
        FROM exploded_values
        WHERE lower(individual_value) RLIKE '(?i)({regex_pattern})'
    )
    SELECT 
        table_name,
        column_name,
        concat_ws(', ', collect_list(individual_value)) AS matched_values
    FROM filtered_values
    GROUP BY table_name, column_name
    ORDER BY table_name, column_name
    '''
    
    return spark.sql(query)

# Streamlit UI
st.title("🔍 Metadata Search Tool")
st.write("Search for values across your data catalog - **Optimized for Performance**")

# User input for filter terms
filter_input = st.text_input(
    "Enter search terms (comma-separated)", 
    placeholder="e.g., covid, glp-1, diabetes"
)

# Search button
if st.button("🚀 Search", type="primary") and filter_input:
    # Parse the input
    filter_list = [term.strip() for term in filter_input.split(',') if term.strip()]
    
    st.info(f"Searching for: **{', '.join(filter_list)}**")
    
    with st.spinner("Searching... (Single optimized query)"):
        # Execute OPTIMIZED query - single table scan
        result_df = search_metadata(filter_list)
        
        # Convert to Pandas for display
        result_pd = result_df.toPandas()
    
    if len(result_pd) > 0:
        st.success(f"✅ Found {len(result_pd)} matches!")
        
        # Display results
        st.dataframe(
            result_pd, 
            use_container_width=True,
            height=400
        )
        
        # Expandable section for each result
        with st.expander("📊 View Detailed Results"):
            for idx, row in result_pd.iterrows():
                st.markdown(f"**Table:** `{row['table_name']}` | **Column:** `{row['column_name']}`")
                st.text(row['matched_values'])
                st.divider()
        
        # Download option
        csv = result_pd.to_csv(index=False)
        st.download_button(
            label="📥 Download Results as CSV",
            data=csv,
            file_name=f"search_results_{len(filter_list)}_terms.csv",
            mime="text/csv"
        )
        
        # Performance info
        st.caption(f"⚡ Optimized search completed with single table scan for {len(filter_list)} filter term(s)")
    else:
        st.warning("❌ No matches found. Try different search terms.")

# Sidebar with info
with st.sidebar:
    st.header("ℹ️ About")
    st.write("This tool searches across all tables and columns in your data warehouse.")
    st.write("**Performance:** Single optimized SQL query with regex pattern matching")
    st.write("**Matching:** Case-insensitive partial matching")
"""

# COMMAND ----------
# Alternative: REST API approach for Streamlit

"""
If you prefer a REST API approach, use Databricks SQL endpoints:

1. Create a SQL endpoint in Databricks
2. Use the Databricks SQL Connector in Streamlit:

from databricks import sql

connection = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/endpoints/your-endpoint-id",
    access_token="your-token"
)

cursor = connection.cursor()
cursor.execute(query)
results = cursor.fetchall()
"""
