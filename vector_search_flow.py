"""
Vector Search Flow in LLM Router Agent - Step by Step Example
"""

def demonstrate_vector_search_flow():
    """Show exactly how vector search works in the LLM Router Agent"""
    
    # Example user question
    user_question = "Why are Q3 pharmacy claims 18% higher than forecast?"
    
    print("=" * 60)
    print("VECTOR SEARCH FLOW DEMONSTRATION")
    print("=" * 60)
    
    # STEP 1: LLM generates intelligent search queries
    print("\n🧠 STEP 1: LLM Generates Search Strategy")
    print(f"User Question: {user_question}")
    
    # LLM would generate something like this:
    llm_generated_searches = {
        "search_queries": [
            {
                "query_text": "pharmacy claims variance analysis forecast budget deviation healthcare finance",
                "purpose": "Find datasets with pharmacy claims and forecast comparison capabilities",
                "weight": 0.8
            },
            {
                "query_text": "claims transaction data quarterly analysis member utilization pharmacy costs",
                "purpose": "Find detailed pharmacy claims transaction data for variance analysis", 
                "weight": 0.7
            },
            {
                "query_text": "financial variance budget actual forecast healthcare expense tracking",
                "purpose": "Find financial datasets for variance and budget comparison",
                "weight": 0.6
            }
        ]
    }
    
    print("Generated Search Queries:")
    for i, query in enumerate(llm_generated_searches['search_queries'], 1):
        print(f"  {i}. '{query['query_text']}'")
        print(f"     Purpose: {query['purpose']}")
        print(f"     Weight: {query['weight']}")
    
    # STEP 2: Execute vector searches
    print("\n🔍 STEP 2: Execute Vector Searches Against Your Databricks Index")
    
    # This is what happens in _perform_intelligent_vector_search()
    all_results = []
    
    for i, search_query in enumerate(llm_generated_searches['search_queries'], 1):
        print(f"\n  Executing Search {i}:")
        print(f"  Query: {search_query['query_text']}")
        
        # 🎯 THIS IS WHERE THE ACTUAL VECTOR SEARCH HAPPENS
        # db_client.vector_search_tables() calls:
        vector_search_sql = f"""
        SELECT table_name, table_summary as content, table_kg 
        FROM VECTOR_SEARCH(
            index => 'prd_optumrx_orxfdmprdsa.rag.table_chunks',
            query_text => '{search_query['query_text']}',
            num_results => 3
        )
        """
        
        print(f"  SQL Generated: {vector_search_sql}")
        
        # Simulated results (what you'd get from your Databricks vector search)
        mock_results = [
            {
                "table_name": "v_f_clm_transaction",
                "content": "Pharmacy claim-level adjudicated data with monetary values, claim identifiers, member details...",
                "table_kg": '{"table_name":"v_f_clm_transaction","columns":[{"column_name":"CLAIM_NBR","data_type":"bigint"}...]}'
            },
            {
                "table_name": "v_d_budget_forecast", 
                "content": "Budget and forecast data for financial variance analysis...",
                "table_kg": '{"table_name":"v_d_budget_forecast","columns":[{"column_name":"BUDGET_AMOUNT","data_type":"decimal"}...]}'
            }
        ]
        
        # Add search metadata to each result
        for result in mock_results:
            result['search_purpose'] = search_query['purpose']
            result['search_weight'] = search_query['weight']
            result['search_query'] = search_query['query_text']
            all_results.append(result)
        
        print(f"  → Found {len(mock_results)} tables")
        for result in mock_results:
            print(f"    - {result['table_name']}")
    
    # STEP 3: Deduplicate and rank results
    print(f"\n📊 STEP 3: Consolidate Results")
    print(f"Total results from all searches: {len(all_results)}")
    
    # Remove duplicates (same table from multiple searches)
    unique_tables = {}
    for result in all_results:
        table_name = result['table_name']
        if table_name not in unique_tables:
            unique_tables[table_name] = result
        else:
            # Combine search purposes if table found in multiple searches
            existing = unique_tables[table_name]
            existing['search_purpose'] += f" | {result['search_purpose']}"
            existing['search_weight'] = max(existing['search_weight'], result['search_weight'])
    
    final_results = list(unique_tables.values())
    print(f"Unique tables after deduplication: {len(final_results)}")
    
    for result in final_results:
        print(f"  - {result['table_name']} (weight: {result['search_weight']})")
        print(f"    Purposes: {result['search_purpose']}")
    
    # STEP 4: LLM analyzes and selects best dataset
    print(f"\n🎯 STEP 4: LLM Expert Selection")
    print("LLM analyzes each dataset against the user's question...")
    
    # The LLM would receive the final_results and make expert selection
    simulated_llm_selection = {
        "selected_dataset": "v_f_clm_transaction",
        "selection_confidence": 0.92,
        "expert_reasoning": "v_f_clm_transaction is optimal because it contains detailed pharmacy claim data with monetary values needed for variance analysis. The table includes claim amounts, member details, and temporal data required to compare Q3 actuals against forecasts.",
        "suitability_scores": {
            "relevance": 0.95,
            "completeness": 0.90,
            "granularity": 0.88
        }
    }
    
    print(f"Selected: {simulated_llm_selection['selected_dataset']}")
    print(f"Confidence: {simulated_llm_selection['selection_confidence']:.1%}")
    print(f"Reasoning: {simulated_llm_selection['expert_reasoning'][:100]}...")
    
    return final_results, simulated_llm_selection

# The key function where vector search actually happens
def show_actual_vector_search_calls():
    """Show the exact databricks calls"""
    
    print("\n" + "="*60)
    print("ACTUAL DATABRICKS VECTOR SEARCH CALLS")
    print("="*60)
    
    print("\n1. In databricks_client.py - vector_search_tables() method:")
    print("""
    def vector_search_tables(self, query_text: str, num_results: int = 5):
        # 🎯 THIS IS THE ACTUAL VECTOR SEARCH CALL
        sql_query = f'''
        SELECT table_name, table_summary as content, table_kg 
        FROM VECTOR_SEARCH(
            index => '{self.VECTOR_TBL_INDEX}',  # Your index: prd_optumrx_orxfdmprdsa.rag.table_chunks
            query_text => '{query_text.replace("'", "''")}',
            num_results => {num_results}
        )
        '''
        
        # Execute via Databricks SQL API
        response = requests.post(
            f"{self.DATABRICKS_HOST}/api/2.0/sql/statements/",
            headers={"Authorization": f"Bearer {self.DATABRICKS_TOKEN}"},
            json={
                "warehouse_id": self.SQL_WAREHOUSE_ID,
                "statement": sql_query,
                "disposition": "INLINE"
            }
        )
        
        return parsed_results
    """)
    
    print("\n2. In LLM Router Agent - _perform_intelligent_vector_search():")
    print("""
    # For each LLM-generated search query:
    for search_query in search_config['search_queries']:
        
        # 🎯 VECTOR SEARCH CALL HAPPENS HERE
        results = self.db_client.vector_search_tables(
            search_query['query_text'],  # LLM-optimized query
            num_results=3
        )
        
        # Results come back from your Databricks vector index
        # Each result contains: table_name, content, table_kg
    """)
    
    print("\n3. What gets sent to Databricks:")
    print("""
    Example queries sent to your vector index:
    
    Query 1: "pharmacy claims variance analysis forecast budget deviation healthcare finance"
    → Databricks Vector Search finds tables matching this semantic meaning
    
    Query 2: "claims transaction data quarterly analysis member utilization pharmacy costs"  
    → Finds more granular transaction-level datasets
    
    Query 3: "financial variance budget actual forecast healthcare expense tracking"
    → Finds budget/forecast comparison datasets
    """)

if __name__ == "__main__":
    demonstrate_vector_search_flow()
    show_actual_vector_search_calls()
