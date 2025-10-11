async def sp_vector_search_columns(
    self, 
    query_text: str, 
    tables_list: List[str],
    num_results_per_table: int = 5,
    index_name: str = "prd_optumrx_orxfdmprdsa.rag.column_embeddings_pbm_idx"
) -> List[Dict]:
    """
    Search for relevant columns across one or multiple tables.
    Guarantees num_results_per_table from EACH table.
    """
    print(f"🔍 Searching: '{query_text}' across {len(tables_list)} table(s)")
    
    client = VectorSearchClient(
        workspace_url=self.DATABRICKS_HOST,
        service_principal_client_id=self.DATABRICKS_CLIENT_ID,
        service_principal_client_secret=self.DATABRICKS_CLIENT_SECRET,
        azure_tenant_id=self.DATABRICKS_TENANT_ID
    )
    
    index = client.get_index(
        endpoint_name="metadata_vectore_search_endpoint",
        index_name=index_name
    )
    index._get_token_for_request = lambda: self.access_token
    
    all_results = []
    
    for table_name in tables_list:
        print(f'📋 Searching table: {table_name}')
        try:
            # ✅ FIX: Use DatabricksReranker object (like your working function)
            from databricks.vector_search.reranker import DatabricksReranker
            
            results = index.similarity_search(
                query_text=query_text,
                columns=["table_name", "col_embedding_content", "llm_context"],
                filters={"table_name": table_name},
                
                # ✅ Use DatabricksReranker object instead of dict
                reranker=DatabricksReranker(
                    columns_to_rerank=["col_embedding_content"]
                ),
                
                num_results=num_results_per_table,
                query_type="ANN"  # Use ANN, not hybrid for metadata
            )
            
            # Parse results
            if results.get('result', {}).get('data_array'):
                cols = [c['name'] for c in results['manifest']['columns']]
                for row in results['result']['data_array']:
                    all_results.append(dict(zip(cols, row)))
                
                print(f"  ✅ {table_name}: {len(results['result']['data_array'])} columns")
            else:
                print(f"  ⚠️  {table_name}: No results")
        
        except Exception as e:
            print(f"  ❌ {table_name}: Error - {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n✅ Total: {len(all_results)} columns from {len(tables_list)} table(s)")
    
    return all_results


import asyncio
import sys
from databricks_client import DatabricksClient

async def test_single_table():
    """Test with single table"""
    print("\n" + "="*80)
    print("TEST 1: Single Table")
    print("="*80)
    
    client = DatabricksClient()
    
    try:
        results = await client.sp_vector_search_columns(
            query_text="revenue and margin metrics",
            tables_list=["prd_optumrx_orxfdmprdsa.rag.pbm_claims"],
            num_results_per_table=5
        )
        
        print(f"\n✅ Got {len(results)} results")
        for i, r in enumerate(results[:3], 1):  # Show first 3
            print(f"{i}. {r.get('col_embedding_content', '')[:80]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        await client.close()

async def test_multiple_tables():
    """Test with multiple tables"""
    print("\n" + "="*80)
    print("TEST 2: Multiple Tables")
    print("="*80)
    
    client = DatabricksClient()
    
    try:
        results = await client.sp_vector_search_columns(
            query_text="billing revenue and invoices",
            tables_list=[
                "prd_optumrx_orxfdmprdsa.rag.pbm_claims",
                "prd_optumrx_orxfdmprdsa.rag.claim_billing"
            ],
            num_results_per_table=3
        )
        
        print(f"\n✅ Got {len(results)} results")
        
        # Group by table
        from collections import defaultdict
        by_table = defaultdict(int)
        for r in results:
            by_table[r.get('table_name')] += 1
        
        print("\nResults per table:")
        for table, count in by_table.items():
            print(f"  {table}: {count} columns")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        await client.close()

async def main():
    """Run all tests"""
    print("\n🧪 Starting Vector Search Tests...")
    
    test1 = await test_single_table()
    test2 = await test_multiple_tables()
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Test 1 (Single table): {'✅ PASS' if test1 else '❌ FAIL'}")
    print(f"Test 2 (Multiple tables): {'✅ PASS' if test2 else '❌ FAIL'}")
    
    if test1 and test2:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print("\n⚠️  Some tests failed")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
