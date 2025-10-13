def __init__(self):
    # ... all your existing __init__ code ...
    
    # ADD THESE LINES AT THE END:
    self._vector_client: Optional[VectorSearchClient] = None
    self._vector_indexes: Dict[str, Any] = {}  # Cache indexes by name

async def _get_vector_client(self) -> VectorSearchClient:
    """Lazy-load and cache the VectorSearchClient"""
    if self._vector_client is None:
        self._vector_client = VectorSearchClient(
            workspace_url=self.DATABRICKS_HOST,
            service_principal_client_id=self.DATABRICKS_CLIENT_ID,
            service_principal_client_secret=self.DATABRICKS_CLIENT_SECRET,
            azure_tenant_id=self.DATABRICKS_TENANT_ID
        )
    return self._vector_client

async def _get_vector_index(self, index_name: str):
    """Get and cache vector search index"""
    if index_name not in self._vector_indexes:
        client = await self._get_vector_client()
        index = client.get_index(
            endpoint_name="metadata_vectore_search_endpoint",
            index_name=index_name
        )
        index._get_token_for_request = lambda: self.access_token
        self._vector_indexes[index_name] = index
    return self._vector_indexes[index_name]

async def sp_vector_search_columns(
    self, 
    query_text: str, 
    tables_list: List[str], 
    num_results_per_table: int,
    index_name: str
) -> List[Dict]:
    """
    Search columns across multiple tables in parallel with caching.
    For each table: Fetch 50 results, rerank, keep top 10.
    """
    print(f"🔍 Searching: '{query_text}' across {len(tables_list)} table(s)")
    
    # ✅ Use cached index (eliminates the wait!)
    index = await self._get_vector_index(index_name)
    
    # ✅ Define async search function for one table
    async def search_table(table_name: str):
        print(f'📋 Searching table: {table_name}')
        try:
            from databricks.vector_search.reranker import DatabricksReranker
            
            results = index.similarity_search(
                query_text=query_text,
                columns=["table_name", "column_name", "col_embedding_content", "llm_context"],
                filters={"table_name": table_name},
                reranker=DatabricksReranker(
                    columns_to_rerank=["column_name", "llm_context"]
                ),
                num_results=50,  # Fetch 50
                query_type="Hybrid"
            )
            
            if results.get('result', {}).get('data_array'):
                cols = [c['name'] for c in results['manifest']['columns']]
                table_results = [dict(zip(cols, row)) for row in results['result']['data_array']]
                
                # Sort by score and keep top 10
                table_results.sort(key=lambda x: x.get('score', 0.0), reverse=True)
                top_10 = table_results[:10]
                
                print(f"  ✅ {table_name}: {len(top_10)} columns")
                return top_10
            else:
                print(f"  ⚠️ {table_name}: No results")
                return []
                
        except Exception as e:
            print(f"  ❌ {table_name}: Error - {e}")
            import traceback
            traceback.print_exc()
            return []
    
    # ✅ Search all tables in parallel
    results_per_table = await asyncio.gather(*[search_table(t) for t in tables_list])
    
    # Flatten results
    all_results = [item for sublist in results_per_table for item in sublist]
    
    print(f"\n✅ Total: {len(all_results)} columns from {len(tables_list)} table(s)")
    return all_results
