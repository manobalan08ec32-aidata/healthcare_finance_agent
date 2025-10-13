async def get_metadata(self, state: Dict, selected_dataset: list) -> Dict:
        """Extract metadata with mandatory embeddings - simplified direct merge"""
        try:
            current_question = state.get('rewritten_question', state.get('current_question', ''))
            selection_reasoning = state.get('selection_reasoning', '')
            filter_context = state.get('selected_filter_context', '')
            
            if state.get('requires_dataset_clarification', False):

                followup_reasoning = state.get('followup_reasoning', '')
            else:
                followup_reasoning = ''

            # Concatenate current_question, selection_reasoning, and filter_context for query_text
            query_text = f"{current_question} {followup_reasoning} {selection_reasoning} {filter_context} ".strip()

            embedding_idx = "prd_optumrx_orxfdmprdsa.rag.column_embeddings_test_idx"
            
            tables_list = selected_dataset if isinstance(selected_dataset, list) else [selected_dataset] if selected_dataset else []
            print(f'📊 query text selected: {query_text}')
            
            # ===== STEP 1: Load Mandatory Embeddings (Cached - Fast!) =====
            mandatory_contexts = get_mandatory_embeddings_for_tables(tables_list)
            print(f'✅ Mandatory contexts loaded: {len(mandatory_contexts)} tables')
            
            # ===== STEP 2: Get Vector Search Results =====
            print(f'🔍 Searching: {query_text!r} across {len(tables_list)} table(s)')
            vector_results = await self.db_client.sp_vector_search_columns(
                query_text=query_text,
                tables_list=tables_list,
                num_results_per_table=50,
                index_name=embedding_idx
            )
            
            # ===== STEP 3: Group Vector Results by Table =====
            vector_contexts_by_table = {}
            for result in vector_results:
                table = result['table_name']
                if table not in vector_contexts_by_table:
                    vector_contexts_by_table[table] = []
                # Collect all llm_context strings for this table
                vector_contexts_by_table[table].append(result['llm_context'])
            
            # Log what we got
            for table, contexts in vector_contexts_by_table.items():
                print(f'  ✅ {table}: {len(contexts)} vector contexts')
            
            # ===== STEP 4: Build Metadata (Simple Merge - No Deduplication) =====
            metadata = ""
            
            for table in tables_list:
                metadata += f"\n## Table: {table}\n\n"
                
                # Add mandatory contexts first (if any)
                if table in mandatory_contexts:
                    for ctx in mandatory_contexts[table]:
                        metadata += ctx + "\n"
                
                # Add vector contexts (if any)
                if table in vector_contexts_by_table:
                    for ctx in vector_contexts_by_table[table]:
                        # Clean up the context (remove leading/trailing whitespace and newlines)
                        clean_ctx = ctx.strip()
                        if clean_ctx:  # Only add non-empty contexts
                            metadata += clean_ctx + "\n"
                
                metadata += "\n"  # Extra line between tables

            print("embeddings metadata",metadata)

            return {
                'status': 'success',
                'metadata': metadata,
                'error': False
            }
            
        except Exception as e:
            print(f"❌ Metadata extraction failed: {str(e)}")
            import traceback
            traceback.print_exc()
            
            return {
                'status': 'error',
                'metadata': '',
                'error': True,
                'error_message': f"Metadata extraction failed: {str(e)}"
            }
