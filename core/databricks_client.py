async def get_metadata(self, state: Dict, selected_dataset: list) -> Dict:
    """Extract metadata from mandatory embeddings JSON file"""
    try:
        tables_list = selected_dataset if isinstance(selected_dataset, list) else [selected_dataset] if selected_dataset else []
        
        print(f'📊 Loading metadata for {len(tables_list)} table(s): {tables_list}')
        
        # ===== STEP 1: Load Mandatory Embeddings (Full Metadata) =====
        mandatory_contexts = get_mandatory_embeddings_for_tables(tables_list)
        print(f'✅ Mandatory contexts loaded: {len(mandatory_contexts)} tables')
        
        # ===== STEP 2: Build Metadata from Mandatory Contexts =====
        metadata = ""
        
        for table in tables_list:
            metadata += f"## Table: {table}\n\n"
            
            # Add all contexts for this table
            if table in mandatory_contexts:
                for ctx in mandatory_contexts[table]:
                    # Clean up the context (remove leading/trailing whitespace)
                    clean_ctx = ctx.strip()
                    if clean_ctx:  # Only add non-empty contexts
                        metadata += clean_ctx + "\n"
                
                print(f'  ✅ {table}: {len(mandatory_contexts[table])} contexts added')
            else:
                print(f'  ⚠️ {table}: No metadata found in mandatory embeddings')
            
            metadata += "\n"  # Extra line between tables

        print(f"\n📋 Total metadata length: {len(metadata)} characters")

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
