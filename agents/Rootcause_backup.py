{
  "prd_optumrx_orxfdmprdsa.rag.pbm_claims": [
    "**submit_date**: Exact claim submission date. Use for daily trending and time-window filters. format is YYYY-MM-DD.",
    "**year**: Calendar year of claim submission; supports YoY comparisons.contains like 2025.",
    "**month**: Calendar month of submission; it has numerical value (1-12).",
    "**quarter**: Calendar quarter; supports quarter-over-quarter analysis.contains Q1,Q2,Q3,Q4",
    "**client_id**: used for client analysis.This contain unique 5-6 digit client id.Sample values [MDOVA,PDIND,MDCSP,57939]",
    "**unadjusted_script_count**: Raw script/claim count per event. Use for total volume and as the denominator for revenue_per_script."
  ],
  
  "prd_optumrx_orxfdmprdsa.rag.claim_billing": [
    "**invoice_date**: (YYYY-MM-DD) format. Date when the invoice was generated and issued to the client.",
    "**client_id**: This contain unique 5-6 digit client code.Sample values [ABCD,WXYZ,57939]",
    "**inv_nbr**: The invoice number is a unique identifier used to distinguish individual invoices within the billing system."
  ],
  
  "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast": [
    "**ledger**: Allowed values: GAAP, BUDGET, 8+4, 5+7, 2+10.If the question does not mention actuals, forecast, or budget, set ledger = GAAP.",
    "**metric_type**: Allowed values [COGS Post Reclass,SG&A Post Reclass,IOI,Revenues,Unadjusted Scripts,etc]. Always include GROUP BY metric_type when comparing actuals vs forecast.",
    "**transaction_date**: Exact transaction date (YYYY-MM-DD). Supports monthly, quarterly, and annual trend analysis.",
    "**year**: Four-digit year of the transaction.",
    "**month**: Numeric month of the transaction (1-12)."
  ]
}

"""
Single JSON file loader with caching for optimal performance.
"""

import json
from pathlib import Path
from typing import Dict, List
from functools import lru_cache

# Path to single JSON file
MANDATORY_EMBEDDINGS_FILE = Path(__file__).parent / "mandatory_embeddings.json"


@lru_cache(maxsize=1)
def _load_mandatory_embeddings_raw() -> Dict[str, List[str]]:
    """
    Load all mandatory embeddings from JSON file.
    Cached permanently - only loads once per process.
    """
    try:
        with open(MANDATORY_EMBEDDINGS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        total_embeddings = sum(len(v) for v in data.values())
        print(f"✅ Loaded {total_embeddings} mandatory embeddings for {len(data)} tables from JSON")
        
        return data
        
    except FileNotFoundError:
        print(f"⚠️  Mandatory embeddings file not found: {MANDATORY_EMBEDDINGS_FILE}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in mandatory embeddings file: {e}")
        return {}
    except Exception as e:
        print(f"❌ Error loading mandatory embeddings: {e}")
        return {}


def get_mandatory_embeddings_for_tables(tables_list: List[str]) -> Dict[str, List[str]]:
    """
    Get mandatory embeddings for specified tables.
    Ultra-fast after first call (cached in memory).
    """
    # Load all embeddings (cached)
    all_embeddings = _load_mandatory_embeddings_raw()
    
    # Filter to requested tables
    result = {}
    for table in tables_list:
        if table in all_embeddings:
            result[table] = all_embeddings[table]
        else:
            print(f"⚠️  No mandatory embeddings defined for table: {table}")
    
    return result


def clear_cache():
    """Clear cache to force reload (useful during development)"""
    _load_mandatory_embeddings_raw.cache_clear()
    print("✅ Mandatory embeddings cache cleared - will reload on next access")


def get_cache_info():
    """Get cache statistics"""
    return _load_mandatory_embeddings_raw.cache_info()


def list_available_tables() -> List[str]:
    """Get list of tables that have mandatory embeddings defined"""
    all_embeddings = _load_mandatory_embeddings_raw()
    return list(all_embeddings.keys())


from config.mandatory_embeddings_loader import get_mandatory_embeddings_for_tables

async def get_metadata(self, state: Dict, selected_dataset: list) -> Dict:
    """Extract metadata with mandatory embeddings from single JSON file"""
    try:
        current_question = state.get('rewritten_question', state.get('current_question', ''))
        embedding_idx = "prd_optumrx_orxfdmprdsa.rag.column_embeddings_pbm_idx"
        
        tables_list = selected_dataset if isinstance(selected_dataset, list) else [selected_dataset] if selected_dataset else []
        print(f'📊 Tables selected: {tables_list}')
        
        # ===== STEP 1: Load Mandatory Embeddings (Cached - Fast!) =====
        mandatory_contexts = get_mandatory_embeddings_for_tables(tables_list)
        mandatory_count = sum(len(v) for v in mandatory_contexts.values())
        
        # ===== STEP 2: Get Vector Search Results =====
        vector_results = await self.db_client.sp_vector_search_columns(
            query_text=current_question,
            tables_list=tables_list,
            num_results_per_table=15,
            index_name=embedding_idx
        )
        
        # ===== STEP 3: Group Vector Results =====
        vector_contexts_by_table = {}
        for result in vector_results:
            table = result['table_name']
            if table not in vector_contexts_by_table:
                vector_contexts_by_table[table] = []
            vector_contexts_by_table[table].append(result['llm_context'])
        
        # ===== STEP 4: Merge & Deduplicate =====
        merged_contexts_by_table = {}
        
        for table in tables_list:
            seen_columns = set()
            merged_contexts_by_table[table] = []
            
            # Add mandatory first
            if table in mandatory_contexts:
                for ctx in mandatory_contexts[table]:
                    col_name = ctx.split('**')[1].split(':')[0].strip() if ctx.startswith('**') and ':' in ctx else None
                    if col_name:
                        seen_columns.add(col_name)
                    merged_contexts_by_table[table].append(ctx)
            
            # Add vector results (skip duplicates)
            if table in vector_contexts_by_table:
                for ctx in vector_contexts_by_table[table]:
                    col_name = ctx.split('**')[1].split(':')[0].strip() if ctx.startswith('**') and ':' in ctx else None
                    if col_name and col_name not in seen_columns:
                        merged_contexts_by_table[table].append(ctx)
                        seen_columns.add(col_name)
        
        # ===== STEP 5: Build Metadata =====
        metadata = ""
        for table in tables_list:
            if table in merged_contexts_by_table and merged_contexts_by_table[table]:
                metadata += f"\n## Table: {table}\n\n"
                for ctx in merged_contexts_by_table[table]:
                    metadata += ctx + "\n"
                metadata += "\n"
        
        total_columns = sum(len(v) for v in merged_contexts_by_table.values())
        print(f'✅ Metadata: {total_columns} columns ({mandatory_count} mandatory + {len(vector_results)} vector)')
        
        return {
            'status': 'success',
            'metadata': metadata,
            'total_tables': len(tables_list),
            'mandatory_columns': mandatory_count,
            'vector_columns': len(vector_results),
            'total_columns': total_columns,
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
