from typing import Dict, List, Optional, Any
import json
import asyncio
import re
import concurrent.futures
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from config.mandatory_embeddings_loader import get_mandatory_embeddings_for_tables
from core.logger import setup_logger, log_with_user_context

# Initialize logger for this module
logger = setup_logger(__name__)

class LLMRouterAgent:
    """Enhanced router agent with dataset selection and SQL generation"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        self.max_retries = 3
    
    def _log(self, level: str, message: str, state: AgentState = None, **extra):
        """Helper method to log with session context from state"""
        user_email = state.get('user_email') if state else None
        session_id = state.get('session_id') if state else None
        log_with_user_context(logger, level, f"[Router] {message}", user_email, session_id, **extra)
    
    def _format_json_compact(self, data: Any) -> str:
        """Format JSON with compact arrays (keep arrays on single lines)"""
        def format_value(obj, indent_level=0):
            indent = "  " * indent_level
            next_indent = "  " * (indent_level + 1)
            
            if isinstance(obj, dict):
                lines = ["{"]
                items = list(obj.items())
                for i, (key, value) in enumerate(items):
                    comma = "," if i < len(items) - 1 else ""
                    if isinstance(value, (list, dict)):
                        lines.append(f'{next_indent}"{key}": {format_value(value, indent_level + 1)}{comma}')
                    else:
                        lines.append(f'{next_indent}"{key}": {json.dumps(value)}{comma}')
                lines.append(f"{indent}}}")
                return "\n".join(lines)
            
            elif isinstance(obj, list):
                if not obj:
                    return "[]"
                # Check if all items are simple types (strings, numbers, booleans)
                if all(isinstance(item, (str, int, float, bool, type(None))) for item in obj):
                    # Keep array on single line
                    return json.dumps(obj)
                else:
                    # If array contains objects, format each on new line
                    lines = ["["]
                    for i, item in enumerate(obj):
                        comma = "," if i < len(obj) - 1 else ""
                        lines.append(f"{next_indent}{format_value(item, indent_level + 1)}{comma}")
                    lines.append(f"{indent}]")
                    return "\n".join(lines)
            
            else:
                return json.dumps(obj)
        
        return format_value(data)
        
    async def select_dataset(self, state: AgentState) -> Dict[str, any]:
        """Enhanced dataset selection with complete workflow handling"""
        # Initialize control flow variables
        create_sql = False
        create_sql_after_followup = False
        metadata_result = None
        selected_dataset = None
        selection_reasoning = ''
        functional_names = []
        filter_metadata_results = []  # Initialize to prevent UnboundLocalError

        # Priority 1: Check if this is a dataset clarification follow-up
        if state.get('requires_dataset_clarification', False) and not state.get('is_sql_followup', False):
            self._log('info', "Processing dataset clarification follow-up", state)
            # Get existing filter metadata from state if available
            existing_filter_metadata = state.get('filter_metadata_results', [])
            result = await self._fix_router_llm_call(state, existing_filter_metadata)
           
            if result.get('error', False):
                self._log('error', "Dataset selection failed", state, error=result.get('error_message'))
                return {
                    'error':True,
                    'error_message': result.get('error_message', 'Unknown error occurred during dataset selection')
                    }
            elif result.get('topic_drift', False):
                return {'topic_drift':True }
            # After clarification, extract metadata and set flag for SQL generation
            else:

                state['selected_dataset'] = result.get('selected_dataset', [])
                state['selection_reasoning'] = result.get('selection_reasoning', '')
                state['functional_names'] = result.get('final_functional_table_names', [])
                selected_dataset = result.get('selected_dataset', [])
                
                # Store selected filter context if available
                selected_filter_context = result.get('selected_filter_context')
                if selected_filter_context:
                    state['selected_filter_context'] = selected_filter_context
                    print(f"🎯 Storing filter context for SQL generation: {selected_filter_context}")

                metadata_result = await self.get_metadata(
                    state=state,
                    selected_dataset=result.get('selected_dataset', [])
                )

                if metadata_result.get('error', False):
                    return {
                        'error': True,
                        'error_message': "Databricks vector search failed"
                    }
                
                # Update state with metadata and selected dataset
                state['dataset_metadata'] = metadata_result.get('metadata', '')
                
                create_sql = True
        
        # Priority 2: Check if this is a SQL follow-up (user answered SQL clarification)
        elif state.get('is_sql_followup', False):
            print(f"🔄 Processing SQL follow-up answer")
            # Set is_sql_followup to False after processing
            state['is_sql_followup'] = False
            create_sql_after_followup = True

        # Priority 3: Normal flow - initial call
        else:
            user_question = state.get('current_question', state.get('original_question', ''))

            if not user_question:
                raise Exception("No user question found in state")
            
            # 1. Check for filter values and search metadata if available
            filter_values = state.get('filter_values', [])
            filter_metadata_results = []
            
            if filter_values:
                print(f"🔍 Filter values detected: {filter_values}")
                domain_selection = state.get('domain_selection', '')
            
            # 2. Call search_column_values (NEW) if filter values exist
            # FILTER OUT common categorical values (case-insensitive), pass only meaningful values
            common_categorical_values = {'pbm','pbm retail', 'hdp', 'home delivery', 'mail', 'specialty','sp','claim fee','claim cost','admin fee','claimfee','claimcost','adminfee','8+4','9+3','2+10','5+7','optum','retail'}
            print("Current Timestamp before filter value call", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            # Filter out common categorical values
            meaningful_filter_values = []
            if filter_values:
                filter_values_lower = [str(val).lower().strip() for val in filter_values]
                meaningful_filter_values = [
                    filter_values[i] for i, val in enumerate(filter_values_lower) 
                    if val not in common_categorical_values
                ]
                
                if meaningful_filter_values:
                    print(f"🔍 Filtered filter values (removed categories): {meaningful_filter_values}")
                else:
                    print(f"⏭️ Skipping metadata search - all filter values are common categories: {filter_values}")
            
            metadata_search_task = None
            if meaningful_filter_values:
                print(f"🔍 Searching column index for filter values: {meaningful_filter_values}")
                    # Call async function directly (already in async context)
                metadata_search_task = self.db_client.search_column_values(
                    meaningful_filter_values,
                    max_columns=7,
                    max_values_per_column=5
                )

            
            # 3. Execute metadata search if it was initiated
            print('metadata_search_task:',metadata_search_task)
            if metadata_search_task:
                try:
                    filter_metadata_result = await metadata_search_task
                    print(f"📊 Found {len(filter_metadata_result)} filter metadata matches")
                    if filter_metadata_result:
                        filter_metadata_results = "\n**Columns found with this filter value**\n"
                        for result in filter_metadata_result:
                            filter_metadata_results += f"- {result}\n"
                    else:
                        filter_metadata_results = "\n No specific filter values found in metadata.\n"
                    print('filter', filter_metadata_results)
                    # Store in state for persistence
                    state['filter_metadata_results'] = filter_metadata_results
                except Exception as e:
                    print(f"⚠️ Filter metadata search failed: {e}")
                    filter_metadata_results = []
                    state['filter_metadata_results'] = []
            
            print("Current Timestamp  after filter value call", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            # 🆕 NEW: Check if user pre-selected datasets from UI dropdown
            user_selected_datasets = state.get('user_selected_datasets', [])
            domain_selection = state.get('domain_selection', '')
            print(f"🎯 User selected datasets from UI: {user_selected_datasets}")
            
            # 🆕 OPTIMIZATION: If user selected exactly ONE dataset, skip LLM call and use metadata.json
            if user_selected_datasets and len(user_selected_datasets) == 1:
                selected_functional_name = user_selected_datasets[0]
                print(f"✅ User pre-selected single dataset: {selected_functional_name}")
                
                # Load metadata.json to get the actual table name
                try:
                    metadata_config_path = "config/metadata/metadata.json"
                    with open(metadata_config_path, 'r') as f:
                        metadata_config = json.load(f)
                        # Get the array for the current domain
                        domain_datasets = metadata_config.get(domain_selection, [])
                    print(f"✅ Loaded metadata config from: {metadata_config_path} for domain '{domain_selection}'")
                except Exception as e:
                    print(f"❌ Failed loading metadata config: {e}")
                    return {
                        'error': True,
                        'error_message': f"Failed to load metadata config: {str(e)}"
                    }
                
                # Find the matching table from metadata config
                # domain_datasets is a list of dicts like: [{"Peoplesoft GL": "table_name"}, {"Rx Claims": "table_name"}]
                actual_table_name = None
                
                for dataset_dict in domain_datasets:
                    # Each dataset_dict has one key-value pair: functional_name -> actual_table_name
                    if selected_functional_name in dataset_dict:
                        actual_table_name = dataset_dict[selected_functional_name]
                        break
                
                if actual_table_name:
                    print(f"✅ Matched '{selected_functional_name}' to actual table: {actual_table_name}")
                    
                    # Skip LLM call - directly use user selection
                    selection_result = {
                        'status': 'success',
                        'final_actual_tables': [actual_table_name],
                        'functional_names': [selected_functional_name],
                        'selection_reasoning': f"User pre-selected {selected_functional_name} from UI dropdown",
                        'selected_filter_context': '',
                        'error': False
                    }
                    print(f"⚡ Skipped LLM call - using user's single dataset selection from metadata.json")
                else:
                    print(f"⚠️ Could not find matching table for '{selected_functional_name}' in metadata.json")
                    return {
                        'error': True,
                        'error_message': f"Dataset '{selected_functional_name}' not found in metadata configuration"
                    }
            else:
                # Normal flow: Multiple selections or no selection - Load dataset config and call LLM
                print(f"📊 Loading dataset config for LLM selection")
                
                mapped_dataset_file = None
                if domain_selection == 'PBM Network':
                    mapped_dataset_file = 'pbm_dataset.json'
                elif domain_selection == 'Optum Pharmacy':
                    mapped_dataset_file = 'pharmacy_dataset.json'
                else:
                    # Default fallback if domain not recognized
                    raise Exception("Domain Not Found")
                
                print(f"📂 Domain selection in router: {mapped_dataset_file}")
                search_results = []
                try:
                    drillthrough_config_path = f"config/datasets/{mapped_dataset_file}"
                    with open(drillthrough_config_path, 'r') as f:
                        search_results = json.load(f)
                    print(f"✅ Loaded dataset config from: {drillthrough_config_path} for domain '{domain_selection}'")
                    print(f"📊 Found {len(search_results)} datasets in config")
                except Exception as e:
                    print(f"❌ Failed loading dataset config {mapped_dataset_file}: {e}")
                    return {
                        'error': True,
                        'error_message': f"Failed to load dataset config {mapped_dataset_file}: {str(e)}"
                    }

                if not search_results:
                    raise Exception("No datasets found in config")
                
                # Call LLM for dataset selection
                if user_selected_datasets and len(user_selected_datasets) > 1:
                    print(f"📊 User selected {len(user_selected_datasets)} datasets - LLM will choose best match")
                else:
                    print(f"📊 No user selection - LLM will analyze all available datasets")
                
                # LLM selection with filter metadata integration
                selection_result = await self._llm_dataset_selection(search_results, state, filter_metadata_results)
            
            # Return results - either final selection or clarification needed
            if selection_result.get('error', False):
                print(f"❌ Dataset selection failed with error")
                return {
                    'error':True,
                    'error_message': selection_result.get('error_message', 'Unknown error occurred during dataset selection')
                    }
            elif selection_result.get('requires_clarification', False):
                print(f"❓ Clarification needed - preparing follow-up question")
                return {
                    'dataset_followup_question': selection_result.get('clarification_question'),
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'candidate_actual_tables': selection_result.get('candidate_actual_tables', []),
                    'functional_names': selection_result.get('functional_names', []),
                    'requires_clarification': True,
                    'filter_metadata_results': filter_metadata_results,
                    'selected_filter_context': selection_result.get('selected_filter_context', '')
                }
            elif selection_result.get('status','')=="missing_items":
                return {
                    'missing_dataset_items': True,
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'functional_names': selection_result.get('functional_names', []),
                    'user_message': selection_result.get('user_message', ''),
                    'filter_metadata_results': filter_metadata_results
                } 
            elif selection_result.get('status','')=="phi_found":
                return {
                    'phi_found': True,
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'functional_names': selection_result.get('functional_names', []),
                    'user_message': selection_result.get('user_message', ''),
                    'filter_metadata_results': filter_metadata_results
                }
            else:
                print(f"✅ Dataset selection complete")
                state['selected_dataset'] = selection_result.get('final_actual_tables', [])
                state['selected_filter_context'] = selection_result.get('selected_filter_context', '')
                state['selection_reasoning'] = selection_result.get('selection_reasoning', '')
                # After selection, extract metadata and set flag for SQL generation
                metadata_result = await self.get_metadata(
                    state=state,
                    selected_dataset=selection_result.get('final_actual_tables', [])
                )

                if metadata_result.get('error', False):
                    return {
                        'error': True,
                        'error_message': "Databricks vector search failed"
                    }
                
                # Update state with metadata and selected dataset
                state['dataset_metadata'] = metadata_result.get('metadata', '')
                selected_dataset = selection_result.get('final_actual_tables', [])
                selection_reasoning = selection_result.get('selection_reasoning', '')
                functional_names = selection_result.get('functional_names', [])
                create_sql = True

        # SQL Generation and Execution Phase
        if create_sql or create_sql_after_followup:

            print(f"🔧 Starting SQL generation and execution phase")

            # Generate SQL based on the flow type
            if create_sql_after_followup:
                # SQL follow-up flow
                context = self._extract_context(state)
                sql_followup_question = state.get('sql_followup_question', '')
                sql_followup_answer = state.get('current_question', '')
                
                sql_result = await self._generate_sql_with_followup_async(context, sql_followup_question, sql_followup_answer, state)

                # ========================================
                # CHECK FOR TOPIC DRIFT OR NEW QUESTION
                # ========================================
                if sql_result.get('topic_drift', False):
                    print("⚠️ Topic drift detected - user response unrelated to follow-up question")
                    return {
                        'success': False,
                        'sql_followup_topic_drift': True,
                        'sql_followup_but_new_question': False
                    }
                
                if sql_result.get('new_question', False):
                    print("⚠️ New question detected - user asked different question instead of answering")
                    return {
                        'success': False,
                        'sql_followup_topic_drift': False,
                        'sql_followup_but_new_question': True,
                        'detected_new_question': sql_result.get('detected_new_question', '')

                    }
                
                print("✅ Follow-up response validated - proceeding with SQL execution")

            else:
                # Initial SQL generation flow
                sql_result = await self._assess_and_generate_sql_async(self._extract_context(state), state)
                # print('sql result',sql_result)
            # Handle follow-up questions if needed
            if sql_result.get('needs_followup'):
                return {
                    'success': True,
                    'needs_followup': True,
                    'sql_followup_question': sql_result['sql_followup_questions'],
                    'selected_dataset': selected_dataset or state.get('selected_dataset', []),
                    'dataset_metadata': state.get('dataset_metadata', ''),
                    'selection_reasoning': selection_reasoning,
                    'functional_names': functional_names
                }
            
            if not sql_result['success']:
                return {
                    'success': False,
                    'error': sql_result['error'],
                    'selected_dataset': selected_dataset or state.get('selected_dataset', []),
                    'dataset_metadata': state.get('dataset_metadata', '')
                }
            
            # Execute SQL queries and return results
            context = self._extract_context(state)
            if sql_result.get('multiple_sql', False):
                print(f"  ⚡ Executing {len(sql_result['sql_queries'])} SQL queries in parallel...")
                final_result = await self._execute_multiple_sql_queries_async(sql_result, context)
            else:
                print(f"  ⚡ Executing single SQL query...")
                final_result = await self._execute_single_sql_query_async(sql_result, context, create_sql_after_followup)
            
            # ========================================
            # CHECK IF SQL RESULTS HAVE DATA
            # ========================================
            history_sql_used_flag = sql_result.get('history_sql_used', False)
            
            # Extract sql_story from sql_result (generated by LLM)
            sql_story = sql_result.get('sql_story', '')
            if sql_story:
                print(f"📖 SQL story extracted from LLM response: {sql_story[:100]}...")
            
            if final_result.get('success', False):
                has_data = False
                
                # Check for multiple results
                if final_result.get('multiple_results', False):
                    query_results = final_result.get('query_results', [])
                    # Check if any query has data
                    has_data = any(
                        result.get('data') and len(result.get('data', [])) > 0 
                        for result in query_results
                    )
                    if not has_data:
                        print(f"⚠️ Multiple SQL queries executed but no data returned")
                else:
                    # Check for single result
                    query_results = final_result.get('query_results')
                    if query_results and len(query_results) > 0:
                        has_data = True
                    else:
                        print(f"⚠️ SQL query executed but no data returned")
                
                # Mark success as false if no data found
                if not has_data:
                    history_sql_used_flag = False  # Set history_sql_used to False when no data
                    print(f"❌ Marking success as False due to empty result set")

            # Add sql_story to final_result if it exists
            if sql_story:
                final_result['sql_story'] = sql_story
                print(f"✅ Added sql_story to final_result for state propagation")
            
            # Return comprehensive results
            return {
                'sql_result': final_result,
                'selected_dataset': selected_dataset or state.get('selected_dataset', []),
                'dataset_metadata': state.get('dataset_metadata', ''),
                'dataset_followup_question': None,
                'selection_reasoning': selection_reasoning or state.get('selection_reasoning', ''),
                'functional_names': functional_names or state.get('functional_names', []),
                'requires_clarification': False,
                'filter_metadata_results': filter_metadata_results,
                'history_sql_used': history_sql_used_flag
            }
        
        # Should not reach here, but handle gracefully
        return {
            'error': True,
            'error_message': "Unexpected flow in dataset selection"
        }       
    
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

    async def _llm_dataset_selection(self, search_results: List[Dict], state: AgentState, filter_metadata_results: List[str] = None) -> Dict:
        """Enhanced LLM selection with validation, disambiguation handling, filter-based selection, and historical learning"""
        
        user_question = state.get('current_question', state.get('original_question', ''))
        filter_values = state.get('filter_values', [])
        
        # NEW: Get feedback match for historical learning (3 lines)
        feedback_match = state.get('feedback_match_result', {})
        has_history = feedback_match.get('status') == 'match_found'
        matched_table = feedback_match.get('table_name', '') if has_history else ''
        
        # NEW: Build history hint (only if match exists) (5 lines)
        history_hint = ""
        if has_history:
            history_hint = f"""
    **HISTORICAL HINT**: A similar question previously used table: {matched_table}
    - If this table is valid and you're deciding between multiple options, prefer it.
    - If it's not in search results or doesn't meet requirements, ignore this hint.
    """

        selection_prompt = f"""
You are analyzing a database schema to route SQL queries to the correct table(s).

⚠️ CRITICAL CONTEXT: When users request metrics like "revenue per script", "cost per claim", etc., they are asking which TABLE contains the data - NOT asking you to perform calculations. The SQL generation system downstream will handle all calculations.

Your job: Select the correct table(s) based on required columns and data availability.

IMPORTANT: You have ONE opportunity to ask for clarification if genuinely ambiguous. Use it wisely.

====================================================
INPUTS
====================================================
QUESTION: {user_question}
EXTRACTED FILTER VALUES: {filter_values}
FILTER METADATA: {filter_metadata_results}
AVAILABLE DATASETS: 
{search_results}

====================================================
SELECTION RULES - APPLY IN STRICT SEQUENCE
====================================================

**RULE 1: PHI/PII SECURITY CHECK (IMMEDIATE STOP)**
- Check if user requests PHI/PII columns (SSN, member_id, patient names, addresses)
- Compare with each dataset's "PHI_PII_Columns" field
- IF found → STOP → Return status="phi_found" with appropriate message
- IF not found → Continue to Rule 2

**RULE 2: DIRECT TABLE KEYWORD MATCH**
- Check for explicit table references in question:
  * "ledger" → actuals_vs_forecast table
  * "claims" or "claim" → claim_transaction table  
  * "billing" → billing_extract table
- IF keyword found AND that table has required columns → Return status="success"
- IF keyword found BUT table lacks columns → Continue to Rule 3
- IF no keyword → Continue to Rule 3

**RULE 3: EXTRACT COMPONENTS FROM QUESTION**
Extract three types of components:

A. METRICS (measurable values):
   - Direct matches: revenue, expense, cost, cogs, billed amount, allowed amount, scripts, volume, margin
   - Fuzzy matches for metrics:
     * "scripts/prescriptions" → unadjusted_scripts, adjusted_scripts, 30_day_scripts, 90_day_scripts
     * "cost/costs" → expense, cogs, cost per script
     * "margin" → gross margin, Gross Margin %, margin per script
     * "billing/billed" → billed amount, revenue from billing
   - Skip mathematical operations: variance, growth, change, percentage, performance

B. ATTRIBUTES (dimensions for breakdown):
   - Direct matches: Client Name, Drug Name, Therapy Class, Carrier ID, Client ID
   - Apply fuzzy matching for readable names:
     * "therapy/therapies" → Therapy Class
     * "client/clients" → Client Name, Client ID
     * "pharmacy/pharmacies" → Pharmacy Name, Pharmacy NPI
     * "lob/line of business" → Line of Business
     * "invoice" → Invoice Number, Invoice Date
     * "member/patient" → Member ID (PHI check required)
   - BLOCK creative substitutions (Product Category ≠ drug, ingredient_fee ≠ expense)

C. FILTERS (specific values to filter by):
   - Extract actual values from question
   - SKIP these common filters: external,SP,PBM, HDP, optum, mail, specialty, home delivery,PBM retail.These exists in all tables

**RULE 4: ELIMINATE UNSUITABLE TABLES (MANDATORY CHECK)**
For each available dataset, perform TWO checks:

A. Check "not_useful_for" field:
   - IF question pattern matches "not_useful_for" → ELIMINATE immediately
   - Ex-1: Q:"top 10 clients by expense" + Ledger has not_useful_for:["client expense/margin alone"] → ELIMINATE Ledger
   - Ex-2: Q:"top 10 drugs,therapy class by revenue" + Ledger has not_useful_for:["drug/therapy analysis"] → ELIMINATE Ledger
   - **CRITICAL**: Any question with drug/therapy/vaccine/medication filters OR breakdowns → Check "not_useful_for" for "drug/therapy analysis"

B. Check "attrs" field for exact attribute match:
   - Extract ALL attributes from the dataset's "attrs" list
   - Compare question's required attributes with this EXACT list
   - **DO NOT assume a table has attributes if they're not explicitly listed in "attrs"**
   - IF question needs attributes NOT IN "attrs" list → ELIMINATE immediately
   - Examples:
     * Q: "revenue by therapy class, drug name"
     * Ledger attrs: ["Ledger", "Mail Service", "Home Delivery", "Specialty", "Line of Business", "Transaction Date", "Year", "Month", "Quarter", "Product Category", "Product Category Level 1", "Product Category Level 2", "Client ID", "Client Name"]
     * Does NOT contain "Therapy Class" or "Drug Name" → ELIMINATE Ledger ❌
     * Claims attrs: ["Claim Number", "Submit Date", "Client ID", "Client Name", "Pharmacy NPI", "Drug Name", "Therapy Class", ...]
     * CONTAINS "Therapy Class" and "Drug Name" → KEEP Claims ✅
   
   - **CRITICAL FOR FILTERS**: Even if question has no breakdown, check filter applicability:
     * Q: "script count for vaccines" (vaccines is a therapy class value)
     * Ledger: Check "not_useful_for" → Contains "drug/therapy analysis" → ELIMINATE ❌
     * Claims: Check "not_useful_for" → Does not block therapy analysis → KEEP ✅

**RULE 5: VALIDATE ATTRIBUTE REQUIREMENTS**
For remaining tables after Rule 4:

CRITICAL: Use ONLY the exact attribute names listed in each dataset's "attrs" field. Do NOT assume or infer attributes.

- IF question needs attributes (has breakdown/dimension/by/for grouping):
  * Step 1: List ALL required attributes from question
  * Step 2: For EACH table, check if its "attrs" field contains EXACT matches (case-insensitive, fuzzy OK for synonyms)
  * Step 3: Table MUST have 100% of required attributes in its "attrs" list → KEEP
  * Step 4: Table missing ANY attribute from its "attrs" list → ELIMINATE
  * Example:
    - Required: ["therapy class", "drug name"]
    - Table A attrs contains: ["Therapy Class", "Drug Name"] → KEEP ✅
    - Table B attrs contains: ["Product Category", "Client Name"] → ELIMINATE ❌ (missing both)

- IF question needs NO attributes (summary/total only):
  * Keep all tables with required metrics

**RULE 6: FILTER VALUE DISAMBIGUATION**
When filter values exist in metadata:
- Check filter_metadata for column matches
- IF value found in SINGLE column → AUTO-SELECT that column
- IF value found in MULTIPLE columns:
  * Only ONE column has COMPLETE match → AUTO-SELECT that column
  * Multiple columns have matches → Mark needs_disambiguation
  * Example: "covid vaccine" in [drug_name, therapy_class_name] → needs_disambiguation

**RULE 7: TIE-BREAKER - HIGH-LEVEL TABLE PRIORITY**
Apply when multiple tables remain after Rules 1-6:

- IF multiple tables have SAME metrics AND SAME attributes (exact tie):
  * Check metadata field "high_level" for each table
  * IF one table has "high_level": true → AUTO-SELECT it (status="success")
  * Example: Both Claims and Ledger have [revenue, Client Name], Ledger has high_level=true → SELECT Ledger ✓
  
- IF multiple tables but NO tie (different metrics/attributes):
  * Normal evaluation continues
  
- IF question is metrics-only (no breakdown/attributes needed):
  * Prefer high_level=true table when available

**RULE 8: MULTI-TABLE SELECTION**
- IF user explicitly requests multiple datasets (uses "and", "compare", "both"):
  * Return all qualifying tables
- OTHERWISE: Select single best table

**RULE 9: FINAL DECISION**
Count tables that passed all rules:
- SINGLE table → Return status="success"
- MULTIPLE tables with EXACT TIE (same metrics + attributes):
  * HIGH_LEVEL tie-breaker ALREADY applied in Rule 7
  * If still tied (both high_level or both not) → Check "useful_for" 
  * Still tied → Return status="needs_disambiguation"
- NO tables → Return status="missing_items"

====================================================
CRITICAL REMINDERS
====================================================
1. EXACT TIE (same metrics + same attributes) → AUTO-SELECT high_level=true table (no disambiguation)
2. You have ONE follow-up opportunity - use only for genuine ambiguity (NOT for ties)
3. Attributes must match 100% - no partial matching
4. Filter disambiguation is automatic when possible

====================================================
OUTPUT FORMAT
====================================================

First provide CONCISE REASONING (3-5 lines maximum):
- Extracted: metrics=[list], attributes=[list], filters=[list]
- Eliminated: [Table1: reason in 5 words], [Table2: reason in 5 words]
- Selected: [TableName] - [brief reason why]

Then provide JSON response in <json> tags:

For SUCCESS:

  "status": "success",
  "final_actual_tables": ["table_name"],
  "functional_names": ["user_friendly_name"],
  "selection_reasoning": "1-2 lines explaining selection",
  "high_level_table_selected": true/false,
  "selected_filter_context": "column: [actual_column_name], values: [sample_values]" // if filter applied

For NEEDS_DISAMBIGUATION:

  "status": "needs_disambiguation",
  "tables_identified_for_clarification": ["table_1", "table_2"],
  "functional_table_name_identified_for_clarification": ["friendly_name_1", "friendly_name_2"],
  "requires_clarification": true,
  "clarification_question": "Your question could use either [friendly_name_1] or [friendly_name_2]. Which would you prefer?",
  "selected_filter_context": null

For MISSING_ITEMS:

  "status": "missing_items",
  "user_message": "Cannot find tables with required [missing attributes/metrics]. Please rephrase or verify availability."

For PHI_FOUND:

  "status": "phi_found",
  "user_message": "This query requests protected health information that cannot be accessed."

========================
EXAMPLES FOR CLARITY
========================

Example 1: "Revenue by client or line of business"
- Extracted: metrics=[revenue], attributes=[client name, line of business], filters=[]
- Eliminated: None (both Ledger and Claims have revenue + client name + line of business)
- Selected: Ledger - high_level_table=true (Rule 7 tiebreaker)

Example 2: "Script count and revenue by drug name and therapy class"
- Extracted: metrics=[script count, revenue], attributes=[drug name, therapy class], filters=[]
- Eliminated: Ledger (not_useful_for="drug/therapy analysis")
- Selected: Claims - only table with required attribute

Remember: Check "not_useful_for" FIRST, then verify exact "attrs" list, eliminate tables immediately when rules fail.
"""

        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # print("Sending selection prompt to LLM...",selection_prompt)
                print("Current Timestamp before dataset selector call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                # print("Raw dataset prompt:", selection_prompt)
                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": selection_prompt}],
                    max_tokens=1000,
                    temperature=0.0,  # Deterministic for dataset selection
                    top_p=0.1,
                    system_prompt="You are a database table routing assistant. Your job is to analyze user questions and select the appropriate database table(s) based on the available schema metadata. You output structured JSON responses for downstream SQL generation systems."
                )
                
                print("Raw LLM response:", llm_response)
                print("Current Timestamp after dataset selector call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                
                # Log LLM output - actual response truncated to 500 chars
                self._log('info', "LLM response received from dataset selector", state,
                         llm_response=llm_response,
                         attempt=retry_count)

                # Extract reasoning stream before JSON tag
                reasoning_stream = ""
                json_start = llm_response.find('<json>')
                if json_start > 0:
                    reasoning_stream = llm_response[:json_start].strip()
                    print(f"📝 Captured dataset selection reasoning stream ({len(reasoning_stream)} chars)")
                
                # Store reasoning stream in state
                state['dataset_selection_reasoning_stream'] = reasoning_stream

                # Extract JSON from the response using the existing helper method
                json_content = self._extract_json_from_response(llm_response)
                
                selection_result = json.loads(json_content)
                
                # Handle different status types
                status = selection_result.get('status', 'success')
                
                if status == "missing_items":
                    print(f"❌ Missing items found: {selection_result.get('missing_items')}")
                    return {
                        'final_actual_tables': [],
                        'functional_names': [],
                        'requires_clarification': False,
                        'selection_reasoning': selection_result.get('selection_reasoning', ''),
                        'missing_items': selection_result.get('missing_items', {}),
                        'user_message': selection_result.get('user_message', ''),
                        'error_message': '',
                        'status': 'missing_items',
                        'selected_filter_context': ''
                    }
                
                elif status in ("needs_disambiguation", "needs_clarification"):
                    print(f"❓ Clarification needed - preparing follow-up question")
                    return {
                        'final_actual_tables': selection_result.get('final_actual_tables', []),
                        'functional_names': selection_result.get('functional_table_name_identified_for_clarification', []),
                        'requires_clarification': True,
                        'clarification_question': selection_result.get('clarification_question'),
                        'candidate_actual_tables': selection_result.get('tables_identified_for_clarification', []),
                        'selection_reasoning': selection_result.get('selection_reasoning', ''),
                        'missing_items': selection_result.get('missing_items', {}),
                        'error_message': '',
                        'selected_filter_context': selection_result.get('selected_filter_context', '')
                    }
                
                elif status == "phi_found":
                    print(f"❌ PHI/PII information detected: {selection_result.get('selection_reasoning', '')}")
                    return {
                        'final_actual_tables': [],
                        'functional_names': [],
                        'requires_clarification': False,
                        'selection_reasoning': selection_result.get('selection_reasoning', ''),
                        'user_message': selection_result.get('user_message', ''),
                        'error_message': '',
                        'status': 'phi_found',
                        'selected_filter_context': ''
                    }
                
                else:  # status == "success"
                    high_level_selected = selection_result.get('high_level_table_selected', False)
                    if has_history and matched_table in selection_result.get('final_actual_tables', []):
                        print(f"✅ Dataset selection complete (Historical match used): {selection_result.get('functional_names')}")
                    elif high_level_selected:
                        print(f"✅ Dataset selection complete (High-level table prioritized): {selection_result.get('functional_names')}")
                    else:
                        print(f"✅ Dataset selection complete: {selection_result.get('functional_names')}")
                    return {
                        'final_actual_tables': selection_result.get('final_actual_tables', []),
                        'functional_names': selection_result.get('functional_names', []),
                        'requires_clarification': False,
                        'selection_reasoning': selection_result.get('selection_reasoning', ''),
                        'missing_items': selection_result.get('missing_items', {}),
                        'error_message': '',
                        'high_level_table_selected': high_level_selected,
                        'selected_filter_context': selection_result.get('selected_filter_context', '')
                    }
                        
            except Exception as e:
                retry_count += 1
                print(f"⚠ Dataset selection attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying... ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    return {
                        'final_actual_tables': [],
                        'functional_names': [],
                        'requires_clarification': False,
                        'selection_reasoning': 'Dataset selection failed',
                        'missing_items': {'metrics': [], 'attributes': []},
                        'error': True,
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}",
                        'selected_filter_context': ''
                    }

    def _extract_json_from_response(self, response: str) -> str:
        """Extract JSON content from XML tags or return the response if no tags found"""
        import re
        
        # Try to extract content between <json> tags
        json_match = re.search(r'<json>(.*?)</json>', response, re.DOTALL)
        if json_match:
            return json_match.group(1).strip()
        
        # If no XML tags found, assume the entire response is JSON
        return response.strip()


    async def _fix_router_llm_call(self, state: AgentState, filter_metadata_results: List[str] = None) -> Dict:
        """Handle follow-up clarification with topic drift detection in single call"""
        
        user_clarification = state.get('current_question', '')
        followup_question = state.get('dataset_followup_question', '')
        candidate_actual_tables = state.get('candidate_actual_tables', [])
        functional_names = state.get('functional_names', [])
        original_question = state.get('rewritten_question', state.get('original_question', ''))
        followup_reasoning = state.get('requires_dataset_clarification',  '')

        
        # Format filter metadata for prompt
        filter_metadata_text = ""
        if filter_metadata_results:
            filter_metadata_text = "\n**AVAILABLE FILTER METADATA:**\n"
            for result in filter_metadata_results:
                filter_metadata_text += f"- {result}\n"
        
        combined_prompt = f"""
You need to analyze the user's response and either process clarification or detect topic drift.

CONTEXT:
ORIGINAL QUESTION: "{original_question}"
YOUR CLARIFICATION QUESTION: "{followup_question}"
USER'S RESPONSE: "{user_clarification}"

CANDIDATE TABLES: {candidate_actual_tables}
FUNCTIONAL NAMES: {functional_names}
EXTRACTED COLUMNS WITH FILTER VALUES:

{filter_metadata_text}

TASK: Determine what type of response this is and handle accordingly.

ANALYSIS STEPS:
1. **Response Type Detection**:
- CLARIFICATION_ANSWER: User is responding to your clarification question
- NEW_QUESTION: User is asking a completely different question (topic drift)
- MODIFIED_SCOPE: User modified the original question's scope

2. **Action Based on Type**:
- If CLARIFICATION_ANSWER → Select final dataset from candidates AND extract column selection if applicable
- If NEW_QUESTION → Signal topic drift for fresh processing
- If MODIFIED_SCOPE → Signal scope change for revalidation

3. **Column Selection Extraction** (for CLARIFICATION_ANSWER only):
- If the clarification question was about column selection AND filter metadata is available
- Analyze user's response to identify which column they selected
- Extract the specific column name and its values from filter metadata
- Format selected_filter_context as string: "user selected info - col name - [actual_column_name] , sample values [sample_values_from_metadata]"

RESPONSE FORMAT MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```
{{
    "response_type": "clarification_answer" | "new_question" | "modified_scope",
    "final_actual_tables": ["table_name"] if clarification_answer else [],
    "final_functional_table_names": ["functional_name"] if clarification_answer else [],
    "selection_reasoning": "explanation" if clarification_answer else "topic drift/scope change detected",
    "topic_drift": true if new_question else false,
    "modified_scope": true if modified_scope else false,
    "selected_filter_context": "user selected info - col name - [actual_column_name] , sample values [sample_values_from_metadata]" if clarification_answer and column selection detected else null
}}

DECISION LOGIC:
- If user response clearly chooses between dataset options → CLARIFICATION_ANSWER
- If user asks about completely different metrics/attributes → NEW_QUESTION  
- If user refines original question without answering clarification → MODIFIED_SCOPE

CRITICAL: Be decisive about response type to avoid processing loops.
"""
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # print('dataset follow up prompt',combined_prompt)
                llm_response = await self.db_client.call_claude_api_endpoint_async([
                    {"role": "user", "content": combined_prompt}
                ])
                # print('dataset follow up response',llm_response)
                
                # Log LLM output - actual response truncated to 500 chars
                self._log('info', "LLM response received from dataset clarification handler", state,
                         llm_response=llm_response,
                         retry_attempt=retry_count)

               

                result = json.loads(llm_response)
                response_type = result.get('response_type')
                
                if response_type == 'new_question':
                    print(f"🔄 Topic drift detected - treating as new question")
                    return {

                        'topic_drift': True,
                    }
                
                elif response_type == 'modified_scope':
                    print(f"🔄 Modified scope detected - restarting validation")
                    return {
                        'topic_drift': True
                    }
                
                else:  # clarification_answer
                    print(f"✅ Clarification resolved: {result.get('final_actual_tables')}")


                    # selected_filter_context = result.get('selected_filter_context')
                    # if selected_filter_context:
                    #     print(f"🎯 Column selection detected: {selected_filter_context}")
                    
                    return {
                        'dataset_followup_question': None,
                        'selected_dataset': result.get('final_actual_tables', []),
                        'requires_clarification': False,
                        'selection_reasoning': result.get('selection_reasoning', ''),
                        'selected_filter_context': result.get('selected_filter_context', ''),
                        'final_functional_table_names': result.get('final_functional_table_names', [])
                    }
                
            except Exception as e:
                retry_count += 1
                print(f"⚠ Combined clarification attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying... ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    return {
                        'dataset_followup_question': "Error processing clarification",
                        'selected_dataset': [],
                        'requires_clarification': False,
                        'error': True,
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }

    # ============ SQL GENERATION AND EXECUTION METHODS ============

   

    async def _execute_single_sql_query_async(self, sql_result: Dict, context: Dict, is_sql_followup: bool) -> Dict[str, Any]:
        """Execute single SQL query and return results (narrative handled separately)"""
        
        # Execute with retry logic
        print('coming inside _execute_single_sql_query_async')
        execution_result = await self._execute_sql_with_retry_async(sql_result['sql_query'], context)
        if not execution_result['success']:
            return {
                'success': False,
                'error': execution_result['error'],
                'failed_sql': sql_result['sql_query'],
                'execution_attempts': execution_result['attempts']
            }
        
        # Return SQL results immediately (narrative processed separately)
        print(f"  ✅ SQL execution complete, returning results for immediate display")

        return {
            'success': True,
            'multiple_results': False,
            'sql_query': execution_result['final_sql'],
            'query_results': execution_result['data'],
            'narrative': '',  # Will be populated by narrative agent
            'summary': '',  # Will be populated by narrative agent
            'execution_attempts': execution_result['attempts'],
            'row_count': len(execution_result['data']) if execution_result['data'] else 0,
            'used_followup': bool(is_sql_followup)
        }

    async def _execute_multiple_sql_queries_async(self, sql_result: Dict, context: Dict) -> Dict[str, Any]:
        """Execute multiple SQL queries in parallel and return results (narrative handled separately)"""
        
        sql_queries = sql_result['sql_queries']
        query_titles = sql_result.get('query_titles', [])
        
        # Ensure we have titles for all queries
        while len(query_titles) < len(sql_queries):
            query_titles.append(f"Query {len(query_titles) + 1}")
        
        async def execute_single_query(query_data):
            """Execute a single query with its title"""
            sql_query, title, index = query_data
            
            print(f"    Executing {title} (Query {index + 1})...")
            
            # Execute SQL with retry logic
            execution_result = await self._execute_sql_with_retry_async(sql_query, context)
            
            return {
                'index': index,
                'title': title,
                'sql_query': sql_query,
                'execution_result': execution_result
            }
        
        async def prepare_single_result(result_data):
            """Prepare SQL results for a single query (no narrative processing)"""
            index = result_data['index']
            title = result_data['title']
            execution_result = result_data['execution_result']
            
            if not execution_result['success']:
                return {
                    'index': index,
                    'title': title,
                    'success': False,
                    'error': execution_result['error']
                }
            
            print(f"    Preparing SQL results for {title}...")
            
            # Return SQL results immediately (narrative will be handled separately)
            return {
                'index': index,
                'title': title,
                'success': True,
                'sql_query': execution_result['final_sql'],
                'data': execution_result['data'],
                'narrative': '',  # Will be added by separate narrative agent
                'summary': '',  # Will be added by separate narrative agent
                'execution_attempts': execution_result['attempts'],
                'row_count': len(execution_result['data']) if execution_result['data'] else 0
            }
        
        try:
            # Step 1: Execute all SQL queries in parallel using asyncio
            query_data = [(sql_queries[i], query_titles[i], i) for i in range(len(sql_queries))]
            
            execution_results = await asyncio.gather(*[execute_single_query(qd) for qd in query_data])
            
            # Check if any executions failed
            failed_executions = [r for r in execution_results if not r['execution_result']['success']]
            if failed_executions:
                failed_query = failed_executions[0]
                return {
                    'success': False,
                    'error': f"Failed to execute {failed_query['title']}: {failed_query['execution_result']['error']}",
                    'failed_sql': failed_query['sql_query']
                }
            
            # Step 2: Prepare all SQL results in parallel using asyncio  
            prepared_results = await asyncio.gather(*[prepare_single_result(er) for er in execution_results])
            
            # Check if any preparation failed
            failed_preparation = [r for r in prepared_results if not r['success']]
            if failed_preparation:
                print(f"⚠️ Some SQL result preparation failed, but continuing with available results")
            
            # Sort results by original index to maintain order
            prepared_results.sort(key=lambda x: x['index'])
            
            # Prepare final SQL results
            query_results = []
            for result in prepared_results:
                if result['success']:
                    query_results.append({
                        'title': result['title'],
                        'sql_query': result['sql_query'],
                        'data': result['data'],
                        'narrative': '',  # Will be populated by narrative agent
                        'summary': '',  # Will be populated by narrative agent
                        'execution_attempts': result['execution_attempts'],
                        'row_count': result['row_count']
                    })
                else:
                    query_results.append({
                        'title': result['title'],
                        'sql_query': '',
                        'data': [],
                        'narrative': f"Failed to execute: {result['error']}",
                        'summary': '',
                        'execution_attempts': 0,
                        'row_count': 0
                    })
            
            return {
                'success': True,
                'multiple_results': True,
                'query_results': query_results,
                'total_queries': len(query_results),
                'successful_queries': len([r for r in prepared_results if r['success']])
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Parallel execution failed: {str(e)}",
                'execution_attempts': 0
            }

    def _extract_context(self, state: Dict) -> Dict:
        """Extract context from state, including extracting table_kg from dataset_metadata"""
        questions_history = state.get('questions_sql_history', [])
        recent_history = questions_history[-4:] if questions_history else []
        dataset_metadata = state.get('dataset_metadata', '')
        dataset_name = state.get('selected_dataset', [])
        current_question = state.get('rewritten_question', state.get('current_question', ''))
        selected_filter_context = state.get('selected_filter_context')
        
        return {
            'recent_history': recent_history,
            'dataset_metadata': dataset_metadata,
            'dataset_name': dataset_name,
            'current_question': current_question,
            'selected_filter_context': selected_filter_context
        }

    async def _assess_and_generate_sql_async(self, context: Dict, state: Dict) -> Dict[str, Any]:
        """SQL generation with optional historical learning context"""
        
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        join_clause = state.get('join_clause', '')
        selected_filter_context = context.get('selected_filter_context')
        
        
        # Get selected datasets for table filtering
        selected_datasets = state.get('selected_dataset', [])
        filter_metadata_results=state.get('filter_metadata_results',[])
        # NEW: Search for historical SQL feedback BEFORE generating SQL
        # This ensures we only search for SQL from the selected dataset(s)
        print(f"🔍 Searching feedback SQL embeddings for selected dataset(s): {selected_datasets}")
        feedback_results = await self.db_client.search_feedback_sql_embeddings(current_question, table_names=selected_datasets)
        
        # Process feedback results with LLM selection
        matched_sql = ''
        history_question_match = ''
        matched_table_name = ''
        
        if feedback_results:
            print(f"🤖 Analyzing {len(feedback_results)} feedback SQL candidates from selected dataset(s)...")
            feedback_selection_result = await self.db_client._llm_feedback_selection(feedback_results, state)
            
            if feedback_selection_result.get('status') == 'match_found':
                # Extract seq_id from the LLM selection result
                matched_seq_id = feedback_selection_result.get('seq_id')
                
                # Filter feedback results to find the matching record
                matched_record = None
                for result in feedback_results:
                    if result.get('seq_id') == matched_seq_id:
                        matched_record = result
                        break
                
                if matched_record:
                    # Extract matched results
                    history_question_match = matched_record.get('user_question', '')
                    matched_sql = matched_record.get('sql_query', '')
                    matched_table_name = matched_record.get('table_name', '')
                    
                    # Store in state for consistency
                    state['history_question_match'] = history_question_match
                    state['matched_sql'] = matched_sql
                    state['matched_table_name'] = matched_table_name
                    
                    print(f"✅ Feedback match found from {matched_table_name}")
                    print(f"   Matched question: {history_question_match[:100]}...")
                else:
                    print(f"⚠️ Matched seq_id {matched_seq_id} not found in results")
            else:
                print(f"ℹ️ No suitable feedback SQL match found (status: {feedback_selection_result.get('status')})")
        else:
            print(f"ℹ️ No feedback SQL embeddings found for selected dataset(s)")
        
        # Check if history exists and is relevant
        has_history = bool(matched_sql and history_question_match and matched_table_name)
        
        # Check if we have multiple tables
        selected_datasets = state.get('selected_dataset', [])

        # Define mandatory column mapping
        mandatory_column_mapping = {
            "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast": [
                "Ledger"
            ],"prd_optumrx_orxfdmprdsa.rag.pbm_claims": [
                "product_category='PBM'"
            ]
        }
        
        # Extract mandatory columns based on selected datasets
        mandatory_columns_info = []
        if isinstance(selected_datasets, list):
            for dataset in selected_datasets:
                if dataset in mandatory_column_mapping:
                    mandatory_columns = mandatory_column_mapping[dataset]
                    for col in mandatory_columns:
                        mandatory_columns_info.append(f"Table {dataset}: {col} (MANDATORY)")
                else:
                    mandatory_columns_info.append(f"Table {dataset}: Not Applicable")
        
        # Format mandatory columns for prompt
        mandatory_columns_text = "\n".join(mandatory_columns_info) if mandatory_columns_info else "No mandatory columns required"
        
        # Format selected filter context for prompt
        filter_context_text = ""
        if selected_filter_context:
            filter_context_text = f"""
    SELECTED FILTER CONTEXT Available for SQL generation if the filter values exactly matches:
        {selected_filter_context}

    """
        
        has_multiple_tables = len(selected_datasets) > 1 if isinstance(selected_datasets, list) else False

        # NEW: Build conditional history context
        history_section = ""
        check_5_text = "**CHECK 5: Historical SQL availability**: N/A (no historical reference)"

        if has_history:
            history_section= f"""
HISTORICAL SQL REFERENCE (Internal Use Only - Do NOT mention to user)
PREVIOUS QUESTION: {history_question_match}
TABLE: {matched_table_name}

<historical_sql>
{matched_sql}
</historical_sql>

Use this in Stage 4 to determine pattern reuse level.
    """
        else:
            history_section =  """---
HISTORICAL SQL REFERENCE
---
No historical SQL available. Generate fresh SQL in Stage 5.
    """
        if has_history:
            stage_4_hist_learn="""

STAGE 4: HISTORICAL SQL PATTERN MATCHING

This stage determines HOW to use historical SQL (if available).
Historical SQL is an INTERNAL optimization - never mention it to user.

IF NO HISTORICAL SQL AVAILABLE:
- Skip this stage
- Generate SQL fresh in Stage 5
- Set history_sql_used = false

IF HISTORICAL SQL IS AVAILABLE:

STEP 4.1: SEMANTIC COMPARISON
Compare current question vs historical question:

A. SAME METRIC REQUESTED?
   Current asks for: [identify metric]
   Historical had: [identify metric]
   Match: YES / NO

B. SAME GROUPING DIMENSIONS?
   Current groups by: [identify dimensions]
   Historical grouped by: [identify dimensions]
   Match: YES / NO

C. SAME ANALYSIS TYPE?
   Types: breakdown | top-N | comparison | trend | calculation
   Current: [type]
   Historical: [type]
   Match: YES / NO

STEP 4.2: PATTERN DECISION MATRIX

IF Metric=YES AND Grouping=YES AND Type=YES:
  -> FULL PATTERN REUSE
  -> Copy entire SQL structure
  -> Replace ONLY filter values (dates, entities) with current values
  -> Set history_sql_used = true

IF Metric=YES AND (Grouping=NO OR Type=NO):
  -> PARTIAL PATTERN REUSE
  -> Keep: Metric calculations, CASE WHEN patterns, aggregation methods
  -> Rebuild: GROUP BY from current question
  -> Set history_sql_used = partial

IF Metric=NO:
  -> STRUCTURAL LEARNING ONLY
  -> Learn: UNION patterns, CTE structure, NULLIF safety, ROUND formatting
  -> Build: Fresh SQL for current question using these techniques
  -> Set history_sql_used = false

WHAT TO ALWAYS LEARN (regardless of match level):
- CASE WHEN for side-by-side columns (month comparisons)
- UNION/UNION ALL patterns (detail rows + total row)
- Division safety: NULLIF(denominator, 0)
- Rounding: ROUND(amount, 0), ROUND(percentage, 3)
- Case-insensitive: UPPER(column) = UPPER('value')

WHAT TO NEVER COPY (always from current question):
- Filter values (dates, carrier_id, entity names)
- Specific time periods
- Any <parameter> placeholders - use actual values

CRITICAL VALIDATION:
- Every column in final SQL must exist in CURRENT metadata
- Historical SQL may reference columns not in current dataset - verify before using"""      

        else:
            stage_4_hist_learn="No history sql available"

        
        # 🆕 STORE history_section in state for follow-up SQL generation
        state['sql_history_section'] = history_section
   
        assessment_prompt = f"""You are a Databricks SQL generator for DANA (Data Analytics & Navigation Assistant).

CORE PRINCIPLES:
1. ACCURACY OVER SPEED - Never guess. If uncertain, ask one follow-up question.
2. USE ONLY PROVIDED DATA - Only use columns from METADATA, values from EXTRACTED FILTERS
3. ONE FOLLOW-UP MAXIMUM - Ask one clarifying question if needed, then generate SQL
4. SILENT REASONING - Analyze internally, output only the required format

YOUR TASK: Analyze user question -> Validate mappings -> Either ask ONE follow-up OR generate SQL

---
INPUTS
---
CURRENT QUESTION: {current_question}

AVAILABLE METADATA:
{dataset_metadata}
MANDATORY FILTER COLUMNS:
{mandatory_columns_text}

EXTRACTED Column contain FILTER VALUES:
{filter_metadata_results}

JOIN INFORMATION:
{join_clause}

{history_section}

STAGE 1: SEMANTIC ANALYSIS

Analyze the question using CONFIDENCE-BASED mapping (not word-for-word matching).

STEP 1.0: EXTRACT USER HINTS/CORRECTIONS (check FIRST before any mapping)
If user explicitly provides guidance in their question, treat as HIGH CONFIDENCE override:
- Column specification: "use carrier_id", "based on drug_cost" -> Use that exact column
- Clarification: "I mean X not Y", "specifically the net_revenue" -> Use specified column
- Exclusion: "ignore therapy class", "don't group by month" -> Exclude from query
- Correction: "not carrier_name, use carrier_id" -> Apply correction directly

These user hints override any ambiguity - skip follow-up for terms user already clarified.

STEP 1.1: IDENTIFY MEANINGFUL TERMS
Extract terms that need column mapping:
- EXTRACT: Metrics (revenue, cost, margin, amount, count)
- EXTRACT: Dimensions (carrier, product, category, month, year)
- EXTRACT: Filter values (MPDOVA, HDP, August, 2025)
- SKIP: Generic words (show, get, give, data, analysis, performance, please)

STEP 1.2: SEMANTIC COLUMN MAPPING
For each meaningful term, find semantically related columns in METADATA:

HIGH CONFIDENCE (proceed without asking):
- ONE column semantically matches, even if wording differs
  Example: "network revenue" -> revenue_amount (only revenue column exists)
- Standard date parsing
  Example: "August 2025" -> month=8, year=2025
  Example: "Q3" -> month IN (7,8,9) or quarter=3

LOW CONFIDENCE - AMBIGUOUS (must ask follow-up):
- Multiple columns in SAME semantic category
  Example: "revenue" -> [gross_revenue, net_revenue, total_revenue]
  Example: "date" -> [service_date, fill_date, process_date]
  Example: "cost" -> [drug_cost, admin_cost, shipping_cost]
- Generic term with multiple plausible interpretations
  Example: "amount" -> [revenue_amount, cost_amount, margin_amount]
  Example: "rate" -> [fill_rate, dispense_rate, rejection_rate]

NO MATCH (explain limitation, never invent):
- Business term has zero related columns in metadata
  Example: "customer satisfaction" -> not available
  Example: "NPS score" -> not in this dataset
- NEVER invent columns or calculations for unmapped terms

STEP 1.3: INTENT DETECTION FOR MULTIPLE VALUES
When user mentions multiple specific values (HDP, SP) or time periods (Jan to Dec):

DEFAULT BEHAVIOR - Show breakdown (GROUP BY the dimension):
- "revenue for HDP, SP" -> GROUP BY product_category (show each separately)
- "revenue Jan to March" -> GROUP BY month (show each month)
- "revenue for drug1, drug2" -> GROUP BY drug_name (show each drug)

EXCEPTION - Aggregate only if explicit language:
- "total revenue for HDP and SP combined" -> No GROUP BY, aggregate together
- "sum of Jan through March" -> No GROUP BY month, aggregate together

STEP 1.4: BUILD MAPPING SUMMARY
Create internal mapping:
- term_mappings: [term]->[column](confidence) | [term]->[col1,col2](AMBIGUOUS)
- intent: breakdown | aggregate | comparison
- ambiguities: list any LOW CONFIDENCE mappings

STAGE 2: FILTER RESOLUTION

Resolve filter values mentioned in the question to specific columns.
Use EXTRACTED FILTER VALUES as the source of truth.

RESOLUTION PRIORITY (check in order):

PRIORITY 1: Question has ATTRIBUTE + VALUE
If question mentions both the dimension AND the value:
- "revenue by carrier for MPDOVA" -> Check EXTRACTED FILTERS for which column has MPDOVA
  - If carrier_id=MPDOVA in extracted -> Use carrier_id
  - If carrier_name=MPDOVA in extracted -> Use carrier_name
  - If neither has MPDOVA -> Ask follow-up
- "product category HDP" -> Check EXTRACTED FILTERS for product_category=HDP

PRIORITY 2: Question has VALUE only (no attribute hint)
Check EXTRACTED FILTER VALUES section:

SCENARIO A - Single column has match:
  Extracted shows: carrier_id=MPDOVA (only match)
  -> Use carrier_id='MPDOVA'. No follow-up needed.

SCENARIO B - Multiple columns have exact match with attribute hint in question:
  Question: "carrier MPDOVA"
  Extracted: carrier_id=MPDOVA, client_id=MPDOVA
  -> Question says "carrier" -> Check which carrier column has value -> Use that one

SCENARIO C - Multiple columns have exact match, NO attribute hint:
  Question: "revenue for MPDOVA"
  Extracted: carrier_id=MPDOVA (exact), client_id=MPDOVA (exact)
  -> Genuinely ambiguous -> MUST ask follow-up

SCENARIO D - One EXACT match, others PARTIAL:
  Question: "revenue for MPDOVA"
  Extracted: carrier_id=MPDOVA (exact), client_id=MPDO (partial)
  -> Use exact match (carrier_id). No follow-up needed.

PRIORITY 3: Value not in extracted filters
If value is in question but NOT in extracted filters:
- Check if it's a standard value (month name, year, etc.) -> Parse directly
- If can't resolve -> Ask follow-up

FILTER RESOLUTION OUTPUT:
- filters_resolved: [column=value](Y) | [value->[col1,col2]](AMBIGUOUS)

STAGE 3: DECISION GATE

DECISION LOGIC:
- IF any AMBIGUOUS mappings from Stage 1 or Stage 2:
  -> Output <followup> with specific question
  -> Output <reasoning_summary>
  -> STOP - Do not generate SQL

- IF all mappings are HIGH CONFIDENCE:
  -> Skip follow-up
  -> Proceed to Stage 4 (History) and Stage 5 (SQL Generation)

WHEN TO ASK FOLLOW-UP:
1. AMBIGUOUS METRIC: Multiple columns could be the requested metric
   Ask: "Which [term] are you looking for?"
   Show: Available columns from metadata

2. AMBIGUOUS FILTER: Value matches multiple columns, no attribute hint
   Ask: "The value '[X]' exists in multiple columns. Which one?"
   Show: Columns where value was found

3. UNDEFINED CALCULATION: User asks for metric not in metadata and no clear formula
   Ask: "How should [metric] be calculated?"
   Show: Available columns that could be used

4. VAGUE TIME REFERENCE: "recently", "a while ago" (not "last month", "YTD")
   Ask: "What time period specifically?"
   Show: Available time columns

DO NOT ASK FOLLOW-UP FOR:
- Single semantic match exists (even if wording differs)
- Extracted filter already resolved the value to single column
- Standard date parsing applies (August=8, Q3=7,8,9)
- Generic terms that don't need column mapping (show, get, analysis)
- One exact match among multiple partial matches
- User provided explicit hint or correction in question (handled in Step 1.0)

{stage_4_hist_learn}

STAGE 5: SQL GENERATION
Generate SQL using resolved mappings from Stage 1-2 and patterns from Stage 4.

PRIORITY 0: MANDATORY REQUIREMENTS (violation = query failure)

M1. MANDATORY FILTERS - Must be in WHERE clause
Check MANDATORY FILTER COLUMNS input
- If ledger is MANDATORY -> WHERE ledger = 'GAAP' AND ...
- If product_category='PBM' is MANDATORY -> WHERE product_category = 'PBM' AND ...

M2. CASE-INSENSITIVE STRING COMPARISON
- Always use: WHERE UPPER(column) = UPPER('value')
- Never use: WHERE column = 'value'

M3. SAFE DIVISION
- Always use: NULLIF(denominator, 0)
- Never use: bare division that could divide by zero

M4. NUMERIC FORMATTING
- Amounts: ROUND(value, 0) AS column_name
- Percentages: ROUND(value, 3) AS column_pct

PRIORITY 1: METRIC TYPE HANDLING (critical for calculations)

When table has metric_type column (Revenue, COGS, Expenses, etc.):

FOR CALCULATIONS (margin, ratios, differences):
Pivot metric_type into CASE WHEN columns, do NOT group by metric_type:

CORRECT:
SELECT 
    ledger, year, month,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) AS revenues,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS cogs,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) - 
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS gross_margin
FROM table
WHERE UPPER(metric_type) IN (UPPER('Revenues'), UPPER('COGS'))
GROUP BY ledger, year, month

WRONG (breaks calculations):
GROUP BY ledger, metric_type  -- Creates separate rows, can't calculate across

FOR LISTING INDIVIDUAL METRICS:
Only GROUP BY metric_type when user explicitly asks to see each metric as separate rows.

PRIORITY 2: COMPONENT DISPLAY RULE

For ANY calculated metric, show source components:

Example for "cost per script by carrier":
SELECT 
  carrier_id,
  SUM(total_cost) AS total_cost,
  COUNT(script_id) AS script_count,
  ROUND(SUM(total_cost) / NULLIF(COUNT(script_id), 0), 2) AS cost_per_script
FROM table
GROUP BY carrier_id

PRIORITY 3: QUERY PATTERNS

PATTERN - TOP N:
SELECT column, SUM(metric) AS metric
FROM table
WHERE [mandatory filters]
GROUP BY column
ORDER BY metric DESC
LIMIT N

PATTERN - TIME COMPARISON (side-by-side periods):
SELECT dimension,
       SUM(CASE WHEN month = 7 THEN metric END) AS jul_value,
       SUM(CASE WHEN month = 8 THEN metric END) AS aug_value
FROM table
WHERE [mandatory filters] AND month IN (7, 8)
GROUP BY dimension

PATTERN - PERCENTAGE OF TOTAL:
SELECT column,
       SUM(metric) AS value,
       ROUND(SUM(metric) * 100.0 / 
             (SELECT SUM(metric) FROM table WHERE [same filters]), 3) AS pct
FROM table
WHERE [mandatory filters]
GROUP BY column

PATTERN - BREAKDOWN BY MULTIPLE VALUES (default for multiple values mentioned):
SELECT product_category, SUM(revenue) AS revenue
FROM table
WHERE UPPER(product_category) IN (UPPER('HDP'), UPPER('SP'))
GROUP BY product_category

PATTERN - MULTI-TABLE (when JOIN provided):
SELECT t1.dimension, SUM(t1.metric) AS m1, SUM(t2.metric) AS m2
FROM table1 t1
[JOIN clause from input]
WHERE t1.mandatory_filter = value
GROUP BY t1.dimension

---
OUTPUT FORMAT
---
Always output <reasoning_summary> first, then either <followup> OR <sql>/<multiple_sql>.

REASONING SUMMARY (always output):
<reasoning_summary>
term_mappings: [term]->[column](Y), [term]->[column](Y), [term]->[col1,col2](AMBIGUOUS)
filter_resolution: [column]=[value](Y), [value]->[col1,col2](AMBIGUOUS)
intent: breakdown | aggregate | comparison | top-N | calculation
mandatory_filters: [filter1](Y applied), [filter2](Y applied)
history_pattern: TRUE | FALSE
ambiguities: NONE | [list specific ambiguities]
decision: SQL_GENERATION | FOLLOWUP_REQUIRED
</reasoning_summary>

IF FOLLOWUP REQUIRED:
<followup>
I need one clarification to generate accurate SQL:

[Specific ambiguity]: [Direct question]

Available options:
1. [column_1] - [description]
2. [column_2] - [description]

Please specify which one.
</followup>

[STOP HERE - Do not output SQL]

IF SQL GENERATION:

For SINGLE query:
<sql>
[Complete Databricks SQL]
</sql>

<sql_story>
[2-3 sentences in business-friendly language explaining:
 - What table/data is being queried
 - What filters are applied
 - What metric/calculation is returned]
</sql_story>

<history_sql_used>true | false</history_sql_used>

For MULTIPLE queries:
<multiple_sql>
<query1_title>[Short title - max 8 words]</query1_title>
<query1>[SQL]</query1>
<query2_title>[Short title]</query2_title>
<query2>[SQL]</query2>
</multiple_sql>

<sql_story>
[2-3 sentences explaining the queries]
</sql_story>

<history_sql_used>true | false</history_sql_used>

HISTORY_SQL_USED VALUES:
- true = Used historical SQL structure with filter replacement
- false = Generated fresh (no history or history not applicable)


EXECUTION INSTRUCTION
Execute stages in order. Stop at Stage 3 if follow-up needed.

1. STAGE 1: Semantic Analysis -> Map terms to columns (confidence-based)
2. STAGE 2: Filter Resolution -> Resolve filter values to columns using EXTRACTED FILTERS
3. STAGE 3: Decision Gate -> If ANY ambiguity: output follow-up and STOP
4. STAGE 4: History Pattern -> Determine reuse level (if history available)
5. STAGE 5: SQL Generation -> Build SQL with mandatory requirements

OUTPUT REQUIREMENTS:
- Always output <reasoning_summary> first
- Then output either <followup> OR (<sql> + <sql_story> + <history_sql_used>)
- Never output both <followup> and <sql>

CRITICAL REMINDERS:
- Every mandatory filter MUST be in WHERE clause
- Use UPPER() for all string comparisons
- Show calculation components (don't just show the result)
- Default to GROUP BY when multiple values mentioned (unless "total" language)
- Only ask follow-up for genuine ambiguity, not for semantic matches
- Filter resolution uses EXTRACTED FILTERS as source of truth
"""

        for attempt in range(self.max_retries):
            try:
                print('sql llm prompt', assessment_prompt)
                print("Current Timestamp before SQL writer:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": assessment_prompt}],
                    max_tokens=3000,
                    temperature=0.0,  # Deterministic for SQL generation
                    top_p=0.1,
                    system_prompt="You are a Databricks SQL code generator. Your role is to generate syntactically correct SQL queries based on database metadata and user questions. When users request metrics like 'revenue per script' or 'cost per member', they are asking you to generate SQL that calculates these values - not asking you to perform the calculations yourself. You output SQL code wrapped in XML tags for downstream execution systems."
                )
            
                print('sql llm response', llm_response)
                print("Current Timestamp after SQL write call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                
                # Log LLM output - actual response truncated to 500 chars
                self._log('info', "LLM response received from SQL generator", state,
                         llm_response=llm_response,
                         attempt=attempt + 1)
                
                # Extract assessment reasoning stream before XML tags
                assessment_reasoning = ""
                # Find first XML tag (could be <sql>, <multiple_sql>, or <followup>)
                xml_patterns = ['<sql>', '<multiple_sql>', '<followup>']
                first_xml_pos = len(llm_response)
                for pattern in xml_patterns:
                    pos = llm_response.find(pattern)
                    if pos != -1 and pos < first_xml_pos:
                        first_xml_pos = pos
                
                if first_xml_pos > 0 and first_xml_pos < len(llm_response):
                    assessment_reasoning = llm_response[:first_xml_pos].strip()
                    print(f"📝 Captured SQL assessment reasoning stream ({len(assessment_reasoning)} chars)")
                
                # Store assessment reasoning in state
                state['sql_assessment_reasoning_stream'] = assessment_reasoning
                
                # NEW: Extract history_sql_used flag
                history_sql_used = False
                history_used_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', llm_response, re.IGNORECASE | re.DOTALL)
                if history_used_match:
                    history_sql_used = history_used_match.group(1).strip().lower() == 'true'
                
                # NEW: Extract sql_story tag
                sql_story = ""
                story_match = re.search(r'<sql_story>(.*?)</sql_story>', llm_response, re.DOTALL)
                if story_match:
                    sql_story = story_match.group(1).strip()
                    print(f"📖 Captured SQL generation story ({len(sql_story)} chars)")
                
                # Extract SQL or follow-up questions
                # Check for multiple SQL queries first
                multiple_sql_match = re.search(r'<multiple_sql>(.*?)</multiple_sql>', llm_response, re.DOTALL)
                if multiple_sql_match:
                    multiple_content = multiple_sql_match.group(1).strip()
                    
                    # Extract individual queries with titles
                    query_matches = re.findall(r'<query(\d+)_title>(.*?)</query\1_title>.*?<query\1>(.*?)</query\1>', multiple_content, re.DOTALL)
                    if query_matches:
                        sql_queries = []
                        query_titles = []
                        for i, (query_num, title, query) in enumerate(query_matches):
                            cleaned_query = query.strip().replace('`', '')
                            cleaned_title = title.strip()
                            if cleaned_query and cleaned_title:
                                sql_queries.append(cleaned_query)
                                query_titles.append(cleaned_title)
                        
                        if sql_queries:
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,
                                'query_count': len(sql_queries),
                                'used_history_asset': has_history,
                                'history_sql_used': history_sql_used,  # NEW: LLM's flag
                                'sql_story': sql_story  # NEW: Business-friendly explanation
                            }
                    
                    raise ValueError("Empty or invalid multiple SQL queries in XML response")
                
                # Check for single SQL query
                sql_match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if sql_match:
                    sql_query = sql_match.group(1).strip()
                    sql_query = sql_query.replace('`', '')  # Remove backticks
                    
                    if not sql_query:
                        raise ValueError("Empty SQL query in XML response")
                    
                    return {
                        'success': True,
                        'multiple_sql': False,
                        'sql_query': sql_query,
                        'used_history_asset': has_history,
                        'history_sql_used': history_sql_used,  # NEW: LLM's flag
                        'sql_story': sql_story  # NEW: Business-friendly explanation
                    }
                
                # Check for follow-up questions
                followup_match = re.search(r'<followup>(.*?)</followup>', llm_response, re.DOTALL)
                if followup_match:
                    followup_text = followup_match.group(1).strip()
                    
                    if not followup_text:
                        raise ValueError("Empty follow-up questions in XML response")
                    
                    # Set is_sql_followup to True when asking follow-up questions
                    state['is_sql_followup'] = True
                    
                    return {
                        'success': True,
                        'needs_followup': True,
                        'sql_followup_questions': followup_text,
                        'used_history_asset': False,
                        'history_sql_used': False  # NEW: Not applicable for follow-up
                    }
                
                # Neither SQL nor follow-up found
                raise ValueError("No SQL or follow-up questions found in response")
            
            except Exception as e:
                print(f"❌ SQL assessment attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying SQL assessment... (Attempt {attempt + 1}/{self.max_retries})")
                    await asyncio.sleep(2 ** attempt)
        
        return {
            'success': False,
            'error': f"SQL assessment failed after {self.max_retries} attempts due to Model errors",
            'used_history_asset': False,
            'history_sql_used': False  # NEW FIELD
        }
    
    async def _generate_sql_with_followup_async(self, context: Dict, sql_followup_question: str, sql_followup_answer: str, state: Dict) -> Dict[str, Any]:
        """Generate SQL using original question + follow-up Q&A with relevance validation in single call"""
        
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        join_clause = state.get('join_clause', '')
        selected_filter_context = context.get('selected_filter_context')
        filter_metadata_results = state.get('filter_metadata_results', [])
        
        # 🆕 RETRIEVE history_section from state
        history_section = state.get('sql_history_section', '')
        if history_section:
            print(f"📖 Retrieved sql_history_section from state")
        else:
            print(f"⚠️ No sql_history_section found in state - proceeding without history context")
            history_section = """
=== HISTORICAL SQL ===
Not available

"""
        if history_section:
            stage_3_hist_learn="""

STAGE 3: HISTORICAL SQL PATTERN MATCHING

This stage determines HOW to use historical SQL (if available).
Historical SQL is an INTERNAL optimization - never mention it to user.

IF NO HISTORICAL SQL AVAILABLE:
- Skip this stage
- Generate SQL fresh in Stage 5
- Set history_sql_used = false

IF HISTORICAL SQL IS AVAILABLE:

STEP 4.1: SEMANTIC COMPARISON
Compare current question vs historical question:

A. SAME METRIC REQUESTED?
   Current asks for: [identify metric]
   Historical had: [identify metric]
   Match: YES / NO

B. SAME GROUPING DIMENSIONS?
   Current groups by: [identify dimensions]
   Historical grouped by: [identify dimensions]
   Match: YES / NO

C. SAME ANALYSIS TYPE?
   Types: breakdown | top-N | comparison | trend | calculation
   Current: [type]
   Historical: [type]
   Match: YES / NO

STEP 4.2: PATTERN DECISION MATRIX

IF Metric=YES AND Grouping=YES AND Type=YES:
  -> FULL PATTERN REUSE
  -> Copy entire SQL structure
  -> Replace ONLY filter values (dates, entities) with current values
  -> Set history_sql_used = true

IF Metric=YES AND (Grouping=NO OR Type=NO):
  -> PARTIAL PATTERN REUSE
  -> Keep: Metric calculations, CASE WHEN patterns, aggregation methods
  -> Rebuild: GROUP BY from current question
  -> Set history_sql_used = partial

IF Metric=NO:
  -> STRUCTURAL LEARNING ONLY
  -> Learn: UNION patterns, CTE structure, NULLIF safety, ROUND formatting
  -> Build: Fresh SQL for current question using these techniques
  -> Set history_sql_used = false

WHAT TO ALWAYS LEARN (regardless of match level):
- CASE WHEN for side-by-side columns (month comparisons)
- UNION/UNION ALL patterns (detail rows + total row)
- Division safety: NULLIF(denominator, 0)
- Rounding: ROUND(amount, 0), ROUND(percentage, 3)
- Case-insensitive: UPPER(column) = UPPER('value')

WHAT TO NEVER COPY (always from current question):
- Filter values (dates, carrier_id, entity names)
- Specific time periods
- Any <parameter> placeholders - use actual values

CRITICAL VALIDATION:
- Every column in final SQL must exist in CURRENT metadata
- Historical SQL may reference columns not in current dataset - verify before using"""      

        else:
            stage_3_hist_learn="No history sql available"
        
        # Check if we have multiple tables
        selected_datasets = state.get('selected_dataset', [])
        
        # Define mandatory column mapping
        mandatory_column_mapping = {
            "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast": [
                "Ledger"
            ]
        }
        
        # Extract mandatory columns based on selected datasets
        mandatory_columns_info = []
        if isinstance(selected_datasets, list):
            for dataset in selected_datasets:
                if dataset in mandatory_column_mapping:
                    mandatory_columns = mandatory_column_mapping[dataset]
                    for col in mandatory_columns:
                        mandatory_columns_info.append(f"Table {dataset}: {col} (MANDATORY)")
                else:
                    mandatory_columns_info.append(f"Table {dataset}: Not Applicable")
        
        # Format mandatory columns for prompt
        mandatory_columns_text = "\n".join(mandatory_columns_info) if mandatory_columns_info else "Not Applicable"
        
        # Format selected filter context for prompt
        filter_context_text = ""
        if selected_filter_context:
            filter_context_text = f"""
    SELECTED FILTER CONTEXT Available for SQL generation based on user follow up answer:
        final selection : {selected_filter_context}
    """
        
        has_multiple_tables = len(selected_datasets) > 1 if isinstance(selected_datasets, list) else False

        followup_sql_prompt = f"""You are a Databricks SQL generator for DANA (Data Analytics & Navigation Assistant).

CONTEXT: This is PHASE 2 of a two-phase process. In Phase 1, you asked a clarifying question. The user has now responded. Your task is to generate SQL using the original question + user's clarification.

CORE PRINCIPLES:
1. USER'S RESPONSE IS THE ANSWER - Treat follow-up answer as HIGH CONFIDENCE override, apply directly
2. NO MORE FOLLOW-UPS - Either generate SQL or return drift flag. Do NOT ask another question.
3. USE ONLY PROVIDED DATA - Only use columns from METADATA, values from EXTRACTED FILTERS
4. GENERATE SQL OR FLAG DRIFT - Only two valid outcomes from this prompt

INPUTS

ORIGINAL USER QUESTION: {current_question}

AVAILABLE METADATA:
{dataset_metadata}

MANDATORY FILTER COLUMNS:
{mandatory_columns_text}

EXTRACTED column contain FILTER VALUES:
{filter_metadata_results}

JOIN INFORMATION:
{join_clause}

{history_section}

FOLLOW-UP CONTEXT

YOUR PREVIOUS QUESTION: {sql_followup_question}

USER'S RESPONSE: {sql_followup_answer}

STAGE 1: VALIDATE FOLLOW-UP RESPONSE

Analyze if the user's response is relevant to your question:

RELEVANT (proceed to SQL generation):
- User directly answered your question
- User provided the clarification you asked for
- User gave additional context that resolves the ambiguity
-> PROCEED to Stage 2

NEW_QUESTION (return flag and stop):
- User asked a completely different question instead of answering
- User changed the topic entirely
-> Return <new_question> flag and STOP

TOPIC_DRIFT (return flag and stop):
- User's response is completely unrelated/off-topic
- User provided gibberish or irrelevant information
-> Return <topic_drift> flag and STOP


STAGE 2: APPLY USER'S CLARIFICATION (Only if RELEVANT)

The user's response resolves the ambiguity from Phase 1.
Apply it as HIGH CONFIDENCE override - no further validation needed.

INTEGRATION RULES:
- If user specified a column: Use that exact column
- If user specified a filter value: Apply to the column they indicated
- If user clarified a calculation: Implement their exact formula
- If user defined a time period: Use their exact dates/ranges
- If user chose between options: Apply their choice directly

Do NOT re-validate or question the user's choice. They have answered - now generate SQL.

{stage_3_hist_learn}

STAGE 4: SQL GENERATION

Generate SQL using:
- Original question as primary requirement
- User's clarification as resolved input
- Historical patterns from Stage 3 (if applicable)

PRIORITY 0: MANDATORY REQUIREMENTS (violation = query failure)

M1. MANDATORY FILTERS - Must be in WHERE clause
Check MANDATORY FILTER COLUMNS input
- If ledger is MANDATORY -> WHERE ledger = 'GAAP' AND ...
- If product_category='PBM' is MANDATORY -> WHERE product_category = 'PBM' AND ...

M2. CASE-INSENSITIVE STRING COMPARISON
- Always use: WHERE UPPER(column) = UPPER('value')
- Never use: WHERE column = 'value'

M3. SAFE DIVISION
- Always use: NULLIF(denominator, 0)
- Never use: bare division that could divide by zero

M4. NUMERIC FORMATTING
- Amounts: ROUND(value, 0) AS column_name
- Percentages: ROUND(value, 3) AS column_pct

PRIORITY 1: METRIC TYPE HANDLING (critical for calculations)

When table has metric_type column (Revenue, COGS, Expenses, etc.):

FOR CALCULATIONS (margin, ratios, differences):
Pivot metric_type into CASE WHEN columns, do NOT group by metric_type:

CORRECT:
SELECT 
    ledger, year, month,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) AS revenues,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS cogs,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) - 
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS gross_margin
FROM table
WHERE UPPER(metric_type) IN (UPPER('Revenues'), UPPER('COGS'))
GROUP BY ledger, year, month

WRONG (breaks calculations):
GROUP BY ledger, metric_type  -- Creates separate rows, can't calculate across

FOR LISTING INDIVIDUAL METRICS:
Only GROUP BY metric_type when user explicitly asks to see each metric as separate rows.

PRIORITY 2: COMPONENT DISPLAY RULE

For ANY calculated metric, show source components:

Example for "cost per script by carrier":
SELECT 
  carrier_id,
  SUM(total_cost) AS total_cost,
  COUNT(script_id) AS script_count,
  ROUND(SUM(total_cost) / NULLIF(COUNT(script_id), 0), 2) AS cost_per_script
FROM table
GROUP BY carrier_id

PRIORITY 3: QUERY PATTERNS

PATTERN - TOP N:
SELECT column, SUM(metric) AS metric
FROM table
WHERE [mandatory filters]
GROUP BY column
ORDER BY metric DESC
LIMIT N

PATTERN - TIME COMPARISON (side-by-side periods):
SELECT dimension,
       SUM(CASE WHEN month = 7 THEN metric END) AS jul_value,
       SUM(CASE WHEN month = 8 THEN metric END) AS aug_value
FROM table
WHERE [mandatory filters] AND month IN (7, 8)
GROUP BY dimension

PATTERN - PERCENTAGE OF TOTAL:
SELECT column,
       SUM(metric) AS value,
       ROUND(SUM(metric) * 100.0 / 
             (SELECT SUM(metric) FROM table WHERE [same filters]), 3) AS pct
FROM table
WHERE [mandatory filters]
GROUP BY column

PATTERN - BREAKDOWN BY MULTIPLE VALUES:
SELECT product_category, SUM(revenue) AS revenue
FROM table
WHERE UPPER(product_category) IN (UPPER('HDP'), UPPER('SP'))
GROUP BY product_category

PATTERN - MULTI-TABLE (when JOIN provided):
SELECT t1.dimension, SUM(t1.metric) AS m1, SUM(t2.metric) AS m2
FROM table1 t1
[JOIN clause from input]
WHERE UPPER(t1.mandatory_filter) = UPPER('value')
GROUP BY t1.dimension

OUTPUT FORMAT

Always output <reasoning_summary> first, then ONE of the following: <new_question>, <topic_drift>, <sql>, or <multiple_sql>.

REASONING SUMMARY (always output):
<reasoning_summary>
followup_validation: RELEVANT | NEW_QUESTION | TOPIC_DRIFT
clarification_applied: [what the user clarified and how it was applied]
term_mappings: [term]->[column](Y) based on user clarification
filter_resolution: [column]=[value](Y) based on user clarification
intent: breakdown | aggregate | comparison | top-N | calculation
mandatory_filters: [filter1](Y applied), [filter2](Y applied)
history_pattern: TRUE | FALSE 
decision: SQL_GENERATION | NEW_QUESTION_DETECTED | TOPIC_DRIFT_DETECTED
</reasoning_summary>

IF NEW QUESTION DETECTED:
<new_question>
<detected>true</detected>
<reasoning>[Brief 1-sentence why this is a new question, not an answer to your clarification]</reasoning>
</new_question>

[STOP HERE - Do not output SQL]

IF TOPIC DRIFT DETECTED:
<topic_drift>
<detected>true</detected>
<reasoning>[Brief 1-sentence why this is off-topic/unrelated]</reasoning>
</topic_drift>

[STOP HERE - Do not output SQL]

IF SQL GENERATION (user response was RELEVANT):

For SINGLE query:
<sql>
[Complete Databricks SQL incorporating original question + user's clarification]
</sql>

<sql_story>
[2-3 sentences in business-friendly language explaining:
 - What table/data is being queried
 - What filters are applied
 - What metric/calculation is returned
 - How user's clarification was incorporated]
</sql_story>

<history_sql_used>true | false</history_sql_used>

For MULTIPLE queries:
<multiple_sql>
<query1_title>[Short title - max 8 words]</query1_title>
<query1>[SQL]</query1>
<query2_title>[Short title]</query2_title>
<query2>[SQL]</query2>
</multiple_sql>

<sql_story>
[2-3 sentences explaining the queries and how user's clarification was applied]
</sql_story>

<history_sql_used>true | false</history_sql_used>

HISTORY_SQL_USED VALUES:
- true = Used historical SQL structure with filter replacement
- false = Generated fresh (no history or history not applicable)


EXECUTION INSTRUCTION

Execute stages in order:

1. STAGE 1: Validate follow-up response -> RELEVANT / NEW_QUESTION / TOPIC_DRIFT
2. If NEW_QUESTION or TOPIC_DRIFT -> Output flag and STOP
3. STAGE 2: Apply user's clarification as HIGH CONFIDENCE override
4. STAGE 3: Determine history pattern reuse level (if history available)
5. STAGE 4: Generate SQL with mandatory requirements
6. Output reasoning_summary + SQL

OUTPUT REQUIREMENTS:
- Always output <reasoning_summary> first
- Then output ONE of: <new_question>, <topic_drift>, <sql>, or <multiple_sql>
- NEVER ask another follow-up question - this is the final generation step

CRITICAL REMINDERS:
- User's response resolves the ambiguity - apply it directly
- Every mandatory filter MUST be in WHERE clause
- Use UPPER() for all string comparisons
- Show calculation components (don't just show the result)
- Do NOT ask another follow-up question under any circumstances
"""

        for attempt in range(self.max_retries):
            try:
                # print('follow up sql prompt',followup_sql_prompt)
                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": followup_sql_prompt}],
                    max_tokens=3000,
                    temperature=0.0,  # Deterministic for SQL generation
                    top_p=0.1,
                    system_prompt="You are a Databricks SQL code generator processing follow-up clarifications. Your role is to generate SQL queries that incorporate both the original user question and their clarification answers. When users request calculations like 'cost per member' or 'margin per script', you generate SQL code to compute these - you do not perform the calculations yourself. You output SQL code wrapped in XML tags."
                )
                print('follow up sql response',llm_response)
                
                # Log LLM output - actual response truncated to 500 chars
                self._log('info', "LLM response received from SQL followup handler", state,
                         llm_response=llm_response,
                         attempt=attempt + 1)
                
                # Check for new_question flag first
                new_question_match = re.search(r'<new_question>.*?<detected>(.*?)</detected>.*?<reasoning>(.*?)</reasoning>.*?</new_question>', llm_response, re.DOTALL)
                if new_question_match:
                    detected = new_question_match.group(1).strip().lower() == 'true'
                    reasoning = new_question_match.group(2).strip()
                    if detected:
                        return {
                            'success': False,
                            'topic_drift': False,
                            'new_question': True,
                            'message': f"You've asked a new question instead of providing clarification. {reasoning}",
                            'original_followup_question': sql_followup_question,
                            'detected_new_question': sql_followup_answer
                        }

                # Check for topic_drift flag
                topic_drift_match = re.search(r'<topic_drift>.*?<detected>(.*?)</detected>.*?<reasoning>(.*?)</reasoning>.*?</topic_drift>', llm_response, re.DOTALL)
                if topic_drift_match:
                    detected = topic_drift_match.group(1).strip().lower() == 'true'
                    reasoning = topic_drift_match.group(2).strip()
                    if detected:
                        return {
                            'success': False,
                            'topic_drift': True,
                            'new_question': False,
                            'message': f"Your response seems unrelated to the clarification requested. {reasoning}",
                            'original_followup_question': sql_followup_question
                        }

                # Extract sql_story tag
                sql_story = ""
                story_match = re.search(r'<sql_story>(.*?)</sql_story>', llm_response, re.DOTALL)
                if story_match:
                    sql_story = story_match.group(1).strip()
                    print(f"📖 Captured SQL generation story from followup ({len(sql_story)} chars)")

                # Check for multiple SQL queries
                multiple_sql_match = re.search(r'<multiple_sql>(.*?)</multiple_sql>', llm_response, re.DOTALL)
                if multiple_sql_match:
                    multiple_content = multiple_sql_match.group(1).strip()
                    
                    # Extract individual queries with titles
                    query_matches = re.findall(r'<query(\d+)_title>(.*?)</query\1_title>.*?<query\1>(.*?)</query\1>', multiple_content, re.DOTALL)
                    if query_matches:
                        sql_queries = []
                        query_titles = []
                        for i, (query_num, title, query) in enumerate(query_matches):
                            cleaned_query = query.strip().replace('`', '')
                            cleaned_title = title.strip()
                            if cleaned_query and cleaned_title:
                                sql_queries.append(cleaned_query)
                                query_titles.append(cleaned_title)
                        
                        if sql_queries:
                            # 🆕 Extract history_sql_used flag
                            history_sql_used = False
                            history_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', llm_response, re.IGNORECASE)
                            if history_match:
                                history_sql_used = history_match.group(1).lower() == 'true'
                                print(f"📊 history_sql_used flag from LLM: {history_sql_used}")
                            
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'topic_drift': False,
                                'new_question': False,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,
                                'query_count': len(sql_queries),
                                'history_sql_used': history_sql_used,  # 🆕 NEW FIELD
                                'sql_story': sql_story  # NEW: Business-friendly explanation
                            }
                    
                    raise ValueError("Empty or invalid multiple SQL queries in XML response")
                
                # Check for single SQL query
                match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if match:
                    sql_query = match.group(1).strip()
                    sql_query = sql_query.replace('`', '')  # Remove backticks
                    
                    if not sql_query:
                        raise ValueError("Empty SQL query in XML response")
                    
                    # 🆕 Extract history_sql_used flag
                    history_sql_used = False
                    history_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', llm_response, re.IGNORECASE)
                    if history_match:
                        history_sql_used = history_match.group(1).lower() == 'true'
                        print(f"📊 history_sql_used flag from LLM: {history_sql_used}")
                    
                    return {
                        'success': True,
                        'multiple_sql': False,
                        'topic_drift': False,
                        'new_question': False,
                        'sql_query': sql_query,
                        'history_sql_used': history_sql_used,  # 🆕 NEW FIELD
                        'sql_story': sql_story  # NEW: Business-friendly explanation
                    }
                else:
                    raise ValueError("No valid XML response found (expected sql, multiple_sql, new_question, or topic_drift)")
            
            except Exception as e:
                print(f"❌ SQL generation with follow-up attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying SQL generation with follow-up... (Attempt {attempt + 1}/{self.max_retries})")
                    await asyncio.sleep(2 ** attempt)
        
        return {
            'success': False,
            'topic_drift': False,
            'new_question': False,
            'history_sql_used': False,  # 🆕 NEW FIELD
            'error': f"SQL generation with follow-up failed after {self.max_retries} attempts due to Model errors"
    }

    async def _execute_sql_with_retry_async(self, initial_sql: str, context: Dict, max_retries: int = 3) -> Dict:
        """Execute SQL with intelligent retry logic and async handling"""
        
        current_sql = initial_sql
        errors_history = []
        print('coming inside _execute_sql_with_retry_async')
        for attempt in range(max_retries):
            try:
                # Execute SQL with async handling
                print('calling execute_sql_async')
                result = await self.db_client.execute_sql_async(current_sql, timeout=300)
                
                
                # execute_sql_async returns List[Dict] directly, not a wrapped response
                if isinstance(result, list):
                    print(f"    ✅ SQL executed successfully on attempt {attempt + 1}")
                    return {
                        'success': True,
                        'data': result,
                        'final_sql': current_sql,
                        'attempts': attempt + 1,
                        'execution_time': 0  # Not available from direct list response
                    }
                else:
                    # This shouldn't happen with execute_sql_async, but handle just in case
                    error_msg = f"Unexpected response format: {type(result)}"
                    errors_history.append(f"Attempt {attempt + 1}: {error_msg}")
                    
                    # If not last attempt, try to fix the SQL
                    if attempt < max_retries - 1:
                        fix_result = await self._fix_sql_with_llm_async(current_sql, error_msg, errors_history, context)
                        if fix_result['success']:
                            current_sql = fix_result['fixed_sql']
                        else:
                            # If fixing fails, break the retry loop
                            break
            
            except Exception as e:
                error_msg = str(e)
                errors_history.append(f"Attempt {attempt + 1}: {error_msg}")
                
                if attempt < max_retries - 1:
                    # Check if this is a connection error - if so, add a delay before retry
                    is_connection_error = any(err_pattern in error_msg.lower() for err_pattern in [
                        'connection was forcibly closed',
                        'connection reset',
                        'connection aborted',
                        'winerror 10054',
                        'winerror 10053'
                    ])
                    
                    if is_connection_error:
                        import asyncio
                        delay = 3 * (attempt + 1)  # Progressive delay: 3s, 6s, 9s
                        print(f"    🔄 Connection error detected, waiting {delay}s before retry...")
                        await asyncio.sleep(delay)
                    
                    fix_result = await self._fix_sql_with_llm_async(current_sql, error_msg, errors_history, context)
                    if fix_result['success']:
                        current_sql = fix_result['fixed_sql']
                    else:
                        # If fixing fails, break the retry loop
                        break
        
        # All attempts failed
        return {
            'success': False,
            'error': f"All {max_retries} attempts failed. Last error: {errors_history[-1] if errors_history else 'Unknown error'}",
            'final_sql': current_sql,
            'attempts': max_retries,
            'errors_history': errors_history
        }

    async def _validate_fixed_sql(self, fixed_sql: str, original_sql: str, error_msg: str) -> Dict[str, Any]:
        """
        Guardrail validation to prevent embarrassing SQL outputs
        Returns dict with success=False if guardrails are violated
        """
        forbidden_patterns = [
            'show tables',
            'show databases',
            'describe table',
            'describe ',
            'information_schema.tables',
            'information_schema.columns',
            'show schemas',
            'list tables'
        ]
        
        sql_lower = fixed_sql.lower().strip()
        
        # Check for forbidden patterns
        for pattern in forbidden_patterns:
            if pattern in sql_lower:
                print(f"🚫 GUARDRAIL VIOLATION: Blocked '{pattern}' in LLM response")
                print(f"❌ Original error was: {error_msg}")
                
                return {
                    'success': False,
                    'error': f"Guardrail triggered: LLM attempted to generate '{pattern}' instead of fixing the query. This is not allowed.",
                    'violated_pattern': pattern,
                    'original_sql': original_sql
                }
        
        # Additional check: if it's a table not found error, ensure the fixed SQL 
        # still references a table (not just a utility query)
        table_not_found_indicators = ['table not found', 'table does not exist', 'no such table', 'invalid table name']
        if any(indicator in error_msg.lower() for indicator in table_not_found_indicators):
            # Check if the fixed SQL is suspiciously short or doesn't contain FROM
            if len(sql_lower) < 20 or 'from' not in sql_lower:
                print(f"🚫 GUARDRAIL VIOLATION: Fixed SQL too short or missing FROM clause for table-not-found error")
                return {
                    'success': False,
                    'error': "Guardrail triggered: Invalid fix for table-not-found error. Fixed query must contain valid FROM clause.",
                    'original_sql': original_sql
                }
        
        return {'success': True, 'fixed_sql': fixed_sql}


    async def _fix_sql_with_llm_async(self, failed_sql: str, error_msg: str, errors_history: List[str], context: Dict) -> Dict[str, Any]:
        """Use LLM to fix SQL based on error with enhanced prompting and retry logic async"""

        history_text = "\n".join(errors_history) if errors_history else "No previous errors"
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')

        fix_prompt = f"""
        You are an expert Databricks SQL developer. A SQL query has **FAILED** and needs to be **FIXED or REWRITTEN**.

        ==============================
        CONTEXT
        ==============================
        - ORIGINAL USER QUESTION: "{current_question}"
        - TABLE METADATA: {dataset_metadata}

        ==============================
        FAILURE DETAILS
        ==============================
        - FAILED SQL QUERY:
        ```sql
        {failed_sql}
        ```
        - ERROR MESSAGE: {error_msg}
        - PREVIOUS RETRY ERRORS: {history_text}

        ==============================
        CRITICAL ERROR INTERPRETATION RULES (FOLLOW EXACTLY)
        ==============================
        1. TIMEOUT / TRANSIENT EXECUTION ERRORS
            - If the ERROR MESSAGE indicates a timeout or transient execution condition (contains ANY of these case-insensitive substrings: 
            "timeout", "timed out", "cancelled due to timeout", "query exceeded", "network timeout", "request timed out", "socket timeout"),
            then DO NOT modify the SQL. Return the ORIGINAL FAILED SQL verbatim as the fixed version. (Root cause is environmental, not syntax.)
            - Still wrap it in <sql> tags exactly as required.

        2. COLUMN NOT FOUND / INVALID IDENTIFIER
            - If missing/invalid column error.
            - FIRST: If error text itself lists / hints alternative or available columns (patterns like "Did you mean", "Available columns", "Similar: colA, colB"), pick the best match to the ORIGINAL USER QUESTION intent from those suggestions (these override metadata if conflict).
            - ELSE: Select a replacement from TABLE METADATA (exact / case-insensitive / close semantic match). Never reuse the invalid name. Do not invent new columns.
            - Change only what is required; keep all other logic intact.

        3. TABLE NOT FOUND / TABLE DOES NOT EXIST
            ⚠️ CRITICAL PROHIBITION:
            - If the ERROR MESSAGE indicates a table does not exist (contains ANY of these case-insensitive substrings: "table not found", "table does not exist", "no such table", "invalid table name"):
            
            🚫 ABSOLUTELY FORBIDDEN - DO NOT GENERATE:
                - SHOW TABLES
                - SHOW DATABASES  
                - DESCRIBE TABLE
                - Information schema queries
                - Any query that lists or discovers tables
            
            ✅ ONLY ALLOWED OPTIONS:
                a) If TABLE METADATA contains a similarly named valid table → Replace with that exact table name
                b) If no valid alternative exists → Return ORIGINAL FAILED SQL unchanged with a SQL comment explaining why
            
            - The query MUST still attempt to answer the ORIGINAL USER QUESTION using only tables in TABLE METADATA

        4. OTHER ERROR TYPES (syntax, mismatched types, aggregation issues, grouping issues, function misuse, alias conflicts, etc.)
            - Rewrite or minimally adjust the SQL to resolve the issue while preserving the analytical intent of the ORIGINAL USER QUESTION.
            - Ensure any columns used are present in TABLE METADATA.
            - If a derived metric is implied, derive it transparently in SELECT with proper component columns.

        5. NEVER:
            - Never fabricate table or column names not present in metadata.
            - Never remove necessary GROUP BY columns required for non-aggregated selected columns.
            - Never switch to a different table unless clearly required to satisfy a missing valid column.
            - Never generate SHOW TABLES, DESCRIBE, or any schema discovery query.

        6. ALWAYS:
            - Preserve filters, joins, and calculation intent unless they reference invalid columns.
            - Use consistent casing and UPPER() comparisons for string equality.
            - Include replaced column(s) in SELECT list if they are used in filters or aggregations.

        ==============================
        EXAMPLE: TABLE NOT FOUND ERROR
        ==============================
        ❌ WRONG - THIS WILL BE REJECTED:
        <sql>
        SHOW TABLES;
        </sql>

        ✅ CORRECT - Return original with comment:
        <sql>
        -- Table 'sales_2024' not found in metadata. Returning original query.
        -- Available tables should be verified in TABLE METADATA.
        SELECT * FROM sales_2024 WHERE year = 2024;
        </sql>

        ✅ ALSO CORRECT - Use alternative from metadata:
        <sql>
        -- Replaced 'sales_2024' with 'sales_data' from available tables
        SELECT * FROM sales_data WHERE year = 2024;
        </sql>

        ==============================
        DECISION PATH (FOLLOW IN ORDER)
        ==============================
        IF timeout-related → return original query unchanged
        ELSE IF column-not-found → replace invalid column with valid one from metadata  
        ELSE IF table-not-found → use alternative from metadata OR return original with comment
        ELSE → fix syntax/logic while preserving intent

        ==============================
        FINAL VALIDATION BEFORE OUTPUT
        ==============================
        ✓ Does SQL contain SHOW, DESCRIBE, or INFORMATION_SCHEMA? → If YES, REWRITE
        ✓ Does SQL use only tables from TABLE METADATA? → If NO, return original
        ✓ Does SQL answer the ORIGINAL USER QUESTION? → If NO, revise
        ✓ Is it wrapped in <sql></sql> tags? → If NO, add them

        ==============================
        RESPONSE FORMAT
        ==============================
        Return ONLY the fixed SQL query wrapped in XML tags. No other text, explanations, or formatting.

        <sql>
        SELECT ...your fixed SQL here...
        </sql>
        """

        for attempt in range(self.max_retries):
            try:
                llm_response = await self.db_client.call_claude_api_endpoint_async([
                    {"role": "user", "content": fix_prompt}
                ])
                # print('sql fix prompt', fix_prompt)
                
                # Log LLM output - actual response truncated to 500 chars (no state available in this utility function)
                self._log('info', "LLM response received from SQL fix handler", None,
                         llm_response=llm_response,
                         attempt=attempt + 1)

                # Extract SQL from XML tags
                match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if match:
                    fixed_sql = match.group(1).strip()
                    fixed_sql = fixed_sql.replace('`', '')  # Remove backticks

                    if not fixed_sql:
                        raise ValueError("Empty fixed SQL query in XML response")

                    # 🛡️ GUARDRAIL VALIDATION
                    validation_result = await self._validate_fixed_sql(fixed_sql, failed_sql, error_msg)
                    
                    if not validation_result['success']:
                        # Guardrail violated - return error immediately
                        print(f"⛔ Guardrail check failed: {validation_result['error']}")
                        return validation_result
                    
                    # Validation passed
                    return {
                        'success': True,
                        'fixed_sql': fixed_sql
                    }
                else:
                    raise ValueError("No SQL found in XML tags")

            except Exception as e:
                print(f"❌ SQL fix attempt {attempt + 1} failed: {str(e)}")

                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying SQL fix... (Attempt {attempt + 1}/{self.max_retries})")
                    await asyncio.sleep(2 ** attempt)

        return {
            'success': False,
            'error': f"SQL fix failed after {self.max_retries} attempts due to Model errors"
        }
