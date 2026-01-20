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
            common_categorical_values = {'pbm','pbm retail', 'hdp', 'home delivery', 'mail', 'specialty','sp','claim fee','claim cost','activity fee','claimfee','claimcost','activityfee','8+4','9+3','2+10','5+7','optum','retail'}
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
                        filter_metadata_results = "\n**Columns found with this filter value which can be verified against meta data**\n"
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
        
        selection_prompt = f"""
You are analyzing a database schema to route SQL queries to the correct table(s).⚠️ CRITICAL CONTEXT: When users request metrics like "revenue per script", "cost per claim", etc., they are asking which TABLE contains the data - NOT asking you to perform calculations. The SQL generation system downstream will handle all calculations.

CORE PRINCIPLES

1. ALL-OR-NOTHING: A table is valid ONLY if it has ALL required metrics + ALL required attributes + ALL required filter columns. Partial matches are INVALID.

2. NO TRADE-OFFS: Never pick "closest match". If no table has all components → status="missing_items".
   WRONG: "Ledger has membership but no Carrier ID, picking anyway"
   CORRECT: "No table has both → missing_items"

3. ONE FOLLOW-UP CHANCE: You have exactly ONE opportunity to ask user for clarification. Use ONLY when:
   - Multiple tables FULLY qualify and need user preference
   - Filter value maps to multiple columns (genuine ambiguity)
   - User's question contains multiple sub-questions needing different tables
   NEVER hallucinate or guess. If genuinely ambiguous → ASK. If no table qualifies → missing_items.

**INPUTS**

QUESTION: {user_question}
EXTRACTED FILTER VALUES ALONG WITH TABLE NAMES: {filter_metadata_results}
AVAILABLE DATASETS:
{search_results}

STEP 1: CHECK USER EXPLICIT TABLE HINTS

If user explicitly specifies tables, RESPECT their choice even if suboptimal:
- "from table A and table B" / "compare X vs Y" → Select BOTH tables
- "using [table_name]" / "fetch from [table_name]" → Select that table
- "get X from table A, Y from table B" → Select BOTH tables A and B

Decision logic:
- User mentions multiple tables → Return ALL mentioned (status="success", multi_table_mode="user_requested")
- Single table sufficient but user wants multiple → RESPECT user's choice
- User specifies table lacking required columns → status="needs_disambiguation", explain limitation

If no explicit hint → Continue to Step 2.

STEP 2: EXTRACT REQUIRED COMPONENTS

A. METRICS (values to aggregate):
- Direct: revenue, expense, cost, cogs, membership, scripts, volume, margin, billed amount, allowed amount
- Fuzzy:
  * "scripts/prescriptions" → unadjusted_scripts, adjusted_scripts, 30_day_scripts, 90_day_scripts
  * "cost/costs" → expense, cogs, cost per script
  * "margin" → gross margin, Gross Margin %, margin per script
  * "billing/billed" → billed amount, revenue from billing
- Skip calculations (handled downstream): variance, growth, change, percentage, performance

B. ATTRIBUTES (GROUP BY dimensions):
- Triggered by: "by [X]", "per [X]", "for each [X]", "top N [X]"
- Direct: Client Name, Drug Name, Therapy Class, Carrier ID, Client ID, Line of Business
- Fuzzy:
  * "therapy/therapies" → Therapy Class
  * "client/clients" → Client Name, Client ID
  * "carrier" → Carrier ID
  * "member/patient" → Member ID (PHI check required)
- BLOCK substitutions: Product Category ≠ Drug, ingredient_fee ≠ expense

C. FILTER COLUMNS (WHERE clause):
- Triggered by: "for [value]", specific values in question
- Identify COLUMN NAME required, not just value:
  * "for carrier MPDOVA" → filter_column: Carrier ID
  * "for client Acme Corp" → filter_column: Client Name
  * "for drug Humira" → filter_column: Drug Name
  * "for vaccines" (therapy class value) → filter_column: Therapy Class
- CRITICAL: Filter columns are REQUIRED - table MUST have them in "attrs" list
- Skip universal filters (exist in all tables): external, SP, PBM, HDP, optum, mail, specialty, home delivery, PBM retail

STEP 3: CHECK DIRECT TABLE KEYWORD MATCH

Check for explicit table references:
- "ledger" → actuals_vs_forecast table
- "claims" or "claim" → claim_transaction table
- "billing" → billing_extract table

If keyword found AND table passes all validations → status="success"
If keyword found BUT table fails validations → Continue to full evaluation
If no keyword → Continue to Step 4

STEP 4: VALIDATE EACH TABLE (ALL-OR-NOTHING)

For each table, apply checks IN ORDER. Must pass ALL to be valid.

CHECK 1 - PHI/PII Security:
- Question requests columns in "phi_cols"? → status="phi_found", STOP

CHECK 2 - "not_useful_for" Elimination:
- Question matches any item in "not_useful_for"? → ELIMINATE
- Check BOTH breakdown AND filter contexts:
  * Filter "for carrier X" + "carrier analysis" in not_useful_for → ELIMINATE
  * Filter "for drug X" + "drug/therapy analysis" in not_useful_for → ELIMINATE
  * Breakdown "by therapy class" + "drug/therapy analysis" in not_useful_for → ELIMINATE
  * Filter "for vaccines" (therapy value) + "drug/therapy analysis" in not_useful_for → ELIMINATE
- CRITICAL: Any question with drug/therapy/vaccine/medication filters OR breakdowns → Check for "drug/therapy analysis"
- Examples:
  * Q: "top 10 clients by expense" + Ledger not_useful_for:["client expense/margin alone"] → ELIMINATE Ledger
  * Q: "top 10 drugs by revenue" + Ledger not_useful_for:["drug/therapy analysis"] → ELIMINATE Ledger
  * Q: "script count for vaccines" + Ledger not_useful_for:["drug/therapy analysis"] → ELIMINATE Ledger

CHECK 3 - Metrics Validation:
- Table's "metrics" contains ALL required metrics? → If missing any → ELIMINATE

CHECK 4 - Attributes Validation:
- Table's "attrs" contains ALL required attributes?
- DO NOT assume attributes not explicitly listed in "attrs"
- Must have 100% match - no partial matching
- If missing any → ELIMINATE
- Example:
  * Q: "revenue by therapy class, drug name"
  * Required: ["Therapy Class", "Drug Name"]
  * Claims CONTAINS both → KEEP Claims ✅

CHECK 5 - Filter Columns Validation:
- Table's "attrs" contains ALL required filter columns?
- If missing any → ELIMINATE
- Example:
  * Q: "membership for carrier MPDOVA"
  * Required filter column: Carrier ID
  * Ledger attrs: [..., "Client ID", "Client Name"] → NO "Carrier ID" → ELIMINATE Ledger ❌
  * Claims attrs: [..., "Carrier ID", ...] → KEEP Claims ✅

STEP 5: TABLE SELECTION BASED ON FILTER COLUMN AVAILABILITY

When filter values exist in EXTRACTED FILTER VALUES:
- Filter column in ONE table → SELECT that table
- Filter column in MULTIPLE tables → Match question keywords to table:
  * "billing/billed/bill" → Billing table
  * "claim/claims/transaction" → Claims table
  * "ledger/forecast/actual" → Ledger table
  * No keyword match → Use "useful_for", "high_level", then ask if still tied
- Filter column in NO tables → ELIMINATE those tables

Note: Never ask "which column?" - SQL Writer handles column selection.

STEP 6: CHECK FOR MULTI-QUESTION OR JOIN SCENARIOS
   - User asks multiple questions where each needs a different table (e.g., "show revenue from claims and script count from ledger")
   - Tables can be joined using relationships defined in metadata (if join keys available)
   - User explicitly requests data from multiple sources
   This is NOT a fallback for missing components - each table must independently qualify for its portion.

STEP 7: DECIDE BASED ON VALID TABLES

ONE table passed:
→ status="success"

MULTIPLE tables passed (for same question):
→ Apply tie-breakers:
  1. Check "high_level" field: prefer high_level=true for summary queries
  2. Check "useful_for" for best match
  3. Still tied → status="needs_disambiguation", use your ONE follow-up chance
→ Example: Both Claims and Ledger have [revenue, Client Name], Ledger has high_level=true → SELECT Ledger

ZERO tables passed:
→ status="missing_items"
→ Explain what's missing and suggest alternatives
→ NEVER hallucinate or pick "closest match"

EXAMPLES

Example 1: Tie-breaker with high_level
Q: "Revenue by client or line of business"
- Extracted: metrics=[revenue], attributes=[client name, line of business], filter_columns=[]
- Validation: Ledger PASSED, Claims PASSED (both have revenue + client name + lob)
- Decision: Multiple passed → Ledger has high_level=true → SELECT Ledger

Example 2: Drug/therapy eliminates Ledger and triggers not_useful_for
Q: "script count for vaccines"
- Extracted: metrics=[scripts], attributes=[], filter_columns=[Therapy Class]
- Validation: Ledger ELIMINATED (not_useful_for "drug/therapy analysis"), Claims PASSED
- Decision: SELECT Claims - Ledger blocked for therapy filtering

Example 4: Missing filter column → missing_items
Q: "membership count for carrier MPDOVA"
- Extracted: metrics=[membership], attributes=[], filter_columns=[Carrier ID]
- Validation:
  * Ledger: membership ✓, Carrier ID ✗, not_useful_for "carrier analysis" ✗ → ELIMINATED
  * Claims: membership ✗ → ELIMINATED
  * Billing: membership ✗ → ELIMINATED
- Decision: ZERO passed → status="missing_items"
- Message: "'membership' only exists in Ledger, but Ledger lacks Carrier ID. Try: 'membership by client' or 'script count for carrier MPDOVA'."

Example 5: Multi-question complementary
Q: "show claims revenue by client and also billing amount by client"
- Sub-question 1: "claims revenue by client" → Claims table (has revenue, Client Name) ✓
- Sub-question 2: "billing amount by client" → Billing table (has billed amount, Client Description) ✓
- Decision: Both tables qualify for their portions → status="success", multi_table_mode="complementary"

**OUTPUT FORMAT**

REASONING (3-5 lines):
- Extracted: metrics=[...], attributes=[...], filter_columns=[...]
- Eliminated: [Table: reason], [Table: reason]
- Selected: [Table] - [brief reason]

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
                    if high_level_selected:
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
    
    def _build_sql_planner_prompt(self, context: Dict, state: Dict) -> str:
        """Build the SQL planner prompt (Call 1)
        
        Outputs unified structured context for SQL writer.
        """
        
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        filter_metadata_results = state.get('filter_metadata_results', [])
        mandatory_columns_text = state.get('mandatory_columns_text', '')
        history_question_match = state.get('history_question_match', '')
        matched_sql = state.get('matched_sql', '')

        # History hint for filter resolution
        history_hint = ""
        if matched_sql and history_question_match:
            history_hint = f"""
HISTORY REFERENCE (for filter column resolution - NOT for time filters):
Previous question: {history_question_match}
<historical_sql>
{matched_sql}
</historical_sql>
Use history to validate filter column choices. Never use history for time values.
"""

        prompt = f"""BUSINESS CONTEXT: You are a SQL query planning assistant for DANA (Data Analytics Assistant), an internal enterprise business intelligence system at Optum. Your role is to help analysts generate accurate database queries for authorized business reporting and analytics on de-identified aggregate healthcare metrics such as revenue, cost, utilization, and operational performance.

TASK: Analyze the user's business question and validate that all required data elements can be mapped to available database columns. Output a structured plan for SQL generation.

CORE RULES

1. ONE FOLLOW-UP OPPORTUNITY
   You have exactly ONE chance to ask clarification:
   - Unknown value that can't be mapped → ASK
   - Multiple columns match same value → ASK  
   - Vague time reference ("recently", "lately") → ASK
   - Unclear metric or grouping intent → ASK
   BETTER TO ASK than to ASSUME WRONG.

2. MAPPING PRINCIPLES
   - TERMS (revenue, cost, count, margin) → Match semantically to columns
   - VALUES (MPDOVA, Specialty, HDP) → EXACT match only from EXTRACTED FILTERS or METADATA

3. ZERO INVENTION
   - Never add filters not mentioned in question
   - Never assume time period if not specified
   - Never guess column when multiple options exist

INPUTS

QUESTION: {current_question}

EXTRACTED FILTER VALUES: {filter_metadata_results}

{history_hint}

METADATA: {dataset_metadata}
MANDATORY FILTERS: {mandatory_columns_text}


VALIDATION STEPS

【STEP 1: PARSE QUESTION】

Extract from question:
- TERMS: Business concepts (revenue, cost, scripts, margin, carrier, client, product)
- VALUES: Specific data points (MPDOVA, Specialty, HDP, July 2025, Q3)
- INTENT: simple_aggregate | breakdown | comparison | top_n | trend
- USER HINTS: Explicit guidance like "use carrier_id" → Apply as override

【STEP 2: MAP TERMS TO COLUMNS】

For each TERM, find matching column in METADATA:
- Single match found → Use it
- Multiple matches → Follow-up required
- No match → Follow-up required

【STEP 2B: BUILD METRIC EXPRESSIONS】

Construct full SQL expression based on METADATA structure:

Pattern A - Direct Column:
  Table has revenue_amount column
  → SUM(t1.revenue_amount) AS total_revenue

Pattern B - Metric Type Filter:
  Table has amount + metric_type column
  → SUM(CASE WHEN UPPER(t1.metric_type) = UPPER('REVENUE') THEN t1.amount ELSE 0 END) AS total_revenue

Pattern C - Calculated Metric:
  margin = revenue - cost
  → SUM(CASE WHEN UPPER(t1.metric_type) = UPPER('REVENUE') THEN t1.amount ELSE 0 END) - SUM(CASE WHEN UPPER(t1.metric_type) = UPPER('COST') THEN t1.amount ELSE 0 END) AS margin

【STEP 3: MAP VALUES TO COLUMNS】

For each VALUE, resolve using this priority:

A. SYNONYM CHECK - Look for patterns in METADATA like "Mail→Home Delivery", "SP→Specialty"
   If found → Use mapped column with mapped value

B. EXTRACTED FILTERS CHECK
   EXTRACTED FILTER VALUES are pre-verified values from the database.
   - Value found in ONE column → Validate column exists in METADATA → Use it (case-insensitive, UPPER() handles it)
   - Value found in MULTIPLE columns → Apply intelligent selection:
     1. Check which column's sample values actually contain the exact filter value from question
     2. If ONE column has exact match → Use that column
     3. If MULTIPLE columns have exact match or NONE have exact match → Check HISTORY for hint
     4. If no history → Follow-up asking WHICH COLUMN
   - Value NOT found in extracted filters → Continue to METADATA check

   ⚠️ NEVER ask about value case sensitivity - UPPER() in SQL handles all case matching

C. HISTORY SQL REFERENCE (if available)
   - Value in MULTIPLE columns + History used one → Use history's column
   - Value in SINGLE column + Same in history → Confirms mapping
   ⚠️ Never use history for TIME filters

D. METADATA SAMPLES CHECK - Search sample values in column descriptions
   Found → Use that column

E. VALUE NOT MAPPED - If fails all checks and not a number → Follow-up required

【STEP 4: MAP TIME FILTERS】

If question contains time references:
1. PARSE naturally (July 2025, Q3 2024, 2025, Jan to March 2025, YTD)
   Vague like "recently", "lately" → Follow-up required
2. MAP to date columns in METADATA (year, month, quarter, or date columns)
3. CONSTRUCT filter with correct data type

No time mentioned → Do NOT add time filters

【STEP 5: MANDATORY FILTER CHECK】

Every MANDATORY filter must appear in output.
Missing mandatory → Cannot generate SQL

【STEP 6: MULTI-TABLE HANDLING】

Single table → Include one QUERY block
Multiple tables with JOIN INFO → Include JOIN details
Multiple tables, no join → Include separate QUERY blocks

【STEP 7: FINAL DECISION】

FOLLOWUP_REQUIRED if ANY: Unknown value | Ambiguous column | Vague time | Unclear intent
SQL_READY if ALL: Every term mapped | Every value mapped | Time mapped (or none needed)

OUTPUT FORMAT

Output ONLY <context> block, optionally followed by <followup>:

<context>
DECISION: [SQL_READY | FOLLOWUP_REQUIRED]
QUERY_TYPE: [SINGLE_TABLE | MULTI_TABLE_JOIN | MULTI_TABLE_SEPARATE]
INTENT: [simple_aggregate | breakdown | comparison | top_n | trend]

QUERY_1:
TABLE: [full.table.name] AS [alias]
ANSWERS: [what this query answers - use "full question" for single query]

SELECT:
- [t1.column1]
- [SUM(CASE WHEN UPPER(t1.metric_type) = UPPER('REVENUE') THEN t1.amount ELSE 0 END) AS revenue]

FILTERS:
- [UPPER(t1.carrier_id) = UPPER('MPDOVA')] [STRING]
- [t1.year = 2025] [INT]
- [t1.month = 7] [INT]
- [UPPER(t1.ledger) = UPPER('GAAP')] [MANDATORY]

GROUP_BY: [t1.column1, t1.column2] or [none]
ORDER_BY: [revenue DESC] or [none]
LIMIT: [10] or [none]

JOIN: [t1.key = t2.key LEFT JOIN] or [none]

QUERY_2 (only if MULTI_TABLE_SEPARATE or MULTI_TABLE_JOIN):
TABLE: [full.table.name] AS [alias]
ANSWERS: [what this query answers]

SELECT:
- [columns and expressions]

FILTERS:
- [filters with type tags]

GROUP_BY: [columns] or [none]
ORDER_BY: [direction] or [none]
LIMIT: [number] or [none]
</context>

IF FOLLOWUP_REQUIRED, add after </context>:

<followup>
I need one clarification to generate accurate SQL:

[Brief question about the specific ambiguity]

Options:
1. [column_name] - [brief description with sample values]
2. [column_name] - [brief description with sample values]

Which one did you mean?
</followup>

RULES FOR OUTPUT
- Always include DECISION, QUERY_TYPE, INTENT at top
- Always use QUERY_1 block (even for single table)
- QUERY_2 only when multiple tables needed
- FILTERS must include data type: [STRING], [INT], [DATE]
- FILTERS must mark [MANDATORY] for mandatory filters
- String filters must use UPPER(): UPPER(col) = UPPER('value')
- SELECT expressions must be complete and ready to use
- Use table alias (t1, t2) for all column references
"""
        return prompt


    def _build_sql_writer_prompt(self, context_output: str, state: Dict, current_question: str) -> str:
        """Build the SQL writer prompt (Call 2)
        
        Takes raw context from planner and generates SQL with pattern learning.
        """
        
        history_question_match = state.get('history_question_match', '')
        matched_sql = state.get('matched_sql', '')
        has_history = bool(matched_sql and history_question_match)
        
        
        # Build history section
        if has_history:
            history_section = f"""
HISTORICAL SQL FOR PATTERN LEARNING

PREVIOUS QUESTION: {history_question_match}

<historical_sql>
{matched_sql}
</historical_sql>

PURPOSE: History represents LEARNED DETAIL PREFERENCES. Enhance simple questions with historical detail patterns.
PRINCIPLE: If history shows breakdown + totals, provide that detail level even if user asks simple question.

PATTERN DETECTION:

DETECT PATTERN TYPE:
- Contains "GROUPING SETS" + "GROUPING(" function → GROUPING_SETS_TOTAL
- Contains "UNION ALL" + 'Total'/'OVERALL' literal → UNION_TOTAL
- Neither → SIMPLE

IF GROUPING_SETS_TOTAL detected, extract:
- breakdown_column: column inside GROUPING() function
- parent_dimension: the parent filter column
- total_label: label used (OVERALL_TOTAL, Total, etc.)
- order_position: total first (0) or last (1) in ORDER BY

IF UNION_TOTAL detected, extract:
- How the total row is constructed
- What literal is used for the total label

ENHANCEMENT DECISION:

ENHANCE = YES when ALL true:
✓ Pattern is GROUPING_SETS_TOTAL or UNION_TOTAL
✓ Same/similar metric (both ask for revenue, both ask for cost, etc.)
✓ Current question filters on PARENT dimension of history's breakdown
✓ User did NOT say "total only", "just sum", "single number", "aggregate only"

ENHANCE = NO when ANY true:
✗ Pattern is SIMPLE (nothing to inherit)
✗ Different metric type entirely
✗ User explicitly wants only aggregate total
✗ Current already has different explicit grouping

HISTORY_SQL_USED FLAG:

Set history_sql_used = TRUE when ANY of these apply:
✓ Pattern inherited (GROUPING_SETS_TOTAL or UNION_TOTAL applied)
✓ History question is similar AND you learned/referenced:
  - Column selections or naming conventions
  - Filter logic or WHERE clause patterns
  - Calculation approach (CASE WHEN structure, formulas)
  - SQL structure (CTE, subquery patterns)
✓ History provided trusted reference that influenced your SQL generation

Set history_sql_used = FALSE ONLY when:
✗ No history SQL available
✗ History is completely irrelevant (different metric, different table, unrelated question)

NOTE: Even if ENHANCE = NO (no GROUPING_SETS/UNION pattern), set history_sql_used = TRUE if history influenced column choices, calculations, or structure.
"""
        else:
            history_section = """
HISTORICAL SQL:
No historical SQL available. Generate fresh SQL based on context.
Set pattern_detected = NONE and history_sql_used = false
"""
        prompt = f"""BUSINESS CONTEXT: You are a Databricks SQL code generator for DANA (Data Analytics Assistant), an internal enterprise business intelligence system at Optum. Your role is to generate accurate SQL queries for authorized business reporting and analytics on de-identified aggregate healthcare metrics.

TASK: Generate production-ready Databricks SQL based on the validated query plan and historical pattern learning.

CURRENT QUESTION: {current_question}

PLANNED CONTEXT:
{context_output}
{history_section}

SQL GENERATION RULES

PRIORITY 0: MANDATORY REQUIREMENTS (violation = query failure)

M1. MANDATORY FILTERS - Filters marked [MANDATORY] MUST be in WHERE clause, NEVER omit
M2. CASE-INSENSITIVE - Use UPPER() for all string comparisons
M3. SAFE DIVISION - Always use NULLIF(denominator, 0)
M4. NUMERIC FORMATTING - Amounts: ROUND(x, 0), Percentages: ROUND(x, 3)
M5. ONE COLUMN PER FILTER - Apply filter to single column, no OR across multiple columns

PRIORITY 1: METRIC TYPE HANDLING

- Pivot metric_type via CASE WHEN, never GROUP BY metric_type for calculations
- Pattern: SUM(CASE WHEN metric_type = 'X' THEN amt ELSE 0 END) AS x_value
- Calculations happen across columns in same row, not across rows

PRIORITY 2: COMPONENT DISPLAY

- Always show source components alongside calculated metrics
- Pattern: component_1, component_2, component_1 / NULLIF(component_2, 0) AS derived_metric

PRIORITY 3: QUERY PATTERNS

TIME COMPARISON (Always side-by-side columns, NEVER GROUP BY time period):
- Each period as column: SUM(CASE WHEN period = X THEN amt ELSE 0 END) AS period_x
- Include variance: period_2 - period_1 AS variance
- Include variance_pct: (variance / NULLIF(period_1, 0)) * 100
- Applies to: MoM, QoQ, YoY, any "X vs Y" time comparison

TOP N:
- ORDER BY metric DESC LIMIT N
- Include percentage of total when relevant

PERCENTAGE OF TOTAL:
- value, value * 100.0 / NULLIF((SELECT SUM FROM same_filters), 0) AS pct

PATTERN - BREAKDOWN BY MULTIPLE VALUES:
SELECT product_category, ROUND(SUM(amount), 0) AS value
FROM table
WHERE UPPER(product_category) IN (UPPER('HDP'), UPPER('SP'))
GROUP BY product_category

READING CONTEXT:

QUERY_TYPE = SINGLE_TABLE:
- Read QUERY_1 block
- Build single SELECT...FROM...WHERE...GROUP BY statement

QUERY_TYPE = MULTI_TABLE_JOIN:
- Read QUERY_1 and QUERY_2 blocks
- Use JOIN clause from context
- Build single SQL with JOIN

QUERY_TYPE = MULTI_TABLE_SEPARATE:
- Read each QUERY_N block
- Generate SEPARATE SQL for each
- Each query answers part of the question (see ANSWERS field)
- Output in <multiple_sql> format

BUILDING SQL FROM CONTEXT:

1. SELECT: Use expressions from SELECT section exactly as provided
2. FROM: Use TABLE from context with alias
3. WHERE: Apply all FILTERS from context (already have UPPER() for strings)
4. GROUP BY: Use GROUP_BY from context (skip if "none")
5. ORDER BY: Use ORDER_BY from context (skip if "none")
6. LIMIT: Use LIMIT from context (skip if "none")

APPLY HISTORY PATTERN (if ENHANCE = YES):

IF GROUPING_SETS_TOTAL:
-- Use CTE for calculations, then apply GROUPING() in final SELECT. Learn that exact pattern from history sql.

IF UNION_TOTAL:
-- Detail query
SELECT dimension, breakdown_col, ROUND(SUM(metric), 0) AS metric
FROM table WHERE [filters]
GROUP BY dimension, breakdown_col

UNION ALL

-- Total query  
SELECT dimension, 'OVERALL_TOTAL' AS breakdown_col, ROUND(SUM(metric), 0) AS metric
FROM table WHERE [filters]
GROUP BY dimension

ORDER BY dimension, CASE WHEN breakdown_col = 'OVERALL_TOTAL' THEN 0 ELSE 1 END

IF ENHANCE = NO:
Generate straightforward SQL based on context INTENT:
- simple_aggregate → No GROUP BY on dimensions, just aggregate
- breakdown → GROUP BY dimension columns
- comparison → Side-by-side CASE WHEN for periods/categories
- top_n → ORDER BY metric DESC LIMIT N
- trend → GROUP BY time dimension, ORDER BY time

OUTPUT FORMAT

FOR SINGLE_TABLE and MULTI_TABLE_JOIN:

<pattern_analysis>
pattern_detected: [GROUPING_SETS_TOTAL | UNION_TOTAL | SIMPLE | NONE]
breakdown_column: [column or null]
parent_dimension: [column or null]
enhance_decision: [YES | NO]
enhance_reason: [brief explanation]
</pattern_analysis>

<sql>
[Complete Databricks SQL]
</sql>

<sql_story>
[2-3 sentences explaining the query in business terms]
</sql_story>

<history_sql_used>[true | false]</history_sql_used>

FOR MULTI_TABLE_SEPARATE:

<pattern_analysis>
pattern_detected: NONE
enhance_decision: NO
enhance_reason: Multiple separate queries - history pattern not applicable
</pattern_analysis>

<multiple_sql>
<query1_title>[From QUERY_1 ANSWERS field - max 8 words]</query1_title>
<query1>
[SQL for QUERY_1]
</query1>
<query2_title>[From QUERY_2 ANSWERS field - max 8 words]</query2_title>
<query2>
[SQL for QUERY_2]
</query2>
</multiple_sql>

<sql_story>
[Explain that question required data from multiple tables without join relationship. Describe what each query returns.]
</sql_story>

<history_sql_used>false</history_sql_used>
"""
        return prompt


    def _extract_context_and_followup(self, response: str) -> tuple[str, bool, str]:
        """Extract context block and check for followup
        
        Returns:
            tuple: (context_content, needs_followup, followup_text)
        """
        
        # Extract context block
        context_match = re.search(r'<context>(.*?)</context>', response, re.DOTALL)
        if not context_match:
            raise ValueError("No <context> block found in response")
        
        context_content = context_match.group(1).strip()
        
        # Check for followup - either in tag or in DECISION
        followup_match = re.search(r'<followup>(.*?)</followup>', response, re.DOTALL)
        needs_followup = bool(followup_match) or 'DECISION: FOLLOWUP_REQUIRED' in context_content
        followup_text = followup_match.group(1).strip() if followup_match else ""
        
        return context_content, needs_followup, followup_text


    async def _assess_and_generate_sql_async(self, context: Dict, state: Dict) -> Dict[str, Any]:
        """SQL generation with two-stage approach
        
        Stage 1 (SQL Planner): Validates and builds context
        Stage 2 (SQL Writer): Generates SQL from context
        """
        
        current_question = context.get('current_question', '')
        selected_datasets = state.get('selected_dataset', [])
        
        # ============================================================
        # STEP 1: Search for historical SQL feedback
        # ============================================================
        print(f"🔍 Searching feedback SQL for: {selected_datasets}")
        feedback_results = await self.db_client.sp_vector_search_feedback_sql(
            current_question, table_names=selected_datasets
        )
        
        matched_sql = ''
        history_question_match = ''
        matched_table_name = ''
        
        if feedback_results:
            print(f"🤖 Analyzing {len(feedback_results)} feedback candidates...")
            feedback_selection_result = await self.db_client._llm_feedback_selection(feedback_results, state)
            
            if feedback_selection_result.get('status') == 'match_found':
                matched_seq_id = feedback_selection_result.get('seq_id')
                
                for result in feedback_results:
                    if result.get('seq_id') == matched_seq_id:
                        history_question_match = result.get('user_question', '')
                        matched_sql = result.get('sql_query', '')
                        matched_table_name = result.get('table_name', '')
                        
                        state['history_question_match'] = history_question_match
                        state['matched_sql'] = matched_sql
                        state['matched_table_name'] = matched_table_name
                        
                        print(f"✅ History match: {matched_table_name}")
                        break
        
        has_history = bool(matched_sql and history_question_match)

        if has_history :
            history_section = f"""
HISTORICAL SQL FOR PATTERN LEARNING

PREVIOUS QUESTION: {history_question_match}

<historical_sql>
{matched_sql}
</historical_sql>

PURPOSE: History represents LEARNED DETAIL PREFERENCES. Enhance simple questions with historical detail patterns.
PRINCIPLE: If history shows breakdown + totals, provide that detail level even if user asks simple question.

PATTERN DETECTION:

DETECT PATTERN TYPE:
- Contains "GROUPING SETS" + "GROUPING(" function → GROUPING_SETS_TOTAL
- Contains "UNION ALL" + 'Total'/'OVERALL' literal → UNION_TOTAL
- Neither → SIMPLE

IF GROUPING_SETS_TOTAL detected, extract:
- breakdown_column: column inside GROUPING() function
- parent_dimension: the parent filter column
- total_label: label used (OVERALL_TOTAL, Total, etc.)
- order_position: total first (0) or last (1) in ORDER BY

IF UNION_TOTAL detected, extract:
- How the total row is constructed
- What literal is used for the total label

ENHANCEMENT DECISION:

ENHANCE = YES when ALL true:
✓ Pattern is GROUPING_SETS_TOTAL or UNION_TOTAL
✓ Same/similar metric (both ask for revenue, both ask for cost, etc.)
✓ Current question filters on PARENT dimension of history's breakdown
✓ User did NOT say "total only", "just sum", "single number", "aggregate only"

ENHANCE = NO when ANY true:
✗ Pattern is SIMPLE (nothing to inherit)
✗ Different metric type entirely
✗ User explicitly wants only aggregate total
✗ Current already has different explicit grouping

HISTORY_SQL_USED FLAG:

Set history_sql_used = TRUE when ANY of these apply:
✓ Pattern inherited (GROUPING_SETS_TOTAL or UNION_TOTAL applied)
✓ History question is similar AND you learned/referenced:
  - Column selections or naming conventions
  - Filter logic or WHERE clause patterns
  - Calculation approach (CASE WHEN structure, formulas)
  - SQL structure (CTE, subquery patterns)
✓ History provided trusted reference that influenced your SQL generation

Set history_sql_used = FALSE ONLY when:
✗ No history SQL available
✗ History is completely irrelevant (different metric, different table, unrelated question)

NOTE: Even if ENHANCE = NO (no GROUPING_SETS/UNION pattern), set history_sql_used = TRUE if history influenced column choices, calculations, or structure.

"""
        else:
            history_section = """
HISTORICAL SQL:
No historical SQL available. Generate fresh SQL based on context.
Set pattern_detected = NONE and history_sql_used = false
"""
        state['sql_history_section'] = history_section
        
        # ============================================================
        # STEP 2: Build mandatory columns
        # ============================================================
        mandatory_column_mapping = {
            "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast": ["Ledger"],
            "prd_optumrx_orxfdmprdsa.rag.pbm_claims": ["product_category='PBM'"]
        }
        
        mandatory_columns_info = []
        if isinstance(selected_datasets, list):
            for dataset in selected_datasets:
                if dataset in mandatory_column_mapping:
                    for col in mandatory_column_mapping[dataset]:
                        mandatory_columns_info.append(f"Table {dataset}: {col} (MANDATORY)")
        
        state['mandatory_columns_text'] = "\n".join(mandatory_columns_info) if mandatory_columns_info else "None"
        
        # ============================================================
        # CALL 1: SQL Planner
        # ============================================================
        print("=" * 60)
        print("📋 CALL 1: SQL Planner")
        print("=" * 60)
        
        planner_prompt = self._build_sql_planner_prompt(context, state)
        print("planner Prompt:", planner_prompt)
        
        context_output = None
        
        for attempt in range(self.max_retries):
            try:
                planner_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": planner_prompt}],
                    max_tokens=10000,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt="You are a SQL query planning assistant for an internal enterprise business intelligence system. Your role is to validate and map business questions to database schemas for authorized analytics reporting. Output ONLY <context> block, optionally followed by <followup>. No other text."
                )
                
                print("SQL Planner Response:", planner_response)                
                # Extract context and check followup
                context_output, needs_followup, followup_text = self._extract_context_and_followup(planner_response)
                
                if needs_followup:
                    print(f"❓ Followup needed: {followup_text[:150]}...")
                    state['is_sql_followup'] = True
                    state['reasoning_context'] = context_output
                    
                    return {
                        'success': True,
                        'needs_followup': True,
                        'sql_followup_questions': followup_text,
                        'used_history_asset': False,
                        'history_sql_used': False,
                        'reasoning_context': context_output
                    }
                
                print("✅ Planner complete - SQL_READY")
                state['reasoning_context'] = context_output
                break
                
            except Exception as e:
                print(f"❌ Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return {
                        'success': False,
                        'error': f"SQL Planner failed: {e}",
                        'used_history_asset': False,
                        'history_sql_used': False
                    }
        
        # ============================================================
        # CALL 2: SQL Writer
        # ============================================================
        print("=" * 60)
        print("🔨 CALL 2: SQL Writer")
        print("=" * 60)
        
        writer_prompt = self._build_sql_writer_prompt(context_output, state, current_question)
        print(f"📊 Prompt: {len(writer_prompt)} chars | {datetime.now().strftime('%H:%M:%S')}")
        
        for attempt in range(self.max_retries):
            try:
                print("writer prompt",writer_prompt)
                writer_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": writer_prompt}],
                    max_tokens=2500,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt="You are a Databricks SQL code generator for an internal enterprise business intelligence system. Your role is to generate production-ready SQL queries for authorized analytics reporting based on validated query plans. Output in the specified XML format."
                )
                
                print(f"SQL writer Response: {writer_response}")
                
                # Extract pattern analysis
                pattern_analysis = ""
                pattern_match = re.search(r'<pattern_analysis>(.*?)</pattern_analysis>', writer_response, re.DOTALL)
                if pattern_match:
                    pattern_analysis = pattern_match.group(1).strip()
                    state['pattern_analysis'] = pattern_analysis
                
                # Extract history_sql_used
                history_sql_used = False
                history_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', writer_response, re.IGNORECASE)
                if history_match:
                    history_sql_used = history_match.group(1).strip().lower() == 'true'
                
                # Extract sql_story
                sql_story = ""
                story_match = re.search(r'<sql_story>(.*?)</sql_story>', writer_response, re.DOTALL)
                if story_match:
                    sql_story = story_match.group(1).strip()
                
                # Check for multiple SQL
                multiple_match = re.search(r'<multiple_sql>(.*?)</multiple_sql>', writer_response, re.DOTALL)
                if multiple_match:
                    multiple_content = multiple_match.group(1).strip()
                    query_matches = re.findall(
                        r'<query(\d+)_title>(.*?)</query\1_title>.*?<query\1>(.*?)</query\1>',
                        multiple_content, re.DOTALL
                    )
                    
                    if query_matches:
                        sql_queries = []
                        query_titles = []
                        for _, title, query in query_matches:
                            cleaned_query = query.strip().replace('`', '')
                            cleaned_title = title.strip()
                            if cleaned_query:
                                sql_queries.append(cleaned_query)
                                query_titles.append(cleaned_title)
                        
                        if sql_queries:
                            print(f"✅ Generated {len(sql_queries)} queries")
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,
                                'query_count': len(sql_queries),
                                'used_history_asset': has_history,
                                'history_sql_used': history_sql_used,
                                'sql_story': sql_story,
                                'pattern_analysis': pattern_analysis,
                                'reasoning_context': context_output
                            }
                
                # Extract single SQL
                sql_match = re.search(r'<sql>(.*?)</sql>', writer_response, re.DOTALL)
                if sql_match:
                    sql_query = sql_match.group(1).strip().replace('`', '')
                    if sql_query.startswith('sql'):
                        sql_query = sql_query[3:].strip()
                    
                    if not sql_query:
                        raise ValueError("Empty SQL")
                    
                    print(f"✅ Generated SQL: {len(sql_query)} chars")
                    return {
                        'success': True,
                        'multiple_sql': False,
                        'sql_query': sql_query,
                        'used_history_asset': has_history,
                        'history_sql_used': history_sql_used,
                        'sql_story': sql_story,
                        'pattern_analysis': pattern_analysis,
                        'reasoning_context': context_output
                    }
                
                raise ValueError("No SQL in response")
                
            except Exception as e:
                print(f"❌ Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return {
                        'success': False,
                        'error': f"SQL Writer failed: {e}",
                        'used_history_asset': has_history,
                        'history_sql_used': False,
                        'reasoning_context': context_output
                    }
        
        return {
            'success': False,
            'error': "SQL generation failed",
            'used_history_asset': False,
            'history_sql_used': False
        }
    
    async def _generate_sql_with_followup_async(self, context: Dict, sql_followup_question: str, sql_followup_answer: str, state: Dict) -> Dict[str, Any]:
        """Generate SQL using original question + follow-up Q&A with relevance validation in single call"""
        
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        join_clause = state.get('join_clause', '')
        filter_metadata_results = state.get('filter_metadata_results', [])
        
        # 🆕 RETRIEVE history_section from state
        history_section = state.get('sql_history_section', '')        
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
        
        has_multiple_tables = len(selected_datasets) > 1 if isinstance(selected_datasets, list) else False

        followup_sql_prompt = f"""You are a Databricks SQL generator for DANA (Data Analytics & Navigation Assistant).

CONTEXT: This is PHASE 2 of a two-phase process. In Phase 1, you asked a clarifying question. The user has now responded. Your task is to generate Databricks SQL using the original question + user's clarification.

ORIGINAL USER QUESTION: {current_question}
**AVAILABLE METADATA**: {dataset_metadata}
MULTIPLE TABLES AVAILABLE: {has_multiple_tables}
JOIN INFORMATION: {join_clause if join_clause else "No join clause provided"}
MANDATORY FILTER COLUMNS: {mandatory_columns_text}
EXTRACTED column contain FILTER VALUES:
{filter_metadata_results}


{history_section}

====================================
STEP 1: VALIDATE FOLLOW-UP RESPONSE
====================================

YOUR PREVIOUS QUESTION: {sql_followup_question}
USER'S RESPONSE: {sql_followup_answer}

**FIRST, analyze if the user's response is relevant:**

1. **RELEVANT**: User directly answered or provided clarification → PROCEED to SQL generation
2. **NEW_QUESTION**: User asked a completely new question instead of answering → STOP, return new_question flag
3. **TOPIC_DRIFT**: User's response is completely unrelated/off-topic → STOP, return topic_drift flag

**If NOT RELEVANT (categories 2 or 3), immediately return the appropriate XML response below and STOP.**
**If RELEVANT (category 1), proceed to STEP 2 for SQL generation.*

=========================================
STEP 2: SQL GENERATION (Only if RELEVANT)
=========================================

Generate a high-quality Databricks SQL query using:
1. The ORIGINAL user question as the primary requirement
2. The USER'S CLARIFICATION to resolve any ambiguities
3. Available metadata for column mapping
4. Multi-table strategy assessment (single vs multiple queries)
5. Historical SQL Patterns(if available)-Follow structured approach from history_section - inherit if aligned, learn patterns if not.
6. All SQL generation best practices

**MULTI-QUERY DECISION LOGIC**:
- **SINGLE QUERY WITH JOIN**: Simple analysis requiring related data from multiple tables
- **MULTIPLE QUERIES - MULTI-TABLE**: Complementary analysis from different tables OR no join exists
- **MULTIPLE QUERIES - COMPLEX SINGLE-TABLE**: Multiple analytical dimensions (trends + rankings)
- **SINGLE QUERY**: Simple, focused questions with one analytical dimension

SQL GENERATION RULES

PRIORITY 0: MANDATORY REQUIREMENTS (violation = query failure)

M1. MANDATORY FILTERS - Filters marked [MANDATORY] MUST be in WHERE clause, NEVER omit
M2. CASE-INSENSITIVE - Use UPPER() for all string comparisons
M3. SAFE DIVISION - Always use NULLIF(denominator, 0)
M4. NUMERIC FORMATTING - Amounts: ROUND(x, 0), Percentages: ROUND(x, 3)
M5. ONE COLUMN PER FILTER - Apply filter to single column, no OR across multiple columns

PRIORITY 1: METRIC TYPE HANDLING

- Pivot metric_type via CASE WHEN, never GROUP BY metric_type for calculations
- Pattern: SUM(CASE WHEN metric_type = 'X' THEN amt ELSE 0 END) AS x_value
- Calculations happen across columns in same row, not across rows

PRIORITY 2: COMPONENT DISPLAY

- Always show source components alongside calculated metrics
- Pattern: component_1, component_2, component_1 / NULLIF(component_2, 0) AS derived_metric

PRIORITY 3: QUERY PATTERNS

TIME COMPARISON (Always side-by-side columns, NEVER GROUP BY time period):
- Each period as column: SUM(CASE WHEN period = X THEN amt ELSE 0 END) AS period_x
- Include variance: period_2 - period_1 AS variance
- Include variance_pct: (variance / NULLIF(period_1, 0)) * 100
- Applies to: MoM, QoQ, YoY, any "X vs Y" time comparison

TOP N:
- ORDER BY metric DESC LIMIT N
- Include percentage of total when relevant

PERCENTAGE OF TOTAL:
- value, value * 100.0 / NULLIF((SELECT SUM FROM same_filters), 0) AS pct

PATTERN - BREAKDOWN BY MULTIPLE VALUES:
SELECT product_category, ROUND(SUM(amount), 0) AS value
FROM table
WHERE UPPER(product_category) IN (UPPER('HDP'), UPPER('SP'))
GROUP BY product_category

PATTERN - BREAKDOWN BY MULTIPLE VALUES:
SELECT product_category, ROUND(SUM(amount), 0) AS value
FROM table
WHERE UPPER(product_category) IN (UPPER('HDP'), UPPER('SP'))
GROUP BY product_category

APPLY HISTORY PATTERN (if ENHANCE = YES):

IF GROUPING_SETS_TOTAL:
-- Use CTE for calculations, then apply GROUPING() in final SELECT. Learn that exact pattern from history sql.

IF UNION_TOTAL:
-- Detail query
SELECT dimension, breakdown_col, ROUND(SUM(metric), 0) AS metric
FROM table WHERE [filters]
GROUP BY dimension, breakdown_col

UNION ALL

-- Total query  
SELECT dimension, 'OVERALL_TOTAL' AS breakdown_col, ROUND(SUM(metric), 0) AS metric
FROM table WHERE [filters]
GROUP BY dimension

ORDER BY dimension, CASE WHEN breakdown_col = 'OVERALL_TOTAL' THEN 0 ELSE 1 END

IF ENHANCE = NO:
Generate straightforward SQL based on context INTENT:
- simple_aggregate → No GROUP BY on dimensions, just aggregate
- breakdown → GROUP BY dimension columns
- comparison → Side-by-side CASE WHEN for periods/categories
- top_n → ORDER BY metric DESC LIMIT N
- trend → GROUP BY time dimension, ORDER BY time

==============================
OUTPUT FORMATS
==============================
IMPORTANT: You can use proper SQL formatting with line breaks and indentation inside the XML tags
return ONLY the SQL query wrapped in XML tags. No other text, explanations, or formatting

**OPTION 1: If user's response is a NEW QUESTION**
<new_question>
<detected>true</detected>
<reasoning>[Brief 1-sentence why this is a new question]</reasoning>
</new_question>

**OPTION 2: If user's response is TOPIC DRIFT (unrelated)**
<topic_drift>
<detected>true</detected>
<reasoning>[Brief 1-sentence why this is off-topic]</reasoning>
</topic_drift>

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

1. Validate follow-up response -> RELEVANT / NEW_QUESTION / TOPIC_DRIFT
2. If NEW_QUESTION or TOPIC_DRIFT -> Output flag and STOP
3. Apply user's clarification as HIGH CONFIDENCE override
4.Determine history pattern reuse level (if history available)
5.STAGE 4: Generate SQL with mandatory requirements
6.Output reasoning_summary + SQL

CRITICAL REMINDERS:
- User's response resolves the ambiguity - apply it directly
- Every mandatory filter MUST be in WHERE clause
- Use UPPER() for all string comparisons
- Show calculation components (don't just show the result)
- Do NOT ask another follow-up question under any circumstances
"""

        for attempt in range(self.max_retries):
            try:
                print('follow up sql prompt',followup_sql_prompt)
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
