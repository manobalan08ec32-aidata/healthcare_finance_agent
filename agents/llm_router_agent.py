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
            
            # 2. Call search_metadata_sql if filter values exist
            # FILTER OUT common categorical values (case-insensitive), pass only meaningful values
            common_categorical_values = {'pbm','pbm retail', 'hdp', 'home delivery', 'mail', 'specialty','sp','claim fee','claim cost','admin fee','claimfee','claimcost','adminfee','8+4','9+3','2+10','5+7','optum','retail'}
            
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
                print(f"🔍 Searching metadata for filter values: {meaningful_filter_values}")
                metadata_search_task = self.db_client.search_metadata_sql(meaningful_filter_values)
            
            # 3. Execute metadata search if it was initiated
            if metadata_search_task:
                try:
                    filter_metadata_results = await metadata_search_task
                    print(f"📊 Found {len(filter_metadata_results)} filter metadata matches")
                    # Store in state for persistence
                    state['filter_metadata_results'] = filter_metadata_results
                except Exception as e:
                    print(f"⚠️ Filter metadata search failed: {e}")
                    filter_metadata_results = []
                    state['filter_metadata_results'] = []
            
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
        
        # Format filter metadata results for the prompt
        filter_metadata_text = ""
        if filter_metadata_results:
            filter_metadata_text = "\n**FILTER METADATA FOUND:**\n"
            for result in filter_metadata_results:
                filter_metadata_text += f"- {result}\n"
        else:
            filter_metadata_text = "\n**FILTER METADATA:** No specific filter values found in metadata.\n"
        
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
FILTER METADATA: {filter_metadata_text}
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
            check_5_text = "**CHECK 5: Historical SQL availability**: ✓ Available (learning template)"
            history_section = f"""
=== HISTORICAL SQL REFERENCE ===
Previous: "{history_question_match}" | Table: {matched_table_name}

<historical_sql>
{matched_sql}
</historical_sql>

**STEP 1: QUESTION ALIGNMENT CHECK**
Current asks for: [Interpret current question's grouping needs]
Historical had: [Observe historical GROUP BY dimensions]

**INHERIT DECISIONS:**
A. **DIMENSIONS/GROUP BY**: 
   ✅ INHERIT if: Questions ask for same analysis type (e.g., both ask "by product category")
   ❌ DON'T INHERIT if: Different analysis requested (e.g., current asks "by carrier" vs historical "by product")
   
B. **STRUCTURAL PATTERNS** (ALWAYS PRESERVE):
   - Side-by-side comparisons (CASE WHEN for periods)
   - UNION/UNION ALL patterns (detail + OVERALL_TOTAL rows)
   - CTE/subquery architecture
   - Calculation methods (ROUND, NULLIF, percentages)
   - Window functions structure

**STEP 2: SELECTIVE LEARNING**

✅ **ALWAYS LEARN** (Universal Patterns):
- CASE WHEN for side-by-side columns (august_revenue, september_revenue)
- Aggregation placement (SUM/COUNT/AVG)
- Division safety (NULLIF patterns)
- UPPER() for case-insensitive filters
- ROUND(amounts,0), ROUND(percentages,3)

⚡ **CONDITIONALLY INHERIT** (Check Alignment First):
- GROUP BY dimensions → Only if questions ask same granularity
- SELECT columns → Match if same analysis type
- JOIN patterns → Use if same tables needed

❌ **NEVER COPY** (Always From Current):
- Filter values (dates, carrier_id, entities)
- <parameter> placeholders → use actual values
- Specific time periods → use current question's dates

**STEP 3: VALIDATION**
1. Verify columns exist in AVAILABLE METADATA
2. Add MANDATORY FILTER COLUMNS if missing
3. Apply ✓Valid filters from current question

**DECISION LOGIC:**
IF current_question_pattern == historical_pattern:
    → Inherit dimensions + structure
    → Set history_sql_used = true
ELSE:
    → Learn only structural patterns (CASE WHEN, UNION)
    → Build GROUP BY from current question
    → Set history_sql_used = partial

**CRITICAL**: Structural patterns (side-by-side) are proven UX patterns - preserve these even when dimensions differ.

    """
        else:
            history_section = """
    === HISTORICAL SQL ===
    Not available

    """
        
        # 🆕 STORE history_section in state for follow-up SQL generation
        state['sql_history_section'] = history_section
        print(f"💾 Stored sql_history_section in state (length: {len(history_section)} chars)")
        
        assessment_prompt = f"""
⚠️ IMPORTANT CONTEXT - READ THIS FIRST ⚠️
You are a Databricks SQL code generator. Your task is to analyze user questions and generate SQL queries.


CURRENT QUESTION: {current_question}
AVAILABLE METADATA: {dataset_metadata}
MANDATORY FILTER COLUMNS: {mandatory_columns_text}
FILTER VALUES EXTRACTED: {filter_context_text}
JOIN INFORMATION: {join_clause if join_clause else "No joins needed"}

{history_section} 

PHASE 1: PRE-VALIDATION PIPELINE

Execute these checks sequentially. Document each finding.

**CHECK 1: Term Extraction & Mapping**
Extract EVERY business term from the question and map to metadata:

Examples:
- "revenue by carrier for August" → Extract: [revenue, carrier, August]
- "top 5 products with highest margin" → Extract: [products, margin, top 5]
- "MPDOVA claims in Q3" → Extract: [MPDOVA, claims, Q3]

For each term, validate against AVAILABLE METADATA:
- Exact match: "carrier" → carrier_id → ✓Found(carrier_id)
- Fuzzy match: "revenue" → revenue_amount → ✓Found(revenue_amount)  
- No match: "profitability" → (no profit columns) → ✗Not Found
- Ambiguous: "amount" → (revenue_amount, expense_amount) → ⚠Ambiguous(revenue_amount, expense_amount)

Output:
Terms: [revenue, carrier, August]
Mapping: revenue(✓revenue_amount) | carrier(✓carrier_id) | August(✓month)

**CHECK 2: Mandatory Filter Validation** ⚠️ CRITICAL
Mandatory filters MUST be in WHERE clause or query fails.

Check: {mandatory_columns_text}
- If "ledger: MANDATORY" → SQL MUST have "WHERE ledger = ..."
- If "data_source: MANDATORY" → SQL MUST have "WHERE data_source = ..."

Status: ✓Ready (will add to WHERE) | ✗STOP (mandatory column not in metadata)

**CHECK 3: Filter Context Validation**
When user mentions values without attributes (e.g., "MPDOVA", "2024", "Specialty"):

Process:
1. Is value in user's question? → If no, skip this filter
2. Is it in FILTER VALUES EXTRACTED with exact match? → Check
3. Does the mapped column exist in metadata? → Verify

Examples:
- Question has "MPDOVA", Filter shows "carrier_id: MPDOVA", carrier_id in metadata → ✓Use(carrier_id='MPDOVA')

Status: ✓Valid([column=value pairs]) | ✗Invalid | N/A

**CHECK 4: Clarity Assessment**

A. **Temporal Clarity**
- Specific dates (past/present/future): "August 2024", "Q3 2025", "Jan 2026" → ✓Clear
- Relative dates: "last month", "YTD", "next quarter" → ✓Clear
- Vague: "recently", "a while ago", "soon" → ✗Unclear

B. **Metric Clarity**
- Standard aggregations: "total revenue", "average cost" → ✓Clear(SUM/AVG)
- Known calculations: "margin = revenue - cost" → ✓Clear
- Undefined: "efficiency score", "performance index" → ✗Unclear formula

C. **Grouping/Filtering Clarity**
- Explicit: "by product category and month" → ✓Clear
- Vague: "top products" (by what metric?) → ✗Unclear
- Missing: "compare regions" (which regions?) → ✗Unclear

D. **Multiple Value Intent**
When user lists multiple specific values (HDP, SP or drug1, drug2):
- Multiple values mentioned → Need breakdown by each → ✓Show individually
- "total for HDP and SP" → Aggregate together → ✓Clear intent
- "compare HDP vs SP" → Show side-by-side → ✓Clear intent

Examples:
- "revenue for HDP, SP" → SELECT category, SUM(revenue) GROUP BY category
- "total revenue for HDP and SP" → WHERE category IN ('HDP','SP') [no GROUP BY category]
- "HDP vs SP comparison" → Separate columns for each

**CHECK 5: {check_5_text} **

==========================
VALIDATION DECISION GATE
==========================
⚠️ OUTPUT STARTS HERE - DO NOT SHOW PRE-VALIDATION PIPELINE ⚠️

**VALIDATION OUTPUT:**
□ CHECK 1 Mapping: (✓Revenues) | External LOB(✓line_of_business) | HDP(✓product_category)| July 2025(✓month, year)
□ CHECK 2 Mandatory: [✓Ready | ✗STOP]
□ CHECK 3 Filters: [✓Valid | ✗Invalid | N/A]
□ CHECK 4 Clarity: A[✓/✗] B[✓/✗] C[✓/✗]
□ CHECK 5 Rules: [✓history sql used | ✗ history sql not used]

**DECISION LOGIC:**
- ALL checks ✓ or N/A → PROCEED TO SQL GENERATION
- ANY check ✗ or blocking ⚠ → GENERATE FOLLOW-UP
- ONE failure = STOP (Do not attempt SQL with uncertainty)

⚠️ After outputting the decision above, immediately proceed to <sql>, <multiple_sql>, or <followup> tags.
⚠️ Do NOT output any additional explanation or detailed assessment.

<followup>
I need clarification to generate accurate SQL:

**[Specific issue from unclear area]**: [Direct question in one sentence]
- Available data: [specific column names from metadata]
- Suggested approach: [concrete calculation option]

**[Second issue if needed]**: [Second direct question in one sentence only if multiple areas unclear]
- Available data: [relevant columns]
- Alternative: [another option]

Please clarify these points.
</followup>

PHASE 2: SQL GENERATION RULES 

⚠️ ONLY PROCEED HERE IF ALL VALIDATION PASSED

**PRIORITY 0: MANDATORY REQUIREMENTS** (Violation = Query Failure)

M1. **Mandatory Filters** ⚠️⚠️⚠️ CRITICAL
MUST include EVERY mandatory filter in WHERE clause:
```sql
-- Example: If ledger is MANDATORY:
WHERE ledger = 'GAAP'
  AND [other conditions]
```

M2. **Validated Filter Values**
Use ONLY filters marked ✓Valid in CHECK 3:
```sql
WHERE UPPER(carrier_id) = UPPER('MPDOVA')  -- Only if validated
```

M3. **CALCULATED FORMULAS HANDLING (CRITICAL)-Metric Type Grouping Rule for Calculations**
When calculating derived metrics (Gross Margin, Cost %, Margin %), DO NOT group by metric_type:
```sql
-- ✓ CORRECT (for calculations):
SELECT 
    ledger, year, month,  -- Business dimensions only
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) AS revenues,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS cogs,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) - 
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS gross_margin
FROM table
WHERE UPPER(metric_type) IN (UPPER('Revenues'), UPPER('COGS'))
GROUP BY ledger, year, month  -- NOT metric_type

-- ✗ WRONG (breaks calculations):
GROUP BY ledger, metric_type  -- Creates separate rows per metric_type
```
**Only group by metric_type when user explicitly asks to see individual metric types as separate rows.**

**PRIORITY 1: STRUCTURAL PATTERNS**

S1. **Historical SQL Patterns** (if available)
Follow structured approach from history_section - inherit if aligned, learn patterns if not.

S2. **Aggregation Requirements**
Always aggregate metrics unless "line items" explicitly requested.

S3. **Component Display Rule**
ALWAYS show source components for ANY calculation:
```sql
-- For "cost per member by state":
SELECT 
  state_name,
  SUM(total_cost) as total_cost,      -- Component 1
  COUNT(member_id) as member_count,   -- Component 2  
  ROUND(SUM(total_cost) / NULLIF(COUNT(member_id), 0), 2) as cost_per_member  -- Result
FROM table
GROUP BY state_name
```

**PRIORITY 2: QUERY PATTERNS**

P1. **Top N Pattern**
```sql
-- "Top 5 carriers"
SELECT carrier_id, 
       SUM(amount) as amount,
       (SELECT SUM(amount) FROM table) as total,
       ROUND(SUM(amount)/(SELECT SUM(amount) FROM table)*100, 3) as pct
FROM table
WHERE [mandatory filters]
GROUP BY carrier_id
ORDER BY amount DESC LIMIT 5
```

P2. **Time Comparison Pattern**
```sql
-- "Compare Q3 months"
SELECT product,
       SUM(CASE WHEN month = 7 THEN revenue END) as jul_revenue,
       SUM(CASE WHEN month = 8 THEN revenue END) as aug_revenue,
       SUM(CASE WHEN month = 9 THEN revenue END) as sep_revenue
FROM table
WHERE quarter = 3 AND [mandatory filters]
GROUP BY product
```

P3. **Multi-Table Pattern**
```sql
-- When JOIN needed
SELECT t1.dim, SUM(t1.metric) as m1, SUM(t2.metric) as m2
FROM table1 t1
{join_clause}  -- Use provided join
WHERE t1.mandatory_col = value
GROUP BY t1.dim
```

**PRIORITY 3: FORMATTING STANDARDS**

F1. **String Comparison** - Always case-insensitive:
```sql
WHERE UPPER(column) = UPPER('value')
```

F2. **Numeric Formatting**:
- Amounts: ROUND(x, 0) AS name_amount
- Percentages: ROUND(x, 3) AS name_percent
- Division safety: NULLIF(denominator, 0)

PHASE 3: OUTPUT GENERATION & VALIDATION

**PRE-OUTPUT CHECKLIST:** ⚠️ MUST VERIFY
□ All mandatory filters present in WHERE clause?
□ All validated filters applied with UPPER()?
□ Source components shown for calculations?

**OUTPUT FORMAT:**

For SINGLE query:
<sql>
[Complete SQL with all mandatory filters]
</sql>
<sql_story>
[2-3 lines explaining in business-friendly language: what table(s) you're using, what filters are applied, and what metric/calculation you're providing to answer the question. Make it simple for non-technical business users to understand.]
</sql_story>
<history_sql_used>[true if used historical patterns | false]</history_sql_used>

For MULTIPLE queries:
<multiple_sql>
<query1_title>[Title - max 8 words]</query1_title>
<query1>[SQL]</query1>
<query2_title>[Title]</query2_title>  
<query2>[SQL]</query2>
</multiple_sql>
<sql_story>
[2-3 lines explaining in business-friendly language: what tables you're using, what filters are applied, and what metrics/calculations you're providing to answer the question,how the history sql pattern applied. Make it simple for non-technical business users to understand.]
</sql_story>
<history_sql_used>[true | false]</history_sql_used>

**HISTORY_SQL_USED FLAG RULES:**
- If historical SQL was available AND you used its structure/patterns → true
- If historical SQL was available BUT you generated from scratch → false
- If historical SQL was not available → false

**SQL_STORY REQUIREMENTS:**
- Write 2-3 concise lines in plain business English
- Explain: (1) Which table(s) are being queried, (2) What filters/constraints are applied, (3) What metric or calculation is being retrieved (4) How the history SQL pattern applied
- Avoid technical jargon - use business-friendly terms
- Example: "I'm querying the Claims Transaction table to find revenue by therapy class. I've filtered the data to show only PBM claims for August 2025. The results will show total revenue grouped by each therapy class."

FINAL EXECUTION INSTRUCTION

1. Complete ALL Phase 1 validation checks - show your work
2. Make DECISION: PROCEED or FOLLOW-UP
3. If FOLLOW-UP: Generate clarification questions
4. If PROCEED: Apply SQL rules in priority order (M→S→P→F)
5. Validate final databricks SQL contains ALL mandatory requirements
6. Output SQL with history_sql_used flag

⚠️ REMEMBER: You generate SQL CODE only, not business answers.
⚠️ CRITICAL: Every mandatory filter MUST be in WHERE clause.
"""

        for attempt in range(self.max_retries):
            try:
                # print('sql llm prompt', assessment_prompt)
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
        if state.get('requires_dataset_clarification', False):
            followup_reasoning = state.get('followup_reasoning', '')
        else:
            followup_reasoning = state.get('selection_reasoning','')
        
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

        followup_sql_prompt = f"""
⚠️ IMPORTANT CONTEXT - READ THIS FIRST ⚠️

You are a Databricks SQL code generator processing follow-up clarifications. Your task is to generate SQL queries that incorporate both the original question and the user's clarification.

When users request calculations like "revenue per script" or "cost breakdown" - they are asking you to generate SQL code that performs these calculations. You generate the SQL that will be executed by the database.

You are a Healthcare Finance SQL specialist working on PHASE 2 of a two-phase process.

ORIGINAL USER QUESTION: {current_question}
**AVAILABLE METADATA**: {dataset_metadata}
MULTIPLE TABLES AVAILABLE: {has_multiple_tables}
JOIN INFORMATION: {join_clause if join_clause else "No join clause provided"}
MANDATORY FILTER COLUMNS: {mandatory_columns_text}

{history_section}

FILTER VALUES EXTRACTED:
{filter_context_text}

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
**If RELEVANT (category 1), proceed to STEP 2 for SQL generation.**

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

========================================
CRITICAL DATABRICKS SQL GENERATION RULES
=========================================


**PRIORITY 0: MANDATORY REQUIREMENTS** (Violation = Query Failure)

M1. **Mandatory Filters** ⚠️⚠️⚠️ CRITICAL
MUST include EVERY mandatory filter in WHERE clause:
```sql
-- Example: If ledger is MANDATORY:
WHERE ledger = 'GAAP'
  AND [other conditions]
```

M2. **Validated Filter Values**
Use ONLY filters marked ✓Valid in CHECK 3:
```sql
WHERE UPPER(carrier_id) = UPPER('MPDOVA')  -- Only if validated
```

M3. **CALCULATED FORMULAS HANDLING (CRITICAL)-Metric Type Grouping Rule for Calculations**
When calculating derived metrics (Gross Margin, Cost %, Margin %), DO NOT group by metric_type:
```sql
-- ✓ CORRECT (for calculations):
SELECT 
    ledger, year, month,  -- Business dimensions only
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) AS revenues,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS cogs,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount ELSE 0 END) - 
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS') THEN amount ELSE 0 END) AS gross_margin
FROM table
WHERE UPPER(metric_type) IN (UPPER('Revenues'), UPPER('COGS'))
GROUP BY ledger, year, month  -- NOT metric_type

-- ✗ WRONG (breaks calculations):
GROUP BY ledger, metric_type  -- Creates separate rows per metric_type
```
**Only group by metric_type when user explicitly asks to see individual metric types as separate rows.**

**PRIORITY 1: STRUCTURAL PATTERNS**

S1. **Historical SQL Patterns** (if available)
Follow structured approach from history_section - inherit if aligned, learn patterns if not.

S2. **Aggregation Requirements**
Always aggregate metrics unless "line items" explicitly requested.

S3. **Component Display Rule**
ALWAYS show source components for ANY calculation:
```sql
-- For "cost per member by state":
SELECT 
  state_name,
  SUM(total_cost) as total_cost,      -- Component 1
  COUNT(member_id) as member_count,   -- Component 2  
  ROUND(SUM(total_cost) / NULLIF(COUNT(member_id), 0), 2) as cost_per_member  -- Result
FROM table
GROUP BY state_name
```

**PRIORITY 2: QUERY PATTERNS**

P1. **Top N Pattern**
```sql
-- "Top 5 carriers"
SELECT carrier_id, 
       SUM(amount) as amount,
       (SELECT SUM(amount) FROM table) as total,
       ROUND(SUM(amount)/(SELECT SUM(amount) FROM table)*100, 3) as pct
FROM table
WHERE [mandatory filters]
GROUP BY carrier_id
ORDER BY amount DESC LIMIT 5
```

P2. **Time Comparison Pattern**
```sql
-- "Compare Q3 months"
SELECT product,
       SUM(CASE WHEN month = 7 THEN revenue END) as jul_revenue,
       SUM(CASE WHEN month = 8 THEN revenue END) as aug_revenue,
       SUM(CASE WHEN month = 9 THEN revenue END) as sep_revenue
FROM table
WHERE quarter = 3 AND [mandatory filters]
GROUP BY product
```

P3. **Multi-Table Pattern**
```sql
-- When JOIN needed
SELECT t1.dim, SUM(t1.metric) as m1, SUM(t2.metric) as m2
FROM table1 t1
{join_clause}  -- Use provided join
WHERE t1.mandatory_col = value
GROUP BY t1.dim
```

**PRIORITY 3: FORMATTING STANDARDS**

F1. **String Comparison** - Always case-insensitive:
```sql
WHERE UPPER(column) = UPPER('value')
```

F2. **Numeric Formatting**:
- Amounts: ROUND(x, 0) AS name_amount
- Percentages: ROUND(x, 3) AS name_percent
- Division safety: NULLIF(denominator, 0)

PHASE 3: OUTPUT GENERATION & VALIDATION

**PRE-OUTPUT CHECKLIST:** ⚠️ MUST VERIFY
□ All mandatory filters present in WHERE clause?
□ All validated filters applied with UPPER()?
□ Source components shown for calculations?

==============================
INTEGRATION INSTRUCTIONS
==============================

- Integrate the user's clarification naturally into the SQL logic
- If clarification provided specific formulas, implement them precisely
- If clarification resolved time periods, use exact dates/ranges specified  
- If clarification defined metrics, use the exact business definitions provided
- Maintain all original SQL quality standards while incorporating clarifications

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

**OPTION 3: If RELEVANT - Single SQL Query**
<sql>
[Your complete SQL query incorporating both original question and clarifications]
</sql>
<sql_story>
[2-3 lines explaining in business-friendly language: what table(s) you're using, what filters are applied, and what metric/calculation you're providing to answer the question. Make it simple for non-technical business users to understand.]
</sql_story>
<history_sql_used>[true/false - Did you use the historical SQL pattern/structure? true if you inherited dimensions, JOINs, or structural patterns from history_section; false if you generated SQL from scratch]</history_sql_used>

**OPTION 4: If RELEVANT - Multiple SQL Queries**
<multiple_sql>
<query1_title>[Brief descriptive title - max 8 words]</query1_title>
<query1>[First SQL query]</query1>
<query2_title>[Brief descriptive title - max 8 words]</query2_title>
<query2>[Second SQL query]</query2>
</multiple_sql>
<sql_story>
[2-3 lines explaining in business-friendly language: what tables you're using, what filters are applied, and what metrics/calculations you're providing to answer the question. Make it simple for non-technical business users to understand.]
</sql_story>
<history_sql_used>[true/false - Did you use the historical SQL pattern/structure? true if you inherited dimensions, JOINs, or structural patterns from history_section; false if you generated SQL from scratch]</history_sql_used>

**SQL_STORY REQUIREMENTS:**
- Write 2-3 concise lines in plain business English
- Explain: (1) Which table(s) are being queried, (2) What filters/constraints are applied, (3) What metric or calculation is being retrieved
- Avoid technical jargon - use business-friendly terms
- Example: "I'm querying the Claims Transaction table to find revenue by therapy class. I've filtered the data to show only PBM claims for August 2025. The results will show total revenue grouped by each therapy class."

Reminder: Must preserve the mandatory filter value in the SQL generation

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
