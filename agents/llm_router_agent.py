from typing import Dict, List, Optional, Any, Tuple
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
from prompts.router_prompts import (
    ROUTER_SYSTEM_PROMPT,
    DATASET_SELECTION_PROMPT,
    DATASET_CLARIFICATION_PROMPT,
    SQL_PLANNER_SYSTEM_PROMPT,
    SQL_PLANNER_PROMPT,
    SQL_WRITER_SYSTEM_PROMPT,
    SQL_WRITER_PROMPT,
    SQL_DATASET_CHANGE_PROMPT,
    SQL_HISTORY_SECTION_TEMPLATE,
    SQL_NO_HISTORY_SECTION,
    SQL_FOLLOWUP_SYSTEM_PROMPT,
    SQL_FOLLOWUP_PROMPT,
    SQL_FOLLOWUP_VALIDATION_SYSTEM_PROMPT,
    SQL_FOLLOWUP_VALIDATION_PROMPT,
    SQL_FOLLOWUP_SIMPLE_APPROVAL_PROMPT,
    SQL_FIX_PROMPT,
)

# Initialize logger for this module
logger = setup_logger(__name__)

class LLMRouterAgent:
    """Enhanced router agent with dataset selection and SQL generation"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        self.max_retries = 3

    def _load_available_datasets(self, domain_selection: str) -> List[Dict[str, str]]:
        """Load available datasets from metadata.json for the given domain"""
        try:
            metadata_config_path = "config/metadata/metadata.json"
            with open(metadata_config_path, 'r') as f:
                metadata_config = json.load(f)

            domain_datasets = metadata_config.get(domain_selection, [])

            # Convert to list of dicts with functional_name and table_name
            available_datasets = []
            for dataset_dict in domain_datasets:
                for functional_name, table_name in dataset_dict.items():
                    available_datasets.append({
                        "functional_name": functional_name,
                        "table_name": table_name
                    })

            print(f"✅ Loaded {len(available_datasets)} available datasets for {domain_selection}")
            return available_datasets

        except Exception as e:
            print(f"❌ Failed to load available datasets: {e}")
            return []

    def _log(self, level: str, message: str, state: AgentState = None, **extra):
        """Helper method to log with session context from state"""
        user_email = state.get('user_email') if state else None
        session_id = state.get('session_id') if state else None
        log_with_user_context(logger, level, f"[Router] {message}", user_email, session_id, **extra)
        
        
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

        # Load available datasets for the domain (simple approach - no caching needed)
        domain_selection = state.get('domain_selection', '')
        
        if domain_selection:
            available_datasets = self._load_available_datasets(domain_selection)
            state['available_datasets'] = available_datasets
            print(f"📊 Loaded {len(available_datasets)} available datasets for {domain_selection}")

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
                state['functional_names'] = selection_result.get('functional_names', '')
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

            # Get domain selection from state
            domain_selection = state.get('domain_selection', 'Optum Pharmacy')
            print(f'🎯 Domain selection: {domain_selection}')

            # ===== STEP 1: Load Mandatory Embeddings (Full Metadata) =====
            mandatory_contexts = get_mandatory_embeddings_for_tables(tables_list, domain_selection)
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

        # Use imported prompt template
        selection_prompt = DATASET_SELECTION_PROMPT.format(
            user_question=user_question,
            filter_metadata_results=filter_metadata_results,
            search_results=search_results
        )

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
                    system_prompt=ROUTER_SYSTEM_PROMPT
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

        # Use imported prompt template
        combined_prompt = DATASET_CLARIFICATION_PROMPT.format(
            original_question=original_question,
            followup_question=followup_question,
            user_clarification=user_clarification,
            candidate_actual_tables=candidate_actual_tables,
            functional_names=functional_names,
            filter_metadata_text=filter_metadata_text
        )
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                print('dataset follow up prompt',combined_prompt)
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
        Uses SQL_PLANNER_PROMPT template from prompts package.
        """

        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        filter_metadata_results = state.get('filter_metadata_results', [])
        mandatory_columns_text = state.get('mandatory_columns_text', '')
        history_question_match = state.get('history_question_match', '')
        matched_sql = state.get('matched_sql', '')

        # Build dataset context functional_names
        selected_functional_names = state.get('functional_names', [])
        available_datasets = state.get('available_datasets', [])
        print(f"Selected functional names: {selected_functional_names}")
        print(f"Selected available_datasets : {available_datasets}")
        # Get other available datasets (excluding selected ones)
        other_datasets = [
            d['functional_name'] for d in available_datasets
            if d['functional_name'] not in selected_functional_names
        ]
        print("other_datasets",other_datasets)
        dataset_context = f"""
SELECTED DATASET: {', '.join(selected_functional_names) if selected_functional_names else 'None'}

OTHER AVAILABLE DATASETS: {', '.join(other_datasets) if other_datasets else 'None'}
"""

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

        # Build user modification section if present (for re-planning after user modifies plan)
        user_modification = state.get('user_modification', '')
        user_modification_section = ""
        if user_modification:
            user_modification_section = f"""
USER MODIFICATION FROM PREVIOUS PLAN:
The user reviewed a previous plan and requested changes:
"{user_modification}"

IMPORTANT: Incorporate this modification into your analysis. The user wants to change
filters, time ranges, or other aspects of the query. The EXTRACTED FILTER VALUES
above have been updated with column matches for any new filter values mentioned.

⚠️ MANDATORY: Since this is a user modification of a previous plan, you MUST output <plan_approval> block
to confirm the updated plan with the user. Use EXECUTION_PATH: SHOW_PLAN. Do NOT skip to SQL_READY.
"""

        # Use imported prompt template
        prompt = SQL_PLANNER_PROMPT.format(
            current_question=current_question,
            filter_metadata_results=filter_metadata_results,
            history_hint=history_hint,
            dataset_metadata=dataset_metadata,
            mandatory_columns_text=mandatory_columns_text,
            dataset_context=dataset_context,
            other_available_datasets=', '.join(other_datasets) if other_datasets else 'None',
            user_modification_section=user_modification_section
        )

        return prompt


    def _build_sql_writer_prompt(self, context_output: str, state: Dict, current_question: str) -> str:
        """Build the SQL writer prompt (Call 2)

        Takes raw context from planner and generates SQL with pattern learning.
        Uses SQL_WRITER_PROMPT template from prompts package.
        """

        history_question_match = state.get('history_question_match', '')
        matched_sql = state.get('matched_sql', '')
        has_history = bool(matched_sql and history_question_match)

        # Build history section using imported templates
        if has_history:
            history_section = SQL_HISTORY_SECTION_TEMPLATE.format(
                history_question_match=history_question_match,
                matched_sql=matched_sql
            )
        else:
            history_section = SQL_NO_HISTORY_SECTION

        # Use imported prompt template
        prompt = SQL_WRITER_PROMPT.format(
            current_question=current_question,
            context_output=context_output,
            history_section=history_section
        )
        return prompt


    def _extract_context_and_followup(self, response: str, state: Dict) -> tuple[str, bool, str]:
        """Extract context block and check for followup or plan_approval

        Returns:
            tuple: (context_content, needs_followup, followup_or_plan_text)
        """

        # Extract context block
        context_match = re.search(r'<context>(.*?)</context>', response, re.DOTALL)
        if not context_match:
            raise ValueError("No <context> block found in response")

        context_content = context_match.group(1).strip()

        # Check for followup - either in tag or in DECISION
        followup_match = re.search(r'<followup>(.*?)</followup>', response, re.DOTALL)

        # Check for plan_approval block
        plan_approval_match = re.search(r'<plan_approval>(.*?)</plan_approval>', response, re.DOTALL)

        # Determine if needs followup (followup OR plan_approval)
        needs_followup = bool(followup_match) or bool(plan_approval_match) or 'DECISION: FOLLOWUP_REQUIRED' in context_content

        # Set plan approval flag in state
        state['plan_approval_exists_flg'] = bool(plan_approval_match)

        # Get the text to display (followup takes priority if both exist)
        if followup_match:
            followup_text = followup_match.group(1).strip()
        elif plan_approval_match:
            followup_text = plan_approval_match.group(1).strip()
        else:
            followup_text = ""

        return context_content, needs_followup, followup_text

    async def _search_and_store_feedback_history(self, current_question: str, table_names: list, state: Dict) -> bool:
        """Search feedback SQL and store history in state

        Args:
            current_question: The user's question to search for
            table_names: List of table names to search against
            state: State dict to update with results

        Returns:
            bool: True if history match found, False otherwise
        """
        print(f"🔍 Searching feedback SQL for: {table_names}")
        feedback_results = await self.db_client.sp_vector_search_feedback_sql(
            current_question, table_names=table_names
        )

        # Initialize with empty strings (no match case)
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
                        print(f"✅ History match: {matched_table_name}")
                        break

        # Store in state (empty strings if no match)
        state['history_question_match'] = history_question_match
        state['matched_sql'] = matched_sql
        state['matched_table_name'] = matched_table_name

        has_history = bool(matched_sql and history_question_match)

        # Build and store history section
        if has_history:
            history_section = SQL_HISTORY_SECTION_TEMPLATE.format(
                history_question_match=history_question_match,
                matched_sql=matched_sql
            )
        else:
            history_section = SQL_NO_HISTORY_SECTION
        state['sql_history_section'] = history_section

        return has_history

    async def _load_metadata_for_dataset_change(
        self,
        matched_tables: list,
        matched_functional_names: list,
        state: Dict
    ) -> Dict[str, Any]:
        """Load metadata for dataset change and refresh feedback history

        Args:
            matched_tables: List of new table names
            matched_functional_names: List of new functional names
            state: State dict to update

        Returns:
            Dict with 'success' bool and 'error_message' if failed
        """
        print(f"🔄 Dataset change requested: {', '.join(matched_functional_names)}")

        # Load new metadata
        new_metadata_result = await self.get_metadata(
            state=state,
            selected_dataset=matched_tables
        )

        if new_metadata_result.get('error', False):
            return {
                'success': False,
                'error_message': new_metadata_result.get('message', 'Failed to load metadata')
            }

        # Update state with new dataset(s) and metadata
        state['selected_dataset'] = matched_tables
        state['functional_names'] = matched_functional_names
        state['dataset_metadata'] = new_metadata_result.get('metadata', '')

        print(f"✅ Switched to dataset(s): {', '.join(matched_functional_names)}")
        print(f"✅ Loaded new metadata for {len(matched_tables)} table(s)")

        # Refresh feedback history for new tables
        current_question = state.get('current_question', '')
        await self._search_and_store_feedback_history(
            current_question=current_question,
            table_names=matched_tables,
            state=state
        )
        print(f"🔄 Refreshed feedback history for new dataset(s)")

        return {'success': True}

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
        has_history = await self._search_and_store_feedback_history(
            current_question=current_question,
            table_names=selected_datasets,
            state=state
        )

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
        print("planner Prompt:", planner_prompt[:5000])
        
        context_output = None
        
        for attempt in range(self.max_retries):
            try:
                planner_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": planner_prompt}],
                    max_tokens=5000,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt="You are a SQL query planning assistant for an internal enterprise business intelligence system. Your role is to validate and map business questions to database schemas for authorized analytics reporting. Output ONLY <context> block, optionally followed by <followup> or <plan_approval>. No other text."
                )
                
                print("SQL Planner Response:", planner_response)      

                self._log('info', "LLM response received from SQL planner prompt", state,
                         llm_response=planner_response,
                         attempt=attempt + 1)          
                # Extract context and check followup (state is updated with plan_approval_exists_flg inside function)
                context_output, needs_followup, followup_text = self._extract_context_and_followup(planner_response, state)

                # Handle force_show_plan: LLM should have produced <plan_approval> based on prompt instruction
                force_show_plan = state.get('force_show_plan', False)
                if force_show_plan:
                    # Clear force_show_plan after this iteration
                    state['force_show_plan'] = False
                    if not needs_followup:
                        # LLM didn't produce plan_approval despite instruction - log warning but proceed
                        print("⚠️ force_show_plan=True but LLM didn't produce <plan_approval>. Proceeding with SQL generation.")

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
                print("writer prompt",writer_prompt[:7000])
                writer_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": writer_prompt}],
                    max_tokens=2500,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt="You are a Databricks SQL code generator for an internal enterprise business intelligence system. Your role is to generate production-ready SQL queries for authorized analytics reporting based on validated query plans. Output in the specified XML format."
                )
                
                print(f"SQL writer Response: {writer_response}")

                self._log('info', "LLM response received from SQL writer prompt", state,
                         llm_response=writer_response,
                         attempt=attempt + 1)
                
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

    # ============ TWO-STEP FOLLOWUP HANDLING METHODS ============

    async def _llm_extract_filter_values(self, user_modification: str, state: Dict) -> Dict[str, List[str]]:
        """Use LLM to extract ALL elements from user modification, categorized

        Extracts and categorizes elements into:
        - filter_values: Actual data values (e.g., "CIGNA", "MPDOVA") - searchable via search_column_values()
        - other_elements: Column names, metrics, time periods - validated by planner against metadata

        Args:
            user_modification: User's modification text
            state: Current state dict

        Returns:
            Dict with 'filter_values' and 'other_elements' lists
        """
        extraction_prompt = f"""Extract and categorize ALL elements from the user's plan modification.

USER MODIFICATION: {user_modification}

TASK: Identify elements and categorize them:

1. FILTER_VALUES - Actual data values that exist in database columns:
   - Carrier/client names: "CIGNA", "MPDOVA", "Acme Corp"
   - Specific codes or IDs: "ABC123", "RX001"
   - NOT numeric values, NOT column names

2. OTHER_ELEMENTS - Everything else the user mentions:
   - Column names: "carrier_region", "client_name"
   - Metrics: "rebate margin", "gross profit", "cost per script"
   - Time periods: "Q1 2025", "FY 2024", "January 2025"

EXCLUDE generic terms: pbm, hdp, mail, specialty, retail, optum, yes, no, ok, looks good

OUTPUT: Return ONLY a JSON object with two arrays:
{{
  "filter_values": ["CIGNA", "MPDOVA"],
  "other_elements": ["carrier_region", "Q1 2025"]
}}
If nothing found, return: {{"filter_values": [], "other_elements": []}}
"""

        try:
            llm_response = await self.db_client.call_claude_api_endpoint_async(
                messages=[{"role": "user", "content": extraction_prompt}],
                max_tokens=300,
                temperature=0.0,
                top_p=0.1,
                system_prompt="You are a data element extractor. Output only valid JSON objects."
            )

            # Parse JSON response
            result = json.loads(llm_response.strip())
            filter_values = result.get('filter_values', [])
            other_elements = result.get('other_elements', [])

            print(f"🔍 LLM extracted filter_values: {filter_values}")
            print(f"🔍 LLM extracted other_elements: {other_elements}")

            return {
                'filter_values': filter_values if isinstance(filter_values, list) else [],
                'other_elements': other_elements if isinstance(other_elements, list) else []
            }

        except Exception as e:
            print(f"⚠️ Element extraction failed: {e}")
            return {'filter_values': [], 'other_elements': []}

    async def _extract_and_search_new_filters(self, user_modification: str, state: Dict) -> Tuple[str, List[str]]:
        """Extract elements using LLM and search for filter values in columns

        Only filter_values are searched via search_column_values().
        Other elements (column names, metrics, time periods) are passed to second planner
        for validation against metadata.

        Args:
            user_modification: User's modification text
            state: Current state dict

        Returns:
            Tuple of (additional_filter_metadata, unresolved_filter_values)
            - additional_filter_metadata: New metadata to APPEND to existing filter_metadata_results
            - unresolved_filter_values: filter values that couldn't be found in any column
        """
        # Use LLM to extract and categorize elements
        extracted = await self._llm_extract_filter_values(user_modification, state)
        filter_values = extracted.get('filter_values', [])
        other_elements = extracted.get('other_elements', [])

        additional_metadata = ""
        unresolved_filter_values = []

        # Search for filter_values only (actual data values)
        if filter_values:
            print(f"🔍 Searching columns for filter values: {filter_values}")

            new_column_matches = await self.db_client.search_column_values(
                filter_values,
                max_columns=7,
                max_values_per_column=5
            )

            # Track which filter values were found
            found_values = set()
            if new_column_matches:
                for match in new_column_matches:
                    # Parse match to extract the value
                    for fv in filter_values:
                        if fv.lower() in str(match).lower():
                            found_values.add(fv.lower())

            # Identify unresolved filter values
            unresolved_filter_values = [fv for fv in filter_values if fv.lower() not in found_values]

            if new_column_matches:
                additional_metadata += "\n**Columns found for modified filter values:**\n"
                for result in new_column_matches:
                    additional_metadata += f"- {result}\n"
                print(f"📊 Found {len(new_column_matches)} column matches for filter values")

            if unresolved_filter_values:
                print(f"⚠️ Unresolved filter values: {unresolved_filter_values}")
        else:
            print("📊 No filter values extracted from modification")

        # Add other_elements info for second planner to validate against metadata
        if other_elements:
            additional_metadata += f"\n**Other elements from user modification (validate against metadata):**\n"
            for elem in other_elements:
                additional_metadata += f"- {elem}\n"
            print(f"📋 Other elements for planner validation: {other_elements}")

        return additional_metadata, unresolved_filter_values

    async def _validate_followup_response_async(
        self,
        sql_followup_question: str,
        sql_followup_answer: str,
        state: Dict
    ) -> Dict[str, Any]:
        """
        Step 1: Classify user's follow-up response with lightweight LLM call (~300 tokens).

        This is the first step of the two-step follow-up handling flow for token efficiency.
        It classifies the user's response without performing SQL generation.

        Returns:
            Dict with keys:
            - classification: SIMPLE_APPROVAL | APPROVAL_WITH_MODIFICATIONS |
                             DATASET_CHANGE_REQUEST | NEW_QUESTION | TOPIC_DRIFT
            - modifications: str (if APPROVAL_WITH_MODIFICATIONS)
            - requested_datasets: List[str] (if DATASET_CHANGE_REQUEST)
            - reasoning: str
        """
        current_question = state.get('rewritten_question', state.get('current_question', ''))
        selected_functional_names = state.get('functional_names', [])
        available_datasets = state.get('available_datasets', [])

        # Get other available datasets (excluding selected ones)
        other_datasets = [
            d['functional_name'] for d in available_datasets
            if d['functional_name'] not in selected_functional_names
        ]

        selected_dataset_name = ', '.join(selected_functional_names) if selected_functional_names else 'None'
        other_available_datasets = ', '.join(other_datasets) if other_datasets else 'None'

        # Clean the follow-up answer
        cleaned_sql_followup_answer = re.sub(r'(?i)\bfollow[- ]?up\b', '', sql_followup_answer).strip()

        # Build lightweight validation prompt
        validation_prompt = SQL_FOLLOWUP_VALIDATION_PROMPT.format(
            current_question=current_question,
            selected_dataset_name=selected_dataset_name,
            other_available_datasets=other_available_datasets,
            sql_followup_question=sql_followup_question,
            sql_followup_answer=cleaned_sql_followup_answer
        )

        for attempt in range(self.max_retries):
            try:
                print(f"🔍 Step 1: Validating follow-up response (attempt {attempt + 1})...")
                print(f"📝 Validation prompt: {validation_prompt[:7000]}...")

                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": validation_prompt}],
                    max_tokens=200,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt=SQL_FOLLOWUP_VALIDATION_SYSTEM_PROMPT
                )

                print(f"📊 Validation response: {llm_response}")

                self._log('info', "LLM response received from follow-up validation", state,
                         llm_response=llm_response,
                         attempt=attempt + 1)

                # Parse XML response
                classification_match = re.search(r'<classification>(.*?)</classification>', llm_response, re.DOTALL)
                modifications_match = re.search(r'<modifications>(.*?)</modifications>', llm_response, re.DOTALL)
                datasets_match = re.search(r'<requested_datasets>(.*?)</requested_datasets>', llm_response, re.DOTALL)
                reasoning_match = re.search(r'<reasoning>(.*?)</reasoning>', llm_response, re.DOTALL)

                if not classification_match:
                    raise ValueError("No <classification> tag found in validation response")

                classification = classification_match.group(1).strip().upper()
                modifications = modifications_match.group(1).strip() if modifications_match else 'none'
                requested_datasets_str = datasets_match.group(1).strip() if datasets_match else 'none'
                reasoning = reasoning_match.group(1).strip() if reasoning_match else ''

                # Parse requested datasets into list
                requested_datasets = []
                if requested_datasets_str.lower() != 'none' and requested_datasets_str:
                    requested_datasets = [d.strip() for d in requested_datasets_str.split(',') if d.strip()]

                print(f"✅ Validation complete: {classification}")

                return {
                    'classification': classification,
                    'modifications': modifications if modifications.lower() != 'none' else '',
                    'requested_datasets': requested_datasets,
                    'reasoning': reasoning
                }

            except Exception as e:
                print(f"❌ Validation attempt {attempt + 1} failed: {str(e)}")

                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying validation... ({attempt + 1}/{self.max_retries})")
                    await asyncio.sleep(2 ** attempt)

        # Fallback: if validation fails, default to APPROVAL_WITH_MODIFICATIONS to trigger full SQL generation
        print("⚠️ Validation failed, defaulting to APPROVAL_WITH_MODIFICATIONS for full processing")
        return {
            'classification': 'APPROVAL_WITH_MODIFICATIONS',
            'modifications': sql_followup_answer,
            'requested_datasets': [],
            'reasoning': 'Validation failed, defaulting to full processing'
        }

    async def _generate_sql_simple_approval_async(
        self,
        context: Dict,
        sql_followup_question: str,
        sql_followup_answer: str,
        state: Dict
    ) -> Dict[str, Any]:
        """
        Step 2a: Generate SQL for simple approval case with lightweight prompt (~800 tokens).

        This method is called when the user simply approved the clarification without
        adding new requirements. Uses a minimal prompt without full SQL generation rules.

        Returns:
            Dict with SQL generation result (same structure as full method)
        """
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        filter_metadata_results = state.get('filter_metadata_results', [])

        # Get history section from state
        history_section = state.get('sql_history_section', SQL_NO_HISTORY_SECTION)

        # Build mandatory columns text
        selected_datasets = state.get('selected_dataset', [])
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

        mandatory_columns_text = "\n".join(mandatory_columns_info) if mandatory_columns_info else "Not Applicable"

        # Clean the follow-up answer
        cleaned_sql_followup_answer = re.sub(r'(?i)\bfollow[- ]?up\b', '', sql_followup_answer).strip()

        # Build lightweight SQL generation prompt
        simple_approval_prompt = SQL_FOLLOWUP_SIMPLE_APPROVAL_PROMPT.format(
            current_question=current_question,
            sql_followup_question=sql_followup_question,
            sql_followup_answer=cleaned_sql_followup_answer,
            dataset_metadata=dataset_metadata,
            mandatory_columns_text=mandatory_columns_text,
            filter_metadata_results=filter_metadata_results,
            history_section=history_section,
            reasoning_context=state.get('reasoning_context', '')
        )

        for attempt in range(self.max_retries):
            try:
                print(f"🔨 Step 2a: Generating SQL for simple approval (attempt {attempt + 1})...")
                print(f"📝 Simple approval prompt: {simple_approval_prompt[:5000]}...")

                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": simple_approval_prompt}],
                    max_tokens=2500,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt=SQL_FOLLOWUP_SYSTEM_PROMPT
                )

                print(f"📊 Simple approval SQL response: {llm_response}")

                self._log('info', "LLM response received from simple approval SQL generation", state,
                         llm_response=llm_response,
                         attempt=attempt + 1)

                # Extract sql_story
                sql_story = ""
                story_match = re.search(r'<sql_story>(.*?)</sql_story>', llm_response, re.DOTALL)
                if story_match:
                    sql_story = story_match.group(1).strip()
                    print(f"📖 Captured SQL story from simple approval ({len(sql_story)} chars)")

                # Extract history_sql_used flag
                history_sql_used = False
                history_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', llm_response, re.IGNORECASE)
                if history_match:
                    history_sql_used = history_match.group(1).strip().lower() == 'true'
                    print(f"📊 history_sql_used flag: {history_sql_used}")

                # Check for multiple SQL queries
                multiple_sql_match = re.search(r'<multiple_sql>(.*?)</multiple_sql>', llm_response, re.DOTALL)
                if multiple_sql_match:
                    multiple_content = multiple_sql_match.group(1).strip()

                    # Extract individual queries with titles
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
                            if cleaned_query and cleaned_title:
                                sql_queries.append(cleaned_query)
                                query_titles.append(cleaned_title)

                        if sql_queries:
                            print(f"✅ Generated {len(sql_queries)} queries via simple approval")
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'topic_drift': False,
                                'new_question': False,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,
                                'query_count': len(sql_queries),
                                'history_sql_used': history_sql_used,
                                'sql_story': sql_story
                            }

                    raise ValueError("Empty or invalid multiple SQL queries")

                # Check for single SQL query
                sql_match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if sql_match:
                    sql_query = sql_match.group(1).strip().replace('`', '')

                    if not sql_query:
                        raise ValueError("Empty SQL query in response")

                    print(f"✅ Generated single SQL via simple approval ({len(sql_query)} chars)")
                    return {
                        'success': True,
                        'multiple_sql': False,
                        'topic_drift': False,
                        'new_question': False,
                        'sql_query': sql_query,
                        'history_sql_used': history_sql_used,
                        'sql_story': sql_story
                    }

                raise ValueError("No SQL found in response")

            except Exception as e:
                print(f"❌ Simple approval SQL attempt {attempt + 1} failed: {str(e)}")

                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying... ({attempt + 1}/{self.max_retries})")
                    await asyncio.sleep(2 ** attempt)

        return {
            'success': False,
            'topic_drift': False,
            'new_question': False,
            'history_sql_used': False,
            'error': f"Simple approval SQL generation failed after {self.max_retries} attempts"
        }

    async def _handle_dataset_change_from_followup(
        self,
        requested_dataset_names: list,
        context: Dict,
        state: Dict
    ) -> Dict[str, Any]:
        """
        Handle dataset change request detected during follow-up validation.

        Maps functional names to table names, loads new metadata, and generates SQL.

        Args:
            requested_dataset_names: List of functional dataset names requested by user
            context: Current execution context
            state: Current state dict

        Returns:
            SQL generation result dict
        """
        available_datasets = state.get('available_datasets', [])
        matched_tables = []
        matched_functional_names = []

        for req_name in requested_dataset_names:
            for dataset in available_datasets:
                if dataset['functional_name'].strip().lower() == req_name.strip().lower():
                    matched_tables.append(dataset['table_name'])
                    matched_functional_names.append(dataset['functional_name'])
                    break

        if not matched_tables:
            print(f"⚠️ No matching tables found for requested datasets: {requested_dataset_names}")
            return {
                'success': False,
                'topic_drift': True,
                'new_question': False,
                'message': f"Requested dataset(s) not found: {requested_dataset_names}"
            }

        # Load metadata for new dataset(s)
        print(f"🔄 Dataset change: {matched_functional_names}")
        load_result = await self._load_metadata_for_dataset_change(
            matched_tables=matched_tables,
            matched_functional_names=matched_functional_names,
            state=state
        )

        if not load_result.get('success'):
            return {
                'success': False,
                'topic_drift': False,
                'new_question': False,
                'error': load_result.get('error_message', 'Failed to load new dataset metadata')
            }

        # Generate SQL with new dataset by REUSING the full planner -> writer pipeline
        # This ensures proper validation against new schema without code duplication
        print(f"🔄 Running SQL Planner + Writer pipeline for new dataset: {matched_functional_names}")
        sql_context = self._extract_context(state)

        # Reset plan iteration for fresh start with new dataset
        # This ensures user gets full two-planner flow with new schema
        state['plan_iteration'] = 0
        state['user_modification'] = None
        state['force_show_plan'] = False

        # REUSE the main two-stage pipeline (_assess_and_generate_sql_async) which:
        # 1. SQL Planner (Call 1) - validates question against new schema
        # 2. SQL Writer (Call 2) - generates SQL if planner approves
        return await self._assess_and_generate_sql_async(sql_context, state)

    async def _generate_sql_with_followup_async(self, context: Dict, sql_followup_question: str, sql_followup_answer: str, state: Dict) -> Dict[str, Any]:
        """
        Orchestrate two-step follow-up handling for token efficiency.

        Step 1: Validate response (lightweight ~300 tokens)
        Step 2: Generate SQL (conditional, based on classification)

        This replaces the previous monolithic approach that always used ~3000 tokens.

        Flow:
        - SIMPLE_APPROVAL → Lightweight SQL generation (~800 tokens)
        - APPROVAL_WITH_MODIFICATIONS → Full SQL generation (~2500 tokens)
        - DATASET_CHANGE_REQUEST → Dataset change flow (existing)
        - NEW_QUESTION → Return immediately, no SQL call
        - TOPIC_DRIFT → Return immediately, no SQL call
        """

        print("=" * 60)
        print("🔄 TWO-STEP FOLLOW-UP HANDLING")
        print("=" * 60)

        # ========================================
        # STEP 1: VALIDATE FOLLOW-UP RESPONSE
        # ========================================
        print("\n📋 STEP 1: Validating follow-up response...")

        validation_result = await self._validate_followup_response_async(
            sql_followup_question=sql_followup_question,
            sql_followup_answer=sql_followup_answer,
            state=state
        )

        classification = validation_result.get('classification', 'APPROVAL_WITH_MODIFICATIONS')
        modifications = validation_result.get('modifications', '')
        requested_datasets = validation_result.get('requested_datasets', [])
        reasoning = validation_result.get('reasoning', '')

        print(f"\n✅ Validation Result:")
        print(f"   Classification: {classification}")
        print(f"   Modifications: {modifications}")
        print(f"   Requested Datasets: {requested_datasets}")
        print(f"   Reasoning: {reasoning}")

        # Store validation result in state for debugging/logging
        state['followup_validation_result'] = validation_result
        state['followup_classification'] = classification

        # ========================================
        # HANDLE NON-SQL CASES (no second LLM call)
        # ========================================

        if classification == 'TOPIC_DRIFT':
            print("\n🚫 TOPIC_DRIFT detected - returning immediately (no SQL generation)")
            return {
                'success': False,
                'topic_drift': True,
                'new_question': False,
                'message': f"Your response seems unrelated to the clarification requested. {reasoning}",
                'original_followup_question': sql_followup_question
            }

        if classification == 'NEW_QUESTION':
            print("\n🔄 NEW_QUESTION detected - returning immediately (no SQL generation)")
            return {
                'success': False,
                'topic_drift': False,
                'new_question': True,
                'message': f"You've asked a new question instead of providing clarification. {reasoning}",
                'original_followup_question': sql_followup_question,
                'detected_new_question': sql_followup_answer
            }

        # ========================================
        # STEP 2: SQL GENERATION (based on classification)
        # ========================================
        print("\n📋 STEP 2: SQL Generation...")

        if classification == 'DATASET_CHANGE_REQUEST':
            print(f"\n🔄 DATASET_CHANGE_REQUEST detected - handling dataset change flow")
            return await self._handle_dataset_change_from_followup(
                requested_dataset_names=requested_datasets,
                context=context,
                state=state
            )

        if classification == 'SIMPLE_APPROVAL':
            print(f"\n✅ SIMPLE_APPROVAL detected - using lightweight SQL generation (~800 tokens)")
            return await self._generate_sql_simple_approval_async(
                context=context,
                sql_followup_question=sql_followup_question,
                sql_followup_answer=sql_followup_answer,
                state=state
            )

        # APPROVAL_WITH_MODIFICATIONS: Handle with re-planning logic
        print(f"\n✏️ APPROVAL_WITH_MODIFICATIONS detected")

        # Check plan iteration to decide flow (max 2 plans)
        plan_iteration = state.get('plan_iteration', 0)

        if plan_iteration == 0:
            # First modification - extract elements and search for filter values
            print(f"📋 First plan modification (iteration {plan_iteration}) - extracting elements")

            # Extract and search for filter values
            # Returns (additional_metadata, unresolved_filter_values)
            additional_metadata, unresolved_filter_values = await self._extract_and_search_new_filters(
                modifications, state
            )

            # Append additional metadata (found columns + other elements)
            existing_filter_metadata = state.get('filter_metadata_results', '')
            state['filter_metadata_results'] = existing_filter_metadata + additional_metadata

            # If there are unresolved filter values, add them to metadata for LLM to validate
            # Let the second planner decide if it can resolve them against metadata
            if unresolved_filter_values:
                unresolved_str = ", ".join(unresolved_filter_values)
                print(f"⚠️ Unresolved filter values (passing to planner): {unresolved_filter_values}")
                state['filter_metadata_results'] += f"\n**Unresolved filter values (validate against metadata):**\n- {unresolved_str}\n"

            # Store modification context for planner
            state['user_modification'] = modifications
            state['plan_iteration'] = 1
            state['force_show_plan'] = True  # Always show 2nd plan for confirmation

            # Re-run SQL Planner with updated context
            # This will generate a SECOND plan incorporating user's modifications
            print(f"🔄 Re-running SQL Planner with user modifications...")
            sql_context = self._extract_context(state)
            return await self._assess_and_generate_sql_async(sql_context, state)

        else:
            # Second plan approved with modifications
            # Go directly to SQL Writer (no third plan allowed)
            print(f"🔨 Second plan iteration ({plan_iteration}) - proceeding to SQL Writer (max plans reached)")

            if modifications:
                state['followup_modifications'] = modifications

            # Use the original full SQL_FOLLOWUP_PROMPT for final SQL generation
            return await self._generate_sql_full_followup_async(
                context=context,
                sql_followup_question=sql_followup_question,
                sql_followup_answer=sql_followup_answer,
                state=state
            )

    async def _generate_sql_full_followup_async(self, context: Dict, sql_followup_question: str, sql_followup_answer: str, state: Dict) -> Dict[str, Any]:
        """
        Full SQL generation with follow-up using the original SQL_FOLLOWUP_PROMPT.

        This method is used for APPROVAL_WITH_MODIFICATIONS cases where the user
        added new requirements or constraints that need to be incorporated.

        This is the original implementation, preserved for complex cases.
        """
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        join_clause = state.get('join_clause', '')
        filter_metadata_results = state.get('filter_metadata_results', [])

        # Retrieve history_section from state
        history_section = state.get('sql_history_section', '')
        # Check if we have multiple tables
        selected_datasets = state.get('selected_dataset', [])

        # Define mandatory column mapping
        mandatory_column_mapping = {
            "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast": [
                "Ledger"
            ],
            "prd_optumrx_orxfdmprdsa.rag.pbm_claims": ["product_category='PBM'"]
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

        # Build dataset context for followup
        selected_functional_names = state.get('functional_names', [])
        available_datasets = state.get('available_datasets', [])

        # Get other available datasets (excluding selected ones)
        other_datasets = [
            d['functional_name'] for d in available_datasets
            if d['functional_name'] not in selected_functional_names
        ]

        selected_dataset_name = ', '.join(selected_functional_names) if selected_functional_names else 'None'
        other_available_datasets = ', '.join(other_datasets) if other_datasets else 'None'

        # Clean the follow-up answer
        cleaned_sql_followup_answer = re.sub(r'(?i)\bfollow[- ]?up\b', '', sql_followup_answer).strip()

        followup_sql_prompt = SQL_FOLLOWUP_PROMPT.format(
            current_question=current_question,
            dataset_metadata=dataset_metadata,
            has_multiple_tables=has_multiple_tables,
            join_clause=join_clause if join_clause else "No join clause provided",
            mandatory_columns_text=mandatory_columns_text,
            filter_metadata_results=filter_metadata_results,
            history_section=history_section,
            sql_followup_question=sql_followup_question,
            sql_followup_answer=cleaned_sql_followup_answer,
            selected_dataset_name=selected_dataset_name,
            other_available_datasets=other_available_datasets
        )

        for attempt in range(self.max_retries):
            try:
                print(f'🔨 Full follow-up SQL generation (attempt {attempt + 1})...')
                print(f'follow up sql prompt: {followup_sql_prompt[:7000]}...')

                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": followup_sql_prompt}],
                    max_tokens=3000,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt=SQL_FOLLOWUP_SYSTEM_PROMPT
                )
                print(f'follow up sql response: {llm_response}')

                self._log('info', "LLM response received from full SQL followup handler", state,
                         llm_response=llm_response,
                         attempt=attempt + 1)

                # Note: Skip dataset_change, new_question, topic_drift checks since
                # we already validated in Step 1. Go directly to SQL extraction.

                # Extract sql_story tag
                sql_story = ""
                story_match = re.search(r'<sql_story>(.*?)</sql_story>', llm_response, re.DOTALL)
                if story_match:
                    sql_story = story_match.group(1).strip()
                    print(f"📖 Captured SQL generation story from full followup ({len(sql_story)} chars)")

                # Extract history_sql_used flag
                history_sql_used = False
                history_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', llm_response, re.IGNORECASE)
                if history_match:
                    history_sql_used = history_match.group(1).strip().lower() == 'true'
                    print(f"📊 history_sql_used flag: {history_sql_used}")

                # Check for multiple SQL queries
                multiple_sql_match = re.search(r'<multiple_sql>(.*?)</multiple_sql>', llm_response, re.DOTALL)
                if multiple_sql_match:
                    multiple_content = multiple_sql_match.group(1).strip()

                    # Extract individual queries with titles
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
                            if cleaned_query and cleaned_title:
                                sql_queries.append(cleaned_query)
                                query_titles.append(cleaned_title)

                        if sql_queries:
                            print(f"✅ Generated {len(sql_queries)} queries via full followup")
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'topic_drift': False,
                                'new_question': False,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,
                                'query_count': len(sql_queries),
                                'history_sql_used': history_sql_used,
                                'sql_story': sql_story
                            }

                    raise ValueError("Empty or invalid multiple SQL queries in XML response")

                # Check for single SQL query
                sql_match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if sql_match:
                    sql_query = sql_match.group(1).strip().replace('`', '')

                    if not sql_query:
                        raise ValueError("Empty SQL query in XML response")

                    print(f"✅ Generated single SQL via full followup ({len(sql_query)} chars)")
                    return {
                        'success': True,
                        'multiple_sql': False,
                        'topic_drift': False,
                        'new_question': False,
                        'sql_query': sql_query,
                        'history_sql_used': history_sql_used,
                        'sql_story': sql_story
                    }

                raise ValueError("No valid SQL found in response")

            except Exception as e:
                print(f"❌ Full SQL generation attempt {attempt + 1} failed: {str(e)}")

                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying... ({attempt + 1}/{self.max_retries})")
                    await asyncio.sleep(2 ** attempt)

        return {
            'success': False,
            'topic_drift': False,
            'new_question': False,
            'history_sql_used': False,
            'error': f"Full SQL generation with follow-up failed after {self.max_retries} attempts"
        }

    async def _generate_sql_after_dataset_change(self, context: Dict, state: Dict) -> Dict[str, Any]:
        """Generate SQL after dataset change without re-validation

        This method is called when user changes dataset during follow-up.
        It generates SQL directly using the new dataset metadata without
        going through the planner validation again.

        Args:
            context: Execution context containing question, metadata, etc.
            state: Current state with updated dataset information

        Returns:
            Dictionary with SQL generation result
        """

        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        filter_metadata_results = state.get('filter_metadata_results', [])
        selected_datasets = state.get('selected_dataset', [])

        # Build mandatory columns (same as in _generate_sql_with_followup_async)
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

        mandatory_columns_text = "\n".join(mandatory_columns_info) if mandatory_columns_info else "Not Applicable"

        # Build history section (if available)
        history_question_match = state.get('history_question_match', '')
        matched_sql = state.get('matched_sql', '')
        has_history = bool(matched_sql and history_question_match)

        if has_history:
            history_section = SQL_HISTORY_SECTION_TEMPLATE.format(
                history_question_match=history_question_match,
                matched_sql=matched_sql
            )
        else:
            history_section = SQL_NO_HISTORY_SECTION

        # Use comprehensive SQL_DATASET_CHANGE_PROMPT template
        # This is a copy of SQL_WRITER_PROMPT but without context_output field
        cleaned_sql_followup_answer = re.sub(r'(?i)\bfollow[- ]?up\b', '', state.get('current_question', '')).strip()

        sql_generation_prompt = SQL_DATASET_CHANGE_PROMPT.format(
            current_question=current_question,
            dataset_metadata=dataset_metadata,
            mandatory_columns_text=mandatory_columns_text,
            filter_metadata_results=filter_metadata_results,
            history_section=history_section,
            sql_followup_question=state.get('sql_followup_question', ''),
            sql_followup_answer=cleaned_sql_followup_answer
        )

        # Retry logic for SQL generation
        for attempt in range(self.max_retries):
            try:
                print(f'Dataset change SQL prompt: {sql_generation_prompt[:7000]}')
                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": sql_generation_prompt}],
                    max_tokens=3000,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt=SQL_WRITER_SYSTEM_PROMPT
                )

                print(f'Dataset change SQL response: {llm_response}')

                # Extract sql_story FIRST (before other tags)
                sql_story = ""
                story_match = re.search(r'<sql_story>(.*?)</sql_story>', llm_response, re.DOTALL)
                if story_match:
                    sql_story = story_match.group(1).strip()
                    print(f"📖 Captured SQL generation story from dataset change ({len(sql_story)} chars)")

                # Check for multiple SQL queries FIRST
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
                            # Extract history_sql_used flag
                            history_sql_used = False
                            history_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', llm_response, re.IGNORECASE)
                            if history_match:
                                history_sql_used = history_match.group(1).lower() == 'true'
                                print(f"📊 history_sql_used flag from LLM: {history_sql_used}")

                            print(f"✅ Multiple SQL queries generated after dataset change")
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'topic_drift': False,
                                'new_question': False,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,
                                'query_count': len(sql_queries),
                                'history_sql_used': history_sql_used,
                                'sql_story': sql_story
                            }

                    raise ValueError("Empty or invalid multiple SQL queries in XML response")

                # Check for single SQL query
                match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if match:
                    sql_query = match.group(1).strip()
                    sql_query = sql_query.replace('`', '')  # Remove backticks

                    if not sql_query:
                        raise ValueError("Empty SQL query in XML response")

                    # Extract history_sql_used flag
                    history_sql_used = False
                    history_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', llm_response, re.IGNORECASE)
                    if history_match:
                        history_sql_used = history_match.group(1).lower() == 'true'
                        print(f"📊 history_sql_used flag from LLM: {history_sql_used}")

                    print(f"✅ SQL generated successfully after dataset change")
                    return {
                        'success': True,
                        'multiple_sql': False,
                        'topic_drift': False,
                        'new_question': False,
                        'sql_query': sql_query,
                        'history_sql_used': history_sql_used,
                        'sql_story': sql_story
                    }
                else:
                    raise ValueError("No valid XML response found (expected sql or multiple_sql)")

            except Exception as e:
                print(f"❌ SQL generation after dataset change - attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        return {
            'success': False,
            'topic_drift': False,
            'new_question': False,
            'history_sql_used': False,
            'error': f"SQL generation failed after dataset change ({self.max_retries} attempts)"
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
        """Use LLM to fix SQL based on error with enhanced prompting and retry logic async

        Uses SQL_FIX_PROMPT template from prompts package.
        """

        history_text = "\n".join(errors_history) if errors_history else "No previous errors"
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')

        # Use imported prompt template
        fix_prompt = SQL_FIX_PROMPT.format(
            current_question=current_question,
            dataset_metadata=dataset_metadata,
            failed_sql=failed_sql,
            error_msg=error_msg,
            history_text=history_text
        )

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
