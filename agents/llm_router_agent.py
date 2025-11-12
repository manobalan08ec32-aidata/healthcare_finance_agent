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

class LLMRouterAgent:
    """Enhanced router agent with dataset selection and SQL generation"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        self.max_retries = 3
    
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
        if state.get('requires_dataset_clarification', False):
            print(f"🔄 Processing dataset clarification follow-up")
            # Get existing filter metadata from state if available
            existing_filter_metadata = state.get('filter_metadata_results', [])
            result = await self._fix_router_llm_call(state, existing_filter_metadata)
           
            if result.get('error', False):
                print(f"❌ Dataset selection failed with error")
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
            common_categorical_values = {'pbm', 'hdp', 'home delivery', 'mail', 'specialty','claim fee','claim cost','admin fee','claimfee','claimcost','adminfee','8+4','9+3','2+10','5+7','optum','retail'}
            
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
                except Exception as e:
                    print(f"⚠️ Filter metadata search failed: {e}")
                    filter_metadata_results = []
            
            # 4. Load domain-specific dataset config
            domain_selection = state.get('domain_selection', '')
            mapped_dataset_file = None
            if domain_selection == 'PBM Network':
                mapped_dataset_file = 'pbm_dataset.json'
            elif domain_selection == 'Optum Pharmacy':
                mapped_dataset_file = 'pharmacy_dataset.json'
            else:
                # Default fallback if domain not recognized; could still use vector search fallback
                raise Exception("Domain Not Found")
            print('domain selection in router',mapped_dataset_file)
            search_results = []
            try:
                drillthrough_config_path = f"config/datasets/{mapped_dataset_file}"
                with open(drillthrough_config_path, 'r') as f:
                    search_results = json.load(f)
                print(f"✅ Loaded dataset config from: {drillthrough_config_path} for domain '{domain_selection}'")
            except Exception as e:
                print(f"❌ Failed loading dataset config {mapped_dataset_file}: {e}")
                return {
                    'error': True,
                    'error_message': f"Failed to load dataset config {mapped_dataset_file}: {str(e)}"
                }

            if not search_results:
                raise Exception("No datasets found in config")

            # 3. LLM selection with filter metadata integration
            selection_result = await self._llm_dataset_selection(search_results, state, filter_metadata_results)
            # 3. Return results - either final selection or clarification needed
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
⚠️⚠️⚠️ CRITICAL INSTRUCTION - READ THIS FIRST ⚠️⚠️⚠️

You are a DATABASE TABLE SELECTION SYSTEM - NOT an AI assistant that answers business questions.
Your ONLY function is to analyze metadata and select the right database table(s) for query routing.
====================================================

You are a Dataset Identifier Agent. You have FIVE sequential tasks to complete.

    CURRENT QUESTION: {user_question}

    EXTRACTED COLUMNS WITH FILTER VALUES: {filter_values}
    {filter_metadata_text}

    AVAILABLE DATASETS (JSON FORMAT): 

```json
{self._format_json_compact(search_results)}
```

    A. **PHI/PII SECURITY CHECK**:
- First, examine each dataset's "PHI_PII_Columns" field (if present)
- Analyze the user's question to identify if they are requesting any PHI/PII information
- PHI/PII includes: SSN, member IDs, personal identifiers, patient names, addresses, etc.
- Check if the user's question mentions or implies access to columns listed in "PHI_PII_Columns"
- If PHI/PII columns are requested, IMMEDIATELY return phi_found status (do not proceed to other checks)

B. **METRICS & ATTRIBUTES CHECK**:
- Extract requested metrics/measures and attributes/dimensions
- Apply smart mapping with these rules:

**TIER 1 - Direct Matches**: Exact column names
**TIER 2 - Standard Healthcare Mapping**: 
    * "therapies" → "therapy_class_name"
    * "scripts" → "unadjusted_scripts/adjusted_scripts"  
    * "drugs" → "drug_name"
    * "clients" → "client_id/client_name"

**TIER 3 - Mathematical Operations**: 
    * "variance/variances" → calculated from existing metrics over time periods
    * "growth/change" → period-over-period calculations
    * "percentage/rate" → ratio calculations

**TIER 4 - Skip Common Filter Values**: 
    * Skip validation for: "external","PBM","HDP", "optum", "mail", "Specialty", "Home Delivery", "brand", "generic","retail"
    * These appear to be filter values, not missing attributes

**BLOCK - Creative Substitutions**:
    * Do NOT map unrelated terms (e.g., "ingredient fee" ≠ "expense")
    * Do NOT assume domain knowledge not in metadata

- Only mark as missing if NO reasonable Tier 1-3 mapping exists

**NEW - EXPLICIT ATTRIBUTE DETECTION**:
- Scan the user's question for explicit attribute keywords(examples below):
    * "carrier" → carrier_name, carrier_id
    * "drug" → drug_name, drug_id
    * "pharmacy" → pharmacy_name, pharmacy_id
    * "therapy" → therapy_class_name, therapy_id
    * "client" → client_name, client_id
    * "manufacturer" → drug_manufctr_nm, manufacturer_name
- Flag as "explicit_attribute_mentioned = true/false"
- Store mapped column names for later filter disambiguation check
- This detection is case-insensitive and looks for these keywords anywhere in the question

C. **KEYWORD & SUITABILITY ANALYSIS**:
- **KEYWORD MATCHING**: Look for domain keywords that indicate preferences:
* "claim/claims" → indicates claim_transaction dataset relevance
* "forecast/budget" → indicates actuals_vs_forecast dataset relevance  
* "ledger" → indicates actuals_vs_forecast dataset relevance

- **CRITICAL: SUITABILITY VALIDATION (HARD CONSTRAINTS)**:
* **BLOCKING RULE**: If a dataset's "not_useful_for" field contains keywords/patterns that match the user's question, IMMEDIATELY EXCLUDE that dataset regardless of other factors
* **POSITIVE VALIDATION**: Check if user's question aligns with "useful_for" field patterns
* **Example Applications**:
- User asks "top 10 clients by expense" → Ledger has "not_useful_for": ["client level expense"] → EXCLUDE ledger table completely
- User asks "drug or therapy level info" → Ledger has "not_useful_for": → EXCLUDE ledger table completely
* **PRECEDENCE**: not_useful_for OVERRIDES metrics/attributes availability - even if a table has the columns, exclude it if explicitly marked as not suitable

- Verify time_grains match user needs (daily vs monthly vs quarterly)
- Note: Keywords indicate relevance but suitability constraints are MANDATORY

D. **COMPLEMENTARY ANALYSIS CHECK**:
- **PURPOSE**: Identify if multiple datasets together provide more complete analysis than any single dataset
- **LOOK FOR THESE PATTERNS**:
* Primary metric in one dataset + dimensional attributes in another 
* Different analytical perspectives on same business question 
* One dataset provides core data, another provides breakdown/segmentation (claims + billing)
* Cross-dataset comparison needs (e.g., budget vs actual vs claims)
* **BREAKDOWN ANALYSIS**: When question asks for metric breakdown by dimensions not available in the primary dataset

- **EVALUATION CRITERIA**:
* Single dataset with ALL metrics + attributes → SELECT IT
* No single complete dataset → SELECT MULTIPLE if complementary
* Primary metric in A + breakdown dimension in B → SELECT BOTH

**KEY EXAMPLES**:
- "top 10 drugs by revenue" → Claims table (has revenue + drug_name) NOT Ledger (missing drug_name)
- "total revenue" → Ledger table (high_level_table tie-breaker when both have revenue)
- "ledger revenue breakdown by drug" → Both tables (complementary: ledger revenue + claims drug_name)

**CLARIFICATION vs COMPLEMENTARY**:
- Ask clarification when: Same data available in multiple datasets with different contexts OR multiple columns in same table
- Select multiple when: Different but compatible data needed from each dataset for complete analysis

F. **FINAL DECISION LOGIC**:
- **STEP 1**: Check results from sections A through D
- **STEP 2**: MANDATORY Decision order:
* **FIRST**: Apply suitability constraints - eliminate datasets with "not_useful_for" matches
* **SECOND**: Validate complete coverage (metrics + attributes) on remaining datasets
* **THIRD**: Single complete dataset → SELECT IT

* **NEW - SMART FILTER DISAMBIGUATION CHECK**:
**STEP 3A**: Check if filter values exist in multiple columns
**STEP 3B**: Determine if follow-up is needed using this SIMPLIFIED logic:

1. **Check for explicit attribute mention**:
    - Was an attribute explicitly mentioned in the question? (from Section B detection)
    - Examples: "Carrier", "drug", "pharmacy", "therapy", "client", "manufacturer", "plan"

2. **Count matching columns in selected dataset**:
    - From filter metadata, identify all columns containing the filter value
    - Filter to only columns that exist in the selected dataset's attributes
    - Store as: matching_columns_count

3. **SIMPLE DECISION TREE**:
    - IF explicit_attribute_mentioned = true → NO FOLLOW-UP (trust user's specification)
    - IF explicit_attribute_mentioned = false:
        * IF matching_columns_count = 1 → NO FOLLOW-UP (obvious choice)
        * IF matching_columns_count > 1 → RETURN "needs_disambiguation"

**Examples**:
- "Carrier MPDOVA billed amount" + MPDOVA in [carrier_name, carrier_id, plan_name] → ✓ NO follow-up (user said "Carrier")
- "MPDOVA billed amount" + MPDOVA in [carrier_name] only → ✓ NO follow-up (only 1 match)
- "covid vaccine billed amount" + covid vaccine in [drug_name, pharmacy_name, therapy_class_name] → ❌ ASK which column (no explicit attribute, 3 matches)

* **FOURTH**: No single complete → SELECT MULTIPLE if complementary
* **FIFTH**: Multiple complete → Use traditional tie-breakers (keywords, high_level_table)
* **SIXTH**: Still tied → RETURN "needs_disambiguation" and ask user to choose
* **LAST**: No coverage OR unresolvable ambiguity → Report as missing items or request clarification

**HIGH LEVEL TABLE PRIORITY RULE** (ONLY APPLIES DURING TIES):
- **CRITICAL**: High-level table priority is ONLY used as a tie-breaker when multiple datasets have ALL required metrics AND attributes
- **PRIMARY RULE**: ALWAYS validate that dataset has both required metrics AND required attributes FIRST
- **HIGH LEVEL QUESTION INDICATORS**: Questions asking for summary metrics, totals, aggregates, or general overviews without specific breakdowns
- **Examples of HIGH LEVEL**: "total revenue", "overall costs", "summary metrics", "high-level view", "aggregate performance", "what is the revenue", "show me costs"  
- **Examples of NOT HIGH LEVEL**: "revenue breakdown by therapy", "costs by client", "detailed analysis", "revenue by drug category", "performance by region", "top drugs by revenue", "top clients by cost"
- **VALIDATION FIRST RULE**: 
* Step 1: Check if dataset has required metrics (revenue, cost, etc.)
* Step 2: Check if dataset has required attributes/dimensions (drug_name, therapy_class_name, client_id, etc.)
* Step 3: ONLY if multiple datasets pass Steps 1 & 2, then check "high_level_table": "True" as tie-breaker
- **NEVER OVERRIDE RULE**: Never select high_level_table if it's missing required attributes, even for "high-level" questions

==============================
DECISION CRITERIA
==============================

**PHI_FOUND** IF:
- User question requests or implies access to PHI/PII columns
- Any columns mentioned in "PHI_PII_Columns" are being requested
- Must be checked FIRST before other validations

**PROCEED** (SELECT DATASET) IF:
- **STANDARD PATH**: Dataset passes suitability validation (not blocked by "not_useful_for" field) AND all requested metrics/attributes have Tier 1-3 matches AND clear selection
- Single dataset meets all requirements after suitability and coverage checks
- Complementary datasets identified for complete coverage after all validations

**MISSING_ITEMS** IF:
- Required metrics/attributes don't have Tier 1-3 matches in any dataset
- No suitable alternatives available after all validation steps

**REQUEST_FOLLOW_UP** IF:
- **PRIORITY 1 - DATASET AMBIGUITY**: 
* Multiple datasets with conflicting contexts AND no clear preference from traditional validation
* ALWAYS ask user to specify which table/dataset to use FIRST

- **PRIORITY 2 - SMART FILTER COLUMN AMBIGUITY**: 
* User did NOT explicitly mention an attribute (e.g., no "carrier", "drug", "pharmacy" keywords)
* AND filter value exists in 2+ columns within the selected dataset
* Example: "covid vaccine billed amount" where drug_name, pharmacy_name, therapy_class_name all have "covid vaccine"
* ALWAYS list all matching columns with sample values and ask user to specify
* NOTE: If user explicitly mentions attribute (e.g., "Carrier MPDOVA"), NO follow-up needed regardless of multiple matches

==============================
ASSESSMENT FORMAT - Brief reasoning
==============================

**MAPPING:** Metrics: [m→col (T1/2/3)] | Attrs: [a→col (T1/2/3)] | Explicit attr: [Y/N]
**SUIT:** [tbl]: ✓/❌ | [tbl2]: ✓/❌
**STRATEGY:** [Single/Complementary/Disambiguation]
**COVERAGE:** [tbl]: [X/Y]m, [A/B]a → [OK/MISS]

**DECISION:** [PROCEED/MISSING_ITEMS/NEEDS_DISAMBIGUATION]
**SELECTED:** [table(s)]
**REASON:** [1 sentence with key column refs]

=======================
RESPONSE FORMAT
=======================

Provide assessment above, then JSON in <json> tags.

"status": "phi_found" | "success" | "missing_items" | "needs_disambiguation",
"final_actual_tables": ["table_name_1","table_name2"] if status = success else [],
"functional_names": ["functional_name"] if status = success else [],
"tables_identified_for_clarification": ["table_1", "table_2"] if status = needs_disambiguation else [],
"functional_table_name_identified_for_clarification": ["functional_name_1", "functional_name_2"] if status = needs_disambiguation else [],
"requires_clarification": true if status = needs_disambiguation else false,
"selection_reasoning": "2-3 lines max explanation",
"high_level_table_selected": true/false if status = success else null,
"user_message": "message to user" if status = phi_found or missing_items else null,
"clarification_question": "The column exists in multiple table.Please tell me which [functional table name1] , [functional table name 2] to select" if status = needs_disambiguation else null,
"selected_filter_context": "col name - [actual_column_name], sample values [all values from filter extract]" if column selected from filter context else null


**FIELD POPULATION RULES FOR needs_disambiguation STATUS**:
- tables_identified_for_clarification: ALWAYS populate when status = needs_disambiguation
* If PRIORITY 1 (dataset ambiguity): List all candidate tables that need disambiguation
* If PRIORITY 2 (column ambiguity): List the single selected table where column disambiguation is needed

"""

        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # print("Sending selection prompt to LLM...",selection_prompt)
                print("Current Timestamp before dataset selector call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": selection_prompt}],
                    max_tokens=2500,
                    temperature=0.0,  # Deterministic for dataset selection
                    top_p=0.1,
                    system_prompt="DATASET SELECTION SYSTEM: You are an automated dataset identification and validation system for SQL query routing infrastructure."
                )
                
                print("Raw LLM response:", llm_response)
                print("Current Timestamp after dataset selector call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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
                "product_category"
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
            check_5_text = "**CHECK 5: Historical SQL availability**: ✓ Available (using as learning template)"
            history_section = f"""
=== HISTORICAL SUCCESSFUL SQL (LEARNING REFERENCE) ===

A similar question was successfully answered with this SQL:
- Previous Question: "{history_question_match}"
- Table Used: {matched_table_name}

<historical_sql>
{matched_sql}
</historical_sql>

**CRITICAL - HOW TO USE THIS REFERENCE:**

✅ LEARN FROM (Structure & Logic):
1. **Query Structure**: 
- Observe GROUP BY strategy (dimensions used)
- Study CASE WHEN patterns (side-by-side columns) and must keep side by side comparison pattern
- Note aggregation logic (SUM, COUNT, AVG placement)
- Review calculation methods (ROUND, NULLIF usage)

2. **Column Selection**:
- See which business dimensions are included
- Understand metric aggregations used
- Notice naming conventions (e.g., august_revenue_amount)

3. **Best Practices**:
- UPPER() for case-insensitive filters
- ROUND(x, 0) for amounts, ROUND(x, 3) for percentages
- NULLIF for division safety
- Clean, descriptive column aliases

4. **STRUCTURAL PATTERNS - MUST PRESERVE**:
- **UNION/UNION ALL**: If historical uses UNION → replicate this exact pattern (e.g., detail rows + OVERALL_TOTAL)
- **CTEs/Subqueries**: Preserve WITH clauses, nested queries, and subquery structure
- **Window Functions**: Keep PARTITION BY and ORDER BY logic intact
- **Dimension Columns**: Use EXACT column names (e.g., product_sub_category_lvl_2, not lvl_1) unless current question explicitly specifies different level
- **COPY the complete structure; ONLY change WHERE clause filter values to match current question**

5. **DIMENSION COLUMNS - CRITICAL FOR FEEDBACK LEARNING**:
- Observe ALL dimension columns in SELECT and GROUP BY clauses
- These represent the level of detail that was SUCCESSFUL and USEFUL to users
- **DEFAULT BEHAVIOR: PRESERVE all dimensions from historical SQL**
- Example: Historical has [product_category, product_sub_category_lvl_2, year, month]
    → Keep ALL of these in your SELECT and GROUP BY
- This is USER FEEDBACK in action - they found this granularity valuable

**Only REMOVE a dimension if:**
* Current question EXPLICITLY asks for higher-level aggregation (e.g., "total PBM revenue" without any breakdown)
* Dimension column doesn't exist in current AVAILABLE METADATA
* Current question specifies DIFFERENT grouping dimensions (e.g., "by line_of_business" when historical was "by product_sub_category")

**When in doubt: KEEP the historical dimensions**

❌ DO NOT COPY DIRECTLY (Adapt These):
1. **Filter Values**: 
- Historical may have <parameter> placeholders or specific values
- ALWAYS extract filters from CURRENT question
- Example: Historical has carrier_id = 'MPDOVA' → Use carrier_id from current question

2. **Date/Time Values**:
- Historical may have specific dates/periods
- ALWAYS use dates from CURRENT question
- Example: Historical has "year = 2024" → Use year from current question

3. **Entity Names**:
- Client names, carrier IDs, product categories, etc.
- ALWAYS use entities from CURRENT question

⚠️ MANDATORY VALIDATIONS:
1. **Add Mandatory Filters**: 
- Check MANDATORY FILTER COLUMNS section above
- Historical SQL may not have these (different requirements)
- YOU MUST ADD any mandatory filters listed

2. **Verify Column Availability**:
- Confirm all columns exist in AVAILABLE METADATA
- If historical column missing, use equivalent from metadata

3. **Apply Current Filters**:
- Use FILTER VALUES EXTRACTED section (marked ✓Valid)
- Apply filters from CURRENT question, not historical

4. **Update Time Logic**:
- Match time structure to CURRENT question
- Monthly trend? YoY comparison? Date range? Use current requirement

5. **PRESERVE Historical Dimensions**:
- Keep ALL GROUP BY columns from historical SQL in your SELECT and GROUP BY
- These dimensions represent proven user value from feedback
- Only remove if current question explicitly asks for higher aggregation
- Example: Historical has "product_sub_category_lvl_2" → Keep it unless user says "total" or "overall"

**ADAPTATION PRIORITY:**
Content from CURRENT question > Historical structure > Metadata defaults

**DIMENSION PRESERVATION PRIORITY:**
Keep historical GROUP BY dimensions > Only remove if explicitly contradicted

This is a LEARNING TEMPLATE, not a query to copy. Generate ADAPTED SQL for current question while PRESERVING the dimensional granularity that made the historical query successful.

**IMPORTANT**: 
- If you use historical SQL's structure AND preserve its dimensions → set history_sql_used = true
- If you generate from scratch without using historical patterns → set history_sql_used = false
- Preserving dimensions is CRITICAL - it represents user feedback on useful granularity

====================================================

    """
        else:
            history_section = """
    === HISTORICAL SQL ===
    Not available

    """


        assessment_prompt = f"""
⚠️⚠️⚠️ CRITICAL ROLE - READ THIS FIRST ⚠️⚠️⚠️

You are a DATABRICKS SQL CODE GENERATOR - NOT an AI assistant. You do NOT answer business questions.
You do NOT provide insights. You ONLY generate SQL code.

Think of yourself as: translate(question + metadata) → SQL code | validation → follow-up questions
⚠️ REMINDER: This is a technical code generation task, NOT answering user's business question.

====================================================

You are a Healthcare Finance SQL code generator. You have TWO sequential technical tasks:
TASK 1: Assess if metadata is sufficient for SQL generation
TASK 2: Generate SQL code (only if TASK 1 says PROCEED)

CURRENT QUESTION: {current_question}
MULTIPLE TABLES AVAILABLE: {has_multiple_tables}
JOIN INFORMATION: {join_clause if join_clause else "No join clause provided"}
MANDATORY FILTER COLUMNS: {mandatory_columns_text}

FILTER VALUES EXTRACTED:
{filter_context_text}

AVAILABLE METADATA: {dataset_metadata}

{history_section}

==============================
COMPREHENSIVE ASSESSMENT
==============================

Perform complete validation covering ALL aspects (metadata, filters, context, rules). Each check must pass for SQL generation.

**1. TERM EXTRACTION & METADATA MAPPING**
- Extract ALL terms (metrics, attributes, filters, dimensions) from question
- Validate each against AVAILABLE METADATA:
  * Exact match: "carrier_id" → carrier_id → ✓Found(carrier_id)
  * Fuzzy match: "carrier" → carrier_id → ✓Found(carrier_id)
  * No match: "xyz" → ❌NotFound
  * Ambiguous: "region" → state/territory/district → ⚠️Ambiguous(col1,col2)

**2. FILTER CONTEXT VALIDATION**
If question has filter value WITHOUT attribute name (e.g., "MPDOVA" not "carrier_id MPDOVA"), check FILTER VALUES EXTRACTED:
- Exact match in question + column in metadata → ✓Valid(column_name)
- Partial match OR missing column → ❌Invalid

**3. CLARIFICATION RULES CHECK**
If metadata has "clarification_rules", evaluate question against each rule:
- Rule triggered → ❌Blocked(rule)
- No rules triggered → ✓Clear

**4. TEMPORAL SCOPE**
- Past/current dates (≤Oct 2025) → ✓Valid
- Near-future (≤12 months) → ✓Valid
- Far-future (>Nov 2026) → ❌NeedsClarify

**5. METRIC DEFINITIONS**
- Standard aggregations (SUM/COUNT/AVG) → ✓Clear
- Custom formulas not specified → ❌Unclear

**6. BUSINESS CONTEXT**
- Filtering AND grouping explicit → ✓Clear
- Missing critical context ("top by what?") → ❌Unclear

**7. QUERY STRATEGY**
- Single/multi/join approach clear → ✓Clear
- Multi-table approach unclear → ❌Unclear

{check_5_text}

⚠️ IMPORTANT: Perform all checks above internally, but DO NOT display the detailed step-by-step assessment. Only output the compact ASSESSMENT OUTPUT format below.

===================
ASSESSMENT OUTPUT
===================

**MAPPING:** Metrics:[m→col(agg)] | Attrs:[a→col] | Filters:[f:WHERE]
**VALIDATION:** term1(✓col) | term2(❌) | term3(⚠️col1,col2)
**FILTER CTX:** ✓Valid(col) | ❌Invalid | N/A
**RULES:** ✓Clear | ❌Blocked(rule)
**CHECKS:** 1:✓/❌ | 2:✓/❌ | 3:✓/❌ | 4:✓/❌ | 5:✓/❌ | 6:✓/❌ | 7:✓/❌
**STRATEGY:** [Single/Multi/Join] - [why]
**HISTORY:** (if avail) Learned:[pattern] | Preserved:[dims] | Adapted:[changes]

**DECISION:** PROCEED | FOLLOW-UP
**REASON:** [1 sentence]

======================
DECISION CRITERIA
======================

**PROCEED if:** ALL checks (1-7) = ✓ or N/A, NO ❌, NO blocking ⚠️
**FOLLOW-UP if:** ANY check = ❌ OR any ⚠️ affecting SQL accuracy

**Rule: ONE failure = STOP. No SQL with uncertainty.**

====================================
FOLLOW-UP GENERATION (If DECISION = FOLLOW-UP)
===================================

Generate follow-up questions to gather missing metadata/context needed for SQL generation.

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

==============================================
TASK 2: HIGH-QUALITY DATABRICKS SQL GENERATION 
==============================================

⚠️ REMINDER: Generate SQL code ONLY. Do NOT answer the business question. Do NOT provide insights.

**CORE SQL GENERATION RULES:**
1. MANDATORY FILTERS - ALWAYS APPLY
- Review MANDATORY FILTER COLUMNS section - any marked MANDATORY must be in WHERE clause

2. VALIDATED FILTERS - APPLY FROM CHECK 2
**Rule**: If COMPREHENSIVE ASSESSMENT Check 2 marked filters as ✓Valid:
- Apply exact match filter: WHERE UPPER(column_name) = UPPER('VALUE')
- For multiple values use IN: WHERE UPPER(column_name) IN (UPPER('VAL1'), UPPER('VAL2'))

Only use filters validated in Check 2 (Filter Context Validation)

3. CALCULATED FORMULAS HANDLING (CRITICAL)
**When calculating derived metrics (Gross Margin, Cost %, Margin %, etc.), DO NOT group by metric_type:**

CORRECT PATTERN:
```sql
SELECT 
    ledger, year, month,  -- Business dimensions only
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount_or_count ELSE 0 END) AS revenues,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS Post Reclass') THEN amount_or_count ELSE 0 END) AS expense_cogs,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount_or_count ELSE 0 END) - 
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS Post Reclass') THEN amount_or_count ELSE 0 END) AS gross_margin
FROM table
WHERE conditions AND UPPER(metric_type) IN (UPPER('Revenues'), UPPER('COGS Post Reclass'))
GROUP BY ledger, year, month  -- Group by dimensions, NOT metric_type
```

WRONG PATTERN:
```sql
GROUP BY ledger, metric_type  -- Creates separate rows per metric_type, breaks formulas
```

**Only group by metric_type when user explicitly asks to see individual metric types as separate rows.**

4. METRICS & AGGREGATIONS
- Always use appropriate aggregation functions for numeric metrics: SUM, COUNT, AVG, MAX, MIN
- Even with specific entity filters (invoice #123, member ID 456), always aggregate unless user asks for "line items" or "individual records"
- Include time dimensions (month, quarter, year) when relevant to question
- Use business-friendly dimension names (therapeutic_class, service_type, age_group, state_name)

5. SELECT CLAUSE STRATEGY

**Calculations & Breakdowns (analysis BY dimensions):**
- Include ALL columns used in WHERE, GROUP BY, and calculations
- **MANDATORY: When calculating ANY metric, ALWAYS show source components in SELECT**
- Pattern: Display [source_metric_1], [source_metric_2], ..., [calculated_result]
- Why: Users need to see underlying values that produced the calculation

Examples:
- Percentage → Show: numerator, denominator, calculated_percentage
- Variance → Show: current_value, prior_value, variance
- Ratio → Show: numerator, denominator, ratio
- Formula → Show: component_1, component_2, calculated_result
- Per-unit → Show: total_amount, unit_count, per_unit_value

Example: "Cost per member by state" 
→ SELECT state_name, total_cost, member_count, cost_per_member

6. MULTI-TABLE JOIN SYNTAX (when applicable)
- Use provided join clause exactly as specified
- Qualify all columns with table aliases
- Include all necessary tables in FROM/JOIN clauses
- Only join if question requires related data together; otherwise use separate queries

7. ATTRIBUTE-ONLY QUERIES
- If question asks only about attributes (age, name, type) without metrics, return relevant columns without aggregation

8. STRING FILTERING - CASE INSENSITIVE
- Always use UPPER() on both sides for text/string comparisons
- Example: WHERE UPPER(product_category) = UPPER('Specialty')

9. TOP N/BOTTOM N QUERIES WITH CONTEXT
-Show requested top/bottom N records with their individual values
-CRITICAL: Include the overall total as an additional COLUMN in each row (not as a separate row)
-Calculate and show percentage contribution: (individual value / overall total) × 100
Overall totals logic:
    -✅ Include overall total column for summable metrics: revenue, cost, expense, amount, count, volume, scripts, quantity, spend
    -❌ Exclude overall total column for derived metrics: margin %, ratios, rates, per-unit calculations, averages
-Use subquery in SELECT to show overall total alongside each individual record
-Column structure: [dimension] | [individual_value] | [overall_total] | [percentage_contribution]
-ALWAYS filter out blank/null records: WHERE column_name NOT IN ('-', 'BL')

10. COMPARISON QUERIES - SIDE-BY-SIDE FORMAT
- When comparing two related metrics (actual vs forecast, budget vs actual), use side-by-side columns
- For time-based comparisons (month-over-month, year-over-year), display time periods as adjacent columns with clear month/period names
- Example: Display "January_Revenue", "February_Revenue", "March_Revenue" side by side for easy comparison
- Include variance/difference columns when comparing metrics
- Prevents users from manually comparing separate rows

11. DATABRICKS SQL COMPATIBILITY
- Standard SQL functions: SUM, COUNT, AVG, MAX, MIN
- Date functions: date_trunc(), year(), month(), quarter()
- Conditional logic: CASE WHEN
- CTEs: WITH clauses for complex logic

12. FORMATTING & NAMING
**Numeric columns:**
- Amounts/Counts/Totals: ROUND(value, 0) AS name_amount or name_count
- Percentages/Ratios : ROUND(value, 3) AS name_percent
- Examples: total_revenue_amount, cost_ratio_percent, script_count

**Ordering:** ORDER BY date columns DESC only. Use business-relevant names.

**Ordering:** ORDER BY date columns DESC only. Use business-relevant names.

==============================
OUTPUT FORMATS
==============================

Return ONLY the result in XML tags with no additional text.

**SINGLE SQL QUERY:**
<sql>
[Your complete SQL query]
</sql>
<history_sql_used>[true or false]</history_sql_used>

**MULTIPLE SQL QUERIES:**
<multiple_sql>
<query1_title>[Title - max 8 words]</query1_title>
<query1>[SQL query]</query1>
<query2_title>[Title - max 8 words]</query2_title>
<query2>[SQL query]</query2>
</multiple_sql>
<history_sql_used>[true or false]</history_sql_used>

**HISTORY_SQL_USED FLAG RULES:**
- If historical SQL was available AND you used its structure/patterns → true
- If historical SQL was available BUT you generated from scratch → false
- If historical SQL was not available → false

==============================
EXECUTION INSTRUCTION
==============================

⚠️ FINAL REMINDER: You are generating SQL CODE, not answering the business question.

1. Complete TASK 1 strict assessment (A-G with clear marks)
2. Apply STRICT decision: ANY ❌ or blocking ⚠️ = FOLLOW-UP
3. If PROCEED: Execute TASK 2 with SQL generation (learn from historical SQL if available)
4. Must preserve the mandatory filter value in the SQL generation
5. If FOLLOW-UP: Ask targeted questions
6. Always include history_sql_used flag in output (true/false)

**Show your work**: assessment, then SQL or follow-up.
**Remember**: ONE failure = STOP.
**Critical**: You are a SQL code generator, NOT a business question answerer.
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
                    system_prompt="DATABRICKS SQL GENERATOR SYSTEM: You are an automated SQL query generation system for Databricks analytics infrastructure. Your ONLY job is to generate high-quality, syntactically correct SQL queries based on metadata and user requirements. You do NOT answer business questions, provide data analysis, or interpret results. You ONLY generate SQL code. This is a technical code generation task, not business analysis."
                )
            
                print('sql llm response', llm_response)
                print("Current Timestamp after SQL write call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                
                # NEW: Extract history_sql_used flag
                history_sql_used = False
                history_used_match = re.search(r'<history_sql_used>\s*(true|false)\s*</history_sql_used>', llm_response, re.IGNORECASE | re.DOTALL)
                if history_used_match:
                    history_sql_used = history_used_match.group(1).strip().lower() == 'true'
                
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
                                'history_sql_used': history_sql_used  # NEW: LLM's flag
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
                        'history_sql_used': history_sql_used  # NEW: LLM's flag
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
⚠️⚠️⚠️ CRITICAL INSTRUCTION - READ THIS FIRST ⚠️⚠️⚠️

You are a DATABRICKS SQL CODE GENERATOR - NOT an AI assistant that answers business questions.
Your ONLY function is to generate high-quality, syntactically correct SQL queries based on clarification answers.
====================================================

You are a highly skilled Healthcare Finance SQL analyst. This is PHASE 2 of a two-phase process.

ORIGINAL USER QUESTION: {current_question}
**AVAILABLE METADATA**: {dataset_metadata}
MULTIPLE TABLES AVAILABLE: {has_multiple_tables}
JOIN INFORMATION: {join_clause if join_clause else "No join clause provided"}
MANDATORY FILTER COLUMNS: {mandatory_columns_text}

⚠️ REMINDER: You are NOT answering this question. You are ONLY generating SQL code based on the clarification.

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
5. All SQL generation best practices

**MULTI-QUERY DECISION LOGIC**:
- **SINGLE QUERY WITH JOIN**: Simple analysis requiring related data from multiple tables
- **MULTIPLE QUERIES - MULTI-TABLE**: Complementary analysis from different tables OR no join exists
- **MULTIPLE QUERIES - COMPLEX SINGLE-TABLE**: Multiple analytical dimensions (trends + rankings)
- **SINGLE QUERY**: Simple, focused questions with one analytical dimension

========================================
CRITICAL DATABRICKS SQL GENERATION RULES
=========================================


1. MANDATORY FILTERS - ALWAYS APPLY
- Review MANDATORY FILTER COLUMNS section - any marked MANDATORY must be in WHERE clause

1b. FILTER VALUES EXTRACTED - APPLY WHEN NO ATTRIBUTE MAPPING
**Rule**: If user question does NOT contain an attribute name that maps to metadata columns, check FILTER VALUES EXTRACTED section:
- If "final selection" shows: column_name - [column_name], sample values [VALUE]
- AND that VALUE exactly appears in the user's question
- THEN apply exact match filter: WHERE UPPER(column_name) = UPPER('VALUE')
- For multiple values use IN: WHERE UPPER(column_name) IN (UPPER('VAL1'), UPPER('VAL2'))
- Do NOT use filters from  if not in "user question"

2. CALCULATED FORMULAS HANDLING (CRITICAL)
**When calculating derived metrics (Gross Margin, Cost %, Margin %, etc.), DO NOT group by metric_type:**

CORRECT PATTERN:
```sql
SELECT 
    ledger, year, month,  -- Business dimensions only
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount_or_count ELSE 0 END) AS revenues,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS Post Reclass') THEN amount_or_count ELSE 0 END) AS expense_cogs,
    SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount_or_count ELSE 0 END) - 
    SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS Post Reclass') THEN amount_or_count ELSE 0 END) AS gross_margin
FROM table
WHERE conditions AND UPPER(metric_type) IN (UPPER('Revenues'), UPPER('COGS Post Reclass'))
GROUP BY ledger, year, month  -- Group by dimensions, NOT metric_type
```

WRONG PATTERN:
```sql
GROUP BY ledger, metric_type  -- Creates separate rows per metric_type, breaks formulas
```

**Only group by metric_type when user explicitly asks to see individual metric types as separate rows.**

3. METRICS & AGGREGATIONS
- Always use appropriate aggregation functions for numeric metrics: SUM, COUNT, AVG, MAX, MIN
- Even with specific entity filters (invoice #123, member ID 456), always aggregate unless user asks for "line items" or "individual records"
- Include time dimensions (month, quarter, year) when relevant to question
- Use business-friendly dimension names (therapeutic_class, service_type, age_group, state_name)

4. SELECT CLAUSE STRATEGY

**Simple Aggregates (no breakdown requested):**
- Show only the aggregated metric and essential time dimensions if specified
- Example: "What is total revenue?" → SELECT SUM(revenue) AS total_revenue
- Do NOT include unnecessary business dimensions or filter columns

**Calculations & Breakdowns (analysis BY dimensions):**
- Include ALL columns used in WHERE, GROUP BY, and calculations when relevant to question
- For calculations, show all components for transparency:
  * Percentage: Include numerator + denominator + percentage
  * Variance: Include original values + variance
  * Ratios: Include both components + ratio
- Example: "Cost per member by state" → SELECT state_name, total_cost, member_count, cost_per_member

5. MULTI-TABLE JOIN SYNTAX (when applicable)
- Use provided join clause exactly as specified
- Qualify all columns with table aliases
- Include all necessary tables in FROM/JOIN clauses
- Only join if question requires related data together; otherwise use separate queries

6. ATTRIBUTE-ONLY QUERIES
- If question asks only about attributes (age, name, type) without metrics, return relevant columns without aggregation

7. STRING FILTERING - CASE INSENSITIVE
- Always use UPPER() on both sides for text/string comparisons
- Example: WHERE UPPER(product_category) = UPPER('Specialty')

8. TOP N/BOTTOM N QUERIES WITH CONTEXT
-Show requested top/bottom N records with their individual values
-CRITICAL: Include the overall total as an additional COLUMN in each row (not as a separate row)
-Calculate and show percentage contribution: (individual value / overall total) × 100
Overall totals logic:
    -✅ Include overall total column for summable metrics: revenue, cost, expense, amount, count, volume, scripts, quantity, spend
    -❌ Exclude overall total column for derived metrics: margin %, ratios, rates, per-unit calculations, averages
-Use subquery in SELECT to show overall total alongside each individual record
-Column structure: [dimension] | [individual_value] | [overall_total] | [percentage_contribution]
-ALWAYS filter out blank/null records: WHERE column_name NOT IN ('-', 'BL')

9. COMPARISON QUERIES - SIDE-BY-SIDE FORMAT
- When comparing two related metrics (actual vs forecast, budget vs actual), use side-by-side columns
- For time-based comparisons (month-over-month, year-over-year), display time periods as adjacent columns with clear month/period names
- Example: Display "January_Revenue", "February_Revenue", "March_Revenue" side by side for easy comparison
- Include variance/difference columns when comparing metrics
- Prevents users from manually comparing separate rows

10. DATABRICKS SQL COMPATIBILITY
- Standard SQL functions: SUM, COUNT, AVG, MAX, MIN
- Date functions: date_trunc(), year(), month(), quarter()
- Conditional logic: CASE WHEN
- CTEs: WITH clauses for complex logic

11. FORMATTING & ORDERING
- Show whole numbers for metrics, round percentages to 4 decimal places
- Use ORDER BY only for date columns in descending order
- Use meaningful, business-relevant column names aligned with user's question

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

**OPTION 4: If RELEVANT - Multiple SQL Queries**
<multiple_sql>
<query1_title>[Brief descriptive title - max 8 words]</query1_title>
<query1>[First SQL query]</query1>
<query2_title>[Brief descriptive title - max 8 words]</query2_title>
<query2>[Second SQL query]</query2>
</multiple_sql>


**FOR SINGLE SQL QUERY:**
<sql>
[Your complete SQL query incorporating both original question and clarifications]
</sql>

**FOR MULTIPLE SQL QUERIES:**
If analysis requires multiple queries for better understanding:
<multiple_sql>
<query1_title>
[Brief descriptive title for first query - max 8 words]
</query1_title>
<query1>
[First SQL query here]
</query1>
<query2_title>
[Brief descriptive title for second query - max 8 words]
</query2_title>
<query2>
[Second SQL query here]
</query2>
</multiple_sql>

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
                    system_prompt="DATABRICKS SQL GENERATOR SYSTEM: You are an automated SQL query generation system for Databricks analytics infrastructure. Your ONLY job is to generate high-quality, syntactically correct SQL queries based on metadata, historical context, and clarification answers. You do NOT answer business questions, provide data analysis, or interpret results. You ONLY generate SQL code. This is a technical code generation task, not business analysis."
                )
                print('follow up sql response',llm_response)
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
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'topic_drift': False,
                                'new_question': False,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,
                                'query_count': len(sql_queries)
                            }
                    
                    raise ValueError("Empty or invalid multiple SQL queries in XML response")
                
                # Check for single SQL query
                match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if match:
                    sql_query = match.group(1).strip()
                    sql_query = sql_query.replace('`', '')  # Remove backticks
                    
                    if not sql_query:
                        raise ValueError("Empty SQL query in XML response")
                    
                    return {
                        'success': True,
                        'multiple_sql': False,
                        'topic_drift': False,
                        'new_question': False,
                        'sql_query': sql_query
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
