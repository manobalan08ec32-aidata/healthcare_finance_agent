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

class LLMRouterAgent:
    """Enhanced router agent with dataset selection and SQL generation"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        self.max_retries = 3
    
    async def search_metadata_sql(self, filter_list: List[str], user_segment: str) -> List[str]:
        """
        Search metadata for filter values using optimized SQL query
        Returns list of concatenated strings in format: "table_name.column_name: matched_values"
        """
        try:
            if not filter_list:
                return []
            
            # Build regex pattern with proper escaping for SQL
            all_patterns = []
            score_cases = []
            
            for term in filter_list:
                term_clean = term.strip().lower()
                escaped_exact = term_clean.replace("'", "\\'")
                
                whole_phrase = term_clean.replace(' ', '.*')
                escaped_whole = whole_phrase.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
                all_patterns.append(escaped_whole)
                
                score_cases.append(f"CASE WHEN lower(trim(exploded_value)) = '{escaped_exact}' THEN 100 ELSE 0 END")
                score_cases.append(f"CASE WHEN lower(trim(exploded_value)) RLIKE '(?i)({escaped_whole})' THEN 50 ELSE 0 END")
                
                if ' ' in term_clean:
                    first_space_idx = term_clean.index(' ')
                    first_part = term_clean[:first_space_idx]
                    second_part = term_clean[first_space_idx+1:].replace(' ', '.*')
                    
                    escaped_first = first_part.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
                    escaped_second = second_part.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
                    
                    all_patterns.extend([escaped_first, escaped_second])
                    score_cases.append(f"CASE WHEN lower(trim(exploded_value)) RLIKE '(?i)({escaped_first})' THEN 30 ELSE 0 END")
                    score_cases.append(f"CASE WHEN lower(trim(exploded_value)) RLIKE '(?i)({escaped_second})' THEN 10 ELSE 0 END")
            
                unique_patterns = list(set(all_patterns))
                regex_pattern = '|'.join(unique_patterns)
                score_calculation = ' + '.join(score_cases)
                
                query = f"""
                WITH matched_data AS (
                    SELECT
                        column_name,
                        trim(exploded_value) AS individual_value,
                        ({score_calculation}) AS value_score
                    FROM prd_optumrx_orxfdmprdsa.rag.distinct_values_metadata1
                    LATERAL VIEW explode(split(distinct_values, ',')) AS exploded_value
                    WHERE  lower(trim(exploded_value)) RLIKE '(?i)({regex_pattern})'
                ),
                scored_aggregated AS (
                    SELECT
                        column_name,
                        collect_list(individual_value) AS all_matched_values,
                        SUM(value_score) AS relevance_score
                    FROM matched_data
                    GROUP BY  column_name
                )
                SELECT
                    column_name,
                    concat_ws(', ', slice(all_matched_values, 1, 5)) AS matched_values,
                    relevance_score
                FROM scored_aggregated
                ORDER BY relevance_score DESC, column_name
                LIMIT 7
                """
            
            print(f"🔍 Executing filter values search for terms: {filter_list}")
            print(f"📊 SQL Query: {query}")
            
            # Execute the query using databricks client
            result_data = await self.db_client.execute_sql_async(query)
            print('results_data_filter',result_data)
            
            # Handle result_data - it's coming as list of dictionaries
            if not isinstance(result_data, list) or not result_data:
                print(f"❌ Filter values search failed: {result_data}")
                return []
                
            print(f"✅ Found {len(result_data)} filter value matches")
            
            # Group results by table and format for LLM usage
            table_groups = {}
            for row in result_data:
                column_name = row.get('column_name', '')
                matched_values = row.get('matched_values', '')

                if column_name not in table_groups:
                    table_groups[column_name] = []
                
                table_groups[column_name].append(f"{column_name}: {matched_values}")
            
            # Format as grouped strings for LLM
            concatenated_results = []
            for column_name, columns in table_groups.items():
                table_summary = f"Column: {column_name}\n" + "\n".join([f"  - {col}" for col in columns])
                concatenated_results.append(table_summary)
            print("results",concatenated_results)
            return concatenated_results
                
        except Exception as e:
            print(f"❌ Error in search_metadata_sql: {str(e)}")
            return []
        
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
            print('llm fix follow up dataset',result)
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
                state['selected_dataset'] = result.get('selected_dataset', [])
                state['selection_reasoning'] = metadata_result.get('selection_reasoning', '')
                selected_dataset = result.get('selected_dataset', [])
                
                # Store selected filter context if available
                selected_filter_context = result.get('selected_filter_context')
                if selected_filter_context:
                    state['selected_filter_context'] = selected_filter_context
                    print(f"🎯 Storing filter context for SQL generation: {selected_filter_context}")
                
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
                # Search metadata for filter values
                filter_metadata_results = await self.search_metadata_sql(filter_values, domain_selection)
                print('filter_metadata_results',filter_metadata_results)
                
                # print(f"📊 Found {len(filter_metadata_results)} filter metadata matches")
            
            # 2. Load domain-specific dataset config
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
            print('router result',selection_result)
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
                state['selected_dataset'] = selection_result.get('final_actual_tables', [])
                state['selected_filter_context'] = selection_result.get('selected_filter_context', '')
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
            else:
                # Initial SQL generation flow
                sql_result = await self._assess_and_generate_sql_async(self._extract_context(state), state)
            
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
            
            # Return comprehensive results
            return {
                'sql_result': final_result,
                'selected_dataset': selected_dataset or state.get('selected_dataset', []),
                'dataset_metadata': state.get('dataset_metadata', ''),
                'dataset_followup_question': None,
                'selection_reasoning': selection_reasoning,
                'functional_names': functional_names,
                'requires_clarification': False,
                'filter_metadata_results': filter_metadata_results
            }
        
        # Should not reach here, but handle gracefully
        return {
            'error': True,
            'error_message': "Unexpected flow in dataset selection"
        }    

    async def get_metadata(self, state: Dict, selected_dataset: list) -> Dict:
        """Extract metadata using vector search for the given selected_dataset"""
        try:
            current_question = state.get('rewritten_question', state.get('current_question', ''))

            # Determine embedding index based on domain selection
            domain_selection = state.get('domain_selection', '').strip()
            embedding_idx = None
            if domain_selection == 'PBM Network':
                embedding_idx = 'prd_optumrx_orxfdmprdsa.rag.column_embeddings_pbm_idx'
            elif domain_selection == 'Optum Pharmacy':
                embedding_idx = 'prd_optumrx_orxfdmprdsa.rag.column_embeddings_pharmacy_idx'
            else:
                # Default fallback if domain not recognized; could still use vector search fallback
                raise Exception("Domain Not Found")
    
            # Use selected_dataset as tables_list if available, else empty list
            tables_list = selected_dataset if isinstance(selected_dataset, list) else [selected_dataset] if selected_dataset else []
            print('tables_list', tables_list)
            
            cluster_results = await self.db_client.sp_vector_search_columns(
                query_text=current_question,
                tables_list=tables_list,
                num_results_per_table=20,
                index_name=embedding_idx,
                
            )
            print('vector results', cluster_results)
            # Group llm_context by table_name and build markdown metadata
            clusters_by_table = {}
            for cluster in cluster_results:
                table = cluster['table_name']
                if table not in clusters_by_table:
                    clusters_by_table[table] = []
                clusters_by_table[table].append(cluster['llm_context'])

            # Build final markdown metadata
            metadata = ""
            for table, clusters in clusters_by_table.items():
                metadata += f"\n## Table: {table}\n\n"
                for cluster_markdown in clusters:
                    metadata += cluster_markdown + "\n\n"

            return {
                'status': 'success',
                'metadata': metadata,
                'error': False
            }
            
        except Exception as e:
            print(f"❌ Databricks vector search failed: {str(e)}")
            return {
                'status': 'error',
                'metadata': '',
                'error': True,
                'error_message': f"Databricks vector search failed: {str(e)}"
            }

    def _direct_vector_search_dataset(self, user_question: str, num_results: int = 10) -> List[Dict]:
        """Vector search returning full results including actual table names"""
        
        try:
            # Get full search results with actual table names
            full_results = self.db_client.vector_search_tables(
                query_text=user_question,
                num_results=num_results
            )
            
            return full_results
            
        except Exception as e:
            raise Exception(f"Vector search failed: {str(e)}")


    async def _llm_dataset_selection(self, search_results: List[Dict], state: AgentState, filter_metadata_results: List[str] = None) -> Dict:
        """Enhanced LLM selection with validation, disambiguation handling, and filter-based selection"""
        
        user_question = state.get('current_question', state.get('original_question', ''))
        filter_values = state.get('filter_values', [])
        
        # Format filter metadata results for the prompt
        print('inside filter metadata', filter_metadata_results)
        filter_metadata_text = ""
        if filter_metadata_results:
            filter_metadata_text = "\n**FILTER METADATA FOUND:**\n"
            for result in filter_metadata_results:
                filter_metadata_text += f"- {result}\n"
        else:
            filter_metadata_text = "\n**FILTER METADATA:** No specific filter values found in metadata.\n"

        selection_prompt = f"""
            You are a Dataset Identifier Agent. You have FIVE sequential tasks to complete.

            CURRENT QUESTION: {user_question}
            EXTRACTED COLUMNS WITH FILTER VALUES: {filter_values}
            {filter_metadata_text}
            AVAILABLE DATASETS: {search_results}

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
                    * Skip validation for: "external", "internal", "retail", "mail order", "commercial", "medicare", "brand", "generic"
                    * These appear to be filter values, not missing attributes
                
                **BLOCK - Creative Substitutions**:
                    * Do NOT map unrelated terms (e.g., "ingredient fee" ≠ "expense")
                    * Do NOT assume domain knowledge not in metadata

                - Only mark as missing if NO reasonable Tier 1-3 mapping exists

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
              - User asks "claim-level analysis" → Claims has "useful_for": ["claim-level financial analysis"] → PREFER claims table
            * **PRECEDENCE**: not_useful_for OVERRIDES metrics/attributes availability - even if a table has the columns, exclude it if explicitly marked as not suitable
            
            - Verify time_grains match user needs (daily vs monthly vs quarterly)
            - Note: Keywords indicate relevance but suitability constraints are MANDATORY

            D. **COMPLEMENTARY ANALYSIS CHECK**:
            - **PURPOSE**: Identify if multiple datasets together provide more complete analysis than any single dataset
            - **LOOK FOR THESE PATTERNS**:
            * Primary metric in one dataset + dimensional attributes in another (e.g., "ledger revenue" + "therapy breakdown")
            * Different analytical perspectives on same business question (e.g., actuals view + claims view)
            * One dataset provides core data, another provides breakdown/segmentation
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

            E. **FILTER-BASED FROM FILTER VALUES FALLBACK CHECK**:
            - **LAST RESORT ONLY**: Use filter metadata ONLY when traditional validation (A-D) cannot provide clear selection
            - **TRIGGER CONDITIONS**: Multiple datasets pass all validations OR no clear winner from standard analysis
            - **APPLICATION**: If sections A-D do not provide a clear selection and filter values are available, you MUST always ask a follow-up question listing all columns and their tables that contain the filter value, and request the user to specify which column context to use for filtering. This applies even if only one column matches. Do not select confidently based on filter values alone.
            - In your follow-up, list the column names and their respective tables for the user to choose, e.g.:
                "I found the filter value in multiple columns:
                - Table: [table_name], Column: [column_name] (values include: ...)
                - Table: [table_name], Column: [column_name] (values include: ...)
                Which column should I use for filtering?"
            - Note: Table names are not present in the extracted filter output, so use metadata to infer and present the correct table/column mapping in your follow-up.


            F. **FINAL DECISION LOGIC**:
            - **STEP 1**: Check results from sections A through E
            - **STEP 2**: MANDATORY Decision order:
            * **FIRST**: Apply suitability constraints - eliminate datasets with "not_useful_for" matches
            * **SECOND**: Validate complete coverage (metrics + attributes) on remaining datasets
            * **THIRD**: Single complete dataset → SELECT IT
            
            * **NEW - FILTER DISAMBIGUATION CHECK**:
            - Before declaring success, check if filter values exist in multiple columns within the selected dataset
            - IF selected dataset has metric BUT filter appears in 2+ columns → RETURN "needs_disambiguation"

            * **FOURTH**: No single complete → SELECT MULTIPLE if complementary
            * **FIFTH**: Multiple complete → Use traditional tie-breakers (keywords, high_level_table)
            * **SIXTH**: Still tied → Apply filter-based selection as final tie-breaker
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
            - **FILTER-FALLBACK PATH**: When multiple datasets pass all validations, filter metadata provides clear tie-breaker

            **MISSING_ITEMS** IF:
            - Required metrics/attributes don't have Tier 1-3 matches in any dataset
            - No suitable alternatives available after all validation steps

            **REQUEST_FOLLOW_UP** IF:
            - **FILTER COLUMN AMBIGUITY (CHECK FIRST)**: Selected dataset has required metric BUT filter value appears in 2+ columns within that dataset - ALWAYS ask user to specify which column to use
            - Multiple datasets with conflicting contexts AND no clear preference from traditional validation
            - All metrics available in multiple datasets AND no traditional tie-breaker available
            - Filter values match more than one table (multi-table disambiguation): ALWAYS ask the user to specify which table to use for filtering.
            - Filter values match more than one column in the same table (multi-column disambiguation): ALWAYS ask the user to specify which column to use for filtering.
            - If filter values do not result in a single, perfect column match, you MUST ask the user for clarification, listing all possible columns and tables.
            - Any unresolvable ambiguity exists after full validation including filter fallback: ALWAYS request user clarification rather than guessing.

            ==============================
            ASSESSMENT FORMAT (BRIEF)
            ==============================

            **ASSESSMENT**: A:✓(no PHI) B:✓(metrics found) C:✓(suitability passed) D:✓(complementary) E:✓(filter fallback available) F:✓(clear selection)
            **DECISION**: PROCEED - [One sentence reasoning]

            Keep assessment ultra-brief:
            - Use checkmarks (✓) or X marks (❌) with 10 words max explanation in parentheses
            - **CRITICAL for C (suitability)**: ✓ means no "not_useful_for" conflicts, ❌ means blocked by suitability constraints
            - **CRITICAL for E (filter fallback)**: ✓ means filter metadata available for tie-breaking, ❌ means no filter guidance available
            - Each area gets: "A:✓(brief reason)" or "A:❌(brief issue)"
            - Decision reasoning maximum 15 words
            - No detailed explanations or bullet points in assessment
            - Save detailed analysis for JSON selection_reasoning field

            ==============================
            RESPONSE FORMAT
            ==============================

            IMPORTANT: Keep assessment ultra-brief (1-2 lines max), then output ONLY the JSON wrapped in <json> tags.

            Brief assessment, then JSON with status: "phi_found", "success", "missing_items", or "needs_disambiguation"
            Include required fields: final_actual_tables, functional_names, requires_clarification, selection_reasoning (2-3 lines max)
            Add status-specific fields: user_message (phi_found/missing_items), high_level_table_selected (success), clarification_question (needs_disambiguation)
            Add selected_filter_context: "only mention one column name strictly - col name - [actual_column_name] , sample values [sample value]" if column the selected from filter context metadata

               CRITICAL RULES:
                - Use TIER-based validation: Direct matches > Healthcare mapping > Math operations > Skip filter values
                - NEVER override missing attributes with high_level_table priority
                - Validate complete coverage FIRST, then apply tie-breakers
                - Use ONLY provided metadata, no external assumptions
                - When unsure, ASK CLARIFICATION rather than guess
                """
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                print("Sending selection prompt to LLM...",selection_prompt)
                llm_response = await self.db_client.call_claude_api_endpoint_async([
                    {"role": "user", "content": selection_prompt}
                ])
                print("Raw LLM response:", llm_response)
                
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
                
                elif status == "needs_disambiguation":
                    print(f"❓ Clarification needed - preparing follow-up question")
                    return {
                        'final_actual_tables': selection_result.get('final_actual_tables', []),
                        'functional_names': selection_result.get('functional_names', []),
                        'requires_clarification': True,
                        'clarification_question': selection_result.get('clarification_question'),
                        'candidate_actual_tables': selection_result.get('final_actual_tables', []),
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
                print('dataset follow up prompt',combined_prompt)
                llm_response = await self.db_client.call_claude_api_endpoint_async([
                    {"role": "user", "content": combined_prompt}
                ])
                print('dataset follow up response',llm_response)
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
                    selected_filter_context = result.get('selected_filter_context')
                    if selected_filter_context:
                        print(f"🎯 Column selection detected: {selected_filter_context}")
                    
                    return {
                        'dataset_followup_question': None,
                        'selected_dataset': result.get('final_actual_tables', []),
                        'requires_clarification': False,
                        'selection_reasoning': result.get('selection_reasoning', ''),
                        'selected_filter_context': selected_filter_context
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
        """Initial assessment: Generate SQL if clear, or ask follow-up questions if unclear"""
        
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        join_clause = state.get('join_clause', '')
        selected_filter_context = context.get('selected_filter_context')
        
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
        mandatory_columns_text = "\n".join(mandatory_columns_info) if mandatory_columns_info else "No mandatory columns required"
        
        # Format selected filter context for prompt
        filter_context_text = ""
        if selected_filter_context:
            filter_context_text = f"""
SELECTED FILTER CONTEXT Available for SQL generation:
    {selected_filter_context}

"""
        
        has_multiple_tables = len(selected_datasets) > 1 if isinstance(selected_datasets, list) else False
        
        assessment_prompt = f"""
        You are a highly skilled Healthcare Finance SQL analyst. You have TWO sequential tasks to complete.

        CURRENT QUESTION: {current_question}
        MULTIPLE TABLES AVAILABLE: {has_multiple_tables}
        JOIN INFORMATION: {join_clause if join_clause else "No join clause provided"}
        MANDATORY FILTER COLUMNS: {mandatory_columns_text}

        FILTER VALUES EXTRACTED:
        {filter_context_text}

        AVAILABLE METADATA: {dataset_metadata}

            ==============================
            TASK 1: BRIEF ASSESSMENT
            ==============================

            Analyze the user's question for clarity across these areas:

            A. METRIC DEFINITIONS: Clear business metrics exists or not 
            B. BUSINESS CONTEXT: Clear filtering/grouping vs unclear dimensions
            C. FORMULA REQUIREMENTS: Check if the formula exists in meta data for the question
            D. METADATA MAPPING: Terms map to columns vs unclear mapping and use FILTER VALUES EXTRACTED if needed.
            E. QUERY STRATEGY: Single query vs multiple queries needed

            **MULTI-QUERY SCENARIOS**:
            - Multi-table: Different data sources for complementary analysis
            - Complex single-table: Multiple analytical dimensions (trends + rankings)
            - Decision logic: JOIN if related data, SEPARATE if complementary analysis

            ==============================
            ASSESSMENT FORMAT (BRIEF)
            ==============================

            **ASSESSMENT**: A:✓(metrics mapped) B:✓(context clear) C:✓(formulas exist) D:✓(columns found) E:Single/Multi
            **DECISION**: PROCEED - [One sentence reasoning]

            Keep assessment ultra-brief:
            - Use checkmarks (✓) or X marks (❌) with 10 words max explanation in parentheses
            - Each area gets: "A:✓(brief reason)" or "A:❌(brief issue)"
            - Decision reasoning maximum 15 words
            - No detailed explanations in assessment
            - Save detailed analysis for follow-up if needed

            ==============================
            DECISION CRITERIA
            ==============================
            
            PROCEED TO TASK 2 (Generate SQL) IF:
            - All areas (A-F) are sufficiently clear
            - You can map user request to available columns with 100% confidence
            - Standard SQL functions can handle all requested calculations
            - No ambiguous business logic or custom formulas needed
            - Multi-table strategy is clear (join vs separate queries)

            REQUEST FOLLOW-UP IF: 
            - ANY of areas A-F have significant ambiguity
            - Metadata mapping is uncertain
            - Multi-table approach is unclear

            ==============================================
            TASK 2: HIGH-QUALITY DATABRICKS SQL GENERATION 
            ==============================================

            (Only execute if Task 1 assessment says "PROCEED")

            **MULTI-TABLE DECISION LOGIC:**
            1. **SINGLE QUERY WITH JOIN**: If question requires related data from multiple tables AND join clause exists
            2. **MULTIPLE SEPARATE QUERIES**: If question requires complementary analysis from different tables OR no join exists
            3. **SINGLE TABLE**: If question can be answered from one table

            **SQL GENERATION RULES:**

            0. **CRITICAL: CALCULATED FORMULAS HANDLING**
            - **CALCULATED METRICS DETECTION**: If the question asks for calculated formulas like Gross Margin, Cost %, Gross Margin %, etc., DO NOT use "GROUP BY metric_type"
            - **CORRECT PATTERN for calculated formulas for example**:
              ```sql
              SELECT 
                  ledger, year, month,  -- Include grouping dimensions
                  SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount_or_count ELSE 0 END) AS revenues,
                  SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS Post Reclass') THEN amount_or_count ELSE 0 END) AS expense_cogs,
                  SUM(CASE WHEN UPPER(metric_type) = UPPER('Revenues') THEN amount_or_count ELSE 0 END) - 
                  SUM(CASE WHEN UPPER(metric_type) = UPPER('COGS Post Reclass') THEN amount_or_count ELSE 0 END) AS gross_margin
              FROM table
              WHERE conditions AND UPPER(metric_type) IN (UPPER('Revenues'), UPPER('COGS Post Reclass'))
              GROUP BY ledger, year, month  -- Group by dimensions, NOT by metric_type
              ```
            - **WRONG PATTERN** (DO NOT USE for calculated formulas):
              ```sql
              GROUP BY ledger, metric_type  -- This will create separate rows per metric_type
              ```
            - **RULE**: Only group by metric_type when user explicitly asks to see individual metric types separately, NOT when calculating derived formulas

            1. MANDATORY FILTERS
            
            - CRITICAL: Review the MANDATORY FILTER COLUMNS section above - any columns marked as MANDATORY must be used as filters in your WHERE clause
            - CRITICAL: If the user's current question does not explicitly mention an attribute name or column, but filter values are available (as extracted from FILTER VALUES EXTRACTED), then the system must map those filter values to the appropriate metadata columns using the metadata schema.
            - If a table shows "Not Applicable" for mandatory columns, no additional mandatory filters are required for that table
            - If the metadata specifies mandatory filter columns, include them in the WHERE clause of your SQL query
            
            2. MEANINGFUL COLUMN NAMES
            - Use user-friendly, business-relevant column names that align with the user's question.
            - Generate a month-over-month comparison that clearly displays month names side by side in the output

            3a. SIMPLE AGGREGATE QUERIES - MINIMAL COLUMNS
            - **WHEN TO APPLY**: User query asks for simple totals/aggregates WITHOUT any breakdown attributes (e.g., "what is the revenue", "total cost", "sum of expenses")
            - **CRITICAL RULE**: If user query contains NO attributes/dimensions for breakdown, answer cumulatively with minimal columns
            - **GROUP BY RESTRICTION**: Do NOT use unnecessary business dimension columns in GROUP BY unless explicitly requested by user
            - **MANDATORY FILTERS ONLY- CRITICAL**: Apply only mandatory filters but do NOT include these filter columns in SELECT output
            - **SELECT STRATEGY**: Show only the requested metric and essential time dimensions if specified in query

        3b. CALCULATION QUERIES - SHOW ALL COMPONENTS
            - **WHEN TO APPLY**: User query involves calculations, breakdowns, or analysis BY specific dimensions
            - **MANDATORY**: Include ALL columns used in WHERE clause, GROUP BY clause, and calculations in the SELECT output when they are relevant to the user's question
            - **CALCULATION TRANSPARENCY**: If calculating a percentage, include the numerator, denominator, AND the percentage itself
            - **VARIANCE CALCULATIONS**: If calculating a variance, include the original values AND the variance
            - **BREAKDOWN QUERIES**: If user asks for breakdown BY product_category, include product_category in SELECT
            - **GROUPING QUERIES**: If user asks for grouping BY therapeutic_class, include therapeutic_class in SELECT
            - This ensures users can see the full context and verify how calculations were derived

            Examples:
            -- User asks: "Cost per member for Specialty products by state"
            SELECT 
                product_category,           -- Filter component (used in WHERE)
                state_name,                -- Grouping component (used in GROUP BY)  
                total_cost,                -- Numerator component
                member_count,              -- Denominator component
                total_cost / member_count AS cost_per_member  -- Final calculation
            FROM table 
            WHERE UPPER(product_category) = UPPER('Specialty')
            GROUP BY product_category, state_name

            4. METRICS & AGGREGATIONS
            - If the question includes metrics (e.g., costs, amounts, counts, totals, averages), use appropriate aggregation functions (SUM, COUNT, AVG) and include GROUP BY clauses with relevant business dimensions.
            - **CRITICAL**: When calculating derived formulas (Gross Margin, Cost %, etc.), DO NOT group by metric_type. Group by business dimensions only (ledger, year, month, product_category, etc.)
            - **ONLY group by metric_type** when user explicitly wants to see each metric type as separate rows, NOT when calculating cross-metric formulas
           

            5. MULTI-TABLE JOIN SYNTAX (when applicable):
            - Use the provided join clause exactly as specified
            - Ensure all selected columns are properly qualified with table aliases
            - Include all necessary tables in the FROM/JOIN clauses

            6. ATTRIBUTE-ONLY QUERIES
            - If the question asks only about attributes (e.g., member age, drug name, provider type) and does NOT request metrics, return only the relevant columns without aggregation.

            7. STRING FILTERING - CASE INSENSITIVE
            - When filtering on text/string columns, always use UPPER() function on BOTH sides for case-insensitive matching.
            - Example: WHERE UPPER(product_category) = UPPER('Specialty')

            8. **CRITICAL TOP N/BOTTOM N QUERIES WITH SELECTIVE TOTALS **
            - When the user asks for "top 10", "bottom 10", "highest", "lowest", etc., provide comprehensive context:
            * Always show the requested top/bottom N records with their individual values
            * **SELECTIVE TOTAL LOGIC**: Include overall totals ONLY for summable metrics using these rules:
              - ✅ INCLUDE TOTALS FOR: Absolute values that can be summed meaningfully
                * Keywords: revenue, cost, expense, amount, count, volume, scripts, units, quantity, spend, sales, fees, charges, claims
                * Pattern: Raw monetary amounts, counts, volumes (e.g., "total revenue", "script count", "claim volume")
              - ❌ EXCLUDE TOTALS FOR: Calculated/derived metrics that cannot be summed
                * Keywords: margin, percentage, ratio, rate, per-, average, mean, median, utilization, efficiency, productivity
                * Patterns: Any metric with %, ratios (e.g., "gross margin %"), per-unit calculations (e.g., "cost per member"), averages
            * When totals are appropriate, also show percentage contribution of top/bottom N to overall total
        
            - **CRITICAL for Top N queries**: Always filter out blank/null records to avoid blank entries in top results. Add WHERE clause with column_name NOT IN ('-', 'BL') to exclude backend null representations (where "-" and "BL" are used for null values).

            9. HEALTHCARE FINANCE BEST PRACTICES
            - Always include time dimensions (month, quarter, year) when relevant to the user's question.
            - Use business-friendly dimensions (e.g., therapeutic_class, service_type, age_group, state).

            10. DATABRICKS SQL COMPATIBILITY
            - Use standard SQL functions: SUM, COUNT, AVG, MAX, MIN
            - Use date functions: date_trunc(), year(), month(), quarter()
            - Use CASE WHEN for conditional logic
            - Use CTEs (WITH clauses) for complex logic

            11. FORMATTING
            - Show whole numbers for metrics and round percentages to four decimal places.
            - Use the ORDER BY clause only for date columns and use descending order.

            12. SIDE-BY-SIDE COMPARISON RULE
            - **CRITICAL**: When comparing actuals vs forecast, budget vs actual, or any two related metrics, generate SIDE-BY-SIDE format for easy interpretation
            - Example for actuals vs forecast: Display "Actual_Revenue" and "Forecast_Revenue" columns side by side with variance calculation
            - This prevents users from having to manually compare separate rows of actual and forecast data
            - Apply this rule wherever comparison analysis is requested (variance, difference, comparison queries)

            ==============================
            OUTPUT FORMATS
            ==============================
            IMPORTANT: You can use proper SQL formatting with line breaks and indentation inside the XML tags
            return ONLY the SQL query wrapped in XML tags. No other text, explanations, or formatting

            **FOR SINGLE SQL QUERY:**
            If TASK 1 says PROCEED → Execute TASK 2:
            <sql>
            [Your complete SQL query here]
            </sql>

            **FOR MULTIPLE SQL QUERIES:**
            If TASK 1 says PROCEED and requires multiple queries:
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

            If TASK 1 says REQUEST FOLLOW-UP, return ONLY the followup wrapped in XML tags. No other text, explanations, or formatting

            ==============================
            DETERMINISTIC FOLLOW-UP RULES
            ==============================

            STEP 1: Identify which specific areas from your 5-area assessment (A-E) were marked as "❌ Needs Clarification"

            STEP 2: Generate questions ONLY for the unclear areas identified in Step 1

            FOR ANY FOLLOW-UP SITUATION:
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

            FOLLOW-UP CONSTRAINTS:
            - Ask questions ONLY for areas marked as "❌ Needs Clarification" in your assessment
            - If only 1 area unclear → ask only 1 question
            - If 2+ areas unclear → maximum 2 questions for most critical areas
            - Never ask about areas already marked as "✓ Clear"
            - Each main question gets exactly 2 sub-bullets: one for available data, one for suggestion
            - Use actual column names from metadata
            - Keep all bullets short and actionable

            ==============================
            EXECUTION INSTRUCTION
            ==============================

            Show your brief assessment first, then provide SQL or follow-up:

            1. Complete brief TASK 1 assessment (A-F checkmarks)
            2. Make clear PROCEED/FOLLOW-UP decision in one sentence
            3. If PROCEED: Execute TASK 2 with SQL generation
            4. If FOLLOW-UP: Ask targeted questions for unclear areas only

            You only get ONE opportunity for follow-up, so be decisive in your assessment.
        """

        for attempt in range(self.max_retries):
            try:
                print('sql prompt', assessment_prompt)
                llm_response = await self.db_client.call_claude_api_endpoint_async([
                    {"role": "user", "content": assessment_prompt}
                ])
                print('sql llm response', llm_response)
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
                                'query_count': len(sql_queries)
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
                        'sql_query': sql_query
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
                        'sql_followup_questions': followup_text
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
            'error': f"SQL assessment failed after {self.max_retries} attempts due to Model errors"
        }

    async def _generate_sql_with_followup_async(self, context: Dict, sql_followup_question: str, sql_followup_answer: str, state: Dict) -> Dict[str, Any]:
        """Generate SQL using original question + follow-up Q&A with multiple SQL support async"""
        
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        join_clause = state.get('join_clause', '')
        selected_filter_context = context.get('selected_filter_context')
        
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
SELECTED FILTER CONTEXT (from user clarification):
{selected_filter_context}

**CRITICAL**: Use this specific column and values for filtering in your SQL query if needed.
"""
        
        has_multiple_tables = len(selected_datasets) > 1 if isinstance(selected_datasets, list) else False

        followup_sql_prompt = f"""
        You are a highly skilled Healthcare Finance SQL analyst. This is PHASE 2 of a two-phase process.
        Your task is to generate a **high-quality Databricks SQL query** based on the user's question

        ORIGINAL USER QUESTION: {current_question}
        **AVAILABLE METADATA**: {dataset_metadata}
        MULTIPLE TABLES AVAILABLE: {has_multiple_tables}
        JOIN INFORMATION: {join_clause if join_clause else "No join clause provided"}
        MANDATORY FILTER COLUMNS: {mandatory_columns_text}

        FILTER VALUES EXTRACTED::
        {filter_context_text}

        ==============================
        FOLLOW-UP CLARIFICATION RECEIVED
        ==============================
        
        YOUR PREVIOUS QUESTION: {sql_followup_question}
        USER'S CLARIFICATION: {sql_followup_answer}

        ==============================
        MULTI-TABLE AND COMPLEX QUERY ANALYSIS
        ==============================
            
        Before generating SQL, assess if this requires multiple queries for better user understanding:

        **SCENARIOS REQUIRING MULTIPLE QUERIES**:
        
        MULTI-TABLE PATTERNS:
        - "Ledger revenue + breakdown by drug" → financial table + claim table
        - "Budget metrics + client breakdown" → forecast table + transaction table
        
        SINGLE-TABLE COMPLEX PATTERNS:
        - "Membership trends AND top drugs by revenue" → trend query + ranking query
        - "Summary metrics AND detailed breakdown" → summary query + detail query
        - "Current performance AND comparative analysis" → performance query + comparison query

        **DECISION CRITERIA**:
        - Multiple distinct analytical purposes in one question
        - Different aggregation levels (summary + detail)
        - Combines trends with rankings or comparisons
        - Question contains "AND" connecting different analysis types
        - Would result in overly complex single query
        
        **MULTI-QUERY DECISION LOGIC**:
        1. **SINGLE QUERY WITH JOIN**: Simple analysis requiring related data from multiple tables with join
        2. **MULTIPLE QUERIES - MULTI-TABLE**: Complementary analysis from different tables OR no join exists
        3. **MULTIPLE QUERIES - COMPLEX SINGLE-TABLE**: Complex question with multiple analytical dimensions
        4. **SINGLE QUERY**: Simple, focused questions with one analytical dimension

        ==============================
        FINAL SQL GENERATION TASK
        ==============================
        
        Now generate a high-quality Databricks SQL query using:
        1. The ORIGINAL user question as the primary requirement
        2. The USER'S CLARIFICATION to resolve any ambiguities
        3. Available metadata for column mapping
        4. Multi-table strategy assessment (single vs multiple queries)
        5. All SQL generation best practices below

        IMPORTANT: No more questions allowed - this is the definitive SQL generation using all available information.

        ==============================
        CRITICAL SQL GENERATION RULES
        ==============================
        1. MANDATORY FILTERS
        - CRITICAL: Review the MANDATORY FILTER COLUMNS section above - any columns marked as MANDATORY must be used as filters in your WHERE clause
        - CRITICAL: If the user's current question does not explicitly mention an attribute name or column, but filter values are available (as extracted from FILTER VALUES EXTRACTED), then the system must map those filter values to the appropriate metadata columns using the metadata schema.
        - If a table shows "Not Applicable" for mandatory columns, no additional mandatory filters are required for that table
        - If the metadata specifies mandatory filter columns, include them in the WHERE clause of your SQL query
        
        2. MEANINGFUL COLUMN NAMES
        - Use user-friendly, business-relevant column names that align with the user's question.
        - Generate a month-over-month comparison that clearly displays month names side by side in the output

        3a. SIMPLE AGGREGATE QUERIES - MINIMAL COLUMNS
        - **WHEN TO APPLY**: User query asks for simple totals/aggregates WITHOUT any breakdown attributes (e.g., "what is the revenue", "total cost", "sum of expenses")
        - **CRITICAL RULE**: If user query contains NO attributes/dimensions for breakdown, answer cumulatively with minimal columns
        - **GROUP BY RESTRICTION**: Do NOT use unnecessary business dimension columns in GROUP BY unless explicitly requested by user
        - **MANDATORY FILTERS ONLY - CRITICAL**: Apply only mandatory filters but do NOT include these filter columns in SELECT output
        - **SELECT STRATEGY**: Show only the requested metric and essential time dimensions if specified in query

        3b. CALCULATION QUERIES - SHOW ALL COMPONENTS
        - **WHEN TO APPLY**: User query involves calculations, breakdowns, or analysis BY specific dimensions
        - **MANDATORY**: Include ALL columns used in WHERE clause, GROUP BY clause, and calculations in the SELECT output when they are relevant to the user's question
        - **CALCULATION TRANSPARENCY**: If calculating a percentage, include the numerator, denominator, AND the percentage itself
        - **VARIANCE CALCULATIONS**: If calculating a variance, include the original values AND the variance
        - **BREAKDOWN QUERIES**: If user asks for breakdown BY product_category, include product_category in SELECT
        - **GROUPING QUERIES**: If user asks for grouping BY therapeutic_class, include therapeutic_class in SELECT
        - This ensures users can see the full context and verify how calculations were derived

        Example:
        -- User asks: "Cost per member for Specialty products by state"
        SELECT 
            product_category,           -- Filter component (used in WHERE)
            state_name,                -- Grouping component (used in GROUP BY)  
            total_cost,                -- Numerator component
            member_count,              -- Denominator component
            total_cost / member_count AS cost_per_member  -- Final calculation
        FROM table 
        WHERE UPPER(product_category) = UPPER('Specialty')
        GROUP BY product_category, state_name

        4. METRICS & AGGREGATIONS
        - If the question includes metrics (e.g., costs, amounts, counts, totals, averages), use appropriate aggregation functions (SUM, COUNT, AVG) and include GROUP BY clauses with relevant business dimensions.
    

        5. MULTI-TABLE JOIN SYNTAX (when applicable):
        - Use the provided join clause exactly as specified
        - Ensure all selected columns are properly qualified with table aliases
        - Include all necessary tables in the FROM/JOIN clauses

        6. ATTRIBUTE-ONLY QUERIES
        - If the question asks only about attributes (e.g., member age, drug name, provider type) and does NOT request metrics, return only the relevant columns without aggregation.

        7. STRING FILTERING - CASE INSENSITIVE
        - When filtering on text/string columns, always use UPPER() function on BOTH sides for case-insensitive matching.
        - Example: WHERE UPPER(product_category) = UPPER('Specialty')

        8. **CRITICAL TOP N/BOTTOM N QUERIES WITH SELECTIVE TOTALS **
            - When the user asks for "top 10", "bottom 10", "highest", "lowest", etc., provide comprehensive context:
            * Always show the requested top/bottom N records with their individual values
            * **SELECTIVE TOTAL LOGIC**: Include overall totals ONLY for summable metrics using these rules:
              - ✅ INCLUDE TOTALS FOR: Absolute values that can be summed meaningfully
                * Keywords: revenue, cost, expense, amount, count, volume, scripts, units, quantity, spend, sales, fees, charges, claims
                * Pattern: Raw monetary amounts, counts, volumes (e.g., "total revenue", "script count", "claim volume")
              - ❌ EXCLUDE TOTALS FOR: Calculated/derived metrics that cannot be summed
                * Keywords: margin, percentage, ratio, rate, per-, average, mean, median, utilization, efficiency, productivity
                * Patterns: Any metric with %, ratios (e.g., "gross margin %"), per-unit calculations (e.g., "cost per member"), averages
            * When totals are appropriate, also show percentage contribution of top/bottom N to overall total
        
        9. HEALTHCARE FINANCE BEST PRACTICES
        - Always include time dimensions (month, quarter, year) when relevant to the user's question.
        - Use business-friendly dimensions (e.g., therapeutic_class, service_type, age_group, state).

        10. DATABRICKS SQL COMPATIBILITY
        - Use standard SQL functions: SUM, COUNT, AVG, MAX, MIN
        - Use date functions: date_trunc(), year(), month(), quarter()
        - Use CASE WHEN for conditional logic
        - Use CTEs (WITH clauses) for complex logic

        11. FORMATTING
        - Show whole numbers for metrics and round percentages to four decimal places.
        - Use the ORDER BY clause only for date columns and use descending order.

        12. SIDE-BY-SIDE COMPARISON RULE
        - **CRITICAL**: When comparing actuals vs forecast, budget vs actual, or any two related metrics, generate SIDE-BY-SIDE format for easy interpretation
        - Example for actuals vs forecast: Display "Actual_Revenue" and "Forecast_Revenue" columns side by side with variance calculation
        - This prevents users from having to manually compare separate rows of actual and forecast data
        - Apply this rule wherever comparison analysis is requested (variance, difference, comparison queries)

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

        Generate the definitive SQL query now.
        """

        for attempt in range(self.max_retries):
            try:
                print('followup sql',followup_sql_prompt)
                llm_response = await self.db_client.call_claude_api_endpoint_async([
                    {"role": "user", "content": followup_sql_prompt}
                ])
                print('followup sql llm response',llm_response)
                
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
                        'sql_query': sql_query
                    }
                else:
                    raise ValueError("No SQL found in XML tags")
            
            except Exception as e:
                print(f"❌ SQL generation with follow-up attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying SQL generation with follow-up... (Attempt {attempt + 1}/{self.max_retries})")
                    await asyncio.sleep(2 ** attempt)
        
        return {
            'success': False,
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
                print('sql fix prompt', fix_prompt)

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
