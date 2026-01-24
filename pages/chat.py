from typing import Dict, List, Optional, Any
import pandas as pd
import json
import asyncio
import re
import os
from datetime import datetime
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from core.logger import setup_logger, log_with_user_context

# Initialize logger for this module
logger = setup_logger(__name__)

class NarrativeAgent:
    """Narrative synthesis agent: generates intelligent narratives from SQL results"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        self.powerbi_metadata_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'powerbi_report_metadata.json')
        
        # Report name to URL mapping
        self.report_url_mapping = {
            "Snapshot": "https://app.powerbi.com/groups/me/apps/84236afb-d260-4b84-84df-aace54fdf6f5/reports/f1228200-f00a-4c39-8604-f6240b896309/35206c2d6dd05a569e8e?experience=power-bi",
            "Oracle Client LOB": "https://app.powerbi.com/groups/me/apps/84236afb-d260-4b84-84df-aace54fdf6f5/reports/f1228200-f00a-4c39-8604-f6240b896309/505fa5d9534fad70e718?experience=power-bi",
            "Oracle Client Trend": "https://app.powerbi.com/groups/me/apps/84236afb-d260-4b84-84df-aace54fdf6f5/reports/f1228200-f00a-4c39-8604-f6240b896309/7fa55631de2387f4d1e3?experience=power-bi",
            "Trend": "https://app.powerbi.com/groups/me/apps/84236afb-d260-4b84-84df-aace54fdf6f5/reports/f1228200-f00a-4c39-8604-f6240b896309/be2aa0dd0c87ee944bda?experience=power-bi",
            "LOB": "https://app.powerbi.com/groups/me/apps/84236afb-d260-4b84-84df-aace54fdf6f5/reports/f1228200-f00a-4c39-8604-f6240b896309/9d1c3701b2eae5e54414?experience=power-bi"

        }
    
    def _log(self, level: str, message: str, state: AgentState = None, **extra):
        """Helper method to log with user context from state"""
        user_email = state.get('user_email') if state else None
        session_id = state.get('session_id') if state else None
        log_with_user_context(logger, level, f"[Narrative] {message}", user_email, session_id, **extra)
    
    async def synthesize_narrative(self, state: AgentState) -> Dict[str, Any]:
        """Generate narrative from SQL results and find matching Power BI reports in parallel"""
        
        self._log('info', "Starting narrative synthesis from SQL results", state)
        
        try:
            # Get SQL results from state
            sql_result = state.get('sql_result', {})
            current_question = state.get('rewritten_question', '')
            selected_dataset = state.get('selected_dataset', [])  # Get selected table/dataset
            
            if not sql_result or not sql_result.get('success', False):
                self._log('warning', "No valid SQL results found for narrative", state)
                return {
                    'success': False,
                    'error': 'No valid SQL results found for narrative synthesis'
                }
            
            # Extract SQL queries for Power BI matching
            sql_queries = []
            if sql_result.get('multiple_results', False):
                query_results = sql_result.get('query_results', [])
                sql_queries = [qr.get('sql_query', '') for qr in query_results]
            else:
                sql_queries = [sql_result.get('sql_query', '')]
            
            # Check if SQL results have data (0 records = skip Power BI matching)
            has_data = False
            if sql_result.get('multiple_results', False):
                query_results = sql_result.get('query_results', [])
                has_data = any(len(qr.get('data', [])) > 0 for qr in query_results)
            else:
                data = sql_result.get('query_results', [])
                has_data = len(data) > 0
            
            print(f"📊 SQL results has_data: {has_data}")
            
            # Run narrative synthesis and Power BI matching in parallel
            print(f"🚀 Running narrative synthesis and Power BI matching in parallel")
            
            narrative_task = None
            powerbi_task = None
            
            # Check if single or multiple SQL results for narrative
            if sql_result.get('multiple_results', False):
                # Handle multiple SQL queries
                query_results = sql_result.get('query_results', [])
                narrative_task = self._synthesize_multiple_narratives_async(query_results, current_question)
            else:
                # Handle single SQL query
                sql_query = sql_result.get('sql_query', '')
                data = sql_result.get('query_results', [])
                
                # Get current conversation memory from state
                conversation_memory = state.get('conversation_memory', {
                    'dimensions': {},
                    'analysis_context': {
                        'current_analysis_type': None,
                        'analysis_history': []
                    }
                })
                
                narrative_task = self._synthesize_single_narrative_async(data, current_question, sql_query, conversation_memory)
            
            # Create Power BI matching task ONLY if SQL results have data
            if has_data:
                print(f"✅ SQL results have data - creating Power BI matching task")
                powerbi_task = self._match_powerbi_report_async(sql_queries[0] if sql_queries else '', selected_dataset)
            else:
                print(f"⏭️ SQL results have 0 records - skipping Power BI matching")
                # Create a dummy coroutine that returns no match
                async def no_powerbi_match():
                    return {
                        'report_found': False,
                        'report_name': None,
                        'report_url': None,
                        'match_type': 'none',
                        'reason': 'No data in SQL results',
                        'filters_to_apply': None
                    }
                powerbi_task = no_powerbi_match()
            
            # Execute both tasks in parallel
            narrative_result, powerbi_result = await asyncio.gather(narrative_task, powerbi_task)
            
            print(f"✅ Parallel execution complete")
            print(f"   Narrative: {'Success' if narrative_result.get('success') else 'Failed'}")
            print(f"   Power BI: {'Match found' if powerbi_result.get('report_found') else 'No match'}")
            
            if narrative_result['success']:
                # Update state with narrative results
                if sql_result.get('multiple_results', False):
                    # Update each query result with its narrative
                    updated_query_results = []
                    narratives = narrative_result.get('narratives', [])
                    
                    for idx, query_result in enumerate(query_results):
                        updated_query = query_result.copy()
                        if idx < len(narratives):
                            updated_query['narrative'] = narratives[idx]
                        updated_query_results.append(updated_query)
                    
                    # Update the sql_result with narratives
                    state['sql_result']['query_results'] = updated_query_results
                else:
                    # Update single result with narrative
                    state['sql_result']['narrative'] = narrative_result.get('narrative', '')
                    
                    # 🆕 UPDATE CONVERSATION MEMORY (Programmatic merge)
                    if narrative_result.get('memory'):
                        current_memory = state.get('conversation_memory', {
                            'dimensions': {},
                            'analysis_context': {
                                'current_analysis_type': None,
                                'analysis_history': []
                            }
                        })
                        
                        # Merge LLM memory with existing memory
                        merged_memory = self._merge_memory_intelligently(current_memory, narrative_result['memory'])
                        state['conversation_memory'] = merged_memory
                        print(f"✅ Conversation memory updated with {len(merged_memory.get('dimensions', {}))} dimensions")
                
                # Merge Power BI results into return output
                return {
                    'success': True,
                    'narrative_complete': True,
                    'chart': narrative_result.get('chart', {'render': False, 'reason': 'No chart data'}),
                    'report_found': powerbi_result.get('report_found', False),
                    'report_url': powerbi_result.get('report_url'),
                    'report_filter': powerbi_result.get('filters_to_apply'),
                    'report_name': powerbi_result.get('report_name'),
                    'tab_name': None,  # No longer returned by LLM
                    'match_type': powerbi_result.get('match_type'),
                    'report_reason': powerbi_result.get('reason'),
                    'unsupported_filters': None,  # No longer returned by LLM
                    'report_warnings': []  # No longer returned by LLM
                }
            else:
                return {
                    'success': False,
                    'error': narrative_result.get('error', 'Narrative synthesis failed'),
                    'report_found': False
                }
                
        except Exception as e:
            error_msg = f"Narrative agent failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'report_found': False
            }
    
    async def _match_powerbi_report_async(self, sql_query: str, selected_dataset: List[str]) -> Dict[str, Any]:
        """Match SQL query to Power BI report with retries
        
        Args:
            sql_query: SQL query string to match
            selected_dataset: List of selected table names from state - used ONLY to filter metadata, not passed to LLM
        """
        
        print(f"\n🔍 Power BI Report Matcher: Analyzing SQL query")
        print(f"   Selected dataset (for metadata filtering): {selected_dataset}")
        
        if not sql_query or sql_query.strip() == '':
            print("⚠️ Empty SQL query, skipping Power BI matching")
            return {
                'report_found': False,
                'report_name': None,
                'report_url': None,
                'match_type': 'none',
                'reason': 'No SQL query provided',
                'filters_to_apply': None
            }
        
        try:
            # Load full Power BI metadata
            with open(self.powerbi_metadata_path, 'r') as f:
                full_metadata = json.load(f)
            
            print(f"✅ Loaded Power BI metadata from {self.powerbi_metadata_path}")
            
            # Filter metadata to only the selected dataset's table (if dataset specified)
            # This reduces the metadata sent to LLM, but we don't mention the dataset name in the prompt
            filtered_metadata = {}
            if selected_dataset and len(selected_dataset) > 0:
                # Extract table name from selected_dataset (handle list format)
                dataset_name = selected_dataset[0] if isinstance(selected_dataset, list) else selected_dataset
                
                print(f"🔍 Filtering metadata for dataset: {dataset_name}")
                
                # Search for matching table in metadata
                for table_key, table_data in full_metadata.items():
                    # Check if the dataset name appears in the table key
                    # e.g., "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast" contains "ledger_actual_vs_forecast"
                    if dataset_name.lower() in table_key.lower():
                        filtered_metadata[table_key] = table_data
                        print(f"✅ Found matching table: {table_key}")
                        break
                
                if not filtered_metadata:
                    print(f"⚠️ Dataset '{dataset_name}' not found in Power BI metadata - skipping Power BI matching")
                    return {
                        'report_found': False,
                        'report_name': None,
                        'report_url': None,
                        'match_type': 'none',
                        'reason': f'Dataset {dataset_name} not available in Power BI reports',
                        'filters_to_apply': None
                    }
            else:
                # No dataset specified, use full metadata
                print("ℹ️ No dataset specified, using full metadata")
                filtered_metadata = full_metadata
            
        except Exception as e:
            print(f"❌ Failed to load Power BI metadata: {str(e)}")
            return {
                'report_found': False,
                'report_name': None,
                'report_url': None,
                'match_type': 'none',
                'reason': f'Metadata load failed: {str(e)}',
                'filters_to_apply': None
            }
        
        # Build matching prompt with filtered metadata
        # NOTE: We send both SQL query AND filtered metadata to LLM, but NOT the dataset name
        matching_prompt = f"""You are a Power BI Tab Matcher. Your job is to analyze a SQL query and determine which Power BI report tab(s) can answer the same question.

---

METADATA STRUCTURE:

The metadata contains:
- common.measures: All available metrics (shared across all tabs)
- common.synonyms: Maps SQL column/value names to Power BI slicer names
- report.report_name: The Power BI report name
- report.tabs[]: List of tabs, each containing:
  - tab_name: Name of the tab
  - slicer_mode: "comparison" (actuals vs forecast side-by-side) OR "single_view" (one metric over time)
  - granularity: "product_lob" (product level) OR "client_lob" (client level)
  - tab_description: What the tab shows and its limitations
  - slicers: Available filters with their possible values
  - use_when: When this tab is the right choice
  - do_not_use_when: When to avoid this tab
---
INSTRUCTIONS (Follow these steps in order):

STEP 1: PARSE SQL
Extract from the SQL query:
- Columns in SELECT (what is being retrieved)
- Aggregations used (SUM, COUNT, AVG)
- Filters in WHERE clause (column = value pairs)
- Columns in GROUP BY (how data is grouped) - SAVE THESE for filters_to_apply
- ORDER BY presence (indicates trend/time-series)

STEP 2: MAP USING SYNONYMS
Translate SQL column and value names to Power BI slicer names:
- Use common.synonyms to map column names (e.g., client_id → Oracle Customer ID, line_of_business → customer (Line of Business))
- Use common.synonyms to map values (e.g., HDP → Home Delivery)
- Map GROUP BY columns to slicer names if they exist in Power BI slicers
- Keep track of mapped filters AND mapped GROUP BY columns for output

STEP 3: DETERMINE INTENT
Identify two key characteristics:

A) COMPARISON vs SINGLE VIEW:

COMPARISON indicators (slicer_mode = "comparison"):
- SQL SELECT has both actuals AND forecast columns
- SQL has columns like: actuals, forecast_5_7, variance, gaap_revenue, forecast_revenue
- SQL WHERE has multiple ledger values: ledger IN ('GAAP', '5+7')
- SQL compares two forecast versions: forecast_5_7, forecast_9_3
- Question asks about "variance", "vs", "compare", "against"

SINGLE VIEW indicators (slicer_mode = "single_view"):
- SQL SELECT has only actuals OR only one forecast version
- SQL WHERE filters to single ledger: ledger = 'GAAP'
- SQL has time-series pattern: GROUP BY month ORDER BY month
- Question asks about "trend", "over time", "monthly", "pattern", "historical"

B) CLIENT vs PRODUCT level:

CLIENT level (granularity = "client_lob"):
- SQL has client_id, oracle_cust_id, or client in SELECT, WHERE, or GROUP BY
- Question mentions specific client names or asks "which clients"

PRODUCT level (granularity = "product_lob"):
- SQL has product_category, product_cd in SELECT, WHERE, or GROUP BY
- SQL has NO client columns
- Question mentions Home Delivery, Specialty, PBM

STEP 4: FILTER TABS BY MODE AND GRANULARITY
Based on Step 3, narrow down candidate tabs:
- If COMPARISON + CLIENT → look for slicer_mode="comparison" AND granularity="client_lob"
- If COMPARISON + PRODUCT → look for slicer_mode="comparison" AND granularity="product_lob"
- If SINGLE VIEW + CLIENT → look for slicer_mode="single_view" AND granularity="client_lob"
- If SINGLE VIEW + PRODUCT → look for slicer_mode="single_view" AND granularity="product_lob"

STEP 5: VALIDATE AGAINST use_when AND do_not_use_when
For each candidate tab:
- Read use_when: Does the SQL intent match?
- Read do_not_use_when: Is there any exclusion that applies?
- Eliminate tabs where do_not_use_when matches

STEP 6: CHECK SLICER COVERAGE
For each remaining candidate tab:
- Check if each SQL WHERE filter has a matching slicer in the tab
- Check if the filter VALUE exists in the slicer's possible values
- Flag any filters that cannot be applied (unsupported)
- Prefer tabs with higher slicer coverage

STEP 7: SELECT BEST TAB(S)
- Rank tabs by: slicer coverage > use_when fit > description match
- Return best matching tab(s)
- If multiple tabs are equally relevant, return all
- If no tab matches well, return closest match with explanation

---

SPECIAL CASES:

Snapshot vs LOB (both are product-level comparison):
- Snapshot: Has forecast 1 + forecast 2 → use when comparing TWO forecast versions
- LOB: Has Ledger 1 + Ledger 2 → use when comparing actuals vs ONE forecast

Oracle Client LOB vs Oracle Client Trend (both are client-level):
- Oracle Client LOB: Has Ledger 1 + Ledger 2 → use for client actuals vs forecast comparison
- Oracle Client Trend: Has only Ledger 1 (GAAP) → use for client trend WITHOUT forecast comparison

---

SQL QUERY:
```sql
{sql_query}
```

AVAILABLE POWER BI REPORTS:
```json
{json.dumps(filtered_metadata, indent=2)}
```

OUTPUT FORMAT (return ONLY valid JSON, no markdown):
CRITICAL: Return ONLY the JSON output. Do NOT include any analysis, explanation, or step-by-step reasoning in your response. Your response must start with {{ and end with }}. No text before or after the JSON.


{{
  "report_found": true,
  "report_name": "Actual Report Name",
  "tab_name": ["Tab Name 1", "Tab Name 2"]
  "match_type": "exact" or "partial" or "none",
  "reason": "One sentence explaining why this tab was selected based on SQL intent and slicer match",
  "filters_to_apply": "slicer1 - value1, slicer2 - value2" or null,
  "unsupported_filters": "slicer1 - value1" or null
}}

RULES FOR tab_name:
- Return list of matching tab names from metadata
- Order by relevance (best match first)
- Include partial matches
- Return empty list [] if no tabs match

RULES FOR filters_to_apply:
- Extract filters from SQL WHERE clause
- ALSO extract GROUP BY columns (if they exist in Power BI slicers)
- Map to Power BI slicer names using synonyms
- Format for WHERE filters: "slicer_name - value, slicer_name - value"
- Format for GROUP BY columns: "group_by - slicer_name" (e.g., "group_by - customer (Line of Business)")
- Use friendly date format with RANGES when multiple consecutive months/years:
  * Single month: "Month - Jul"
  * Multiple consecutive months: "Month - Jul to Oct" (NOT "Month - Jul, Month - Aug, Month - Sep, Month - Oct")
  * Multiple non-consecutive months: "Month - Jan,Mar,May"
  * Single year: "Year - 2025"
  * Multiple consecutive years: "Year - 2024 to 2025" (NOT "Year - 2024, Year - 2025")
- Only include filters that ARE supported by matched tab(s)
- Example with GROUP BY: "Year - 2025, Month - Jul to Oct, ledger - GAAP, group_by - customer (Line of Business)"

RULES FOR unsupported_filters:
- Include filters from SQL that matched tabs do NOT support
- Use Power BI slicer names (mapped via synonyms)
- Set to null if all filters are supported

RULES FOR reason:
- Keep concise, one sentence
- Mention what matched: slicer_mode, granularity, key slicers
- If partial match, explain what is missing


"""
        
        # Retry logic (3 attempts)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"🔄 Power BI matching attempt {attempt + 1}/{max_retries}")
                
                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": matching_prompt}],
                    max_tokens=1000,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt="You are a Power BI tab matching system. Analyze SQL queries and match them to the most appropriate Power BI report tabs. Return ONLY valid JSON output - no explanations, no analysis steps, no markdown. Your response must be pure JSON starting with { and ending with }."
                )
                
                print(f"📥 Power BI matcher response received",llm_response)
                
                # Log LLM output - actual response truncated to 500 chars (no state in this internal method)
                self._log('info', "LLM response received from Power BI matcher", None,
                         llm_response=llm_response,
                         attempt=attempt + 1)
                
                # Parse JSON response
                # Remove markdown code blocks if present
                cleaned_response = llm_response.strip()
                if cleaned_response.startswith("```json"):
                    cleaned_response = cleaned_response[7:]
                if cleaned_response.startswith("```"):
                    cleaned_response = cleaned_response[3:]
                if cleaned_response.endswith("```"):
                    cleaned_response = cleaned_response[:-3]
                cleaned_response = cleaned_response.strip()
                
                result = json.loads(cleaned_response)
                
                # Validate required fields
                if 'report_found' not in result:
                    raise ValueError("Response missing 'report_found' field")
                
                report_found = result.get('report_found', False)
                base_report_name = result.get('report_name')  # e.g., "OptumRX Financial Reporting Hub"
                tab_names = result.get('tab_name', [])  # e.g., ["Snapshot", "Oracle Client LOB"]
                
                print(f"✅ Power BI matching successful (attempt {attempt + 1})")
                print(f"   Report found: {report_found}")
                print(f"   Base report name from LLM: {base_report_name}")
                print(f"   Tab names from LLM: {tab_names}")
                
                # Process report name and URL mapping
                final_report_name = None
                report_url = None
                
                if report_found and base_report_name and tab_names and len(tab_names) > 0:
                    # Take the first (best) tab match
                    first_tab_name = tab_names[0]
                    
                    # Use tab_name to find URL from mapping
                    report_url = self.report_url_mapping.get(first_tab_name)
                    
                    # Concatenate report_name + " - " + tab_name for display
                    final_report_name = f"{base_report_name} - {first_tab_name}"
                    
                    if not report_url:
                        print(f"⚠️ No URL mapping found for tab: {first_tab_name}")
                        print(f"   Available mappings: {list(self.report_url_mapping.keys())}")
                    else:
                        print(f"✅ Concatenated report name: {final_report_name}")
                        print(f"✅ Mapped tab '{first_tab_name}' to URL: {report_url}")
                
                # Return formatted result (tab_name NOT included - only used internally for URL lookup)
                return {
                    'report_found': report_found,
                    'report_name': final_report_name,  # Concatenated: "OptumRX Financial Reporting Hub - Snapshot"
                    'report_url': report_url,
                    'match_type': result.get('match_type', 'none'),
                    'reason': result.get('reason', ''),
                    'filters_to_apply': result.get('filters_to_apply')
                }
                
            except Exception as e:
                print(f"⚠️ Power BI matching attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    print(f"❌ All Power BI matching attempts failed")
                    return {
                        'report_found': False,
                        'report_name': None,
                        'report_url': None,
                        'match_type': 'none',
                        'reason': f'Matching failed after {max_retries} attempts: {str(e)}',
                        'filters_to_apply': None
                    }
    
    async def _synthesize_multiple_narratives_async(self, query_results: List[Dict], question: str) -> Dict[str, Any]:
        """Generate narratives for multiple SQL queries"""
        
        print(f"📊 Synthesizing narratives for {len(query_results)} queries")
        
        narratives = []
        
        for idx, query_result in enumerate(query_results):
            title = query_result.get('title', f'Query {idx+1}')
            sql_query = query_result.get('sql_query', '')
            data = query_result.get('data', [])
            
            print(f"  🔄 Processing narrative for: {title}")
            
            narrative_result = await self._synthesize_single_narrative_async(data, f"{question} - {title}", sql_query)
            
            if narrative_result['success']:
                narratives.append(narrative_result['narrative'])
            else:
                narratives.append(f"Narrative generation failed for {title}")
        
        return {
            'success': True,
            'narratives': narratives
        }
    
    def _merge_memory_intelligently(self, current_memory: Dict, llm_memory: Dict) -> Dict:
        """
        Programmatically merge LLM's new memory with existing memory
        Maintains FIFO logic for last 5 dimensions
        """
        
        # Initialize result with existing memory structure
        merged = {
            'dimensions': {},
            'analysis_context': llm_memory.get('analysis_context', current_memory.get('analysis_context', {
                'current_analysis_type': None,
                'analysis_history': []
            }))
        }
        
        current_dimensions = current_memory.get('dimensions', {})
        llm_dimensions = llm_memory.get('dimensions', {})
        
        print(f"🔄 Merging memory:")
        print(f"   Current dimensions keys: {list(current_dimensions.keys())}")
        print(f"   Current dimensions values: {current_dimensions}")
        print(f"   LLM dimensions keys: {list(llm_dimensions.keys())}")
        print(f"   LLM dimensions values: {llm_dimensions}")
        
        # Step 1: Start with current dimensions (preserve order and ALL dimensions)
        for dim_key, dim_values in current_dimensions.items():
            merged['dimensions'][dim_key] = dim_values.copy() if isinstance(dim_values, list) else [dim_values]
            print(f"   → Preserved existing dimension: {dim_key} with {len(merged['dimensions'][dim_key])} values")
        
        # Step 2: Process LLM dimensions
        for llm_dim_key, llm_dim_values in llm_dimensions.items():
            
            if llm_dim_key in merged['dimensions']:
                # RULE B: Dimension exists - MERGE values (new first, then old, top 5)
                existing_values = merged['dimensions'][llm_dim_key]
                new_values = llm_dim_values if isinstance(llm_dim_values, list) else [llm_dim_values]
                
                # Merge: new values first, then old values (avoiding duplicates)
                merged_values = new_values.copy()
                for val in existing_values:
                    if val not in merged_values:
                        merged_values.append(val)
                    if len(merged_values) >= 5:
                        break
                
                merged['dimensions'][llm_dim_key] = merged_values[:5]
                print(f"   ✓ Merged {llm_dim_key}: {len(new_values)} new + {len(existing_values)} old → {len(merged['dimensions'][llm_dim_key])} total")
                print(f"     Final values for {llm_dim_key}: {merged['dimensions'][llm_dim_key]}")
            
            else:
                # RULE C: New dimension - Add at end, apply FIFO if needed
                total_dims = len(merged['dimensions'])
                
                if total_dims < 5:
                    # Just add the new dimension
                    merged['dimensions'][llm_dim_key] = llm_dim_values if isinstance(llm_dim_values, list) else [llm_dim_values]
                    print(f"   ✓ Added new dimension {llm_dim_key} (now {total_dims + 1} dimensions)")
                    print(f"     Values for {llm_dim_key}: {merged['dimensions'][llm_dim_key]}")
                
                else:
                    # FIFO: Delete oldest (first), add new (last)
                    oldest_key = list(merged['dimensions'].keys())[0]
                    del merged['dimensions'][oldest_key]
                    merged['dimensions'][llm_dim_key] = llm_dim_values if isinstance(llm_dim_values, list) else [llm_dim_values]
                    print(f"   ✓ FIFO: Deleted {oldest_key}, added {llm_dim_key} (maintained 5 dimensions)")
                    print(f"     Values for {llm_dim_key}: {merged['dimensions'][llm_dim_key]}")
        
        print(f"   📋 Final merged dimensions: {list(merged['dimensions'].keys())}")
        print(f"   📋 Final merged memory: {merged}")
        return merged
    
    async def _synthesize_single_narrative_async(self, sql_data: List[Dict], question: str, sql_query: str, conversation_memory: Dict = None) -> Dict[str, Any]:
        """Generate narrative for a single SQL query result with LLM-managed memory"""
        
        print(f"🚀 _synthesize_single_narrative_async CALLED with {len(sql_data) if sql_data else 0} data rows")
        
        if not sql_data:
            return {
                'success': True,
                'narrative': "No data was found matching your query criteria.",
                'memory': conversation_memory or {
                    'dimensions': {},
                    'analysis_context': {
                        'current_analysis_type': None,
                        'analysis_history': []
                    }
                },
                'chart': {'render': False, 'reason': 'No data available'}
            }
        
        try:
            # Convert SQL data to DataFrame for cleaner handling
            df = self._json_to_dataframe(sql_data)
            
            if df.empty:
                return {
                    'success': True,
                    'narrative': "No data was found matching your query criteria.",
                    'memory': conversation_memory or {
                        'dimensions': {},
                        'analysis_context': {
                            'current_analysis_type': None,
                            'analysis_history': []
                        }
                    },
                    'chart': {'render': False, 'reason': 'No data available'}
                }

            row_count = len(df)
            columns = list(df.columns)
            column_count = len(columns)
            print('total count',row_count)
            print('column_count',column_count)
            total_count = row_count * column_count
            print('total count',total_count)
            if total_count > 10000:
                return {
                    'success': True,
                    'narrative': "Too many records to synthesize.",
                    'memory': conversation_memory or {
                        'dimensions': {},
                        'analysis_context': {
                            'current_analysis_type': None,
                            'analysis_history': []
                        }
                    },
                    'chart': {'render': False, 'reason': 'Too many records for visualization'}
                }

            # Calculate distinct value counts for categorical columns (for chart eligibility)
            distinct_value_summary = {}
            categorical_columns = []
            for col in df.columns:
                if df[col].dtype == 'object':  # String/categorical columns
                    distinct_count = df[col].nunique()
                    distinct_value_summary[col] = distinct_count
                    categorical_columns.append(col)

            # Format for LLM prompt
            distinct_summary_str = ", ".join([f"{col}: {count} distinct values" for col, count in distinct_value_summary.items()])
            if not distinct_summary_str:
                distinct_summary_str = "No categorical columns detected"

            print(f"📊 Distinct value summary: {distinct_summary_str}")
            print(f"📊 Categorical columns count: {len(categorical_columns)}")

            # Convert conversation memory to formatted string for LLM
            memory_context = json.dumps(conversation_memory, indent=2)

            # Convert DataFrame to clean string representation for LLM
            df_string = df.to_string(index=False, max_rows=5000)

            synthesis_prompt = f"""
You are a Healthcare Finance Data Analyst. Create concise, meaningful insights from SQL results AND determine if a chart would be valuable.

USER QUESTION: "{question}"
DATA: {row_count} rows, {', '.join(columns)}
**Query Output**:
{df_string}

**ADAPTIVE ANALYSIS RULES**:

**FOR LIMITED DATA (1-2 rows OR insufficient data for patterns)**:
- Report only factual data points and direct answers to the question
- NO forced analysis categories when data doesn't support them
- Keep insights concise and practical

**FOR RICH DATA (3+ rows with meaningful patterns)**:
- Apply relevant analysis types: TREND, PATTERN, ANOMALY, DRIVER, COMPARATIVE
- Only include analysis types that the data actually supports
- Prioritize business significance

**FOR DATASETS WITH 10+ RECORDS AND NOTABLE FINDINGS**:
- If you identify significant anomalies, outliers, or bright spots in the data
- Leverage your broad knowledge to provide contextual interpretation
- Add "Information from Web Knowledge:" section with 2-3 lines of relevant real-world context
- Only include this if you can provide meaningful context from your training data
- Examples: industry trends, known factors affecting the metric, regulatory changes, seasonal patterns
- If no relevant context available, omit this section entirely

**OUTPUT GUIDELINES**:
- Bullets: ≤20 words each, focus on business value
- Use exact data names, auto-scale: ≥1B→x.xB, ≥1M→x.xM, ≥1K→x.xK
- CRITICAL: Use canonical business names for readability while maintaining context
    • Use "Drug MOUNJARO" instead of "drug_name MOUNJARO"
    • Use "Client MDOVA" instead of "client_name MDOVA"
    • Use "revenue per script" instead of "revenue_per_script"
    • First mention: include attribute context (e.g., "Drug MOUNJARO"), subsequent mentions in same bullet: just value ("MOUNJARO")
- Use ONLY names/values present in the dataset - never invent or modify names
- Maintain attribute context for query generation while improving readability
- Summary: 1-2 sentences for limited data, 2-3 lines for rich data
- Skip empty or obvious statements

**OUTPUT FORMAT**:
<insights>
• Key finding 1 (only include meaningful insights)
• Key finding 2 (if data supports additional insights)
• Additional insights only if data is rich enough

Information from LLM's Knowledge:
[Only include if 10+ records AND you have relevant real-world context about the findings. Otherwise omit this line entirely. 2-3 lines maximum explaining relevant industry context, trends, or factors that help interpret the anomalies/patterns found in the data.]
</insights>

<chart>
{{
  "render": true|false,
  "chart_type": "line"|"bar"|"bar_grouped",
  "title": "[Clear title - max 60 chars]",
  "x_column": "[TIME column for line, CATEGORY column for bar/bar_grouped]",
  "y_column": "[single NUMERIC column]" or ["actuals_col", "forecast_col"] for bar_grouped,
  "group_column": "[single categorical column for legend/grouping in line charts]" or null,
  "top_n": 10 or null,
  "reason": "[1-2 sentences why this chart type or why no chart]"
}}
</chart>

**CHART RULES (STRICT - CONSERVATIVE APPROACH)**:

CATEGORICAL COLUMN INFO (use this for eligibility check):
{distinct_summary_str}
Number of categorical columns: {len(categorical_columns)}

ELIGIBILITY CHECK (must pass ALL - if ANY fails, set render=false):
1. Only ONE categorical column allowed (if 2+ categorical columns exist → render=false)
2. Clear numeric column for Y-axis
3. No "OVERALL_TOTAL", "Total", "Grand Total" mixed with other category values → render=false

TOP 10 RULE:
- If the categorical column has >10 distinct values → set "top_n": 10
- This will sort by Y-axis amount descending and show only top 10

CHART TYPE SELECTION (3 types supported):
| Pattern | Chart Type | When |
|---------|------------|------|
| Time series trend | line | X=time (month/year), Y=amount, group_column=category (optional) |
| Category ranking | bar | X=category (drug_name, client), Y=amount (top N comparison) |
| Actuals vs Forecast | bar_grouped | ONLY when comparing 2 numeric columns side by side |

LINE CHART RULES:
- x_column = time column (month, year, date, quarter)
- y_column = single amount column (pick based on user question)
- group_column = single categorical column for multiple colored lines (if ≤10 values), else null
- Use when question asks about "trend", "over time", "monthly", "yearly"

BAR CHART RULES (Category Rankings):
- x_column = categorical column (drug_name, client_name, product, LOB)
- y_column = single amount column (revenue, count, etc.)
- group_column = null (not used for bar charts)
- Use for "top 10", "by drug", "by client", "ranking", "comparison by category"
- If >10 categories, set top_n: 10

BAR_GROUPED RULES (Actuals vs Forecast ONLY):
- x_column = categorical column (product, LOB, etc.)
- y_column = ["actuals_column", "forecast_column"] (must be exactly 2 numeric columns)
- Use ONLY when question explicitly asks to compare actuals vs forecast
- Categorical column must have ≤10 distinct values

COLUMN SELECTION:
- x_column: Time column (for line) OR category column (for bar_grouped)
- y_column: Pick ONE amount column based on user question - MUST be exact column name from: {columns}
- group_column: The single categorical column (if exists and ≤10 values), else null

WHEN TO SKIP CHART (render=false):
- No time column AND not actuals vs forecast comparison
- 2+ categorical columns (too complex)
- "OVERALL_TOTAL" or similar totals mixed with data
- 0-1 rows of data
- No clear numeric column
- Question is lookup/definitional ("what is X")

Return ONLY the XML with <insights> and <chart> tags, nothing else.
"""

            # Call LLM with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # print('synthesis_prompt',synthesis_prompt)
                    response_text = await self.db_client.call_claude_api_endpoint_async([
                    {"role": "user", "content": synthesis_prompt}
                    ])
        
                    print('LLM response received', response_text)
                    
                    # Log LLM output - actual response truncated to 500 chars (no state in this internal method)
                    self._log('info', "LLM response received from narrative synthesizer", None,
                             llm_response=response_text,
                             attempt=attempt + 1)
                    
                    # Process the response
                    if not response_text:
                        raise ValueError("Empty response from LLM")

                    # Extract insights
                    insights_match = re.search(r'<insights>(.*?)</insights>', response_text, re.DOTALL)
                    insights = insights_match.group(1).strip() if insights_match else ""

                    if not insights or len(insights) < 10:
                        raise ValueError("Empty or insufficient insights in XML response")

                    # Extract chart specification
                    chart_match = re.search(r'<chart>(.*?)</chart>', response_text, re.DOTALL)
                    chart_data = {'render': False, 'reason': 'No chart specification in response'}

                    if chart_match:
                        try:
                            chart_json_str = chart_match.group(1).strip()
                            # Clean up potential formatting issues
                            chart_json_str = chart_json_str.replace('```json', '').replace('```', '').strip()
                            chart_data = json.loads(chart_json_str)

                            # Validate chart specification if render is true
                            if chart_data.get('render', False):
                                x_col = chart_data.get('x_column')
                                y_col = chart_data.get('y_column')
                                group_col = chart_data.get('group_column')
                                valid_cols = list(df.columns)

                                # Validate x_column
                                if x_col and x_col not in valid_cols:
                                    print(f"⚠️ Invalid x_column '{x_col}'. Available: {valid_cols}")
                                    chart_data['render'] = False
                                    chart_data['reason'] = f"Invalid x_column: {x_col}"

                                # Validate y_column (can be string or list for bar_grouped)
                                elif y_col:
                                    if isinstance(y_col, str):
                                        if y_col not in valid_cols:
                                            print(f"⚠️ Invalid y_column '{y_col}'. Available: {valid_cols}")
                                            chart_data['render'] = False
                                            chart_data['reason'] = f"Invalid y_column: {y_col}"
                                    elif isinstance(y_col, list):
                                        invalid_cols = [c for c in y_col if c not in valid_cols]
                                        if invalid_cols:
                                            print(f"⚠️ Invalid y_columns: {invalid_cols}. Available: {valid_cols}")
                                            chart_data['render'] = False
                                            chart_data['reason'] = f"Invalid y_columns: {invalid_cols}"

                                # Validate group_column (optional)
                                if chart_data.get('render', False) and group_col:
                                    if group_col not in valid_cols:
                                        print(f"⚠️ Invalid group_column '{group_col}'. Available: {valid_cols}")
                                        chart_data['render'] = False
                                        chart_data['reason'] = f"Invalid group_column: {group_col}"

                                if chart_data.get('render', False):
                                    print(f"✅ Chart: render=True, type={chart_data.get('chart_type')}, x={x_col}, y={y_col}, group={group_col}, top_n={chart_data.get('top_n')}")
                            else:
                                print(f"ℹ️ Chart: render=False, reason={chart_data.get('reason', 'Not specified')}")
                        except Exception as e:
                            print(f"⚠️ Chart extraction failed: {e}")
                            chart_data = {'render': False, 'reason': f'Chart parsing error: {str(e)}'}
                    else:
                        print("ℹ️ No <chart> block found in LLM response")

                    # Return insights with chart data
                    return {
                        'success': True,
                        'narrative': insights,
                        'memory': conversation_memory or {
                            'dimensions': {},
                            'analysis_context': {
                                'current_analysis_type': None,
                                'analysis_history': []
                            }
                        },
                        'chart': chart_data
                    }

                except Exception as e:
                    print(f"❌ Narrative synthesis attempt {attempt + 1} failed: {str(e)}")
                    
                    if attempt < max_retries - 1:
                        print(f"🔄 Retrying narrative synthesis... (Attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(2 ** attempt)
            
            return {
                'success': False,
                'error': f"Narrative synthesis failed after {max_retries} attempts due to Model errors"
            }
            
        except Exception as e:
            error_msg = f"Failed to synthesize narrative: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }
    
    def _json_to_dataframe(self, json_data) -> pd.DataFrame:
        """Convert JSON response to pandas DataFrame with proper numeric formatting"""
        
        try:
            if isinstance(json_data, list):
                # List of dictionaries
                df = pd.DataFrame(json_data)
            elif isinstance(json_data, dict):
                if 'data' in json_data:
                    df = pd.DataFrame(json_data['data'])
                elif 'result' in json_data:
                    df = pd.DataFrame(json_data['result'])
                else:
                    # Single record
                    df = pd.DataFrame([json_data])
            else:
                # Fallback
                df = pd.DataFrame()
            
            # Apply numeric formatting to all columns
            if not df.empty:
                df = self._format_dataframe_values(df)
            
            print(f"📊 Created DataFrame: {df.shape[0]} rows x {df.shape[1]} columns")
            return df
            
        except Exception as e:
            print(f"❌ DataFrame conversion failed: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error
    
    def _format_dataframe_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format DataFrame values with proper numeric handling"""
        
        def format_value(val):
            # Handle ISO date strings
            if isinstance(val, str) and 'T' in val and val.endswith('Z'):
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
                    return dt.strftime('%Y-%m-%d')
                except Exception:
                    return val
            
            # PRIORITY 1: Handle scientific notation FIRST (before any other checks)
            elif isinstance(val, str) and ('E' in val.upper() or 'e' in val):
                try:
                    numeric_val = float(val)
                    # Always format as integer with commas for scientific notation
                    return f"{int(round(numeric_val)):,}"
                except Exception:
                    return str(val)
            
            # PRIORITY 2: Detect client codes and other non-numeric strings that contain letters
            elif isinstance(val, str) and not val.replace('.', '').replace('-', '').replace(',', '').replace('E', '').replace('e', '').isdigit():
                # If string contains any letters (except E/e which we handled above), keep as-is
                return val
            
            # Handle year values (don't format as decimals)
            elif isinstance(val, str) and val.isdigit() and len(val) == 4:
                return val  # Keep year as is
            
            # Handle client codes that might be pure numbers but should stay as strings
            elif isinstance(val, str) and val.isdigit() and len(val) <= 6:
                # For short numeric strings (likely IDs/codes), keep as-is without decimals
                return val
                
            # Handle month values (1-12, don't add decimals)
            elif isinstance(val, str) and val.replace('.', '').isdigit():
                try:
                    numeric_val = float(val)
                    if 1 <= numeric_val <= 12 and '.' not in val:  # Month value
                        return str(int(numeric_val))
                    elif numeric_val > 1000:  # Large numbers like revenue
                        return f"{numeric_val:,.3f}"
                    else:
                        return f"{numeric_val:.3f}"
                except Exception:
                    return val
            
            # Handle large numeric strings (like your revenue values) - ONLY for actual decimals
            elif isinstance(val, str) and val.replace('.', '').replace('-', '').isdigit() and '.' in val:
                try:
                    numeric_val = float(val)
                    if numeric_val > 1000:  # Format large numbers with commas
                        return f"{numeric_val:,.3f}"
                    else:
                        return f"{numeric_val:.3f}"
                except Exception:
                    return val
            
            # Handle already formatted dollar amounts (with commas)
            elif isinstance(val, str) and val.startswith('$') and ',' in val:
                return val
            # Handle already formatted strings (with commas)
            elif isinstance(val, str) and ',' in val:
                return val
            # Handle actual numeric types (int, float)
            elif isinstance(val, (int, float)):
                try:
                    # Check if it's a year value (4-digit number in reasonable year range)
                    if val == int(val) and 1900 <= int(val) <= 2100:
                        return str(int(val))  # Keep years without commas
                    
                    if abs(val) >= 1000:  # Large numbers
                        if val == int(val):  # If it's a whole number
                            return f"{int(val):,}"  # Format as integer with commas
                        else:
                            return f"{val:,.3f}"  # Format with 3 decimals
                    else:
                        if val == int(val):  # If it's a whole number
                            return str(int(val))  # No decimals for small whole numbers
                        else:
                            return f"{val:.3f}"
                except Exception:
                    return str(val)
            elif pd.api.types.is_numeric_dtype(type(val)):
                try:
                    if pd.notna(val):
                        # Check if it's a year value (4-digit number in reasonable year range)
                        if float(val) == int(float(val)) and 1900 <= int(float(val)) <= 2100:
                            return str(int(float(val)))  # Keep years without commas
                        
                        if abs(float(val)) >= 1000:
                            if float(val) == int(float(val)):  # Whole number
                                return f"{int(float(val)):,}"
                            else:
                                return f"{float(val):,.3f}"
                        else:
                            if float(val) == int(float(val)):  # Whole number
                                return str(int(float(val)))
                            else:
                                return f"{float(val):.3f}"
                    else:
                        return ""
                except Exception:
                    return str(val)
            else:
                return val
        
        # Apply formatting to all columns
        for col in df.columns:
            df[col] = df[col].apply(format_value)
        
        return df
