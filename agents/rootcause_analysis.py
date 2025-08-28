from typing import Dict, List, Optional, Tuple
import json
import asyncio
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class RootCauseAnalysisAgent:
    """Root Cause Analysis Agent that processes knowledge graphs and generates insights"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
    def analyze_root_cause(self, state: AgentState) -> Dict[str, any]:
        """Main function to perform root cause analysis with enhanced knowledge graph selection"""
        
        user_question = state.get('current_question', state.get('original_question', ''))
        question_type = state.get('question_type', ' ')

        
        if not user_question:
            raise Exception("No user question found in state")
        
        print(f"🔍 Starting enhanced root cause analysis for: '{user_question}'")
        
        # 1. Get knowledge graph with LLM selection and merging
        knowledge_graph = self._get_knowledge_graph(user_question)
        
        if not knowledge_graph:
            raise Exception("No knowledge graph found for root cause analysis")

        # 2. Generate ALL SQL queries at once
        all_sql_queries = self._generate_all_sql_queries(knowledge_graph, user_question,question_type)
        #3. Execute queries in parallel with retry mechanism
        print('all queries',all_sql_queries)
        query_results = self._execute_queries_parallel_with_retry(all_sql_queries, knowledge_graph)

        # # 4. Generate individual insights for each query result (with chart recommendations)
        individual_insights = self._generate_individual_insights(query_results, user_question)        
        # # 5. Create consolidated query details for UI display
        consolidated_query_details = self._create_consolidated_query_details(query_results, individual_insights)
        
        # 6. Create SQL summary for UI display
        sql_summary = self._create_sql_summary(query_results)
        
        # 7. Consolidate all SQL-related information (keeping your existing structure)
        consolidated_sql_info = self._consolidate_sql_information(query_results, individual_insights, user_question)
        
        # 8. Generate consolidated narrative using all insights
        consolidated_narrative = self._generate_consolidated_narrative(
            individual_insights, 
            query_results, 
            user_question
        )
        print('consolidated_narrative',consolidated_narrative)
        
        return {
            'knowledge_graph': knowledge_graph,
            'query_results': query_results,
            'individual_insights': individual_insights,
            'narrative_response': consolidated_narrative,
            'analysis_complete': True,
            
            # NEW: UI-friendly structured data
            'consolidated_query_details': consolidated_query_details,  # Each SQL with all its info
            'sql_summary': sql_summary,  # Just SQL + purpose summary
            
            # Consolidated SQL information for future use (keeping your structure)
            'sql_queries': consolidated_sql_info['all_sql_queries'],
            'query_results_consolidated': consolidated_sql_info['all_query_results'],
            'sql_execution_summary': consolidated_sql_info['execution_summary'],
            'failed_queries': consolidated_sql_info['failed_queries'],
            'successful_queries': consolidated_sql_info['successful_queries'],
            'total_queries_executed': consolidated_sql_info['total_queries'],
            'total_rows_returned': consolidated_sql_info['total_rows']
        }
    
    def _create_consolidated_query_details(self, query_results: List[Dict], individual_insights: List[Dict]) -> List[Dict]:
        """Create consolidated details for each query with all associated information"""
        
        print(f"🔄 Creating consolidated query details for {len(query_results)} queries")
        
        consolidated_details = []
        
        for i, result in enumerate(query_results):
            # Find matching insight
            matching_insight = None
            if i < len(individual_insights):
                matching_insight = individual_insights[i]
            
            # Create consolidated entry
            query_detail = {
                # Basic query info
                'query_id': result.get('query_id'),
                'table': result.get('table'),
                'purpose': result.get('purpose'),
                'sql': result.get('sql'),
                'narrative': result.get('narrative', ''),
                
                # Execution info
                'success': result.get('success', False),
                'execution_time': result.get('execution_time', 'N/A'),
                'retry_count': result.get('retry_count', 0),
                'error': result.get('error') if not result.get('success') else None,
                
                # Results data
                'sql_results': result.get('result', []),
                'row_count': result.get('row_count', 0),
                'has_data': result.get('row_count', 0) > 0,
                
                # Insights and visualization
                'insight_text': matching_insight.get('insight_text', '') if matching_insight else '',
                'chart_recommendation': matching_insight.get('chart_recommendation', {}) if matching_insight else {},
                
                # Metadata
                'kg_row_id': result.get('kg_row_id'),
                'kg_row_index': result.get('kg_row_index'),
                'timestamp': datetime.now().isoformat()
            }
            
            consolidated_details.append(query_detail)
    
        print(f"✅ Created {len(consolidated_details)} consolidated query details")
        return consolidated_details

    def _create_sql_summary(self, query_results: List[Dict]) -> Dict[str, str]:
        """Create a simple summary of all SQL queries with their purposes"""
        
        print(f"📋 Creating SQL summary for {len(query_results)} queries")
        
        sql_summary = {}
        
        for result in query_results:
            query_id = result.get('query_id')
            sql_summary[query_id] = {
                'sql': result.get('sql', ''),
                'purpose': result.get('purpose', ''),
                'table': result.get('table', ''),
                'success': result.get('success', False),
                'row_count': result.get('row_count', 0)
            }
        
        print(f"✅ Created SQL summary with {len(sql_summary)} entries")
        return sql_summary

    def _consolidate_sql_information(self, query_results: List[Dict], individual_insights: List[Dict], user_question: str) -> Dict:
        """Consolidate all SQL queries, results, and execution metadata - keeping your existing structure"""
        
        print(f"🔍 Consolidating SQL information from {len(query_results)} queries")
        
        all_sql_queries = {}
        all_query_results = {}
        successful_queries = {}
        failed_queries = {}
        execution_summary = []
        total_rows = 0
        total_queries = 0
        
        for i, result in enumerate(query_results):
            query_id = result.get('query_id', f'query_{i}')
            sql_query = result.get('sql', '')
            success = result.get('success', False)
            
            # Store queries with full context
            all_sql_queries[query_id] = {
                'query_id': query_id,
                'table': result.get('table', 'unknown'),
                'purpose': result.get('purpose', ''),
                'sql': sql_query,
                'user_question': user_question,
                'timestamp': datetime.now().isoformat(),
                'retry_count': result.get('retry_count', 0)
            }
            total_queries += 1
            
            # Store results with metadata
            query_data = result.get('result', [])
            result_count = len(query_data) if query_data else 0
            total_rows += result_count
            
            all_query_results[query_id] = {
                'query_id': query_id,
                'table': result.get('table', 'unknown'),
                'results': query_data,
                'row_count': result_count,
                'has_data': result_count > 0,
                'insight': individual_insights[i].get('insight', '') if i < len(individual_insights) else '',
                'timestamp': datetime.now().isoformat()
            }
            
            # Categorize successful vs failed queries
            if success:
                successful_queries[query_id] = {
                    'query_id': query_id,
                    'table': result.get('table', 'unknown'),
                    'row_count': result_count,
                    'sql': sql_query,
                    'retry_count': result.get('retry_count', 0)
                }
            else:
                failed_queries[query_id] = {
                    'query_id': query_id,
                    'table': result.get('table', 'unknown'),
                    'error': result.get('error', 'Unknown error'),
                    'sql': sql_query,
                    'retry_history': result.get('retry_history', [])
                }
            
            # Create execution summary for this query
            query_summary = {
                'query_id': query_id,
                'table': result.get('table', 'unknown'),
                'success': success,
                'rows_returned': result_count,
                'retry_count': result.get('retry_count', 0),
                'execution_time': result.get('execution_time', 'N/A'),
                'error': result.get('error') if not success else None
            }
            execution_summary.append(query_summary)
        
        consolidated_info = {
            'all_sql_queries': all_sql_queries,
            'all_query_results': all_query_results,
            'successful_queries': successful_queries,
            'failed_queries': failed_queries,
            'execution_summary': execution_summary,
            'total_queries': total_queries,
            'total_rows': total_rows,
            'successful_query_count': len(successful_queries),
            'failed_query_count': len(failed_queries)
        }
        
        # Log summary
        print(f"📊 SQL Consolidation Summary:")
        print(f"  📝 Total queries: {total_queries}")
        print(f"  ✅ Successful: {len(successful_queries)}")
        print(f"  ❌ Failed: {len(failed_queries)}")
        print(f"  📈 Total rows: {total_rows}")
        
        return consolidated_info
    
    def _generate_all_sql_queries(self, knowledge_graph_rows: List[Dict], user_question: str,question_type:str) -> List[Dict]:
        """Generate SQL queries from multiple knowledge graph rows"""
        
        print(f"🔍 Processing {len(knowledge_graph_rows)} knowledge graph rows for SQL generation")
        
        all_queries = []
        
        for i, kg_row in enumerate(knowledge_graph_rows):
            row_id = kg_row.get('id', f'row_{i}')
            root_cause_kg = kg_row.get('root_cause_kg', {})
            
            # No need to parse JSON - it's already structured
            if not root_cause_kg:
                print(f"⚠️ No root_cause_kg data for row {row_id}")
                continue
            
            print(f"🔍 Generating SQL queries for knowledge graph row: {row_id}")
            
            # Generate SQL queries for this knowledge graph (pass structured data directly)
            row_queries = self._generate_sql_queries_for_single_kg(root_cause_kg, user_question, row_id,question_type)
            print('row_queries',row_queries)
            # Add row identifier to each query
            for query in row_queries:
                query['kg_row_id'] = row_id
                query['kg_row_index'] = i
            
            all_queries.extend(row_queries)
        
        print(f"✅ Generated total {len(all_queries)} SQL queries from {len(knowledge_graph_rows)} knowledge graph rows")
        
        return all_queries


    def _generate_sql_queries_for_single_kg(self, knowledge_graph: Dict, user_question: str, kg_id: str, question_type: str) -> List[Dict]:
        """Generate SQL queries for a single knowledge graph - your existing logic"""
        
        sql_generation_prompt = f"""
                You are Databricks SQL Expert Agent and your objective Build SQL queries to support Healthcare Finance Deep Analysis using Databricks

                
                User Question: "{user_question}"
                Knowledge Graph ID: {kg_id}
                
                Available metadata information:
                {json.dumps(knowledge_graph, indent=2)}
                

                =============================
                SYSTEM PROMPT - Databricks SQL Agent
                =============================

                [ROLE & PURPOSE]
                - Generate **multiple Databricks SQL queries** aligned with the user's intent and metadata.
                - Follow all rules, recipes, and ranking logic from metadata (including llm_instructions).

                [RULES]
                1. **Understand intent**:
                - Detect if the question is about single period, MoM, QoQ, YoY, growth, or decline.
                - Use llm_instructions.comparison_modes and parameters for time windows and thresholds.
                2.**METADATA COVERAGE & VALIDATION**
                - Before generating SQL, parse the metadata JSON and build an internal index of:
                (a) datasets (id, table, columns, date, metrics),
                (b) all group_set entries and each group_set's query_hint,
                (c) llm_instructions (parameters, comparison_modes, ranking_policy, growth/decline recipes),
                (d) sample_questions (for intent steering; do not copy verbatim).
                - Coverage policy:
                * General questions → produce 5-8 meaningful queries that collectively cover every group_set in each relevant dataset (ledger and/or claims), unless the user explicitly restricts scope.
                * Attribute-specific questions → focus on the attribute but still pair it with 4–5 other group_sets to surface drivers.
                * Always apply dataset default filters from metadata.
                * Always respect group_set.query_hint to determine ORDER BY and LIMIT semantics.
                - Comparison & time grain:
                * Select comparison_mode from llm_instructions.comparison_modes: single_period, MoM, QoQ, YoY_month, YoY_quarter.
                * Use dataset.date.default_grain unless the user specifies month/quarter explicitly.
                * Follow llm_instructions.parameters for recent/baseline windows, thresholds, seasonality, and date_alignment_policy (MTD/QTD vs prior MTD/QTD if current period incomplete).
                - Growth vs Decline:
                * Growth → rank by positive pct_change (tie-break abs_change) after applying baseline thresholds from parameters.
                * Decline → rank by negative pct_change (tie-break abs_change) and require baseline strength (baseline_avg >= threshold) to exclude chronic underperformers; optionally check moving-average slope and, in claims, delta_rev_per_script if applicable.
                - Query hint application:
                * 'top_10' → ORDER BY primary metric (single_period) or pct_change (comparative) DESC LIMIT 10.
                * 'bottom_10' → ORDER BY primary metric or pct_change ASC LIMIT 10.
                * 'All' → return all groups sorted by primary metric DESC unless user specifies otherwise.
                6. **Output format**:
                - Return queries wrapped in XML tags with proper structure
                7. **Databricks SQL rules**:
                - Use exact table and column names from metadata.
                - Apply DATE_TRUNC for month/quarter.
                - Return YYYY-MM for monthly analysis.
                - Round percentages to 2 decimals.
                - Always include overall totals in attribute-level queries.
                        
                =============================
                RESPONSE FORMAT
                =============================
                Return the SQL queries wrapped in XML tags. You can use proper SQL formatting with line breaks.

                <queries>
                <query>
                    <query_id>title_of_query_1</query_id>
                    <table>ledger</table>
                    <purpose>Monthly revenue trend analysis</purpose>
                    <sql>
                    SELECT line_of_business, 
                        DATE_TRUNC('month', fscl_date) AS month, 
                        SUM(amount) AS revenue 
                    FROM table_name 
                    WHERE conditions 
                    GROUP BY line_of_business, month 
                    ORDER BY month
                    </sql>
                    <narrative>Analyzes monthly revenue trends by business line to identify growth patterns.</narrative>
                </query>
                <query>
                    <query_id>title_of_query_2</query_id>
                    <table>claims</table>
                    <purpose>Top performing regions</purpose>
                    <sql>
                    SELECT region,
                        SUM(revenue_amt) as total_revenue,
                        COUNT(*) as script_count
                    FROM claims_table
                    WHERE year = 2025
                    GROUP BY region
                    ORDER BY total_revenue DESC
                    LIMIT 10
                    </sql>
                    <narrative>Identifies top 10 regions by total revenue for current year analysis.</narrative>
                </query>
                </queries>

                IMPORTANT: You can use proper SQL formatting with indentation and line breaks inside the <sql> tags.
                """
        
        
        try:
            # Generate queries for this specific knowledge graph
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": sql_generation_prompt}
            ])
            print('drill through response',llm_response)
            # Parse XML response
            queries = self._parse_xml_queries(llm_response)
            print('root cause sql response', queries)
            print(f"✅ Generated {len(queries)} SQL queries for KG {kg_id}")
            print('queries inside llm',queries)
            return queries
            
        except Exception as e:
            print(f"❌ SQL generation failed for KG {kg_id}: {str(e)}")
            return []

    def _parse_xml_queries(self, xml_response: str) -> List[Dict]:
        """Parse XML response to extract multiple SQL queries"""
        import re
        
        queries = []
        
        # Find all query blocks
        query_blocks = re.findall(r'<query>(.*?)</query>', xml_response, re.DOTALL)
        
        for block in query_blocks:
            try:
                # Extract individual fields
                query_id = re.search(r'<query_id>(.*?)</query_id>', block, re.DOTALL)
                table = re.search(r'<table>(.*?)</table>', block, re.DOTALL)
                purpose = re.search(r'<purpose>(.*?)</purpose>', block, re.DOTALL)
                sql = re.search(r'<sql>(.*?)</sql>', block, re.DOTALL)
                narrative = re.search(r'<narrative>(.*?)</narrative>', block, re.DOTALL)
                
                if query_id and table and purpose and sql and narrative:
                    queries.append({
                        "query_id": query_id.group(1).strip(),
                        "table": table.group(1).strip(),
                        "purpose": purpose.group(1).strip(),
                        "sql": sql.group(1).strip().replace('`', ''),  # Remove backticks, keep formatting
                        "narrative": narrative.group(1).strip()
                    })
            except Exception as e:
                print(f"⚠️  Failed to parse query block: {str(e)}")
                continue
    


    def select_and_merge_rootcause_matches(self, search_results: List[Dict], user_question: str) -> List[Dict]:
        """Use LLM to select the SINGLE best match by ID based on keyword relevance and question context"""
        
        if not search_results:
            raise Exception("No search results provided for root cause selection")
        
        # DEBUG: Print all search results with their IDs
        print(f"🔍 DEBUG - Available search results:")
        for result in search_results:
            print(f"  ID={result.get('id')}, Description={result.get('description', 'N/A')}")
        
        # Prepare options for LLM selection - focus on IDs and descriptions
        options_text = []
        available_ids = []
        for result in search_results:
            result_id = result.get('id')
            description = result.get('description', 'N/A')
            available_ids.append(result_id)
            options_text.append(f"""
                ID: {result_id}
                Description: {description}
            """)
        
        selection_prompt = f"""
        Healthcare Finance Root Cause Analysis - Knowledge Graph Selection by ID:
        
        User Question: "{user_question}"
        
        Available Knowledge Graphs:
        {chr(10).join(options_text)}
        
        SELECTION CRITERIA:
        1. KEYWORD MATCHING: Match user question keywords with knowledge graph descriptions
        - "claims" keyword → choose claims-focused knowledge graph
        - "ledger" keyword → choose ledger-focused knowledge graph
        - "revenue" keyword → relevant to both, use other context
        
        2. SPECIFIC vs HOLISTIC: 
        - If question mentions specific topics (claims, drugs, pharmacy) → choose the specific match
        - If question is general → choose the broader knowledge graph
        
        3. RELEVANCE: Focus on which knowledge graph BEST addresses the user's specific question
        
        CRITICAL INSTRUCTIONS:
        - Analyze the user question: "{user_question}"
        - Select the SINGLE most relevant knowledge graph ID
        - Available IDs: {', '.join(map(str, available_ids))}
        - Return ONLY the ID (no option numbers, no index conversion needed)
        
        Response format (return EXACTLY this JSON structure):
        {{
            "selected_id": "the_actual_id_here",
            "reasoning": "Explanation of why this knowledge graph ID was selected based on keyword matching between user question and description"
        }}
        
        Analyze the user question "{user_question}" and select the best knowledge graph ID:
        """
        
        try:
            # Call LLM to select best match by ID
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": selection_prompt}
            ])
            
            selection_result = json.loads(llm_response)
            selected_id = selection_result.get('selected_id')
            reasoning = selection_result.get('reasoning', '')
            
            print(f"🎯 LLM selected knowledge graph ID: {selected_id}")
            print(f"📝 Selection reasoning: {reasoning}")
            
            # Validate that the selected ID exists in our search results
            valid_ids = [str(result.get('id')) for result in search_results]
            if str(selected_id) not in valid_ids:
                print(f"⚠️ Invalid ID selected: {selected_id}")
                print(f"⚠️ Valid IDs are: {valid_ids}")
                print("⚠️ Using first result as fallback")
                selected_id = search_results[0].get('id')
            
            print(f"✅ Final selected knowledge graph ID: {selected_id}")
            
            # Now call SQL to get the complete knowledge graph data
            return self._get_knowledge_graph_by_sql(selected_id)
            
        except Exception as e:
            print(f"❌ LLM selection failed: {str(e)}")
            # Fallback: use first result
            if search_results:
                fallback_id = search_results[0].get('id')
                print(f"🔍 DEBUG - Exception fallback ID: {fallback_id}")
                return self._get_knowledge_graph_by_sql(fallback_id)
            else:
                raise Exception("No fallback option available")

    def _get_knowledge_graph_by_sql(self, root_id: int) -> List[Dict]:
        """Execute SQL template to get knowledge graph data for the selected ID"""
        
        sql_template = """
        select group_id as id, merged_datasets_json as root_cause_kg from (
        WITH root AS (
        SELECT *
        FROM prd_optumrx_orxfdmprdsa.rag.knowledge_unit
        WHERE unit_id = :id AND status = 'active'
        ),
        rels AS (   -- only composed_of
        SELECT dst_unit_id, ordinal, merge_with_parent
        FROM prd_optumrx_orxfdmprdsa.rag.unit_relationship
        WHERE src_unit_id = :id
            AND is_enabled = true
            AND rel_type = 'composed_of'
        ),
        all_nodes AS (   -- parent + children
        SELECT 'self' AS relation, 0 AS ordinal, r.unit_id AS unit_id, TRUE AS merge_with_parent
        FROM root r
        UNION ALL
        SELECT 'composed_of' AS relation, ordinal, dst_unit_id AS unit_id, merge_with_parent
        FROM rels
        ),
        joined AS (   -- attach JSON and extract datasets array as raw JSON string
        SELECT
            a.relation,
            a.ordinal,
            a.unit_id,
            a.merge_with_parent,
            get_json_object(ku.root_cause_kg, '$.analysis_config.datasets') AS ds_json
        FROM all_nodes a
        JOIN prd_optumrx_orxfdmprdsa.rag.knowledge_unit ku
            ON ku.unit_id = a.unit_id AND ku.status = 'active'
        WHERE ku.root_cause_kg IS NOT NULL
            -- drop null/empty/[] datasets (keep as-is per your version)
        ),
        grp AS (   -- decide grouping: parent group vs separate child group
        SELECT
            CASE WHEN relation='self' OR merge_with_parent THEN :id ELSE unit_id END AS group_id,
            /* TWEAK: put parent last so child (dst) comes first in merged JSON */
            CASE WHEN relation='self' THEN 999999 ELSE ordinal END AS seq,
            ds_json
        FROM joined
        ),
        agg AS (   -- keep order: children by ordinal, then parent
        SELECT
            group_id,
            sort_array(collect_list(named_struct('seq', seq, 'ds', ds_json))) AS parts
        FROM grp
        GROUP BY group_id
        )
        SELECT
        group_id,
        -- Append arrays by stripping each outer [ ] and concatenating with commas
        concat(
            '[',
            concat_ws(',', transform(parts, p -> regexp_replace(p.ds, '(^\\\\[)|(\\\\]$)', ''))),
            ']'
        ) AS merged_datasets_json
        FROM agg
        ORDER BY CASE WHEN group_id = :id THEN 0 ELSE 1 END, group_id)
        order by group_id
        """
        
        try:
            print(f"🔍 Executing SQL query for knowledge graph ID: {root_id}")
            
            # Replace the parameter in SQL
            final_sql = sql_template.replace(':id', f"{root_id}")
            
            # Execute SQL
            matched_graph_rows = self.db_client.execute_sql(final_sql)
            
            if not matched_graph_rows:
                raise Exception(f"No knowledge graph data found for ID: {root_id}")
            
            print(f"✅ Retrieved {len(matched_graph_rows)} knowledge graph rows")
            
            # Return the raw results (multiple rows with id and root_cause_kg)
            return matched_graph_rows
            
        except Exception as e:
            print(f"❌ SQL execution failed for ID {root_id}: {str(e)}")
            raise Exception(f"Failed to retrieve knowledge graph data: {str(e)}")
        
    def _get_knowledge_graph(self, user_question: str) -> List[Dict]:
        """Get knowledge graph from vector embeddings with improved single selection and SQL retrieval"""
    
        try:
            print(f"🔍 Step 1.1: Vector search for root cause knowledge graphs")
            
            # Vector search to get root cause knowledge graphs (only id and description)
            search_results = self.db_client.vector_search_rootcause(
                query_text=user_question,
                num_results=5  # Get options for LLM to choose from
            )
            print('search results',search_results)
            if not search_results:
                raise Exception("No search results found for root cause analysis")
            
            print(f"🔍 Step 1.2: LLM single selection and SQL-based retrieval")
            
            # Use improved selection function that returns multiple rows from SQL
            knowledge_graph_rows = self.select_and_merge_rootcause_matches(
                search_results=search_results,
                user_question=user_question
            )
            
            if not knowledge_graph_rows:
                raise Exception("No knowledge graph data retrieved")
                
            print(f"✅ Retrieved {len(knowledge_graph_rows)} knowledge graph rows")
            return knowledge_graph_rows
            
        except Exception as e:
            raise Exception(f"Knowledge graph retrieval failed: {str(e)}")


    def _clean_llm_json_response(self, response: str) -> str:
        """Clean LLM response to extract pure JSON"""
        
        # Remove leading/trailing whitespace
        cleaned = response.strip()
        
        # Remove markdown code blocks if present
        if cleaned.startswith('```json'):
            cleaned = cleaned[7:]  # Remove ```json
        elif cleaned.startswith('```'):
            cleaned = cleaned[3:]   # Remove ```
        
        if cleaned.endswith('```'):
            cleaned = cleaned[:-3]  # Remove closing ```
        
        # Remove any leading/trailing whitespace after markdown removal
        cleaned = cleaned.strip()
        
        # Check if it starts with [ (JSON array)
        if not cleaned.startswith('['):
            # Try to find the JSON array in the response
            import re
            json_match = re.search(r'\[.*\]', cleaned, re.DOTALL)
            if json_match:
                cleaned = json_match.group(0)
            else:
                raise Exception("No valid JSON array found in LLM response")
        
        return cleaned
    
    def _execute_queries_parallel_with_retry(self, queries: List[Dict], knowledge_graph: Dict) -> List[Dict]:
        """Execute all queries in parallel with retry mechanism"""
        print('inside sql call', queries)
        if not queries:
            raise Exception("No queries to execute")
        
        print(f"🔄 Step 3: Executing {len(queries)} queries in parallel with retry mechanism")
        
        # Get config for retry context
        config = knowledge_graph
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit all tasks
            future_to_query = {
                executor.submit(self._execute_single_query_with_retry, query, config): query 
                for query in queries
            }
            
            # Collect results
            query_results = []
            for future in concurrent.futures.as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    result = future.result()
                    query_results.append(result)
                    if result.get('success'):
                        print(f"✅ Completed query: {query.get('query_id')} (Rows: {result.get('row_count', 0)}, Retries: {result.get('retry_count', 0)})")
                    else:
                        print(f"❌ Failed query: {query.get('query_id')} after all retries")
                except Exception as e:
                    print(f"❌ Unexpected error for query {query.get('query_id')}: {str(e)}")
                    query_results.append({
                        'query_id': query.get('query_id'),
                        'table': query.get('table'),
                        'purpose': query.get('purpose'),
                        'sql': query.get('sql'),
                        'narrative': query.get('narrative', ''),
                        'error': str(e),
                        'success': False,
                        'retry_count': 0,
                        'row_count': 0
                    })
        
        print(f"✅ Completed executing {len(query_results)} queries")
        successful = len([r for r in query_results if r.get('success')])
        print(f"📊 Success rate: {successful}/{len(query_results)} queries")
        
        return query_results

    def _execute_single_query_with_retry(self, query: Dict, config: Dict, max_retries: int = 2) -> Dict:
        """Execute a single query with retry mechanism"""
        
        query_id = query.get('query_id')
        original_sql = query.get('sql')  # Extract SQL from query dict
        table = query.get('table')
        purpose = query.get('purpose')
        narrative = query.get('narrative', '')
        retry_history = []
        
        current_sql = original_sql
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                # Execute the SQL string (not the full query dict)
                start_time = datetime.now()
                result_data = self.db_client.execute_sql(current_sql)  # Pass only SQL string
                execution_time = (datetime.now() - start_time).total_seconds()
                
                # Success!
                return {
                    'query_id': query_id,
                    'table': table,
                    'purpose': purpose,
                    'narrative': narrative,
                    'sql': current_sql,
                    'original_sql': original_sql if retry_count > 0 else None,
                    'result': result_data,
                    'row_count': len(result_data) if result_data else 0,
                    'success': True,
                    'retry_count': retry_count,
                    'retry_history': retry_history,
                    'execution_time': f"{execution_time:.2f}s"
                }
                
            except Exception as e:
                error_msg = str(e)
                retry_history.append({
                    'attempt': retry_count + 1,
                    'sql': current_sql,
                    'error': error_msg
                })
                
                print(f"⚠️ Query {query_id} failed (attempt {retry_count + 1}): {error_msg}")
                
                if retry_count < max_retries:
                    # Try to fix the SQL using LLM
                    retry_count += 1
                    print(f"🔧 Attempting to fix SQL (retry {retry_count}/{max_retries})")
                    
                    fixed_sql = self._fix_sql_with_llm(
                        current_sql, 
                        error_msg, 
                        table,
                        config
                    )
                    
                    if fixed_sql and fixed_sql != current_sql:
                        current_sql = fixed_sql
                        print(f"🔄 Retrying with fixed SQL for query {query_id}")
                    else:
                        # LLM couldn't fix it or returned same SQL
                        break
                else:
                    # Max retries reached
                    break
        
        # All retries failed
        return {
            'query_id': query_id,
            'table': table,
            'purpose': purpose,
            'narrative': narrative,
            'sql': original_sql,
            'result': None,
            'error': retry_history[-1]['error'] if retry_history else 'Unknown error',
            'success': False,
            'retry_count': retry_count,
            'retry_history': retry_history,
            'row_count': 0
        }

    def _fix_sql_with_llm(self, failed_sql: str, error_message: str, table_name: str, config: Dict) -> str:
        """Use LLM to fix SQL based on error message with JSON response"""

        fix_prompt = f"""
            You are an expert Databricks SQL developer. A SQL query has **FAILED** and needs to be **FIXED or REWRITTEN**.

            ==============================
            CONTEXT
            ==============================
            Failed SQL:
            {failed_sql}
            
            Error Message:
            {error_message}
            
            Table: {table_name}
            
            Table Configuration:
            {json.dumps(config, indent=2)}


            ============================== INSTRUCTIONS
            Identify the issue based on the error message and metadata.
            Fix the SQL syntax or rewrite the query if needed.
            Ensure the corrected query answers the original user question.
            Use only valid column names and Databricks-compatible SQL.
            ============================== RESPONSE FORMAT
            Return ONLY valid JSON with no markdown, explanations, or extra text:
            -Generate the SQL in single line and dont add '\n' for nrw line.
            -The response MUST be valid JSON. Generate SQL as a single line without line breaks.Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ``

            If the query is fixed: {{ "fixed_sql": "your corrected SQL query here" }}

            """                   
    
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": fix_prompt}
            ])
            
            print(f"🔍 Raw LLM fix response: {repr(llm_response)}")
            
            # Clean the response to extract JSON
            cleaned_response = self._clean_llm_fix_response(llm_response)
            
            # Parse the JSON response
            fix_result = json.loads(cleaned_response)
            
            fixed_sql = fix_result.get('fixed_sql', '').strip()
          
            
            return fixed_sql if fixed_sql else None
            
        except json.JSONDecodeError as e:
            print(f"❌ LLM returned invalid JSON for SQL fix: {str(e)}")
            print(f"🔍 Response was: {repr(llm_response) if 'llm_response' in locals() else 'No response'}")
            return None
        except Exception as e:
            print(f"❌ LLM SQL fix failed: {str(e)}")
            return None

    def _clean_llm_fix_response(self, response: str) -> str:
        """Clean LLM response to extract JSON for SQL fix"""
        
        # Remove leading/trailing whitespace
        cleaned = response.strip()
        
        # Remove markdown code blocks if present
        if cleaned.startswith('```json'):
            cleaned = cleaned[7:]  # Remove ```json
        elif cleaned.startswith('```'):
            cleaned = cleaned[3:]   # Remove ```
        
        if cleaned.endswith('```'):
            cleaned = cleaned[:-3]  # Remove closing ```
        
        # Remove any leading/trailing whitespace after markdown removal
        cleaned = cleaned.strip()
        
        # Check if it starts with { (JSON object)
        if not cleaned.startswith('{'):
            # Try to find the JSON object in the response
            import re
            json_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
            if json_match:
                cleaned = json_match.group(0)
            else:
                raise Exception("No valid JSON object found in LLM response")
        
        return cleaned
    
    def _generate_individual_insights(self, query_results: List[Dict], user_question: str) -> List[Dict]:
        """Generate insights for each individual query result with chart recommendations"""
        
        print(f"💡 Step 4: Generating insights with chart recommendations for {len(query_results)} query results")
        
        insights = []
        
        for result in query_results:
            if result.get('success') and result.get('result'):
                # Generate insight with chart recommendation for successful query with data
                insight_with_chart = self._generate_single_insight_with_chart(result, user_question)
                insights.append({
                    'query_id': result.get('query_id'),
                    'table': result.get('table'),
                    'purpose': result.get('purpose'),
                    'row_count': result.get('row_count', 0),
                    'insight_text': insight_with_chart.get('insight_text', ''),
                    'chart_recommendation': insight_with_chart.get('chart_recommendation', {}),
                    'has_data': True
                })
            elif result.get('success') and not result.get('result'):
                # Successful query but no data
                insights.append({
                    'query_id': result.get('query_id'),
                    'table': result.get('table'),
                    'purpose': result.get('purpose'),
                    'row_count': 0,
                    'insight_text': f"No data found for {result.get('purpose', 'this query')}",
                    'chart_recommendation': {},
                    'has_data': False
                })
            else:
                # Failed query
                insights.append({
                    'query_id': result.get('query_id'),
                    'table': result.get('table'),
                    'purpose': result.get('purpose'),
                    'row_count': 0,
                    'insight_text': f"Analysis failed: {result.get('error', 'Unknown error')}",
                    'chart_recommendation': {},
                    'has_data': False
                })
        
        print(f"✅ Generated {len(insights)} individual insights with chart recommendations")
        return insights

    def _generate_single_insight_with_chart(self, query_result: Dict, user_question: str) -> Dict:
        """Generate insight and chart recommendation for a single query result"""
        
        # Prepare data summary for LLM
        result_data = query_result.get('result', [])
        
        # Limit data for prompt (top 10 rows for LLM analysis)
        sample_data = result_data[:500] if len(result_data) > 10 else result_data
        print('sample data',sample_data)
        # Get column information from sample data
        columns_info = []
        if sample_data and len(sample_data) > 0:
            first_row = sample_data[0]
            for key, value in first_row.items():
                # Determine data type
                data_type = "numeric" if isinstance(value, (int, float)) else "categorical"
                if "date" in key.lower() or "time" in key.lower() or "month" in key.lower():
                    data_type = "temporal"
                columns_info.append(f"- {key}: {data_type}")
        
        insight_prompt = f"""
            Generate a concise, finance-ready insight and chart recommendation from this query result.
            
            User Question: "{user_question}"
            Query Purpose: {query_result.get('purpose')}
            Table: {query_result.get('table')}
            Total Rows: {query_result.get('row_count')}
            
            Available Columns:
            {chr(10).join(columns_info)}
            
            Sample Data (top rows):
            {json.dumps(sample_data, indent=2)}
            
            ========================
            GOAL & AUDIENCE
            ========================
            Goal: Turn SQL result tables into crisp, bullet-only insights for finance leaders.
            Audience: FP&A and operations leaders; expect variance, drivers/suppressors, concentration, and trend callouts.
            
            ========================
            INPUTS
            ========================
            User context: user_question, time_scope, time_grain, currency, primary_metric, top_n, comparison_scope.
            Result tables: totals, top, bottom; optional attribution, notes.
            Metadata: main_attribute, other_groupsets, date_format.
            
            ========================
            CORE COMPUTATIONS (done silently, not shown in output)
            ========================
            - Totals & change: per period_key compute delta, pct_change (baseline=0 or missing → pct=null).
            - Concentration: latest period top-N share; items to ~80% (Pareto).
            - Drivers/Suppressors: rank by absolute delta (prefer attribution; else infer from top/bottom changes).
            - Trend shape: compare last 2–3 periods’ growth rates; detect accelerations, slowdowns, reversals.
            - Scope clarity: always state subset context (e.g., “within Client X”).
            
            ========================
            NARRATIVE RULES
            ========================
            - Bullet points only inside "insight_text".
            - No recommendations or actions—insights only.
            - Use absolute dates from data; avoid “this month”.
            - Auto-scale numbers:
            - ≥1B → currency x.xB
            - ≥1M → currency x.xM
            - ≥1K → currency x.xK
            - else → raw with thousands separators
            - Percentages: 1 decimal place.
            - Keep unit consistent in a section.
            - Each bullet ≤ 22 words; lead with result, then cause.
            
            ========================
            OUTPUT SECTIONS TO COVER IN BULLETS
            ========================
            1. Executive summary (2–3 bullets): direction, magnitude, key drivers/suppressors, scope.
            2. Metric overview (2–3 bullets): latest value, comparison, delta, pct change, time grain.
            3. Drivers (3-4 bullets): top contributors (↑) and suppressors (↓) with delta and share of total.
            4. Concentration (1–3 bullets): top-N share, items to 80%, bottom-N risk.
            5. Trend insights (2–3 bullets): MoM/DoD/QoQ/YoY changes, accelerations/slowdowns, reversals.

            
            ========================
            CHART RECOMMENDATION RULES
            ========================
            - Choose from: line_chart (temporal), bar_chart (categorical), grouped_bar_chart (grouped comparisons).
            - Identify x_axis, y_axis, and group_by (only for grouped_bar_chart).
            - Give descriptive title and axis labels.
            - Briefly explain why the chart type is suitable.
            
            ========================
            RESPONSE FORMAT (must match exactly)
            ========================
            Return this JSON object only:
            {{
                "insight_text": "• Bullet 1\\n• Bullet 2\\n• Bullet 3",
                "chart_recommendation": {{
                    "chart_type": "line_chart|bar_chart|grouped_bar_chart",
                    "x_axis": "column_name_for_x_axis",
                    "y_axis": "column_name_for_y_axis",
                    "group_by": "column_name_for_grouping (only for grouped_bar_chart)",
                    "chart_title": "Descriptive chart title",
                    "x_axis_label": "X-axis label",
                    "y_axis_label": "Y-axis label",
                    "recommended_because": "Brief explanation why this chart type is suitable"
                }}
            }}
            
            Only return the JSON object—no extra commentary, markdown, or code blocks.
            """
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": insight_prompt}
            ])
            
            # Clean and parse the JSON response
            cleaned_response = self._clean_llm_json_response_for_insight(llm_response)
            insight_result = json.loads(cleaned_response)
            
            return {
                'insight_text': insight_result.get('insight_text', ''),
                'chart_recommendation': insight_result.get('chart_recommendation', {})
            }
            
        except Exception as e:
            print(f"⚠️ Insight generation failed: {str(e)}")
            return {
                'insight_text': f"Data retrieved successfully ({query_result.get('row_count')} rows) but insight generation failed.",
                'chart_recommendation': {}
            }

    def _clean_llm_json_response_for_insight(self, response: str) -> str:
        """Clean LLM response to extract JSON for insight generation"""
        
        # Remove leading/trailing whitespace
        cleaned = response.strip()
        
        # Remove markdown code blocks if present
        if cleaned.startswith('```json'):
            cleaned = cleaned[7:]  # Remove ```json
        elif cleaned.startswith('```'):
            cleaned = cleaned[3:]   # Remove ```
        
        if cleaned.endswith('```'):
            cleaned = cleaned[:-3]  # Remove closing ```
        
        # Remove any leading/trailing whitespace after markdown removal
        cleaned = cleaned.strip()
        
        # Check if it starts with { (JSON object)
        if not cleaned.startswith('{'):
            # Try to find the JSON object in the response
            import re
            json_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
            if json_match:
                cleaned = json_match.group(0)
            else:
                raise Exception("No valid JSON object found in LLM response")
        
        return cleaned


    def _generate_consolidated_narrative(self, individual_insights: List[Dict], query_results: List[Dict], user_question: str) -> str:
        """Generate consolidated narrative using grouped insights by purpose"""
        
        print(f"💡 Step 8: Generating consolidated narrative from {len(individual_insights)} insights")
        
        # Group insights by purpose
        insights_by_purpose = {}
        
        for i, insight in enumerate(individual_insights):
            purpose = insight.get('purpose', 'Analysis')
            
            # Initialize purpose group if not exists
            if purpose not in insights_by_purpose:
                insights_by_purpose[purpose] = {
                    'purpose': purpose,
                    'insights': [],
                    'total_rows': 0,
                    'query_count': 0
                }
            
            # Add insight data if it has meaningful content
            if insight.get('has_data', False) and insight.get('insight_text'):
                insights_by_purpose[purpose]['insights'].append({
                    'insight_text': insight.get('insight_text', ''),
                    'row_count': insight.get('row_count', 0),
                    'table': insight.get('table', ''),
                    'query_id': insight.get('query_id', '')
                })
                insights_by_purpose[purpose]['total_rows'] += insight.get('row_count', 0)
            
            insights_by_purpose[purpose]['query_count'] += 1
        
        # Generate consolidated narrative using LLM
        consolidated_narrative = self._generate_consolidated_insights_and_recommendations(insights_by_purpose, user_question)
        
        return consolidated_narrative

    def _generate_consolidated_insights_and_recommendations(self, insights_by_purpose: Dict, user_question: str) -> str:
        """Generate overall narrative using LLM based on grouped insights"""
        
        print(f"💡 Generating consolidated narrative from {len(insights_by_purpose)} purpose groups")
        
        # Prepare grouped insights for LLM
        grouped_insights_text = []
        
        for purpose, data in insights_by_purpose.items():
            if data['insights']:  # Only include purposes with actual insights
                insights_list = []
                for insight in data['insights']:
                    insights_list.append(f"• {insight['insight_text']} (Table: {insight['table']}, Rows: {insight['row_count']})")
                
                grouped_insights_text.append(f"""
    **{purpose}** (Total Rows: {data['total_rows']}, Queries: {data['query_count']}):
    {chr(10).join(insights_list)}
                """)
        
        consolidation_prompt = f"""
        Healthcare Finance Root Cause Analysis - Consolidated Narrative Generation
        
        User Question: "{user_question}"
        
        Grouped Analysis Results:
        {chr(10).join(grouped_insights_text)}
        
        INSTRUCTIONS:
        Generate a comprehensive consolidated narrative that synthesizes all the individual insights above.
        
        NARRATIVE STRUCTURE:
        1. Start with an executive summary of key findings
        2. Highlight cross-cutting themes and patterns
        3. Identify the most significant business impacts
        4. Connect insights between different analysis purposes
        5. Conclude with overall assessment
        
        FORMATTING RULES:
        - Write in clear, professional paragraphs (not bullet points)
        - Use specific numbers and trends from the insights
        - Focus on business impact and financial implications
        - Keep it concise but comprehensive (4-6 paragraphs)
        - Use data-driven language
        
        Generate a consolidated narrative that synthesizes all the insights:
        """
        
        try:
            llm_response = self.db_client.call_sonnet_3_api([
                {"role": "user", "content": consolidation_prompt}
            ])
            
            # Return the narrative directly (no JSON parsing needed)
            consolidated_narrative = llm_response.strip()
            
            print(f"✅ Generated consolidated narrative ({len(consolidated_narrative)} characters)")
            
            return consolidated_narrative
            
        except Exception as e:
            print(f"❌ Consolidated narrative generation failed: {str(e)}")
            
            # Fallback narrative
            purposes_list = list(insights_by_purpose.keys())
            fallback_narrative = f"""
            Analysis completed across {len(purposes_list)} different analysis purposes for the question: "{user_question}".
            
            The investigation covered multiple data sources and generated insights across various business dimensions. 
            Key findings were identified across {len([p for p in insights_by_purpose.values() if p['insights']])} analysis areas with meaningful data.
            
            The analysis processed a total of {sum(p['total_rows'] for p in insights_by_purpose.values())} rows of data 
            across {sum(p['query_count'] for p in insights_by_purpose.values())} queries, providing comprehensive coverage 
            of the requested analysis scope.
            """
            
            return fallback_narrative
