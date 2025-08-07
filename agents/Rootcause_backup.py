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
        
        if not user_question:
            raise Exception("No user question found in state")
        
        print(f"🔍 Starting enhanced root cause analysis for: '{user_question}'")
        
        # 1. Get knowledge graph with LLM selection and merging
        knowledge_graph = self._get_knowledge_graph(user_question)
        
        if not knowledge_graph:
            raise Exception("No knowledge graph found for root cause analysis")
        
        # Log metadata about the selection
        metadata = knowledge_graph.get('_metadata', {})
        if metadata:
            print(f"📊 Analysis based on {metadata['selected_count']} knowledge graph(s):")
            for i, desc in enumerate(metadata.get('selected_descriptions', [])):
                print(f"  {i+1}. {desc}")
        
        # 2. Generate ALL SQL queries at once
        all_sql_queries = self._generate_all_sql_queries(knowledge_graph, user_question)
        
        # 3. Execute queries in parallel with retry mechanism
        query_results = self._execute_queries_parallel_with_retry(all_sql_queries, knowledge_graph)
        
        # 4. Generate individual insights for each query result
        individual_insights = self._generate_individual_insights(query_results, user_question)
        
        # 5. Consolidate all SQL-related information (keeping your structure)
        consolidated_sql_info = self._consolidate_sql_information(query_results, individual_insights, user_question)
        
        # 6. Generate consolidated narrative using all insights
        consolidated_narrative = self._generate_consolidated_narrative(
            individual_insights, 
            query_results, 
            user_question
        )
        
        return {
            'knowledge_graph': knowledge_graph,
            'query_results': query_results,
            'individual_insights': individual_insights,
            'narrative_response': consolidated_narrative,
            'analysis_complete': True,
            'selected_kg_metadata': metadata,
            
            # Consolidated SQL information for future use (keeping your structure)
            'sql_queries': consolidated_sql_info['all_sql_queries'],
            'query_results_consolidated': consolidated_sql_info['all_query_results'],
            'sql_execution_summary': consolidated_sql_info['execution_summary'],
            'failed_queries': consolidated_sql_info['failed_queries'],
            'successful_queries': consolidated_sql_info['successful_queries'],
            'total_queries_executed': consolidated_sql_info['total_queries'],
            'total_rows_returned': consolidated_sql_info['total_rows']
        }

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
    
    def select_and_merge_rootcause_matches(self, search_results: List[Dict], user_question: str, max_selections: int = 2) -> Dict:
        """Use LLM to select best matches, then directly merge their knowledge graphs"""
        
        if not search_results:
            raise Exception("No search results provided for root cause selection")
        
        # Prepare options for LLM selection
        options_text = []
        for i, result in enumerate(search_results):
            options_text.append(f"""
    Option {i+1}:
    - ID: {result.get('id', 'N/A')}
    - Description: {result.get('description', 'N/A')}
    - Definition: {result.get('definition', 'N/A')}
            """)
        
        selection_prompt = f"""
        Healthcare Finance Root Cause Analysis - Knowledge Graph Selection:
        
        User Question: "{user_question}"
        
        Available Root Cause Knowledge Graphs:
        {chr(10).join(options_text)}
        
        Instructions:
        - Analyze which root cause knowledge graph(s) best match the user's question
        - You can select 1 or 2 options that are most relevant
        - Consider the description and definition to make your choice
        - Focus on relevance to the specific healthcare finance question asked
        
        Response format (return EXACTLY this JSON structure):
        {{
            "selected_options": [1, 2],
            "reasoning": "Explanation of why these options were selected"
        }}
        
        If only one option is clearly best, return just that option number.
        If multiple options complement each other, select up to 2.
        """
        
        try:
            # Call LLM to select best matches
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": selection_prompt}
            ])
            
            selection_result = json.loads(llm_response)
            selected_indices = selection_result.get('selected_options', [])
            reasoning = selection_result.get('reasoning', '')
            
            print(f"🎯 LLM selected options: {selected_indices}")
            print(f"📝 Selection reasoning: {reasoning}")
            
            # Filter the selected results based on LLM selection
            selected_results = []
            for idx in selected_indices:
                if 1 <= idx <= len(search_results):
                    selected_results.append(search_results[idx - 1])  # Convert to 0-based index
            
            if not selected_results:
                print("⚠️ No valid selections, using top result as fallback")
                selected_results = [search_results[0]]
            
            print(f"✅ Selected {len(selected_results)} knowledge graph(s)")
            
            # Now directly merge the knowledge graphs
            merged_kg = self._direct_merge_knowledge_graphs(selected_results)
            
            return merged_kg
            
        except Exception as e:
            print(f"❌ LLM selection failed: {str(e)}")
            # Fallback: use top result
            selected_results = [search_results[0]] if search_results else []
            return self._direct_merge_knowledge_graphs(selected_results)

    def _direct_merge_knowledge_graphs(self, selected_results: List[Dict]) -> Dict:
        """Directly merge knowledge graphs with new JSON structure"""
    
        if not selected_results:
            raise Exception("No selected results to merge")
        
        if len(selected_results) == 1:
            # Single result, just parse and return
            kg_str = selected_results[0].get('root_cause_kg', '{}')
            if isinstance(kg_str, str):
                parsed_kg = json.loads(kg_str)
            else:
                parsed_kg = kg_str
            
            # Add metadata
            parsed_kg['_metadata'] = {
                'selected_count': 1,
                'selected_ids': [selected_results[0].get('id')],
                'selected_descriptions': [selected_results[0].get('description')]
            }
            
            return parsed_kg
        
        print(f"🔀 Direct merging {len(selected_results)} root cause knowledge graphs")
        
        # Initialize merged structure with new format
        merged_kg = {
            'revenue_analysis_config': {
                'analysis_flow': ['ledger', 'claims', 'prior_auth', 'formulary'],
                'data_sources': [],
                'cross_reference': {},
                'analysis_keywords': {}
            }
        }
        
        # Track metadata
        selected_ids = []
        selected_descriptions = []
        all_sources = {}
        
        for result in selected_results:
            kg_str = result.get('root_cause_kg', '{}')
            
            # Parse the knowledge graph
            if isinstance(kg_str, str):
                kg = json.loads(kg_str) if kg_str else {}
            else:
                kg = kg_str
            
            # Extract config
            config = kg.get('revenue_analysis_config', {})
            
            # Merge data sources (avoid duplicates by id)
            for source in config.get('data_sources', []):
                source_id = source.get('id')
                if source_id and source_id not in all_sources:
                    all_sources[source_id] = source
            
            # Take cross_reference from first KG or merge if needed
            if not merged_kg['revenue_analysis_config']['cross_reference'] and config.get('cross_reference'):
                merged_kg['revenue_analysis_config']['cross_reference'] = config.get('cross_reference', {})
            
            # Take analysis_keywords from first KG
            if not merged_kg['revenue_analysis_config']['analysis_keywords'] and config.get('analysis_keywords'):
                merged_kg['revenue_analysis_config']['analysis_keywords'] = config.get('analysis_keywords', {})
            
            # Track metadata
            selected_ids.append(result.get('id'))
            selected_descriptions.append(result.get('description'))
        
        # Add all unique data sources
        merged_kg['revenue_analysis_config']['data_sources'] = list(all_sources.values())
        
        # Add metadata
        merged_kg['_metadata'] = {
            'selected_count': len(selected_results),
            'selected_ids': selected_ids,
            'selected_descriptions': selected_descriptions,
            'total_data_sources': len(all_sources)
        }
        
        print(f"✅ Direct merge completed: {len(all_sources)} unique data sources")
        
        return merged_kg
    
    def _get_knowledge_graph(self, user_question: str) -> Dict:
        """Get knowledge graph from vector embeddings with LLM selection and direct merging"""
        
        try:
            print(f"🔍 Step 1.1: Vector search for root cause knowledge graphs")
            
            # Vector search to get root cause knowledge graphs
            search_results = self.db_client.vector_search_rootcause(
                query_text=user_question,
                num_results=5  # Get more options for LLM to choose from
            )
            
            if not search_results:
                raise Exception("No search results found for root cause analysis")
            
            print(f"🔍 Step 1.2: LLM selection and direct merging of knowledge graphs")
            
            # Use combined function: LLM selection + direct merging
            knowledge_graph = self.select_and_merge_rootcause_matches(
                search_results=search_results,
                user_question=user_question,
                max_selections=2
            )
            
            if not knowledge_graph:
                raise Exception("No knowledge graph created")
            
            config = knowledge_graph.get('revenue_analysis_config', {})
            data_sources_count = len(config.get('data_sources', []))
            metadata = knowledge_graph.get('_metadata', {})
            
            print(f"✅ Final knowledge graph prepared with {data_sources_count} data sources")
            print(f"📊 Based on {metadata.get('selected_count', 0)} selected knowledge graph(s)")
            
            return knowledge_graph
            
        except Exception as e:
            raise Exception(f"Knowledge graph retrieval and processing failed: {str(e)}")
    
    def _generate_all_sql_queries(self, knowledge_graph: Dict, user_question: str) -> List[Dict]:
        """Generate ALL SQL queries in one LLM call using the new JSON structure"""
        
        config = knowledge_graph.get('revenue_analysis_config', {})
        
        # Prepare a concise version of config for the prompt
        config_summary = {
            'analysis_flow': config.get('analysis_flow', []),
            'data_sources': [
                {
                    'id': ds.get('id'),
                    'table': ds.get('table'),
                    'dimensions': ds.get('dimensions'),
                    'metrics': ds.get('metrics'),
                    'time_config': ds.get('time_config'),
                    'analysis_hints': ds.get('analysis_hints')
                }
                for ds in config.get('data_sources', [])
            ],
            'cross_reference': config.get('cross_reference', {})
        }
        
        sql_generation_prompt = f"""
        Healthcare Finance SQL Generation Task:
        
        User Question: "{user_question}"
        
        Available Data Sources and Metadata:
        {json.dumps(config_summary, indent=2)}
        
        Instructions:
        1. Analyze the user question to understand what they're asking
        2. Generate SQL queries for relevant tables following the analysis_flow order
        3. Use the dimension groupings properly:
           - Combinable: Can be in same GROUP BY clause
           - Separate: Query individually in separate queries
           - Drill_down: Always add LIMIT 20 and ORDER BY
        4. Include cross-reference queries if comparison/reconciliation is needed
        5. Use appropriate time aggregation based on time_config
        6. Add relevant filters based on the user's question context
        
        Rules:
        - Write complete, executable SQL queries
        - Use exact table names from the config
        - For drill_down dimensions (like drug_name, member_id), always add LIMIT
        - Apply appropriate date filters if time period is mentioned
        - Include ORDER BY for meaningful results
        
        Return a JSON array of queries in this exact format:
        [
            {{
                "query_id": "ledger_revenue_trend",
                "table": "ledger",
                "purpose": "Analyze revenue trend over time",
                "sql": "SELECT DATE_TRUNC('month', fscl_date) as month, SUM(amount) as revenue FROM prod_optumrx_gmraprodadls.default.actuals_vs_forecast WHERE account_types = 'Revenues' GROUP BY month ORDER BY month"
            }},
            {{
                "query_id": "claims_drug_analysis",
                "table": "claims",
                "purpose": "Top drugs by revenue",
                "sql": "SELECT drug_name, SUM(rev_amt) as revenue FROM prd_optumrx_orxfdmprdsa.fdmenh.claim_level_exp_revenue GROUP BY drug_name ORDER BY revenue DESC LIMIT 20"
            }}
        ]
        
        Generate queries for all relevant tables based on the user's question.
        """
        
        try:
            # Single LLM call generates all queries
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": sql_generation_prompt}
            ])
            
            queries = json.loads(llm_response)
            print(f"✅ Generated {len(queries)} SQL queries")
            
            # Log each query
            for i, query in enumerate(queries):
                print(f"  Query {i+1}: {query.get('query_id')} - {query.get('purpose')}")
            
            return queries
            
        except Exception as e:
            print(f"❌ SQL generation failed: {str(e)}")
            raise Exception(f"Failed to generate SQL queries: {str(e)}")
    
    def _execute_queries_parallel_with_retry(self, queries: List[Dict], knowledge_graph: Dict) -> List[Dict]:
        """Execute all queries in parallel with retry mechanism"""
        
        
