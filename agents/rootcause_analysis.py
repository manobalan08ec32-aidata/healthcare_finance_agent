from typing import Dict, List, Optional
import json
import asyncio
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime  # Add this import
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
        
        # 2. Process each aspect in parallel
        aspect_results = self._process_aspects_parallel(knowledge_graph, state)
        
        # 3. Extract and consolidate all SQL-related information
        consolidated_sql_info = self._consolidate_sql_information(aspect_results, user_question)
        
        # 4. Generate consolidated narrative
        consolidated_narrative = self._generate_consolidated_narrative(aspect_results, user_question)
        
        return {
            'knowledge_graph': knowledge_graph,
            'aspect_results': aspect_results,
            'narrative_response': consolidated_narrative,
            'analysis_complete': True,
            'selected_kg_metadata': metadata,
            
            # NEW: Consolidated SQL information for future use
            'sql_queries': consolidated_sql_info['all_sql_queries'],
            'query_results': consolidated_sql_info['all_query_results'],
            'sql_execution_summary': consolidated_sql_info['execution_summary'],
            'failed_queries': consolidated_sql_info['failed_queries'],
            'successful_queries': consolidated_sql_info['successful_queries'],
            'total_queries_executed': consolidated_sql_info['total_queries'],
            'total_rows_returned': consolidated_sql_info['total_rows']
        }

    def _consolidate_sql_information(self, aspect_results: List[Dict], user_question: str) -> Dict:
        """Consolidate all SQL queries, results, and execution metadata"""
        
        print(f"🔍 Consolidating SQL information from {len(aspect_results)} aspects")
        
        all_sql_queries = {}
        all_query_results = {}
        successful_queries = {}
        failed_queries = {}
        execution_summary = []
        total_rows = 0
        total_queries = 0
        
        for aspect_result in aspect_results:
            aspect_id = aspect_result.get('aspect_id', 'unknown')
            sql_queries = aspect_result.get('sql_queries', {})
            query_results = aspect_result.get('query_results', {})
            success = aspect_result.get('success', False)
            
            # Store queries with aspect context
            for query_name, sql_query in sql_queries.items():
                full_query_key = f"{aspect_id}_{query_name}"
                all_sql_queries[full_query_key] = {
                    'aspect_id': aspect_id,
                    'query_name': query_name,
                    'sql': sql_query,
                    'user_question': user_question,
                    'timestamp': datetime.now().isoformat()
                }
                total_queries += 1
            
            # Store results with metadata
            for query_name, results in query_results.items():
                full_query_key = f"{aspect_id}_{query_name}"
                result_count = len(results) if results else 0
                total_rows += result_count
                
                all_query_results[full_query_key] = {
                    'aspect_id': aspect_id,
                    'query_name': query_name,
                    'results': results,
                    'row_count': result_count,
                    'has_data': result_count > 0,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Categorize successful vs failed queries
                if results is not None and result_count >= 0:  # Even 0 rows is a successful execution
                    successful_queries[full_query_key] = {
                        'aspect_id': aspect_id,
                        'query_name': query_name,
                        'row_count': result_count,
                        'sql': sql_queries.get(query_name, '')
                    }
                else:
                    failed_queries[full_query_key] = {
                        'aspect_id': aspect_id,
                        'query_name': query_name,
                        'error': 'No results returned',
                        'sql': sql_queries.get(query_name, '')
                    }
            
            # Create execution summary for this aspect
            aspect_summary = {
                'aspect_id': aspect_id,
                'success': success,
                'queries_attempted': len(sql_queries),
                'queries_successful': len([r for r in query_results.values() if r is not None]),
                'queries_failed': len([r for r in query_results.values() if r is None]),
                'total_rows_returned': sum(len(r) if r else 0 for r in query_results.values()),
                'error': aspect_result.get('error') if not success else None
            }
            execution_summary.append(aspect_summary)
        
        consolidated_info = {
            'all_sql_queries': all_sql_queries,
            'all_query_results': all_query_results,
            'successful_queries': successful_queries,
            'failed_queries': failed_queries,
            'execution_summary': execution_summary,
            'total_queries': total_queries,
            'total_rows': total_rows,
            'successful_query_count': len(successful_queries),
            'failed_query_count': len(failed_queries),
            'aspects_processed': len(aspect_results)
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
            
            # Now directly merge the knowledge graphs without LLM
            merged_kg = self._direct_merge_knowledge_graphs(selected_results)
            
            return merged_kg
            
        except Exception as e:
            print(f"❌ LLM selection failed: {str(e)}")
            # Fallback: use top result
            selected_results = [search_results[0]] if search_results else []
            return self._direct_merge_knowledge_graphs(selected_results)

    def _direct_merge_knowledge_graphs(self, selected_results: List[Dict]) -> Dict:
        """Directly merge knowledge graphs without LLM - simple concatenation"""
    
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
        
        # Combine all aspects from multiple knowledge graphs
        all_aspects = []
        selected_ids = []
        selected_descriptions = []
        
        for result in selected_results:
            kg_str = result.get('root_cause_kg', '{}')
            
            # Parse the knowledge graph
            if isinstance(kg_str, str):
                kg = json.loads(kg_str) if kg_str else {}
            else:
                kg = kg_str
            
            # Extract aspects
            aspects = kg.get('root_cause_aspects', [])
            all_aspects.extend(aspects)
            
            # Track metadata
            selected_ids.append(result.get('id'))
            selected_descriptions.append(result.get('description'))
            
            print(f"📊 Added {len(aspects)} aspects from KG: {result.get('id')}")
        
        # Remove duplicate aspects by aspect_id (keep first occurrence)
        unique_aspects = []
        seen_aspect_ids = set()
        
        for aspect in all_aspects:
            aspect_id = aspect.get('aspect_id', 'unknown')
            if aspect_id not in seen_aspect_ids:
                unique_aspects.append(aspect)
                seen_aspect_ids.add(aspect_id)
            else:
                print(f"⚠️ Skipping duplicate aspect: {aspect_id}")
        
        merged_kg = {
            'root_cause_aspects': unique_aspects,
            '_metadata': {
                'selected_count': len(selected_results),
                'selected_ids': selected_ids,
                'selected_descriptions': selected_descriptions,
                'total_aspects': len(unique_aspects),
                'original_aspects': len(all_aspects)
            }
        }
        
        print(f"✅ Direct merge completed: {len(unique_aspects)} unique aspects from {len(all_aspects)} total")
        
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
            
            aspect_count = len(knowledge_graph.get('root_cause_aspects', []))
            metadata = knowledge_graph.get('_metadata', {})
            
            print(f"✅ Final knowledge graph prepared with {aspect_count} aspects")
            print(f"📊 Based on {metadata.get('selected_count', 0)} selected knowledge graph(s)")
            
            return knowledge_graph
            
        except Exception as e:
            raise Exception(f"Knowledge graph retrieval and processing failed: {str(e)}")
    
    def _process_aspects_parallel(self, knowledge_graph: Dict, state: AgentState) -> List[Dict]:
        """Process all aspects in parallel"""
        
        aspects = knowledge_graph.get('root_cause_aspects', [])
        
        if not aspects:
            raise Exception("No root cause aspects found in knowledge graph")
        
        print(f" Step 2: Processing {len(aspects)} aspects in parallel")
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=min(len(aspects), 5)) as executor:
            # Submit all tasks
            future_to_aspect = {
                executor.submit(self._process_single_aspect, aspect, state): aspect 
                for aspect in aspects
            }
            
            # Collect results
            aspect_results = []
            for future in concurrent.futures.as_completed(future_to_aspect):
                aspect = future_to_aspect[future]
                try:
                    result = future.result()
                    aspect_results.append(result)
                    print(f"✅ Completed aspect: {aspect.get('aspect_id', 'unknown')}")
                except Exception as e:
                    print(f"❌ Failed aspect: {aspect.get('aspect_id', 'unknown')} - {str(e)}")
                    # Add error result
                    aspect_results.append({
                        'aspect_id': aspect.get('aspect_id', 'unknown'),
                        'error': str(e),
                        'narrative': f"Failed to analyze {aspect.get('aspect_id', 'unknown')}: {str(e)}"
                    })
        
        print(f"✅ Completed processing {len(aspect_results)} aspects")
        return aspect_results
    
    def _process_single_aspect(self, aspect: Dict, state: AgentState) -> Dict:
        """Process a single aspect - generate SQL and execute"""
        
        aspect_id = aspect.get('aspect_id', 'unknown')
        print(f"🔍 Processing aspect: {aspect_id}")
        
        try:
            # 1. Generate SQL for this aspect
            sql_queries = self._generate_sql_for_aspect(aspect, state)
            
            # 2. Execute SQL queries
            query_results = self._execute_aspect_queries(sql_queries, aspect)
            
            # 3. Generate narrative for this aspect
            narrative = self._generate_aspect_narrative(aspect, query_results, state)
            
            return {
                'aspect_id': aspect_id,
                'sql_queries': sql_queries,
                'query_results': query_results,
                'narrative': narrative,
                'success': True
            }
            
        except Exception as e:
            print(f"❌ Error processing aspect {aspect_id}: {str(e)}")
            return {
                'aspect_id': aspect_id,
                'error': str(e),
                'narrative': f"Failed to analyze {aspect_id}: {str(e)}",
                'success': False
            }
    
    def _generate_sql_for_aspect(self, aspect: Dict, state: AgentState) -> Dict[str, str]:
        """Generate SQL queries for a specific aspect"""
        
        aspect_id = aspect.get('aspect_id', 'unknown')
        user_question = state.get('current_question', state.get('original_question', ''))
        
        # Get SQL templates from aspect
        sql_templates = aspect.get('sql', {})
        meta = aspect.get('meta', {})
        thresholds = aspect.get('thresholds', {})
        
        sql_generation_prompt = f"""
        Healthcare Finance SQL Generation Task:
        
        User Question: "{user_question}"
        Aspect ID: "{aspect_id}"
        
        Aspect Metadata: {json.dumps(meta, indent=2)}
        Thresholds: {json.dumps(thresholds, indent=2)}
        SQL Templates: {json.dumps(sql_templates, indent=2)}
        
        Instructions:
        - Generate executable SQL queries based on the templates
        - Replace placeholders like {{fact_table}}, {{where}}, {{target_adherence}} with actual values
        - Use appropriate WHERE clauses based on the user question context
        - Ensure queries are optimized and return meaningful results
        - Generate one SQL query for each template provided
        
        Response format (return EXACTLY this JSON structure):
        {{
            "query_1_name": "SELECT ...",
            "query_2_name": "SELECT ...",
            ...
        }}
        """
        
        try:
            # Call LLM to generate SQL
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": sql_generation_prompt}
            ])
            
            sql_queries = json.loads(llm_response)
            print(f"✅ Generated {len(sql_queries)} SQL queries for aspect: {aspect_id}")
            return sql_queries
            
        except Exception as e:
            print(f"❌ SQL generation failed for aspect {aspect_id}: {str(e)}")
            # Return basic fallback queries
            fact_table = meta.get('fact_table', 'unknown_table')
            return {
                'basic_query': f"SELECT * FROM {fact_table} LIMIT 100"
            }
    
    def _execute_aspect_queries(self, sql_queries: Dict[str, str], aspect: Dict) -> Dict[str, List]:
        """Execute all SQL queries for an aspect"""
        
        aspect_id = aspect.get('aspect_id', 'unknown')
        query_results = {}
        
        for query_name, sql_query in sql_queries.items():
            try:
                print(f"🔍 Executing query '{query_name}' for aspect: {aspect_id}")
                
                # Execute SQL query
                results = self.db_client.execute_sql(sql_query)
                query_results[query_name] = results
                
                print(f"✅ Query '{query_name}' returned {len(results) if results else 0} rows")
                
            except Exception as e:
                print(f"❌ Query '{query_name}' failed for aspect {aspect_id}: {str(e)}")
                query_results[query_name] = []
        
        return query_results
    
    def _generate_aspect_narrative(self, aspect: Dict, query_results: Dict, state: AgentState) -> str:
        """Generate narrative for a single aspect based on its results"""
        
        aspect_id = aspect.get('aspect_id', 'unknown')
        user_question = state.get('current_question', state.get('original_question', ''))
        
        narrative_templates = aspect.get('narrative', {})
        meta = aspect.get('meta', {})
        
        narrative_prompt = f"""
        Healthcare Finance Narrative Generation Task:
        
        User Question: "{user_question}"
        Aspect ID: "{aspect_id}"
        Aspect Description: "{meta.get('description', '')}"
        
        Query Results: {json.dumps(query_results, indent=2, default=str)}
        Narrative Templates: {json.dumps(narrative_templates, indent=2)}
        
        Instructions:
        - Analyze the query results and generate a comprehensive narrative
        - Use the narrative templates as guidance but create original content
        - Focus on key insights, trends, and actionable findings
        - Include specific numbers and metrics from the results
        - Make it business-friendly and actionable
        - Structure with summary and drill-down details
        
        Generate a clear, insightful narrative about this aspect of the analysis.
        """
        
        try:
            # Call LLM to generate narrative
            narrative = self.db_client.call_claude_api([
                {"role": "user", "content": narrative_prompt}
            ])
            
            print(f"✅ Generated narrative for aspect: {aspect_id}")
            return narrative
            
        except Exception as e:
            print(f"❌ Narrative generation failed for aspect {aspect_id}: {str(e)}")
            return f"Analysis completed for {aspect_id} but narrative generation failed: {str(e)}"
    
    def _generate_consolidated_narrative(self, aspect_results: List[Dict], user_question: str) -> str:
        """Generate final consolidated narrative from all aspect results"""
        
        print(f"🔍 Step 3: Generating consolidated narrative from {len(aspect_results)} aspects")
        
        # Prepare summary of all aspect results
        aspect_summaries = []
        for result in aspect_results:
            if result.get('success', False):
                aspect_summaries.append({
                    'aspect_id': result.get('aspect_id'),
                    'narrative': result.get('narrative', ''),
                    'key_findings': f"Analysis of {result.get('aspect_id')} completed successfully"
                })
            else:
                aspect_summaries.append({
                    'aspect_id': result.get('aspect_id'),
                    'narrative': result.get('narrative', ''),
                    'key_findings': f"Analysis of {result.get('aspect_id')} encountered issues"
                })
        
        consolidation_prompt = f"""
        Healthcare Finance Root Cause Analysis - Final Report Generation:
        
        Original User Question: "{user_question}"
        
        Individual Aspect Analysis Results:
        {json.dumps(aspect_summaries, indent=2)}
        
        Instructions:
        - Create a comprehensive, executive-level summary of the root cause analysis
        - Synthesize insights from all aspects into a cohesive narrative
        - Identify the most critical findings and their business impact
        - Provide actionable recommendations based on the analysis
        - Structure the response with:
          1. Executive Summary
          2. Key Findings by Aspect
          3. Root Cause Insights
          4. Recommended Actions
        
        Generate a professional, comprehensive root cause analysis report.
        """
        
        try:
            # Call LLM to generate consolidated narrative
            consolidated_narrative = self.db_client.call_claude_api([
                {"role": "user", "content": consolidation_prompt}
            ])
            
            print(f"✅ Generated consolidated narrative")
            return consolidated_narrative
            
        except Exception as e:
            print(f"❌ Consolidated narrative generation failed: {str(e)}")
            
            # Fallback: concatenate all individual narratives
            fallback_narrative = f"# Root Cause Analysis Results\n\n"
            fallback_narrative += f"**Question:** {user_question}\n\n"
            
            for result in aspect_results:
                fallback_narrative += f"## {result.get('aspect_id', 'Unknown Aspect')}\n"
                fallback_narrative += f"{result.get('narrative', 'No narrative available')}\n\n"
            
            return fallback_narrative
    
    def process_clarification_response(self, state: AgentState) -> Dict[str, any]:
        """Handle clarification responses if needed"""
        
        user_clarification = state.get('current_question', '')
        original_question = state.get('original_question', '')
        
        print(f"🔍 Processing clarification response: '{user_clarification}'")
        
        # Re-run analysis with clarification context
        enhanced_state = state.copy()
        enhanced_state['current_question'] = f"{original_question} | User clarification: {user_clarification}"
        
        return self.analyze_root_cause(enhanced_state)
