import requests
import json
from typing import Dict, List, Optional, Any
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class SQLGeneratorAgent:
    """High-quality SQL Generator with retry logic and result synthesis"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        self.max_retries = 3
    
    def generate_and_execute_sql(self, state: AgentState) -> Dict[str, Any]:
        """Main function: Generate SQL, execute with retry, and synthesize results"""
        
        print(f"\n🔧 SQL Generator: Processing question")
        
        try:
            # 1. Extract context from state
            context = self._extract_context(state)
            
            # 2. Generate initial SQL
            print(f"  📝 Generating SQL...")
            sql_query = self._generate_sql(context)
            
            # 3. Execute with retry logic
            print(f"  ⚡ Executing SQL with retry logic...")
            execution_result = self._execute_sql_with_retry(sql_query, context)
            
            if execution_result['success']:
                # 4. Synthesize results into narrative
                print(f"  📖 Synthesizing results...")
                narrative = self._synthesize_results(
                    execution_result['data'], 
                    context['current_question'],
                    sql_query
                )
                
                return {
                    'success': True,
                    'sql_query': execution_result['final_sql'],
                    'query_results': execution_result['data'],
                    'narrative_response': narrative,
                    'execution_attempts': execution_result['attempts'],
                    'row_count': len(execution_result['data']) if execution_result['data'] else 0
                }
            else:
                return {
                    'success': False,
                    'error': execution_result['error'],
                    'failed_sql': sql_query,
                    'execution_attempts': execution_result['attempts']
                }
        
        except Exception as e:
            return {
                'success': False,
                'error': f"SQL Generation failed: {str(e)}",
                'execution_attempts': 0
            }
    
    def _extract_context(self, state: AgentState) -> Dict:
        """Extract context from state"""
        
        # Get last 6 questions from history
        questions_history = state.get('user_questions_history', [])
        recent_history = questions_history[-6:] if len(questions_history) > 6 else questions_history
        
        # Get dataset metadata
        available_datasets = state.get('available_datasets', [])
        selected_dataset = state.get('selected_dataset')
        dataset_metadata = state.get('dataset_metadata', {})
        
        # Current question
        current_question = state.get('current_question', '')
        
        return {
            'recent_history': recent_history,
            'available_datasets': available_datasets,
            'selected_dataset': selected_dataset,
            'dataset_metadata': dataset_metadata,
            'current_question': current_question
        }
    
    def _generate_sql(self, context: Dict) -> str:
        """Generate high-quality Databricks SQL with excellent prompts"""
        
        # Build table schema information
        table_info = ""
        if context['dataset_metadata'].get('table_kg'):
            try:
                kg_data = json.loads(context['dataset_metadata']['table_kg'])
                columns = kg_data.get('columns', [])
                
                table_info = f"""
TABLE SCHEMA for {context['selected_dataset']}:
Description: {context['dataset_metadata'].get('description', 'No description')}

Columns:
"""
                for col in columns:
                    col_name = col.get('column_name', 'unknown')
                    data_type = col.get('data_type', 'unknown')
                    table_info += f"- {col_name} ({data_type})\n"
            except:
                table_info = f"TABLE: {context['selected_dataset']}\nDescription: {context['dataset_metadata'].get('description', 'Healthcare finance table')}"
        
        # Build history context
        history_context = ""
        if context['recent_history']:
            history_context = "PREVIOUS QUESTIONS CONTEXT:\n"
            for i, prev_q in enumerate(context['recent_history'], 1):
                history_context += f"{i}. {prev_q}\n"
        
        sql_prompt = f"""
You are an expert Healthcare Finance SQL analyst. Generate a HIGH-QUALITY Databricks SQL query.

CURRENT QUESTION: "{context['current_question']}"

{history_context}

{table_info}

CRITICAL SQL REQUIREMENTS:
1. **METRICS & AGGREGATIONS**: When user asks for metrics (costs, amounts, counts, totals, averages), ALWAYS use GROUP BY with relevant attributes
   - Example: "pharmacy costs" → GROUP BY therapeutic_class, month, etc.
   - Example: "claim counts" → GROUP BY service_type, provider, etc.
   - Example: "member utilization" → GROUP BY age_group, state, plan_type, etc.

2. **DATABRICKS COMPATIBILITY**:
   - Use backticks for table names: `{context['selected_dataset']}`
   - Use standard SQL functions (SUM, COUNT, AVG, MAX, MIN)
   - Use DATE functions: date_trunc(), year(), month(), quarter()
   - Use CASE WHEN for conditional logic
   - Use CTEs (WITH clauses) for complex queries

3. **HEALTHCARE FINANCE BEST PRACTICES**:
   - Always include relevant time dimensions (month, quarter, year)
   - Group by meaningful business dimensions (therapeutic class, service type, age group, state)
   - Use appropriate aggregations (SUM for costs, COUNT for claims, AVG for per-member metrics)
   - Include member counts when analyzing costs (for per-member calculations)
   - Filter out null/invalid data appropriately

4. **QUERY STRUCTURE**:
   - SELECT meaningful column names
   - Use descriptive aliases
   - ORDER BY the most relevant dimension (usually amounts DESC or time ASC)
   - LIMIT to reasonable number of rows (typically 50-100 for reports)

5. **COLUMN NAMING**:
   - Use business-friendly names in SELECT
   - Format large numbers appropriately
   - Use ROUND() for currency/percentages

EXAMPLES:
- For "pharmacy costs by therapeutic class": GROUP BY therapeutic_class, ORDER BY total_cost DESC
- For "monthly claim trends": GROUP BY year, month, ORDER BY year, month
- For "member utilization by state": GROUP BY state, ORDER BY member_count DESC

Generate ONLY the SQL query, no explanations. Make it production-ready and optimized.

SQL Query:
"""
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": sql_prompt}
            ])
            
            # Clean up the response
            sql_query = llm_response.strip()
            
            # Remove any markdown formatting
            if sql_query.startswith("```sql"):
                sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
            elif sql_query.startswith("```"):
                sql_query = sql_query.replace("```", "").strip()
            
            return sql_query
            
        except Exception as e:
            raise Exception(f"SQL generation failed: {str(e)}")
    
    def _execute_sql_with_retry(self, initial_sql: str, context: Dict, max_retries: int = 3) -> Dict:
        """Execute SQL with intelligent retry logic"""
        
        current_sql = initial_sql
        errors_history = []
        
        for attempt in range(max_retries):
            try:
                print(f"    🔄 Attempt {attempt + 1}/{max_retries}")
                
                # Execute SQL using our own function
                result = self._execute_databricks_sql(current_sql)
                
                if result['success']:
                    print(f"    ✅ SQL executed successfully on attempt {attempt + 1}")
                    return {
                        'success': True,
                        'data': result['data'],
                        'final_sql': current_sql,
                        'attempts': attempt + 1
                    }
                else:
                    # Capture error for retry
                    error_msg = result['error']
                    errors_history.append(f"Attempt {attempt + 1}: {error_msg}")
                    
                    print(f"    ❌ Attempt {attempt + 1} failed: {error_msg}")
                    
                    # If not last attempt, try to fix the SQL
                    if attempt < max_retries - 1:
                        current_sql = self._fix_sql_with_llm(current_sql, error_msg, errors_history, context)
            
            except Exception as e:
                error_msg = str(e)
                errors_history.append(f"Attempt {attempt + 1}: {error_msg}")
                print(f"    ❌ Attempt {attempt + 1} exception: {error_msg}")
                
                if attempt < max_retries - 1:
                    current_sql = self._fix_sql_with_llm(current_sql, error_msg, errors_history, context)
        
        # All attempts failed
        return {
            'success': False,
            'error': f"All {max_retries} attempts failed. Last error: {errors_history[-1] if errors_history else 'Unknown error'}",
            'final_sql': current_sql,
            'attempts': max_retries,
            'errors_history': errors_history
        }
    
    def _execute_databricks_sql(self, sql_query: str) -> Dict:
        """Execute SQL against Databricks - our own implementation"""
        
        try:
            # Use the same configuration as DatabricksClient
            payload = {
                "warehouse_id": self.db_client.SQL_WAREHOUSE_ID,
                "statement": sql_query,
                "disposition": "INLINE"
            }
            
            response = requests.post(
                self.db_client.sql_api_url,
                headers=self.db_client.headers,
                json=payload,
                timeout=120
            )
            
            response.raise_for_status()
            result = response.json()
            
            # Parse results using same pattern as DatabricksClient
            result_data = result.get("result", {})
            if "data_array" not in result_data:
                return {'success': False, 'error': 'No data returned', 'data': []}
            
            # Get column names from manifest
            cols = [c["name"] for c in result["manifest"]["schema"]["columns"]]
            
            # Convert to list of dictionaries
            data = [dict(zip(cols, row)) for row in result_data["data_array"]]
            
            return {'success': True, 'data': data}
            
        except requests.exceptions.RequestException as e:
            return {'success': False, 'error': f"Request failed: {str(e)}", 'data': []}
        except KeyError as e:
            return {'success': False, 'error': f"Response format error: {str(e)}", 'data': []}
        except Exception as e:
            return {'success': False, 'error': f"Execution error: {str(e)}", 'data': []}
    
    def _fix_sql_with_llm(self, failed_sql: str, error_msg: str, errors_history: List[str], context: Dict) -> str:
        """Use LLM to fix SQL based on error"""
        
        history_text = "\n".join(errors_history) if errors_history else "No previous errors"
        
        fix_prompt = f"""
Fix this Databricks SQL query that failed with an error.

ORIGINAL QUESTION: "{context['current_question']}"
TABLE: {context['selected_dataset']}

FAILED SQL:
{failed_sql}

ERROR MESSAGE:
{error_msg}

PREVIOUS ERRORS:
{history_text}

COMMON DATABRICKS ISSUES TO FIX:
1. Column names - check spelling and existence
2. Table name format - use backticks: `schema.table`
3. Date functions - use Databricks syntax
4. Data types - ensure proper casting
5. GROUP BY - all non-aggregate columns must be in GROUP BY
6. Reserved keywords - use backticks if needed

Generate the CORRECTED SQL query only, no explanations:
"""
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": fix_prompt}
            ])
            
            # Clean up the response
            fixed_sql = llm_response.strip()
            if fixed_sql.startswith("```sql"):
                fixed_sql = fixed_sql.replace("```sql", "").replace("```", "").strip()
            elif fixed_sql.startswith("```"):
                fixed_sql = fixed_sql.replace("```", "").strip()
            
            print(f"    🔧 Generated fixed SQL")
            return fixed_sql
            
        except Exception as e:
            print(f"    ❌ Failed to generate fix: {str(e)}")
            return failed_sql  # Return original if fix generation fails
    
    def _synthesize_results(self, sql_data: List[Dict], question: str, sql_query: str) -> str:
        """Synthesize SQL results into narrative story"""
        
        # Prepare data summary for LLM
        if not sql_data:
            return "No data was found matching your query criteria."
        
        # Get data summary
        row_count = len(sql_data)
        
        # Sample of data for context (first few rows)
        data_sample = sql_data[:5] if len(sql_data) > 5 else sql_data
        
        # Get column names and types
        columns = list(sql_data[0].keys()) if sql_data else []
        
        synthesis_prompt = f"""
You are a Healthcare Finance Data Analyst. Create a clear, insightful narrative story from SQL results.

USER QUESTION: "{question}"

SQL QUERY EXECUTED:
{sql_query}

DATA RESULTS:
- Total rows: {row_count}
- Columns: {', '.join(columns)}

SAMPLE DATA (first 5 rows):
{json.dumps(data_sample, indent=2, default=str)}

TASK: Create a professional narrative response that:

1. **DIRECTLY ANSWERS** the user's question
2. **HIGHLIGHTS KEY INSIGHTS** from the data
3. **PROVIDES CONTEXT** with specific numbers and metrics
4. **IDENTIFIES PATTERNS** or notable findings
5. **USES BUSINESS LANGUAGE** appropriate for healthcare finance stakeholders

RESPONSE GUIDELINES:
- Start with a direct answer to the question
- Include specific numbers and percentages
- Highlight top performers, trends, or anomalies
- Use professional healthcare finance terminology
- Keep it concise but comprehensive
- End with actionable insights if relevant

Generate a clear, executive-ready response:
"""
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": synthesis_prompt}
            ])
            
            return llm_response.strip()
            
        except Exception as e:
            # Fallback to basic summary
            return f"""
Based on your query "{question}", I found {row_count} records in the dataset.

Key findings:
- Dataset contains {len(columns)} data columns: {', '.join(columns)}
- Total records analyzed: {row_count}

The data has been successfully retrieved and is available for further analysis.
            """.strip()

# Example usage and testing
if __name__ == "__main__":
    from core.databricks_client import DatabricksClient
    
    # Initialize
    db_client = DatabricksClient()
    sql_agent = SQLGeneratorAgent(db_client)
    
    # Test scenarios
    test_scenarios = [
        {
            'question': 'What are the top 5 therapeutic classes by total pharmacy costs?',
            'dataset': 'healthcare_db.claims.pharmacy_claims',
            'expected_sql_pattern': 'GROUP BY therapeutic_class'
        },
        {
            'question': 'Show me monthly medical claim trends for 2024',
            'dataset': 'healthcare_db.claims.medical_claims', 
            'expected_sql_pattern': 'GROUP BY year, month'
        }
    ]
    
    for scenario in test_scenarios:
        print(f"\n🔍 Testing: {scenario['question']}")
        
        # Mock state
        mock_state = {
            'user_questions_history': ['Previous question 1', 'Previous question 2'],
            'current_question': scenario['question'],
            'selected_dataset': scenario['dataset'],
            'dataset_metadata': {
                'description': 'Healthcare claims data',
                'table_kg': json.dumps({
                    'columns': [
                        {'column_name': 'therapeutic_class', 'data_type': 'string'},
                        {'column_name': 'claim_amount', 'data_type': 'decimal'},
                        {'column_name': 'service_date', 'data_type': 'date'}
                    ]
                })
            },
            'available_datasets': []
        }
        
        try:
            result = sql_agent.generate_and_execute_sql(mock_state)
            
            if result['success']:
                print(f"✅ SQL Generated and Executed")
                print(f"📊 Rows returned: {result['row_count']}")
                print(f"🔄 Attempts: {result['execution_attempts']}")
                print(f"📖 Narrative: {result['narrative_response'][:100]}...")
            else:
                print(f"❌ Failed: {result['error']}")
                
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            
    print("\n" + "="*50)
    print("✅ SQL Generator Agent Testing Complete")