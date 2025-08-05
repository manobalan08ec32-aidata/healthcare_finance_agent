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

    def _extract_context(self, state: Dict) -> Dict:
        """Extract context from state, including extracting table_kg from dataset_metadata"""
        # # Get last 6 questions from history
        questions_history = state.get('questions_sql_history', [])
        recent_history = questions_history[-4:]

        # Get dataset info from dataset_metadata and extract table_kg
        dataset_metadata = state.get('dataset_metadata', {})
        dataset_name = state.get('selected_dataset')
        current_question = state.get('current_question', state.get('original_question', ''))

        return {
            'recent_history': recent_history,
            'dataset_metadata': dataset_metadata,
            'dataset_name': dataset_name,
            'current_question': current_question
        }
    
    def _generate_sql(self, context: Dict) -> str:
        """Generate high-quality Databricks SQL with excellent prompts"""
    
        # Build table schema information from available_datasets
        current_question = context.get('current_question', '')
        recent_history = context.get('recent_history', [])
        dataset_metadata = context.get('dataset_metadata', '')
        selected_dataset = context.get('selected_dataset', '')
        table_info = f"table used - prd_optumrx_orxfdmprdsa.rag.{selected_dataset}"

        
        sql_prompt = f"""
            You are a highly skilled Healthcare Finance SQL analyst. Your task is to generate a **high-quality Databricks SQL query** based on the user's question.

            CURRENT QUESTION: {current_question}

            RECENT HISTORY: {recent_history}

            AVAILABLE METADATA: {dataset_metadata}

            TABLE INFORMATION: {table_info}

            ==============================
            CRITICAL SQL GENERATION RULES
            ==============================

            1. METRICS & AGGREGATIONS
            - If the question includes metrics (e.g., costs, amounts, counts, totals, averages), use appropriate aggregation functions (SUM, COUNT, AVG) and include GROUP BY clauses with relevant business dimensions.

            2. ATTRIBUTE-ONLY QUERIES
            - If the question asks only about attributes (e.g., member age, drug name, provider type) and does NOT request metrics, return only the relevant columns without aggregation.

            4. HEALTHCARE FINANCE BEST PRACTICES
            - Always include time dimensions (month, quarter, year) when relevant with user question.
            - Use business-friendly dimensions (e.g., therapeutic class, service type, age group, state).

            5. DATABRICKS SQL COMPATIBILITY
            - Use standard SQL functions: SUM, COUNT, AVG, MAX, MIN
            - Use date functions: date_trunc(), year(), month(), quarter()
            - Use CASE WHEN for conditional logic
            - Use CTEs (WITH clauses) for complex logic

            6. QUERY STRUCTURE
            - SELECT meaningful column names with descriptive aliases
            - use order by clause only for date columns and use desc order

            7. COLUMN NAMING
            - Use business-friendly names in SELECT
            - Format large numbers appropriately
            - Use ROUND() for currency and percentages

            ==============================
            RESPONSE FORMAT
            ==============================

            If the question is clear and SQL can be generated:
            {{
            "sql_query": "your generated SQL query here"
            }}

            Do NOT include any commentary, markdown, or formatting outside the JSON.
            """
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": sql_prompt}
            ])
            print('sql prompt',sql_prompt)
            print('sql llm response',llm_response)
            # Parse JSON response
            try:
                response_json = json.loads(llm_response.strip())
                sql_query = response_json.get('sql_query', '').strip()
                sql_query = sql_query.replace('`', '')  # Remove backtick characters
                print('sql query', sql_query)
                
                if not sql_query:
                    raise ValueError("Empty SQL query in JSON response")
                
                return sql_query
                
            except json.JSONDecodeError as e:
                raise Exception(f"Failed to parse SQL generation JSON response: {str(e)}")
            
        except Exception as e:
            raise Exception(f"SQL generation failed: {str(e)}")
        
    
    def _execute_sql_with_retry(self, initial_sql: str, context: Dict, max_retries: int = 3) -> Dict:
        """Execute SQL with intelligent retry logic and async handling"""
        
        current_sql = initial_sql
        errors_history = []
        
        for attempt in range(max_retries):
            try:                
                # Execute SQL with async handling
                result = self._execute_databricks_sql(current_sql, timeout=300)
                
                if result['success']:
                    print(f"    ✅ SQL executed successfully on attempt {attempt + 1}")
                    return {
                        'success': True,
                        'data': result['data'],
                        'final_sql': current_sql,
                        'attempts': attempt + 1,
                        'execution_time': result.get('execution_time', 0)
                    }
                else:
                    # Capture error for retry
                    error_msg = result['error']
                    errors_history.append(f"Attempt {attempt + 1}: {error_msg}")
                                        
                    # If not last attempt, try to fix the SQL
                    if attempt < max_retries - 1:
                        current_sql = self._fix_sql_with_llm(current_sql, error_msg, errors_history, context)
            
            except Exception as e:
                error_msg = str(e)
                errors_history.append(f"Attempt {attempt + 1}: {error_msg}")
                
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
    
    def _execute_databricks_sql(self, sql_query: str, timeout: int = 600) -> Dict:
        """Execute SQL against Databricks with proper async handling"""
        
        import time
        
        try:
            start_time = time.time()
            
            # Use the same configuration as DatabricksClient with async handling
            payload = {
                "warehouse_id": self.db_client.SQL_WAREHOUSE_ID,
                "statement": sql_query,
                "disposition": "INLINE",
                "wait_timeout": "5s"  # Short initial wait, then we'll poll
            }
            
            response = requests.post(
                self.db_client.sql_api_url,
                headers=self.db_client.headers,
                json=payload,
                timeout=60  # HTTP request timeout (not query timeout)
            )
            
            response.raise_for_status()
            result = response.json()            
            # Check if query completed immediately
            status = result.get('status', {})
            state = status.get('state', '')
            
            
            if state == 'SUCCEEDED':
                # Query completed immediately
                data = self._extract_sql_results(result)
                execution_time = time.time() - start_time
                return {
                    'success': True, 
                    'data': data,
                    'execution_time': execution_time
                }
            
            elif state in ['PENDING', 'RUNNING']:
                # Query is still running, poll for results
                statement_id = result.get('statement_id')
                if statement_id:
                    data = self._poll_sql_results(statement_id, timeout, start_time)
                    execution_time = time.time() - start_time
                    return {
                        'success': True, 
                        'data': data,
                        'execution_time': execution_time
                    }
                else:
                    return {
                        'success': False, 
                        'error': 'Query is running but no statement_id provided',
                        'data': []
                    }
            
            elif state == 'FAILED':
                error_message = status.get('error', {}).get('message', 'Unknown error')
                return {
                    'success': False, 
                    'error': f"Query failed: {error_message}",
                    'data': []
                }
            
            elif state == 'CANCELED':
                return {
                    'success': False, 
                    'error': 'Query was canceled',
                    'data': []
                }
            
            else:
                # Fallback - try to extract results anyway
                try:
                    data = self._extract_sql_results(result)
                    execution_time = time.time() - start_time
                    return {
                        'success': True, 
                        'data': data,
                        'execution_time': execution_time
                    }
                except:
                    return {
                        'success': False, 
                        'error': f'Unknown query state: {state}',
                        'data': []
                    }
            
        except requests.exceptions.RequestException as e:
            return {'success': False, 'error': f"Request failed: {str(e)}", 'data': []}
        except KeyError as e:
            return {'success': False, 'error': f"Response format error: {str(e)}", 'data': []}
        except Exception as e:
            return {'success': False, 'error': f"Execution error: {str(e)}", 'data': []}
    
    def _extract_sql_results(self, result: Dict) -> List[Dict]:
        """Extract results from Databricks response"""
        
        result_data = result.get("result", {})
        if "data_array" not in result_data:
            return []
        
        # Manifest is at top level
        if "manifest" not in result:
            return []
            
        cols = [c["name"] for c in result["manifest"]["schema"]["columns"]]
        return [dict(zip(cols, row)) for row in result_data["data_array"]]
    
    def _poll_sql_results(self, statement_id: str, timeout: int = 300, start_time: float = None) -> List[Dict]:
        """Poll for query results until completion with progressive backoff"""
        
        import time
        
        if start_time is None:
            start_time = time.time()        
        poll_interval = 2  # Start with 2 second intervals
        max_poll_interval = 30  # Cap at 30 seconds
        
        while time.time() - start_time < timeout:
            try:
                elapsed = time.time() - start_time                
                # Get statement status
                status_url = f"{self.db_client.sql_api_url}{statement_id}"
                response = requests.get(status_url, headers=self.db_client.headers, timeout=30)
                response.raise_for_status()
                
                result = response.json()
                status = result.get('status', {})
                state = status.get('state', '')
                                
                if state == 'SUCCEEDED':
                    return self._extract_sql_results(result)
                
                elif state == 'FAILED':
                    error_message = status.get('error', {}).get('message', 'Unknown error')
                    raise Exception(f"Query failed after {elapsed:.1f}s: {error_message}")
                
                elif state == 'CANCELED':
                    raise Exception(f"Query was canceled after {elapsed:.1f}s")
                
                elif state in ['PENDING', 'RUNNING']:
                    # Still running, wait and poll again
                    time.sleep(poll_interval)
                    
                    # Progressive backoff: increase interval up to max
                    poll_interval = min(poll_interval * 1.5, max_poll_interval)
                    continue
                
                else:
                    time.sleep(poll_interval)
                    continue
                    
            except requests.exceptions.RequestException as e:
                time.sleep(poll_interval)
                continue
        
        raise Exception(f"Query timed out after {timeout} seconds")
    
    def _fix_sql_with_llm(self, failed_sql: str, error_msg: str, errors_history: List[str], context: Dict) -> str:
        """Use LLM to fix SQL based on error with enhanced prompting"""
        
        history_text = "\n".join(errors_history) if errors_history else "No previous errors"
        current_question = context.get('current_question', '')
        recent_history = context.get('recent_history', [])
        dataset_metadata = context.get('dataset_metadata', '')
        dataset_name = context.get('dataset_name', '')
        
        fix_prompt = f"""
            You are an expert Databricks SQL developer. A SQL query has **FAILED** and needs to be **FIXED or REWRITTEN**.

            ==============================
            CONTEXT
            ==============================
            - ORIGINAL USER QUESTION: "{current_question}"
            - TARGET TABLE: `prd_optumrx_orxfdmprdsa.rag.{dataset_name}`
            - TABLE METADATA: {dataset_metadata}

            ==============================
            FAILURE DETAILS
            ==============================
            - FAILED SQL QUERY:
            ```sql
            {failed_sql}
            ERROR MESSAGE: {error_msg}

            PREVIOUS RETRY ERRORS: {history_text}

            ============================== INSTRUCTIONS
            Identify the issue based on the error message and metadata.
            Fix the SQL syntax or rewrite the query if needed.
            Ensure the corrected query answers the original user question.
            Use only valid column names and Databricks-compatible SQL.
            ============================== RESPONSE FORMAT
            Return ONLY valid JSON with no markdown, explanations, or extra text:

            If the query is fixed: {{ "fixed_sql_query": "your corrected SQL query here" }}

            If the query cannot be fixed due to ambiguity: {{ "follow_up_question": "Ask a clarifying question to better understand the user's intent" }} """
                    
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": fix_prompt}
            ])
            
            try:
                response_json = json.loads(llm_response.strip())
                sql_query = response_json.get('fixed_sql_query', '').strip()
                fixed_sql = sql_query.replace('`', '')  # Remove backtick characters
                
                if not fixed_sql:
                    return failed_sql
                
                return fixed_sql
                
            except json.JSONDecodeError as e:
                return failed_sql  # Return original if JSON parsing fails
            
        except Exception as e:
            return failed_sql  # Return original if fix generation fails
    
    def _synthesize_results(self, sql_data: List[Dict], question: str, sql_query: str) -> str:
        """Synthesize SQL results into narrative story"""

        # Prepare data summary for LLM
        if not sql_data:
            return "No data was found matching your query criteria."

        # Get data summary
        row_count = len(sql_data)
        columns = list(sql_data[0].keys()) if sql_data else []
        column_count = len(columns)
        total_count = row_count * column_count

        # If too many records, skip LLM synthesis
        if total_count > 5000:
            return "Too many records to synthesize."

        synthesis_prompt = f"""
                You are a Healthcare Finance Data Analyst. Create a clear, insightful narrative story from SQL results.

                USER QUESTION: "{question}"

                SQL QUERY EXECUTED:
                {sql_query}

                DATA RESULTS:
                - Total rows: {row_count}
                - Columns: {', '.join(columns)}

                SAMPLE DATA (first 5 rows):
                {json.dumps(sql_data, indent=2, default=str)}

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
                - Keep it concise with 4-5 lines maximum.
                - End with actionable insights if relevant

                IMPORTANT: Return your response as EXACT JSON format with no other text, formatting, or markdown:

                {{
                "narrative_response": "your complete narrative analysis here"
                }}

                Do not include any explanations, markdown formatting, or other text outside the JSON.
                """

        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": synthesis_prompt}
            ])

            # Parse JSON response
            try:
                response_json = json.loads(llm_response.strip())
                narrative = response_json.get('narrative_response', '').strip()
                return narrative

            except json.JSONDecodeError as e:
                # Fallback to basic summary
                return

        except Exception as e:
            # Fallback to basic summary
            return
