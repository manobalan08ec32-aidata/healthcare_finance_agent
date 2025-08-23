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
            if 'error_message' in sql_query:
                # Return immediately if SQL generation fails
                return {
                    'success': False,
                    'error': sql_query['error_message']
                }
            
            # 3. Execute with retry logic
            print(f"  ⚡ Executing SQL with retry logic...")
            execution_result = self._execute_sql_with_retry(sql_query, context)
            if 'error_message' in execution_result:
            # Return immediately if SQL execution fails
                return {
                    'success': False,
                    'error': execution_result['error_message']
                }
            
            if execution_result['success']:
                # 4. Synthesize results into narrative
                print(f"  📖 Synthesizing results...")
                narrative = self._synthesize_results(
                    execution_result['data'], 
                    context['current_question'],
                    sql_query
                )

                if 'error_message' in execution_result:
                    return {
                        'success': False,
                        'error': execution_result['error_message']
                    }
                else:

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
        """Generate high-quality Databricks SQL with excellent prompts and retry logic"""

        # Build table schema information from available_datasets
        current_question = context.get('current_question', '')
        recent_history = context.get('recent_history', [])
        dataset_metadata = context.get('dataset_metadata', '')
        selected_dataset = context.get('selected_dataset', '')

        sql_prompt = f"""
            You are a highly skilled Healthcare Finance SQL analyst. Your task is to generate a **high-quality Databricks SQL query** based on the user's question.

            CURRENT QUESTION: {current_question}

            RECENT HISTORY: {recent_history}

            AVAILABLE METADATA: {dataset_metadata}

            ==============================
            CRITICAL SQL GENERATION RULES
            ==============================

            1. **METRICS & AGGREGATIONS**
            - If the question includes metrics (e.g., costs, amounts, counts, totals, averages), use appropriate aggregation functions (SUM, COUNT, AVG) and include GROUP BY clauses with relevant business dimensions.
            - When the questions has only month then use current year for calculation.
            2. **SPECIAL INSTRUCTION**
            - When the user question involves comparing actuals vs forecast vs budget, or asks for actuals overall, and includes product categories such as Specialty, Home Delivery, and PBM (or mentions PBM alone), you must NOT apply GROUP BY on product_category.
            - You must also NOT apply any filtering on product_category in the WHERE clause in these cases.
            - Instead, return an overall summary view without restricting or segmenting by product_category.

            3. **ATTRIBUTE-ONLY QUERIES**
            - If the question asks only about attributes (e.g., member age, drug name, provider type) and does NOT request metrics, return only the relevant columns without aggregation.

            4. **STRING FILTERING - CASE INSENSITIVE**
            - When filtering on text/string columns, always use UPPER() function on BOTH sides for case-insensitive matching.
            - Example: WHERE UPPER(product_category) = UPPER('Specialty')

            5. **TOP/BOTTOM QUERIES WITH TOTALS**
            - When user asks for "top 10" or "bottom 10", also include the overall total/count for context.
            - Show both the individual top/bottom records AND the grand total across all records.
            - Let the LLM decide the best SQL structure to achieve this (CTE, subquery, etc.).

            6. **HEALTHCARE FINANCE BEST PRACTICES**
            - Always include time dimensions (month, quarter, year) when relevant with user question.
            - Use business-friendly dimensions (e.g., therapeutic class, service type, age group, state).

            7. **SHOW CALCULATION COMPONENTS**
            - Include underlying metrics/attributes used in calculations as separate columns.
            - Show filter/group attribute values in results (e.g., if filtering by 'Specialty', include product_category column).
            - For variance: show [Value1], [Value2], [Variance]. For percentages: show [Numerator], [Denominator], [Percentage].

            8. **DATABRICKS SQL COMPATIBILITY**
            - Use standard SQL functions: SUM, COUNT, AVG, MAX, MIN
            - Use date functions: date_trunc(), year(), month(), quarter()
            - Use CASE WHEN for conditional logic
            - Use CTEs (WITH clauses) for complex logic

            9. **FORMATTING**
            - Show whole number for metrics and round it to two decimal for percentages
            - Use order by clause only for date columns and use desc order

            RESPONSE FORMAT:
            The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.

            If the question is clear and SQL can be generated:
            {{
            "sql_query": "your generated SQL query here"
            }}

            """
        
        max_retries = 3
        retry_count = 0
        backoff_time = 1  # Start with 1 second backoff

        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_uhg_openai_api([
                    {"role": "user", "content": sql_prompt}
                ])
                print('sql gen',llm_response)
                
                # Parse JSON response
                try:
                    response_json = json.loads(llm_response.strip())
                    sql_query = response_json.get('sql_query', '').strip()
                    sql_query = sql_query.replace('`', '')  # Remove backtick characters
                    
                    if not sql_query:
                        raise ValueError("Empty SQL query in JSON response")
                    
                    return sql_query
                
                except json.JSONDecodeError as e:
                    raise Exception(f"Failed to parse SQL generation JSON response: {str(e)}")
            
            except Exception as e:
                retry_count += 1
                print(f"❌ SQL generation attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying SQL generation... (Attempt {retry_count}/{max_retries})")
                    import time
                    time.sleep(backoff_time)
                    backoff_time *= 2  # Exponential backoff
                else:
                    return {
                    'success': False,
                    'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }
        
    
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
        """Use LLM to fix SQL based on error with enhanced prompting and retry logic"""
    
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
            
            RESPONSE FORMAT:
            The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.

            If the query is fixed: {{ "fixed_sql_query": "your corrected SQL query here" }}
            """
        
        max_retries = 3
        retry_count = 0
        backoff_time = 1  # Start with 1 second backoff

        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_uhg_openai_api([
                    {"role": "user", "content": fix_prompt}
                ])
                print('sql fix',llm_response)
                try:
                    response_json = json.loads(llm_response.strip())
                    sql_query = response_json.get('fixed_sql_query', '').strip()
                    fixed_sql = sql_query.replace('`', '')  # Remove backtick characters
                    
                    if not fixed_sql:
                        raise ValueError("Empty fixed SQL query in JSON response")
                    
                    return fixed_sql

                except json.JSONDecodeError as e:
                    raise Exception(f"Failed to parse SQL fix JSON response: {str(e)}")
            
            except Exception as e:
                retry_count += 1
                print(f"❌ SQL fix attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying SQL fix... (Attempt {retry_count}/{max_retries})")
                    import time
                    time.sleep(backoff_time)
                    backoff_time *= 2  # Exponential backoff
                else:
                    print(f"❌ SQL fix failed after {max_retries} retries.")
                    return {
                    'success': False,
                    'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }
    
    def _synthesize_results(self, sql_data: List[Dict], question: str, sql_query: str) -> str:
        """Synthesize SQL results into narrative story with retry logic"""

        # Prepare data summary for LLM
        if not sql_data:
            return "No data was found matching your query criteria."

        # Get data summary
        row_count = len(sql_data)
        columns = list(sql_data[0].keys()) if sql_data else []
        column_count = len(columns)
        total_count = row_count * column_count

        # If too many records, skip LLM synthesis
        if total_count > 2000:
            return "Too many records to synthesize."

        # Enhanced data analysis for better narrative decisions
        has_multiple_records = row_count > 1
        has_date_columns = any('date' in col.lower() or 'month' in col.lower() or 'year' in col.lower() or 'quarter' in col.lower() for col in columns)
        has_numeric_columns = False
        
        # Check if we have numeric data for trend analysis
        if sql_data:
            sample_row = sql_data[0]
            for value in sample_row.values():
                if isinstance(value, (int, float)) and value != 0:
                    has_numeric_columns = True
                    break

        synthesis_prompt = f"""
        You are a Healthcare Finance Data Analyst. Create a clear, factual narrative from SQL results.

        USER QUESTION: "{question}"

        SQL QUERY EXECUTED:
        {sql_query}

        DATA ANALYSIS:
        - Total rows: {row_count}
        - Columns: {', '.join(columns)}
        - Has multiple records: {has_multiple_records}
        - Contains date/time data: {has_date_columns}
        - Contains numeric data: {has_numeric_columns}

        SAMPLE DATA (first 5 rows):
        {json.dumps(sql_data, indent=2, default=str)}

        CRITICAL INSTRUCTIONS:

        1. **DATA SUFFICIENCY CHECK**:
        - If there is only 1 record OR insufficient data for meaningful analysis, respond: "Not enough data to create comprehensive narrative analysis."
        - Only proceed with full narrative if there are multiple records with meaningful patterns to analyze

        2. **NARRATIVE CONTENT** (only if sufficient data exists):
        - **DIRECTLY ANSWER** the user's question with specific numbers
        - **IDENTIFY TRENDS** across time periods (if date columns exist)
        - **HIGHLIGHT ANOMALIES** or outliers in the data
        - **SHOW VARIANCES** between different categories/groups

        3. **STRICT PROHIBITIONS**:
        - NO recommendations or suggestions
        - NO "should do" or "consider" statements
        - NO future predictions or advice
        - ONLY factual observations from the data

        4. **FORMAT REQUIREMENTS**:
        - Keep response concise (3-4 sentences max)
        - Include specific numbers in billion or Million and percentages
        - Focus on what the data shows, not what to do about it
        - Use professional healthcare finance terminology
        5. **USE EXACT DATA VALUES**:
        - Use ONLY the exact names, values, and terms that appear in the SQL results
        - Do NOT invent, rename, or modify any names from the data
        - Do NOT create generic categories or simplified labels
        - Keep all therapeutic classes, product categories, and other names exactly as they appear in the results
        - When referencing calculations or trends, use the precise column names and values from the query output

        
        EXAMPLES:

        **Good Response (sufficient data):**
        "The data shows Specialty product category generated $3.61 billion in revenue during July 2025, representing 45% of total revenue. Oncology therapeutic class led with $1.2 billion (33% of Specialty revenue), followed by Rare Diseases at $890 million (25%). Revenue variance between top and bottom therapeutic classes spans $1.1 billion, indicating significant concentration in high-cost specialty treatments."
        **Good Response (insufficient data):**
        "Not enough data to create comprehensive narrative analysis."
        **Bad Response (contains recommendations):**
        "Revenue is high, so you should focus more on Oncology and consider expanding Rare Diseases coverage." ❌"

        RESPONSE FORMAT:
        The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.

        {{
        "narrative_response": "your factual narrative analysis here OR 'Not enough data to create comprehensive narrative analysis.'"
        }}
        """

        max_retries = 3
        retry_count = 0
        backoff_time = 1  # Start with 1 second backoff

        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_uhg_openai_api([
                    {"role": "user", "content": synthesis_prompt}
                ])
                print('sql narr',llm_response)
                # Parse JSON response
                try:
                    response_json = json.loads(llm_response.strip())
                    narrative = response_json.get('narrative_response', '').strip()
                    
                    # Fallback check - if narrative is empty or too short
                    if not narrative or len(narrative) < 10:
                        raise ValueError("Empty or insufficient narrative in JSON response")
                    
                    return narrative

                except json.JSONDecodeError as e:
                    raise Exception(f"Failed to parse narrative JSON response: {str(e)}")
            
            except Exception as e:
                retry_count += 1
                print(f"❌ Narrative synthesis attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying narrative synthesis... (Attempt {retry_count}/{max_retries})")
                    import time
                    time.sleep(backoff_time)
                    backoff_time *= 2  # Exponential backoff
                return {
                    'success': False,
                    'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }
