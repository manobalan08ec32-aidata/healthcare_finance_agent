import requests
import json
import time
from typing import List, Dict, Any, Optional
import datetime

class DatabricksClient:
    """Databricks client for SQL execution, vector search, and Claude API calls"""
    
    def __init__(self):
        self.DATABRICKS_HOST = "https://adb-1446028976895628.8.azuredatabricks.net"
        self.DATABRICKS_TOKEN = "************"  # Use environment variable in production
        self.SQL_WAREHOUSE_ID = "86fe3eb6b45135bb"
        self.VECTOR_TBL_INDEX = "prd_optumrx_orxfdmprdsa.rag.table_chunks"
        self.LLM_MODEL = "databricks-claude-sonnet-4"
        self.SESSION_TABLE = "prd_optumrx_orxfdmprdsa.rag.session_state"
        
        self.headers = {
            "Authorization": f"Bearer {self.DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        }
        
        self.sql_api_url = f"{self.DATABRICKS_HOST}/api/2.0/sql/statements/"
        self.llm_api_url = f"{self.DATABRICKS_HOST}/serving-endpoints/{self.LLM_MODEL}/invocations"
    
    def execute_sql(self, sql_query: str, timeout: int = 300) -> List[Dict]:
        """Execute SQL query and return results with proper async handling"""
        
        payload = {
            "warehouse_id": self.SQL_WAREHOUSE_ID,
            "statement": sql_query,
            "disposition": "INLINE",
            "wait_timeout": "10s"  # Short initial wait, then we'll poll
        }
        
        try:
            response = requests.post(
                self.sql_api_url,
                headers=self.headers,
                json=payload,
                timeout=60  # HTTP request timeout (not query timeout)
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Debug: Print response structure
            print(f"Databricks response keys: {result.keys()}")
            
            # Check if query completed immediately
            status = result.get('status', {})
            state = status.get('state', '')
            
            print(f"Initial query state: {state}")
            
            if state == 'SUCCEEDED':
                # Query completed immediately
                return self._extract_results(result)
            
            elif state in ['PENDING', 'RUNNING']:
                # Query is still running, poll for results
                statement_id = result.get('statement_id')
                if statement_id:
                    print(f"🔄 Query still running, polling for results (timeout: {timeout}s)")
                    return self._poll_for_results(statement_id, timeout)
                else:
                    raise Exception("Query is running but no statement_id provided")
            
            elif state == 'FAILED':
                error_message = status.get('error', {}).get('message', 'Unknown error')
                raise Exception(f"Query failed: {error_message}")
            
            elif state == 'CANCELED':
                raise Exception("Query was canceled")
            
            else:
                # Fallback - try to extract results anyway
                return self._extract_results(result)
                    
        except requests.exceptions.RequestException as e:
            raise Exception(f"SQL execution failed: {str(e)}")
        except KeyError as e:
            raise Exception(f"Unexpected response format: Missing key {str(e)}")
        except Exception as e:
            raise Exception(f"Unexpected error in SQL execution: {str(e)}")
    
    def _extract_results(self, result: Dict) -> List[Dict]:
        """Extract results from Databricks response"""
        
        result_data = result.get("result", {})
        if "data_array" not in result_data:
            return []
        
        # Manifest is at top level
        if "manifest" not in result:
            return []
            
        cols = [c["name"] for c in result["manifest"]["schema"]["columns"]]
        return [dict(zip(cols, row)) for row in result_data["data_array"]]
    
    def _poll_for_results(self, statement_id: str, timeout: int = 300) -> List[Dict]:
        """Poll for query results until completion with progressive backoff"""
        
        print(f"🔄 Polling for results of statement {statement_id} (max {timeout}s)")
        
        start_time = time.time()
        poll_interval = 2  # Start with 2 second intervals
        max_poll_interval = 30  # Cap at 30 seconds
        
        while time.time() - start_time < timeout:
            try:
                elapsed = time.time() - start_time
                print(f"  ⏱️ Elapsed: {elapsed:.1f}s, checking status...")
                
                # Get statement status
                status_url = f"{self.sql_api_url}{statement_id}"
                response = requests.get(status_url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                result = response.json()
                status = result.get('status', {})
                state = status.get('state', '')
                
                print(f"    Status: {state}")
                
                if state == 'SUCCEEDED':
                    print(f"  ✅ Query completed successfully after {elapsed:.1f}s")
                    return self._extract_results(result)
                
                elif state == 'FAILED':
                    error_message = status.get('error', {}).get('message', 'Unknown error')
                    raise Exception(f"Query failed after {elapsed:.1f}s: {error_message}")
                
                elif state == 'CANCELED':
                    raise Exception(f"Query was canceled after {elapsed:.1f}s")
                
                elif state in ['PENDING', 'RUNNING']:
                    # Still running, wait and poll again
                    print(f"    Still running, waiting {poll_interval}s before next check...")
                    time.sleep(poll_interval)
                    
                    # Progressive backoff: increase interval up to max
                    poll_interval = min(poll_interval * 1.5, max_poll_interval)
                    continue
                
                else:
                    print(f"    Unknown state: {state}, continuing to poll...")
                    time.sleep(poll_interval)
                    continue
                    
            except requests.exceptions.RequestException as e:
                print(f"    ⚠️ Network error while polling: {str(e)}, retrying...")
                time.sleep(poll_interval)
                continue
        
        raise Exception(f"Query timed out after {timeout} seconds")

    def vector_search_tables(self, query_text: str, num_results: int = 5, index_name: str = None, timeout: int = 600) -> List[Dict]:
        """Search table chunks using vector search with extended timeout"""
        
        # Use provided index or default
        search_index = index_name if index_name else self.VECTOR_TBL_INDEX
        
        # Escape single quotes properly
        escaped_query = query_text.replace("'", "''")
        
        sql_query = f"""
        SELECT table_name, table_summary as content, table_kg 
        FROM VECTOR_SEARCH(
            index => '{search_index}',
            query_text => '{escaped_query}',
            num_results => {num_results}
        )
        """
        
        print(f"⏱️ Using timeout: {timeout}s")
        
        try:
            start_time = time.time()
            results = self.execute_sql(sql_query, timeout=timeout)
            elapsed = time.time() - start_time
            
            print(f"✅ Vector search completed in {elapsed:.1f}s, returned {len(results)} results")
            
            return results
            
        except Exception as e:
            elapsed = time.time() - start_time if 'start_time' in locals() else 0
            print(f"❌ Vector search failed after {elapsed:.1f}s: {str(e)}")
            raise Exception(f"Vector search failed: {str(e)}")

    
    def call_claude_api(self, messages: List[Dict], system_prompt: str = None, max_tokens: int = 15000) -> str:
        """Call Databricks-hosted Claude endpoint"""
        
        payload = {
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0.1,
            "top_p": 0.9
        }
        
        if system_prompt:
            payload["system"] = system_prompt
        
        try:
            response = requests.post(
                self.llm_api_url,
                headers=self.headers,
                json=payload,
                timeout=120
            )
            response.raise_for_status()
            
            result = response.json()
            return result['choices'][0]['message']['content']
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Claude API call failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Unexpected error in Claude API call: {str(e)}")
    
    def save_session_state(self, state: Dict) -> bool:
        """Save session state to Databricks table"""
        
        try:
            state_string = json.dumps(state).replace("'", "''")
            insert_sql = f"""
            INSERT INTO {self.SESSION_TABLE} (session_id, state_json, timestamp)
            VALUES ('{state["session_id"]}', '{state_string}', '{datetime.datetime.now(datetime.timezone.utc).isoformat()}')
            """
            
            self.execute_sql(insert_sql)
            return True
            
        except Exception as e:
            print(f"Failed to save session state: {str(e)}")
            return False
    
    def load_session_state(self, session_id: str) -> Optional[Dict]:
        """Load latest session state from Databricks table"""
        
        try:
            sql_query = f"""
            SELECT state_json, timestamp 
            FROM {self.SESSION_TABLE} 
            WHERE session_id = '{session_id}' 
            ORDER BY timestamp DESC 
            LIMIT 1
            """
            
            results = self.execute_sql(sql_query)
            
            if results:
                return json.loads(results[0]['state_json'])
            else:
                return None
                
        except Exception as e:
            print(f"Failed to load session state: {str(e)}")
            return None
    
    def get_table_metadata(self, table_name: str) -> Optional[Dict]:
        """Get detailed metadata for a specific table"""
        
        try:
            sql_query = f"""
            SELECT table_name, table_summary, table_kg
            FROM {self.VECTOR_TBL_INDEX.replace('VECTOR_SEARCH', '')}
            WHERE table_name = '{table_name}'
            """
            
            results = self.execute_sql(sql_query)
            return results[0] if results else None
            
        except Exception as e:
            print(f"Failed to get table metadata: {str(e)}")
            return None
    
    def test_connection(self) -> bool:
        """Test Databricks connection with better error handling"""
        try:
            # Simple test query
            test_sql = "SELECT 1 as test_column"
            results = self.execute_sql(test_sql)
            print('result', results)
            
            # Check if we got any results
            if results and len(results) > 0:
                first_result = results[0]
                test_value = None
                
                print(f"First result: {first_result}")
                
                if 'test_column' in first_result:
                    test_value = first_result['test_column']
                    print(f"Found test_column: {test_value} (type: {type(test_value)})")
                elif 'col_0' in first_result:
                    test_value = first_result['col_0']
                    print(f"Found col_0: {test_value} (type: {type(test_value)})")
                elif len(first_result) > 0:
                    test_value = list(first_result.values())[0]
                    print(f"Found first value: {test_value} (type: {type(test_value)})")
                
                return test_value
                
            else:
                print("No results returned from test query")
                return False
            
        except Exception as e:
            # ✅ FIX: Actually raise the exception to see what's wrong
            print(f"Exception in test_connection: {e}")
            import traceback
            traceback.print_exc()
            raise  # This will show the full error

# Example usage and testing
if __name__ == "__main__":
    client = DatabricksClient()
    
    # Test connection
    if client.test_connection():
        print("✅ Databricks connection successful")
        
        # Test vector search
        try:
            results = client.vector_search_tables("claims transaction data", 3)
            print(f"✅ Vector search successful, found {len(results)} tables")
            for result in results:
                print(f"  - {result['table_name']}")
        except Exception as e:
            print(f"❌ Vector search failed: {e}")
        
        # Test Claude API
        try:
            response = client.call_claude_api([
                {"role": "user", "content": "What is healthcare finance?"}
            ])
            print(f"✅ Claude API successful: {response[:100]}...")
        except Exception as e:
            print(f"❌ Claude API failed: {e}")
    else:
        print("❌ Databricks connection failed")
