import requests
import json
import time
from typing import List, Dict, Any, Optional
import datetime
import os
import openai 
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential, get_bearer_token_provider


class DatabricksClient:
    """Databricks client for SQL execution, vector search, and Claude API calls"""
    
    def __init__(self):
        # self.DATABRICKS_HOST =  os.getenv("DATABRICKS_HOST")
        # self.DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN") # Use environment variable in production
        # self.SQL_WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID")
        # self.DATABRICKS_LLM_TOKEN = os.getenv("DATABRICKS_LLM_TOKEN")
        # self.DATABRICKS_LLM_HOST = os.getenv("DATABRICKS_LLM_HOST")
        # self.UHG_CLIENT_ID = os.getenv("UHG_CLIENT_ID")
        # self.UHG_CLIENT_SECRET = os.getenv("UHG_CLIENT_SECRET")
        # self.UHG_PROJECT_ID = os.getenv("UHG_PROJECT_ID")
        
        self.DATABRICKS_HOST =  os.getenv("DATABRICKS_HOST")
        self.DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN") # Use environment variable in production
        self.SQL_WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID")
        self.DATABRICKS_LLM_TOKEN = os.getenv("DATABRICKS_LLM_TOKEN")
        self.DATABRICKS_LLM_HOST = os.getenv("DATABRICKS_LLM_HOST")
        self.UHG_CLIENT_ID = os.getenv("UHG_CLIENT_ID")
        self.UHG_CLIENT_SECRET = os.getenv("UHG_CLIENT_SECRET")
        self.UHG_PROJECT_ID = os.getenv("UHG_PROJECT_ID")
        
        self.VECTOR_TBL_INDEX = "prd_optumrx_orxfdmprdsa.rag.table_chunks"
        self.LLM_MODEL = "databricks-claude-sonnet-4"
        self.SESSION_TABLE = "prd_optumrx_orxfdmprdsa.rag.session_state"
        self.ROOTCAUSE_INDEX = "prd_optumrx_orxfdmprdsa.rag.rootcause_chunks"  # Add this
        self.UHG_AUTH_URL = "https://api.uhg.com/oauth2/token"
        self.UHG_SCOPE = "https://api.uhg.com/.default"
        self.UHG_GRANT_TYPE = "client_credentials" 
        self.UHG_DEPLOYMENT_NAME = "gpt-4o_2024-11-20"
        self.UHG_ENDPOINT = "https://api.uhg.com/api/cloud/api-management/ai-gateway/1.0"
        self.UHG_API_VERSION = "2025-01-01-preview"
        self.CLAUDE_AUTH_URL = "https://api.uhg.com/oauth2/token"
        self.CLAUDE_SCOPE = "https://api.uhg.com/.default"
        self.CLAUDE_GRANT_TYPE = "client_credentials"
        self.CLAUDE_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"
        self.CLAUDE_API_URL = "https://api.uhg.com/api/cloud/api-management/ai-gateway/1.0"
        self.CLAUDE_PROJECT_ID= os.getenv("CLAUDE_PROJECT_ID")
        self.CLAUDE_CLIENT_ID = os.getenv("CLAUDE_CLIENT_ID")
        self.CLAUDE_CLIENT_SECRET = os.getenv("CLAUDE_CLIENT_SECRET")
        
        
        self.headers = {
            "Authorization": f"Bearer {self.DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        }

        self.llm_headers = {
            "Authorization": f"Bearer {self.DATABRICKS_LLM_TOKEN}",
            "Content-Type": "application/json",
        }
        
        self.sql_api_url = f"{self.DATABRICKS_HOST}/api/2.0/sql/statements/"
        self.llm_api_url = f"{self.DATABRICKS_LLM_HOST}/serving-endpoints/{self.LLM_MODEL}/invocations"

    def call_claude_api_endpoint(self, messages: list, max_tokens: int = 25000, temperature: float = 0.1, top_p: float = 0.8, system_prompt: str = "you are an AI assistant") -> str:
        """
        Call UHG Claude API endpoint using HCP credentials and return the response text.

        Args:
            messages: List of message dicts with 'role' and 'content' keys
            max_tokens: Maximum tokens in response (default: 25000)
            temperature: Controls randomness (0.0-1.0, default: 0.1)
            top_p: Controls diversity (0.0-1.0, default: 0.9)  
            system_prompt: System prompt for the assistant
        """
        BASE_URL = self.CLAUDE_API_URL
        PROJECT_ID = self.CLAUDE_PROJECT_ID
        MODEL_ID = self.CLAUDE_MODEL_ID

        CONFIG = {
            "HCP_CLIENT_ID": self.CLAUDE_CLIENT_ID,
            "HCP_CLIENT_SECRET": self.CLAUDE_CLIENT_SECRET,
            "HCP_AUTH": self.CLAUDE_AUTH_URL,
            "HCP_SCOPE": self.CLAUDE_SCOPE,
            "HCP_GRANT_TYPE": self.CLAUDE_GRANT_TYPE
        }

        def fetch_hcp_token(config):
            data = {
                "grant_type": config["HCP_GRANT_TYPE"],
                "client_id": config["HCP_CLIENT_ID"],
                "client_secret": config["HCP_CLIENT_SECRET"],
                "scope": config["HCP_SCOPE"],
            }
            resp = requests.post(config["HCP_AUTH"], data=data)
            resp.raise_for_status()
            return resp.json()["access_token"]

        try:
            # Step 1: Get token
            token = fetch_hcp_token(CONFIG)

            # Step 2: Convert messages to API format
            api_messages = []
            for msg in messages:
                api_messages.append({
                    "role": msg["role"],
                    "content": [{"text": msg["content"]}]
                })

            # Step 3: Prepare headers and payload with temperature and top_p
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
                "projectId": PROJECT_ID,
                "guardrail": "high_strength"
            }
            payload = {
                "system": [{"text": system_prompt}],
                "messages": api_messages,
                "inferenceConfig": {
                    "maxTokens": max_tokens,
                    "temperature": temperature,
                    "topP": top_p
                }
            }
            url = f"{BASE_URL}/model/{MODEL_ID}/converse"
            response = requests.post(url, headers=headers, json=payload, timeout=120)

            # Add debugging information
            print(f"Status code: {response.status_code}")
            if response.status_code != 200:
                print(f"Error response: {response.text}")
            response.raise_for_status()

            result = response.json()
            # Try to extract the response text - adjust based on actual response structure
            try:
                return result["output"]["message"]["content"][0]["text"]
            except KeyError as e:
                print(f"KeyError accessing response: {e}")
                print(f"Available keys: {list(result.keys())}")
                # Fallback - return the full result as string for debugging
                return str(result)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            print(f"Response content: {e.response.text if hasattr(e, 'response') else 'No response content'}")
            raise Exception(f"Claude API endpoint call failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Claude API endpoint call failed: {str(e)}")

    
    
    def vector_search_columns(self, query_text: str, num_results: int = 20, index_name: str = None, timeout: int = 600, tables_list: list = None) -> List[Dict]:
        """Search table chunks using vector search with extended timeout and table filter"""

        # Use provided index or default
        search_index = index_name if index_name else "prd_optumrx_orxfdmprdsa.rag.column_embeddings_idx"

        # Escape single quotes properly
        escaped_query = query_text.replace("'", "''")

        # Prepare WHERE clause for table_name IN (...)
        where_clause = ""
        if tables_list and len(tables_list) > 0:
            # Safely quote each table name
            quoted_tables = [f"'{t}'" for t in tables_list]
            tables_str = ", ".join(quoted_tables)
            where_clause = f"WHERE table_name IN ({tables_str})"

        sql_query = f"""
        SELECT table_name, llm_context
        FROM VECTOR_SEARCH(
            index => '{search_index}',
            query_text => '{escaped_query}',
            num_results => {num_results},
            query_type => 'hybrid'
        )
        {where_clause}
        """
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
    
    def execute_sql(self, sql_query: str, timeout: int = 500) -> List[Dict]:
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
                    poll_interval = min(poll_interval * 1, max_poll_interval)
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

    


