import asyncio
import aiohttp
import json
import time
from typing import List, Dict, Any, Optional
import datetime
import os

class DatabricksClient:
    """Databricks client with async support for SQL execution, vector search, and Claude API calls"""
    
    def __init__(self):
        # Keep all your existing __init__ code exactly the same
        self.DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
        self.DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
        self.SQL_WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID")
        self.DATABRICKS_LLM_TOKEN = os.getenv("DATABRICKS_LLM_TOKEN")
        self.DATABRICKS_LLM_HOST = os.getenv("DATABRICKS_LLM_HOST")
        self.UHG_CLIENT_ID = os.getenv("UHG_CLIENT_ID")
        self.UHG_CLIENT_SECRET = os.getenv("UHG_CLIENT_SECRET")
        self.UHG_PROJECT_ID = os.getenv("UHG_PROJECT_ID")
        
        # ... all your existing constants ...
        self.CLAUDE_PROJECT_ID = os.getenv("CLAUDE_PROJECT_ID")
        self.CLAUDE_CLIENT_ID = os.getenv("CLAUDE_CLIENT_ID")
        self.CLAUDE_CLIENT_SECRET = os.getenv("CLAUDE_CLIENT_SECRET")
        
        # Keep existing headers
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

    # Keep your existing sync method for backward compatibility
    def call_claude_api_endpoint(self, messages: list, max_tokens: int = 25000, temperature: float = 0.1, top_p: float = 0.8, system_prompt: str = "you are an AI assistant") -> str:
        """Original sync version - keep for backward compatibility"""
        # Your existing implementation stays exactly the same
        pass  # (keeping your original code)

    async def call_claude_api_endpoint_async(self, messages: list, max_tokens: int = 25000, temperature: float = 0.1, top_p: float = 0.8, system_prompt: str = "you are an AI assistant") -> str:
        """
        Async version of Claude API endpoint call using HCP credentials.
        
        Args:
            messages: List of message dicts with 'role' and 'content' keys
            max_tokens: Maximum tokens in response (default: 25000)
            temperature: Controls randomness (0.0-1.0, default: 0.1)
            top_p: Controls diversity (0.0-1.0, default: 0.8)  
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

        async def fetch_hcp_token_async(config):
            """Async token fetching"""
            data = {
                "grant_type": config["HCP_GRANT_TYPE"],
                "client_id": config["HCP_CLIENT_ID"],
                "client_secret": config["HCP_CLIENT_SECRET"],
                "scope": config["HCP_SCOPE"],
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(config["HCP_AUTH"], data=data) as resp:
                    resp.raise_for_status()
                    result = await resp.json()
                    return result["access_token"]

        try:
            # Step 1: Get token asynchronously
            token = await fetch_hcp_token_async(CONFIG)

            # Step 2: Convert messages to API format (same as before)
            api_messages = []
            for msg in messages:
                api_messages.append({
                    "role": msg["role"],
                    "content": [{"text": msg["content"]}]
                })

            # Step 3: Prepare headers and payload
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

            # Step 4: Make async API call
            url = f"{BASE_URL}/model/{MODEL_ID}/converse"
            
            timeout = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    # Add debugging information
                    print(f"Status code: {response.status}")
                    if response.status != 200:
                        error_text = await response.text()
                        print(f"Error response: {error_text}")
                        response.raise_for_status()

                    result = await response.json()
                    
                    # Extract response text (same logic as before)
                    try:
                        return result["output"]["message"]["content"][0]["text"]
                    except KeyError as e:
                        print(f"KeyError accessing response: {e}")
                        print(f"Available keys: {list(result.keys())}")
                        return str(result)

        except aiohttp.ClientError as e:
            print(f"HTTP Error: {e}")
            raise Exception(f"Claude API endpoint call failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Claude API endpoint call failed: {str(e)}")

    # Also convert SQL execution to async for completeness
    async def execute_sql_async(self, sql_query: str, timeout: int = 500) -> List[Dict]:
        """Async version of SQL execution"""
        
        payload = {
            "warehouse_id": self.SQL_WAREHOUSE_ID,
            "statement": sql_query,
            "disposition": "INLINE",
            "wait_timeout": "10s"
        }
        
        try:
            timeout_config = aiohttp.ClientTimeout(total=60)
            async with aiohttp.ClientSession(timeout=timeout_config) as session:
                async with session.post(
                    self.sql_api_url,
                    headers=self.headers,
                    json=payload
                ) as response:
                    response.raise_for_status()
                    result = await response.json()
            
            # Debug: Print response structure
            print(f"Databricks response keys: {result.keys()}")
            
            # Check if query completed immediately
            status = result.get('status', {})
            state = status.get('state', '')
            
            print(f"Initial query state: {state}")
            
            if state == 'SUCCEEDED':
                return self._extract_results(result)
            elif state in ['PENDING', 'RUNNING']:
                statement_id = result.get('statement_id')
                if statement_id:
                    print(f"Query still running, polling for results (timeout: {timeout}s)")
                    return await self._poll_for_results_async(statement_id, timeout)
                else:
                    raise Exception("Query is running but no statement_id provided")
            elif state == 'FAILED':
                error_message = status.get('error', {}).get('message', 'Unknown error')
                raise Exception(f"Query failed: {error_message}")
            elif state == 'CANCELED':
                raise Exception("Query was canceled")
            else:
                return self._extract_results(result)
                    
        except aiohttp.ClientError as e:
            raise Exception(f"SQL execution failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Unexpected error in SQL execution: {str(e)}")

    async def _poll_for_results_async(self, statement_id: str, timeout: int = 300) -> List[Dict]:
        """Async version of polling for query results"""
        
        print(f"Polling for results of statement {statement_id} (max {timeout}s)")
        
        start_time = time.time()
        poll_interval = 2
        max_poll_interval = 30
        
        while time.time() - start_time < timeout:
            try:
                elapsed = time.time() - start_time
                print(f"  Elapsed: {elapsed:.1f}s, checking status...")
                
                status_url = f"{self.sql_api_url}{statement_id}"
                timeout_config = aiohttp.ClientTimeout(total=30)
                
                async with aiohttp.ClientSession(timeout=timeout_config) as session:
                    async with session.get(status_url, headers=self.headers) as response:
                        response.raise_for_status()
                        result = await response.json()
                
                status = result.get('status', {})
                state = status.get('state', '')
                
                print(f"    Status: {state}")
                
                if state == 'SUCCEEDED':
                    print(f"  Query completed successfully after {elapsed:.1f}s")
                    return self._extract_results(result)
                elif state == 'FAILED':
                    error_message = status.get('error', {}).get('message', 'Unknown error')
                    raise Exception(f"Query failed after {elapsed:.1f}s: {error_message}")
                elif state == 'CANCELED':
                    raise Exception(f"Query was canceled after {elapsed:.1f}s")
                elif state in ['PENDING', 'RUNNING']:
                    print(f"    Still running, waiting {poll_interval}s before next check...")
                    await asyncio.sleep(poll_interval)  # Non-blocking sleep!
                    poll_interval = min(poll_interval * 1, max_poll_interval)
                    continue
                else:
                    print(f"    Unknown state: {state}, continuing to poll...")
                    await asyncio.sleep(poll_interval)
                    continue
                    
            except aiohttp.ClientError as e:
                print(f"    Network error while polling: {str(e)}, retrying...")
                await asyncio.sleep(poll_interval)
                continue
        
        raise Exception(f"Query timed out after {timeout} seconds")

    # Keep your existing _extract_results method unchanged
    def _extract_results(self, result: Dict) -> List[Dict]:
        """Extract results from Databricks response (unchanged)"""
        result_data = result.get("result", {})
        if "data_array" not in result_data:
            return []
        
        if "manifest" not in result:
            return []
            
        cols = [c["name"] for c in result["manifest"]["schema"]["columns"]]
        return [dict(zip(cols, row)) for row in result_data["data_array"]]
