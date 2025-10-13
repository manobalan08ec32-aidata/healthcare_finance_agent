import asyncio
import aiohttp
import json
import time
from typing import List, Dict, Any, Optional
import datetime
import os
import requests
from databricks.vector_search.client import VectorSearchClient
from databricks.vector_search.reranker import DatabricksReranker
from dotenv import load_dotenv
load_dotenv()

class DatabricksClient:
    """Databricks client with async support for SQL execution, vector search, and Claude API calls"""
    
    def __init__(self):
        
        # API Constants
        self.VECTOR_TBL_INDEX = "prd_optumrx_orxfdmprdsa.rag.table_chunks"
        self.LLM_MODEL = "databricks-claude-sonnet-4"
        self.SESSION_TABLE = "prd_optumrx_orxfdmprdsa.rag.session_state"
        self.ROOTCAUSE_INDEX = "prd_optumrx_orxfdmprdsa.rag.rootcause_chunks"
        self.UHG_AUTH_URL = "https://api.uhg.com/oauth2/token"
        self.UHG_SCOPE = "https://api.uhg.com/.default"
        self.UHG_GRANT_TYPE = "client_credentials"
        self.UHG_DEPLOYMENT_NAME = "gpt-4o_2024-11-20"
        self.UHG_ENDPOINT = "https://api.uhg.com/api/cloud/api-management/ai-gateway/1.0"
        self.UHG_API_VERSION = "2025-01-01-preview"
        

        self.DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
        self.DATABRICKS_LLM_HOST = os.getenv("DATABRICKS_LLM_HOST")
        self.DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
        self.DATABRICKS_LLM_TOKEN = os.getenv("DATABRICKS_LLM_TOKEN")
        self.SQL_WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID")
        self.UHG_PROJECT_ID = os.getenv("UHG_PROJECT_ID")
        self.UHG_CLIENT_ID = os.getenv("UHG_CLIENT_ID")
        self.UHG_CLIENT_SECRET = os.getenv("UHG_CLIENT_SECRET")
        
        # Claude API Configuration
        self.CLAUDE_PROJECT_ID = os.getenv("CLAUDE_PROJECT_ID")
        self.CLAUDE_CLIENT_ID = os.getenv("CLAUDE_CLIENT_ID")
        self.CLAUDE_CLIENT_SECRET = os.getenv("CLAUDE_CLIENT_SECRET")

        self.DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
        self.DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")
        self.DATABRICKS_TENANT_ID = os.getenv("DATABRICKS_TENANT_ID")
        self.DATABRICKS_ADB_ID = os.getenv("DATABRICKS_ADB_ID")

        self.CLAUDE_AUTH_URL = "https://api.uhg.com/oauth2/token"
        self.CLAUDE_SCOPE = "https://api.uhg.com/.default"
        self.CLAUDE_GRANT_TYPE = "client_credentials"
        self.CLAUDE_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"
        self.CLAUDE_API_URL = "https://api.uhg.com/api/cloud/api-management/ai-gateway/1.0"

        # Headers for API calls
        self.headers = {
            "Authorization": f"Bearer {self.DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        }
        self.llm_headers = {
            "Authorization": f"Bearer {self.DATABRICKS_LLM_TOKEN}",
            "Content-Type": "application/json",
        }
        
        # API URLs
        self.sql_api_url = f"{self.DATABRICKS_HOST}/api/2.0/sql/statements/"
        self.llm_api_url = f"{self.DATABRICKS_LLM_HOST}/serving-endpoints/{self.LLM_MODEL}/invocations"

        # Runtime (non-serialized) attributes for improved async performance
        self._http_session: Optional[aiohttp.ClientSession] = None
        # More conservative timeouts for better network reliability
        self._http_session_timeout = aiohttp.ClientTimeout(
            total=180,  # Increased total timeout
            connect=15,  # Increased connection timeout
            sock_read=90,  # Increased read timeout
            sock_connect=15  # Increased socket connection timeout
        )
        self._claude_token: Optional[str] = None
        self._claude_token_expiry: Optional[float] = None  # epoch seconds
        self._token_skew_seconds = 60  # refresh 1 minute before expiry
        self._llm_semaphore = asyncio.Semaphore(3)  # limit concurrent LLM calls

        # ---- Acquire Azure AD access token for Databricks ----
        token_url = f"https://login.microsoftonline.com/{self.DATABRICKS_TENANT_ID}/oauth2/v2.0/token"
        token_payload = {
            "grant_type": "client_credentials",
            "client_id": self.DATABRICKS_CLIENT_ID,
            "client_secret": self.DATABRICKS_CLIENT_SECRET,
            "scope": f"{self.DATABRICKS_ADB_ID}/.default"
        }
        token_resp = requests.post(token_url, data=token_payload)
        token_resp.raise_for_status()
        self.access_token = token_resp.json()["access_token"]
        self.token_headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazily create / reuse a single aiohttp session for the client lifetime."""
        # When using asyncio.run() per request, the event loop is closed each time.
        # If an existing session is bound to a closed loop, recreate it to avoid
        # 'Event loop is closed' errors on follow-up questions.
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        recreate = False
        if self._http_session is None:
            recreate = True
        else:
            # Session object might have a _loop attribute (implementation detail)
            session_loop = getattr(self._http_session, '_loop', None)
            if self._http_session.closed:
                recreate = True
            elif session_loop is not None:
                # If previous loop is closed or differs from current running loop, recreate
                if session_loop.is_closed():
                    recreate = True
                elif current_loop is not None and session_loop is not current_loop:
                    recreate = True

        if recreate:
            if self._http_session and not self._http_session.closed:
                try:
                    await self._http_session.close()
                except Exception:
                    pass
            self._http_session = aiohttp.ClientSession(timeout=self._http_session_timeout)
        return self._http_session

    async def close(self):
        """Explicitly close underlying HTTP session (optional call on app shutdown)."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    async def _fetch_hcp_token_async(self) -> str:
        """Fetch a new token from HCP auth service and cache it with expiry."""
        data = {
            "grant_type": self.CLAUDE_GRANT_TYPE,
            "client_id": self.CLAUDE_CLIENT_ID,
            "client_secret": self.CLAUDE_CLIENT_SECRET,
            "scope": self.CLAUDE_SCOPE,
        }
        session = await self._get_session()
        async with session.post(self.CLAUDE_AUTH_URL, data=data) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise Exception(f"Auth token fetch failed ({resp.status}): {text[:300]}")
            result = json.loads(text)
            access_token = result.get("access_token")
            expires_in = result.get("expires_in", 3000)
            self._claude_token = access_token
            self._claude_token_expiry = time.time() + expires_in
            return access_token

    async def _get_claude_token(self) -> str:
        """Return a cached token if still valid; otherwise fetch a new one."""
        if self._claude_token and self._claude_token_expiry:
            if time.time() < (self._claude_token_expiry - self._token_skew_seconds):
                return self._claude_token
        return await self._fetch_hcp_token_async()

    async def call_claude_api_endpoint_async(self, messages: list, max_tokens: int = 8000, temperature: float = 0.1, top_p: float = 0.8, system_prompt: str = "you are an AI assistant") -> str:
        """
        Async version of Claude API endpoint call using HCP credentials.
        Features: token caching, session reuse, retries, concurrency limiting
        """
        BASE_URL = self.CLAUDE_API_URL
        PROJECT_ID = self.CLAUDE_PROJECT_ID
        MODEL_ID = self.CLAUDE_MODEL_ID

        try:
            # Concurrency guard to prevent overwhelming the gateway
            async with self._llm_semaphore:
                # Step 1: Get (cached) token
                token = await self._get_claude_token()

                # Step 2: Convert messages to API format
                api_messages = []
                for msg in messages:
                    api_messages.append({
                        "role": msg["role"],
                        "content": [{"text": msg["content"]}]
                    })

                # Step 3: Prepare headers and payload
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Authorization": f"Bearer {token}",
                    "projectId": PROJECT_ID,
                    "guardrail": "high_strength",
                    "User-Agent": "agentbot-async/1.0"
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

                # Retry (exponential backoff) for transient gateway errors
                transient_statuses = {502, 503, 504}
                backoff = 1.0
                max_retries = 3
                session = await self._get_session()
                last_error_text = None
                
                for attempt in range(1, max_retries + 1):
                    try:
                        async with session.post(url, headers=headers, json=payload) as response:
                            status = response.status
                            if status == 401:
                                # Token might be expired unexpectedly; force refresh once
                                if attempt == 1:
                                    print("401 received – refreshing token and retrying once")
                                    self._claude_token = None
                                    token = await self._get_claude_token()
                                    headers["Authorization"] = f"Bearer {token}"
                                    continue
                            if status not in (200,):
                                error_text = await response.text()
                                last_error_text = error_text
                                print(f"Claude call failed (status={status}) attempt={attempt}: {error_text[:400]}")
                                if status in transient_statuses and attempt < max_retries:
                                    await asyncio.sleep(backoff)
                                    backoff *= 2
                                    continue
                                response.raise_for_status()
                            result = await response.json()
                            try:
                                return result["output"]["message"]["content"][0]["text"]
                            except KeyError as e:
                                print(f"Response parsing KeyError: {e}; raw keys: {list(result.keys())}")
                                return json.dumps(result)[:2000]
                    except aiohttp.ClientError as ce:
                        print(f"Network client error attempt={attempt}: {ce}")
                        if attempt < max_retries:
                            await asyncio.sleep(backoff)
                            backoff *= 2
                            continue
                        raise
                raise Exception(f"Claude API endpoint call failed after retries. Last error: {last_error_text[:400] if last_error_text else 'No body'}")

        except aiohttp.ClientError as e:
            print(f"HTTP Error: {e}")
            raise Exception(f"Claude API endpoint call failed: {str(e)}")
        except Exception as e:
            raise Exception(f"Claude API endpoint call failed: {str(e)}")

    # Also convert SQL execution to async with session reuse and enhanced retry logic
    async def execute_sql_async(self, sql_query: str, timeout: int = 300) -> List[Dict]:
        """Async version of SQL execution with enhanced network retry logic and session reuse"""
        
        payload = {
            "warehouse_id": self.SQL_WAREHOUSE_ID,
            "statement": sql_query,
            "disposition": "INLINE",
            "wait_timeout": "10s"
        }
        
        # Retry configuration for network issues
        max_retries = 3
        base_delay = 2.0  # Start with 2 seconds
        
        for attempt in range(max_retries):
            try:
                session = await self._get_session()
                
                print(f"SQL execution attempt {attempt + 1}/{max_retries}")
                
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
                error_str = str(e)
                print(f"Network error on attempt {attempt + 1}: {error_str}")
                
                # Check if this is a connection-related error that we should retry
                is_connection_error = any(err_pattern in error_str.lower() for err_pattern in [
                    'connection was forcibly closed',
                    'connection reset',
                    'connection aborted',
                    'connection timeout',
                    'connection refused',
                    'winerror 10054',
                    'winerror 10053',
                    'winerror 10060'
                ])
                
                if is_connection_error and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"Connection error detected, retrying in {delay}s...")
                    
                    # Force recreate session on connection errors
                    if self._http_session and not self._http_session.closed:
                        try:
                            await self._http_session.close()
                        except Exception:
                            pass
                        self._http_session = None
                    
                    await asyncio.sleep(delay)
                    continue
                else:
                    # Not a connection error or max retries reached
                    raise Exception(f"SQL execution failed: {str(e)}")
                    
            except Exception as e:
                error_str = str(e)
                print(f"Unexpected error on attempt {attempt + 1}: {error_str}")
                
                # Don't retry non-network errors unless it's the last attempt
                if attempt < max_retries - 1 and 'connection' in error_str.lower():
                    delay = base_delay * (2 ** attempt)
                    print(f"Connection-related error, retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise Exception(f"Unexpected error in SQL execution: {str(e)}")
        
        # This should not be reached due to the raise statements above
        raise Exception("SQL execution failed after all retry attempts")
    
        # Also convert SQL execution to async with session reuse and enhanced retry logic
    async def execute_sql_async_audit(self, sql_query: str, timeout: int = 300) -> List[Dict]:
        """Async version of SQL execution with enhanced network retry logic and session reuse"""
        
        payload = {
            "warehouse_id": self.SQL_WAREHOUSE_ID,
            "statement": sql_query,
            "disposition": "INLINE",
            "wait_timeout": "10s"
        }
        
        # Retry configuration for network issues
        max_retries = 3
        base_delay = 2.0  # Start with 2 seconds
        
        for attempt in range(max_retries):
            try:
                session = await self._get_session()
                
                print(f"SQL execution attempt {attempt + 1}/{max_retries}")
                
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
                error_str = str(e)
                print(f"Network error on attempt {attempt + 1}: {error_str}")
                
                # Check if this is a connection-related error that we should retry
                is_connection_error = any(err_pattern in error_str.lower() for err_pattern in [
                    'connection was forcibly closed',
                    'connection reset',
                    'connection aborted',
                    'connection timeout',
                    'connection refused',
                    'winerror 10054',
                    'winerror 10053',
                    'winerror 10060'
                ])
                
                if is_connection_error and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"Connection error detected, retrying in {delay}s...")
                    
                    # Force recreate session on connection errors
                    if self._http_session and not self._http_session.closed:
                        try:
                            await self._http_session.close()
                        except Exception:
                            pass
                        self._http_session = None
                    
                    await asyncio.sleep(delay)
                    continue
                else:
                    # Not a connection error or max retries reached
                    raise Exception(f"SQL execution failed: {str(e)}")
                    
            except Exception as e:
                error_str = str(e)
                print(f"Unexpected error on attempt {attempt + 1}: {error_str}")
                
                # Don't retry non-network errors unless it's the last attempt
                if attempt < max_retries - 1 and 'connection' in error_str.lower():
                    delay = base_delay * (2 ** attempt)
                    print(f"Connection-related error, retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise Exception(f"Unexpected error in SQL execution: {str(e)}")
        
        # This should not be reached due to the raise statements above
        raise Exception("SQL execution failed after all retry attempts")

    async def _poll_for_results_async(self, statement_id: str, timeout: int = 300) -> List[Dict]:
        """Async version of polling for query results with 2-second intervals and session reuse"""
        
        print(f"🔄 Polling for results of statement {statement_id} (max {timeout}s)")
        
        start_time = time.time()
        poll_interval = 2  # Fixed 2-second polling interval
        session = await self._get_session()
        
        while time.time() - start_time < timeout:
            try:
                elapsed = time.time() - start_time
                print(f"  ⏱️ Elapsed: {elapsed:.1f}s, checking status...")
                
                status_url = f"{self.sql_api_url}{statement_id}"
                
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
                    print(f"    📊 Still running, waiting {poll_interval}s before next check...")
                    await asyncio.sleep(poll_interval)  # Non-blocking sleep - fixed 2 seconds
                    continue
                else:
                    print(f"    ❓ Unknown state: {state}, continuing to poll...")
                    await asyncio.sleep(poll_interval)
                    continue
                    
            except aiohttp.ClientError as e:
                error_str = str(e)
                print(f"    ⚠️ Network error while polling: {error_str}")
                
                # Check if this is a connection error - if so, recreate session
                is_connection_error = any(err_pattern in error_str.lower() for err_pattern in [
                    'connection was forcibly closed',
                    'connection reset',
                    'connection aborted',
                    'winerror 10054',
                    'winerror 10053'
                ])
                
                if is_connection_error:
                    print(f"    🔄 Connection error detected, recreating session...")
                    # Force recreate session on connection errors
                    if self._http_session and not self._http_session.closed:
                        try:
                            await self._http_session.close()
                        except Exception:
                            pass
                        self._http_session = None
                    session = await self._get_session()
                
                print(f"    ⏱️ Retrying in {poll_interval}s...")
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

    async def vector_search_columns(self, query_text: str, num_results: int = 20, index_name: str = None, timeout: int = 600, tables_list: list = None) -> List[Dict]:
        """Search table chunks using vector search with extended timeout and table filter"""

        # Use provided index or default
        search_index = index_name

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
            print('sql vector',)
            start_time = time.time()
            results = await self.execute_sql_async(sql_query, timeout=timeout)
            elapsed = time.time() - start_time
            print(f"✅ Vector search completed in {elapsed:.1f}s, returned {len(results)} results")
            print('results',results)
            return results

        except Exception as e:
            elapsed = time.time() - start_time if 'start_time' in locals() else 0
            print(f"❌ Vector search failed after {elapsed:.1f}s: {str(e)}")
            raise Exception(f"Vector search failed: {str(e)}")
        
    async def sp_vector_search_columns(
    self, 
    query_text: str, 
    tables_list: List[str], 
    num_results_per_table: int,  # This will be 10 (what you want to return)
    index_name: str
) -> List[Dict]:
        """
        For each table:
        1. Fetch 50 results
        2. Rerank 
        3. Keep only top 10
        4. Append to final results
        """
        print(f"🔍 Searching: '{query_text}' across {len(tables_list)} table(s)")
        
        client = VectorSearchClient(
            workspace_url=self.DATABRICKS_HOST,
            service_principal_client_id=self.DATABRICKS_CLIENT_ID,
            service_principal_client_secret=self.DATABRICKS_CLIENT_SECRET,
            azure_tenant_id=self.DATABRICKS_TENANT_ID
        )
        
        index = client.get_index(
            endpoint_name="metadata_vectore_search_endpoint",
            index_name=index_name
        )
        index._get_token_for_request = lambda: self.access_token
        
        all_results = []
        
        for table_name in tables_list:
            print(f'📋 Searching table: {table_name}')
            try:
                from databricks.vector_search.reranker import DatabricksReranker
                
                # Step 1: Fetch 50 results with reranking
                results = index.similarity_search(
                    query_text=query_text,
                    columns=["table_name", "column_name", "col_embedding_content", "llm_context"],
                    filters={"table_name": table_name},
                    reranker=DatabricksReranker(
                        columns_to_rerank=["column_name", "llm_context"]
                    ),
                    num_results=50,  # ✅ Fetch 50 rows
                    query_type="Hybrid"
                )
                
                # Step 2: Parse results
                if results.get('result', {}).get('data_array'):
                    cols = [c['name'] for c in results['manifest']['columns']]
                    table_results = [dict(zip(cols, row)) for row in results['result']['data_array']]
                    
                    # Step 3: Sort by score and keep only top 20
                    table_results.sort(key=lambda x: x.get('score', 0.0), reverse=True)
                    top_20 = table_results[:10]  # ✅ Keep only top 20

                    # Step 4: Append to all results
                    all_results.extend(top_20)

                else:
                    print(f"  ⚠️ {table_name}: No results")
                    
            except Exception as e:
                print(f"  ❌ {table_name}: Error - {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print(f"\n✅ Total: {len(all_results)} columns from {len(tables_list)} table(s)")
        return all_results
        
#     async def sp_vector_search_columns(
#     self, 
#     query_text: str, 
#     tables_list: List[str],
#     num_results_per_table: int,
#     index_name: str 
# ) -> List[Dict]:
#         """
#         Search for relevant columns across one or multiple tables.
#         Guarantees num_results_per_table from EACH table.
#         """
#         print(f"🔍 Searching: '{query_text}' across {len(tables_list)} table(s)")
        
#         client = VectorSearchClient(
#             workspace_url=self.DATABRICKS_HOST,
#             service_principal_client_id=self.DATABRICKS_CLIENT_ID,
#             service_principal_client_secret=self.DATABRICKS_CLIENT_SECRET,
#             azure_tenant_id=self.DATABRICKS_TENANT_ID
#         )
        
#         index = client.get_index(
#             endpoint_name="metadata_vectore_search_endpoint",
#             index_name=index_name
#         )
#         index._get_token_for_request = lambda: self.access_token
        
#         all_results = []
        
#         for table_name in tables_list:
#             print(f'📋 Searching table: {table_name}')
#             try:
#                 # ✅ FIX: Use DatabricksReranker object (like your working function)
#                 from databricks.vector_search.reranker import DatabricksReranker
                
#                 results = index.similarity_search(
#                     query_text=query_text,
#                     columns=["table_name", "col_embedding_content", "llm_context"],  # Add "score" to columns
#                     filters={"table_name": table_name},
#                     reranker=DatabricksReranker(
#                         columns_to_rerank=["column_name","col_embedding_content"]
#                     ),
#                     num_results=num_results_per_table,
#                     query_type="Hybrid"  # Use ANN, not hybrid for metadata
#                 )
#                 print('Results:', results)
#                 # Parse results
#                 if results.get('result', {}).get('data_array'):
#                     cols = [c['name'] for c in results['manifest']['columns']]
#                     for row in results['result']['data_array']:
#                         all_results.append(dict(zip(cols, row)))
                    
#                     print(f"  ✅ {table_name}: {len(results['result']['data_array'])} columns")
#                 else:
#                     print(f"  ⚠️  {table_name}: No results")
            
#             except Exception as e:
#                 print(f"  ❌ {table_name}: Error - {e}")
#                 import traceback
#                 traceback.print_exc()
#                 continue
        
#         print(f"\n✅ Total: {len(all_results)} columns from {len(tables_list)} table(s)")
        
#         return all_results

    async def renew_vector_search_columns(self, query_text: str, num_results: int = 20, index_name: str = None, tables_list: list = None) -> List[Dict]:
        print(f"🔄 Renewing vector search columns for \nquery_text: {query_text}, \nindex: {index_name}, \ntables_list: {tables_list}")

        # Initialize the VectorSearchClient with service principal authentication
        client = VectorSearchClient(
            workspace_url=self.DATABRICKS_HOST,
            service_principal_client_id=self.DATABRICKS_CLIENT_ID,
            service_principal_client_secret=self.DATABRICKS_CLIENT_SECRET,
            azure_tenant_id=self.DATABRICKS_TENANT_ID
        )

        # Connect to an existing index
        index = client.get_index(
            endpoint_name="/metadata_vectore_search_endpoint",
            index_name=index_name  # "prd_optumrx_orxfdmprdsa.rag.column_embeddings_idx"
        )
        index._get_token_for_request = lambda: self.access_token

        api_url = f"{self.DATABRICKS_HOST}/api/2.1/unity-catalog/tables/{index_name}"#{catalog}.{schema}.{table}"
        response = requests.get(api_url, headers=self.token_headers)
        response.raise_for_status()
        table_info = response.json()
        final_columns = [col['name'] for col in table_info['columns']]
        final_columns = [col for col in final_columns if col not in ("__db_col_embedding_content_vector", "id")]
        table_list_str=", ".join(tables_list)
        final_query_text = query_text + table_list_str

        # ---- Perform Similarity Search ----
        results = index.similarity_search(
            query_text=final_query_text,
            columns=final_columns,
            num_results=num_results,
            query_type="hybrid",  # or "ANN"
            reranker=DatabricksReranker(
                columns_to_rerank=final_columns  # This makes sure that table_name and llm_context are prioritized in the results
            )
        )

        # Convert results['result']['data_array'] to a list of dictionaries using column names from manifest
        columns = [col['name'] for col in results['manifest']['columns']]
        results = [dict(zip(columns, row)) for row in results['result']['data_array']]
        return results
