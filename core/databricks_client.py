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
from core.state_schema import AgentState
import re

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
        self._vector_client: Optional[VectorSearchClient] = None
        self._vector_indexes: Dict[str, Any] = {}  # Cache indexes by name
        

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

        # OpenAI API Configuration
        self.OPENAI_PROJECT_ID = os.getenv("OPENAI_PROJECT_ID", "d7b5ad10-4880-4f07-9495-ae0fcc5035b3")
        self.OPENAI_CLIENT_ID = os.getenv("CLAUDE_CLIENT_ID")
        self.OPENAI_CLIENT_SECRET = os.getenv("CLAUDE_CLIENT_SECRET")
        self.OPENAI_AUTH_URL = "https://api.uhg.com/oauth2/token"
        self.OPENAI_SCOPE = "https://api.uhg.com/.default"
        self.OPENAI_GRANT_TYPE = "client_credentials"
        self.OPENAI_MODEL_ID = "gpt-5_2025-08-07"  # deployment name
        self.OPENAI_API_URL = "https://api.uhg.com/api/cloud/api-management/ai-gateway-reasoning/1.0"
        self.OPENAI_API_VERSION = "2025-01-01-preview"

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
        self._openai_token: Optional[str] = None
        self._openai_token_expiry: Optional[float] = None  # epoch seconds
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
    
    async def _get_vector_client(self) -> VectorSearchClient:
        """Lazy-load and cache the VectorSearchClient"""
        if self._vector_client is None:
            self._vector_client = VectorSearchClient(
                workspace_url=self.DATABRICKS_HOST,
                service_principal_client_id=self.DATABRICKS_CLIENT_ID,
                service_principal_client_secret=self.DATABRICKS_CLIENT_SECRET,
                azure_tenant_id=self.DATABRICKS_TENANT_ID
            )
        return self._vector_client

    async def _get_vector_index(self, index_name: str, endpoint_name: str = "metadata_vectore_search_endpoint"):
        """Get and cache vector search index with configurable endpoint"""
        cache_key = f"{endpoint_name}::{index_name}"
        if cache_key not in self._vector_indexes:
            client = await self._get_vector_client()
            index = client.get_index(
                endpoint_name=endpoint_name,
                index_name=index_name
            )
            index._get_token_for_request = lambda: self.access_token
            self._vector_indexes[cache_key] = index
        return self._vector_indexes[cache_key]

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

    async def _fetch_openai_token_async(self) -> str:
        """Fetch a new OpenAI token from HCP auth service and cache it with expiry."""
        data = {
            "grant_type": self.OPENAI_GRANT_TYPE,
            "client_id": self.OPENAI_CLIENT_ID,
            "client_secret": self.OPENAI_CLIENT_SECRET,
            "scope": self.OPENAI_SCOPE,
        }
        session = await self._get_session()
        async with session.post(self.OPENAI_AUTH_URL, data=data) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise Exception(f"OpenAI auth token fetch failed ({resp.status}): {text[:300]}")
            result = json.loads(text)
            access_token = result.get("access_token")
            expires_in = result.get("expires_in", 3000)
            self._openai_token = access_token
            self._openai_token_expiry = time.time() + expires_in
            return access_token

    async def _get_openai_token(self) -> str:
        """Return a cached OpenAI token if still valid; otherwise fetch a new one."""
        if self._openai_token and self._openai_token_expiry:
            if time.time() < (self._openai_token_expiry - self._token_skew_seconds):
                return self._openai_token
        return await self._fetch_openai_token_async()

    async def call_openai_api_endpoint_async(self, messages: list, max_tokens: int = 8000, temperature: float = 0.1, top_p: float = 0.8, system_prompt: str = "you are an AI assistant") -> str:
        """
        Async version of OpenAI API endpoint call using HCP credentials.
        Features: token caching, session reuse, retries, concurrency limiting
        
        Args:
            messages: List of message dicts with 'role' and 'content' keys
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature (0.0-2.0)
            top_p: Nucleus sampling parameter
            system_prompt: System instruction for the model
            
        Returns:
            String response from the model
        """
        BASE_URL = self.OPENAI_API_URL
        PROJECT_ID = self.OPENAI_PROJECT_ID
        MODEL_NAME = self.OPENAI_MODEL_ID
        API_VERSION = self.OPENAI_API_VERSION

        try:
            # Concurrency guard to prevent overwhelming the gateway
            async with self._llm_semaphore:
                # Step 1: Get (cached) token
                token = await self._get_openai_token()

                # Step 2: Convert messages to OpenAI format (already in correct format)
                api_messages = []
                
                # Add system message if provided
                if system_prompt:
                    api_messages.append({
                        "role": "system",
                        "content": system_prompt
                    })
                
                # Add user messages
                for msg in messages:
                    api_messages.append({
                        "role": msg["role"],
                        "content": msg["content"]
                    })

                # Step 3: Prepare headers and payload
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Authorization": f"Bearer {token}",
                    "projectId": PROJECT_ID,
                    "User-Agent": "agentbot-async/1.0",
                    "api-version": API_VERSION
                }
                
                # GPT-5 uses max_completion_tokens instead of max_tokens
                # GPT-5 only supports default temperature (1.0) - don't send custom values
                payload = {
                    "model": MODEL_NAME,
                    "messages": api_messages,
                    "max_completion_tokens": max_tokens  # Changed from max_tokens for GPT-5
                }
                
                # Note: temperature and top_p are NOT included as GPT-5 only supports default values

                # Use Azure OpenAI standard endpoint format
                # Azure OpenAI uses: /openai/deployments/{deployment}/chat/completions
                url = f"{BASE_URL}/openai/deployments/{MODEL_NAME}/chat/completions?api-version={API_VERSION}"
                
                print(f"📡 OpenAI API URL: {url}")
                print(f"📡 Model: {MODEL_NAME}")

                # Retry (exponential backoff) for transient gateway errors
                transient_statuses = {502, 503, 504}
                backoff = 1.0
                max_retries = 3
                session = await self._get_session()
                last_error_text = None
                
                # Track timing
                start_time = time.time()
                
                for attempt in range(1, max_retries + 1):
                    try:
                        async with session.post(url, headers=headers, json=payload) as response:
                            status = response.status
                            if status == 401:
                                # Token might be expired unexpectedly; force refresh once
                                if attempt == 1:
                                    print("401 received – refreshing OpenAI token and retrying once")
                                    self._openai_token = None
                                    token = await self._get_openai_token()
                                    headers["Authorization"] = f"Bearer {token}"
                                    continue
                            if status not in (200,):
                                error_text = await response.text()
                                last_error_text = error_text
                                print(f"OpenAI call failed (status={status}) attempt={attempt}: {error_text[:400]}")
                                if status in transient_statuses and attempt < max_retries:
                                    await asyncio.sleep(backoff)
                                    backoff *= 2
                                    continue
                                response.raise_for_status()
                            result = await response.json()
                            
                            # Extract token usage and response time
                            elapsed_time = time.time() - start_time
                            usage = result.get("usage", {})
                            input_tokens = usage.get("prompt_tokens", 0)
                            output_tokens = usage.get("completion_tokens", 0)
                            
                            # Print metrics
                            print(f"📊 OpenAI - Input tokens: {input_tokens} | Output tokens: {output_tokens} | Response time: {elapsed_time:.2f}s")
                            
                            # Debug: Print the full response structure if output tokens > 0 but no content
                            if output_tokens > 0:
                                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                                if not content:
                                    print(f"⚠️ WARNING: {output_tokens} output tokens but empty content!")
                                    print(f"Full response structure: {json.dumps(result, indent=2)}")
                            
                            try:
                                return result["choices"][0]["message"]["content"]
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
                raise Exception(f"OpenAI API endpoint call failed after retries. Last error: {last_error_text[:400] if last_error_text else 'No body'}")

        except aiohttp.ClientError as e:
            print(f"HTTP Error: {e}")
            raise Exception(f"OpenAI API endpoint call failed: {str(e)}")
        except Exception as e:
            raise Exception(f"OpenAI API endpoint call failed: {str(e)}")

    async def call_claude_api_endpoint_async(self, messages: list, max_tokens: int = 8000, temperature: float = 0.1, top_p: float = 0.5, system_prompt: str = "you are an AI assistant") -> str:
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
                
                # Track timing
                start_time = time.time()
                
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
                            
                            # Extract token usage and response time
                            elapsed_time = time.time() - start_time
                            input_tokens = result.get("usage", {}).get("inputTokens", 0)
                            output_tokens = result.get("usage", {}).get("outputTokens", 0)
                            
                            # Print metrics
                            print(f"📊 Input tokens: {input_tokens} | Output tokens: {output_tokens} | Response time: {elapsed_time:.2f}s")
                            
                            try:
                                return result["output"]["message"]["content"][0]["text"]
                            except KeyError as e:
                                print(f"Response parsing KeyError: {e}; raw keys: {list(result.keys())}")
                                return json.dumps(result)[:2000]
                    except aiohttp.ClientError as ce:
                        print(f"Network client error attempt={attempt}: {ce}")
                        # On network errors, close the session to force recreation on next retry
                        if self._http_session and not self._http_session.closed:
                            await self._http_session.close()
                            self._http_session = None
                            print("Closed HTTP session due to network error - will recreate on retry")
                        if attempt < max_retries:
                            await asyncio.sleep(backoff)
                            backoff *= 2
                            # Get fresh session for next attempt
                            session = await self._get_session()
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

    def _extract_json_from_response(self, response: str) -> str:
        """Extract JSON content from XML tags or return the response if no tags found"""
        import re
        
        # Try to extract content between <json> tags
        json_match = re.search(r'<json>(.*?)</json>', response, re.DOTALL)
        if json_match:
            return json_match.group(1).strip()
        
        # If no XML tags found, assume the entire response is JSON
        return response.strip()

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
    num_results_per_table: int,
    index_name: str
) -> List[Dict]:
        """
        Search columns across multiple tables in parallel with caching.
        For each table: Fetch 50 results, rerank, keep top 10.
        """
        print(f"🔍 Searching: '{query_text}' across {len(tables_list)} table(s)")
        
        # ✅ Use cached index (eliminates the wait!)
        index = await self._get_vector_index(index_name)
        
        # ✅ Define async search function for one table
        async def search_table(table_name: str):
            print(f'📋 Searching table: {table_name}')
            try:
                from databricks.vector_search.reranker import DatabricksReranker
                
                results = index.similarity_search(
                    query_text=query_text,
                    columns=["table_name", "column_name", "col_embedding_content", "llm_context"],
                    filters={"table_name": table_name},
                    reranker=DatabricksReranker(
                        columns_to_rerank=["column_name", "col_embedding_content","llm_context"]
                    ),
                    num_results=50,  # Fetch 50
                    query_type="Hybrid"
                )
                
                if results.get('result', {}).get('data_array'):
                    cols = [c['name'] for c in results['manifest']['columns']]
                    table_results = [dict(zip(cols, row)) for row in results['result']['data_array']]
                    
                    # Sort by score and keep top 10
                    table_results.sort(key=lambda x: x.get('score', 0.0), reverse=True)
                    top_10 = table_results[:20]
                    
                    print(f"  ✅ {table_name}: {len(top_10)} columns")
                    return top_10
                else:
                    print(f"  ⚠️ {table_name}: No results")
                    return []
                    
            except Exception as e:
                print(f"  ❌ {table_name}: Error - {e}")
                import traceback
                traceback.print_exc()
                return []
        
        # ✅ Search all tables in parallel
        results_per_table = await asyncio.gather(*[search_table(t) for t in tables_list])
        
        # Flatten results
        all_results = [item for sublist in results_per_table for item in sublist]
        
        print(f"\n✅ Total: {len(all_results)} columns from {len(tables_list)} table(s)")
        return all_results
        
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
        
    async def search_feedback_sql_embeddings(self, user_question: str, table_names: List[str] = None) -> List[Dict]:
        """
        Search feedback SQL embeddings index for similar SQL queries based on user question.
        Uses direct REST API call with reranking support and 3 retries with exponential backoff.
        
        Args:
            user_question: The user's question to search for similar historical queries
            table_names: Optional list of table names to filter results (e.g., ["prd_optumrx_orxfdmprdsa.rag.pbm_claims"])
            
        Returns:
            List of top 25 results with columns: seq_id, user_question, sql_query, insert_ts, table_name
        """
        table_filter_text = f" (filtering by tables: {table_names})" if table_names else ""
        print(f"🔍 Searching feedback SQL embeddings for: '{user_question}'{table_filter_text}")
        
        # Retry configuration
        max_retries = 3
        base_delay = 1.0  # Start with 1 second
        
        for attempt in range(max_retries):
            try:
                # Use direct REST API call (matching the curl command pattern)
                url = f"{self.DATABRICKS_HOST}/api/2.0/vector-search/indexes/prd_optumrx_orxfdmprdsa.rag.feedback_sql_embedding_idx/query"

                # Fetch more results if filtering, otherwise fetch 25
                
                payload = {
                    "num_results": 50,
                    "columns": ["seq_id", "user_question", "sql_query", "insert_ts", "table_name"],
                    "query_text": user_question,
                    "query_type": "HYBRID",
                    "reranker": {
                        "model": "databricks_reranker",
                        "parameters": {
                            "columns_to_rerank": ["user_question"]
                        }
                    }
                }
                
                # Use the token headers (same authentication as SQL queries)
                headers = {
                    "Authorization": f"Bearer {self.DATABRICKS_TOKEN}",
                    "Content-Type": "application/json;charset=UTF-8",
                    "Accept": "application/json, text/plain, */*"
                }
                
                session = await self._get_session()
                
                print(f"   Calling REST API with reranking (attempt {attempt + 1}/{max_retries})...")
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        print(f"   ❌ API returned status {response.status}: {error_text}")
                        response.raise_for_status()
                    
                    result = await response.json()
                
                # Parse results from REST API response
                if result.get('result', {}).get('data_array'):
                    manifest = result.get('manifest', {})
                    if 'columns' in manifest:
                        cols = [c['name'] for c in manifest['columns']]
                    else:
                        # Fallback if manifest is different
                        cols = payload['columns']
                    
                    parsed_results = [dict(zip(cols, row)) for row in result['result']['data_array']]
                    
                    # Filter results by table_names if provided
                    if table_names and len(table_names) > 0:
                        print(f"   📋 Filtering results by tables: {table_names}")
                        filtered_results = [
                            result for result in parsed_results 
                            if result.get('table_name') in table_names
                        ]
                        # Return top 25 after filtering
                        return filtered_results[:25]
                    else:
                        print(f"  ✅ Found {len(parsed_results)} similar SQL queries (reranked)")
                        return parsed_results
                else:
                    print(f"  ⚠️ No results found in feedback SQL embeddings")
                    return []
                    
            except aiohttp.ClientError as e:
                error_str = str(e)
                print(f"  ⚠️ Network error on attempt {attempt + 1}: {error_str}")
                
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
                    print(f"  🔄 Connection error detected, retrying in {delay}s...")
                    
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
                    print(f"  ❌ Vector search failed: {str(e)}")
                    return []
                    
            except Exception as e:
                error_str = str(e)
                print(f"  ⚠️ Unexpected error on attempt {attempt + 1}: {error_str}")
                
                # Don't retry non-network errors unless it's the last attempt
                if attempt < max_retries - 1 and 'connection' in error_str.lower():
                    delay = base_delay * (2 ** attempt)
                    print(f"  🔄 Connection-related error, retrying in {delay}s...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    print(f"  ❌ Error searching feedback SQL embeddings: {str(e)}")
                    return []
        
        # Should not reach here, but return empty list if all retries exhausted
        print(f"  ❌ Vector search failed after all {max_retries} retry attempts")
        return []
        
    async def search_metadata_sql(self, filter_list: List[str]) -> List[str]:
        """
        Search metadata with enhanced cascading priority:
        1A. EXACT MATCH (Pure) - ALL values are exact matches
        1B. EXACT MATCH (Mixed) - SOME values are exact, others partial
        2. STARTS-WITH MATCH - Value starts with the full term
        3. CONTAINS MATCH - Individual words found (with dynamic intelligent ranking)
        """
        try:
            if not filter_list:
                return []
            
            print(f"🔍 Starting enhanced search for filters: {filter_list}")
            
            # Build all match conditions for a SINGLE query
            exact_conditions = []
            starts_conditions = []
            contains_conditions = []
            
            for term in filter_list:
                term_clean = term.strip().lower()
                escaped_exact = term_clean.replace("'", "\\'")
                escaped_regex = term_clean.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)').replace('[', '\\[').replace(']', '\\]')
                
                # TIER 1: Exact match condition
                exact_conditions.append(f"lower(trim(exploded_value)) = '{escaped_exact}'")
                
                # TIER 2: Starts-with condition
                starts_conditions.append(f"lower(trim(exploded_value)) RLIKE '^{escaped_regex}\\\\b'")
                
                # TIER 3: Individual words
                words = term_clean.split()
                for word in words:
                    if len(word) > 2:
                        escaped_word = word.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)').replace('[', '\\[').replace(']', '\\]')
                        contains_conditions.append(f"lower(trim(exploded_value)) RLIKE '(?i)\\\\b{escaped_word}'")
            
            # Build tier scoring logic
            exact_clause = ' OR '.join(exact_conditions)
            starts_clause = ' OR '.join(starts_conditions)
            contains_clause = ' OR '.join(contains_conditions)
            
            # Combine all conditions for WHERE clause
            all_conditions = f"({exact_clause}) OR ({starts_clause}) OR ({contains_clause})"
            
            # Tier assignment with tier tracking per value
            tier_assignment = f"""
                CASE 
                    WHEN {exact_clause} THEN 1
                    WHEN {starts_clause} THEN 2
                    ELSE 3
                END
            """
            
            query = f"""
            WITH matched_data AS (
                SELECT
                    column_name,
                    trim(exploded_value) AS individual_value,
                    {tier_assignment} AS match_tier
                FROM prd_optumrx_orxfdmprdsa.rag.distinct_values_metadata1
                LATERAL VIEW explode(split(distinct_values, ',')) AS exploded_value
                WHERE {all_conditions}
            ),
            scored_aggregated AS (
                SELECT
                    column_name,
                    collect_list(individual_value) AS all_matched_values,
                    collect_list(match_tier) AS all_match_tiers,
                    MIN(match_tier) AS best_tier
                FROM matched_data
                GROUP BY column_name
            )
            SELECT
                column_name,
                concat_ws(', ', slice(all_matched_values, 1, 5)) AS matched_values,
                all_match_tiers,
                best_tier
            FROM scored_aggregated
            ORDER BY best_tier ASC, column_name
            LIMIT 7
            """
            
            print(f"📊 Enhanced Query (First 500 chars): {query[:500]}...")
            
            # Execute ONCE using async method
            result_data = await self.execute_sql_async_audit(query)
            print('results_data_filter', result_data)
            
            # Convert DataFrame to list if needed
            if hasattr(result_data, 'collect'):
                # It's a Spark DataFrame - convert to list of dicts
                result_list = [row.asDict() for row in result_data.collect()]
                print(f"✅ Converted DataFrame to list: {len(result_list)} rows")
            elif isinstance(result_data, list):
                result_list = result_data
            else:
                print(f"❌ Unexpected result type: {type(result_data)}")
                return []
            
            if not result_list:
                print(f"❌ No matches found")
                return []
            
            # Python filtering: Return ONLY the highest priority tier
            return self._filter_by_priority_enhanced(result_list, filter_list)
            
        except Exception as e:
            print(f"❌ Error in search_metadata_sql: {str(e)}")
            return []


    def _filter_by_priority_enhanced(self, result_data: list, filter_list: List[str]) -> List[str]:
        """
        Enhanced filtering with:
        - Tier 1A/1B split (purity check)
        - Tier 3 dynamic intelligent ranking (NO HARDCODING)
        """
        if not result_data:
            return []
        
        import json
        
        # Group results by tier with purity check for Tier 1
        tier_1a_results = []  # Pure exact matches
        tier_1b_results = []  # Mixed exact matches
        tier_2_results = []
        tier_3_results = []
        
        for row in result_data:
            best_tier = int(row.get('best_tier', 3))
            column_name = row.get('column_name', '')
            matched_values = row.get('matched_values', '')
            all_match_tiers_raw = row.get('all_match_tiers', [])
            
            # Parse all_match_tiers if it's a JSON string
            if isinstance(all_match_tiers_raw, str):
                try:
                    all_match_tiers = json.loads(all_match_tiers_raw)
                    # Convert string numbers to integers
                    all_match_tiers = [int(tier) for tier in all_match_tiers]
                except:
                    all_match_tiers = []
            elif isinstance(all_match_tiers_raw, list):
                all_match_tiers = [int(tier) for tier in all_match_tiers_raw]
            else:
                all_match_tiers = []
            
            result_entry = {
                'column_name': column_name,
                'matched_values': matched_values,
                'tier': best_tier,
                'all_tiers': all_match_tiers
            }
            
            if best_tier == 1:
                # Check purity: Are ALL matched values exact matches?
                if all_match_tiers and all(tier == 1 for tier in all_match_tiers):
                    result_entry['sub_tier'] = '1A'
                    tier_1a_results.append(result_entry)
                else:
                    result_entry['sub_tier'] = '1B'
                    tier_1b_results.append(result_entry)
            elif best_tier == 2:
                tier_2_results.append(result_entry)
            else:
                tier_3_results.append(result_entry)
        
        print(f"\n📊 Results breakdown:")
        print(f"   Tier 1A (PURE EXACT): {len(tier_1a_results)} columns")
        print(f"   Tier 1B (MIXED EXACT): {len(tier_1b_results)} columns")
        print(f"   Tier 2 (STARTS-WITH): {len(tier_2_results)} columns")
        print(f"   Tier 3 (CONTAINS): {len(tier_3_results)} columns")
        
        # Return ONLY highest priority tier with results
        if tier_1a_results:
            print(f"✅ TIER 1A (PURE EXACT): Found {len(tier_1a_results)} matches - Returning only these")
            return self._format_results(tier_1a_results[:7], "EXACT-PURE")
        
        if tier_1b_results:
            print(f"✅ TIER 1B (MIXED EXACT): Found {len(tier_1b_results)} matches - Returning only these")
            return self._format_results(tier_1b_results[:7], "EXACT-MIXED")
        
        if tier_2_results:
            print(f"✅ TIER 2 (STARTS-WITH): Found {len(tier_2_results)} matches - Returning only these")
            return self._format_results(tier_2_results[:7], "STARTS-WITH")
        
        if tier_3_results:
            print(f"✅ TIER 3 (CONTAINS): Found {len(tier_3_results)} matches - Applying dynamic intelligent ranking...")
            # Apply dynamic intelligent ranking for Tier 3
            ranked_tier_3 = self._rank_tier_3_dynamic(tier_3_results, filter_list)
            return self._format_results(ranked_tier_3[:7], "CONTAINS")
        
        print(f"❌ No results after filtering")
        return []


    def _rank_tier_3_dynamic(self, tier_3_results: list, filter_list: List[str]) -> list:
        """
        Dynamic intelligent ranking for Tier 3 results (NO HARDCODING)
        
        Scoring factors:
        1. Word match count (50 points per word) - How many search words appear in values
        2. Match completeness ratio (30 points) - % of search words found
        3. Token boundary bonus (20 points) - Full word vs substring match
        4. Column name relevance (15 points) - Column name semantic matching
        """
        # Extract all search words
        search_words = []
        for term in filter_list:
            words = term.strip().lower().split()
            search_words.extend([w for w in words if len(w) > 2])
        
        total_search_words = len(search_words)
        print(f"   🔍 Search words for dynamic ranking: {search_words} (total: {total_search_words})")
        
        # Calculate dynamic composite score for each result
        for result in tier_3_results:
            column_name = result['column_name']
            matched_values = result['matched_values'].lower()
            
            # SCORE 1: Word match count (50 points per unique word found)
            words_found = set()
            for word in search_words:
                if word in matched_values:
                    words_found.add(word)
            word_match_count = len(words_found)
            word_match_score = word_match_count * 50
            
            # SCORE 2: Match completeness ratio (30 points max)
            if total_search_words > 0:
                completeness_ratio = word_match_count / total_search_words
                completeness_score = completeness_ratio * 30
            else:
                completeness_score = 0
            
            # SCORE 3: Token boundary quality (20 points max)
            token_boundary_score = 0
            for word in words_found:
                # Check if word appears as full token (with word boundaries)
                if re.search(r'\b' + re.escape(word) + r'\b', matched_values):
                    token_boundary_score += 20 / len(words_found) if words_found else 0
            
            # SCORE 4: Column name relevance (15 points max)
            column_name_lower = column_name.lower()
            column_relevance_score = 0
            
            # Check if any search word appears in column name
            for word in search_words:
                if word in column_name_lower:
                    column_relevance_score += 5
            
            # Semantic column name matching (medical/pharma related terms)
            medical_terms = ['therapy', 'drug', 'medication', 'treatment', 'disease', 'diagnosis', 
                            'class', 'category', 'type', 'vaccine', 'pharmaceutical']
            for med_term in medical_terms:
                if med_term in column_name_lower and any(word in matched_values for word in search_words):
                    column_relevance_score += 10
                    break
            
            # Cap at 15 points
            column_relevance_score = min(column_relevance_score, 15)
            
            # COMPOSITE SCORE
            composite_score = (
                word_match_score + 
                completeness_score + 
                token_boundary_score + 
                column_relevance_score
            )
            
            # Store all scoring details
            result['word_match_count'] = word_match_count
            result['completeness_ratio'] = round(completeness_ratio * 100, 1) if total_search_words > 0 else 0
            result['token_boundary_score'] = round(token_boundary_score, 1)
            result['column_relevance_score'] = round(column_relevance_score, 1)
            result['composite_score'] = round(composite_score, 1)
            
            print(f"   📊 {column_name}: Score={result['composite_score']} "
                f"[Words:{word_match_count}/{total_search_words}, "
                f"Complete:{result['completeness_ratio']}%, "
                f"Token:{result['token_boundary_score']}, "
                f"ColRel:{result['column_relevance_score']}]")
        
        # Sort by composite score (descending)
        tier_3_results.sort(key=lambda x: x['composite_score'], reverse=True)
        
        print(f"   ✅ Dynamically ranked {len(tier_3_results)} Tier 3 results")
        return tier_3_results


    def _format_results(self, result_data: list, match_type: str) -> List[str]:
        """
        Format the query results into a list of strings
        """
        concatenated_results = []
        
        for row in result_data:
            column_name = row.get('column_name', '')
            matched_values = row.get('matched_values', '')
            tier = row.get('tier', '')
            sub_tier = row.get('sub_tier', '')
            
            # Additional info for Tier 3
            extra_info = ""
            if 'composite_score' in row:
                extra_info = (f", Score:{row['composite_score']}")
            
            tier_label = sub_tier if sub_tier else tier
            table_summary = f"Column: {column_name} (Tier: {tier_label}, Type: {match_type}{extra_info})\n  - Values: {matched_values}"
            concatenated_results.append(table_summary)
        
        print(f"📋 Formatted {len(concatenated_results)} results for {match_type} matches")
        return concatenated_results
    
    async def _llm_feedback_selection(self, feedback_results: List[Dict], state: AgentState) -> Dict:
        """
        Pure LLM-based selection of most relevant historical question from feedback results.
        Returns single best match or NO_MATCH status.
        """
        
        user_question = state.get('rewritten_question', state.get('original_question', ''))
        
        # System prompt for feedback matching
        system_prompt = """
You are a SQL pattern matching system. Select the single best historical question or return NO_MATCH.
You compare TEXT PATTERNS - you do NOT answer business questions.

MATCHING RULES (APPLY IN ORDER)

STEP 1: METRIC MATCH (MANDATORY - REJECT IF FAILS)

Current metric(s) MUST appear in history.

Rules:
✅ History has required metric + extras → ALLOW
✅ Metric synonyms count as match (see synonym list)
❌ History missing required metric → REJECT

Examples:
- Current: "revenue" | History: "revenue, volume" → ✅ PASS
- Current: "revenue" | History: "volume" → ❌ REJECT
- Current: "network revenue" | History: "revenue" → ✅ PASS (synonym)

STEP 2: DIMENSION MATCH (MANDATORY - REJECT IF FAILS)

At least ONE dimension must overlap between current and history.
Either side can have more dimensions than the other.

⚠️ CRITICAL: Date dimensions (BY month, BY quarter, BY year) are NOT counted as dimensions here - ignore them completely in this check.

Rules:
✅ Current: BY drug | History: BY drug BY region → PASS (drug overlaps)
✅ Current: BY drug BY region | History: BY drug → PASS (drug overlaps)
✅ Current: BY drug BY class | History: BY drug BY region → PASS (drug overlaps)
❌ Current: BY drug | History: BY region → REJECT (no overlap)
❌ Current: BY drug | History: no dimension → REJECT (no overlap)
✅ Current: no dimension | History: any dimension → PASS (no requirement)

Edge Case - Date Dimensions:
- Current: "revenue BY month" | History: "revenue BY lob" → ❌ REJECT
  Reason: "BY month" is a REAL dimension (not a date filter), no overlap with "lob"
- Current: "revenue for Q3 2025" | History: "revenue for Jan 2024" → ✅ PASS
  Reason: "Q3 2025" and "Jan 2024" are date FILTERS (not dimensions), ignored in Step 3

STEP 3: IGNORE DATE/TIME FILTERS

Strip all date specifications from filters. Any date = any other date.

Rules:
- Q3 2025 = Jan 2024 = July-Aug 2025 = Q1 2023 vs Q1 2022 = ANY
- Single periods, ranges, YoY, MoM, QoQ → ALL IGNORED

STEP 4: RANK BY FILTER VALUE MATCH

After passing Steps 1-3, rank candidates by filter alignment:

TIER A (HIGHEST): Filter value(s) match
- Current filter value(s) appear in history (history can have extras)
- Dimensions overlap + metric matches + filter matches

TIER B (MEDIUM): Filter value differs
- Dimensions overlap + metric matches, but filter value different

TIER C (LOWEST): No dimension requirement
- Metric matches, current has no dimension requirement

STEP 5: SELECT BEST MATCH

1. If any TIER A exists → Pick first TIER A
2. Else if any TIER B exists → Pick first TIER B
3. Else if any TIER C exists → Pick first TIER C
4. Else → NO_MATCH

SYNONYMS

Metrics: revenue=network revenue=product revenue | volume=script count=scripts=line count | cost=expense=spend=cogs
Entities: HDP=Home Delivery=Mail | SP=Specialty | PBM=PBM Retail
Parsing: "PBM revenue" means metric=revenue, filter=PBM (NOT metric="PBM revenue")

KEY EXAMPLES

Example 1: Filter + Dimension Overlap + Metric Match (TIER A)

Current: "revenue BY lob for Prior Auth"
History: "revenue BY lob for Prior Auth, HDP Core"
Result: TIER A ✅ (lob overlaps, revenue matches, Prior Auth found + extras OK)

Current: "revenue BY lob BY region for Prior Auth"
History: "revenue BY lob for Prior Auth"
Result: TIER A ✅ (lob overlaps, revenue matches, Prior Auth found)

Example 2: Dimension Flexibility (History Subset/Superset Both OK)

Current: "revenue BY drug BY therapeutic_class for PBM"
History: "revenue BY drug for PBM"
Result: TIER A ✅ (drug overlaps, revenue matches, PBM matches)

Current: "revenue BY drug for PBM"
History: "revenue BY drug BY therapeutic_class BY region for PBM"
Result: TIER A ✅ (drug overlaps, revenue matches, PBM matches)

Example 3: No Dimension Overlap → REJECT

Current: "revenue BY drug for PBM"
History: "revenue BY region for PBM"
Result: REJECT ❌ (drug vs region - no dimension overlap)

Current: "revenue BY lob for HDP"
History: "revenue for HDP"
Result: REJECT ❌ (current needs lob dimension, history has none - no overlap)

Example 4: Date Filters Completely Ignored

Current: "revenue for PBM in Q3 2025"
History: "revenue for PBM from Jan-Dec 2024"
Result: TIER A ✅ (dates ignored, PBM matches, revenue matches)

Current: "script count for HDP from Jan-June 2025"
History: "volume for Home Delivery Q3 2024 vs Q3 2023"
Result: TIER A ✅ (script count=volume synonym, HDP=Home Delivery synonym, dates ignored)

Example 5: Metric Mismatch → REJECT

Current: "revenue BY lob for PBM"
History: "volume BY lob for PBM"
Result: REJECT ❌ (revenue ≠ volume, failed Step 1)

Current: "script count for HDP"
History: "cost for HDP"
Result: REJECT ❌ (script count ≠ cost, failed Step 1)

Example 6: TIER B (Dimension Overlap But Filter Differs)

Current: "revenue BY lob for Prior Auth"
History: "revenue BY lob for HDP Core"
Result: TIER B (lob overlaps, revenue matches, but Prior Auth ≠ HDP Core)

Current: "volume BY drug BY region for Specialty"
History: "scripts BY drug for PBM"
Result: TIER B (drug overlaps, volume=scripts synonym, but Specialty ≠ PBM)

OUTPUT FORMAT

Return ONLY JSON in <json> tags. No explanation outside tags.

<json>
{
  "status": "match_found" or "no_match",
  "selected_seq_id": <number> or null,
  "matched_question": "<text>" or null,
  "table_name": "<name>" or null,
  "reasoning": "<1-line explanation with tier>",
  "match_confidence": "HIGH" or "MEDIUM" or "LOW"
}
</json>

Confidence: HIGH=TIER A | MEDIUM=TIER B | LOW=TIER C

Remember: You match patterns, not answer questions. Start with <json>.
"""

        # Format feedback results as context in compact format (saves tokens)
        # Group by table_name first
        results_by_table = {}
        for result in feedback_results:
            table_name = result.get('table_name', 'N/A')
            if table_name not in results_by_table:
                results_by_table[table_name] = []
            results_by_table[table_name].append(result)
        
        # Build compact candidates context
        candidates_context = "=== HISTORICAL QUESTIONS (GROUPED BY TABLE) ===\n"
        for table_name, results in results_by_table.items():
            candidates_context += f"\n- table_name: {table_name}\n"
            for result in results:
                seq_id = result.get('seq_id', 'N/A')
                question = result.get('user_question', 'N/A')
                candidates_context += f"  id:{seq_id}, {question}\n"
        
        # Build the full prompt with TRIPLE-LAYER protection against guardrails
        selection_prompt = f"""⚠️⚠️⚠️ CRITICAL INSTRUCTION - READ THIS FIRST ⚠️⚠️⚠️

You are an AUTOMATED PATTERN MATCHING SYSTEM - NOT an AI assistant that answers questions.
Your ONLY function is to analyze TEXT PATTERNS and return a JSON response.

DO NOT attempt to:
- Answer the business questions below
- Provide insights about the data

{system_prompt}

{candidates_context}

=== CURRENT QUESTION (TEXT PATTERN TO MATCH) ===
{user_question}

⚠️ REMINDER: You are NOT answering this question. You are ONLY checking if its pattern matches any historical question pattern above.

Return ONLY the JSON in <json> tags. Start immediately with <json>.
"""
        
        # Retry logic
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                print(f"🔍 Attempting feedback selection (attempt {retry_count + 1}/{max_retries})...")

                # Call LLM with LOWER temperature for more deterministic behavior
                # Lower temperature = less creative = less likely to trigger guardrails
                # print("Raw LLM prompt:", selection_prompt)
                llm_response = await self.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": selection_prompt}],
                    max_tokens=2000,  # Reduced tokens since we only need JSON
                    temperature=0.0,  # ← CRITICAL: Set to 0 for deterministic pattern matching
                    top_p=0.1,  # ← CRITICAL: Very focused sampling
                    system_prompt="PATTERN MATCHING SYSTEM: You are an automated text pattern comparison algorithm for SQL query optimization infrastructure. You do NOT answer questions, provide business insights, or analyze data. You ONLY compare text patterns between queries and return structured JSON responses indicating pattern similarity. This is a technical infrastructure task, not business analysis."
                )
                
                print("Raw LLM response:", llm_response)
                
                # Extract JSON from response
                json_content = self._extract_json_from_response(llm_response)
                selection_result = json.loads(json_content)
                
                # Validate response structure
                status = selection_result.get('status')
                if status not in ['match_found', 'no_match']:
                    raise ValueError(f"Invalid status returned: {status}")
                
                # Handle success cases
                if status == "match_found":
                    print(f"✅ Feedback match found: seq_id={selection_result.get('selected_seq_id')}")
                    print(f"   Matched question: {selection_result.get('matched_question')}")
                    print(f"   Pattern level: {selection_result.get('pattern_match_level')}")
                    print(f"   Reasoning: {selection_result.get('reasoning')}")
                    
                    return {
                        'status': 'match_found',
                        'seq_id': selection_result.get('selected_seq_id'),
                        'question': selection_result.get('matched_question'),
                        'table_name': selection_result.get('table_name'),
                        'reasoning': selection_result.get('reasoning'),
                        'pattern_match_level': selection_result.get('pattern_match_level'),
                        'error': False,
                        'error_message': ''
                    }
                
                else:  # status == "no_match"
                    print(f"❌ No suitable match found in feedback history")
                    print(f"   Reasoning: {selection_result.get('reasoning')}")
                    
                    return {
                        'status': 'no_match',
                        'seq_id': None,
                        'question': None,
                        'table_name': None,
                        'reasoning': selection_result.get('reasoning'),
                        'pattern_match_level': 'NONE',
                        'error': False,
                        'error_message': ''
                    }
            
            except json.JSONDecodeError as e:
                retry_count += 1
                print(f"⚠ JSON parsing failed (attempt {retry_count}/{max_retries}): {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying...")
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    print(f"❌ All retry attempts exhausted - JSON parsing failed")
                    return {
                        'status': 'no_match',
                        'seq_id': None,
                        'question': None,
                        'table_name': None,
                        'reasoning': f"Failed to parse LLM response after {max_retries} attempts: {str(e)}",
                        'pattern_match_level': 'NONE',
                        'error': True,
                        'error_message': f"JSON parsing failed after {max_retries} attempts"
                    }
            
            except Exception as e:
                retry_count += 1
                print(f"⚠ Feedback selection attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying... ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    print(f"❌ All retry attempts exhausted - feedback selection failed")
                    return {
                        'status': 'no_match',
                        'seq_id': None,
                        'question': None,
                        'table_name': None,
                        'reasoning': f"Feedback selection failed after {max_retries} attempts: {str(e)}",
                        'pattern_match_level': 'NONE',
                        'error': True,
                        'error_message': f"LLM call failed after {max_retries} attempts: {str(e)}"
                    }
        
        # Should never reach here, but just in case
        return {
            'status': 'no_match',
            'seq_id': None,
            'question': None,
            'table_name': None,
            'reasoning': 'Unexpected error in retry logic',
            'pattern_match_level': 'NONE',
            'error': True,
            'error_message': 'Unexpected error in retry logic'
        }
