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

    
