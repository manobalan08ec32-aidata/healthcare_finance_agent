"""
Cosmos DB State Manager for persisting chat state across instances.

This module handles:
- Saving/loading chat messages to/from Cosmos DB
- Saving/loading user settings (domain selection, conversation memory, etc.)
- Session continuity across instance switches using user_id + date as session key

Container Structure:
- conversation_history: One document per message (partition key: /user_id)
- user_settings: One document per user (partition key: /user_id)
"""

import os
import json
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional
from azure.cosmos.aio import CosmosClient
from azure.cosmos import PartitionKey, exceptions
from dotenv import load_dotenv

load_dotenv()


class CosmosStateManager:
    """
    Manages state persistence in Azure Cosmos DB.
    
    Containers:
    - conversation_history: Stores individual chat messages
    - user_settings: Stores user preferences and conversation memory
    """
    
    def __init__(self):
        self.cosmos_uri = os.getenv("COSMOS_DB_URI")
        self.cosmos_key = os.getenv("COSMOS_DB_KEY")
        self.database_name = os.getenv("COSMOS_DB_DATABASE", "fdmbot")
        
        # Container names
        self.messages_container_name = "conversation_history"
        self.settings_container_name = "user_settings"
        
        # TTL settings (in seconds)
        self.message_ttl = 7 * 24 * 60 * 60  # 7 days for messages
        self.settings_ttl = 30 * 24 * 60 * 60  # 30 days for settings
        
        # Lazy initialization
        self._client: Optional[CosmosClient] = None
        self._database = None
        self._messages_container = None
        self._settings_container = None
        self._initialized = False
    
    def is_configured(self) -> bool:
        """Check if Cosmos DB is configured"""
        return bool(self.cosmos_uri and self.cosmos_key)
    
    async def _ensure_initialized(self):
        """Lazily initialize Cosmos DB client and containers"""
        if self._initialized:
            return
        
        if not self.is_configured():
            print("⚠️ Cosmos DB not configured - state persistence disabled")
            return
        
        try:
            print(f"🔌 Connecting to Cosmos DB: {self.cosmos_uri[:50]}...")
            
            self._client = CosmosClient(self.cosmos_uri, credential=self.cosmos_key)
            self._database = self._client.get_database_client(self.database_name)
            self._messages_container = self._database.get_container_client(self.messages_container_name)
            self._settings_container = self._database.get_container_client(self.settings_container_name)
            
            self._initialized = True
            print(f"✅ Cosmos DB connected successfully")
            print(f"   Database: {self.database_name}")
            print(f"   Containers: {self.messages_container_name}, {self.settings_container_name}")
            
        except Exception as e:
            print(f"❌ Failed to connect to Cosmos DB: {e}")
            self._initialized = False
    
    def _get_session_date(self) -> str:
        """Get current date string for session grouping"""
        return datetime.now().strftime("%Y-%m-%d")
    
    def _generate_message_id(self, user_id: str, sequence: int) -> str:
        """Generate unique message ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"msg_{user_id.replace('@', '_').replace('.', '_')}_{timestamp}_{sequence}"
    
    # ============ MESSAGE OPERATIONS ============
    
    async def save_message(
        self,
        user_id: str,
        role: str,  # "user" or "assistant"
        content: str,
        session_date: str = None,
        sequence: int = None,
        sql_result: Dict[str, Any] = None,
        followup_questions: List[str] = None,
        rewritten_question: str = None,
        metadata: Dict[str, Any] = None
    ) -> Optional[str]:
        """
        Save a single message to Cosmos DB.
        
        Args:
            user_id: User email/identifier
            role: "user" or "assistant"
            content: Message content
            session_date: Date string (defaults to today)
            sequence: Message order in session
            sql_result: SQL query results (for assistant messages)
            followup_questions: Generated follow-up questions
            rewritten_question: The rewritten/processed question
            metadata: Additional metadata
        
        Returns:
            Message ID if successful, None otherwise
        """
        await self._ensure_initialized()
        
        if not self._initialized:
            return None
        
        try:
            session_date = session_date or self._get_session_date()
            
            # Auto-generate sequence if not provided
            if sequence is None:
                sequence = await self._get_next_sequence(user_id, session_date)
            
            message_id = self._generate_message_id(user_id, sequence)
            
            document = {
                "id": message_id,
                "user_id": user_id,
                "session_date": session_date,
                "sequence": sequence,
                "role": role,
                "content": content,
                "timestamp": datetime.now().isoformat(),
                "ttl": self.message_ttl
            }
            
            # Add optional fields
            if sql_result:
                document["sql_result"] = sql_result
            if followup_questions:
                document["followup_questions"] = followup_questions
            if rewritten_question:
                document["rewritten_question"] = rewritten_question
            if metadata:
                document["metadata"] = metadata
            
            await self._messages_container.upsert_item(document)
            print(f"💾 Saved message: {role} | seq={sequence} | user={user_id[:20]}...")
            
            return message_id
            
        except Exception as e:
            print(f"❌ Failed to save message: {e}")
            return None
    
    async def _get_next_sequence(self, user_id: str, session_date: str) -> int:
        """Get the next sequence number for a user's session"""
        try:
            query = """
                SELECT VALUE MAX(c.sequence)
                FROM c
                WHERE c.user_id = @user_id AND c.session_date = @session_date
            """
            parameters = [
                {"name": "@user_id", "value": user_id},
                {"name": "@session_date", "value": session_date}
            ]
            
            items = []
            async for item in self._messages_container.query_items(
                query=query,
                parameters=parameters,
                partition_key=user_id
            ):
                items.append(item)
            
            max_seq = items[0] if items and items[0] is not None else 0
            return max_seq + 1
            
        except Exception as e:
            print(f"⚠️ Error getting sequence, defaulting to 1: {e}")
            return 1
    
    async def load_messages(
        self,
        user_id: str,
        session_date: str = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Load messages for a user's session.
        
        Args:
            user_id: User email/identifier
            session_date: Date string (defaults to today)
            limit: Maximum number of messages to return
        
        Returns:
            List of message documents ordered by sequence
        """
        await self._ensure_initialized()
        
        if not self._initialized:
            return []
        
        try:
            session_date = session_date or self._get_session_date()
            
            query = """
                SELECT *
                FROM c
                WHERE c.user_id = @user_id AND c.session_date = @session_date
                ORDER BY c.sequence ASC
                OFFSET 0 LIMIT @limit
            """
            parameters = [
                {"name": "@user_id", "value": user_id},
                {"name": "@session_date", "value": session_date},
                {"name": "@limit", "value": limit}
            ]
            
            messages = []
            async for item in self._messages_container.query_items(
                query=query,
                parameters=parameters,
                partition_key=user_id
            ):
                messages.append(item)
            
            print(f"📂 Loaded {len(messages)} messages for {user_id[:20]}... | date={session_date}")
            return messages
            
        except Exception as e:
            print(f"❌ Failed to load messages: {e}")
            return []
    
    async def delete_session_messages(self, user_id: str, session_date: str = None) -> bool:
        """Delete all messages for a user's session"""
        await self._ensure_initialized()
        
        if not self._initialized:
            return False
        
        try:
            session_date = session_date or self._get_session_date()
            
            # First, get all message IDs
            messages = await self.load_messages(user_id, session_date)
            
            # Delete each message
            for msg in messages:
                await self._messages_container.delete_item(
                    item=msg["id"],
                    partition_key=user_id
                )
            
            print(f"🗑️ Deleted {len(messages)} messages for {user_id[:20]}... | date={session_date}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to delete messages: {e}")
            return False
    
    # ============ USER SETTINGS OPERATIONS ============
    
    async def save_user_settings(
        self,
        user_id: str,
        domain_selection: List[str] = None,
        user_selected_datasets: List[str] = None,
        conversation_memory: Dict[str, Any] = None,
        additional_settings: Dict[str, Any] = None
    ) -> bool:
        """
        Save user settings to Cosmos DB.
        
        Args:
            user_id: User email/identifier
            domain_selection: Selected domains (e.g., ["Revenue"])
            user_selected_datasets: Selected datasets
            conversation_memory: Conversation context/memory
            additional_settings: Any additional settings
        
        Returns:
            True if successful, False otherwise
        """
        await self._ensure_initialized()
        
        if not self._initialized:
            return False
        
        try:
            document = {
                "id": user_id,
                "user_id": user_id,
                "last_active": datetime.now().isoformat(),
                "ttl": self.settings_ttl
            }
            
            if domain_selection is not None:
                document["domain_selection"] = domain_selection
            if user_selected_datasets is not None:
                document["user_selected_datasets"] = user_selected_datasets
            if conversation_memory is not None:
                document["conversation_memory"] = conversation_memory
            if additional_settings:
                document.update(additional_settings)
            
            await self._settings_container.upsert_item(document)
            print(f"💾 Saved settings for {user_id[:20]}...")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to save settings: {e}")
            return False
    
    async def load_user_settings(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Load user settings from Cosmos DB.
        
        Args:
            user_id: User email/identifier
        
        Returns:
            Settings document or None if not found
        """
        await self._ensure_initialized()
        
        if not self._initialized:
            return None
        
        try:
            response = await self._settings_container.read_item(
                item=user_id,
                partition_key=user_id
            )
            print(f"📂 Loaded settings for {user_id[:20]}...")
            return response
            
        except exceptions.CosmosResourceNotFoundError:
            print(f"ℹ️ No settings found for {user_id[:20]}...")
            return None
        except Exception as e:
            print(f"❌ Failed to load settings: {e}")
            return None
    
    # ============ BULK OPERATIONS ============
    
    async def save_all_messages(
        self,
        user_id: str,
        messages: List[Dict[str, Any]],
        session_date: str = None
    ) -> bool:
        """
        Save multiple messages at once (for initial sync or backup).
        
        Args:
            user_id: User email/identifier
            messages: List of message dictionaries with 'role' and 'content'
            session_date: Date string (defaults to today)
        
        Returns:
            True if all saved successfully
        """
        await self._ensure_initialized()
        
        if not self._initialized:
            return False
        
        try:
            session_date = session_date or self._get_session_date()
            
            for idx, msg in enumerate(messages, start=1):
                await self.save_message(
                    user_id=user_id,
                    role=msg.get("role", "user"),
                    content=msg.get("content", ""),
                    session_date=session_date,
                    sequence=idx,
                    sql_result=msg.get("sql_result"),
                    followup_questions=msg.get("followup_questions"),
                    rewritten_question=msg.get("rewritten_question"),
                    metadata=msg.get("metadata")
                )
            
            print(f"💾 Bulk saved {len(messages)} messages for {user_id[:20]}...")
            return True
            
        except Exception as e:
            print(f"❌ Failed to bulk save messages: {e}")
            return False
    
    # ============ SESSION SNAPSHOT METHODS ============
    
    async def save_session_snapshot(
        self,
        user_id: str,
        session_date: str,
        state_dict: Dict[str, Any]
    ) -> bool:
        """
        Save complete session state snapshot to Cosmos DB.
        This replaces individual message/settings saves with one snapshot per session.
        
        Args:
            user_id: User email/identifier
            session_date: Date string in YYYY-MM-DD format
            state_dict: Complete st.session_state dictionary with all variables
        
        Returns:
            True if saved successfully, False otherwise
        
        Document structure:
        {
            "id": "snapshot_user@email.com_2026-01-06",
            "user_id": "user@email.com",
            "type": "session_snapshot",
            "session_date": "2026-01-06",
            "state": { ...all session variables... },
            "updated_at": "2026-01-06T10:30:00Z",
            "ttl": 604800
        }
        """
        await self._ensure_initialized()
        
        if not self._initialized:
            return False
        
        try:
            doc_id = f"snapshot_{user_id}_{session_date}"
            
            document = {
                "id": doc_id,
                "user_id": user_id,
                "type": "session_snapshot",
                "session_date": session_date,
                "state": state_dict,
                "updated_at": datetime.utcnow().isoformat(),
                "ttl": self.message_ttl  # Use message TTL (7 days)
            }
            
            # Upsert (create or replace) - use messages container
            await self._messages_container.upsert_item(document)
            
            print(f"💾 Saved session snapshot for {user_id[:20]}... on {session_date} ({len(state_dict)} variables)")
            return True
            
        except Exception as e:
            print(f"❌ Failed to save session snapshot: {e}")
            return False
    
    async def load_session_snapshot(
        self,
        user_id: str,
        session_date: str
    ) -> Optional[Dict[str, Any]]:
        """
        Load complete session state snapshot from Cosmos DB.
        
        Args:
            user_id: User email/identifier
            session_date: Date string in YYYY-MM-DD format
        
        Returns:
            Dictionary with all session state variables, or None if not found
        """
        await self._ensure_initialized()
        
        if not self._initialized:
            return None
        
        try:
            doc_id = f"snapshot_{user_id}_{session_date}"
            
            # Query by ID and partition key (most efficient) - use messages container
            query = "SELECT c.state FROM c WHERE c.id = @doc_id AND c.type = 'session_snapshot'"
            parameters = [{"name": "@doc_id", "value": doc_id}]
            
            items = []
            async for item in self._messages_container.query_items(
                query=query,
                parameters=parameters,
                partition_key=user_id
            ):
                items.append(item)
            
            if items:
                state = items[0].get('state', {})
                print(f"📂 Loaded session snapshot for {user_id[:20]}... on {session_date} ({len(state)} variables)")
                return state
            else:
                print(f"ℹ️ No session snapshot found for {user_id[:20]}... on {session_date}")
                return None
                
        except exceptions.CosmosResourceNotFoundError:
            print(f"ℹ️ No session snapshot found for {user_id[:20]}... on {session_date}")
            return None
        except Exception as e:
            print(f"❌ Failed to load session snapshot: {e}")
            return None
    
    # ============ UTILITY METHODS ============
    
    async def get_recent_sessions(
        self,
        user_id: str,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Get list of recent session dates with message counts.
        
        Args:
            user_id: User email/identifier
            days: Number of days to look back
        
        Returns:
            List of session summaries
        """
        await self._ensure_initialized()
        
        if not self._initialized:
            return []
        
        try:
            query = """
                SELECT c.session_date, COUNT(1) as message_count, MAX(c.timestamp) as last_activity
                FROM c
                WHERE c.user_id = @user_id
                GROUP BY c.session_date
            """
            parameters = [
                {"name": "@user_id", "value": user_id}
            ]
            
            sessions = []
            async for item in self._messages_container.query_items(
                query=query,
                parameters=parameters,
                partition_key=user_id
            ):
                sessions.append(item)
            
            # Sort by date descending
            sessions.sort(key=lambda x: x.get("session_date", ""), reverse=True)
            
            return sessions[:days]
            
        except Exception as e:
            print(f"❌ Failed to get recent sessions: {e}")
            return []
    
    async def close(self):
        """Close the Cosmos DB client"""
        if self._client:
            await self._client.close()
            self._client = None
            self._initialized = False
            print("🔌 Cosmos DB connection closed")


# Singleton instance for the application
_cosmos_manager: Optional[CosmosStateManager] = None


def get_cosmos_manager() -> CosmosStateManager:
    """Get or create the singleton Cosmos state manager"""
    global _cosmos_manager
    if _cosmos_manager is None:
        _cosmos_manager = CosmosStateManager()
    return _cosmos_manager


async def close_cosmos_manager():
    """Close the singleton Cosmos manager"""
    global _cosmos_manager
    if _cosmos_manager:
        await _cosmos_manager.close()
        _cosmos_manager = None
