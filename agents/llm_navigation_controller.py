from typing import Dict, List, Optional
import json
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class LLMNavigationController:
    """Simplified navigation controller with question rewriting and smart routing"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
    
    def process_user_query(self, state: AgentState) -> Dict[str, any]:
        """Main processing: rewrite question + route to next agent"""
        
        # Function 1: Rewrite question based on history if needed
        rewritten_question = self._rewrite_question_from_history(state)
        
        # Function 2: Determine next agent based on context and question type
        next_agent = self._determine_next_agent(state, rewritten_question)
        
        return {
            'rewritten_question': rewritten_question,
            'next_agent': next_agent,
            'question_type': self._get_question_type(rewritten_question)
        }
    
    def _rewrite_question_from_history(self, state: AgentState) -> str:
        """Function 1: Rewrite question based on conversation history"""
        
        current_question = state.get('current_question', state.get('original_question', ''))
        
        # Get last few conversation entries for context
        conversation_history = state.get('conversation_history', [])
        if not conversation_history or len(conversation_history) < 2:
            # No meaningful history, return original question
            return current_question
        
        # Get last 2-3 conversation entries
        recent_history = conversation_history[-3:]
        
        # Build context for LLM
        history_context = ""
        for conv in recent_history:
            if conv.get('user_question') and conv.get('agent_response'):
                history_context += f"Previous Q: {conv['user_question']}\n"
                history_context += f"Previous A: {conv['agent_response'][:200]}...\n\n"
        
        rewrite_prompt = f"""
        You have a healthcare finance question that might be a follow-up to previous conversation.
        
        Current Question: "{current_question}"
        
        Recent Conversation History:
        {history_context}
        
        Task: If the current question is a follow-up that references previous context (like "why is that?", "show me more details", "what about X?" etc.), rewrite it to be self-contained.
        
        Rules:
        1. If question is already self-contained, return it unchanged
        2. If it's a follow-up, incorporate necessary context to make it standalone
        3. Don't change the intent, just make it clear without needing previous context
        4. Keep healthcare finance domain focus
        
        Return only the rewritten question, nothing else.
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": rewrite_prompt}
            ])
            
            # Clean up the response (remove quotes, extra text)
            rewritten = llm_response.strip().strip('"').strip("'")
            
            # If rewrite seems reasonable, use it; otherwise use original
            if len(rewritten) > 10 and len(rewritten) < 500:
                return rewritten
            else:
                return current_question
                
        except Exception as e:
            print(f"Question rewriting failed: {str(e)}")
            return current_question
    
    def _determine_next_agent(self, state: AgentState, question: str) -> str:
        """Function 2: Determine next agent based on context and question type"""
        
        # Check if we have previous context (metadata from last table selection)
        metadata_context = state.get('metadata_context')
        has_context = bool(metadata_context and metadata_context.get('table_name'))
        
        # Get question type
        question_type = self._get_question_type(question)
        
        # Routing logic
        if not has_context:
            # No context available, need to select dataset
            return "router_agent"
        
        # We have context, check if it can answer current question
        can_answer = self._check_if_context_can_answer(metadata_context, question)
        
        if not can_answer:
            # Current context can't answer, need new dataset
            return "router_agent"
        
        # Context can answer the question
        if question_type == "why":
            return "root_cause_agent"
        else:  # "what" questions
            return "sql_generator_agent"
    
    def _check_if_context_can_answer(self, metadata_context: Dict, question: str) -> bool:
        """Check if existing table context can answer the question"""
        
        if not metadata_context:
            return False
        
        table_name = metadata_context.get('table_name', '')
        table_description = metadata_context.get('description', '')
        
        check_prompt = f"""
        You have a healthcare finance question and information about a previously selected table.
        
        Question: "{question}"
        
        Available Table:
        - Name: {table_name}
        - Description: {table_description}
        
        Can this table answer the question? Consider:
        1. Does the table domain match the question domain?
        2. Does the table likely contain the required data elements?
        3. Is the question scope appropriate for this table?
        
        Respond with only: YES or NO
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": check_prompt}
            ])
            
            response = llm_response.strip().upper()
            return "YES" in response
            
        except Exception as e:
            print(f"Context checking failed: {str(e)}")
            # Default to requiring new dataset selection on error
            return False
    
    def _get_question_type(self, question: str) -> str:
        """Simple question type classification: what or why"""
        
        question_lower = question.lower()
        
        # Why question indicators
        why_indicators = ['why', 'what caused', 'what is causing', 'reason for', 'cause of', 
                         'what is driving', 'what drives', 'root cause', 'explain why']
        
        if any(indicator in question_lower for indicator in why_indicators):
            return "why"
        else:
            return "what"

# Example usage
if __name__ == "__main__":
    from core.databricks_client import DatabricksClient
    
    db_client = DatabricksClient()
    nav_controller = LLMNavigationController(db_client)
    
    # Test scenarios
    test_scenarios = [
        {
            'question': 'What are Q3 pharmacy claims costs?',
            'context': None,
            'expected_agent': 'router_agent'
        },
        {
            'question': 'Why are they so high?',
            'context': {'table_name': 'pharmacy_claims', 'description': 'Pharmacy claims data'},
            'expected_agent': 'root_cause_agent'
        },
        {
            'question': 'Show me more details',
            'context': {'table_name': 'pharmacy_claims', 'description': 'Pharmacy claims data'},
            'expected_agent': 'sql_generator_agent'
        }
    ]
    
    for scenario in test_scenarios:
        print(f"\n🔍 Testing: {scenario['question']}")
        
        state = AgentState(
            session_id="test_session",
            user_id="test_user",
            current_question=scenario['question'],
            original_question=scenario['question'],
            metadata_context=scenario['context']
        )
        
        try:
            result = nav_controller.process_user_query(state)
            print(f"✅ Rewritten: {result['rewritten_question']}")
            print(f"📝 Type: {result['question_type']}")
            print(f"🎯 Next Agent: {result['next_agent']}")
            print(f"Expected: {scenario['expected_agent']}")
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            
    print("\n" + "="*50)
    print("Navigation Controller Testing Complete")