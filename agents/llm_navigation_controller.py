from typing import Dict, List, Optional
import json
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class LLMNavigationController:
    """Simple navigation controller: question rewriting + type classification"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
    
    def process_user_query(self, state: AgentState) -> Dict[str, any]:
        """Simple processing: rewrite question + classify type + route"""
        
        # Step 1: Rewrite question using history
        rewritten_question = self._rewrite_question_from_history(state)
        
        # Step 2: Classify question type
        question_type = self._classify_question_type(rewritten_question)
        
        # Step 3: Simple routing based on type
        next_agent = "router_agent" if question_type == "what" else "root_cause_agent"
        
        return {
            'rewritten_question': rewritten_question,
            'question_type': question_type,
            'next_agent': next_agent
        }
    
    def _rewrite_question_from_history(self, state: AgentState) -> str:
        """Rewrite question using user questions history"""
        
        current_question = state.get('current_question', state.get('original_question', ''))
        questions_history = state.get('user_questions_history', [])
        
        # If no history, return original question
        if not questions_history:
            return current_question
        
        # Build history context for LLM
        history_context = ""
        for i, prev_question in enumerate(questions_history[-3:], 1):  # Last 3 questions
            history_context += f"{i}. {prev_question}\n"
        
        rewrite_prompt = f"""
        You have a healthcare finance question that might be a follow-up to previous questions.
        
        Current Question: "{current_question}"
        
        Previous Questions:
        {history_context}
        
        Task: If the current question is a follow-up that references previous context (like "why is that?", "show me more details", "what about the trends?" etc.), rewrite it to be self-contained and clear.
        
        Rules:
        1. If question is already self-contained, return it unchanged
        2. If it's a follow-up, incorporate necessary context to make it standalone
        3. Keep the same intent, just make it clear without needing previous context
        4. Focus on healthcare finance domain
        5. Return only the rewritten question, nothing else
        
        Rewritten Question:
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": rewrite_prompt}
            ])
            
            # Clean up the response
            rewritten = llm_response.strip().strip('"').strip("'")
            
            # Basic validation - if rewrite seems reasonable, use it
            if 10 <= len(rewritten) <= 500 and rewritten.lower() != current_question.lower():
                return rewritten
            else:
                return current_question
                
        except Exception as e:
            print(f"Question rewriting failed: {str(e)}")
            return current_question
    
    def _classify_question_type(self, question: str) -> str:
        """Simple question type classification: what or why"""
        
        classify_prompt = f"""
        Classify this healthcare finance question as either "what" or "why":
        
        Question: "{question}"
        
        Guidelines:
        - "what" questions: Ask for data, facts, numbers, reports, show me, how much, what are the costs, trends, etc.
        - "why" questions: Ask for reasons, causes, explanations, root cause analysis, what caused, what is driving, etc.
        
        Respond with only one word: "what" or "why"
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": classify_prompt}
            ])
            
            response = llm_response.strip().lower()
            
            if "why" in response:
                return "why"
            elif "what" in response:
                return "what"
            else:
                # Fallback to simple keyword detection
                return self._fallback_question_type(question)
                
        except Exception as e:
            print(f"Question classification failed: {str(e)}")
            return self._fallback_question_type(question)
    
    def _fallback_question_type(self, question: str) -> str:
        """Fallback question type detection using keywords"""
        
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
            'history': [],
            'expected_type': 'what',
            'expected_agent': 'router_agent'
        },
        {
            'question': 'Why are they so high?',
            'history': ['What are Q3 pharmacy claims costs?'],
            'expected_type': 'why', 
            'expected_agent': 'root_cause_agent'
        },
        {
            'question': 'Show me the breakdown',
            'history': ['What are Q3 pharmacy claims costs?', 'Why are pharmacy costs increasing?'],
            'expected_type': 'what',
            'expected_agent': 'router_agent'
        },
        {
            'question': 'What is driving the cost increase?',
            'history': [],
            'expected_type': 'why',
            'expected_agent': 'root_cause_agent'
        }
    ]
    
    for i, scenario in enumerate(test_scenarios, 1):
        print(f"\n🔍 Test {i}: {scenario['question']}")
        print(f"History: {scenario['history']}")
        
        state = AgentState(
            session_id="test_session",
            user_id="test_user",
            current_question=scenario['question'],
            original_question=scenario['question'],
            user_questions_history=scenario['history']
        )
        
        try:
            result = nav_controller.process_user_query(state)
            print(f"✅ Rewritten: '{result['rewritten_question']}'")
            print(f"📝 Type: {result['question_type']} (expected: {scenario['expected_type']})")
            print(f"🎯 Agent: {result['next_agent']} (expected: {scenario['expected_agent']})")
            
            # Check if results match expectations
            type_match = result['question_type'] == scenario['expected_type']
            agent_match = result['next_agent'] == scenario['expected_agent']
            print(f"✓ Results: Type {'✅' if type_match else '❌'} Agent {'✅' if agent_match else '❌'}")
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            
    print("\n" + "="*50)
    print("✅ Navigation Controller Testing Complete")