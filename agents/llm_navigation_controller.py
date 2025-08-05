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
        questions_history = state.get('user_question_history', [])
        print('questions_history',questions_history)
        last_three = questions_history[-3:] if questions_history else []
        history_context = "\n".join(last_three)
        print(history_context)
        if not questions_history:
            return current_question
        
        rewrite_prompt = f"""
        You have a healthcare finance question that might be a follow-up to previous questions.
        
        Current Question: "{current_question}"
        
        Previous Questions:
        {history_context}
        
        Task: If the current question is a follow-up that references previous context (like "why is that?", "show me more details", "what about the trends?" etc.), rewrite it to be self-contained and clear.
        
        Rules:
        1. Rewrite the user's query as a standalone question, including all necessary details. Do not add abbreviations unless they are present in the user's original prompt.
        2. Use the 'Conversation History' and any follow-up answers provided by the user to rewrite the query.
        3. If the user's latest question is about a new metric or topic (different from previous questions), treat it as a new query and do NOT use unrelated previous context.
        4. Only use relevant context from the conversation history if the user's latest question is a follow-up or clarification to a previous question.
        5. Ensure the rewritten question is complete and ready to be used for SQL generation and RAG retrieval.
        6. Preserve the keywords in the rewritten question to improve search accuracy and relevance during RAG retrieval.
        7. If the follow up question doesnt have full statement then rewrite using last question.
        
        RESPONSE FORMAT:
        Return ONLY valid JSON with no markdown, explanations, or other text:
        
        {{
            "rewritten_question": "your rewritten question here"
        }}
        
        CRITICAL: Return only the JSON object, no other formatting or text.
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": rewrite_prompt}
            ])
            response_json = json.loads(llm_response)
            rewritten = response_json.get('rewritten_question', '').strip()
            if 10 <= len(rewritten) <= 500 and rewritten.lower() != current_question.lower():
                return rewritten
            else:
                return current_question
        except Exception as e:
            return current_question
    
    def _classify_question_type(self, question: str) -> str:
        """Simple question type classification: what or why"""
        
        classify_prompt = f"""
        Classify this healthcare finance question as either "what" or "why":
        
        Question: "{question}"
        
        Guidelines:
        - "what" questions: Ask for data, facts, numbers, reports, show me, how much, what are the costs, trends, etc.
        - "why" questions: Ask for reasons, causes, explanations, root cause analysis, what caused, what is driving, etc.
        
        RESPONSE FORMAT:
        Return ONLY valid JSON with no markdown, explanations, or other text:
        
        {{
            "question_type": "what"
        }}
        
        OR
        
        {{
            "question_type": "why"
        }}
        
        CRITICAL: Return only the JSON object with question_type as either "what" or "why", no other formatting or text.
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": classify_prompt}
            ])
            
            try:
                response_json = json.loads(llm_response)
                question_type = response_json.get('question_type', '').strip().lower()
                
                if question_type in ['what', 'why']:
                    return question_type
                else:
                    return self._fallback_question_type(question)
                    
            except json.JSONDecodeError as e:
                return self._fallback_question_type(question)
                
        except Exception as e:
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

