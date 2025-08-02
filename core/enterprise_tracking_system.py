import time
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from state_schema import (
    AgentState, create_conversation_entry, create_llm_interaction_entry,
    create_user_interaction_entry, create_agent_execution_entry,
    create_audit_entry, create_performance_entry
)
from databricks_client import DatabricksClient

class EnterpriseTrackingSystem:
    """
    Comprehensive tracking system for enterprise healthcare finance agentic workflows.
    Tracks all interactions, decisions, performance metrics, and audit trails.
    """
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
        # Database tables for enterprise tracking
        self.tables = {
            'conversation_log': 'prd_optumrx_orxfdmprdsa.rag.conversation_log',
            'llm_interactions': 'prd_optumrx_orxfdmprdsa.rag.llm_interactions',
            'agent_executions': 'prd_optumrx_orxfdmprdsa.rag.agent_executions',
            'user_interactions': 'prd_optumrx_orxfdmprdsa.rag.user_interactions',
            'performance_metrics': 'prd_optumrx_orxfdmprdsa.rag.performance_metrics',
            'audit_trail': 'prd_optumrx_orxfdmprdsa.rag.audit_trail',
            'session_summaries': 'prd_optumrx_orxfdmprdsa.rag.session_summaries'
        }
    
    # ============ CONVERSATION TRACKING ============
    
    def track_user_question(self, state: AgentState, question: str) -> AgentState:
        """Track user question with comprehensive metadata"""
        
        # Create conversation entry
        conversation_entry = create_conversation_entry(
            user_question=question,
            agent_response="Processing...",
            metadata={
                'session_id': state['session_id'],
                'user_id': state['user_id'],
                'question_length': len(question),
                'question_type': 'initial' if not state.get('conversation_history') else 'follow_up'
            }
        )
        
        # Create user interaction entry
        user_interaction = create_user_interaction_entry(
            interaction_type='question',
            data={
                'question': question,
                'question_number': len(state.get('conversation_history', [])) + 1,
                'session_context': len(state.get('conversation_history', []))
            }
        )
        
        # Create audit entry
        audit_entry = create_audit_entry(
            action='user_question_submitted',
            user_id=state['user_id'],
            details={
                'question': question,
                'session_id': state['session_id'],
                'timestamp': datetime.now().isoformat()
            }
        )
        
        # Update state with tracking data
        return {
            **state,
            'current_question': question,
            'conversation_history': [conversation_entry],
            'user_interactions': [user_interaction],
            'audit_trail': [audit_entry],
            'last_update_time': datetime.now().isoformat()
        }
    
    def track_agent_response(self, state: AgentState, agent_name: str, response: str) -> AgentState:
        """Track agent response and update conversation"""
        
        # Update last conversation entry with response
        if state.get('conversation_history'):
            last_entry = state['conversation_history'][-1].copy()
            last_entry['agent_response'] = response
            last_entry['response_timestamp'] = datetime.now().isoformat()
            
            # Update conversation history
            updated_history = state['conversation_history'][:-1] + [last_entry]
        else:
            updated_history = state.get('conversation_history', [])
        
        return {
            **state,
            'conversation_history': updated_history,
            'last_update_time': datetime.now().isoformat()
        }
    
    # ============ LLM INTERACTION TRACKING ============
    
    def track_llm_call(self, state: AgentState, agent_name: str, prompt: str, 
                      response: str, duration: float = None, tokens_used: int = None) -> AgentState:
        """Track LLM API calls with performance metrics"""
        
        llm_interaction = create_llm_interaction_entry(
            agent_name=agent_name,
            prompt=prompt,
            response=response,
            tokens_used=tokens_used,
            duration=duration
        )
        
        # Track token usage separately
        token_entry = {
            'timestamp': datetime.now().isoformat(),
            'agent_name': agent_name,
            'tokens_used': tokens_used or 0,
            'estimated_cost': (tokens_used or 0) * 0.00002,  # Rough estimate
            'session_id': state['session_id']
        }
        
        return {
            **state,
            'llm_interactions': [llm_interaction],
            'token_usage': [token_entry],
            'last_update_time': datetime.now().isoformat()
        }
    
    # ============ AGENT EXECUTION TRACKING ============
    
    def track_agent_execution(self, state: AgentState, agent_name: str, action: str, 
                             result: str, duration: float = None, metadata: Dict = None) -> AgentState:
        """Track individual agent executions"""
        
        execution_entry = create_agent_execution_entry(
            agent_name=agent_name,
            action=action,
            result=result,
            duration=duration,
            metadata=metadata
        )
        
        # Track performance
        performance_entry = create_performance_entry(
            operation=f"{agent_name}_{action}",
            duration=duration or 0.0,
            success=result not in ['failed', 'error'],
            metadata=metadata
        )
        
        return {
            **state,
            'agent_executions': [execution_entry],
            'performance_tracking': [performance_entry],
            'last_update_time': datetime.now().isoformat()
        }
    
    # ============ VECTOR SEARCH TRACKING ============
    
    def track_vector_search(self, state: AgentState, query: str, results: List[Dict], 
                           duration: float = None, index_name: str = None) -> AgentState:
        """Track vector search operations"""
        
        search_entry = {
            'timestamp': datetime.now().isoformat(),
            'query': query,
            'index_name': index_name,
            'results_count': len(results),
            'duration_seconds': duration,
            'top_result': results[0].get('table_name') if results else None,
            'session_id': state['session_id']
        }
        
        results_entry = {
            'timestamp': datetime.now().isoformat(),
            'search_query': query,
            'results': results,
            'result_scores': [r.get('score', 0) for r in results],
            'session_id': state['session_id']
        }
        
        return {
            **state,
            'vector_searches': [search_entry],
            'search_results_history': [results_entry],
            'last_update_time': datetime.now().isoformat()
        }
    
    # ============ DATASET SELECTION TRACKING ============
    
    def track_dataset_selection(self, state: AgentState, selected_dataset: str, 
                               reasoning: str, confidence: float, 
                               available_options: List[Dict]) -> AgentState:
        """Track dataset selection decisions"""
        
        selection_entry = {
            'timestamp': datetime.now().isoformat(),
            'selected_dataset': selected_dataset,
            'selection_reasoning': reasoning,
            'confidence_score': confidence,
            'available_options': [opt.get('table_name') for opt in available_options],
            'option_count': len(available_options),
            'selection_method': 'llm_decision',
            'session_id': state['session_id']
        }
        
        return {
            **state,
            'dataset_selections': [selection_entry],
            'last_update_time': datetime.now().isoformat()
        }
    
    # ============ USER CLARIFICATION TRACKING ============
    
    def track_clarification_request(self, state: AgentState, question: str, 
                                   options: List[Dict]) -> AgentState:
        """Track when system requests user clarification"""
        
        clarification_entry = {
            'timestamp': datetime.now().isoformat(),
            'clarification_question': question,
            'options_provided': options,
            'option_count': len(options),
            'trigger_reason': 'ambiguous_dataset_selection',
            'session_id': state['session_id']
        }
        
        return {
            **state,
            'clarification_requests': [clarification_entry],
            'requires_user_input': True,
            'last_update_time': datetime.now().isoformat()
        }
    
    def track_user_clarification(self, state: AgentState, user_choice: Dict) -> AgentState:
        """Track user's clarification response"""
        
        clarification_response = {
            'timestamp': datetime.now().isoformat(),
            'user_choice': user_choice,
            'response_time_seconds': self._calculate_response_time(state),
            'session_id': state['session_id']
        }
        
        return {
            **state,
            'user_clarifications': [clarification_response],
            'requires_user_input': False,
            'last_update_time': datetime.now().isoformat()
        }
    
    # ============ ERROR TRACKING ============
    
    def track_error(self, state: AgentState, agent_name: str, error: str, 
                   error_type: str = 'execution_error') -> AgentState:
        """Track errors and exceptions"""
        
        error_entry = {
            'timestamp': datetime.now().isoformat(),
            'agent_name': agent_name,
            'error_message': error,
            'error_type': error_type,
            'session_id': state['session_id'],
            'user_id': state['user_id'],
            'context': {
                'current_question': state.get('current_question'),
                'selected_dataset': state.get('selected_dataset'),
                'workflow_step': len(state.get('workflow_steps', []))
            }
        }
        
        return {
            **state,
            'error_log': [error_entry],
            'last_update_time': datetime.now().isoformat()
        }
    
    # ============ WORKFLOW STEP TRACKING ============
    
    def track_workflow_step(self, state: AgentState, step_name: str, 
                           step_result: str, duration: float = None) -> AgentState:
        """Track overall workflow progression"""
        
        step_entry = {
            'timestamp': datetime.now().isoformat(),
            'step_name': step_name,
            'step_number': len(state.get('workflow_steps', [])) + 1,
            'step_result': step_result,
            'duration_seconds': duration,
            'session_id': state['session_id']
        }
        
        return {
            **state,
            'workflow_steps': [step_entry],
            'last_update_time': datetime.now().isoformat()
        }
    
    # ============ DATABASE PERSISTENCE ============
    
    def persist_session_data(self, state: AgentState) -> bool:
        """Persist all tracking data to Databricks tables"""
        
        try:
            # Persist conversation log
            self._persist_conversation_log(state)
            
            # Persist LLM interactions
            self._persist_llm_interactions(state)
            
            # Persist agent executions
            self._persist_agent_executions(state)
            
            # Persist performance metrics
            self._persist_performance_metrics(state)
            
            # Persist audit trail
            self._persist_audit_trail(state)
            
            return True
            
        except Exception as e:
            print(f"Failed to persist session data: {str(e)}")
            return False
    
    def _persist_conversation_log(self, state: AgentState):
        """Persist conversation history to database"""
        
        for entry in state.get('conversation_history', []):
            insert_sql = f"""
            INSERT INTO {self.tables['conversation_log']} 
            (session_id, user_id, timestamp, user_question, agent_response, metadata)
            VALUES (
                '{state['session_id']}',
                '{state['user_id']}',
                '{entry['timestamp']}',
                '{entry['user_question'].replace("'", "''")}',
                '{entry['agent_response'].replace("'", "''")}',
                '{json.dumps(entry['metadata']).replace("'", "''")}'
            )
            """
            self.db_client.execute_sql(insert_sql)
    
    def _persist_llm_interactions(self, state: AgentState):
        """Persist LLM interaction logs"""
        
        for interaction in state.get('llm_interactions', []):
            insert_sql = f"""
            INSERT INTO {self.tables['llm_interactions']}
            (session_id, timestamp, agent_name, prompt, response, tokens_used, duration_seconds)
            VALUES (
                '{state['session_id']}',
                '{interaction['timestamp']}',
                '{interaction['agent_name']}',
                '{interaction['prompt'].replace("'", "''")}',
                '{interaction['response'].replace("'", "''")}',
                {interaction['tokens_used'] or 0},
                {interaction['duration_seconds'] or 0.0}
            )
            """
            self.db_client.execute_sql(insert_sql)
    
    def _persist_agent_executions(self, state: AgentState):
        """Persist agent execution logs"""
        
        for execution in state.get('agent_executions', []):
            insert_sql = f"""
            INSERT INTO {self.tables['agent_executions']}
            (session_id, timestamp, agent_name, action, result, duration_seconds, metadata)
            VALUES (
                '{state['session_id']}',
                '{execution['timestamp']}',
                '{execution['agent_name']}',
                '{execution['action']}',
                '{execution['result']}',
                {execution['duration_seconds'] or 0.0},
                '{json.dumps(execution['metadata']).replace("'", "''")}'
            )
            """
            self.db_client.execute_sql(insert_sql)
    
    def _persist_performance_metrics(self, state: AgentState):
        """Persist performance tracking data"""
        
        for metric in state.get('performance_tracking', []):
            insert_sql = f"""
            INSERT INTO {self.tables['performance_metrics']}
            (session_id, timestamp, operation, duration_seconds, success, metadata)
            VALUES (
                '{state['session_id']}',
                '{metric['timestamp']}',
                '{metric['operation']}',
                {metric['duration_seconds']},
                {metric['success']},
                '{json.dumps(metric['metadata']).replace("'", "''")}'
            )
            """
            self.db_client.execute_sql(insert_sql)
    
    def _persist_audit_trail(self, state: AgentState):
        """Persist audit trail for compliance"""
        
        for audit in state.get('audit_trail', []):
            insert_sql = f"""
            INSERT INTO {self.tables['audit_trail']}
            (session_id, timestamp, action, user_id, details)
            VALUES (
                '{state['session_id']}',
                '{audit['timestamp']}',
                '{audit['action']}',
                '{audit['user_id']}',
                '{json.dumps(audit['details']).replace("'", "''")}'
            )
            """
            self.db_client.execute_sql(insert_sql)
    
    # ============ UTILITY FUNCTIONS ============
    
    def _calculate_response_time(self, state: AgentState) -> float:
        """Calculate user response time for clarifications"""
        
        clarification_requests = state.get('clarification_requests', [])
        if clarification_requests:
            last_request_time = clarification_requests[-1]['timestamp']
            request_dt = datetime.fromisoformat(last_request_time.replace('Z', '+00:00'))
            response_time = (datetime.now() - request_dt).total_seconds()
            return response_time