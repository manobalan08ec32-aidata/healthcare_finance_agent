import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from core.logger import setup_logger, log_with_user_context
from prompts.navigation_prompts import NAVIGATION_SYSTEM_PROMPT, NAVIGATION_COMBINED_PROMPT

# Initialize logger for this module
logger = setup_logger(__name__)

class LLMNavigationController:
    """Single-prompt navigation controller with complete analysis, rewriting, and filter extraction"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
    
    def _log(self, level: str, message: str, state: AgentState = None, **extra):
        """Helper method to log with user context from state"""
        user_email = state.get('user_email') if state else None
        session_id = state.get('session_id') if state else None
        log_with_user_context(logger, level, f"[Navigation] {message}", user_email, session_id, **extra)
    
    def _calculate_forecast_cycle(self) -> str:
        """
        Calculate current forecast cycle based on month:
        - Feb to May: 2+10
        - Jun to Aug: 5+7
        - Sep to Oct: 8+4
        - Nov to Jan: 9+3
        """
        from datetime import datetime
        current_month = datetime.now().month
        
        if 2 <= current_month <= 5:  # February to May
            return "2+10"
        elif 6 <= current_month <= 8:  # June to August
            return "5+7"
        elif 9 <= current_month <= 11:  # September to October
            return "8+4"
        else:  # November (11), December (12), January (1)
            return "9+3"
    
    async def process_user_query(self, state: AgentState) -> Dict[str, any]:
        """
        Main entry point: Single-step LLM processing
        
        Args:
            state: Agent state containing question and history
        """
        
        current_question = state.get('current_question', state.get('original_question', ''))
        existing_domain_selection = state.get('domain_selection', [])
        total_retry_count = state.get('llm_retry_count', 0)
        
        self._log('info', "Processing user query", state, question=current_question[:100])
        
        # Single-step processing: Analyze → Rewrite → Extract in one call
        return await self._single_step_processing(
            current_question, existing_domain_selection, total_retry_count, state
        )
    
    async def _single_step_processing(self, current_question: str, existing_domain_selection: List[str], 
                                     total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """Single-step LLM processing: Analyze → Rewrite → Extract in one prompt"""
        
        self._log('info', "Starting question analysis and rewriting")
        questions_history = state.get('user_question_history', [])
        previous_question = state.get('rewritten_question', '') 
        conversation_memory = state.get('conversation_memory', {
            'dimensions': {},
            'analysis_context': {
                'current_analysis_type': None,
                'analysis_history': []
            }
        })
        history_context = questions_history[-2:] if questions_history else []
        
        # Calculate current forecast cycle based on current month
        current_forecast_cycle = self._calculate_forecast_cycle()
        self._log('info', "Calculated forecast cycle", state, cycle=current_forecast_cycle)
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # ═══════════════════════════════════════════════════════════════
                # SINGLE STEP: COMBINED PROMPT
                # ═══════════════════════════════════════════════════════════════
                
                prompt = self._build_combined_prompt(
                    current_question, 
                    previous_question,
                    history_context,
                    current_forecast_cycle,
                    conversation_memory
                )
                print('question prompt', prompt)
                print("Current Timestamp before question validator call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=1000,
                    temperature=0.1,  # Deterministic rewriting
                    top_p=0.7 , # Focused sampling
                    system_prompt=NAVIGATION_SYSTEM_PROMPT
                )
                print("Current Timestamp after Question validator call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print("LLM Response:", response)
                
                # Log LLM output - actual response truncated to 500 chars for safety
                self._log('info', "LLM response received from navigation validator", state,
                         llm_response=response,
                         retry_attempt=retry_count)
                
                # Check if LLM cannot answer - trigger retry
                if "Sorry, the model cannot answer this question" in response:
                    retry_count += 1
                    print(f"⚠️ LLM cannot answer - retrying ({retry_count}/{max_retries})")
                    if retry_count < max_retries:
                        await asyncio.sleep(2 ** retry_count)
                        continue
                    else:
                        print(f"Failed after {max_retries} retries - returning Optum Gateway error message")
                        error_message = "Sorry, the Optum Gateway is rejecting your request. Please try after 5 Minutes."
                        return {
                            'rewritten_question': current_question,
                            'question_type': 'what',
                            'next_agent': 'END',
                            'next_agent_disp': 'Optum Gateway temporarily unavailable',
                            'requires_domain_clarification': False,
                            'domain_followup_question': None,
                            'domain_selection': existing_domain_selection,
                            'greeting_response': error_message,
                            'llm_retry_count': total_retry_count + retry_count,
                            'pending_business_question': '',
                            'error': True,
                            'error_message': error_message,
                            'filter_values': [],
                            'user_friendly_message': error_message
                        }
                
                # Strip markdown code blocks if present
                cleaned_response = response.strip()
                if cleaned_response.startswith("```json"):
                    cleaned_response = cleaned_response[7:]  # Remove ```json
                if cleaned_response.startswith("```"):
                    cleaned_response = cleaned_response[3:]  # Remove ```
                if cleaned_response.endswith("```"):
                    cleaned_response = cleaned_response[:-3]  # Remove trailing ```
                cleaned_response = cleaned_response.strip()
                
                try:
                    result = json.loads(cleaned_response)
                except json.JSONDecodeError as json_error:
                    print(f"LLM response is not valid JSON: {json_error}")
                    print(f"Cleaned response: {cleaned_response[:200]}")
                    # Treat as greeting
                    result = {
                        'analysis': {
                            'input_type': 'greeting',
                            'is_valid_business_question': False,
                            'response_message': response.strip()
                        }
                    }
                
                # Handle non-business questions (greeting, DML, invalid)
                analysis = result.get('analysis', {})
                input_type = analysis.get('input_type', 'business_question')
                is_valid_business_question = analysis.get('is_valid_business_question', False)
                response_message = analysis.get('response_message', '')
                
                if input_type in ['greeting', 'dml_ddl', 'invalid_business']:
                    return {
                        'rewritten_question': current_question,
                        'question_type': 'what',
                        'next_agent': 'END',
                        'next_agent_disp': f'{input_type.replace("_", " ").title()} response',
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': existing_domain_selection,
                        'greeting_response': response_message,
                        'is_dml_ddl': input_type == 'dml_ddl',
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'filter_values': [],
                        'user_friendly_message': response_message
                    }
                
                # Handle business questions
                if input_type == 'business_question' and is_valid_business_question:
                    
                    rewrite = result.get('rewrite', {})
                    filters = result.get('filters', {})
                    
                    rewritten_question = rewrite.get('rewritten_question', current_question)
                    question_type = rewrite.get('question_type', 'what')
                    user_message = rewrite.get('user_message', '')
                    filter_values = filters.get('filter_values', [])
                    
                    # Determine next agent based on question type
                    next_agent = "router_agent" if question_type == "what" else "root_cause_agent"
                    
                    # Return complete result
                    return {
                        'rewritten_question': rewritten_question,
                        'question_type': question_type,
                        'context_type': analysis.get('context_decision', 'NEW'),
                        'inherited_context': user_message,
                        'next_agent': next_agent,
                        'next_agent_disp': next_agent.replace('_', ' ').title(),
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': existing_domain_selection,
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'filter_values': filter_values,
                        'user_friendly_message': user_message,
                        'decision_from_analysis': analysis.get('context_decision', ''),
                        'detected_prefix': analysis.get('detected_prefix', 'none'),
                        'clean_question': analysis.get('clean_question', current_question),
                        'reasoning': analysis.get('reasoning', '')
                    }
                
                # Fallback - invalid business question
                return {
                    'rewritten_question': current_question,
                    'question_type': 'what',
                    'next_agent': 'END',
                    'next_agent_disp': 'Invalid business question',
                    'requires_domain_clarification': False,
                    'domain_followup_question': None,
                    'domain_selection': existing_domain_selection,
                    'greeting_response': "I specialize in healthcare finance analytics. Please ask about claims, ledgers, payments, or other healthcare data.",
                    'llm_retry_count': total_retry_count + retry_count,
                    'pending_business_question': '',
                    'filter_values': [],
                    'user_friendly_message': "I specialize in healthcare finance analytics."
                }
                        
            except Exception as e:
                retry_count += 1
                print(f"Single-step processing attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"Retrying... ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    print(f"All retries failed: {str(e)}")
                    return {
                        'rewritten_question': current_question,
                        'question_type': 'what',
                        'next_agent': 'END',
                        'next_agent_disp': 'Model serving endpoint failed',
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': existing_domain_selection,
                        'greeting_response': "Model serving endpoint failed. Please try again after some time.",
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'error': True,
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts",
                        'filter_values': [],
                        'user_friendly_message': "Service temporarily unavailable."
                    }
    
    def _build_combined_prompt(self, current_question: str, previous_question: str,
                               history_context: List, current_forecast_cycle: str,
                               conversation_memory: Dict = None) -> str:
        """
        Combined Prompt: Analyze → Rewrite → Extract in single call
        Uses the NAVIGATION_COMBINED_PROMPT template from prompts package.
        """

        # Get current year and month dynamically
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        current_month_name = now.strftime("%B")

        # Calculate previous year for month context
        previous_year = current_year - 1

        # Initialize conversation memory if not provided
        if conversation_memory is None:
            conversation_memory = {
                'dimensions': {},
                'analysis_context': {
                    'current_analysis_type': None,
                    'analysis_history': []
                }
            }

        # Build memory context for prompt - keep as JSON for dimension key extraction
        memory_dimensions = conversation_memory.get('dimensions', {})
        memory_context = "None"

        if memory_dimensions:
            memory_context = f"\n**CONVERSATION MEMORY (Recent Analysis Context)**:\n```json\n{json.dumps(memory_dimensions)}\n```\n"
            print('memory_context_json', memory_context)

        # Calculate dynamic year comparisons for the prompt
        december_comparison = "BEFORE" if 12 < current_month else "AFTER" if 12 > current_month else "SAME AS"
        december_year = previous_year if 12 > current_month else current_year
        january_comparison = "BEFORE" if 1 < current_month else "AFTER" if 1 > current_month else "SAME AS"
        january_year = previous_year if 1 > current_month else current_year
        most_recent_december_year = previous_year if current_month < 12 else current_year

        # Quarter year calculations
        q1_year = previous_year if 3 < current_month else current_year
        q2_year = previous_year if 6 < current_month else current_year
        q3_year = previous_year if 9 < current_month else current_year
        q4_year = previous_year if 12 < current_month else current_year

        # Format the prompt template with dynamic values
        prompt = NAVIGATION_COMBINED_PROMPT.format(
            current_question=current_question,
            previous_question=previous_question if previous_question else 'None',
            history_context=history_context,
            current_month_name=current_month_name,
            current_year=current_year,
            current_month=current_month,
            previous_year=previous_year,
            current_forecast_cycle=current_forecast_cycle,
            memory_context=memory_context,
            december_comparison=december_comparison,
            december_year=december_year,
            january_comparison=january_comparison,
            january_year=january_year,
            most_recent_december_year=most_recent_december_year,
            q1_year=q1_year,
            q2_year=q2_year,
            q3_year=q3_year,
            q4_year=q4_year
        )

        return prompt
