import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from core.logger import setup_logger, log_with_user_context
from prompts.navigation_prompts import (
    NAVIGATION_SYSTEM_PROMPT,
    NAVIGATION_COMBINED_PROMPT,
    NAVIGATION_CLASSIFICATION_SYSTEM_PROMPT,
    NAVIGATION_CLASSIFICATION_PROMPT,
    NAVIGATION_REWRITE_SYSTEM_PROMPT,
    NAVIGATION_REWRITE_PROMPT,
    SMART_YEAR_RULES,
    FILTER_INHERITANCE_RULES,
    COMPONENT_EXTRACTION_RULES,
    METRIC_INHERITANCE_RULES,
    FORECAST_CYCLE_RULES,
    REWRITTEN_QUESTION_RULES,
    FILTER_EXTRACTION_RULES
)

# Initialize logger for this module
logger = setup_logger(__name__)

class LLMNavigationController:
    """Two-step navigation controller: Classification → Rewrite (with REFLECTION detection)"""
    
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
        Main entry point: Two-step LLM processing (Classification → Rewrite)

        Flow:
        1. Classification: Detect prefix, classify input type, determine context
        2. Early exit for greetings, DML, invalid inputs
        3. REFLECTION detection: Route to reflection_agent for corrections
        4. Rewrite: Apply context inheritance and extract filters

        Args:
            state: Agent state containing question and history
        """

        current_question = state.get('current_question', state.get('original_question', ''))
        existing_domain_selection = state.get('domain_selection', [])
        total_retry_count = state.get('llm_retry_count', 0)

        self._log('info', "Processing user query (two-step)", state, question=current_question)

        # Two-step processing: Classify → Rewrite (with REFLECTION detection)
        return await self._two_step_processing(
            current_question, existing_domain_selection, total_retry_count, state
        )
    
    async def _two_step_processing(self, current_question: str, existing_domain_selection: List[str],
                                    total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """Two-step LLM processing: Classification → Rewrite (only if needed)

        Step 1 (Classification): Lightweight prefix detection and input classification (~150 tokens)
        - Early exit for greetings, DML, invalid inputs
        - REFLECTION detection routes to reflection_agent

        Step 2 (Rewrite): Full question rewriting with context inheritance (~500 tokens)
        - Only called for valid business questions with NEW or FOLLOW_UP context
        """

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
                # ════════════════════════════════════════════════════════════════════════
                # STEP 1: CLASSIFICATION (lightweight, ~150 tokens output)
                # ════════════════════════════════════════════════════════════════════════

                classification_prompt = self._build_classification_prompt(
                    current_question,
                    previous_question
                )

                print("Current Timestamp before classification call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print("classification prompt:", classification_prompt)
                classification_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": classification_prompt}],
                    max_tokens=500,
                    temperature=0.0,  # Deterministic classification
                    top_p=0.9,
                    system_prompt=NAVIGATION_CLASSIFICATION_SYSTEM_PROMPT
                )
                print("Current Timestamp after classification call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print("Classification Response:", classification_response)

                self._log('info', "Classification response received", state,
                         llm_response=classification_response[:500],
                         retry_attempt=retry_count)

                # Check if LLM cannot answer - trigger retry
                if "Sorry, the model cannot answer this question" in classification_response:
                    retry_count += 1
                    print(f"⚠️ LLM cannot answer classification - retrying ({retry_count}/{max_retries})")
                    if retry_count < max_retries:
                        await asyncio.sleep(2 ** retry_count)
                        continue
                    else:
                        return self._build_error_response(
                            current_question, existing_domain_selection,
                            total_retry_count + retry_count,
                            "Sorry, the Optum Gateway is rejecting your request. Please try after 5 Minutes."
                        )

                # Parse classification response
                classification = self._parse_json_response(classification_response)

                if classification is None:
                    # JSON parse failed, treat as greeting
                    return {
                        'rewritten_question': '',
                        'question_type': 'what',
                        'next_agent': 'END',
                        'next_agent_disp': 'Greeting response',
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': existing_domain_selection,
                        'greeting_response': classification_response.strip(),
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'filter_values': [],
                        'user_friendly_message': classification_response.strip()
                    }

                input_type = classification.get('input_type', 'business_question')
                is_valid = classification.get('is_valid_business_question', False)
                context_decision = classification.get('context_decision', 'NEW')
                detected_prefix = classification.get('detected_prefix', 'none')
                clean_question = classification.get('clean_question', current_question)
                response_message = classification.get('response_message', '')

                # ════════════════════════════════════════════════════════════════════════
                # EARLY EXIT: Non-business questions (greeting, DML, invalid)
                # ════════════════════════════════════════════════════════════════════════

                if input_type in ['greeting', 'dml_ddl', 'invalid']:
                    return {
                        'rewritten_question': state.get('rewritten_question',''),
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
                        'user_friendly_message': response_message,
                        'detected_prefix': detected_prefix,
                        'clean_question': clean_question,
                        'context_type': context_decision
                    }

                # ════════════════════════════════════════════════════════════════════════
                # REFLECTION ROUTE: User wants to correct previous answer
                # ════════════════════════════════════════════════════════════════════════

                if context_decision == 'REFLECTION':
                    self._log('info', "REFLECTION detected - routing to reflection_agent", state,
                             detected_prefix=detected_prefix, clean_question=clean_question)
                    return {
                        'rewritten_question':  state.get('rewritten_question',''),
                        'question_type': 'what',
                        'context_type': 'REFLECTION',
                        'next_agent': 'reflection_agent',
                        'next_agent_disp': 'Reflection Agent',
                        'requires_domain_clarification': False,
                        'domain_followup_question': None,
                        'domain_selection': existing_domain_selection,
                        'is_reflection_mode': True,
                        'is_reflection_handling': False,  # Not yet in follow-up handling mode
                        'reflection_phase': 'diagnosis',  # Explicitly set initial phase
                        'llm_retry_count': total_retry_count + retry_count,
                        'pending_business_question': '',
                        'filter_values': [],
                        'user_friendly_message': '',
                        'detected_prefix': detected_prefix,
                        'clean_question': clean_question,
                        'user_correction_feedback': clean_question  # Store user's correction intent
                    }

                # ════════════════════════════════════════════════════════════════════════
                # STEP 2: REWRITE (only for valid business questions with NEW/FOLLOW_UP)
                # ════════════════════════════════════════════════════════════════════════

                if input_type == 'business_question' and is_valid:

                    rewrite_prompt = self._build_rewrite_prompt(
                        clean_question,
                        previous_question,
                        context_decision,
                        history_context,
                        current_forecast_cycle,
                        conversation_memory
                    )

                    print("Current Timestamp before rewrite call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    # print("classification prompt:", rewrite_prompt)

                    rewrite_response = await self.db_client.call_claude_api_endpoint_async(
                        messages=[{"role": "user", "content": rewrite_prompt}],
                        max_tokens=800,
                        temperature=0.1,  # Slightly creative for rewriting
                        top_p=0.7,
                        system_prompt=NAVIGATION_REWRITE_SYSTEM_PROMPT
                    )
                    print("Current Timestamp after rewrite call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    print("Rewrite Response:", rewrite_response)

                    self._log('info', "Rewrite response received", state,
                             llm_response=rewrite_response[:500],
                             retry_attempt=retry_count)

                    # Parse rewrite response
                    rewrite = self._parse_json_response(rewrite_response)

                    if rewrite is None:
                        # Fallback to original question if rewrite fails
                        rewrite = {
                            'rewritten_question': current_question,
                            'question_type': 'what',
                            'user_message': '',
                            'filter_values': []
                        }

                    rewritten_question = rewrite.get('rewritten_question', current_question)
                    question_type = rewrite.get('question_type', 'what')
                    user_message = rewrite.get('user_message', '')
                    filter_values = rewrite.get('filter_values', [])

                    # Determine next agent based on question type
                    next_agent = "router_agent" if question_type == "what" else "root_cause_agent"

                    # Return complete result
                    return {
                        'rewritten_question': rewritten_question,
                        'question_type': question_type,
                        'context_type': context_decision,
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
                        'decision_from_analysis': context_decision,
                        'detected_prefix': detected_prefix,
                        'clean_question': clean_question,
                        'reasoning': ''
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
                print(f"Two-step processing attempt {retry_count} failed: {str(e)}")

                if retry_count < max_retries:
                    print(f"Retrying... ({retry_count}/{max_retries})")
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    print(f"All retries failed: {str(e)}")
                    return self._build_error_response(
                        current_question, existing_domain_selection,
                        total_retry_count + retry_count,
                        "Model serving endpoint failed. Please try again after some time."
                    )

    def _parse_json_response(self, response: str) -> Optional[Dict]:
        """Parse JSON response, handling markdown code blocks"""
        cleaned_response = response.strip()
        if cleaned_response.startswith("```json"):
            cleaned_response = cleaned_response[7:]
        if cleaned_response.startswith("```"):
            cleaned_response = cleaned_response[3:]
        if cleaned_response.endswith("```"):
            cleaned_response = cleaned_response[:-3]
        cleaned_response = cleaned_response.strip()

        try:
            return json.loads(cleaned_response)
        except json.JSONDecodeError as e:
            print(f"JSON parse error: {e}")
            print(f"Cleaned response: {cleaned_response[:200]}")
            return None

    def _build_error_response(self, current_question: str, existing_domain_selection: List[str],
                              retry_count: int, error_message: str) -> Dict[str, any]:
        """Build standardized error response"""
        return {
            'rewritten_question': current_question,
            'question_type': 'what',
            'next_agent': 'END',
            'next_agent_disp': 'Service temporarily unavailable',
            'requires_domain_clarification': False,
            'domain_followup_question': None,
            'domain_selection': existing_domain_selection,
            'greeting_response': error_message,
            'llm_retry_count': retry_count,
            'pending_business_question': '',
            'error': True,
            'error_message': error_message,
            'filter_values': [],
            'user_friendly_message': error_message
        }

    def _build_classification_prompt(self, current_question: str, previous_question: str) -> str:
        """Build the lightweight classification prompt"""
        return NAVIGATION_CLASSIFICATION_PROMPT.format(
            current_question=current_question,
            previous_question=previous_question if previous_question else 'None'
        )

    def _build_rewrite_prompt(self, clean_question: str, previous_question: str,
                              context_decision: str, history_context: List,
                              current_forecast_cycle: str, conversation_memory: Dict) -> str:
        """Build the rewrite prompt with context inheritance rules"""

        # Get current year and month dynamically
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        current_month_name = now.strftime("%B")
        previous_year = current_year - 1

        # Calculate dynamic year comparisons
        december_comparison = "BEFORE" if 12 < current_month else "AFTER" if 12 > current_month else "SAME AS"
        december_year = previous_year if 12 > current_month else current_year
        january_comparison = "BEFORE" if 1 < current_month else "AFTER" if 1 > current_month else "SAME AS"
        january_year = previous_year if 1 > current_month else current_year
        most_recent_december_year = previous_year if current_month < 12 else current_year

        q1_year = previous_year if 3 < current_month else current_year
        q2_year = previous_year if 6 < current_month else current_year
        q3_year = previous_year if 9 < current_month else current_year
        q4_year = previous_year if 12 < current_month else current_year

        # Format the smart year rules with dynamic values
        formatted_smart_year_rules = SMART_YEAR_RULES.format(
            current_month_name=current_month_name,
            current_year=current_year,
            current_month=current_month,
            previous_year=previous_year,
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

        # Format the forecast cycle rules
        formatted_forecast_rules = FORECAST_CYCLE_RULES.format(
            current_forecast_cycle=current_forecast_cycle
        )

        return NAVIGATION_REWRITE_PROMPT.format(
            current_question=clean_question,
            previous_question=previous_question if previous_question else 'None',
            context_decision=context_decision,
            history_context=history_context,
            current_month_name=current_month_name,
            current_year=current_year,
            current_month=current_month,
            previous_year=previous_year,
            current_forecast_cycle=current_forecast_cycle,
            memory_context='',
            smart_year_rules=formatted_smart_year_rules,
            filter_inheritance_rules=FILTER_INHERITANCE_RULES,
            component_extraction_rules=COMPONENT_EXTRACTION_RULES,
            metric_inheritance_rules=METRIC_INHERITANCE_RULES,
            forecast_cycle_rules=formatted_forecast_rules,
            rewritten_question_rules=REWRITTEN_QUESTION_RULES,
            filter_extraction_rules=FILTER_EXTRACTION_RULES
        )

    # Keep the legacy single-step method for backward compatibility
    async def _single_step_processing(self, current_question: str, existing_domain_selection: List[str],
                                     total_retry_count: int, state: AgentState) -> Dict[str, any]:
        """Legacy single-step LLM processing (deprecated - use _two_step_processing)"""
        # Delegate to two-step processing
        return await self._two_step_processing(
            current_question, existing_domain_selection, total_retry_count, state
        )
    
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
