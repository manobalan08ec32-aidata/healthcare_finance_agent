"""
LLM Reflection Agent

Handles user corrections when previous SQL answers were wrong.
Diagnoses issues, collects user input, presents correction plans for approval,
and executes corrected SQL.

FLOW:
1. Diagnosis: Analyze what went wrong (single LLM call)
2. If vague feedback: Show available datasets and ask user for clarification
3. If clear feedback: Generate correction plan (DATASET_CHANGE only)
4. Execute corrected SQL (SQL generation + execution within this agent)
"""

import asyncio
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Any

from core.state_schema import AgentState
from core.databricks_client import DatabricksClient
from core.logger import setup_logger, log_with_user_context
from prompts.reflection_prompts import (
    REFLECTION_DIAGNOSIS_SYSTEM_PROMPT,
    REFLECTION_DIAGNOSIS_PROMPT,
    REFLECTION_PLAN_SYSTEM_PROMPT,
    REFLECTION_PLAN_PROMPT,
    REFLECTION_FOLLOWUP_ANALYSIS_PROMPT,
    REFLECTION_PLAN_APPROVAL_PROMPT,
    format_available_datasets
)

# Import SQL prompts for SQL generation
from prompts.router_prompts import (
    SQL_WRITER_SYSTEM_PROMPT,
    SQL_DATASET_CHANGE_PROMPT,
    SQL_HISTORY_SECTION_TEMPLATE,
    SQL_NO_HISTORY_SECTION,
    SQL_FIX_PROMPT
)

# Initialize logger for this module
logger = setup_logger(__name__)


class LLMReflectionAgent:
    """Handles user corrections to wrong SQL answers"""

    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        self.max_retries = 3

    def _log(self, level: str, message: str, state: AgentState = None, **extra):
        """Helper method to log with user context from state"""
        user_email = state.get('user_email') if state else None
        session_id = state.get('session_id') if state else None
        log_with_user_context(logger, level, f"[Reflection] {message}", user_email, session_id, **extra)

    # ════════════════════════════════════════════════════════════════════════════════
    # ENTRY POINT
    # ════════════════════════════════════════════════════════════════════════════════

    async def process_reflection(self, state: AgentState) -> Dict[str, Any]:
        """
        Main orchestrator for reflection flow.

        Flow:
        1. First call (no reflection_phase) → diagnose issue (single LLM call)
        2. If needs_followup → return follow-up question, wait for user
        3. User returns with response → process follow-up response
        4. Generate plan (for DATASET_CHANGE) or prepare for auto-execute (FILTER_FIX)
        5. If plan approved → trigger SQL execution through router_agent

        Args:
            state: Agent state with reflection context

        Returns:
            Updated state fields for reflection flow
        """
        self._log('info', "Processing reflection request", state)

        reflection_phase = state.get('reflection_phase', 'diagnosis')

        # Determine which phase we're in
        if reflection_phase == 'diagnosis':
            # First call - diagnose the issue
            return await self._diagnose_issue(state)

        elif reflection_phase == 'input_collection':
            # User responded to follow-up question
            return await self._process_followup_response(state)

        elif reflection_phase == 'plan_review':
            # User is reviewing the plan (approval/modification)
            return await self._handle_plan_review(state)

        elif reflection_phase == 'execution':
            # Plan approved, prepare for SQL execution
            return await self._prepare_for_execution(state)

        else:
            self._log('warning', f"Unknown reflection phase: {reflection_phase}", state)
            return {
                'is_reflection_mode': False,
                'is_reflection_handling': False,
                'reflection_error_msg': f"Unknown reflection phase: {reflection_phase}",
                'next_agent': 'END'
            }

    # ════════════════════════════════════════════════════════════════════════════════
    # PHASE 1: DIAGNOSIS (Single LLM Call - includes follow-up generation)
    # ════════════════════════════════════════════════════════════════════════════════

    async def _diagnose_issue(self, state: AgentState) -> Dict[str, Any]:
        """
        Analyze what went wrong AND generate follow-up question if needed.

        Single LLM call that:
        - Analyzes user's correction feedback
        - Determines issue_type and correction_path
        - If vague: generates follow-up question WITH available datasets
        - If clear: proceeds directly without follow-up

        Returns state updates for next phase.
        """
        self._log('info', "Starting diagnosis phase", state)

        # Extract previous context
        previous_context = self._extract_previous_context(state)
        user_correction_feedback = state.get('user_correction_feedback', state.get('clean_question', ''))
        domain_selection = state.get('domain_selection', '')

        # Load available datasets for this domain
        available_datasets = self._load_available_datasets(domain_selection)

        # Format available datasets for the prompt
        previous_table_used = previous_context.get('table_used', '')
        available_datasets_formatted = format_available_datasets(available_datasets, previous_table_used)
        print('available_datasets_formatted',available_datasets_formatted)
        # Build diagnosis prompt
        prompt = REFLECTION_DIAGNOSIS_PROMPT.format(
            original_question=previous_context.get('question', 'Unknown'),
            previous_table_used=previous_table_used,
            previous_sql=previous_context.get('sql', 'No SQL available'),
            user_correction_feedback=user_correction_feedback,
            domain_selection=domain_selection,
            available_datasets=available_datasets_formatted
        )

        retry_count = 0
        while retry_count < self.max_retries:
            try:
                print("Current Timestamp before diagnosis call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print("Diagnosis Response:", prompt)
                response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=1000,
                    temperature=0.1,
                    top_p=0.9,
                    system_prompt=REFLECTION_DIAGNOSIS_SYSTEM_PROMPT
                )
                print("Current Timestamp after diagnosis call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print("Diagnosis Response:", response)

                self._log('info', "Diagnosis response received", state,
                         llm_response=response[:500], retry_attempt=retry_count)

                # Parse XML response
                diagnosis = self._parse_diagnosis_xml(response)

                if diagnosis is None:
                    retry_count += 1
                    if retry_count < self.max_retries:
                        await asyncio.sleep(2 ** retry_count)
                        continue
                    else:
                        return {
                            'is_reflection_mode': False,
                            'is_reflection_handling': False,
                            'reflection_error_msg': "Failed to parse diagnosis response",
                            'next_agent': 'END'
                        }

                # Store diagnosis results
                issue_type = diagnosis.get('issue_type', 'UNCLEAR')
                correction_path = diagnosis.get('correction_path', 'NEED_CLARIFICATION')
                needs_followup = diagnosis.get('needs_followup', True)
                followup_question = diagnosis.get('followup_question', '')
                suggested_datasets = diagnosis.get('suggested_datasets', [])

                self._log('info', f"Diagnosis complete: issue_type={issue_type}, correction_path={correction_path}, needs_followup={needs_followup}", state)

                # Store diagnosis in state
                result = {
                    'is_reflection_mode': True,
                    'is_reflection_handling': False,  # Will be set to True when waiting for follow-up
                    'reflection_diagnosis': diagnosis,
                    'identified_issue_type': issue_type,
                    'correction_path': correction_path,
                    'previous_sql_for_correction': previous_context.get('sql', ''),
                    'previous_question_for_correction': previous_context.get('question', ''),
                    'previous_table_used': [previous_table_used] if previous_table_used else [],
                    'available_datasets_for_correction': available_datasets
                }

                # Determine next phase
                if needs_followup:
                    # Need to ask user for clarification - set is_reflection_handling for direct routing
                    result.update({
                        'is_reflection_handling': True,
                        'reflection_phase': 'input_collection',
                        'reflection_follow_up_question': followup_question,
                        'reflection_original_followup_question': followup_question,  # Store original for retries
                        'reflection_followup_retry_count': 0,  # Initialize retry count
                        'next_agent': 'END',  # Wait for user input
                        'user_friendly_message': followup_question
                    })
                else:
                    # Issue is clear - ALWAYS go to plan_review for all correction types (mandatory plan)
                    if correction_path == 'DATASET_CHANGE':
                        if suggested_datasets:
                            result.update({
                                'reflection_phase': 'plan_review',
                                'user_dataset_preference': ','.join(suggested_datasets),
                                'next_agent': 'reflection_agent',  # Continue to plan generation
                                'user_friendly_message': f"I'll prepare a correction plan for switching to: {', '.join(suggested_datasets)}"
                            })
                        else:
                            # Need to ask for dataset selection
                            result.update({
                                'is_reflection_handling': True,
                                'reflection_phase': 'input_collection',
                                'reflection_follow_up_question': self._generate_dataset_selection_question(
                                    available_datasets, previous_table_used, previous_context.get('question', '')
                                ),
                                'next_agent': 'END'
                            })
                    elif correction_path in ['FILTER_FIX', 'STRUCTURE_FIX']:
                        # Now also goes to plan_review instead of direct execution (mandatory plan)
                        result.update({
                            'reflection_phase': 'plan_review',
                            'user_correction_intent': diagnosis.get('suggested_fix', ''),
                            'correction_details': diagnosis.get('suggested_fix', ''),
                            'next_agent': 'reflection_agent',  # Continue to plan generation
                            'user_friendly_message': f"I'll prepare a correction plan for: {diagnosis.get('suggested_fix', 'Applying fix')}"
                        })
                    else:
                        # NEED_CLARIFICATION or unknown - ask for more details
                        result.update({
                            'is_reflection_handling': True,
                            'reflection_phase': 'input_collection',
                            'reflection_follow_up_question': self._generate_dataset_selection_question(
                                available_datasets, previous_table_used, previous_context.get('question', '')
                            ),
                            'next_agent': 'END'
                        })

                return result

            except Exception as e:
                retry_count += 1
                print(f"Diagnosis attempt {retry_count} failed: {str(e)}")
                if retry_count < self.max_retries:
                    await asyncio.sleep(2 ** retry_count)
                    continue
                else:
                    return {
                        'is_reflection_mode': False,
                        'is_reflection_handling': False,
                        'reflection_error_msg': f"Diagnosis failed after {self.max_retries} attempts: {str(e)}",
                        'next_agent': 'END'
                    }

    def _parse_diagnosis_xml(self, response: str) -> Optional[Dict[str, Any]]:
        """Parse XML diagnosis response"""
        try:
            # Extract values using regex
            def extract_tag(tag: str, text: str) -> str:
                pattern = f"<{tag}>(.*?)</{tag}>"
                match = re.search(pattern, text, re.DOTALL)
                return match.group(1).strip() if match else ""

            issue_type = extract_tag('issue_type', response)
            issue_details = extract_tag('issue_details', response)
            correction_path = extract_tag('correction_path', response)
            suggested_fix = extract_tag('suggested_fix', response)
            suggested_datasets_str = extract_tag('suggested_datasets', response)
            needs_followup_str = extract_tag('needs_followup', response)
            followup_question = extract_tag('followup_question', response)

            # Parse suggested_datasets as list
            suggested_datasets = []
            if suggested_datasets_str:
                suggested_datasets = [d.strip() for d in suggested_datasets_str.split(',') if d.strip()]

            # Parse needs_followup as boolean
            needs_followup = needs_followup_str.lower() == 'true'

            return {
                'issue_type': issue_type or 'UNCLEAR',
                'issue_details': issue_details,
                'correction_path': correction_path or 'NEED_CLARIFICATION',
                'suggested_fix': suggested_fix,
                'suggested_datasets': suggested_datasets,
                'needs_followup': needs_followup,
                'followup_question': followup_question
            }

        except Exception as e:
            print(f"XML parse error: {e}")
            return None

    # ════════════════════════════════════════════════════════════════════════════════
    # PHASE 1b: PROCESS USER'S FOLLOW-UP RESPONSE (LLM-BASED)
    # ════════════════════════════════════════════════════════════════════════════════

    async def _process_followup_response(self, state: AgentState) -> Dict[str, Any]:
        """
        Process user's response to follow-up question using LLM.

        Uses LLM to analyze user's response instead of hardcoded parsing.
        Handles topic drift with one retry before ending the flow.

        Flow:
        1. Send follow-up question + user response to LLM
        2. LLM determines: is_relevant, action, selected_datasets, correction_details
        3. If topic drift: re-ask once, then end
        4. If valid: route to plan_review for ALL correction types
        """
        self._log('info', "Processing follow-up response with LLM", state)

        user_response = state.get('current_question', '')
        followup_question = state.get('reflection_follow_up_question', '')
        available_datasets = state.get('available_datasets_for_correction', [])
        previous_context = self._extract_previous_context(state)

        # Format available datasets for the prompt
        previous_table_used = previous_context.get('table_used', '')
        available_datasets_formatted = format_available_datasets(available_datasets, previous_table_used)

        # Build LLM prompt
        prompt = REFLECTION_FOLLOWUP_ANALYSIS_PROMPT.format(
            original_question=previous_context.get('question', 'Unknown'),
            previous_table_used=previous_table_used,
            previous_sql=previous_context.get('sql', 'No SQL available'),
            followup_question=followup_question,
            user_response=user_response,
            available_datasets_formatted=available_datasets_formatted
        )

        try:
            print("Current Timestamp before followup analysis call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print("Followup Analysis prompt:", prompt)

            response = await self.db_client.call_claude_api_endpoint_async(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=1000,
                temperature=0.1,
                top_p=0.9,
                system_prompt=REFLECTION_DIAGNOSIS_SYSTEM_PROMPT
            )
            print("Current Timestamp after followup analysis call:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print("Followup Analysis Response:", response)

            self._log('info', "Followup analysis response received", state, llm_response=response[:500])

            # Parse LLM response
            analysis = self._parse_followup_analysis_xml(response)

            if analysis is None:
                return {
                    'is_reflection_mode': True,
                    'is_reflection_handling': True,
                    'reflection_error_msg': "Failed to parse follow-up analysis",
                    'next_agent': 'END'
                }

            # Handle topic drift
            if not analysis.get('is_relevant', True) or analysis.get('action') == 'TOPIC_DRIFT':
                retry_count = state.get('reflection_followup_retry_count', 0)
                original_followup = state.get('reflection_original_followup_question', followup_question)

                if retry_count < 1:
                    # First drift - re-ask with note using ORIGINAL question
                    self._log('info', f"Topic drift detected (attempt {retry_count + 1}), re-asking", state)
                    return {
                        'is_reflection_mode': True,
                        'is_reflection_handling': True,
                        'reflection_phase': 'input_collection',
                        'reflection_followup_retry_count': retry_count + 1,
                        'reflection_original_followup_question': original_followup,  # Preserve original
                        'reflection_follow_up_question': f"I didn't quite understand your response. Let me ask again:\n\n{original_followup}",
                        'next_agent': 'END',
                        'reflection_followup_flg':True,
                        'user_friendly_message': f"I didn't quite understand your response. Let me ask again:\n\n{original_followup}"
                    }
                else:
                    # Second drift - end flow (retry_count >= 1)
                    self._log('info', "Topic drift detected twice, ending reflection flow", state)
                    return {
                        'is_reflection_mode': False,
                        'is_reflection_handling': False,
                        'reflection_phase': None,
                        'reflection_followup_retry_count': 0,  # Reset counter
                        'next_agent': 'END',
                        'reflection_followup_retry_exceeded': True,
                        'user_friendly_message': "I'm having trouble understanding your correction request. Please start a new question when you're ready."
                    }

            # Extract results from LLM analysis
            action = analysis.get('action', 'NEED_MORE_INFO')
            issue_type = analysis.get('issue_type', 'UNCLEAR')
            selected_datasets = analysis.get('selected_datasets', [])
            correction_details = analysis.get('correction_details', '')
            user_intent = analysis.get('user_intent', user_response)
            plan_summary = analysis.get('plan_summary', '')

            self._log('info', f"LLM parsed response: action={action}, issue_type={issue_type}, datasets={selected_datasets}", state)

            # Build result - ALWAYS go to plan_review for valid corrections (mandatory plan)
            result = {
                'is_reflection_mode': True,
                'is_reflection_handling': True,
                'identified_issue_type': issue_type,
                'user_correction_intent': user_intent,
                'reflection_followup_retry_count': 0  # Reset on valid response
            }

            if action == 'DATASET_CHANGE' and selected_datasets:
                result.update({
                    'reflection_phase': 'plan_review',
                    'correction_path': 'DATASET_CHANGE',
                    'user_dataset_preference': ','.join(selected_datasets),
                    'plan_summary': plan_summary,
                    'next_agent': 'reflection_agent'
                })
            elif action in ['FILTER_FIX', 'STRUCTURE_FIX']:
                # Now also goes to plan_review (mandatory plan for all corrections)
                result.update({
                    'reflection_phase': 'plan_review',
                    'correction_path': action,
                    'correction_details': correction_details,
                    'plan_summary': plan_summary,
                    'next_agent': 'reflection_agent'
                })
            elif action == 'NEED_MORE_INFO':
                # Still unclear - ask for more clarification
                result.update({
                    'reflection_phase': 'input_collection',
                    'reflection_follow_up_question': "Could you please provide more details about what needs to be corrected?",
                    'next_agent': 'END',
                    'user_friendly_message': "Could you please provide more details about what needs to be corrected?"
                })
            else:
                # Fallback to diagnosis
                result.update({
                    'reflection_phase': 'diagnosis',
                    'user_correction_feedback': user_response,
                    'next_agent': 'reflection_agent',
                    'user_friendly_message': "Let me analyze your feedback again."
                })

            return result

        except Exception as e:
            self._log('error', f"Follow-up analysis failed: {str(e)}", state)
            return {
                'is_reflection_mode': True,
                'is_reflection_handling': False,
                'reflection_error_msg': f"Follow-up analysis failed: {str(e)}",
                'next_agent': 'END'
            }

    def _parse_followup_analysis_xml(self, response: str) -> Optional[Dict[str, Any]]:
        """Parse XML response from follow-up analysis LLM call"""
        try:
            def extract_tag(tag: str, text: str) -> str:
                pattern = f"<{tag}>(.*?)</{tag}>"
                match = re.search(pattern, text, re.DOTALL)
                return match.group(1).strip() if match else ""

            is_relevant_str = extract_tag('is_relevant', response)
            is_relevant = is_relevant_str.lower() == 'true'

            topic_drift_reason = extract_tag('topic_drift_reason', response)
            action = extract_tag('action', response)
            issue_type = extract_tag('issue_type', response)
            selected_datasets_str = extract_tag('selected_datasets', response)
            correction_details = extract_tag('correction_details', response)
            user_intent = extract_tag('user_intent', response)
            plan_summary = extract_tag('plan_summary', response)

            # Parse selected_datasets as list
            selected_datasets = []
            if selected_datasets_str:
                selected_datasets = [d.strip() for d in selected_datasets_str.split(',') if d.strip()]

            return {
                'is_relevant': is_relevant,
                'topic_drift_reason': topic_drift_reason,
                'action': action or 'NEED_MORE_INFO',
                'issue_type': issue_type or 'UNCLEAR',
                'selected_datasets': selected_datasets,
                'correction_details': correction_details,
                'user_intent': user_intent,
                'plan_summary': plan_summary
            }

        except Exception as e:
            print(f"Follow-up analysis XML parse error: {e}")
            return None

    # ════════════════════════════════════════════════════════════════════════════════
    # PHASE 2: PLAN GENERATION (Now for ALL correction types - mandatory)
    # ════════════════════════════════════════════════════════════════════════════════

    async def _handle_plan_review(self, state: AgentState) -> Dict[str, Any]:
        """
        Generate or handle correction plan review using LLM.

        Now called for ALL correction types (DATASET_CHANGE, FILTER_FIX, STRUCTURE_FIX).
        Plan generation is mandatory to prevent repeated mistakes.
        Uses LLM to parse user's response (approve/reject/modify) instead of hardcoded matching.
        """
        self._log('info', "Handling plan review phase", state)

        # Check if we already have a plan and user is responding
        existing_plan = state.get('reflection_plan')
        user_response = state.get('current_question', '')

        if existing_plan and user_response:
            # User is responding to the plan - use LLM to parse response
            return await self._process_plan_approval_response(state, existing_plan, user_response)

        # Generate new plan
        return await self._generate_correction_plan(state)

    async def _process_plan_approval_response(self, state: AgentState, existing_plan: str, user_response: str) -> Dict[str, Any]:
        """
        Use LLM to analyze user's response to the correction plan.

        Determines: APPROVE, REJECT, MODIFY, or TOPIC_DRIFT
        Handles topic drift with retry before closing loop.
        """
        self._log('info', "Processing plan approval response with LLM", state)

        original_question = state.get('previous_question_for_correction', '')
        issue_type = state.get('identified_issue_type', 'UNKNOWN')
        correction_path = state.get('correction_path', 'UNKNOWN')
        plan_summary = state.get('reflection_follow_up_question', existing_plan[:500])

        # Build LLM prompt
        prompt = REFLECTION_PLAN_APPROVAL_PROMPT.format(
            original_question=original_question,
            issue_type=issue_type,
            correction_path=correction_path,
            plan_summary=plan_summary,
            user_response=user_response
        )

        try:
            print("Current Timestamp before plan approval analysis:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print("Plan Approval Analysis prompt:", prompt)
            response = await self.db_client.call_claude_api_endpoint_async(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,
                temperature=0.1,
                top_p=0.9,
                system_prompt=REFLECTION_DIAGNOSIS_SYSTEM_PROMPT
            )
            print("Current Timestamp after plan approval analysis:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print("Plan Approval Analysis Response:", response)

            self._log('info', "Plan approval analysis response received", state, llm_response=response[:500])

            # Parse LLM response
            analysis = self._parse_plan_approval_xml(response)

            if analysis is None:
                return {
                    'is_reflection_mode': True,
                    'is_reflection_handling': True,
                    'reflection_error_msg': "Failed to parse plan approval response",
                    'next_agent': 'END'
                }

            decision = analysis.get('decision', 'TOPIC_DRIFT')
            is_relevant = analysis.get('is_relevant', True)
            modification_details = analysis.get('modification_details', '')
            additional_context = analysis.get('additional_context', '')

            # Handle topic drift
            if not is_relevant or decision == 'TOPIC_DRIFT':
                retry_count = state.get('reflection_plan_approval_retry_count', 0)

                if retry_count < 1:
                    # First drift - re-ask for approval
                    plan_display = state.get('reflection_follow_up_question', '')
                    self._log('info', f"Plan approval topic drift (attempt {retry_count + 1}), re-asking", state)
                    return {
                        'is_reflection_mode': True,
                        'is_reflection_handling': True,
                        'reflection_phase': 'plan_review',
                        'reflection_plan_approval_retry_count': retry_count + 1,
                        'reflection_follow_up_question': f"I need your response to the correction plan. Please reply with 'approve' to proceed, 'cancel' to stop, or describe any modifications you'd like:\n\n{plan_display}",
                        'next_agent': 'END',
                        'user_friendly_message': f"Please respond to the correction plan above. Reply 'approve' to proceed or describe any changes you'd like."
                    }
                else:
                    # Second drift - close the loop
                    self._log('info', "Plan approval topic drift twice, closing reflection loop", state)
                    return {
                        'is_reflection_mode': False,
                        'is_reflection_handling': False,
                        'reflection_phase': None,
                        'next_agent': 'END',
                        'user_friendly_message': "I'm having trouble understanding your response to the plan. Please start a new question when you're ready."
                    }

            # Handle based on decision
            if decision == 'APPROVE':
                self._log('info', "Plan approved by user", state)
                # Include any additional context from user for SQL generation
                result = {
                    'is_reflection_mode': True,
                    'is_reflection_handling': False,
                    'reflection_plan_approved': True,
                    'reflection_phase': 'execution',
                    'next_agent': 'reflection_agent',  # Continue in reflection agent for execution
                    'user_friendly_message': "Plan approved. Generating corrected SQL..."
                }
                if additional_context:
                    # Pass additional context to SQL generation
                    current_intent = state.get('user_correction_intent', '')
                    result['user_correction_intent'] = f"{current_intent}. Additional: {additional_context}"
                return result

            elif decision == 'REJECT':
                self._log('info', "Plan rejected by user", state)
                return {
                    'is_reflection_mode': False,
                    'is_reflection_handling': False,
                    'reflection_plan_approved': False,
                    'reflection_phase': None,
                    'next_agent': 'END',
                    'user_friendly_message': "Correction cancelled. Feel free to ask a new question."
                }

            elif decision == 'MODIFY':
                self._log('info', f"Plan modification requested: {modification_details}", state)
                # User wants modifications - update the correction intent and regenerate plan
                return {
                    'is_reflection_mode': True,
                    'is_reflection_handling': True,
                    'reflection_phase': 'plan_review',
                    'reflection_plan': None,  # Clear old plan to generate new one
                    'user_correction_intent': modification_details if modification_details else user_response,
                    'correction_details': modification_details if modification_details else user_response,
                    'next_agent': 'reflection_agent',
                    'user_friendly_message': f"I'll update the plan with your modifications: {modification_details or user_response}"
                }

            else:
                # Unknown decision - treat as need more info
                return {
                    'is_reflection_mode': True,
                    'is_reflection_handling': True,
                    'reflection_phase': 'plan_review',
                    'reflection_follow_up_question': "I couldn't understand your response. Please reply 'approve' to proceed with the plan, 'cancel' to stop, or describe what changes you'd like.",
                    'next_agent': 'END',
                    'user_friendly_message': "Please reply 'approve' to proceed, 'cancel' to stop, or describe any modifications."
                }

        except Exception as e:
            self._log('error', f"Plan approval analysis failed: {str(e)}", state)
            return {
                'is_reflection_mode': True,
                'is_reflection_handling': False,
                'reflection_error_msg': f"Plan approval analysis failed: {str(e)}",
                'next_agent': 'END'
            }

    def _parse_plan_approval_xml(self, response: str) -> Optional[Dict[str, Any]]:
        """Parse XML response from plan approval analysis LLM call"""
        try:
            def extract_tag(tag: str, text: str) -> str:
                pattern = f"<{tag}>(.*?)</{tag}>"
                match = re.search(pattern, text, re.DOTALL)
                return match.group(1).strip() if match else ""

            is_relevant_str = extract_tag('is_relevant', response)
            is_relevant = is_relevant_str.lower() == 'true'

            topic_drift_reason = extract_tag('topic_drift_reason', response)
            decision = extract_tag('decision', response)
            modification_details = extract_tag('modification_details', response)
            additional_context = extract_tag('additional_context', response)
            user_intent = extract_tag('user_intent', response)

            return {
                'is_relevant': is_relevant,
                'topic_drift_reason': topic_drift_reason,
                'decision': decision or 'TOPIC_DRIFT',
                'modification_details': modification_details,
                'additional_context': additional_context,
                'user_intent': user_intent
            }

        except Exception as e:
            print(f"Plan approval XML parse error: {e}")
            return None

    async def _generate_correction_plan(self, state: AgentState) -> Dict[str, Any]:
        """Create plan for user approval"""

        original_question = state.get('previous_question_for_correction', '')
        previous_table_used = state.get('previous_table_used', ['Unknown'])[0] if state.get('previous_table_used') else 'Unknown'
        issue_type = state.get('identified_issue_type', 'WRONG_DATASET')
        correction_path = state.get('correction_path', 'DATASET_CHANGE')
        selected_datasets = state.get('user_dataset_preference', '').split(',')
        selected_datasets = [d.strip() for d in selected_datasets if d.strip()]

        # Load metadata for selected datasets (simplified - in real implementation, load from metadata.json)
        dataset_metadata = f"Selected datasets: {', '.join(selected_datasets)}"

        # Build mandatory columns information
        mandatory_column_mapping = {
            "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast": ["Ledger"],
            "prd_optumrx_orxfdmprdsa.rag.pbm_claims": ["product_category='PBM'"]
        }

        mandatory_columns_info = []
        for dataset in selected_datasets:
            if dataset in mandatory_column_mapping:
                for col in mandatory_column_mapping[dataset]:
                    mandatory_columns_info.append(f"Table {dataset}: {col} (MANDATORY)")

        mandatory_columns_text = "\n".join(mandatory_columns_info) if mandatory_columns_info else "Not Applicable"

        prompt = REFLECTION_PLAN_PROMPT.format(
            original_question=original_question,
            previous_table_used=previous_table_used,
            issue_type=issue_type,
            correction_path=correction_path,
            selected_datasets=', '.join(selected_datasets),
            dataset_metadata=dataset_metadata,
            mandatory_columns=mandatory_columns_text
        )

        try:
            print("FPlan generated prompt:", prompt)
            response = await self.db_client.call_claude_api_endpoint_async(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800,
                temperature=0.1,
                top_p=0.9,
                system_prompt=REFLECTION_PLAN_SYSTEM_PROMPT
            )
            print("FPlan generated response:", response)

            self._log('info', "Plan generated", state, llm_response=response[:1000])

            # Parse and format the plan for display
            plan_display = self._format_plan_for_display(response, selected_datasets)

            return {
                'is_reflection_mode': True,
                'is_reflection_handling': True,  # Wait for user approval
                'reflection_phase': 'plan_review',
                'reflection_plan': response,
                'reflection_follow_up_question': plan_display,
                'reflection_plan_approval_retry_count': 0,  # Reset retry count for new plan
                'next_agent': 'END',  # Wait for user approval
                'user_friendly_message': plan_display
            }

        except Exception as e:
            self._log('error', f"Plan generation failed: {str(e)}", state)
            return {
                'is_reflection_mode': False,
                'is_reflection_handling': False,
                'reflection_error_msg': f"Failed to generate correction plan: {str(e)}",
                'next_agent': 'END'
            }

    def _format_plan_for_display(self, plan_response: str, selected_datasets: List[str]) -> str:
        """Extract and format plan from LLM response for user display"""

        # Extract the plan_approval section from the response
        pattern = r"<plan_approval>(.*?)</plan_approval>"
        match = re.search(pattern, plan_response, re.DOTALL)

        if match:
            # Return the content inside plan_approval tags (already formatted with emojis)
            return match.group(1).strip()

        # Fallback if no plan_approval tags found - return the whole response
        # or generate a basic plan
        if "📋" in plan_response:
            return plan_response.strip()

        # Last resort fallback
        return f"""📋 Correction Plan Summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 What We're Fixing: Correcting the previous query

📊 Dataset: {', '.join(selected_datasets)}

📌 What Will Change:
• Switch to the correct dataset

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Options: ✅ Approve | ✏️ Modify | ❌ Cancel"""

    # ════════════════════════════════════════════════════════════════════════════════
    # PHASE 3: SQL EXECUTION
    # ════════════════════════════════════════════════════════════════════════════════

    async def _prepare_for_execution(self, state: AgentState) -> Dict[str, Any]:
        """
        Execute corrected SQL within the reflection agent.

        For DATASET_CHANGE:
        - Map functional names to table names
        - Load metadata for new dataset(s)
        - Generate SQL with new dataset
        - Execute SQL

        For FILTER_FIX/STRUCTURE_FIX:
        - Append correction hint to question
        - Regenerate SQL with correction context
        - Execute SQL
        """
        self._log('info', "Executing corrected SQL", state)

        correction_path = state.get('correction_path', 'DATASET_CHANGE')
        selected_datasets_str = state.get('user_dataset_preference', '')
        selected_datasets = [d.strip() for d in selected_datasets_str.split(',') if d.strip()]

        if correction_path == 'DATASET_CHANGE' and selected_datasets:
            # Handle dataset change - generate and execute SQL with new dataset
            return await self._execute_dataset_change(state, selected_datasets)

        elif correction_path in ['FILTER_FIX', 'STRUCTURE_FIX']:
            # Handle filter/structure fix - append correction to question and regenerate
            return await self._execute_filter_or_structure_fix(state)

        else:
            # Fallback - route to router_agent for normal processing
            return {
                'is_reflection_mode': True,
                'is_reflection_handling': True,
                'reflection_phase': 'execution',
                'next_agent': 'router_agent',
                'user_friendly_message': "Processing correction..."
            }

    async def _execute_dataset_change(self, state: AgentState, selected_datasets: List[str]) -> Dict[str, Any]:
        """
        Execute SQL with changed dataset.

        Steps:
        1. Map functional names to table names
        2. Load metadata for new dataset(s)
        3. Generate SQL with new dataset
        4. Execute SQL
        5. Return results
        """
        self._log('info', f"Executing dataset change: {selected_datasets}", state)

        domain_selection = state.get('domain_selection', '')
        available_datasets = state.get('available_datasets_for_correction', [])

        if not available_datasets:
            available_datasets = self._load_available_datasets(domain_selection)

        # Step 1: Map functional names to table names
        matched_tables = []
        matched_functional_names = []

        for req_name in selected_datasets:
            for dataset in available_datasets:
                func_name = dataset.get('functional_name', '')
                if func_name.strip().lower() == req_name.strip().lower():
                    matched_tables.append(dataset.get('table_name', ''))
                    matched_functional_names.append(func_name)
                    break

        if not matched_tables:
            self._log('warning', f"No matching tables found for: {selected_datasets}", state)
            return {
                'is_reflection_mode': False,
                'is_reflection_handling': False,
                'reflection_error_msg': f"Requested dataset(s) not found: {selected_datasets}",
                'next_agent': 'END'
            }

        print(f"�� Reflection dataset change: {matched_functional_names} -> {matched_tables}")

        # Step 2: Load metadata for new dataset(s)
        try:
            metadata = await self._load_metadata_for_tables(matched_tables, state)
            if not metadata:
                return {
                    'is_reflection_mode': False,
                    'is_reflection_handling': False,
                    'reflection_error_msg': "Failed to load metadata for new dataset",
                    'next_agent': 'END'
                }
            state['dataset_metadata'] = metadata
            state['selected_dataset'] = matched_tables
            state['functional_names'] = matched_functional_names
        except Exception as e:
            self._log('error', f"Metadata loading failed: {e}", state)
            return {
                'is_reflection_mode': False,
                'is_reflection_handling': False,
                'reflection_error_msg': f"Failed to load dataset metadata: {str(e)}",
                'next_agent': 'END'
            }

        # Step 3: Generate SQL with new dataset
        try:
            sql_result = await self._generate_sql_for_correction(state)
            if not sql_result.get('success', False):
                return {
                    'is_reflection_mode': False,
                    'is_reflection_handling': False,
                    'reflection_error_msg': sql_result.get('error', 'SQL generation failed'),
                    'next_agent': 'END'
                }
        except Exception as e:
            self._log('error', f"SQL generation failed: {e}", state)
            return {
                'is_reflection_mode': False,
                'is_reflection_handling': False,
                'reflection_error_msg': f"SQL generation failed: {str(e)}",
                'next_agent': 'END'
            }

        # Step 4: Execute SQL
        try:
            if sql_result.get('multiple_sql', False):
                execution_result = await self._execute_multiple_sql(sql_result, state)
            else:
                execution_result = await self._execute_single_sql(sql_result, state)

            if not execution_result.get('success', False):
                return {
                    'is_reflection_mode': False,
                    'is_reflection_handling': False,
                    'reflection_error_msg': execution_result.get('error', 'SQL execution failed'),
                    'next_agent': 'END'
                }
        except Exception as e:
            self._log('error', f"SQL execution failed: {e}", state)
            return {
                'is_reflection_mode': False,
                'is_reflection_handling': False,
                'reflection_error_msg': f"SQL execution failed: {str(e)}",
                'next_agent': 'END'
            }

        # Step 5: Return results
        return {
            'sql_result': execution_result,
            'selected_dataset': matched_tables,
            'functional_names': matched_functional_names,
            'is_reflection_mode': False,
            'is_reflection_handling': False,
            'reflection_phase': None,
            'next_agent': 'END',
            'user_friendly_message': f"Corrected query executed with {', '.join(matched_functional_names)}"
        }

    async def _execute_filter_or_structure_fix(self, state: AgentState) -> Dict[str, Any]:
        """
        Execute SQL with filter or structure correction.

        Appends correction hint to the question and regenerates SQL.
        """
        self._log('info', "Executing filter/structure fix", state)

        user_intent = state.get('user_correction_intent', '')
        original_question = state.get('previous_question_for_correction', state.get('rewritten_question', ''))

        # Append correction hint to question
        corrected_question = f"{original_question} - CORRECTION: {user_intent}"
        state['rewritten_question'] = corrected_question
        state['current_question'] = corrected_question

        # Generate SQL with correction context
        try:
            sql_result = await self._generate_sql_for_correction(state)
            if not sql_result.get('success', False):
                return {
                    'is_reflection_mode': False,
                    'is_reflection_handling': False,
                    'reflection_error_msg': sql_result.get('error', 'SQL generation failed'),
                    'next_agent': 'END'
                }
        except Exception as e:
            self._log('error', f"SQL generation failed: {e}", state)
            return {
                'is_reflection_mode': False,
                'is_reflection_handling': False,
                'reflection_error_msg': f"SQL generation failed: {str(e)}",
                'next_agent': 'END'
            }

        # Execute SQL
        try:
            if sql_result.get('multiple_sql', False):
                execution_result = await self._execute_multiple_sql(sql_result, state)
            else:
                execution_result = await self._execute_single_sql(sql_result, state)

            if not execution_result.get('success', False):
                return {
                    'is_reflection_mode': False,
                    'is_reflection_handling': False,
                    'reflection_error_msg': execution_result.get('error', 'SQL execution failed'),
                    'next_agent': 'END'
                }
        except Exception as e:
            self._log('error', f"SQL execution failed: {e}", state)
            return {
                'is_reflection_mode': False,
                'is_reflection_handling': False,
                'reflection_error_msg': f"SQL execution failed: {str(e)}",
                'next_agent': 'END'
            }

        return {
            'sql_result': execution_result,
            'is_reflection_mode': False,
            'is_reflection_handling': False,
            'reflection_phase': None,
            'next_agent': 'END',
            'user_friendly_message': f"Corrected query executed with: {user_intent}"
        }

    async def _load_metadata_for_tables(self, table_names: List[str], state: AgentState) -> str:
        """Load metadata for the specified tables using vector search"""
        try:
            metadata_parts = []
            for table_name in table_names:
                # Call vector search for each table
                result = await self.db_client.vector_search_embeddings_async(
                    query=state.get('rewritten_question', ''),
                    table_name=table_name,
                    num_results=15
                )
                if result:
                    metadata_parts.append(f"--- {table_name} ---\n{result}")

            return "\n\n".join(metadata_parts) if metadata_parts else ""
        except Exception as e:
            print(f"Error loading metadata: {e}")
            return ""

    async def _generate_sql_for_correction(self, state: AgentState) -> Dict[str, Any]:
        """Generate SQL for the correction using the dataset change prompt"""

        current_question = state.get('rewritten_question', state.get('current_question', ''))
        dataset_metadata = state.get('dataset_metadata', '')
        filter_metadata_results = state.get('filter_metadata_results', [])
        selected_datasets = state.get('selected_dataset', [])

        # Build mandatory columns
        mandatory_column_mapping = {
            "prd_optumrx_orxfdmprdsa.rag.ledger_actual_vs_forecast": ["Ledger"],
            "prd_optumrx_orxfdmprdsa.rag.pbm_claims": ["product_category='PBM'"]
        }

        mandatory_columns_info = []
        if isinstance(selected_datasets, list):
            for dataset in selected_datasets:
                if dataset in mandatory_column_mapping:
                    for col in mandatory_column_mapping[dataset]:
                        mandatory_columns_info.append(f"Table {dataset}: {col} (MANDATORY)")

        mandatory_columns_text = "\n".join(mandatory_columns_info) if mandatory_columns_info else "Not Applicable"

        # Build history section
        history_question_match = state.get('history_question_match', '')
        matched_sql = state.get('matched_sql', '')
        has_history = bool(matched_sql and history_question_match)

        if has_history:
            history_section = SQL_HISTORY_SECTION_TEMPLATE.format(
                history_question_match=history_question_match,
                matched_sql=matched_sql
            )
        else:
            history_section = SQL_NO_HISTORY_SECTION

        # Build prompt
        sql_generation_prompt = SQL_DATASET_CHANGE_PROMPT.format(
            current_question=current_question,
            dataset_metadata=dataset_metadata,
            mandatory_columns_text=mandatory_columns_text,
            filter_metadata_results=filter_metadata_results if filter_metadata_results else "",
            history_section=history_section,
            sql_followup_question=state.get('sql_followup_question', ''),
            sql_followup_answer=state.get('current_question', '')
        )

        # Generate SQL with retry
        for attempt in range(self.max_retries):
            try:
                print(f"🔄 Reflection SQL generation attempt {attempt + 1}")
                print(f"Reflection SQL prompt: {sql_generation_prompt}...")
                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": sql_generation_prompt}],
                    max_tokens=3000,
                    temperature=0.0,
                    top_p=0.1,
                    system_prompt=SQL_WRITER_SYSTEM_PROMPT
                )

                print(f"Reflection SQL response: {llm_response[:1000]}...")

                # Extract sql_story
                sql_story = ""
                story_match = re.search(r'<sql_story>(.*?)</sql_story>', llm_response, re.DOTALL)
                if story_match:
                    sql_story = story_match.group(1).strip()

                # Check for multiple SQL queries
                multiple_sql_match = re.search(r'<multiple_sql>(.*?)</multiple_sql>', llm_response, re.DOTALL)
                if multiple_sql_match:
                    multiple_content = multiple_sql_match.group(1).strip()
                    query_matches = re.findall(
                        r'<query(\d+)_title>(.*?)</query\1_title>.*?<query\1>(.*?)</query\1>',
                        multiple_content, re.DOTALL
                    )
                    if query_matches:
                        sql_queries = []
                        query_titles = []
                        for query_num, title, query in query_matches:
                            cleaned_query = query.strip().replace('`', '')
                            cleaned_title = title.strip()
                            if cleaned_query and cleaned_title:
                                sql_queries.append(cleaned_query)
                                query_titles.append(cleaned_title)

                        if sql_queries:
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,
                                'query_count': len(sql_queries),
                                'sql_story': sql_story
                            }

                # Check for single SQL query
                match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if match:
                    sql_query = match.group(1).strip().replace('`', '')
                    if sql_query:
                        return {
                            'success': True,
                            'multiple_sql': False,
                            'sql_query': sql_query,
                            'sql_story': sql_story
                        }

                raise ValueError("No valid SQL in response")

            except Exception as e:
                print(f"SQL generation attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue

        return {'success': False, 'error': 'SQL generation failed after retries'}

    async def _execute_single_sql(self, sql_result: Dict, state: AgentState) -> Dict[str, Any]:
        """Execute a single SQL query with intelligent retry and LLM-based fixing"""
        sql_query = sql_result.get('sql_query', '')

        # Build context for SQL fixing
        context = {
            'current_question': state.get('rewritten_question', state.get('current_question', '')),
            'dataset_metadata': state.get('dataset_metadata', '')
        }

        # Use the new retry method with LLM-based fixing
        execution_result = await self._execute_sql_with_retry_async(sql_query, context, state)

        if execution_result.get('success', False):
            return {
                'success': True,
                'multiple_results': False,
                'sql_query': execution_result.get('final_sql', sql_query),
                'query_results': execution_result.get('data', []),
                'columns': execution_result.get('columns', []),
                'narrative': '',
                'summary': '',
                'row_count': len(execution_result.get('data', [])),
                'sql_story': sql_result.get('sql_story', ''),
                'execution_attempts': execution_result.get('attempts', 1)
            }
        else:
            return {
                'success': False,
                'error': execution_result.get('error', 'SQL execution failed after retries'),
                'failed_sql': execution_result.get('final_sql', sql_query),
                'execution_attempts': execution_result.get('attempts', self.max_retries),
                'errors_history': execution_result.get('errors_history', [])
            }

    async def _execute_sql_with_retry_async(self, initial_sql: str, context: Dict, state: AgentState, max_retries: int = 3) -> Dict:
        """Execute SQL with intelligent retry logic and LLM-based fixing"""

        current_sql = initial_sql
        errors_history = []

        self._log('info', f"Starting SQL execution with retry", state)

        for attempt in range(max_retries):
            try:
                print(f"🔄 Reflection: Executing SQL (attempt {attempt + 1}): {current_sql[:100]}...")
                result = await self.db_client.execute_sql_async(current_sql, timeout=300)

                # execute_sql_async returns List[Dict] directly on success
                if isinstance(result, list):
                    print(f"    ✅ SQL executed successfully on attempt {attempt + 1}")
                    return {
                        'success': True,
                        'data': result,
                        'final_sql': current_sql,
                        'attempts': attempt + 1
                    }
                else:
                    # Unexpected response format
                    error_msg = f"Unexpected response format: {type(result)}"
                    errors_history.append(f"Attempt {attempt + 1}: {error_msg}")

                    if attempt < max_retries - 1:
                        fix_result = await self._fix_sql_with_llm_async(current_sql, error_msg, errors_history, context, state)
                        if fix_result['success']:
                            current_sql = fix_result['fixed_sql']
                        else:
                            break

            except Exception as e:
                error_msg = str(e)
                errors_history.append(f"Attempt {attempt + 1}: {error_msg}")
                self._log('warning', f"SQL execution attempt {attempt + 1} failed: {error_msg}", state)

                if attempt < max_retries - 1:
                    # Check for connection errors - add delay before retry
                    is_connection_error = any(err_pattern in error_msg.lower() for err_pattern in [
                        'connection was forcibly closed',
                        'connection reset',
                        'connection aborted',
                        'winerror 10054',
                        'winerror 10053'
                    ])

                    if is_connection_error:
                        delay = 3 * (attempt + 1)  # Progressive delay: 3s, 6s, 9s
                        print(f"    🔄 Connection error detected, waiting {delay}s before retry...")
                        await asyncio.sleep(delay)

                    # Try to fix the SQL with LLM
                    fix_result = await self._fix_sql_with_llm_async(current_sql, error_msg, errors_history, context, state)
                    if fix_result['success']:
                        current_sql = fix_result['fixed_sql']
                        print(f"    🔧 SQL fixed by LLM, retrying with new query")
                    else:
                        print(f"    ❌ SQL fix failed: {fix_result.get('error', 'Unknown')}")
                        break

        # All attempts failed
        return {
            'success': False,
            'error': f"All {max_retries} attempts failed. Last error: {errors_history[-1] if errors_history else 'Unknown error'}",
            'final_sql': current_sql,
            'attempts': max_retries,
            'errors_history': errors_history
        }

    async def _validate_fixed_sql(self, fixed_sql: str, original_sql: str, error_msg: str) -> Dict[str, Any]:
        """Guardrail validation to prevent invalid SQL outputs"""
        forbidden_patterns = [
            'show tables',
            'show databases',
            'describe table',
            'describe ',
            'information_schema.tables',
            'information_schema.columns',
            'show schemas',
            'list tables'
        ]

        sql_lower = fixed_sql.lower().strip()

        # Check for forbidden patterns
        for pattern in forbidden_patterns:
            if pattern in sql_lower:
                print(f"🚫 GUARDRAIL VIOLATION: Blocked '{pattern}' in LLM response")
                print(f"❌ Original error was: {error_msg}")

                return {
                    'success': False,
                    'error': f"Guardrail triggered: LLM attempted to generate '{pattern}' instead of fixing the query.",
                    'violated_pattern': pattern,
                    'original_sql': original_sql
                }

        # Check for table not found errors - ensure fixed SQL has valid FROM clause
        table_not_found_indicators = ['table not found', 'table does not exist', 'no such table', 'invalid table name']
        if any(indicator in error_msg.lower() for indicator in table_not_found_indicators):
            if len(sql_lower) < 20 or 'from' not in sql_lower:
                print(f"🚫 GUARDRAIL VIOLATION: Fixed SQL too short or missing FROM clause")
                return {
                    'success': False,
                    'error': "Guardrail triggered: Invalid fix for table-not-found error.",
                    'original_sql': original_sql
                }

        return {'success': True, 'fixed_sql': fixed_sql}

    async def _fix_sql_with_llm_async(self, failed_sql: str, error_msg: str, errors_history: List[str], context: Dict, state: AgentState) -> Dict[str, Any]:
        """Use LLM to fix SQL based on error"""

        history_text = "\n".join(errors_history) if errors_history else "No previous errors"
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')

        self._log('info', f"Attempting LLM-based SQL fix for error: {error_msg[:200]}", state)

        fix_prompt = SQL_FIX_PROMPT.format(
            current_question=current_question,
            dataset_metadata=dataset_metadata,
            failed_sql=failed_sql,
            error_msg=error_msg,
            history_text=history_text
        )

        for attempt in range(self.max_retries):
            try:
                print(f"    🔧 Reflection: Attempting SQL fix with LLM (attempt {attempt + 1})...")

                llm_response = await self.db_client.call_claude_api_endpoint_async(
                    messages=[{"role": "user", "content": fix_prompt}],
                    max_tokens=2000,
                    temperature=0.0,
                    top_p=0.1
                )

                self._log('info', "LLM response received from SQL fix handler", state,
                         llm_response=llm_response[:500] if llm_response else '',
                         attempt=attempt + 1)

                # Extract SQL from XML tags
                match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if match:
                    fixed_sql = match.group(1).strip()
                    fixed_sql = fixed_sql.replace('`', '')  # Remove backticks

                    if not fixed_sql:
                        raise ValueError("Empty fixed SQL query in XML response")

                    # Guardrail validation
                    validation_result = await self._validate_fixed_sql(fixed_sql, failed_sql, error_msg)

                    if not validation_result['success']:
                        print(f"    ⛔ Guardrail check failed: {validation_result['error']}")
                        return validation_result

                    print(f"    ✅ SQL fix generated successfully")
                    return {
                        'success': True,
                        'fixed_sql': fixed_sql
                    }
                else:
                    raise ValueError("No SQL found in XML tags")

            except Exception as e:
                print(f"    ❌ SQL fix attempt {attempt + 1} failed: {str(e)}")

                if attempt < self.max_retries - 1:
                    print(f"    🔄 Retrying SQL fix... (Attempt {attempt + 1}/{self.max_retries})")
                    await asyncio.sleep(2 ** attempt)

        return {
            'success': False,
            'error': f"SQL fix failed after {self.max_retries} attempts"
        }

    async def _execute_multiple_sql(self, sql_result: Dict, state: AgentState) -> Dict[str, Any]:
        """Execute multiple SQL queries in parallel"""
        sql_queries = sql_result.get('sql_queries', [])
        query_titles = sql_result.get('query_titles', [])

        # Execute all queries in parallel
        tasks = []
        for i, sql_query in enumerate(sql_queries):
            tasks.append(self._execute_single_sql({'sql_query': sql_query}, state))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        query_results = []
        success_count = 0

        for i, result in enumerate(results):
            title = query_titles[i] if i < len(query_titles) else f"Query {i + 1}"

            if isinstance(result, Exception):
                query_results.append({
                    'title': title,
                    'success': False,
                    'error': str(result)
                })
            elif result.get('success', False):
                success_count += 1
                query_results.append({
                    'title': title,
                    'success': True,
                    'sql_query': result.get('sql_query', ''),
                    'query_results': result.get('query_results', []),
                    'columns': result.get('columns', []),
                    'narrative': '',
                    'summary': '',
                    'row_count': result.get('row_count', 0),
                    'execution_attempts': result.get('execution_attempts', 1)
                })
            else:
                query_results.append({
                    'title': title,
                    'success': False,
                    'error': result.get('error', 'Unknown error')
                })

        return {
            'success': success_count > 0,
            'multiple_results': True,
            'query_results': query_results,
            'total_queries': len(sql_queries),
            'successful_queries': success_count,
            'sql_story': sql_result.get('sql_story', '')
        }

    # ════════════════════════════════════════════════════════════════════════════════
    # HELPER METHODS
    # ════════════════════════════════════════════════════════════════════════════════

    def _extract_previous_context(self, state: AgentState) -> Dict:
        """Extract previous SQL, question, and table from state history.

        Returns only the essential context needed for reflection:
        - question: The previous question that was asked
        - sql: The previous SQL that was executed
        - table_used: The table that was used
        """

        # Get the most recent SQL result to extract SQL query
        sql_result = state.get('sql_result', {})

        # Get the previous question (rewritten)
        previous_question = state.get('rewritten_question', state.get('previous_question_for_correction', ''))

        # Get the previous SQL
        previous_sql = ''
        if sql_result and isinstance(sql_result, dict):
            previous_sql = sql_result.get('sql_query', sql_result.get('sql', ''))

        # Get the previous table used
        previous_table = ''
        selected_dataset = state.get('selected_dataset', [])
        if selected_dataset:
            previous_table = selected_dataset[0] if isinstance(selected_dataset, list) else selected_dataset

        return {
            'question': previous_question,
            'sql': previous_sql,
            'table_used': previous_table
            # Removed: 'result': sql_result - not needed
        }

    def _load_available_datasets(self, domain_selection: str) -> List[Dict[str, str]]:
        """Load available datasets from metadata.json for the given domain"""
        try:
            metadata_config_path = "config/metadata/metadata.json"
            with open(metadata_config_path, 'r') as f:
                metadata_config = json.load(f)

            domain_datasets = metadata_config.get(domain_selection, [])

            # Convert to list of dicts with functional_name and table_name
            available_datasets = []
            for dataset_dict in domain_datasets:
                for functional_name, table_name in dataset_dict.items():
                    available_datasets.append({
                        "functional_name": functional_name,
                        "table_name": table_name
                    })

            print(f"✅ Loaded {len(available_datasets)} available datasets for {domain_selection}")
            return available_datasets

        except Exception as e:
            print(f"❌ Failed to load available datasets: {e}")
            return []

    def _generate_dataset_selection_question(self, available_datasets: List[Dict],
                                              previous_table_used: str,
                                              original_question: str) -> str:
        """Generate a follow-up question asking user to select a dataset"""

        datasets_formatted = format_available_datasets(available_datasets, previous_table_used)

        return f"""
I see you want to correct the previous answer. Let me help you fix it.

**Previous Question:** {original_question}
**Table Used:** {previous_table_used}


**AVAILABLE DATASETS:**

{datasets_formatted}


**WHAT TYPE OF ISSUE DID YOU NOTICE?**

1. Wrong dataset/table - Pick from datasets above or combine multiple
2. Wrong filter applied - Filter is on wrong column or wrong value
3. Wrong calculation - Metric aggregation is incorrect
4. Missing breakdown - Need additional grouping or columns

Please select an option number OR specify a dataset name.
""".strip()
