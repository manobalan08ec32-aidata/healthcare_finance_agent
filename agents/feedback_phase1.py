async def _llm_feedback_selection(self, feedback_results: List[Dict], state: AgentState) -> Dict:
        """Enhanced LLM selection with validation, disambiguation handling, and filter-based selection"""
        
        user_question = state.get('current_question', state.get('original_question', ''))
        filter_values = state.get('filter_values', [])
