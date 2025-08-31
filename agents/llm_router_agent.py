from typing import Dict, List, Optional
import json
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class LLMRouterAgent:
    """Enhanced router agent with simplified table selection"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
    def select_dataset(self, state: AgentState) -> Dict[str, any]:
        """Enhanced dataset selection with simplified approach"""
        
        user_question = state.get('current_question', state.get('original_question', ''))
        
        if not user_question:
            raise Exception("No user question found in state")
                
        # 1. Vector search returns full context including actual table names
        search_results = self._direct_vector_search_dataset(user_question)
        if not search_results:
            raise Exception("No datasets found in vector search")
        
        # 2. LLM selection with direct table name handling
        selection_result = self._llm_dataset_selection(search_results, state)
        
        # 3. Handle clarification or return selected datasets
        if selection_result.get('requires_clarification', False):
            print(f"❓ Ambiguous query - preparing clarification question")
            return {
                'dataset_followup_question': selection_result.get('clarification_question'),
                'selection_reasoning': selection_result.get('selection_reasoning', ''),
                'candidate_actual_tables': selection_result.get('candidate_actual_tables', []),
                'functional_names': selection_result.get('functional_names', []),
                'requires_clarification': True
            }
        else:
            return {
                'dataset_followup_question': None,
                'selection_reasoning': selection_result.get('selection_reasoning', ''),
                'selected_dataset': selection_result.get('final_actual_tables', []),
                'functional_names': selection_result.get('functional_names', []),
                'requires_clarification': False
            }

    def _direct_vector_search_dataset(self, user_question: str, num_results: int = 10) -> List[Dict]:
        """Vector search returning full results including actual table names"""
        
        try:
            # Get full search results with actual table names
            full_results = self.db_client.vector_search_tables(
                query_text=user_question,
                num_results=num_results
            )
            
            return full_results
            
        except Exception as e:
            raise Exception(f"Vector search failed: {str(e)}")
    
    def _llm_dataset_selection(self, search_results: List[Dict], state: AgentState) -> Dict:
        """LLM selection with direct actual table name handling"""
        
        user_question = state.get('current_question', state.get('original_question', ''))

        selection_prompt = f"""
        You are a decisive dataset router. Follow this decision process strictly:

        USER QUESTION: "{user_question}"

        AVAILABLE DATASETS:
        {json.dumps(search_results[:5], indent=2)}

        DECISION PROCESS:
        1. **Match Attributes and Metrics First**
        - Check if the dataset contains the attributes and metrics mentioned or implied in the user question
        - Prioritize exact matches (e.g., 'line_of_business', 'month', 'revenue')
        
        2. **Check Time Granularity**
        - Ensure the dataset supports the required time grain (e.g., monthly, daily)
        
        3. **Evaluate Usefulness Tags**
        - If dataset is marked as 'useful_for' the type of analysis requested, increase relevance
        - If dataset is marked as 'not_useful_for' the type of analysis requested, exclude it
        
        4. **Multi-Table Analysis Detection**
        - Detect if query requires joining data across tables (e.g., claim amounts from different tables)
        - Identify queries that benefit from complementary data perspectives (volumes + financials)
        - Recognize queries needing separate analysis on related datasets (demographics + transactions)
        
        5. **Dataset Selection Strategy**
        - Choose the single best dataset that satisfies the above criteria
        - Select multiple datasets only when analysis inherently requires multiple tables
        
        WHEN TO ASK FOLLOW-UP (RARE CASES ONLY):
        
        **Scenario 1: Data Type Ambiguity**
        - ONLY when both tables have same LOB and user didn't specify "ledger" vs "claims" keywords
        - Example: "Which dataset: claims transactions or financial ledger data?"
        
        **Scenario 2: Multi-Table Analysis Confirmation**
        - When query requires joining data across tables (e.g., billed amount from Table A + paid amount from Table B)
        - When query benefits from complementary perspectives (transaction volumes + financial metrics)
        - When query needs separate analysis on related datasets then combining insights
        - Example: "This analysis requires both Claims Details and Payment Records tables. Proceed with both?"
        
        DO NOT ASK FOLLOW-UP FOR GENERAL AMBIGUITIES:
        - Time period vagueness (e.g., "show pharmacy sales" without specifying when) → Pick most recent complete period
        - Aggregation level uncertainty (e.g., "revenue data" without daily/monthly) → Use most appropriate grain available
        - Scope ambiguity (e.g., "claim trends" without LOB specified) → Use broadest/most complete dataset
        - Metric preference vagueness (e.g., "pharmacy performance" unclear on volume vs financial) → Pick best match based on context
        - BE DECISIVE: Make reasonable choices for these ambiguities rather than asking clarification
        
        MULTI-TABLE EXAMPLES:
        - "What is the claim paid and billed amount?" → Need claims table + payments table (JOIN required)
        - "Show pharmacy performance trends" → Transaction data + Financial metrics (Complementary analysis)
        - "Compare member demographics with claim patterns" → Demographics table + Claims table (Separate analysis)
        
        RESPONSE FORMAT (valid JSON only, no markdown):
        {{
            "final_actual_tables": ["actual_table_name1"] or ["table1", "table2"] if multiple needed,
            "functional_names": ["user-friendly name 1"] or ["name1", "name2"] if multiple,
            "requires_clarification": false,
            "clarification_question": null,
            "candidate_actual_tables": ["table1", "table2"] or [] if no clarification needed,
            "selection_reasoning": "Brief explanation of selection based on attribute/metric match"
        }}
        
        IMPORTANT: 
        - When LLM decides on right dataset(s), ALWAYS set requires_clarification: false and clarification_question: null
        - Only set requires_clarification: true and populate clarification_question for the 2 scenarios mentioned above
        - Keep clarification_question short and direct when needed
        - candidate_actual_tables should be populated only when requires_clarification: true
        """
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": selection_prompt}
                ])
                
                selection_result = json.loads(llm_response)
                
                print(f"✅ Dataset selection complete: {selection_result.get('functional_names')}")
                return selection_result
                    
            except Exception as e:
                retry_count += 1
                print(f"❌ Dataset selection attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying... ({retry_count}/{max_retries})")
                    import time
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    return {
                        'final_actual_tables': [],
                        'functional_names': [],
                        'requires_clarification': False,
                        'selection_reasoning': 'Dataset selection failed',
                        'error': True,
                        'error_message': str(e)
                    }

    def _fix_router_llm_call(self, state: AgentState) -> Dict:
        """Handle follow-up clarification with direct actual table names"""
        
        user_clarification = state.get('current_question', '')
        followup_question = state.get('dataset_followup_question', '')
        candidate_actual_tables = state.get('candidate_actual_tables', [])
        functional_names = state.get('functional_names', [])
        question_history = state.get('user_question_history', [])
        original_question = question_history[-1] if question_history else None
        
        selection_prompt = f"""
        Based on the user's clarification, finalize the dataset selection.
        
        Original Question: "{original_question}"
        
        Our Clarification Question: "{followup_question}"
        
        User's Answer: "{user_clarification}"
        
        Candidate Actual Tables: {candidate_actual_tables}
        Functional Names: {functional_names}
        
        Based on the clarification, determine which actual table names should be used.
        
        RESPONSE FORMAT (valid JSON only):
        {{
            "final_actual_tables": ["actual_table_name1", "actual_table_name2"],
            "selection_reasoning": "Brief explanation based on user clarification"
        }}
        """
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": selection_prompt}
                ])
                
                result = json.loads(llm_response)
                
                return {
                    'dataset_followup_question': None,
                    'selected_dataset': result.get('final_actual_tables', []),
                    'functional_names': functional_names,  # Keep original functional names
                    'requires_clarification': False,
                    'selection_reasoning': result.get('selection_reasoning', '')
                }
                
            except Exception as e:
                retry_count += 1
                print(f"❌ Clarification processing attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying... ({retry_count}/{max_retries})")
                    import time
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    return {
                        'dataset_followup_question': "Error processing clarification",
                        'selected_dataset': [],
                        'requires_clarification': False,
                        'error': True,
                        'error_message': str(e)
                    }
