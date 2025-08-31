from typing import Dict, List, Optional
import json
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class LLMRouterAgent:
    """Simplified router agent with direct vector search and LLM selection"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
    def select_dataset(self, state: AgentState) -> Dict[str, any]:
        """Simplified dataset selection: Direct vector search → LLM selection"""
        
        user_question = state.get('current_question', state.get('original_question', ''))
        
        if not user_question:
            raise Exception("No user question found in state")
                
        # 1. Direct vector search with user question
        search_results = self._direct_vector_search_dataset(user_question)
        if not search_results:
            raise Exception("No datasets found in vector search")
        
        # 2. LLM selection from search results
        selection_result = self._llm_dataset_selection(search_results, state)
        # 3. Check if result is ambiguous (needs user clarification)
        if selection_result.get('requires_clarification', False):
            print(f"❓ Step 3: Ambiguous result - preparing for user clarification")
            return {
                'dataset_followup_question': selection_result.get('clarification_question', 'Please clarify your requirements.'),
                'selection_reasoning': selection_result.get('selection_reasoning', ''),
                'selected_dataset': selection_result.get('selected_dataset', {}),
                'requires_clarification': True
            }
        else: 

            return {
                'dataset_followup_question': None,
                'selection_reasoning': selection_result.get('selection_reasoning', ''),
                'table_kg': selection_result.get('table_kg', {}),  # This contains the selected dataset info
                'selected_dataset': selection_result.get('selected_dataset'),  # Add this for clarity
                'requires_clarification': False
            }
            print('router output else clause',selection_result)

    def _direct_vector_search_dataset(self, user_question: str, num_results: int = 10) -> List[Dict]:
        """Direct vector search against specified index"""
        
        try:
            search_results = self.db_client.vector_search_tables(
                query_text=user_question,
                num_results=num_results
            )
            return search_results
            
        except Exception as e:
            raise Exception(f"Vector search failed: {str(e)}")
    
    def _llm_dataset_selection(self, search_results: List[Dict], state: AgentState) -> Dict:
        """Use LLM to select best dataset from search results with enhanced analysis"""
        
        # Prepare dataset information for LLM
        dataset_options = []
        for i, result in enumerate(search_results):
            dataset_info = {
                'option_id': chr(65 + i),  # A, B, C, D... instead of numbers
                'table_name': result.get('table_name'),
                'description': result.get('table_description', '')
            }
            dataset_options.append(dataset_info)
        
        user_question = state.get('current_question', state.get('original_question', ''))

        selection_prompt = f"""
                    You are a meticulous dataset router. Choose EXACTLY ONE dataset.

                    USER QUESTION: "{user_question}"

                    DATASETS (JSON array). Each dataset has:
                    - name,description,metrics,attributes,columns,hints,time_grains

                    DATA:
                    {json.dumps(dataset_options, indent=2)}

                    GOAL
                    Map the user question to required columns using ONLY the dataset metadata and order of meta data is random. Evaluate BOTH datasets. Prefer a table that can satisfy ALL required columns and the requested time grain. If no table can fully satisfy, return the closest table by coverage.

                    Follow this decision process strictly:

                    1. **Match Attributes and Metrics First**
                    - Check if the dataset contains the attributes and metrics mentioned or implied in the user question.
                    - Prioritize exact matches (e.g., 'line_of_business', 'month', 'revenue').

                    2. **Check Time Granularity**
                    - Ensure the dataset supports the required time grain (e.g., monthly, daily).

                    3. **Evaluate Usefulness Tags**
                    - If the dataset is marked as 'useful_for' the type of analysis requested, that increases its relevance.
                    - If the dataset is marked as 'not_useful_for' the type of analysis requested, it should be excluded.

                    4. **Select Only One Dataset**
                    - Choose the single best dataset that satisfies the above criteria.

                    RESPONSE FORMAT:
                    The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.

                    {{ "clear_selection": true, "selected_dataset": "<one of the dataset 'table_name' values>", "selection_reasoning": "One concise sentence referencing why the dataset is best match (e.g., therapy_class_name + month required; only claims has therapy_class_name and supports month/day)." }}

                    """
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": selection_prompt}
                ])
                print('dataset detection',llm_response)
                
                selection_result = json.loads(llm_response)
                
                # Validate required fields
                if not selection_result.get('selected_dataset'):
                    raise ValueError("No selected_dataset in response")
                
                # Find the original metadata for the selected table
                selected_table = selection_result['selected_dataset']
                selected_metadata = next(
                    (ds for ds in search_results if ds['table_name'] == selected_table),
                    search_results[0] if search_results else {}
                )
                
                print(f'✅ Selected table: {selected_table}')
                
                return {
                    'dataset_followup_question': None,
                    'selected_dataset': selected_table,
                    'table_kg': selected_metadata.get('table_kg'),
                    'requires_clarification': False,
                    'selection_reasoning': selection_result.get('selection_reasoning', '')
                }
                    
            except Exception as e:
                retry_count += 1
                print(f"❌ Dataset selection attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying dataset selection... ({retry_count}/{max_retries})")
                    import time
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    print(f"❌ All dataset selection retries failed: {str(e)}")
                    return {
                        'dataset_followup_question': None,
                        'selected_dataset': None,
                        'table_kg': None,
                        'requires_clarification': False,
                        'selection_reasoning': 'Dataset selection failed',
                        'error': True,
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }

    def _fix_router_llm_call(self, state: AgentState) -> Dict:
        """Use LLM to select best dataset from search results with retry logic and fetch table_kg"""
        
        user_clarification_answer = state.get('current_question', state.get('original_question', ''))
        followup_question = state.get('dataset_followup_question', '')
        available_datasets = state.get('selected_dataset', '')
        question_history = state.get('user_question_history', '')
        actual_question = question_history[-1] if question_history else None
        
        selection_prompt = f"""
        Healthcare Finance Dataset Selection Task: You need to identify the actual table based 
        on the user clarification for the earlier follow up question.
        
        User actual Question: "{actual_question}"
        Follow up question: {followup_question}

        User answer: {user_clarification_answer}
        Available Datasets: {available_datasets}
        
        Instructions:
                - Interpret the actual question, follow up question and user clarification answer and pick one table from the available datasets and return
                
        RESPONSE FORMAT:
        The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.
        {{
            "clear_selection": true/false,
            "selected_dataset": "actual_table_name"
        }}
        """
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": selection_prompt}
                ])
                
                selection_result = json.loads(llm_response)
                
                # If we get here, JSON parsing succeeded
                if selection_result.get('selected_dataset'):
                    selected_table = selection_result.get('selected_dataset')
                                    
                    # Fetch table_kg from metadata table
                    try:
                        sql_query = f"""
                        SELECT table_kg 
                        FROM prd_optumrx_orxfdmprdsa.rag.metadata_tbl_level 
                        WHERE table_name = '{selected_table}'
                        """
                                    
                        # Execute SQL query
                        sql_results = self.db_client.execute_sql(sql_query)
                        
                        table_kg = None
                        if sql_results and len(sql_results) > 0:
                            table_kg = sql_results[0].get('table_kg', None)
                        else:
                            print(f"⚠️ No table_kg found for table: {selected_table}")
                        
                        return {
                            'dataset_followup_question': None,
                            'selected_dataset': selected_table,
                            'table_kg': table_kg,
                            'requires_clarification': False
                        }
                        
                    except Exception as sql_error:
                        # Return with error for SQL failure
                        return {
                            'dataset_followup_question': None,
                            'selected_dataset': None,
                            'table_kg': None,
                            'requires_clarification': False,
                            'error': True,
                            'error_message': f"SQL error while fetching table metadata: {str(sql_error)}"
                        }
                else:
                    return {
                        'dataset_followup_question': "Could not determine the appropriate dataset from your clarification.",
                        'selected_dataset': None,
                        'table_kg': None,
                        'requires_clarification': True
                    }
                
            except Exception as e:
                retry_count += 1
                print(f"❌ Dataset clarification attempt {retry_count} failed: {str(e)}")
                
                if retry_count < max_retries:
                    print(f"🔄 Retrying dataset clarification... ({retry_count}/{max_retries})")
                    import time
                    time.sleep(2 ** retry_count)
                    continue
                else:
                    print(f"❌ All dataset clarification retries failed: {str(e)}")
                    return {
                        'dataset_followup_question': "Error processing your clarification. Please try again.",
                        'selected_dataset': None,
                        'table_kg': None,
                        'requires_clarification': True,
                        'error': True,
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }
    
