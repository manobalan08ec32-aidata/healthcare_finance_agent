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
        
        print(f"🔍 Step 1: Direct vector search for: '{user_question}'")
        
        # 1. Direct vector search with user question
        search_results = self._direct_vector_search_dataset(user_question)
        
        if not search_results:
            raise Exception("No datasets found in vector search")
        
        # 2. LLM selection from search results
        selection_result = self._llm_dataset_selection(search_results, state)
        # print("selection result",selection_result)
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
        """Use LLM to select best dataset from search results with retry logic"""
        
        # Prepare dataset information for LLM
        dataset_options = []
        for i, result in enumerate(search_results):
            dataset_info = {
                'rank': i + 1,
                'table_name': result.get('table_name'),
                'description': result.get('content', '')
            }
            dataset_options.append(dataset_info)
        
        user_question = state.get('current_question', state.get('original_question', ''))
        
        selection_prompt = f"""
        Healthcare Finance Dataset Selection Task:
        
        User Question: "{user_question}"
        
        Available Datasets: {json.dumps(dataset_options, indent=2)}
        
        Analyze each dataset's suitability for answering the user's question based on the table name and description.
        
        Instructions:
            - Read the user's question and the dataset descriptions and pick one dataset which matches users question.
            - Do NOT interpret or infer the meaning of metrics, attributes, or business terms in the user's question.
            - Do NOT reason about dates, timeframes, or business context unless explicitly mentioned in the dataset description.
            - Only select a dataset if the description clearly contains terms from the user's question.
            - If there is only one candidate table, select it **only if** its description contains relevant terms from the user's question.
            - If multiple tables could be relevant, select the best match **only if** one clearly stands out.
            - If no clear match exists, respond with a clarification question. Include the available tables and their descriptions using business-friendly language.

        Response format (return EXACTLY this structure with no additional text):
        {{
            "clear_selection": true/false,
            "selected_dataset": "actual_table_name",
            "selection_reasoning": "2-3 lines on why this dataset is best",
            "requires_clarification": false/true,
            "ambiguous_options": [
                {{"table_name": "actual_table_name", "business_name": "Claims Data", "use_case": "for detailed transaction analysis"}},
                {{"table_name": "actual_table_name", "business_name": "Member Data", "use_case": "for member demographics"}}
            ],
            "clarification_question": "Are you looking for detailed claim transactions or member enrollment information?"
        }}
        
        IMPORTANT: 
        - Generate business_name dynamically based on table description
        - Make clarification_question specific to the user's query context
        - Use functional language, not technical jargon
        """
        
        # Retry logic - up to 2 attempts
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                if attempt == 0:
                    # First attempt
                    llm_response = self.db_client.call_claude_api([
                        {"role": "user", "content": selection_prompt}
                    ])
                else:
                    # Retry with correction instruction
                    retry_prompt = f"""
                    RETRY ATTEMPT {attempt}: The previous response had invalid JSON format.
                    
                    {selection_prompt}
                    
                    CRITICAL ERROR CORRECTION:
                    - Your previous response could not be parsed as valid JSON
                    - Return ONLY valid JSON format - no markdown, no code blocks, no extra text
                    - Do not wrap response in ```json or ``` 
                    - Start directly with {{ and end with }}
                    - Double-check all quotes and brackets are properly closed
                    """
                    
                    llm_response = self.db_client.call_claude_api([
                        {"role": "user", "content": retry_prompt}
                    ])
                print('llm_response from dataset selection',llm_response)
                selection_result = json.loads(llm_response)
                
                # If we get here, JSON parsing succeeded
                # Add metadata from search results
                if selection_result.get('selected_dataset'):
                    selected_table = selection_result['selected_dataset']
                    print('selected table',selected_table)
                    selected_metadata = next(
                        (ds for ds in search_results if ds['table_name'] == selected_table),
                        search_results[0] if search_results else {}
                    )
                
                # Add all search results for reference
                # selection_result['available_datasets'] = search_results
                print('return router output',selection_result)
                
                return {
                        'dataset_followup_question': selection_result.get('clarification_question'),
                        'selected_dataset': selection_result.get('selected_dataset'),
                        'table_kg': selected_metadata.get('table_kg'),
                        'requires_clarification': selection_result.get('requires_clarification'),
                        'selection_reasoning':selection_result.get('selection_reasoning')
                    }
                
            except json.JSONDecodeError as e:
                
                if attempt == max_retries:
                    # Final attempt failed, use fallback
                    return  {
                        'dataset_followup_question': 'Errored',
                        'selected_dataset': 'Errored',
                        'table_kg': 'Errored',
                        'requires_clarification': 'Errored'
                    }
                else:
                    # Continue to next retry
                    continue
                    
            except Exception as e:
                if attempt == max_retries:
                    raise Exception(f"LLM dataset selection failed after {max_retries + 1} attempts: {str(e)}")
                else:
                    continue
    
    
def _fix_router_llm_call(self, state: AgentState) -> Dict:
    """Use LLM to select best dataset from search results with retry logic and fetch table_kg"""
    
    user_clarification_answer= state.get('current_question', state.get('original_question', ''))
    followup_question = state.get('dataset_followup_question', '')
    available_datasets = state.get('selected_dataset', '')
    question_history=state.get('user_question_history', '')
    actual_question = actual_question = questions_history[-1] if questions_history else None
    selection_prompt = f"""
    Healthcare Finance Dataset Selection Task: You need to identify the actual table based 
    on the user clarification for the earlier follow up question.
    
    User actual Question: "{actual_question}"
    Follow up question: {followup_question}

    User answer: {user_clarification_answer}
    Available Datasets: {available_datasets}
    
    Instructions:
            - Interpret the actual question, follow up question and user clarification answer and pick one table from the available datasets and return
            
    Response format (return EXACTLY this structure with no additional text):
    {{
        "clear_selection": true/false,
        "selected_dataset": "actual_table_name"
    }}
    """
    
    # Retry logic - up to 2 attempts
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            if attempt == 0:
                # First attempt
                llm_response = self.db_client.call_claude_api([
                    {"role": "user", "content": selection_prompt}
                ])
            else:
                # Retry with correction instruction
                retry_prompt = f"""
                RETRY ATTEMPT {attempt}: The previous response had invalid JSON format.
                
                {selection_prompt}
                
                CRITICAL ERROR CORRECTION:
                - Your previous response could not be parsed as valid JSON
                - Return ONLY valid JSON format - no markdown, no code blocks, no extra text
                - Do not wrap response in ```json or ``` 
                - Start directly with {{ and end with }}
                - Double-check all quotes and brackets are properly closed
                """
                
                llm_response = self.db_client.call_claude_api([
                    {"role": "user", "content": retry_prompt}
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
                    # Return without table_kg if SQL fails
                    return {
                        'dataset_followup_question': None,
                        'selected_dataset': 'Error',
                        'table_kg': 'Error',
                        'requires_clarification': False
                    }
            else:
                return {
                    'dataset_followup_question': "Could not determine the appropriate dataset from your clarification.",
                    'selected_dataset': None,
                    'table_kg': None,
                    'requires_clarification': True
                }
            
        except json.JSONDecodeError as e:            
            if attempt == max_retries:
                # Final attempt failed, use fallback
                return {
                    'dataset_followup_question': "Error processing your clarification. Please try again.",
                    'selected_dataset': None,
                    'table_kg': None,
                    'requires_clarification': True
                }
            else:
                # Continue to next retry
                continue
                
        except Exception as e:
            if attempt == max_retries:
                return {
                    'dataset_followup_question': "Error processing your clarification. Please try again.",
                    'selected_dataset': None,
                    'table_kg': None,
                    'requires_clarification': True
                }
            else:
                continue
    
