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
        search_results = self._direct_vector_search(user_question)
        
        if not search_results:
            raise Exception("No datasets found in vector search")
        
        print(f"  → Found {len(search_results)} datasets")
        
        # 2. LLM selection from search results
        print(f"🧠 Step 2: LLM dataset selection from top results")
        selection_result = self._llm_dataset_selection(search_results, state)
        
        # 3. Check if result is ambiguous (needs user clarification)
        if selection_result.get('requires_clarification', False):
            print(f"❓ Step 3: Ambiguous result - preparing for user clarification")
            return self._prepare_clarification_interrupt(selection_result, search_results, state)
        
        print(f"✅ Step 3: Clear selection made")
        return selection_result
    
    def _direct_vector_search(self, user_question: str, num_results: int = 5) -> List[Dict]:
        """Direct vector search against specified index"""
        
        try:
            search_results = self.db_client.vector_search_tables(
                query_text=user_question,
                num_results=num_results
            )
            
            # Parse table_kg JSON if it exists
            for result in search_results:
                if result.get('table_kg'):
                    try:
                        result['table_kg_parsed'] = json.loads(result['table_kg'])
                    except json.JSONDecodeError:
                        result['table_kg_parsed'] = {}
                else:
                    result['table_kg_parsed'] = {}
            
            return search_results
            
        except Exception as e:
            raise Exception(f"Vector search failed: {str(e)}")
    
    def _llm_dataset_selection(self, search_results: List[Dict], state: AgentState) -> Dict:
        """Use LLM to select best dataset from search results"""
        
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
        
        Available Datasets (from vector search): {json.dumps(dataset_options, indent=2)}
        
        Previous Context: {state.get('context_from_previous_query', 'None')}
        User Preferences: {state.get('user_preferences', {})}
        
        Analyze each dataset's suitability for answering the user's question based on the table name and description.
        
        SELECTION CRITERIA:
        1. RELEVANCE: How directly can this dataset answer the question?
        2. COMPLETENESS: Does the description suggest it has necessary data elements?
        3. HEALTHCARE FINANCE FIT: Appropriate for the domain?
        
        CLARITY ASSESSMENT:
        - If ONE dataset is clearly best (confidence >85%): Select it
        - If multiple datasets could work equally well: Mark as ambiguous
        
        Respond with JSON:
        {{
            "clear_selection": true,
            "selected_dataset": "table_name",
            "selection_confidence": 0.95,
            "selection_reasoning": "detailed explanation",
            "requires_clarification": false,
            "ambiguous_options": [
                {{"table_name": "option1", "use_case": "when this would be better"}},
                {{"table_name": "option2", "use_case": "when this would be better"}}
            ],
            "clarification_question": "What specific aspect are you most interested in?"
        }}
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": selection_prompt}
            ])
            
            selection_result = json.loads(llm_response)
            
            # Add metadata from search results
            if selection_result.get('selected_dataset'):
                selected_table = selection_result['selected_dataset']
                selected_metadata = next(
                    (ds for ds in search_results if ds['table_name'] == selected_table),
                    search_results[0] if search_results else {}
                )
                
                selection_result['dataset_metadata'] = {
                    'table_name': selected_metadata.get('table_name'),
                    'description': selected_metadata.get('content'),
                    'table_kg': selected_metadata.get('table_kg'),
                    'columns': selected_metadata.get('table_kg_parsed', {})
                }
            
            # Add all search results for reference
            selection_result['available_datasets'] = search_results
            
            return selection_result
            
        except json.JSONDecodeError as e:
            return self._fallback_selection(search_results, state)
        except Exception as e:
            raise Exception(f"LLM dataset selection failed: {str(e)}")
    
    def _prepare_clarification_interrupt(self, selection_result: Dict, search_results: List[Dict], 
                                       state: AgentState) -> Dict:
        """Prepare data for LangGraph interrupt to ask user for clarification"""
        
        ambiguous_options = selection_result.get('ambiguous_options', [])
        clarification_question = selection_result.get('clarification_question', 
                                                    "Which dataset would you prefer?")
        
        # Prepare user-friendly options
        user_options = []
        for option in ambiguous_options:
            table_name = option.get('table_name')
            use_case = option.get('use_case')
            
            # Find full metadata for this option
            full_metadata = next(
                (ds for ds in search_results if ds['table_name'] == table_name),
                {}
            )
            
            user_options.append({
                'option_id': len(user_options) + 1,
                'table_name': table_name,
                'display_name': table_name.split('.')[-1],  # Show just table name
                'use_case': use_case,
                'description': full_metadata.get('content', '')[:150] + '...',
                'column_count': len(full_metadata.get('table_kg_parsed', {}).get('columns', []))
            })
        
        return {
            'requires_clarification': True,
            'clarification_question': clarification_question,
            'user_options': user_options,
            'selection_reasoning': selection_result.get('selection_reasoning'),
            'available_datasets': search_results,
            'interrupt_data': {
                'type': 'dataset_clarification',
                'question': clarification_question,
                'options': user_options
            }
        }
    
    def process_user_clarification(self, user_choice: Dict, available_datasets: List[Dict]) -> Dict:
        """Process user's clarification choice and return final selection"""
        
        selected_option_id = user_choice.get('option_id')
        selected_table_name = user_choice.get('table_name')
        
        # Find the selected dataset
        selected_metadata = next(
            (ds for ds in available_datasets if ds['table_name'] == selected_table_name),
            None
        )
        
        if not selected_metadata:
            raise Exception(f"Selected dataset {selected_table_name} not found")
        
        return {
            'selected_dataset': selected_table_name,
            'dataset_metadata': {
                'table_name': selected_metadata.get('table_name'),
                'description': selected_metadata.get('content'),
                'table_kg': selected_metadata.get('table_kg'),
                'columns': selected_metadata.get('table_kg_parsed', {})
            },
            'selection_confidence': 1.0,  # User made the choice
            'selection_reasoning': f"User selected option {selected_option_id}: {selected_table_name}",
            'user_clarified': True,
            'available_datasets': available_datasets
        }
    
    def _fallback_selection(self, search_results: List[Dict], state: AgentState) -> Dict:
        """Fallback selection when LLM fails"""
        
        if not search_results:
            raise Exception("No datasets available for selection")
        
        # Select first result as fallback
        selected = search_results[0]
        
        return {
            'selected_dataset': selected.get('table_name'),
            'selection_confidence': 0.6,
            'selection_reasoning': 'Fallback selection - first available dataset',
            'dataset_metadata': {
                'table_name': selected.get('table_name'),
                'description': selected.get('content'),
                'table_kg': selected.get('table_kg', '{}'),
                'columns': selected.get('table_kg_parsed', {})
            },
            'requires_clarification': False,
            'available_datasets': search_results
        }