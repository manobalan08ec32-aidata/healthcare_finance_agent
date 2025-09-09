from typing import Dict, List, Optional
import json
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class LLMRouterAgent:
    """Enhanced router agent with simplified table selection"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
    def select_dataset(self, state: AgentState) -> Dict[str, any]:
        """Enhanced dataset selection with state-based flow handling"""

        # Check if this is a follow-up call
        if state.get('requires_dataset_clarification', False):
            print(f"🔄 Processing follow-up clarification")
            result = self._fix_router_llm_call(state)
            # After clarification, extract metadata using selected_dataset from result
            metadata = self.get_metadata(
                state=state,
                selected_dataset=result.get('selected_dataset', [])
            )
            result['dataset_metadata'] = metadata
            return result

        # This is initial call - proceed with normal flow
        user_question = state.get('current_question', state.get('original_question', ''))

        if not user_question:
            raise Exception("No user question found in state")
                
        # 1. Vector search returns full context including actual table names
        # search_results = self._direct_vector_search_dataset(user_question)
        search_results = [
                {"table_name":"prd_optumrx_orxfdmprdsa.rag.claim_transaction_for_pharmacy_pbm","description":"Claim-level dataset supporting financial and utilization analysis across client, pharmacy, and drug dimensions. Enables variance tracking by therapy class, manufacturer, and other attributes, with daily granularity and derived metrics like revenue per script and GDR.","useful_for":["claim-level financial analysis","client-level analysis","drug and therapy class performance","manufacturer-level insights","revenue per script and GDR metrics","line-of-business tracking","daily and monthly trend analysis","rate analysis"],"not_useful_for":["budget-level summaries","forecast comparisons","aggregated financial reporting without claim-level detail"],"metrics":["revenue","expense","cogs","wac","awp","unadjusted_scripts","adjusted_scripts","30_day_scripts","90_day_scripts","revenue_per_script","gdr","volume"],"attributes":{"claim_nbr":"string","submit_date":"date","client_id":"string","pharmacy_npi_no":"string","drug_name":"string","therapy_class_name":"string","drug_manufctr_nm":"string","line_of_business":"string","state_cd":"string","brand_vs_generic":"string","client_type":"string","gpi_no":"string","pharmacy_name":"string","pharmacy_type":"string","carrier_id":"string","member_date_of_birth":"date","member_sex":"string"},"time_grains":["daily","monthly","quarterly","yearly"]},  
                {"table_name":"prd_optumrx_orxfdmprdsa.rag.actuals_vs_forecast_analysis","description":"This table contains ledger-level financial data including actuals, forecasts, and budget figures. It supports comparative analysis across time periods and tracks performance at the line-of-business level.","useful_for":["monthly actuals analysis","actuals vs forecast vs budget comparison","line-of-business level tracking"],"not_useful_for":["rate_analysis","claim-level analysis","daily granularity","client-level insights"],"metrics":["revenue","expense","cogs","sg&a","ioi","membership","unadjusted_scripts","adjusted_scripts","30_day_scripts","90_day_scripts","volume"],"attributes":{"ledger":"string","line_of_business":"string","transaction_date":"date","year":"integer","month":"integer","quarter":"integer","state_cd":"string"},"time_grains":["monthly","quarterly","yearly"]}        
                                                  ]
        if not search_results:
            raise Exception("No datasets found in vector search")
        
        # 2. LLM selection with direct table name handling
        selection_result = self._llm_dataset_selection(search_results, state)

        # 3. Return results - either final selection or clarification needed
        if selection_result.get('requires_clarification', False):
            print(f"❓ Clarification needed - preparing follow-up question")
            return {
                'dataset_followup_question': selection_result.get('clarification_question'),
                'selection_reasoning': selection_result.get('selection_reasoning', ''),
                'candidate_actual_tables': selection_result.get('candidate_actual_tables', []),
                'functional_names': selection_result.get('functional_names', []),
                'requires_clarification': True,
                'error_message': selection_result.get('error_message', '')
            }
        else:
            print(f"✅ Dataset selection complete")
            # After selection, extract metadata using selected_dataset from selection_result
            metadata = self.get_metadata(
                state=state,
                selected_dataset=selection_result.get('final_actual_tables', [])
            )
            return {
                'dataset_followup_question': None,
                'selection_reasoning': selection_result.get('selection_reasoning', ''),
                'selected_dataset': selection_result.get('final_actual_tables', []),
                'functional_names': selection_result.get('functional_names', []),
                'requires_clarification': False,
                'error_message': selection_result.get('error_message', ''),
                'dataset_metadata': metadata
            }

    def get_metadata(self, state: Dict, selected_dataset: list) -> str:
        """Extract metadata using vector search for the given selected_dataset"""
        current_question = state.get('rewritten_question', state.get('current_question', ''))

        # Use selected_dataset as tables_list if available, else empty list
        tables_list = selected_dataset if isinstance(selected_dataset, list) else [selected_dataset] if selected_dataset else []
        cluster_results = self.db_client.vector_search_columns(
            query_text=current_question,
            num_results=20,
            tables_list=tables_list
        )

        # Group llm_context by table_name and build markdown metadata
        clusters_by_table = {}
        for cluster in cluster_results:
            table = cluster['table_name']
            if table not in clusters_by_table:
                clusters_by_table[table] = []
            clusters_by_table[table].append(cluster['llm_context'])

        # Build final markdown metadata
        metadata = ""
        for table, clusters in clusters_by_table.items():
            metadata += f"\n## Table: {table}\n\n"
            for cluster_markdown in clusters:
                metadata += cluster_markdown + "\n\n"

        return metadata

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
        You are a meticulous dataset router. Choose EXACTLY ONE dataset OR ask for clarification.

        USER QUESTION: "{user_question}"

        AVAILABLE DATASETS:
        {search_results}

        GOAL: Map the user question to required columns using ONLY the dataset metadata. Evaluate ALL datasets. Prefer a table that can satisfy ALL required columns and the requested time grain.

        DECISION PROCESS:
        1. **Match Attributes and Metrics First**
        - Check if the dataset contains the attributes and metrics mentioned or implied in the user question
        - Prioritize exact matches (e.g., 'line_of_business', 'month', 'revenue')

        2. **Check Time Granularity** 
        - Ensure the dataset supports the required time grain (e.g., monthly, daily)

        3. **Evaluate Suitability Tags**
        - If dataset is marked in 'useful_for' for the analysis type, increase relevance
        - If dataset is marked in 'not_useful_for' for the analysis type, exclude it

        4. **Final Selection**
        - If EXACTLY ONE dataset satisfies all criteria → SELECT IT
        - If MULTIPLE datasets satisfy criteria → ASK CLARIFICATION  
        - If NO dataset fully satisfies → SELECT closest match by coverage

        WHEN TO ASK CLARIFICATION:
        - Multiple datasets have the required attributes/metrics AND both are suitable
        - Question could legitimately use different dataset types
        - Ask: "Multiple datasets match your requirements. Which analysis: [brief dataset type descriptions]?"
                
        RESPONSE FORMAT :
        The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```
        {{
            "final_actual_tables": ["actual_table_name1"] or ["table1", "table2"] if multiple needed,
            "functional_names": ["user-friendly name 1"] or ["name1", "name2"] if multiple,
            "requires_clarification": false if dataset found else true,
            "clarification_question": null if dataset found else ask question,
            "candidate_actual_tables": []  ,
            "selection_reasoning": "Brief explanation of selection based on attribute/metric match"
        }}
        
        CRITICAL RULES:
        - Use ONLY the provided dataset metadata
        - When multiple datasets match equally, ASK clarification rather than guessing
        - Be decisive when there's a clear best match
        - Focus on exact attribute/metric matches first
        
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
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }

    def _fix_router_llm_call(self, state: AgentState) -> Dict:
        """Handle follow-up clarification with direct actual table names"""
        
        user_clarification = state.get('current_question', '')
        followup_question = state.get('dataset_followup_question', '')
        candidate_actual_tables = state.get('candidate_actual_tables', [])
        functional_names = state.get('functional_names', [])
        original_question = state.get('rewritten_question', [])
        
        selection_prompt = f"""
        Based on the user's clarification, finalize the dataset selection.
        
        Original Question: "{original_question}"
        
        Your previous Clarification Question: "{followup_question}"
        
        User's Answer: "{user_clarification}"
        
        Candidate Actual Tables: {candidate_actual_tables}
        Functional Names: {functional_names}
        
        Based on the clarification, determine which actual table names should be used.
        
        RESPONSE FORMAT (valid JSON only):
        {{
            "final_actual_tables": ["actual_table_name1", "actual_table_name2"]        
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
                    'requires_clarification': False
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
                        'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                    }
