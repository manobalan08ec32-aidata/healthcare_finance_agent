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
    

def _llm_dataset_selection(self, search_results: List[Dict], state: AgentState) -> Dict:
    """Enhanced LLM selection with validation and disambiguation handling"""
    
    user_question = state.get('current_question', state.get('original_question', ''))

    selection_prompt = f"""
    You are a Dataset Identifier Agent. You have TWO sequential tasks to complete.

    CURRENT QUESTION: {user_question}
    AVAILABLE DATASETS: {search_results}

    ==============================
    TASK 1: COMPREHENSIVE DATASET ASSESSMENT
    ==============================

    Analyze the user's question for the following validation areas:

    A. METRICS AVAILABILITY CHECK:
    - Extract all requested metrics/measures from the question
    - Map each metric to available "metrics" arrays across datasets
    - Identify any metrics that don't exist in any dataset

    B. ATTRIBUTES AVAILABILITY CHECK:
    - Extract all requested attributes/dimensions from the question  
    - Map each attribute to available "attributes" objects across datasets
    - Identify any attributes that don't exist in any dataset

    C. DATASET SUITABILITY ANALYSIS:
    - Which datasets match the requested metrics AND attributes?
    - Check "useful_for" and "not_useful_for" fields
    - Verify time_grains match user needs (daily vs monthly vs quarterly)

    D. COLUMN DISAMBIGUATION NEEDS:
    - Are there overlapping column names across multiple suitable datasets?
    - Do overlapping columns have different contexts (e.g., claim-level vs ledger-level)?
    - Would this create confusion for the user?

    E. ALTERNATIVE SUGGESTIONS:
    - For missing items, are there similar available alternatives?
    - Can user needs be met with different but related metrics/attributes?

    ==============================
    TASK 1 DECISION CRITERIA
    ==============================

    PROCEED TO TASK 2 (Dataset Selection) IF:
    - All requested metrics/attributes exist in available datasets
    - Clear dataset match with no disambiguation needed
    - No overlapping columns requiring clarification

    MISSING ITEMS RESPONSE IF:
    - Any requested metrics/attributes are missing from all datasets
    - No suitable alternatives exist

    REQUEST FOLLOW-UP IF:
    - Multiple datasets have same columns but different contexts
    - User intent unclear due to ambiguous terms
    - Multiple equally suitable datasets exist

    ==============================
    TASK 2: DATASET RECOMMENDATION
    ==============================

    (Only execute if Task 1 assessment says "PROCEED")

    Select the single best dataset based on:
    1. Complete metric/attribute coverage
    2. Appropriate time granularity
    3. Suitability tags alignment
    4. Purpose match

    ==============================
    ASSESSMENT FORMAT
    ==============================

    A. METRICS AVAILABILITY CHECK: ✓ Clear / ❌ Issues Found
    - [List each requested metric and its availability status]

    B. ATTRIBUTES AVAILABILITY CHECK: ✓ Clear / ❌ Issues Found  
    - [List each requested attribute and its availability status]

    C. DATASET SUITABILITY ANALYSIS: ✓ Clear / ❌ Issues Found
    - [Analysis of which datasets match requirements]

    D. COLUMN DISAMBIGUATION NEEDS: ✓ Clear / ❌ Disambiguation Needed
    - [Analysis of any overlapping columns]

    E. ALTERNATIVE SUGGESTIONS: ✓ Not Needed / 📝 Alternatives Available
    - [Any suggested alternatives for missing items]

    **DECISION: PROCEED** / **MISSING_ITEMS** / **REQUEST_FOLLOW_UP** - [Brief reasoning]

    ==============================
    RESPONSE FORMAT
    ==============================
    
    The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting.

    If DECISION = PROCEED:
    {{
        "status": "success",
        "final_actual_tables": ["table_name"],
        "functional_names": ["user-friendly name"],
        "requires_clarification": false,
        "selection_reasoning": "Brief explanation with assessment summary",
        "missing_items": {{"metrics": [], "attributes": []}},
        "user_message": null
    }}

    If DECISION = MISSING_ITEMS:
    {{
        "status": "missing_items", 
        "final_actual_tables": [],
        "functional_names": [],
        "requires_clarification": false,
        "selection_reasoning": "Assessment summary showing missing items",
        "missing_items": {{"metrics": ["missing_metric1"], "attributes": ["missing_attr1"]}},
        "user_message": "I don't have access to the following metrics: [list] and attributes: [list] in our current datasets. You'll need to reach out to the data team for these. However, I can help you with similar available metrics like: [alternatives]. Would you like to proceed with those instead?"
    }}

    If DECISION = REQUEST_FOLLOW_UP:
    {{
        "status": "needs_disambiguation",
        "final_actual_tables": [],
        "functional_names": [],
        "requires_clarification": true,
        "clarification_question": "Specific question about dataset choice or column disambiguation",
        "candidate_actual_tables": ["table1", "table2"],
        "selection_reasoning": "Assessment summary showing ambiguity",
        "missing_items": {{"metrics": [], "attributes": []}},
        "user_message": null
    }}

    CRITICAL RULES:
    - Complete TASK 1 assessment across all 5 areas (A-E)
    - Show your reasoning with ✓ or ❌ for each area
    - Make clear PROCEED/MISSING_ITEMS/REQUEST_FOLLOW_UP decision
    - Only set requires_clarification=true for genuine ambiguity, not missing items
    """
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            llm_response = self.db_client.call_claude_api_endpoint([
                {"role": "user", "content": selection_prompt}
            ])
            selection_result = json.loads(llm_response)
            
            # Handle different status types
            status = selection_result.get('status', 'success')
            
            if status == "missing_items":
                print(f"❌ Missing items found: {selection_result.get('missing_items')}")
                return {
                    'final_actual_tables': [],
                    'functional_names': [],
                    'requires_clarification': False,
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'missing_items': selection_result.get('missing_items', {}),
                    'user_message': selection_result.get('user_message', ''),
                    'error_message': ''
                }
            
            elif status == "needs_disambiguation":
                print(f"❓ Clarification needed - preparing follow-up question")
                return {
                    'final_actual_tables': [],
                    'functional_names': [],
                    'requires_clarification': True,
                    'clarification_question': selection_result.get('clarification_question'),
                    'candidate_actual_tables': selection_result.get('candidate_actual_tables', []),
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'missing_items': selection_result.get('missing_items', {}),
                    'error_message': ''
                }
            
            else:  # status == "success"
                print(f"✅ Dataset selection complete: {selection_result.get('functional_names')}")
                return {
                    'final_actual_tables': selection_result.get('final_actual_tables', []),
                    'functional_names': selection_result.get('functional_names', []),
                    'requires_clarification': False,
                    'selection_reasoning': selection_result.get('selection_reasoning', ''),
                    'missing_items': selection_result.get('missing_items', {}),
                    'error_message': ''
                }
                    
        except Exception as e:
            retry_count += 1
            print(f"⚠ Dataset selection attempt {retry_count} failed: {str(e)}")
            
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
                    'missing_items': {'metrics': [], 'attributes': []},
                    'error': True,
                    'error_message': f"Model serving endpoint failed after {max_retries} attempts: {str(e)}"
                }

    def _extract_json_from_response(self, response: str) -> str:
        """Extract JSON content from XML tags or return the response if no tags found"""
        import re
        
        # Try to extract content between <json> tags
        json_match = re.search(r'<json>(.*?)</json>', response, re.DOTALL)
        if json_match:
            return json_match.group(1).strip()
        
        # If no XML tags found, assume the entire response is JSON
        return response.strip()


    def _fix_router_llm_call(self, state: AgentState) -> Dict:
    """Handle follow-up clarification with topic drift detection in single call"""
    
    user_clarification = state.get('current_question', '')
    followup_question = state.get('dataset_followup_question', '')
    candidate_actual_tables = state.get('candidate_actual_tables', [])
    functional_names = state.get('functional_names', [])
    original_question = state.get('rewritten_question', state.get('original_question', ''))
    
    combined_prompt = f"""
    You need to analyze the user's response and either process clarification or detect topic drift.

    CONTEXT:
    ORIGINAL QUESTION: "{original_question}"
    YOUR CLARIFICATION QUESTION: "{followup_question}"
    USER'S RESPONSE: "{user_clarification}"
    
    CANDIDATE TABLES: {candidate_actual_tables}
    FUNCTIONAL NAMES: {functional_names}

    TASK: Determine what type of response this is and handle accordingly.

    ANALYSIS STEPS:
    1. **Response Type Detection**:
       - CLARIFICATION_ANSWER: User is responding to your clarification question
       - NEW_QUESTION: User is asking a completely different question (topic drift)
       - MODIFIED_SCOPE: User modified the original question's scope

    2. **Action Based on Type**:
       - If CLARIFICATION_ANSWER → Select final dataset from candidates
       - If NEW_QUESTION → Signal topic drift for fresh processing
       - If MODIFIED_SCOPE → Signal scope change for revalidation

    RESPONSE FORMAT (valid JSON only):
    {{
        "response_type": "clarification_answer" | "new_question" | "modified_scope",
        "final_actual_tables": ["table_name"] if clarification_answer else [],
        "selection_reasoning": "explanation" if clarification_answer else "topic drift/scope change detected",
        "topic_drift": true if new_question else false,
        "modified_scope": true if modified_scope else false,
        "new_question": "{user_clarification}" if new_question else null,
        "modified_question": "{user_clarification}" if modified_scope else null
    }}

    DECISION LOGIC:
    - If user response clearly chooses between dataset options → CLARIFICATION_ANSWER
    - If user asks about completely different metrics/attributes → NEW_QUESTION  
    - If user refines original question without answering clarification → MODIFIED_SCOPE

    CRITICAL: Be decisive about response type to avoid processing loops.
    """
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            llm_response = self.db_client.call_claude_api_endpoint([
                {"role": "user", "content": combined_prompt}
            ])
            
            result = json.loads(llm_response)
            response_type = result.get('response_type')
            
            if response_type == 'new_question':
                print(f"🔄 Topic drift detected - treating as new question")
                return {
                    'dataset_followup_question': None,
                    'selected_dataset': [],
                    'functional_names': [],
                    'requires_clarification': False,
                    'topic_drift': True,
                    'new_question': result.get('new_question', user_clarification)
                }
            
            elif response_type == 'modified_scope':
                print(f"🔄 Modified scope detected - restarting validation")
                return {
                    'dataset_followup_question': None,
                    'selected_dataset': [],
                    'functional_names': [],
                    'requires_clarification': False,
                    'modified_scope': True,
                    'modified_question': result.get('modified_question', user_clarification)
                }
            
            else:  # clarification_answer
                print(f"✅ Clarification resolved: {result.get('final_actual_tables')}")
                return {
                    'dataset_followup_question': None,
                    'selected_dataset': result.get('final_actual_tables', []),
                    'functional_names': functional_names,
                    'requires_clarification': False,
                    'selection_reasoning': result.get('selection_reasoning', '')
                }
            
        except Exception as e:
            retry_count += 1
            print(f"⚠ Combined clarification attempt {retry_count} failed: {str(e)}")
            
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
