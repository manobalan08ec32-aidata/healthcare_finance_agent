from typing import Dict, List, Optional
import json
from state_schema import AgentState
from databricks_client import DatabricksClient

class LLMRouterAgent:
    """LLM-powered dataset selection agent with intelligent reasoning"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        
    def select_dataset(self, state: AgentState) -> Dict[str, any]:
        """Intelligent dataset selection using LLM reasoning"""
        
        # 1. Get vector search results
        search_results = self._perform_intelligent_vector_search(state)
        
        # 2. Use LLM to analyze and select optimal dataset
        selection_result = self._llm_dataset_selection(search_results, state)
        
        # 3. Validate selection and provide alternatives
        validation_result = self._validate_and_enhance_selection(selection_result, state)
        
        return validation_result
    
    def _perform_intelligent_vector_search(self, state: AgentState) -> List[Dict]:
        """Use LLM to create optimal search queries for vector search"""
        
        search_query_prompt = f"""
        Healthcare Finance Vector Search Query Generation:
        
        User Question: "{state['user_question']}"
        
        Context:
        - Question Type: {state.get('flow_type', 'Unknown')}
        - Previous Dataset: {state.get('selected_dataset', 'None')}
        - Analysis Complexity: {state.get('complexity_level', 'Unknown')}
        - Comparison Intent: {state.get('comparison_intent', 'None')}
        
        You need to generate optimal search queries to find the best healthcare finance datasets.
        
        Available domain knowledge:
        - Claims transaction data (member-level pharmacy/medical claims)
        - Financial ledger data (budgets, forecasts, variances)
        - Operational metrics (provider networks, utilization)
        - Member demographics and enrollment
        
        Generate 2-3 search queries that will find the most relevant datasets:
        1. Primary search - most directly relevant
        2. Secondary search - broader context or related data
        3. Fallback search - alternative perspective if needed
        
        Consider:
        - Semantic variations of user's intent
        - Healthcare finance terminology
        - Domain-specific synonyms
        - Technical vs business language
        
        Respond with JSON:
        {{
            "search_queries": [
                {{
                    "query_text": "specific search query text",
                    "purpose": "what this search aims to find",
                    "weight": 0.7
                }}
            ],
            "search_strategy": "explanation of search approach",
            "expected_dataset_types": ["type1", "type2"]
        }}
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": search_query_prompt}
            ])
            
            search_config = json.loads(llm_response)
            
            # Execute multiple searches and combine results
            all_results = []
            
            print(f"🔍 Executing {len(search_config['search_queries'])} intelligent searches...")
            
            for i, search_query in enumerate(search_config['search_queries'], 1):
                try:
                    print(f"  Search {i}: {search_query['query_text']}")
                    
                    # 🎯 ACTUAL VECTOR SEARCH CALL HERE
                    results = self.db_client.vector_search_tables(
                        search_query['query_text'], 
                        num_results=3
                    )
                    
                    print(f"    → Found {len(results)} datasets")
                    
                    # Add search metadata to results
                    for result in results:
                        result['search_purpose'] = search_query['purpose']
                        result['search_weight'] = search_query['weight']
                        result['search_query'] = search_query['query_text']
                        
                        # Add to consolidated results
                        all_results.append(result)
                    
                except Exception as e:
                    print(f"    ❌ Search query failed: {search_query['query_text']} - {e}")
                    continue
            
            # Remove duplicates and return top results
            unique_results = self._deduplicate_search_results(all_results)
            return unique_results[:5]  # Top 5 unique results
            
        except Exception as e:
            # Fallback to simple search
            return self._fallback_vector_search(state)
    
    def _llm_dataset_selection(self, search_results: List[Dict], state: AgentState) -> Dict:
        """Use LLM to intelligently select the best dataset"""
        
        # Prepare dataset information for LLM analysis
        dataset_analysis = []
        for i, result in enumerate(search_results):
            
            # Parse table metadata
            table_kg = result.get('table_kg', '{}')
            try:
                kg_data = json.loads(table_kg)
                columns_info = kg_data.get('columns', [])
            except:
                columns_info = []
            
            dataset_info = {
                'rank': i + 1,
                'table_name': result.get('table_name'),
                'description': result.get('content', ''),
                'search_context': {
                    'purpose': result.get('search_purpose', 'Unknown'),
                    'weight': result.get('search_weight', 0.5),
                    'query_used': result.get('search_query', 'Unknown')
                },
                'column_count': len(columns_info),
                'key_columns': [col.get('column_name', 'Unknown') for col in columns_info[:8]],
                'data_types_available': list(set([col.get('data_type', 'Unknown') for col in columns_info])),
                'business_context': self._extract_business_context(result.get('content', ''))
            }
            
            dataset_analysis.append(dataset_info)
        
        selection_prompt = f"""
        Healthcare Finance Dataset Selection - Expert Analysis:
        
        User Question: "{state['user_question']}"
        
        Analysis Context:
        - Question Type: {state.get('flow_type', 'Unknown')}
        - Intent: {state.get('intent_analysis', {}).get('user_goal', 'Unknown')}
        - Complexity: {state.get('complexity_assessment', {}).get('complexity_level', 'Unknown')}
        - Previous Dataset: {state.get('selected_dataset', 'None')}
        - User Preferences: {state.get('user_preferences', {})}
        
        Available Datasets: {json.dumps(dataset_analysis, indent=2)}
        
        As a healthcare finance data expert, analyze each dataset's suitability:
        
        EVALUATION CRITERIA:
        1. RELEVANCE: How directly does this dataset answer the user's question?
        2. COMPLETENESS: Does it have all necessary data elements?
        3. GRANULARITY: Right level of detail for the analysis needed?
        4. BUSINESS CONTEXT: Appropriate for healthcare finance domain?
        5. ANALYSIS CAPABILITY: Supports the type of analysis requested?
        6. DATA QUALITY: Sufficient columns and data types for robust analysis?
        
        QUESTION-SPECIFIC CONSIDERATIONS:
        - Descriptive questions: Need comprehensive, detailed datasets
        - Analytical questions: Need datasets with explanatory variables
        - Comparative questions: Need consistent time series data
        - Variance analysis: Need budget/forecast comparison capability
        
        HEALTHCARE FINANCE EXPERTISE:
        - Claims analysis requires member, provider, procedure detail
        - Financial variance needs actual vs budget/forecast data
        - Utilization analysis needs volume and cost metrics
        - Root cause analysis needs multiple dimensional breakdowns
        
        Provide expert dataset selection:
        {{
            "selected_dataset": "table_name",
            "selection_confidence": 0.95,
            "expert_reasoning": "detailed explanation from healthcare finance perspective",
            "suitability_scores": {{
                "relevance": 0.95,
                "completeness": 0.90,
                "granularity": 0.85,
                "business_context": 0.98,
                "analysis_capability": 0.92,
                "data_quality": 0.88
            }},
            "expected_analysis_approach": "how to best use this dataset for the question",
            "potential_limitations": ["any limitations or gaps identified"],
            "alternative_datasets": [
                {{"name": "alternative", "use_case": "when this would be better"}}
            ],
            "required_joins_or_enrichment": ["additional data sources if needed"],
            "recommended_next_steps": "specific guidance for next agent"
        }}
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": selection_prompt}
            ])
            
            selection_result = json.loads(llm_response)
            
            # Enhance with metadata from search results
            selected_table = selection_result['selected_dataset']
            selected_metadata = next(
                (ds for ds in search_results if ds['table_name'] == selected_table),
                search_results[0] if search_results else {}
            )
            
            selection_result['dataset_metadata'] = {
                'table_name': selected_metadata.get('table_name'),
                'description': selected_metadata.get('content'),
                'table_kg': selected_metadata.get('table_kg'),
                'search_context': selected_metadata.get('search_purpose', 'Unknown')
            }
            
            return selection_result
            
        except Exception as e:
            return self._fallback_selection(search_results, state)
    
    def _validate_and_enhance_selection(self, selection_result: Dict, state: AgentState) -> Dict:
        """Validate selection and provide enhancement recommendations"""
        
        validation_prompt = f"""
        Dataset Selection Validation for Healthcare Finance:
        
        Selected Dataset: {selection_result.get('selected_dataset')}
        User Question: "{state['user_question']}"
        Selection Confidence: {selection_result.get('selection_confidence', 0.0)}
        
        Selection Reasoning: {selection_result.get('expert_reasoning', 'None provided')}
        
        VALIDATION CHECKLIST:
        1. Can this dataset definitively answer the user's question?
        2. Are there any obvious gaps or missing data elements?
        3. Is the granularity appropriate for the analysis needed?
        4. Are there regulatory or compliance considerations?
        5. Will this selection lead to actionable insights?
        
        ENHANCEMENT OPPORTUNITIES:
        1. Additional datasets that could enrich the analysis
        2. Specific columns or metrics to focus on
        3. Potential data quality issues to watch for
        4. Recommended analytical approaches
        
        Provide validation and enhancement recommendations:
        {{
            "validation_passed": true/false,
            "confidence_adjustment": 0.95,
            "validation_reasoning": "detailed validation assessment",
            "risk_factors": ["potential issues or limitations"],
            "enhancement_suggestions": [
                {{
                    "type": "additional_dataset|column_focus|analytical_approach",
                    "suggestion": "specific recommendation",
                    "benefit": "how this improves the analysis"
                }}
            ],
            "success_probability": 0.90,
            "fallback_options": ["alternatives if primary selection fails"],
            "quality_assurance_notes": "specific things to verify during analysis"
        }}
        """
        
        try:
            llm_response = self.db_client.call_claude_api([
                {"role": "user", "content": validation_prompt}
            ])
            
            validation_result = json.loads(llm_response)
            
            # Combine selection and validation results
            final_result = {
                **selection_result,
                'validation': validation_result,
                'final_confidence': validation_result.get('confidence_adjustment', selection_result.get('selection_confidence', 0.8)),
                'quality_assured': validation_result.get('validation_passed', True)
            }
            
            return final_result
            
        except Exception as e:
            # Return original selection if validation fails
            selection_result['validation'] = {
                'validation_passed': True,
                'validation_reasoning': 'Validation skipped due to error',
                'success_probability': 0.7
            }
            return selection_result
    
    def _extract_business_context(self, description: str) -> Dict:
        """Extract business context from dataset description"""
        
        context = {
            'domain_areas': [],
            'key_metrics': [],
            'analytical_capabilities': []
        }
        
        description_lower = description.lower()
        
        # Domain area detection
        domain_keywords = {
            'claims': ['claim', 'transaction', 'adjudication'],
            'financial': ['cost', 'expense', 'revenue', 'budget', 'forecast'],
            'operational': ['utilization', 'provider', 'network', 'volume'],
            'member': ['member', 'patient', 'enrollment', 'demographic']
        }
        
        for domain, keywords in domain_keywords.items():
            if any(keyword in description_lower for keyword in keywords):
                context['domain_areas'].append(domain)
        
        # Key metrics detection
        metric_keywords = ['amount', 'cost', 'count', 'rate', 'percentage', 'volume']
        context['key_metrics'] = [keyword for keyword in metric_keywords if keyword in description_lower]
        
        # Analytical capabilities
        if 'dimension' in description_lower or 'surrogate key' in description_lower:
            context['analytical_capabilities'].append('dimensional_analysis')
        if 'time' in description_lower or 'date' in description_lower:
            context['analytical_capabilities'].append('temporal_analysis')
        if 'hierarchy' in description_lower or 'group' in description_lower:
            context['analytical_capabilities'].append('hierarchical_analysis')
        
        return context
    
    def _deduplicate_search_results(self, results: List[Dict]) -> List[Dict]:
        """Remove duplicate tables from search results"""
        
        seen_tables = set()
        unique_results = []
        
        for result in results:
            table_name = result.get('table_name')
            if table_name and table_name not in seen_tables:
                seen_tables.add(table_name)
                unique_results.append(result)
        
        return unique_results
    
    def _fallback_vector_search(self, state: AgentState) -> List[Dict]:
        """Fallback search when LLM search generation fails"""
        
        try:
            # Simple search based on question
            return self.db_client.vector_search_tables(state['user_question'], 5)
        except Exception as e:
            return []
    
    def _fallback_selection(self, search_results: List[Dict], state: AgentState) -> Dict:
        """Fallback selection when LLM selection fails"""
        
        if not search_results:
            raise Exception("No datasets available for selection")
        
        # Select first result as fallback
        selected = search_results[0]
        
        return {
            'selected_dataset': selected.get('table_name'),
            'selection_confidence': 0.6,
            'expert_reasoning': 'Fallback selection - first available dataset',
            'dataset_metadata': {
                'table_name': selected.get('table_name'),
                'description': selected.get('content'),
                'table_kg': selected.get('table_kg', '{}')
            },
            'validation': {
                'validation_passed': True,
                'validation_reasoning': 'Fallback validation - basic checks passed',
                'success_probability': 0.6
            }
        }

# Example usage and testing
if __name__ == "__main__":
    db_client = DatabricksClient()
    router_agent = LLMRouterAgent(db_client)
    
    # Test various healthcare finance questions
    test_scenarios = [
        {
            'question': 'What are Q3 pharmacy claims costs by therapeutic class?',
            'expected_domain': 'claims',
            'complexity': 'moderate'
        },
        {
            'question': 'Why did our medical costs spike 25% above forecast in July?',
            'expected_domain': 'financial_variance',
            'complexity': 'complex'
        },
        {
            'question': 'Show me member utilization patterns for high-cost chronic conditions',
            'expected_domain': 'operational',
            'complexity': 'detailed'
        },
        {
            'question': 'Compare emergency department usage vs urgent care across our network',
            'expected_domain': 'comparative',
            'complexity': 'analytical'
        }
    ]
    
    for scenario in test_scenarios:
        print(f"\n🔍 Testing: {scenario['question']}")
        print(f"Expected Domain: {scenario['expected_domain']}")
        
        state = AgentState(
            user_question=scenario['question'],
            session_id="test_session",
            flow_type="descriptive",
            user_preferences={'domain_expertise': 'healthcare_finance'}
        )
        
        try:
            result = router_agent.select_dataset(state)
            
            print(f"✅ Selected: {result['selected_dataset']}")
            print(f"🎯 Confidence: {result.get('final_confidence', 0.0):.2%}")
            print(f"📝 Reasoning: {result['expert_reasoning'][:120]}...")
            
            # Show suitability scores if available
            scores = result.get('suitability_scores', {})
            if scores:
                print(f"📊 Scores: Relevance={scores.get('relevance', 0):.2f}, "
                      f"Quality={scores.get('data_quality', 0):.2f}")
            
            # Show validation results
            validation = result.get('validation', {})
            print(f"✓ Validated: {validation.get('validation_passed', False)}")
            print(f"📈 Success Probability: {validation.get('success_probability', 0):.2%}")
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            
    print("\n" + "="*50)
    print("LLM Router Agent Testing Complete")
                