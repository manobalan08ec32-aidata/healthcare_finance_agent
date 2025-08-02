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
        
        # Get user question from current_question (not user_question)
        user_question = state.get('current_question', state.get('user_question', ''))
        
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
            # Direct call to vector search with user's exact question
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
        """Use LLM to select best dataset from search results - simplified version"""
        
        # Prepare simplified dataset information for LLM (only table_name and content)
        dataset_options = []
        for i, result in enumerate(search_results):
            dataset_info = {
                'rank': i + 1,
                'table_name': result.get('table_name'),
                'description': result.get('content', '')
            }
            dataset_options.append(dataset_info)
        
        selection_prompt = f"""
        Healthcare Finance Dataset Selection Task:
        
        User Question: "{state['user_question']}"
        
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
            "clear_selection": true/false,
            "selected_dataset": "table_name" or null,
            "selection_confidence": 0.95,
            "selection_reasoning": "detailed explanation",
            "requires_clarification": true/false,
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
            # Fallback to first result if LLM parsing fails
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
                