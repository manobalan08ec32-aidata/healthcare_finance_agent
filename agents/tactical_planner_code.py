import json
import time
from typing import Dict, List, Optional, Any
import re

class TacticalPlanner:
    """Tactical Investigation Planner for comprehensive drill-through analysis"""
    
    def __init__(self, databricks_client, config_path: str = "configs/strategy_node/revenue.json"):
        self.db_client = databricks_client
        self.max_retries = 3
        self.config_cache = {}
        # Load the tactical config that matches the investigation type
        self.tactical_config_path = "configs/tactical_planner_config.json"
        self.tactical_config = self._load_tactical_config()
    
    def _load_tactical_config(self) -> Dict:
        """Load tactical planner configuration"""
        try:
            with open(self.tactical_config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Failed to load tactical config: {str(e)}")
    
    def execute_tactical_investigation(self, state: Dict) -> Dict[str, Any]:
        """Main tactical planner execution with user approval workflow"""
        
        print(f"\n🔍 Tactical Planner: Starting tactical investigation")
        
        try:
            # 1. Extract context from state
            context = self._extract_context(state)
            
            # 2. Generate investigation menu using LLM (combines strategic analysis + planning)
            investigation_menu_result = self._generate_investigation_menu_with_llm(context)
            
            if not investigation_menu_result['success']:
                return {
                    'success': False,
                    'error': investigation_menu_result['error']
                }
            
            # 3. Present investigation menu to user for approval
            user_approval_result = self._present_investigation_menu_for_approval(
                investigation_menu_result['investigation_menu'],
                investigation_menu_result['recommended_investigations']
            )
            
            if not user_approval_result['approved']:
                return {
                    'success': True,
                    'pending_user_approval': True,
                    'investigation_menu': investigation_menu_result['investigation_menu'],
                    'awaiting_user_selection': True
                }
            
            # 4. Execute approved investigations
            execution_result = self._execute_approved_investigations(
                user_approval_result['selected_investigations'],
                context
            )
            
            if not execution_result['success']:
                return {
                    'success': False,
                    'error': execution_result['error']
                }
            
            # 5. Synthesize findings into comprehensive report
            synthesis_result = self._synthesize_tactical_findings_with_llm(
                execution_result['investigation_results'],
                context
            )
            
            if not synthesis_result['success']:
                return {
                    'success': False,
                    'error': synthesis_result['error']
                }
            
            return {
                'success': True,
                'tactical_investigation_menu': investigation_menu_result['investigation_menu'],
                'executed_investigations': execution_result['investigation_results'],
                'tactical_findings_report': synthesis_result['findings_report'],
                'investigation_summary': execution_result['investigation_summary'],
                'total_queries_executed': execution_result['total_queries']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Tactical investigation failed: {str(e)}"
            }
    
    def _extract_context(self, state: Dict) -> Dict:
        """Extract context from state for tactical analysis"""
        return {
            'current_question': state.get('rewritten_question', ''),
            'strategic_intelligence': state.get('strategic_intelligence', ''),
            'strategic_results': state.get('strategic_results', []),
            'investigation_type': state.get('investigation_type', 'revenue_investigation_config'),
            'dataset_metadata': state.get('dataset_metadata', {})
        }
    
    def _generate_investigation_menu_with_llm(self, context: Dict) -> Dict[str, Any]:
        """Use LLM to analyze strategic findings and generate tactical investigation menu"""
        
        strategic_intelligence = context['strategic_intelligence']
        current_question = context['current_question']
        tactical_config = self.tactical_config['tactical_planner_config']
        
        investigation_menu_prompt = f"""
        You are a Tactical Investigation Analyst. Analyze the strategic findings and create a comprehensive tactical investigation menu.

        USER QUESTION: {current_question}

        STRATEGIC INTELLIGENCE REPORT:
        {strategic_intelligence}

        TACTICAL INVESTIGATION FRAMEWORK:
        {json.dumps(tactical_config, indent=2)}

        TACTICAL INVESTIGATION REQUIREMENTS:

        1. ANALYZE STRATEGIC FINDINGS:
        - Extract key findings, variance drivers, and business impacts from strategic report
        - Identify specific entities (clients, LOBs, therapy classes) mentioned
        - Determine investigation priorities based on impact magnitude

        2. GENERATE INVESTIGATION MENU:
        Based on strategic findings and business intelligence patterns, create investigation recommendations across three layers:

        LAYER 1 - GUIDED INVESTIGATIONS (40%):
        - Map strategic findings to business intelligence patterns in the config
        - Recommend investigations that validate and deepen strategic insights
        - Focus on highest impact areas identified in strategic analysis

        LAYER 2 - SYSTEMATIC EXPLORATION (40%):
        - Recommend comprehensive dimensional analysis across ALL table attributes
        - Identify dimensions not covered in strategic analysis that might reveal insights
        - Suggest correlation discovery across multiple business dimensions

        LAYER 3 - ANOMALY DISCOVERY (20%):
        - Recommend outlier and anomaly detection investigations
        - Suggest novel correlation discovery beyond standard patterns
        - Identify potential hidden gems in the data

        3. USER QUESTION CONTEXT:
        - Extract time periods, comparison baselines from original user question
        - Determine geographic, demographic, or business segment scope
        - Apply user context to all investigation recommendations

        4. INVESTIGATION MENU FORMAT:
        Create a user-friendly investigation menu showing:
        - Recommended investigations with clear rationale
        - Expected insights and business value
        - Available metrics and dimensions for each investigation
        - Investigation priority and complexity levels

        OUTPUT REQUIREMENTS:
        Generate a comprehensive investigation menu in this JSON format:

        {{
            "investigation_menu": {{
                "strategic_context_summary": "Brief summary of strategic findings",
                "recommended_investigations": [
                    {{
                        "investigation_id": 1,
                        "investigation_name": "Client Portfolio Deep-Dive",
                        "layer": "guided_investigation",
                        "priority": "high",
                        "rationale": "Strategic findings show $1.6M client churn impact",
                        "expected_insights": "Timeline and therapy impact of client departures",
                        "key_dimensions": ["client_name", "submit_date", "therapy_class_name"],
                        "key_metrics": ["revenue_amt", "unadjusted_script_count"],
                        "estimated_queries": 2,
                        "business_value": "Validate client departure timeline and identify retention opportunities"
                    }}
                ],
                "systematic_explorations": [
                    {{
                        "exploration_id": 1,
                        "exploration_name": "Comprehensive Dimensional Correlation Analysis",
                        "purpose": "Explore all table dimensions for hidden correlations",
                        "dimensions_to_explore": ["ALL available dimensions"],
                        "correlation_approach": "cross_dimensional_pattern_recognition"
                    }}
                ],
                "anomaly_investigations": [
                    {{
                        "anomaly_id": 1,
                        "anomaly_name": "Statistical Outlier Detection",
                        "purpose": "Identify anomalies and unusual patterns for insight discovery"
                    }}
                ]
            }},
            "user_approval_options": [
                "Execute all recommended investigations",
                "Select specific investigations by layer",
                "Choose individual investigations",
                "Modify investigation scope"
            ],
            "total_estimated_queries": "estimated_total_query_count"
        }}

        CRITICAL INSTRUCTIONS:
        1. Base ALL recommendations on the strategic findings provided
        2. Use business intelligence patterns from config as guidance, not constraints
        3. Ensure comprehensive coverage of ALL table dimensions
        4. Extract time periods and scope from user question context
        5. Provide clear business rationale for each investigation
        6. Balance guided investigation with open exploration for hidden gems

        Return ONLY the JSON format above. No other text or formatting.
        """
        
        for attempt in range(self.max_retries):
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": investigation_menu_prompt}
                ])
                
                # Parse JSON response
                investigation_menu = json.loads(llm_response.strip())
                
                if not investigation_menu or 'investigation_menu' not in investigation_menu:
                    raise ValueError("Invalid investigation menu format")
                
                return {
                    'success': True,
                    'investigation_menu': investigation_menu['investigation_menu'],
                    'recommended_investigations': investigation_menu['investigation_menu'].get('recommended_investigations', []),
                    'user_approval_options': investigation_menu.get('user_approval_options', [])
                }
                
            except Exception as e:
                print(f"❌ Investigation menu generation attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return {
            'success': False,
            'error': f"Investigation menu generation failed after {self.max_retries} attempts"
        }
    
    def _present_investigation_menu_for_approval(self, investigation_menu: Dict, recommended_investigations: List[Dict]) -> Dict[str, Any]:
        """Present investigation menu to user for approval (placeholder for user interaction)"""
        
        # For now, auto-approve all recommended investigations
        # In your actual implementation, this would present the menu to user and get their selection
        
        print(f"\n📋 Tactical Investigation Menu Generated:")
        print(f"Strategic Context: {investigation_menu.get('strategic_context_summary', 'N/A')}")
        print(f"Recommended Investigations: {len(recommended_investigations)}")
        
        for investigation in recommended_investigations:
            print(f"  - {investigation['investigation_name']} ({investigation['priority']} priority)")
            print(f"    Rationale: {investigation['rationale']}")
        
        # Auto-approve for now (replace with actual user interaction)
        return {
            'approved': True,
            'selected_investigations': recommended_investigations,
            'approval_type': 'all_recommended'
        }
    
    def _execute_approved_investigations(self, selected_investigations: List[Dict], context: Dict) -> Dict[str, Any]:
        """Execute the approved tactical investigations"""
        
        try:
            investigation_results = []
            total_queries = 0
            
            for investigation in selected_investigations:
                print(f"  🔍 Executing {investigation['investigation_name']}")
                
                investigation_result = self._execute_single_investigation(investigation, context)
                
                if not investigation_result['success']:
                    print(f"  ❌ Investigation {investigation['investigation_name']} failed: {investigation_result['error']}")
                    continue
                
                investigation_results.append({
                    'investigation_name': investigation['investigation_name'],
                    'investigation_purpose': investigation.get('rationale', ''),
                    'sql_queries': investigation_result['sql_queries'],
                    'results': investigation_result['results'],
                    'insights': investigation_result.get('insights', [])
                })
                
                total_queries += len(investigation_result['sql_queries'])
            
            return {
                'success': True,
                'investigation_results': investigation_results,
                'total_queries': total_queries,
                'investigation_summary': f"Executed {len(investigation_results)} investigations with {total_queries} total queries"
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Investigation execution failed: {str(e)}"
            }
    
    def _execute_single_investigation(self, investigation: Dict, context: Dict) -> Dict[str, Any]:
        """Execute a single tactical investigation using LLM-generated SQL"""
        
        current_question = context['current_question']
        strategic_intelligence = context['strategic_intelligence']
        tactical_config = self.tactical_config['tactical_planner_config']
        
        investigation_sql_prompt = f"""
        You are a Tactical SQL Analyst. Generate SQL queries for this specific tactical investigation.

        USER QUESTION: {current_question}

        STRATEGIC INTELLIGENCE CONTEXT:
        {strategic_intelligence}

        INVESTIGATION DETAILS:
        Investigation Name: {investigation['investigation_name']}
        Purpose: {investigation.get('rationale', '')}
        Expected Insights: {investigation.get('expected_insights', '')}
        Key Dimensions: {investigation.get('key_dimensions', [])}
        Key Metrics: {investigation.get('key_metrics', [])}
        Layer: {investigation.get('layer', '')}

        TACTICAL CONFIGURATION:
        Table Metadata: {json.dumps(tactical_config['claims_drivers_table_metadata'], indent=2)}
        Business Intelligence Patterns: {json.dumps(tactical_config['business_intelligence_guidance'], indent=2)}

        SQL GENERATION REQUIREMENTS:

        1. INVESTIGATION-SPECIFIC SQL:
        - Generate 1-2 SQL queries that address this specific investigation
        - Use the key dimensions and metrics specified for this investigation
        - Apply user question context for time periods and comparison baselines
        - Focus on the business rationale and expected insights

        2. COMPREHENSIVE ANALYSIS:
        - Include all relevant dimensions from the investigation specification
        - Calculate appropriate metrics and comparisons
        - Apply filters based on strategic findings (specific clients, LOBs, etc.)
        - Use proper SQL best practices (UPPER for text filtering, meaningful column names)

        3. BUSINESS CONTEXT APPLICATION:
        - Extract time periods from user question for comparison analysis
        - Apply any entity filters mentioned in strategic findings
        - Focus on significant variances and business impacts
        - Include statistical measures where appropriate

        TABLE DETAILS:
        Table Name: {tactical_config['claims_drivers_table_metadata']['table_name']}
        Available Columns: {[col['name'] for col in tactical_config['claims_drivers_table_metadata']['correlation_rich_columns']]}

        OUTPUT FORMAT:
        Return a JSON with the SQL queries for this investigation:

        {{
            "investigation_queries": [
                {{
                    "query_purpose": "Description of what this query investigates",
                    "sql_query": "Complete SQL query",
                    "expected_output": "Description of expected results"
                }}
            ]
        }}

        CRITICAL REQUIREMENTS:
        1. Generate practical, executable SQL queries
        2. Use exact table name and column names from metadata
        3. Apply user question time context appropriately
        4. Focus on the specific investigation purpose
        5. Include proper GROUP BY, ORDER BY, and filtering

        Return ONLY the JSON format above. No other text or formatting.
        """
        
        try:
            # Generate SQL using LLM
            sql_generation_result = self._generate_sql_with_llm(investigation_sql_prompt, investigation['investigation_name'])
            
            if not sql_generation_result['success']:
                return {
                    'success': False,
                    'error': f"SQL generation failed: {sql_generation_result['error']}"
                }
            
            # Execute the generated queries
            execution_results = []
            executed_queries = []
            
            for query_info in sql_generation_result['queries']:
                sql_query = query_info['sql_query']
                query_purpose = query_info['query_purpose']
                
                execution_result = self._execute_databricks_sql(sql_query)
                
                if execution_result['success']:
                    execution_results.append({
                        'query_purpose': query_purpose,
                        'data': execution_result['data'],
                        'row_count': len(execution_result['data']) if execution_result['data'] else 0
                    })
                    executed_queries.append(sql_query)
                else:
                    print(f"  ⚠️ Query failed: {execution_result['error']}")
            
            return {
                'success': True,
                'sql_queries': executed_queries,
                'results': execution_results,
                'query_count': len(executed_queries)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Investigation execution failed: {str(e)}"
            }
    
    def _generate_sql_with_llm(self, sql_prompt: str, investigation_name: str) -> Dict[str, Any]:
        """Generate SQL queries using LLM"""
        
        for attempt in range(self.max_retries):
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": sql_prompt}
                ])
                
                # Parse JSON response
                sql_response = json.loads(llm_response.strip())
                
                if not sql_response or 'investigation_queries' not in sql_response:
                    raise ValueError("Invalid SQL response format")
                
                queries = sql_response['investigation_queries']
                if not queries or len(queries) == 0:
                    raise ValueError("No SQL queries generated")
                
                return {
                    'success': True,
                    'queries': queries
                }
                
            except Exception as e:
                print(f"❌ SQL generation for {investigation_name} attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return {
            'success': False,
            'error': f"SQL generation for {investigation_name} failed after {self.max_retries} attempts"
        }
    
    def _execute_databricks_sql(self, sql_query: str) -> Dict:
        """Execute SQL against Databricks - placeholder for your existing logic"""
        try:
            # This should use your existing SQLGeneratorAgent._execute_databricks_sql logic
            # Placeholder implementation - replace with your actual databricks execution
            result = self.db_client.execute_sql(sql_query)
            return {
                'success': True,
                'data': result.get('data', []),
                'execution_time': result.get('execution_time', 0)
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _synthesize_tactical_findings_with_llm(self, investigation_results: List[Dict], context: Dict) -> Dict[str, Any]:
        """Use LLM to synthesize all tactical investigation results into comprehensive findings report"""
        
        current_question = context['current_question']
        strategic_intelligence = context['strategic_intelligence']
        
        synthesis_prompt = f"""
        You are a Tactical Intelligence Analyst. Synthesize all tactical investigation results into a comprehensive findings report.

        USER QUESTION: {current_question}

        STRATEGIC INTELLIGENCE CONTEXT:
        {strategic_intelligence}

        TACTICAL INVESTIGATION RESULTS:
        {json.dumps(investigation_results, indent=2)}

        TACTICAL FINDINGS SYNTHESIS REQUIREMENTS:

        Create a comprehensive tactical findings report with the following structure:

        # Tactical Investigation Results

        ## Investigation Summary and Approach
        - Brief overview of investigations conducted
        - Connection to strategic findings
        - Investigation methodology and scope

        ## Business Intelligence Pattern Findings
        - Findings from guided investigations based on business patterns
        - Validation of strategic hypotheses
        - Key business correlations discovered

        ## Systematic Exploration Discoveries
        - Insights from comprehensive dimensional analysis
        - Hidden correlations across multiple business dimensions
        - Unexpected patterns not covered in guided investigations

        ## Hidden Gems and Novel Insights
        - Counter-intuitive findings and business anomalies
        - Opportunities or risks not apparent from strategic analysis
        - Statistical significance of discovered patterns

        ## Cross-Validation and Consistency Analysis
        - Consistency between tactical findings and strategic analysis
        - Validation of strategic hypotheses with operational data
        - Any contradictions or refinements to strategic conclusions

        ## Key Business Insights and Implications
        - Most significant tactical discoveries
        - Business impact quantification
        - Actionable insights for decision making

        ## Recommendations and Next Steps
        - Specific business actions based on tactical findings
        - Areas requiring further investigation
        - Risk mitigation or opportunity capture recommendations

        ANALYSIS INSTRUCTIONS:
        1. Use EXACT values, names, and amounts from investigation results
        2. Identify patterns and correlations across different investigations
        3. Highlight counter-intuitive or unexpected findings
        4. Provide statistical context for significance of findings
        5. Connect tactical findings back to strategic analysis
        6. Quantify business impacts with specific dollar amounts and percentages
        7. Focus on actionable insights that inform business decisions
        8. Identify hidden gems that provide competitive advantage

        CRITICAL REQUIREMENTS:
        - Synthesize findings across ALL investigation results
        - Highlight most significant business insights
        - Provide clear connection between tactical findings and business value
        - Include specific, actionable recommendations
        - Maintain focus on hidden gems and novel discoveries

        Return ONLY the comprehensive markdown report. No other text or formatting.
        """
        
        for attempt in range(self.max_retries):
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": synthesis_prompt}
                ])
                
                if not llm_response or len(llm_response.strip()) < 100:
                    raise ValueError("Empty or insufficient tactical findings report")
                
                return {
                    'success': True,
                    'findings_report': llm_response.strip()
                }
                
            except Exception as e:
                print(f"❌ Tactical findings synthesis attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return {
            'success': False,
            'error': f"Tactical findings synthesis failed after {self.max_retries} attempts"
        }
