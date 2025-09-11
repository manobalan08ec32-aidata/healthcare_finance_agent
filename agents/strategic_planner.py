import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import re

class StrategicPlanner:
    """Strategic Investigation Planner for drill-through analysis"""
    
    def __init__(self, databricks_client, config_path: str = "configs/strategic_planner_config.json"):
        self.db_client = databricks_client
        self.config = self._load_config(config_path)
        self.max_retries = 3
    
    def _load_config(self, config_path: str) -> Dict:
        """Load strategic planner configuration"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Failed to load strategic config: {str(e)}")
    
    def execute_strategic_investigation(self, state: Dict) -> Dict[str, Any]:
        """Main strategic planner execution - 3 strategic queries + intelligence synthesis"""
        
        print(f"\n🎯 Strategic Planner: Starting investigation")
        
        try:
            # 1. Extract context from state
            context = self._extract_context(state)
            
            # 2. Classify question and determine investigation pattern  
            classification = self._classify_question(context['current_question'])
            
            # 3. Execute 3 strategic queries
            strategic_results = self._execute_strategic_queries(context, classification)
            
            if not strategic_results['success']:
                return {
                    'success': False,
                    'error': strategic_results['error']
                }
            
            # 4. Synthesize intelligence and create markdown report
            intelligence_result = self._synthesize_strategic_intelligence(
                strategic_results['query_results'],
                context,
                classification
            )
            
            if not intelligence_result['success']:
                return {
                    'success': False, 
                    'error': intelligence_result['error']
                }
            
            return {
                'success': True,
                'strategic_queries': strategic_results['queries'],
                'strategic_results': strategic_results['query_results'],
                'strategic_intelligence': intelligence_result['markdown_report'],
                'tactical_direction': intelligence_result['tactical_direction'],
                'investigation_classification': classification
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Strategic investigation failed: {str(e)}"
            }
    
    def _extract_context(self, state: Dict) -> Dict:
        """Extract context from state for strategic analysis"""
        return {
            'current_question': state.get('rewritten_question', ''),
            'dataset_metadata': state.get('dataset_metadata', {}),
            'selected_dataset': state.get('selected_dataset', []),
            'questions_history': state.get('questions_sql_history', [])
        }
    
    def _classify_question(self, question: str) -> Dict:
        """Classify question type and determine investigation pattern"""
        
        question_lower = question.lower()
        config = self.config['strategic_planner_config']
        
        # Check for revenue variance patterns
        revenue_patterns = config['question_classification']['revenue_variance']
        
        for keyword_group in revenue_patterns['trigger_keywords']:
            if all(any(keyword in question_lower for keyword in kw.split('|')) 
                   for kw in keyword_group):
                return {
                    'type': 'revenue_variance_investigation',
                    'pattern_id': revenue_patterns['pattern_id'],
                    'complexity': 'high',
                    'expected_queries': 3
                }
        
        # Default classification
        return {
            'type': 'general_investigation', 
            'pattern_id': 'revenue_variance_investigation',  # fallback
            'complexity': 'medium',
            'expected_queries': 3
        }
    
    def _execute_strategic_queries(self, context: Dict, classification: Dict) -> Dict[str, Any]:
        """Execute the 3 strategic queries in sequence"""
        
        try:
            query_results = []
            executed_queries = []
            
            # Query 1: Revenue Variance Detection
            print("  📊 Executing Strategic Query 1: Revenue Variance Detection")
            query_1_result = self._execute_revenue_variance_query(context)
            
            if not query_1_result['success']:
                return {
                    'success': False,
                    'error': f"Strategic Query 1 failed: {query_1_result['error']}"
                }
            
            query_results.append({
                'query_name': 'revenue_variance_detection',
                'purpose': 'Identify primary revenue variance areas',
                'sql_query': query_1_result['sql_query'],
                'data': query_1_result['data'],
                'row_count': len(query_1_result['data']) if query_1_result['data'] else 0
            })
            executed_queries.append(query_1_result['sql_query'])
            
            # Query 2: Client/Membership Analysis  
            print("  📈 Executing Strategic Query 2: Client/Membership Analysis")
            query_2_result = self._execute_client_membership_query(context, query_1_result['data'])
            
            if not query_2_result['success']:
                return {
                    'success': False,
                    'error': f"Strategic Query 2 failed: {query_2_result['error']}"
                }
            
            query_results.append({
                'query_name': 'client_membership_analysis',
                'purpose': 'Understand client portfolio and membership changes', 
                'sql_query': query_2_result['sql_query'],
                'data': query_2_result['data'],
                'row_count': len(query_2_result['data']) if query_2_result['data'] else 0
            })
            executed_queries.append(query_2_result['sql_query'])
            
            # Query 3: Carrier Analysis
            print("  🏢 Executing Strategic Query 3: Carrier Analysis")
            query_3_result = self._execute_carrier_analysis_query(context, query_1_result['data'])
            
            if not query_3_result['success']:
                return {
                    'success': False,
                    'error': f"Strategic Query 3 failed: {query_3_result['error']}"
                }
            
            query_results.append({
                'query_name': 'carrier_analysis',
                'purpose': 'Identify carrier utilization changes',
                'sql_query': query_3_result['sql_query'], 
                'data': query_3_result['data'],
                'row_count': len(query_3_result['data']) if query_3_result['data'] else 0
            })
            executed_queries.append(query_3_result['sql_query'])
            
            return {
                'success': True,
                'query_results': query_results,
                'queries': executed_queries
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Strategic queries execution failed: {str(e)}"
            }
    
    def _execute_revenue_variance_query(self, context: Dict) -> Dict[str, Any]:
        """Generate and execute revenue variance detection query"""
        
        current_question = context['current_question']
        dataset_metadata = context['dataset_metadata']
        
        # Extract time periods from question
        time_periods = self._extract_time_periods(current_question)
        
        sql_prompt = f"""
        You are a Strategic Revenue Analyst. Generate a HIGH-LEVEL revenue variance query for strategic analysis.

        USER QUESTION: {current_question}
        TABLE METADATA: {dataset_metadata}
        TIME PERIODS DETECTED: {time_periods}

        STRATEGIC QUERY 1 REQUIREMENTS:
        - Compare revenue variance by line_of_business and product_category
        - Use ledger_overview table (metric_type = 'Revenues', ledger = 'GAAP')
        - Include both absolute and percentage variance
        - Focus on {time_periods.get('comparison', 'quarter over quarter')} comparison
        - Show variance that exceeds $250K absolute impact

        SQL GENERATION RULES:
        1. Use UPPER() for case-insensitive text filtering
        2. Include all grouping dimensions in SELECT
        3. Calculate variance as (current - previous) and percentage change
        4. Order by absolute variance impact (descending)
        5. Include meaningful column names

        Return ONLY the SQL query in XML tags:
        <sql>
        [Your strategic revenue variance SQL here]
        </sql>
        """
        
        return self._execute_sql_with_llm(sql_prompt, "revenue_variance_detection")
    
    def _execute_client_membership_query(self, context: Dict, revenue_data: List[Dict]) -> Dict[str, Any]:
        """Generate and execute client/membership analysis query"""
        
        current_question = context['current_question']
        dataset_metadata = context['dataset_metadata']
        
        # Analyze revenue data to focus on significant LOBs
        significant_lobs = self._identify_significant_lobs(revenue_data)
        
        sql_prompt = f"""
        You are a Strategic Client Analyst. Generate a client portfolio and membership analysis query.

        USER QUESTION: {current_question}
        TABLE METADATA: {dataset_metadata}
        REVENUE VARIANCE FINDINGS: LOBs with significant impact: {significant_lobs}

        STRATEGIC QUERY 2 REQUIREMENTS:
        - Analyze client-level revenue changes for significant LOBs: {significant_lobs}
        - Use claims_drivers table
        - Compare current vs previous period client revenue
        - Identify client churn (clients with zero current revenue)
        - Identify new clients (clients with zero previous revenue)
        - Focus on changes >$100K impact

        SQL GENERATION RULES:
        1. Include line_of_business, client_name in grouping
        2. Calculate current_revenue, previous_revenue, variance
        3. Identify client_status (Lost, New, Existing)
        4. Order by absolute variance impact
        5. Filter for significant changes only

        Return ONLY the SQL query in XML tags:
        <sql>
        [Your strategic client/membership analysis SQL here]
        </sql>
        """
        
        return self._execute_sql_with_llm(sql_prompt, "client_membership_analysis")
    
    def _execute_carrier_analysis_query(self, context: Dict, revenue_data: List[Dict]) -> Dict[str, Any]:
        """Generate and execute carrier utilization analysis query"""
        
        current_question = context['current_question']
        dataset_metadata = context['dataset_metadata']
        
        significant_lobs = self._identify_significant_lobs(revenue_data)
        
        sql_prompt = f"""
        You are a Strategic Carrier Analyst. Generate a carrier utilization analysis query.

        USER QUESTION: {current_question}
        TABLE METADATA: {dataset_metadata}
        FOCUS LOBS: {significant_lobs}

        STRATEGIC QUERY 3 REQUIREMENTS:
        - Analyze carrier utilization changes for LOBs: {significant_lobs}
        - Use claims_drivers table
        - Compare carrier revenue and script volume between periods
        - Identify carriers with significant utilization changes
        - Focus on changes that could explain revenue variance

        SQL GENERATION RULES:
        1. Include carrier_id, line_of_business in grouping
        2. Calculate revenue and script count changes
        3. Calculate utilization changes (revenue per script)
        4. Filter for meaningful carrier changes
        5. Order by impact significance

        Return ONLY the SQL query in XML tags:
        <sql>
        [Your strategic carrier analysis SQL here]
        </sql>
        """
        
        return self._execute_sql_with_llm(sql_prompt, "carrier_analysis")
    
    def _execute_sql_with_llm(self, sql_prompt: str, query_type: str) -> Dict[str, Any]:
        """Generate SQL with LLM and execute with retry logic"""
        
        for attempt in range(self.max_retries):
            try:
                # Generate SQL
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": sql_prompt}
                ])
                
                # Extract SQL from XML tags
                sql_match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if not sql_match:
                    raise ValueError("No SQL found in XML tags")
                
                sql_query = sql_match.group(1).strip().replace('`', '')
                
                if not sql_query:
                    raise ValueError("Empty SQL query")
                
                # Execute SQL
                execution_result = self._execute_databricks_sql(sql_query)
                
                if execution_result['success']:
                    return {
                        'success': True,
                        'sql_query': sql_query,
                        'data': execution_result['data'],
                        'execution_time': execution_result.get('execution_time', 0)
                    }
                else:
                    # Try to fix SQL if execution failed
                    if attempt < self.max_retries - 1:
                        fix_result = self._fix_sql_with_llm(sql_query, execution_result['error'])
                        if fix_result['success']:
                            sql_query = fix_result['fixed_sql']
                            execution_result = self._execute_databricks_sql(sql_query)
                            if execution_result['success']:
                                return {
                                    'success': True,
                                    'sql_query': sql_query,
                                    'data': execution_result['data'],
                                    'execution_time': execution_result.get('execution_time', 0)
                                }
                    
                    raise Exception(f"SQL execution failed: {execution_result['error']}")
                    
            except Exception as e:
                print(f"❌ {query_type} attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return {
            'success': False,
            'error': f"{query_type} failed after {self.max_retries} attempts"
        }
    
    def _execute_databricks_sql(self, sql_query: str) -> Dict:
        """Execute SQL against Databricks - reuse your existing SQL execution logic"""
        # This would use the same logic as your SQLGeneratorAgent._execute_databricks_sql
        # For now, placeholder that follows your pattern
        try:
            # Use your existing databricks execution logic here
            # This is just a placeholder showing the expected interface
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
    
    def _fix_sql_with_llm(self, failed_sql: str, error_msg: str) -> Dict[str, Any]:
        """Fix SQL using LLM - reuse your existing fix logic"""
        # This would use the same logic as your SQLGeneratorAgent._fix_sql_with_llm
        # Placeholder for now
        return {'success': False, 'error': 'SQL fix not implemented'}
    
    def _identify_significant_lobs(self, revenue_data: List[Dict]) -> List[str]:
        """Identify LOBs with significant variance for focused analysis"""
        if not revenue_data:
            return ["C&S", "E&I", "M&R"]  # default
        
        significant_lobs = []
        threshold = 250000  # $250K threshold from config
        
        for row in revenue_data:
            # Look for variance columns (could be various names)
            variance_value = 0
            for key, value in row.items():
                if 'variance' in key.lower() and isinstance(value, (int, float)):
                    variance_value = abs(value)
                    break
            
            if variance_value >= threshold and 'line_of_business' in row:
                lob = row['line_of_business']
                if lob not in significant_lobs:
                    significant_lobs.append(lob)
        
        return significant_lobs if significant_lobs else ["C&S", "E&I", "M&R"]
    
    def _extract_time_periods(self, question: str) -> Dict:
        """Extract time period comparison from question"""
        question_lower = question.lower()
        
        if any(term in question_lower for term in ['q3', 'q2', 'q1', 'q4', 'quarter']):
            return {'comparison': 'quarter over quarter', 'period_type': 'quarterly'}
        elif any(term in question_lower for term in ['month', 'monthly', 'mom']):
            return {'comparison': 'month over month', 'period_type': 'monthly'}
        elif any(term in question_lower for term in ['year', 'annual', 'yoy']):
            return {'comparison': 'year over year', 'period_type': 'yearly'}
        else:
            return {'comparison': 'quarter over quarter', 'period_type': 'quarterly'}  # default
    
    def _synthesize_strategic_intelligence(self, query_results: List[Dict], context: Dict, classification: Dict) -> Dict[str, Any]:
        """Synthesize all strategic query results into markdown intelligence report"""
        
        current_question = context['current_question']
        
        synthesis_prompt = f"""
        You are a Strategic Intelligence Analyst. Create a comprehensive markdown intelligence report.

        USER QUESTION: {current_question}

        STRATEGIC QUERY RESULTS:
        
        Query 1 - Revenue Variance Detection:
        Data: {query_results[0]['data'] if query_results else []}
        
        Query 2 - Client/Membership Analysis:
        Data: {query_results[1]['data'] if len(query_results) > 1 else []}
        
        Query 3 - Carrier Analysis:
        Data: {query_results[2]['data'] if len(query_results) > 2 else []}

        INTELLIGENCE SYNTHESIS REQUIREMENTS:

        Create a markdown report with these sections:

        # Strategic Intelligence Report

        ## Executive Summary
        - Total variance quantification
        - Primary driver identification  
        - Confidence level assessment

        ## Detailed Findings

        ### Revenue Variance Analysis
        - LOB-level variance breakdown
        - Product category impact
        - Absolute and percentage impacts

        ### Client Portfolio Changes
        - Client churn analysis
        - New client acquisition
        - Revenue impact by client changes

        ### Membership Impact
        - Membership changes by LOB
        - Correlation with revenue variance

        ### Carrier Analysis
        - Carrier utilization changes
        - Impact on overall variance

        ## Tactical Investigation Plan

        ### Primary Focus: [Area with highest impact]
        - Rationale: [Why this area is primary]
        - Targets: [Specific investigation targets]

        ### Secondary Focus: [Second priority area]
        - Rationale: [Why this needs investigation]
        - Targets: [Specific analysis needed]

        ### Validation Requirements
        - Cross-table validation needs
        - Specific confirmation requirements

        FORMATTING RULES:
        - Use exact data values and names from results
        - Quantify all impacts with dollar amounts and percentages
        - Be specific about investigation targets
        - Keep tactical direction actionable and focused

        Return ONLY the markdown report. No other text or formatting.
        """
        
        for attempt in range(self.max_retries):
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": synthesis_prompt}
                ])
                
                if not llm_response or len(llm_response.strip()) < 100:
                    raise ValueError("Empty or insufficient intelligence report")
                
                # Extract tactical direction for handoff
                tactical_direction = self._extract_tactical_direction(llm_response)
                
                return {
                    'success': True,
                    'markdown_report': llm_response.strip(),
                    'tactical_direction': tactical_direction
                }
                
            except Exception as e:
                print(f"❌ Intelligence synthesis attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return {
            'success': False,
            'error': f"Intelligence synthesis failed after {self.max_retries} attempts"
        }
    
    def _extract_tactical_direction(self, markdown_report: str) -> Dict:
        """Extract tactical direction from markdown report for next planner"""
        
        # Extract primary and secondary focus areas
        primary_focus = ""
        secondary_focus = ""
        validation_requirements = ""
        
        lines = markdown_report.split('\n')
        current_section = ""
        
        for line in lines:
            line = line.strip()
            if 'Primary Focus:' in line:
                primary_focus = line.replace('### Primary Focus:', '').strip()
                current_section = "primary"
            elif 'Secondary Focus:' in line:
                secondary_focus = line.replace('### Secondary Focus:', '').strip()
                current_section = "secondary"
            elif 'Validation Requirements' in line:
                current_section = "validation"
            elif current_section == "validation" and line.startswith('-'):
                validation_requirements += line + "\n"
        
        return {
            'primary_focus': primary_focus,
            'secondary_focus': secondary_focus,
            'validation_requirements': validation_requirements.strip(),
            'tactical_queries_recommended': 2,
            'operational_queries_recommended': 1
        }
