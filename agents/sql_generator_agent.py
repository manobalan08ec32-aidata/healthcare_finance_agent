import requests
import json
import time
from typing import Dict, List, Optional, Any
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class SQLGeneratorAgent:
    """High-quality SQL Generator with follow-up questions and retry logic"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
        self.max_retries = 3
    
    def generate_and_execute_sql(self, state: AgentState) -> Dict[str, Any]:
        """Main function: Generate SQL with follow-up capability, execute with retry, and synthesize results"""
        
        print(f"\n🔧 SQL Generator: Processing question")
        
        try:
            # 1. Extract context from state
            context = self._extract_context(state)
            
            # 2. Check if this is a follow-up response
            is_sql_followup = state.get('is_sql_followup', False)
            
            if is_sql_followup:
                # User has answered follow-up questions - generate SQL directly
                print("  📋 Processing follow-up answers...")
                sql_followup_question = state.get('sql_followup_question', '')
                sql_followup_answer = state.get('current_question', '')
                sql_result = self._generate_sql_with_followup(context, sql_followup_question, sql_followup_answer,state)
                
                # Set is_sql_followup to False after generating SQL
                state['is_sql_followup'] = False
            else:
                # Initial question - assess and generate or ask for follow-up
                print("  🔍 Initial assessment and SQL generation...")
                sql_result = self._assess_and_generate_sql(context, state)            
            # 3. Handle follow-up questions if needed
            if sql_result.get('needs_followup'):
                return {
                    'success': True,
                    'needs_followup': True,
                    'sql_followup_question': sql_result['sql_followup_questions']
                }
            
            if not sql_result['success']:
                return {
                    'success': False,
                    'error': sql_result['error']
                }
            
            # 4. Check if we have multiple SQL queries
            if sql_result.get('multiple_sql', False):
                print(f"  ⚡ Executing {len(sql_result['sql_queries'])} SQL queries in parallel...")
                return self._execute_multiple_sql_queries(sql_result, context)
            else:
                print(f"  ⚡ Executing single SQL query...")
                return self._execute_single_sql_query(sql_result, context, is_sql_followup)
            
        except Exception as e:
            return {
                'success': False,
                'error': f"SQL Generation failed: {str(e)}",
                'execution_attempts': 0
            }

    def _execute_single_sql_query(self, sql_result: Dict, context: Dict, is_sql_followup: bool) -> Dict[str, Any]:
        """Execute single SQL query and synthesize results"""
        
        # Execute with retry logic
        execution_result = self._execute_sql_with_retry(sql_result['sql_query'], context)
        if not execution_result['success']:
            return {
                'success': False,
                'error': execution_result['error'],
                'failed_sql': sql_result['sql_query'],
                'execution_attempts': execution_result['attempts']
            }
        
        # Synthesize results into narrative
        print(f"  📖 Synthesizing results...")
        narrative_result = self._synthesize_results(
            execution_result['data'], 
            context['current_question'],
            execution_result['final_sql']
        )
        
        if not narrative_result['success']:
            return {
                'success': False,
                'error': narrative_result['error']
            }

        return {
            'success': True,
            'multiple_results': False,
            'sql_query': execution_result['final_sql'],
            'query_results': execution_result['data'],
            'narrative_response': narrative_result['narrative'],
            'execution_attempts': execution_result['attempts'],
            'row_count': len(execution_result['data']) if execution_result['data'] else 0,
            'used_followup': bool(is_sql_followup)
        }

    def _execute_multiple_sql_queries(self, sql_result: Dict, context: Dict) -> Dict[str, Any]:
        """Execute multiple SQL queries in parallel and synthesize results"""
        
        import concurrent.futures
        import threading
        
        sql_queries = sql_result['sql_queries']
        query_titles = sql_result.get('query_titles', [])
        
        # Ensure we have titles for all queries
        while len(query_titles) < len(sql_queries):
            query_titles.append(f"Query {len(query_titles) + 1}")
        
        def execute_single_query(query_data):
            """Execute a single query with its title"""
            sql_query, title, index = query_data
            
            print(f"    Executing {title} (Query {index + 1})...")
            
            # Execute SQL with retry logic
            execution_result = self._execute_sql_with_retry(sql_query, context)
            
            return {
                'index': index,
                'title': title,
                'sql_query': sql_query,
                'execution_result': execution_result
            }
        
        def synthesize_single_result(result_data):
            """Synthesize results for a single query"""
            index = result_data['index']
            title = result_data['title']
            execution_result = result_data['execution_result']
            
            if not execution_result['success']:
                return {
                    'index': index,
                    'title': title,
                    'success': False,
                    'error': execution_result['error']
                }
            
            print(f"    Synthesizing results for {title}...")
            
            # Synthesize results
            narrative_result = self._synthesize_results(
                execution_result['data'],
                context['current_question'],
                execution_result['final_sql']
            )
            
            return {
                'index': index,
                'title': title,
                'success': True,
                'sql_query': execution_result['final_sql'],
                'data': execution_result['data'],
                'narrative': narrative_result.get('narrative', '') if narrative_result['success'] else 'Failed to generate narrative',
                'execution_attempts': execution_result['attempts'],
                'row_count': len(execution_result['data']) if execution_result['data'] else 0
            }
        
        try:
            # Step 1: Execute all SQL queries in parallel
            query_data = [(sql_queries[i], query_titles[i], i) for i in range(len(sql_queries))]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                execution_results = list(executor.map(execute_single_query, query_data))
            
            # Check if any executions failed
            failed_executions = [r for r in execution_results if not r['execution_result']['success']]
            if failed_executions:
                failed_query = failed_executions[0]
                return {
                    'success': False,
                    'error': f"Failed to execute {failed_query['title']}: {failed_query['execution_result']['error']}",
                    'failed_sql': failed_query['sql_query']
                }
            
            # Step 2: Synthesize all results in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                synthesis_results = list(executor.map(synthesize_single_result, execution_results))
            
            # Check if any synthesis failed
            failed_synthesis = [r for r in synthesis_results if not r['success']]
            if failed_synthesis:
                print(f"⚠️ Some narrative synthesis failed, but continuing with available results")
            
            # Sort results by original index to maintain order
            synthesis_results.sort(key=lambda x: x['index'])
            
            # Prepare final results
            query_results = []
            for result in synthesis_results:
                if result['success']:
                    query_results.append({
                        'title': result['title'],
                        'sql_query': result['sql_query'],
                        'data': result['data'],
                        'narrative': result['narrative'],
                        'execution_attempts': result['execution_attempts'],
                        'row_count': result['row_count']
                    })
                else:
                    query_results.append({
                        'title': result['title'],
                        'sql_query': '',
                        'data': [],
                        'narrative': f"Failed to execute: {result['error']}",
                        'execution_attempts': 0,
                        'row_count': 0
                    })
            
            return {
                'success': True,
                'multiple_results': True,
                'query_results': query_results,
                'total_queries': len(query_results),
                'successful_queries': len([r for r in synthesis_results if r['success']])
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Parallel execution failed: {str(e)}",
                'execution_attempts': 0
            }

    def _extract_context(self, state: Dict) -> Dict:
        """Extract context from state, including extracting table_kg from dataset_metadata"""
        questions_history = state.get('questions_sql_history', [])
        recent_history = questions_history[-4:]
        dataset_metadata = state.get('dataset_metadata', {})
        dataset_name = state.get('selected_dataset')
        current_question = state.get('rewritten_question')
        return {
            'recent_history': recent_history,
            'dataset_metadata': dataset_metadata,
            'dataset_name': dataset_name,
            'current_question': current_question
        }
    def _assess_and_generate_sql(self, context: Dict, state: Dict) -> Dict[str, Any]:
        """Initial assessment: Generate SQL if clear, or ask follow-up questions if unclear"""
        
        current_question = context.get('current_question', '')
        recent_history = context.get('recent_history', [])
        dataset_metadata = context.get('dataset_metadata', '')
        join_clause = state.get('join_clause', '')  
        
        # Check if we have multiple tables
        selected_datasets = state.get('selected_dataset', [])
        has_multiple_tables = len(selected_datasets) > 1 if isinstance(selected_datasets, list) else False
        
        assessment_prompt = f"""
            You are a highly skilled Healthcare Finance SQL analyst. You have TWO sequential tasks to complete.

            CURRENT QUESTION: {current_question}
            RECENT HISTORY: {recent_history}
            AVAILABLE METADATA: {dataset_metadata}
            MULTIPLE TABLES AVAILABLE: {has_multiple_tables}
            JOIN INFORMATION: {join_clause if join_clause else "No join clause provided"}

            ==============================
            TASK 1: COMPREHENSIVE ASSESSMENT
            ==============================

            Analyze the user's question for the following clarity issues:

            A. TIME PERIOD CLARITY:
            - Is the time reference specific? ("Q3 2024" vs "last quarter")
            - If relative time mentioned, is baseline clear? ("compared to what?")

            B. METRIC DEFINITIONS:
            - Are business metrics clearly defined? ("cost" - total cost? per member cost? unit cost?)
            - Are calculation methods obvious? ("performance" - what specific measure?)
            - Are formulas needed that aren't standard? (custom ratios, complex calculations)

            C. BUSINESS CONTEXT:
            - Are filtering criteria clear? ("top products" - by what measure?)
            - Are grouping dimensions obvious? ("by region" - state level? territory level?)
            - Are comparison baselines specified? ("variance" - vs what baseline?)

            D. FORMULA & CALCULATION REQUIREMENTS:
            - Does the question require custom formulas not in standard SQL functions?
            - Are there healthcare-specific calculations needed? (PMPM, utilization rates, etc.)
            - Do complex business rules need clarification? (exclusions, special logic)

            E. METADATA MAPPING:
            - Can all user terms be confidently mapped to available columns?
            - Are there ambiguous references that could match multiple columns?
            - Are there business terms mentioned that don't clearly exist in metadata?

            F. MULTI-TABLE AND SINGLE-TABLE COMPLEX QUERY ANALYSIS:
            - **PRIMARY ANALYSIS**: Does the question require data from multiple tables OR complex analysis that benefits from multiple perspectives?
            - **JOIN FEASIBILITY**: Can multi-table analysis be performed with a JOIN operation (if join clause exists)?
            - **COMPLEMENTARY REQUIREMENT**: Does it require separate analyses for better user understanding?
            - **COMPLEXITY ASSESSMENT**: Would splitting into multiple queries provide clearer insights?

            **SCENARIOS REQUIRING MULTIPLE QUERIES**:
            
            MULTI-TABLE PATTERNS:
            - "Ledger revenue + breakdown by drug" → financial table + claim table
            - "Budget metrics + client breakdown" → forecast table + transaction table
            
            SINGLE-TABLE COMPLEX PATTERNS:
            - "Membership trends AND top drugs by revenue" → trend query + ranking query
            - "Summary metrics AND detailed breakdown" → summary query + detail query
            - "Current performance AND comparative analysis" → performance query + comparison query

            **DECISION CRITERIA**:
            - Multiple distinct analytical purposes in one question
            - Different aggregation levels (summary + detail)
            - Combines trends with rankings or comparisons
            - Question contains "AND" connecting different analysis types
            - Would result in overly complex single query
            
            **MULTI-QUERY DECISION LOGIC**:
            1. **SINGLE QUERY WITH JOIN**: Simple analysis requiring related data from multiple tables with join
            2. **MULTIPLE QUERIES - MULTI-TABLE**: Complementary analysis from different tables OR no join exists
            3. **MULTIPLE QUERIES - COMPLEX SINGLE-TABLE**: Complex question with multiple analytical dimensions
            4. **SINGLE QUERY**: Simple, focused questions with one analytical dimension
            ==============================
            TASK 1 DECISION CRITERIA
            ==============================

            PROCEED TO TASK 2 (Generate SQL) IF:
            - All areas (A-F) are sufficiently clear
            - You can map user request to available columns with 95% confidence
            - Standard SQL functions can handle all requested calculations
            - No ambiguous business logic or custom formulas needed
            - Multi-table strategy is clear (join vs separate queries)

            REQUEST FOLLOW-UP IF:
            - ANY of areas A-F have significant ambiguity
            - Custom formulas/calculations need clarification
            - Business logic requires domain expertise
            - Metadata mapping is uncertain
            - Multi-table approach is unclear

            ==============================================
            TASK 2: HIGH-QUALITY DATABRICKS SQL GENERATION 
            ==============================================

            (Only execute if Task 1 assessment says "PROCEED")

            **MULTI-TABLE DECISION LOGIC:**
            1. **SINGLE QUERY WITH JOIN**: If question requires related data from multiple tables AND join clause exists
            2. **MULTIPLE SEPARATE QUERIES**: If question requires complementary analysis from different tables OR no join exists
            3. **SINGLE TABLE**: If question can be answered from one table

            **SQL GENERATION RULES:**

            1. MEANINGFUL COLUMN NAMES
            - Use user-friendly, business-relevant column names that align with the user's question.
            - Generate a month-over-month comparison that clearly displays month names side by side in the output

            2. COMPLETE TRANSPARENCY - SHOW ALL COMPONENTS
            - MANDATORY: Include ALL columns used in WHERE clause, GROUP BY clause, and calculations in the SELECT output
            - If calculating a percentage, include the numerator, denominator, AND the percentage itself
            - If calculating a variance, include the original values AND the variance
            - If filtering by product_category, include product_category in SELECT
            - If grouping by therapeutic_class, include therapeutic_class in SELECT
            - This ensures users can see the full context and verify how results were derived

            Example:
            -- User asks: "Cost per member for Specialty products by state"
            SELECT 
                product_category,           -- Filter component (used in WHERE)
                state_name,                -- Grouping component (used in GROUP BY)  
                total_cost,                -- Numerator component
                member_count,              -- Denominator component
                total_cost / member_count AS cost_per_member  -- Final calculation
            FROM table 
            WHERE UPPER(product_category) = UPPER('Specialty')
            GROUP BY product_category, state_name

            3. SPECIAL TABLE-LEVEL FILTERING RULES
            - When building SQL queries using this table's metadata, check if the special_table_level_instruction key is present. If it exists, follow the filtering rules defined in it. For example, if the instruction specifies that no filters should be applied to product_category when the user mentions 'Specialty', 'Home Delivery', and 'PBM' together—or 'PBM' alone—then ensure that no filters are added to product_category in those cases

            4. METRICS & AGGREGATIONS
            - If the question includes metrics (e.g., costs, amounts, counts, totals, averages), use appropriate aggregation functions (SUM, COUNT, AVG) and include GROUP BY clauses with relevant business dimensions.
            - When the question specifies only a month, use the current year (2025) for calculations.

            5. MULTI-TABLE JOIN SYNTAX (when applicable):
            - Use the provided join clause exactly as specified
            - Ensure all selected columns are properly qualified with table aliases
            - Include all necessary tables in the FROM/JOIN clauses

            6. ATTRIBUTE-ONLY QUERIES
            - If the question asks only about attributes (e.g., member age, drug name, provider type) and does NOT request metrics, return only the relevant columns without aggregation.

            7. STRING FILTERING - CASE INSENSITIVE
            - When filtering on text/string columns, always use UPPER() function on BOTH sides for case-insensitive matching.
            - Example: WHERE UPPER(product_category) = UPPER('Specialty')

            8. TOP/BOTTOM QUERIES WITH TOTALS
            - When the user asks for "top 10", "bottom 10", "highest", "lowest", also include the overall total for context.
            - Include ranking position information
            - Let the LLM decide the best SQL structure to achieve this (CTE, subquery, etc.).

            9. HEALTHCARE FINANCE BEST PRACTICES
            - Always include time dimensions (month, quarter, year) when relevant to the user's question.
            - Use business-friendly dimensions (e.g., therapeutic_class, service_type, age_group, state).

            10. DATABRICKS SQL COMPATIBILITY
            - Use standard SQL functions: SUM, COUNT, AVG, MAX, MIN
            - Use date functions: date_trunc(), year(), month(), quarter()
            - Use CASE WHEN for conditional logic
            - Use CTEs (WITH clauses) for complex logic

            11. FORMATTING
            - Show whole numbers for metrics and round percentages to four decimal places.
            - Use the ORDER BY clause only for date columns and use descending order.

            ==============================
            OUTPUT FORMATS
            ==============================
            IMPORTANT: You can use proper SQL formatting with line breaks and indentation inside the XML tags
            return ONLY the SQL query wrapped in XML tags. No other text, explanations, or formatting

            **FOR SINGLE SQL QUERY:**
            If TASK 1 says PROCEED → Execute TASK 2:
            <sql>
            [Your complete SQL query here]
            </sql>

            **FOR MULTIPLE SQL QUERIES:**
            If TASK 1 says PROCEED and requires multiple queries:
            <multiple_sql>
            <query1_title>
            [Brief descriptive title for first query - max 8 words]
            </query1_title>
            <query1>
            [First SQL query here]
            </query1>
            <query2_title>
            [Brief descriptive title for second query - max 8 words]
            </query2_title>
            <query2>
            [Second SQL query here]
            </query2>
            </multiple_sql>

            If TASK 1 says REQUEST FOLLOW-UP, return ONLY the followup wrapped in XML tags. No other text, explanations, or formatting

            ==============================
            DETERMINISTIC FOLLOW-UP RULES
            ==============================

            STEP 1: Identify which specific areas from your 5-area assessment (A-E) were marked as "❌ Needs Clarification"

            STEP 2: Generate questions ONLY for the unclear areas identified in Step 1

            FOR ANY FOLLOW-UP SITUATION:
            <followup>
            I need clarification to generate accurate SQL:

            **[Specific issue from unclear area]**: [Direct question in one sentence]
            - Available data: [specific column names from metadata]
            - Suggested approach: [concrete calculation option]

            **[Second issue if needed]**: [Second direct question in one sentence only if multiple areas unclear]
            - Available data: [relevant columns]
            - Alternative: [another option]

            Please clarify these points.
            </followup>

            FOLLOW-UP CONSTRAINTS:
            - Ask questions ONLY for areas marked as "❌ Needs Clarification" in your assessment
            - If only 1 area unclear → ask only 1 question
            - If 2+ areas unclear → maximum 2 questions for most critical areas
            - Never ask about areas already marked as "✓ Clear"
            - Each main question gets exactly 2 sub-bullets: one for available data, one for suggestion
            - Use actual column names from metadata
            - Keep all bullets short and actionable

            ==============================
            EXECUTION INSTRUCTION
            ==============================

            1. Complete TASK 1 assessment across all 5 areas
            2. Make clear PROCEED/FOLLOW-UP decision
            3. If PROCEED: Execute TASK 2 with full SQL generation
            4. If FOLLOW-UP: Ask questions covering ONLY the unclear areas from your assessment

            You only get ONE opportunity for follow-up, so be thorough in your assessment.
            """

        for attempt in range(self.max_retries):
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": assessment_prompt}
                ])
                print('sql assessment', llm_response)
                
                # Extract SQL or follow-up questions
                import re
                
                # Check for multiple SQL queries first
                multiple_sql_match = re.search(r'<multiple_sql>(.*?)</multiple_sql>', llm_response, re.DOTALL)
                if multiple_sql_match:
                    multiple_content = multiple_sql_match.group(1).strip()
                    
                    # Extract individual queries with titles
                    query_matches = re.findall(r'<query(\d+)_title>(.*?)</query\1_title>.*?<query\1>(.*?)</query\1>', multiple_content, re.DOTALL)
                    if query_matches:
                        sql_queries = []
                        query_titles = []
                        for i, (query_num, title, query) in enumerate(query_matches):
                            cleaned_query = query.strip().replace('`', '')
                            cleaned_title = title.strip()
                            if cleaned_query and cleaned_title:
                                sql_queries.append(cleaned_query)
                                query_titles.append(cleaned_title)
                        
                        if sql_queries:
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,  # Add this line
                                'query_count': len(sql_queries)
                            }
                    
                    raise ValueError("Empty or invalid multiple SQL queries in XML response")
                
                # Check for single SQL query
                sql_match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if sql_match:
                    sql_query = sql_match.group(1).strip()
                    sql_query = sql_query.replace('`', '')  # Remove backticks
                    
                    if not sql_query:
                        raise ValueError("Empty SQL query in XML response")
                    
                    return {
                        'success': True,
                        'multiple_sql': False,
                        'sql_query': sql_query
                    }
                
                # Check for follow-up questions
                followup_match = re.search(r'<followup>(.*?)</followup>', llm_response, re.DOTALL)
                if followup_match:
                    followup_text = followup_match.group(1).strip()
                    
                    if not followup_text:
                        raise ValueError("Empty follow-up questions in XML response")
                    
                    # Set is_sql_followup to True when asking follow-up questions
                    state['is_sql_followup'] = True
                    
                    return {
                        'success': True,
                        'needs_followup': True,
                        'sql_followup_questions': followup_text
                        }
                
                # Neither SQL nor follow-up found
                raise ValueError("No SQL or follow-up questions found in response")
            
            except Exception as e:
                print(f"❌ SQL assessment attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying SQL assessment... (Attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(2 ** attempt)
        
        return {
            'success': False,
            'error': f"SQL assessment failed after {self.max_retries} attempts due to Model errors"
        }
    
    
    def _generate_sql_with_followup(self, context: Dict, sql_followup_question: str, sql_followup_answer: str, state: Dict) -> Dict[str, Any]:
        """Generate SQL using original question + follow-up Q&A with multiple SQL support"""
        
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')
        join_clause = state.get('join_clause', '')  # Get join clause from state
        
        # Check if we have multiple tables
        selected_datasets = state.get('selected_dataset', [])
        has_multiple_tables = len(selected_datasets) > 1 if isinstance(selected_datasets, list) else False

        followup_sql_prompt = f"""
            You are a highly skilled Healthcare Finance SQL analyst. This is PHASE 2 of a two-phase process.
            Your task is to generate a **high-quality Databricks SQL query** based on the user's question
            ==============================
            CONTEXT: FOLLOW-UP CLARIFICATION PROCESS
            ==============================
            
            PHASE 1 COMPLETED: You previously analyzed the user's question and determined that clarification was needed for accurate SQL generation.
            
            PHASE 2 NOW: Generate the final SQL query using the original question PLUS the clarifications provided.

            ORIGINAL USER QUESTION: {current_question}
            **AVAILABLE METADATA**: {dataset_metadata}
            MULTIPLE TABLES AVAILABLE: {has_multiple_tables}
            JOIN INFORMATION: {join_clause if join_clause else "No join clause provided"}

            ==============================
            FOLLOW-UP CLARIFICATION RECEIVED
            ==============================
            
            YOUR PREVIOUS QUESTION: {sql_followup_question}
            
            USER'S CLARIFICATION: {sql_followup_answer}

            ==============================
            MULTI-TABLE AND COMPLEX QUERY ANALYSIS
            ==============================
            
            Before generating SQL, assess if this requires multiple queries for better user understanding:

            **SCENARIOS REQUIRING MULTIPLE QUERIES**:
            
            MULTI-TABLE PATTERNS:
            - "Ledger revenue + breakdown by drug" → financial table + claim table
            - "Budget metrics + client breakdown" → forecast table + transaction table
            
            SINGLE-TABLE COMPLEX PATTERNS:
            - "Membership trends AND top drugs by revenue" → trend query + ranking query
            - "Summary metrics AND detailed breakdown" → summary query + detail query
            - "Current performance AND comparative analysis" → performance query + comparison query

            **DECISION CRITERIA**:
            - Multiple distinct analytical purposes in one question
            - Different aggregation levels (summary + detail)
            - Combines trends with rankings or comparisons
            - Question contains "AND" connecting different analysis types
            - Would result in overly complex single query
            
            **MULTI-QUERY DECISION LOGIC**:
            1. **SINGLE QUERY WITH JOIN**: Simple analysis requiring related data from multiple tables with join
            2. **MULTIPLE QUERIES - MULTI-TABLE**: Complementary analysis from different tables OR no join exists
            3. **MULTIPLE QUERIES - COMPLEX SINGLE-TABLE**: Complex question with multiple analytical dimensions
            4. **SINGLE QUERY**: Simple, focused questions with one analytical dimension

            ==============================
            FINAL SQL GENERATION TASK
            ==============================
            
            Now generate a high-quality Databricks SQL query using:
            1. The ORIGINAL user question as the primary requirement
            2. The USER'S CLARIFICATION to resolve any ambiguities
            3. Available metadata for column mapping
            4. Multi-table strategy assessment (single vs multiple queries)
            5. All SQL generation best practices below

            IMPORTANT: No more questions allowed - this is the definitive SQL generation using all available information.

            ==============================
            CRITICAL SQL GENERATION RULES
            ==============================
            1. MEANINGFUL COLUMN NAMES
            - Use user-friendly, business-relevant column names that align with the user's question.
            - Generate a month-over-month comparison that clearly displays month names side by side in the output

            2. COMPLETE TRANSPARENCY - SHOW ALL COMPONENTS
            - MANDATORY: Include ALL columns used in WHERE clause, GROUP BY clause, and calculations in the SELECT output
            - If calculating a percentage, include the numerator, denominator, AND the percentage itself
            - If calculating a variance, include the original values AND the variance
            - If filtering by product_category, include product_category in SELECT
            - If grouping by therapeutic_class, include therapeutic_class in SELECT
            - This ensures users can see the full context and verify how results were derived

            Example:
            -- User asks: "Cost per member for Specialty products by state"
            SELECT 
                product_category,           -- Filter component (used in WHERE)
                state_name,                -- Grouping component (used in GROUP BY)  
                total_cost,                -- Numerator component
                member_count,              -- Denominator component
                total_cost / member_count AS cost_per_member  -- Final calculation
            FROM table 
            WHERE UPPER(product_category) = UPPER('Specialty')
            GROUP BY product_category, state_name

            3. SPECIAL TABLE-LEVEL FILTERING RULES
            - When building SQL queries using this table's metadata, check if the special_table_level_instruction key is present. If it exists, follow the filtering rules defined in it. For example, if the instruction specifies that no filters should be applied to product_category when the user mentions 'Specialty', 'Home Delivery', and 'PBM' together—or 'PBM' alone—then ensure that no filters are added to product_category in those cases

            4. METRICS & AGGREGATIONS
            - If the question includes metrics (e.g., costs, amounts, counts, totals, averages), use appropriate aggregation functions (SUM, COUNT, AVG) and include GROUP BY clauses with relevant business dimensions.
            - When the question specifies only a month, use the current year (2025) for calculations.

            5. MULTI-TABLE JOIN SYNTAX (when applicable):
            - Use the provided join clause exactly as specified
            - Ensure all selected columns are properly qualified with table aliases
            - Include all necessary tables in the FROM/JOIN clauses

            6. ATTRIBUTE-ONLY QUERIES
            - If the question asks only about attributes (e.g., member age, drug name, provider type) and does NOT request metrics, return only the relevant columns without aggregation.

            7. STRING FILTERING - CASE INSENSITIVE
            - When filtering on text/string columns, always use UPPER() function on BOTH sides for case-insensitive matching.
            - Example: WHERE UPPER(product_category) = UPPER('Specialty')

            8. TOP/BOTTOM QUERIES WITH TOTALS
            - When the user asks for "top 10", "bottom 10", "highest", "lowest", also include the overall total for context.
            - Include ranking position information
            - Let the LLM decide the best SQL structure to achieve this (CTE, subquery, etc.).

            9. HEALTHCARE FINANCE BEST PRACTICES
            - Always include time dimensions (month, quarter, year) when relevant to the user's question.
            - Use business-friendly dimensions (e.g., therapeutic_class, service_type, age_group, state).

            10. DATABRICKS SQL COMPATIBILITY
            - Use standard SQL functions: SUM, COUNT, AVG, MAX, MIN
            - Use date functions: date_trunc(), year(), month(), quarter()
            - Use CASE WHEN for conditional logic
            - Use CTEs (WITH clauses) for complex logic

            11. FORMATTING
            - Show whole numbers for metrics and round percentages to four decimal places.
            - Use the ORDER BY clause only for date columns and use descending order.

            ==============================
            INTEGRATION INSTRUCTIONS
            ==============================
            
            - Integrate the user's clarification naturally into the SQL logic
            - If clarification provided specific formulas, implement them precisely
            - If clarification resolved time periods, use exact dates/ranges specified  
            - If clarification defined metrics, use the exact business definitions provided
            - Maintain all original SQL quality standards while incorporating clarifications

            ==============================
            OUTPUT FORMATS
            ==============================
            IMPORTANT: You can use proper SQL formatting with line breaks and indentation inside the XML tags
            return ONLY the SQL query wrapped in XML tags. No other text, explanations, or formatting

            **FOR SINGLE SQL QUERY:**
            <sql>
            [Your complete SQL query incorporating both original question and clarifications]
            </sql>

            **FOR MULTIPLE SQL QUERIES:**
            If analysis requires multiple queries for better understanding:
            <multiple_sql>
            <query1_title>
            [Brief descriptive title for first query - max 8 words]
            </query1_title>
            <query1>
            [First SQL query here]
            </query1>
            <query2_title>
            [Brief descriptive title for second query - max 8 words]
            </query2_title>
            <query2>
            [Second SQL query here]
            </query2>
            </multiple_sql>

            Generate the definitive SQL query now.
            """

        for attempt in range(self.max_retries):
            try:
                print('followup_sql_prompt',followup_sql_prompt)
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": followup_sql_prompt}
                ])
                print('sql generation with followup', llm_response)
                
                # Extract SQL or multiple SQL from XML tags
                import re
                
                # Check for multiple SQL queries first
                multiple_sql_match = re.search(r'<multiple_sql>(.*?)</multiple_sql>', llm_response, re.DOTALL)
                if multiple_sql_match:
                    multiple_content = multiple_sql_match.group(1).strip()
                    
                    # Extract individual queries with titles
                    query_matches = re.findall(r'<query(\d+)_title>(.*?)</query\1_title>.*?<query\1>(.*?)</query\1>', multiple_content, re.DOTALL)
                    if query_matches:
                        sql_queries = []
                        query_titles = []
                        for i, (query_num, title, query) in enumerate(query_matches):
                            cleaned_query = query.strip().replace('`', '')
                            cleaned_title = title.strip()
                            if cleaned_query and cleaned_title:
                                sql_queries.append(cleaned_query)
                                query_titles.append(cleaned_title)
                        
                        if sql_queries:
                            return {
                                'success': True,
                                'multiple_sql': True,
                                'sql_queries': sql_queries,
                                'query_titles': query_titles,
                                'query_count': len(sql_queries)
                            }
                    
                    raise ValueError("Empty or invalid multiple SQL queries in XML response")
                
                # Check for single SQL query
                match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if match:
                    sql_query = match.group(1).strip()
                    sql_query = sql_query.replace('`', '')  # Remove backticks
                    
                    if not sql_query:
                        raise ValueError("Empty SQL query in XML response")
                    
                    return {
                        'success': True,
                        'multiple_sql': False,
                        'sql_query': sql_query
                    }
                else:
                    raise ValueError("No SQL found in XML tags")
            
            except Exception as e:
                print(f"❌ SQL generation with follow-up attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying SQL generation with follow-up... (Attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(2 ** attempt)
        
        return {
            'success': False,
            'error': f"SQL generation with follow-up failed after {self.max_retries} attempts due to Model errors"
        }

    
    
    def _execute_sql_with_retry(self, initial_sql: str, context: Dict, max_retries: int = 3) -> Dict:
        """Execute SQL with intelligent retry logic and async handling"""
        
        current_sql = initial_sql
        errors_history = []
        
        for attempt in range(max_retries):
            try:                
                # Execute SQL with async handling
                result = self._execute_databricks_sql(current_sql, timeout=300)
                
                if result['success']:
                    print(f"    ✅ SQL executed successfully on attempt {attempt + 1}")
                    return {
                        'success': True,
                        'data': result['data'],
                        'final_sql': current_sql,
                        'attempts': attempt + 1,
                        'execution_time': result.get('execution_time', 0)
                    }
                else:
                    # Capture error for retry
                    error_msg = result['error']
                    errors_history.append(f"Attempt {attempt + 1}: {error_msg}")
                                        
                    # If not last attempt, try to fix the SQL
                    if attempt < max_retries - 1:
                        fix_result = self._fix_sql_with_llm(current_sql, error_msg, errors_history, context)
                        if fix_result['success']:
                            current_sql = fix_result['fixed_sql']
                        else:
                            # If fixing fails, break the retry loop
                            break
            
            except Exception as e:
                error_msg = str(e)
                errors_history.append(f"Attempt {attempt + 1}: {error_msg}")
                
                if attempt < max_retries - 1:
                    fix_result = self._fix_sql_with_llm(current_sql, error_msg, errors_history, context)
                    if fix_result['success']:
                        current_sql = fix_result['fixed_sql']
                    else:
                        # If fixing fails, break the retry loop
                        break
        
        # All attempts failed
        return {
            'success': False,
            'error': f"All {max_retries} attempts failed. Last error: {errors_history[-1] if errors_history else 'Unknown error'}",
            'final_sql': current_sql,
            'attempts': max_retries,
            'errors_history': errors_history
        }
    
    def _execute_databricks_sql(self, sql_query: str, timeout: int = 600) -> Dict:
        """Execute SQL against Databricks with proper async handling"""
        
        try:
            start_time = time.time()
            
            payload = {
                "warehouse_id": self.db_client.SQL_WAREHOUSE_ID,
                "statement": sql_query,
                "disposition": "INLINE",
                "wait_timeout": "5s"
            }
            
            response = requests.post(
                self.db_client.sql_api_url,
                headers=self.db_client.headers,
                json=payload,
                timeout=60
            )
            
            response.raise_for_status()
            result = response.json()
            
            status = result.get('status', {})
            state = status.get('state', '')
            
            if state == 'SUCCEEDED':
                data = self._extract_sql_results(result)
                execution_time = time.time() - start_time
                return {
                    'success': True, 
                    'data': data,
                    'execution_time': execution_time
                }
            
            elif state in ['PENDING', 'RUNNING']:
                statement_id = result.get('statement_id')
                if statement_id:
                    data = self._poll_sql_results(statement_id, timeout, start_time)
                    execution_time = time.time() - start_time
                    return {
                        'success': True, 
                        'data': data,
                        'execution_time': execution_time
                    }
                else:
                    return {
                        'success': False, 
                        'error': 'Query is running but no statement_id provided',
                        'data': []
                    }
            
            elif state == 'FAILED':
                error_message = status.get('error', {}).get('message', 'Unknown error')
                return {
                    'success': False, 
                    'error': f"Query failed: {error_message}",
                    'data': []
                }
            
            elif state == 'CANCELED':
                return {
                    'success': False, 
                    'error': 'Query was canceled',
                    'data': []
                }
            
            else:
                # Fallback - try to extract results anyway
                try:
                    data = self._extract_sql_results(result)
                    execution_time = time.time() - start_time
                    return {
                        'success': True, 
                        'data': data,
                        'execution_time': execution_time
                    }
                except:
                    return {
                        'success': False, 
                        'error': f'Unknown query state: {state}',
                        'data': []
                    }
            
        except requests.exceptions.RequestException as e:
            return {'success': False, 'error': f"Request failed: {str(e)}", 'data': []}
        except KeyError as e:
            return {'success': False, 'error': f"Response format error: {str(e)}", 'data': []}
        except Exception as e:
            return {'success': False, 'error': f"Execution error: {str(e)}", 'data': []}
    
    def _extract_sql_results(self, result: Dict) -> List[Dict]:
        """Extract results from Databricks response"""
        
        result_data = result.get("result", {})
        if "data_array" not in result_data:
            return []
        
        if "manifest" not in result:
            return []
            
        cols = [c["name"] for c in result["manifest"]["schema"]["columns"]]
        return [dict(zip(cols, row)) for row in result_data["data_array"]]
    
    def _poll_sql_results(self, statement_id: str, timeout: int = 300, start_time: float = None) -> List[Dict]:
        """Poll for query results until completion with fixed 3-second intervals"""
        
        if start_time is None:
            start_time = time.time()
            
        poll_interval = 3  # Fixed 3-second polling interval
        
        while time.time() - start_time < timeout:
            try:
                elapsed = time.time() - start_time
                
                status_url = f"{self.db_client.sql_api_url}{statement_id}"
                response = requests.get(status_url, headers=self.db_client.headers, timeout=30)
                response.raise_for_status()
                
                result = response.json()
                status = result.get('status', {})
                state = status.get('state', '')
                                
                if state == 'SUCCEEDED':
                    return self._extract_sql_results(result)
                
                elif state == 'FAILED':
                    error_message = status.get('error', {}).get('message', 'Unknown error')
                    raise Exception(f"Query failed after {elapsed:.1f}s: {error_message}")
                
                elif state == 'CANCELED':
                    raise Exception(f"Query was canceled after {elapsed:.1f}s")
                
                elif state in ['PENDING', 'RUNNING']:
                    time.sleep(poll_interval)
                    continue
                
                else:
                    time.sleep(poll_interval)
                    continue
                    
            except requests.exceptions.RequestException:
                time.sleep(poll_interval)
                continue
        
        raise Exception(f"Query timed out after {timeout} seconds")
    
    def _fix_sql_with_llm(self, failed_sql: str, error_msg: str, errors_history: List[str], context: Dict) -> Dict[str, Any]:
        """Use LLM to fix SQL based on error with enhanced prompting and retry logic"""

        history_text = "\n".join(errors_history) if errors_history else "No previous errors"
        current_question = context.get('current_question', '')
        dataset_metadata = context.get('dataset_metadata', '')

        fix_prompt = f"""
            You are an expert Databricks SQL developer. A SQL query has **FAILED** and needs to be **FIXED or REWRITTEN**.

            ==============================
            CONTEXT
            ==============================
            - ORIGINAL USER QUESTION: "{current_question}"
            - TABLE METADATA: {dataset_metadata}

            ==============================
            FAILURE DETAILS
            ==============================
            - FAILED SQL QUERY:
            ```sql
            {failed_sql}
            ```
            - ERROR MESSAGE: {error_msg}
            - PREVIOUS RETRY ERRORS: {history_text}

            ==============================
            INSTRUCTIONS
            ==============================
            Identify the issue based on the error message and metadata.
            Fix the SQL syntax or rewrite the query if needed.
            Ensure the corrected query answers the original user question.
            Use only valid column names and Databricks-compatible SQL.

            ==============================
            RESPONSE FORMAT
            ==============================
            Return ONLY the fixed SQL query wrapped in XML tags. No other text, explanations, or formatting.

            <sql>
            SELECT ...your fixed SQL here...
            </sql>

            IMPORTANT: You can use proper SQL formatting with line breaks and indentation inside the <sql> tags. This makes the SQL readable and maintainable.
            """

        for attempt in range(self.max_retries):
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": fix_prompt}
                ])
                print('sql fix', llm_response)

                # Extract SQL from XML tags
                import re
                match = re.search(r'<sql>(.*?)</sql>', llm_response, re.DOTALL)
                if match:
                    fixed_sql = match.group(1).strip()
                    fixed_sql = fixed_sql.replace('`', '')  # Remove backticks

                    if not fixed_sql:
                        raise ValueError("Empty fixed SQL query in XML response")

                    return {
                        'success': True,
                        'fixed_sql': fixed_sql
                    }
                else:
                    raise ValueError("No SQL found in XML tags")

            except Exception as e:
                print(f"❌ SQL fix attempt {attempt + 1} failed: {str(e)}")

                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying SQL fix... (Attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(2 ** attempt)

        return {
            'success': False,
            'error': f"SQL fix failed after {self.max_retries} attempts due to Model errors"
        }
    
    def _synthesize_results(self, sql_data: List[Dict], question: str, sql_query: str) -> Dict[str, Any]:
        """Synthesize SQL results into narrative story with retry logic"""

        if not sql_data:
            return {
                'success': True,
                'narrative': "No data was found matching your query criteria."
            }

        row_count = len(sql_data)
        columns = list(sql_data[0].keys()) if sql_data else []
        column_count = len(columns)
        total_count = row_count * column_count

        if total_count > 2000:
            return {
                'success': True,
                'narrative': "Too many records to synthesize."
            }

        has_multiple_records = row_count > 1
        has_date_columns = any('date' in col.lower() or 'month' in col.lower() or 'year' in col.lower() or 'quarter' in col.lower() for col in columns)
        has_numeric_columns = False
        
        if sql_data:
            sample_row = sql_data[0]
            for value in sample_row.values():
                if isinstance(value, (int, float)) and value != 0:
                    has_numeric_columns = True
                    break

        synthesis_prompt = f"""
        You are a Healthcare Finance Data Analyst. Create a clear, factual narrative from SQL results.

        USER QUESTION: "{question}"

        SQL QUERY EXECUTED:
        {sql_query}

        DATA ANALYSIS:
        - Total rows: {row_count}
        - Columns: {', '.join(columns)}
        - Has multiple records: {has_multiple_records}
        - Contains date/time data: {has_date_columns}
        - Contains numeric data: {has_numeric_columns}

        **Query Output**:
        {sql_data}

        CRITICAL INSTRUCTIONS:

        **DATA SUFFICIENCY CHECK**:
        - If there is only 1 record OR insufficient data for meaningful analysis, respond: "Not enough data to create comprehensive narrative analysis."
        - Only proceed with full narrative if there are multiple records with meaningful patterns to analyze

        ========================
        GOAL & AUDIENCE
        ========================
        Goal: Turn SQL result tables into crisp, bullet-only insights for finance leaders.
        Audience: FP&A and operations leaders; expect variance, drivers/suppressors, concentration, and trend callouts.
        
        RULES:
        - If ≤1 record: respond "Not enough data to create comprehensive narrative analysis."
        - Bullets only, ≤22 words each, new lines between bullets
        - Use exact names from data, no modifications
        - Auto-scale: ≥1B→x.xB, ≥1M→x.xM, ≥1K→x.xK
        - Percentages: 1 decimal place
        - Use ONLY the exact names, values, and terms that appear in the SQL results
        - Do NOT invent, rename, or modify any names from the data
        - Do NOT create generic categories or simplified labels

        SECTIONS (3-6 bullets total):
        1. Executive summary (1-2 bullets): direction, magnitude, scope
        2. Key drivers/suppressors (2-3 bullets): top contributors with deltas
        3. Trends (1-2 bullets): period-over-period changes

        RESPONSE FORMAT:
        The response MUST be valid JSON. Do NOT include any extra text, markdown, or formatting. The response MUST not start with ```json and end with ```.

        {{"narrative_response": "• First bullet\\n• Second bullet\\n• Third bullet"}}
        
        """

        for attempt in range(self.max_retries):
            try:
                llm_response = self.db_client.call_claude_api_endpoint([
                    {"role": "user", "content": synthesis_prompt}
                ])
                print('sql narr', llm_response)
                
                response_json = json.loads(llm_response.strip())
                narrative = response_json.get('narrative_response', '').strip()
                
                if not narrative or len(narrative) < 10:
                    raise ValueError("Empty or insufficient narrative in JSON response")
                
                return {
                    'success': True,
                    'narrative': narrative
                }

            except Exception as e:
                print(f"❌ Narrative synthesis attempt {attempt + 1} failed: {str(e)}")
                
                if attempt < self.max_retries - 1:
                    print(f"🔄 Retrying narrative synthesis... (Attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(2 ** attempt)
        
        return {
            'success': False,
            'error': f"Narrative synthesis failed after {self.max_retries} attempts due to Model errors"
        }
