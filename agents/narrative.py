from typing import Dict, List, Optional, Any
import pandas as pd
import json
import asyncio
import re
from datetime import datetime
from core.state_schema import AgentState
from core.databricks_client import DatabricksClient

class NarrativeAgent:
    """Narrative synthesis agent: generates intelligent narratives from SQL results"""
    
    def __init__(self, databricks_client: DatabricksClient):
        self.db_client = databricks_client
    
    async def synthesize_narrative(self, state: AgentState) -> Dict[str, Any]:
        """Generate narrative from SQL results"""
        
        print(f"\n📝 Narrative Agent: Synthesizing narrative from SQL results")
        
        try:
            # Get SQL results from state
            sql_result = state.get('sql_result', {})
            current_question = state.get('rewritten_question', '')
            
            if not sql_result or not sql_result.get('success', False):
                return {
                    'success': False,
                    'error': 'No valid SQL results found for narrative synthesis'
                }
            
            # Check if single or multiple SQL results
            if sql_result.get('multiple_results', False):
                # Handle multiple SQL queries
                query_results = sql_result.get('query_results', [])
                narrative_result = await self._synthesize_multiple_narratives_async(query_results, current_question)
            else:
                # Handle single SQL query
                sql_query = sql_result.get('sql_query', '')
                data = sql_result.get('query_results', [])
                narrative_result = await self._synthesize_single_narrative_async(data, current_question, sql_query)
            
            if narrative_result['success']:
                # Update state with narrative results
                if sql_result.get('multiple_results', False):
                    # Update each query result with its narrative and summary
                    updated_query_results = []
                    narratives = narrative_result.get('narratives', [])
                    summaries = narrative_result.get('summaries', [])
                    
                    for idx, query_result in enumerate(query_results):
                        updated_query = query_result.copy()
                        if idx < len(narratives):
                            updated_query['narrative'] = narratives[idx]
                        if idx < len(summaries):
                            updated_query['summary'] = summaries[idx]
                        updated_query_results.append(updated_query)
                    
                    # Update the sql_result with narratives
                    state['sql_result']['query_results'] = updated_query_results
                else:
                    # Update single result with narrative and summary
                    state['sql_result']['narrative'] = narrative_result.get('narrative', '')
                    state['sql_result']['summary'] = narrative_result.get('summary', '')
                
                return {
                    'success': True,
                    'narrative_complete': True
                }
            else:
                return {
                    'success': False,
                    'error': narrative_result.get('error', 'Narrative synthesis failed')
                }
                
        except Exception as e:
            error_msg = f"Narrative agent failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }
    
    async def _synthesize_multiple_narratives_async(self, query_results: List[Dict], question: str) -> Dict[str, Any]:
        """Generate narratives for multiple SQL queries"""
        
        print(f"📊 Synthesizing narratives for {len(query_results)} queries")
        
        narratives = []
        summaries = []
        
        for idx, query_result in enumerate(query_results):
            title = query_result.get('title', f'Query {idx+1}')
            sql_query = query_result.get('sql_query', '')
            data = query_result.get('data', [])
            
            print(f"  🔄 Processing narrative for: {title}")
            
            narrative_result = await self._synthesize_single_narrative_async(data, f"{question} - {title}", sql_query)
            
            if narrative_result['success']:
                narratives.append(narrative_result['narrative'])
                summaries.append(narrative_result.get('summary', ''))
            else:
                narratives.append(f"Narrative generation failed for {title}")
                summaries.append(f"Summary generation failed for {title}")
        
        return {
            'success': True,
            'narratives': narratives,
            'summaries': summaries
        }
    
    async def _synthesize_single_narrative_async(self, sql_data: List[Dict], question: str, sql_query: str) -> Dict[str, Any]:
        """Generate narrative for a single SQL query result with robust logic matching original implementation"""
        
        if not sql_data:
            return {
                'success': True,
                'narrative': "No data was found matching your query criteria.",
                'summary': "No data returned from the query."
            }
        
        try:
            # Convert SQL data to DataFrame for cleaner handling
            df = self._json_to_dataframe(sql_data)
            
            if df.empty:
                return {
                    'success': True,
                    'narrative': "No data was found matching your query criteria.",
                    'summary': "No data returned from the query."
                }

            row_count = len(df)
            columns = list(df.columns)
            column_count = len(columns)
            total_count = row_count * column_count

            # # Skip LLM call for single row - not enough data for insights
            # if row_count == 1:
            #     return {
            #         'success': True,
            #         'narrative': "Not many rows to generate insights.",
            #         'summary': "Not many rows to generate insights."
            #     }

            if total_count > 5000:
                return {
                    'success': True,
                    'narrative': "Too many records to synthesize.",
                    'summary': "Dataset too large for comprehensive analysis."
                }

            has_multiple_records = row_count > 1
            has_date_columns = any('date' in col.lower() or 'month' in col.lower() or 'year' in col.lower() or 'quarter' in col.lower() for col in columns)
            has_numeric_columns = False
            
            # Check for numeric columns in DataFrame
            for col in df.columns:
                if df[col].dtype in ['int64', 'float64'] or df[col].astype(str).str.contains(r'^\d+\.?\d*$', na=False).any():
                    has_numeric_columns = True
                    break

            # Convert DataFrame to clean string representation for LLM
            df_string = df.to_string(index=False, max_rows=2000)

            synthesis_prompt = f"""
            You are a Healthcare Finance Data Analyst. Create concise, meaningful insights from SQL results.

USER QUESTION: "{question}"
DATA: {row_count} rows, {', '.join(columns)}
**Query Output**: 
{df_string}

**ADAPTIVE ANALYSIS RULES**:

**FOR LIMITED DATA (1-2 rows OR insufficient data for patterns)**:
- Report only factual data points and direct answers to the question
- NO forced analysis categories when data doesn't support them
- Keep insights concise and practical

**FOR RICH DATA (3+ rows with meaningful patterns)**:
- Apply relevant analysis types: TREND, PATTERN, ANOMALY, DRIVER, COMPARATIVE
- Only include analysis types that the data actually supports
- Prioritize business significance

**OUTPUT GUIDELINES**:
- Bullets: ≤20 words each, focus on business value
- Use exact data names, auto-scale: ≥1B→x.xB, ≥1M→x.xM, ≥1K→x.xK
- CRITICAL: Use canonical business names for readability while maintaining context
    • Use "Drug MOUNJARO" instead of "drug_name MOUNJARO" 
    • Use "Client MDOVA" instead of "client_name MDOVA"
    • Use "revenue per script" instead of "revenue_per_script"
    • First mention: include attribute context (e.g., "Drug MOUNJARO"), subsequent mentions in same bullet: just value ("MOUNJARO")
- Use ONLY names/values present in the dataset - never invent or modify names
- Maintain attribute context for query generation while improving readability
- Summary: 1-2 sentences for limited data, 2-3 lines for rich data
- Skip empty or obvious statements

**RESPONSE FORMAT** (valid XML):
CRITICAL: Return ONLY the XML below with NO extra text, markdown, or formatting.
<analysis>
<insights_analysis>
• Key finding 1 (only include meaningful insights)
• Key finding 2 (if data supports additional insights)
• Additional insights only if data is rich enough
</insights_analysis>
<summary>
Concise summary appropriate to data richness - brief for limited data, detailed for comprehensive data.
</summary>
</analysis>
            """

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    
                    llm_response = await self.db_client.call_claude_api_endpoint_async([
                        {"role": "user", "content": synthesis_prompt}
                    ])
                    
                    print('narrative output', llm_response)
                    # Extract insights and summary from XML response using regex
                    insights_match = re.search(r'<insights_analysis>(.*?)</insights_analysis>', llm_response, re.DOTALL)
                    summary_match = re.search(r'<summary>(.*?)</summary>', llm_response, re.DOTALL)
                    
                    insights = insights_match.group(1).strip() if insights_match else ""
                    summary = summary_match.group(1).strip() if summary_match else ""
                    
                    if not insights or len(insights) < 10:
                        raise ValueError("Empty or insufficient insights in XML response")
                    
                    return {
                        'success': True,
                        'narrative': insights,
                        'summary': summary
                    }

                except Exception as e:
                    print(f"❌ Narrative synthesis attempt {attempt + 1} failed: {str(e)}")
                    
                    if attempt < max_retries - 1:
                        print(f"🔄 Retrying narrative synthesis... (Attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(2 ** attempt)
            
            return {
                'success': False,
                'error': f"Narrative synthesis failed after {max_retries} attempts due to Model errors"
            }
            
        except Exception as e:
            error_msg = f"Failed to synthesize narrative: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }
    
    def _json_to_dataframe(self, json_data) -> pd.DataFrame:
        """Convert JSON response to pandas DataFrame with proper numeric formatting"""
        
        try:
            if isinstance(json_data, list):
                # List of dictionaries
                df = pd.DataFrame(json_data)
            elif isinstance(json_data, dict):
                if 'data' in json_data:
                    df = pd.DataFrame(json_data['data'])
                elif 'result' in json_data:
                    df = pd.DataFrame(json_data['result'])
                else:
                    # Single record
                    df = pd.DataFrame([json_data])
            else:
                # Fallback
                df = pd.DataFrame()
            
            # Apply numeric formatting to all columns
            if not df.empty:
                df = self._format_dataframe_values(df)
            
            print(f"📊 Created DataFrame: {df.shape[0]} rows x {df.shape[1]} columns")
            return df
            
        except Exception as e:
            print(f"❌ DataFrame conversion failed: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error
    
    def _format_dataframe_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format DataFrame values with proper numeric handling"""
        
        def format_value(val):
            # Handle ISO date strings
            if isinstance(val, str) and 'T' in val and val.endswith('Z'):
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
                    return dt.strftime('%Y-%m-%d')
                except Exception:
                    return val
            
            # PRIORITY 1: Handle scientific notation FIRST (before any other checks)
            elif isinstance(val, str) and ('E' in val.upper() or 'e' in val):
                try:
                    numeric_val = float(val)
                    # Always format as integer with commas for scientific notation
                    return f"{int(round(numeric_val)):,}"
                except Exception:
                    return str(val)
            
            # PRIORITY 2: Detect client codes and other non-numeric strings that contain letters
            elif isinstance(val, str) and not val.replace('.', '').replace('-', '').replace(',', '').replace('E', '').replace('e', '').isdigit():
                # If string contains any letters (except E/e which we handled above), keep as-is
                return val
            
            # Handle year values (don't format as decimals)
            elif isinstance(val, str) and val.isdigit() and len(val) == 4:
                return val  # Keep year as is
            
            # Handle client codes that might be pure numbers but should stay as strings
            elif isinstance(val, str) and val.isdigit() and len(val) <= 6:
                # For short numeric strings (likely IDs/codes), keep as-is without decimals
                return val
                
            # Handle month values (1-12, don't add decimals)
            elif isinstance(val, str) and val.replace('.', '').isdigit():
                try:
                    numeric_val = float(val)
                    if 1 <= numeric_val <= 12 and '.' not in val:  # Month value
                        return str(int(numeric_val))
                    elif numeric_val > 1000:  # Large numbers like revenue
                        return f"{numeric_val:,.3f}"
                    else:
                        return f"{numeric_val:.3f}"
                except Exception:
                    return val
            
            # Handle large numeric strings (like your revenue values) - ONLY for actual decimals
            elif isinstance(val, str) and val.replace('.', '').replace('-', '').isdigit() and '.' in val:
                try:
                    numeric_val = float(val)
                    if numeric_val > 1000:  # Format large numbers with commas
                        return f"{numeric_val:,.3f}"
                    else:
                        return f"{numeric_val:.3f}"
                except Exception:
                    return val
            
            # Handle already formatted dollar amounts (with commas)
            elif isinstance(val, str) and val.startswith('$') and ',' in val:
                return val
            # Handle already formatted strings (with commas)
            elif isinstance(val, str) and ',' in val:
                return val
            # Handle actual numeric types (int, float)
            elif isinstance(val, (int, float)):
                try:
                    # Check if it's a year value (4-digit number in reasonable year range)
                    if val == int(val) and 1900 <= int(val) <= 2100:
                        return str(int(val))  # Keep years without commas
                    
                    if abs(val) >= 1000:  # Large numbers
                        if val == int(val):  # If it's a whole number
                            return f"{int(val):,}"  # Format as integer with commas
                        else:
                            return f"{val:,.3f}"  # Format with 3 decimals
                    else:
                        if val == int(val):  # If it's a whole number
                            return str(int(val))  # No decimals for small whole numbers
                        else:
                            return f"{val:.3f}"
                except Exception:
                    return str(val)
            elif pd.api.types.is_numeric_dtype(type(val)):
                try:
                    if pd.notna(val):
                        # Check if it's a year value (4-digit number in reasonable year range)
                        if float(val) == int(float(val)) and 1900 <= int(float(val)) <= 2100:
                            return str(int(float(val)))  # Keep years without commas
                        
                        if abs(float(val)) >= 1000:
                            if float(val) == int(float(val)):  # Whole number
                                return f"{int(float(val)):,}"
                            else:
                                return f"{float(val):,.3f}"
                        else:
                            if float(val) == int(float(val)):  # Whole number
                                return str(int(float(val)))
                            else:
                                return f"{float(val):.3f}"
                    else:
                        return ""
                except Exception:
                    return str(val)
            else:
                return val
        
        # Apply formatting to all columns
        for col in df.columns:
            df[col] = df[col].apply(format_value)
        
        return df
    
    
