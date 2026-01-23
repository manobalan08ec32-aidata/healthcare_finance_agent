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
                    # Update each query result with its narrative
                    updated_query_results = []
                    narratives = narrative_result.get('narratives', [])
                    
                    for idx, query_result in enumerate(query_results):
                        updated_query = query_result.copy()
                        if idx < len(narratives):
                            updated_query['narrative'] = narratives[idx]
                        updated_query_results.append(updated_query)
                    
                    # Update the sql_result with narratives
                    state['sql_result']['query_results'] = updated_query_results
                else:
                    # Update single result with narrative
                    state['sql_result']['narrative'] = narrative_result.get('narrative', '')
                    
                    # 🆕 UPDATE CONVERSATION MEMORY
                    if narrative_result.get('memory'):
                        current_memory = state.get('conversation_memory', {
                            'dimensions': {},
                            'analysis_context': {
                                'current_analysis_type': None,
                                'analysis_history': []
                            }
                        })
                        
                        new_memory = narrative_result['memory']
                        
                        # Merge memory intelligently
                        updated_memory = self._merge_memory(current_memory, new_memory)
                        state['conversation_memory'] = updated_memory
                        
                        print(f"✅ Conversation memory updated")
                
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
        
        for idx, query_result in enumerate(query_results):
            title = query_result.get('title', f'Query {idx+1}')
            sql_query = query_result.get('sql_query', '')
            data = query_result.get('data', [])
            
            print(f"  🔄 Processing narrative for: {title}")
            
            narrative_result = await self._synthesize_single_narrative_async(data, f"{question} - {title}", sql_query)
            
            if narrative_result['success']:
                narratives.append(narrative_result['narrative'])
            else:
                narratives.append(f"Narrative generation failed for {title}")
        
        return {
            'success': True,
            'narratives': narratives
        }
    
    async def _synthesize_single_narrative_async(self, sql_data: List[Dict], question: str, sql_query: str) -> Dict[str, Any]:
        """Generate narrative for a single SQL query result with memory extraction"""
        
        if not sql_data:
            return {
                'success': True,
                'narrative': "No data was found matching your query criteria.",
                'memory': {
                    'dimensions': {},
                    'analysis_context': {
                        'current_analysis_type': None,
                        'analysis_history': []
                    }
                },
                'chart': {'render': False, 'reason': 'No data available'}
            }
        
        try:
            # Convert SQL data to DataFrame for cleaner handling
            df = self._json_to_dataframe(sql_data)
            
            if df.empty:
                return {
                    'success': True,
                    'narrative': "No data was found matching your query criteria.",
                    'memory': {
                        'dimensions': {},
                        'analysis_context': {
                            'current_analysis_type': None,
                            'analysis_history': []
                        }
                    },
                    'chart': {'render': False, 'reason': 'No data available'}
                }

            row_count = len(df)
            columns = list(df.columns)
            column_count = len(columns)
            total_count = row_count * column_count

            if total_count > 5000:
                return {
                    'success': True,
                    'narrative': "Too many records to synthesize.",
                    'memory': {
                        'dimensions': {},
                        'analysis_context': {
                            'current_analysis_type': None,
                            'analysis_history': []
                        }
                    },
                    'chart': {'render': False, 'reason': 'Too many records for visualization'}
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
You are a Healthcare Finance Data Analyst. Create concise, meaningful insights from SQL results AND determine if a chart would be valuable.

USER QUESTION: "{question}"
DATA: {row_count} rows, {', '.join(columns)}
**Query Output**:
{df_string}

**ANALYSIS RULES**:
- Limited data (1-2 rows): Report facts only, no trend analysis
- Adequate data (3+ rows): Identify trends, patterns, top performers, outliers

**STYLE**:
- Professional business language, concise but informative
- Bold key metrics and values for scannability
- Focus on TOP 5-6 most significant entities when discussing dimensions
- Avoid generic statements, provide actionable insights

**OUTPUT FORMAT**:
<insights>
[Your narrative - focus on TOP 5-6 most significant entities mentioned here]
</insights>

<memory>
{{
  "dimensions": {{"column_name": ["val1", "val2", "val3", "val4", "val5"]}},
  "analysis_context": {{"current_analysis_type": "metric_name", "analysis_history": []}}
}}
</memory>

<chart>
{{
  "render": true|false,
  "chart_type": "bar_horizontal"|"bar_vertical"|"bar_grouped"|"bar_variance"|"line"|"line_multi"|"pie"|"scatter"|"area"|"none",
  "title": "[Clear title - max 60 chars]",
  "x_column": "[exact column name from DataFrame]",
  "y_column": "[exact column name]" or ["col1", "col2"] for multi-series,
  "x_label": "[axis label]",
  "y_label": "[axis label]",
  "sort_by": "value_desc"|"value_asc"|"label_asc"|"none",
  "show_values": true|false,
  "color_by_sign": true|false,
  "reason": "[1-2 sentences why this chart type or why no chart]"
}}
</chart>

**CHART GENERATION RULES**:

STEP 1 - DECIDE WHETHER TO VISUALIZE:

ROW COUNT INTELLIGENCE (use judgment, not strict rules):
| Rows | Default Decision | Exceptions |
|------|------------------|------------|
| 0-1 | NO chart | Never chart a single value |
| 2-3 | YES if meaningful | e.g., 3 LOBs for pie, 2 years comparison |
| 4-30 | YES (ideal range) | Most chart types work well |
| 31-50 | MAYBE | Bar charts get cluttered; Line/Area still OK |
| 50+ | Depends on chart type | Line/Area for time series = OK; Bar = skip |

Return render=false if:
- Data has 0-1 rows (nothing to visualize)
- No numeric columns exist
- Question is lookup/definitional ("what is X", "what is the code for Y")
- All values are nearly identical (no variation to show)
- Too many categories for bar chart (>30) AND not a time-based question

Return render=true if:
- Data has 2+ rows with at least one numeric column
- Question implies comparison ("top", "compare", "breakdown by", "vs")
- Question implies trends ("trend", "over time", "monthly", "yearly")
- Clear categorical vs. numeric relationship
- Time series data (even 100+ rows for line charts is OK)

STEP 2 - SELECT CHART TYPE:

| Data Pattern | Chart Type | When to Use |
|--------------|------------|-------------|
| Categories (3-15) + 1 metric | bar_horizontal | Rankings, top N |
| Categories (2-5) + 1 metric | bar_vertical | Small comparisons |
| Time dimension + 1 metric | line | Trends over time |
| Time dimension + 2+ metrics | line_multi | Multiple trends (actual vs forecast) |
| Categories + 2 metrics comparison | bar_grouped | Actuals vs forecast, budget vs actual |
| Variance/growth data (pos/neg values) | bar_variance | MoM change, % variance (red/green coloring) |
| Parts of whole (2-7 items) | pie | Percentage breakdowns |
| 2 numeric columns | scatter | Correlation |
| Time + cumulative metric | area | Volume trends |

COMPARISON CHART RULES:
- If question contains "vs", "compare", "actual", "forecast", "budget":
  - Time-based comparison → use line_multi (two lines overlaid)
  - Category-based comparison → use bar_grouped (side-by-side bars)

VARIANCE/GROWTH CHART RULES:
- If column contains "variance", "growth", "change", "delta", or has positive AND negative values:
  - Use bar_variance with color_by_sign: true
  - Red for negative values, green for positive values

COLUMN VALIDATION:
- x_column and y_column MUST be exact column names from: {columns}
- For multi-series (line_multi, bar_grouped), y_column is a list: ["col1", "col2"]


Return ONLY the XML with <insights> and <chart> tags, nothing else.
"""

            # Call LLM with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response_text = await self.db_client.generate_text_async(
                        prompt=synthesis_prompt,
                        max_tokens=2000,
                        temperature=0.3,
                        system_prompt="You are a healthcare finance analyst. Generate insights and extract memory in the specified XML format."
                    )
                    
                    # Extract insights
                    insights_match = re.search(r'<insights>(.*?)</insights>', response_text, re.DOTALL)
                    insights = insights_match.group(1).strip() if insights_match else ""
                    
                    if not insights or len(insights) < 10:
                        raise ValueError("Empty or insufficient insights in XML response")
                    
                    # Extract memory
                    memory_match = re.search(r'<memory>(.*?)</memory>', response_text, re.DOTALL)
                    memory_data = None
                    
                    if memory_match:
                        try:
                            memory_json_str = memory_match.group(1).strip()
                            # Clean up potential formatting issues
                            memory_json_str = memory_json_str.replace('```json', '').replace('```', '').strip()
                            memory_data = json.loads(memory_json_str)
                            
                            # Validate and ensure top 5 limit per dimension
                            if 'dimensions' in memory_data:
                                for dim_key, values in memory_data['dimensions'].items():
                                    if isinstance(values, list) and len(values) > 5:
                                        memory_data['dimensions'][dim_key] = values[:5]
                                        print(f"⚠️ Trimmed {dim_key} to top 5 values")
                            
                            print(f"✅ Memory extracted: {len(memory_data.get('dimensions', {}))} dimensions, metric: {memory_data.get('analysis_context', {}).get('current_analysis_type')}")
                        except Exception as e:
                            print(f"⚠️ Memory extraction failed: {e}")
                            memory_data = {
                                'dimensions': {},
                                'analysis_context': {
                                    'current_analysis_type': None,
                                    'analysis_history': []
                                }
                            }
                    else:
                        memory_data = {
                            'dimensions': {},
                            'analysis_context': {
                                'current_analysis_type': None,
                                'analysis_history': []
                            }
                        }

                    # Extract chart specification
                    chart_match = re.search(r'<chart>(.*?)</chart>', response_text, re.DOTALL)
                    chart_data = {'render': False, 'reason': 'No chart specification in response'}

                    if chart_match:
                        try:
                            chart_json_str = chart_match.group(1).strip()
                            # Clean up potential formatting issues
                            chart_json_str = chart_json_str.replace('```json', '').replace('```', '').strip()
                            chart_data = json.loads(chart_json_str)

                            # Validate chart specification if render is true
                            if chart_data.get('render', False):
                                x_col = chart_data.get('x_column')
                                y_col = chart_data.get('y_column')
                                valid_cols = list(df.columns)

                                # Validate x_column
                                if x_col and x_col not in valid_cols:
                                    print(f"⚠️ Invalid x_column '{x_col}'. Available: {valid_cols}")
                                    chart_data['render'] = False
                                    chart_data['reason'] = f"Invalid x_column: {x_col}"

                                # Validate y_column (can be string or list for multi-series)
                                elif y_col:
                                    if isinstance(y_col, str):
                                        if y_col not in valid_cols:
                                            print(f"⚠️ Invalid y_column '{y_col}'. Available: {valid_cols}")
                                            chart_data['render'] = False
                                            chart_data['reason'] = f"Invalid y_column: {y_col}"
                                    elif isinstance(y_col, list):
                                        invalid_cols = [c for c in y_col if c not in valid_cols]
                                        if invalid_cols:
                                            print(f"⚠️ Invalid y_columns: {invalid_cols}. Available: {valid_cols}")
                                            chart_data['render'] = False
                                            chart_data['reason'] = f"Invalid y_columns: {invalid_cols}"

                                if chart_data.get('render', False):
                                    print(f"✅ Chart: render=True, type={chart_data.get('chart_type')}, x={x_col}, y={y_col}")
                            else:
                                print(f"ℹ️ Chart: render=False, reason={chart_data.get('reason', 'Not specified')}")
                        except Exception as e:
                            print(f"⚠️ Chart extraction failed: {e}")
                            chart_data = {'render': False, 'reason': f'Chart parsing error: {str(e)}'}
                    else:
                        print("ℹ️ No <chart> block found in LLM response")

                    return {
                        'success': True,
                        'narrative': insights,
                        'memory': memory_data,
                        'chart': chart_data
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
    
    def _merge_memory(self, current: Dict, new: Dict) -> Dict:
        """
        Merge new memory with existing memory
        
        DIMENSIONS: Accumulate (merge new + existing, keep top 10 per dimension)
        ANALYSIS_TYPE: Replace current and track history (last 5)
        """
        
        merged = {
            'dimensions': {},
            'analysis_context': {
                'current_analysis_type': None,
                'analysis_history': []
            }
        }
        
        # ========================================
        # DIMENSIONS: ACCUMULATE (never clear)
        # ========================================
        all_dim_keys = set(list(current.get('dimensions', {}).keys()) + 
                           list(new.get('dimensions', {}).keys()))
        
        for dim_key in all_dim_keys:
            current_values = current.get('dimensions', {}).get(dim_key, [])
            new_values = new.get('dimensions', {}).get(dim_key, [])
            
            # Merge: new values first (most recent), then existing
            # Deduplicate case-insensitively
            combined = []
            seen = set()
            
            for value in new_values + current_values:
                value_upper = str(value).upper()
                if value_upper not in seen:
                    seen.add(value_upper)
                    combined.append(value)
            
            # Limit to top 10 values per dimension
            merged['dimensions'][dim_key] = combined[:10]
        
        # ========================================
        # ANALYSIS TYPE: REPLACE + TRACK HISTORY
        # ========================================
        current_history = current.get('analysis_context', {}).get('analysis_history', [])
        old_analysis_type = current.get('analysis_context', {}).get('current_analysis_type')
        new_analysis_type = new.get('analysis_context', {}).get('current_analysis_type')
        
        if new_analysis_type:
            # Set new as current
            merged['analysis_context']['current_analysis_type'] = new_analysis_type
            
            # Add old current to history (if it exists and is different)
            if old_analysis_type and old_analysis_type != new_analysis_type:
                # Avoid consecutive duplicates in history
                if not current_history or current_history[-1] != old_analysis_type:
                    current_history.append(old_analysis_type)
            
            # Keep last 5 in history
            merged['analysis_context']['analysis_history'] = current_history[-5:]
        else:
            # If new analysis type is null/empty, keep existing
            merged['analysis_context']['current_analysis_type'] = old_analysis_type
            merged['analysis_context']['analysis_history'] = current_history
        
        print(f"📊 Memory merged - Dimensions: {list(merged['dimensions'].keys())}, Current metric: {merged['analysis_context']['current_analysis_type']}, History: {merged['analysis_context']['analysis_history']}")
        
        return merged
    
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
