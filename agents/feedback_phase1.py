async def _llm_feedback_selection(self, feedback_results: List[Dict], state: AgentState) -> Dict:
    """
    Pure LLM-based selection of most relevant historical question from feedback results.
    Returns single best match or NO_MATCH status.
    """
    
    user_question = state.get('current_question', state.get('original_question', ''))
    
    # System prompt for feedback matching
    system_prompt = """
You are a SQL query matching expert. Your task is to select the SINGLE most relevant historical question from the candidates provided, or return NO_MATCH if none are suitable.

=== CORE MATCHING PRINCIPLES ===

1. SYNONYMS & VARIATIONS (Treat as identical):
   - "revenue" = "network revenue" = "product revenue" = "network product revenue"
   - "script count" = "volume" = "script volume" = "unadjusted scripts"
   - "Home Delivery" = "HDP" = "Mail"
   - "Specialty" = "SP"
   - These are THE SAME THING - match them as exact

2. DATES (Completely ignore):
   - "July 2025" vs "August 2024" → IGNORE, focus on pattern
   - "Q3 2025" vs "Q2 2024" → IGNORE, focus on comparison structure
   - "Jan-Sep 2025" vs "Apr-Jun 2024" → IGNORE, focus on date range pattern
   - What MATTERS: Is it a single period? Date range? Period comparison (YoY/QoQ/MoM)?

3. FILTER VALUES (Different values are OK):
   - "Specialty" vs "Home Delivery" → SAME PATTERN (product_category filter)
   - "client MDOVA" vs "client PDIND" → SAME PATTERN (client_id filter)
   - "GLP-1" vs "Oncology" → SAME PATTERN (therapy_class filter)
   - Different filter VALUES are fine, same filter TYPE is what matters

4. EXTRA METRICS IN SELECT (OK):
   - History: "script count, revenue for carrier MPDOVA"
   - Current: "script count for carrier MPDOVA"
   - → MATCH! Extra metrics in history are harmless, just ignore them

5. ENTITY OVER-SPECIFICATION (REJECT):
   - History: "revenue for client MDOVA"
   - Current: "revenue"
   - → REJECT! History has entity filter that current doesn't mention
   - Risk: Will confuse LLM to add unwanted filters

6. MISSING DIMENSIONS (REJECT):
   - History: "revenue"
   - Current: "revenue by line of business"
   - → REJECT! History lacks the GROUP BY structure needed
   - Risk: Won't help with dimensional breakdown

7. EXTRA DIMENSIONS IN HISTORY (REJECT):
   - History: "revenue by month"
   - Current: "revenue"
   - → REJECT! History has GROUP BY that current doesn't want
   - Risk: Will confuse LLM to add unwanted grouping

=== DECISION PROCESS ===

STEP 1: Check table compatibility
- Must be same table_name
- If different → REJECT immediately

STEP 2: Check metric compatibility
- Must have at least one overlapping metric (accounting for synonyms)
- "revenue" matches "network revenue" ✅
- "script count" matches "volume" ✅
- "revenue" does NOT match "script count" ❌

STEP 3: Check for over-specification
- Does history have entity filters (client, carrier, specific categories) that current doesn't mention?
- If YES → REJECT
- Example: History="revenue for PBM for E&I", Current="revenue for PBM" → REJECT

STEP 4: Check for missing dimensions
- Does current need GROUP BY dimensions (by drug, by client, by month) that history doesn't have?
- If YES → REJECT
- Example: History="revenue", Current="revenue by drug name" → REJECT

STEP 5: Check for extra dimensions
- Does history have GROUP BY dimensions that current doesn't mention?
- If YES → REJECT
- Example: History="revenue by month", Current="revenue" → REJECT

STEP 6: Pattern matching (for remaining candidates)
Look for BEST match based on:
A. EXACT pattern match (highest priority):
   - Same calculation type (variance, comparison, split, ratio)
   - Same comparison pattern (YoY, QoQ, MoM, vs previous)
   - Same dimensions (by drug_name, by client, etc.)
   - Same structure (side-by-side vs rows)

B. PARTIAL pattern match (if no exact):
   - Same metric and structure
   - Different filter values OK
   - Similar aggregation approach

C. If multiple good matches:
   - Prefer more recent (higher seq_id)
   - Prefer same filter categories
   - Prefer similar complexity

STEP 7: Final validation
Ask: "Will this history help or confuse the LLM?"
- If history has extra unwanted filters → CONFUSE → REJECT
- If history lacks needed structure → CONFUSE → REJECT  
- If pattern clearly transferable → HELP → ACCEPT

=== EXAMPLES ===

Example 1 - SYNONYM MATCH:
Current: "what is network revenue for PBM"
History: "what is revenue for PBM"
Decision: SELECT ✅
Reason: "network revenue" = "revenue" (synonym), same table, same filter, exact match

Example 2 - EXTRA METRIC OK:
Current: "script count for carrier MPDOVA"
History: "script count, revenue for carrier MPDOVA"  
Decision: SELECT ✅
Reason: Same entity filter, same dimensions, extra metric (revenue) in history is harmless

Example 3 - OVER-SPECIFICATION REJECT:
Current: "revenue for PBM"
History: "revenue for PBM for client MDOVA"
Decision: REJECT ❌
Reason: History has client filter that current doesn't mention - will confuse LLM

Example 4 - MISSING DIMENSION REJECT:
Current: "revenue by line of business"
History: "revenue"
Decision: REJECT ❌
Reason: History lacks GROUP BY dimension that current needs

Example 5 - EXTRA DIMENSION REJECT:
Current: "revenue for carrier MPDOVA"
History: "revenue for carrier MPDOVA by month"
Decision: REJECT ❌
Reason: History has GROUP BY month that current doesn't want

Example 6 - FILTER VALUE CHANGE OK:
Current: "script count for Specialty by drug name Q3 2025"
History: "script count for Home Delivery by drug name Q2 2024"
Decision: SELECT ✅
Reason: Same pattern (by drug_name, single quarter), different filter values are fine

Example 7 - PATTERN MATCH:
Current: "volume and revenue for GLP-1 by drug name Q3 2025 vs Q3 2024"
History: "volume and revenue for Oncology by drug name Q2 2025 vs Q2 2024"
Decision: SELECT ✅
Reason: Exact same structure (YoY comparison, by drug_name, volume+revenue), only filter value differs

Example 8 - NO MATCH:
Current: "top 10 clients with highest variance"
Candidates: 
  - "revenue by client"
  - "variance by therapy class"
Decision: NO_MATCH ❌
Reason: First lacks variance logic and top N, second lacks client dimension

=== OUTPUT FORMAT ===

IMPORTANT: Return ONLY valid JSON wrapped in <json> tags (no markdown, no extra text):

<json>
{
  "status": "match_found" or "no_match",
  "selected_seq_id": <number> or null,
  "matched_question": "<exact question text>" or null,
  "table_name": "<table name>" or null,
  "reasoning": "<detailed explanation of why selected or why NO_MATCH>",
  "pattern_match_level": "EXACT" or "PARTIAL" or "NONE"
}
</json>

Be conservative. If uncertain, return status: "no_match". Better to have no match than wrong match.
"""

    # Format feedback results as context
    candidates_context = "\n\n=== HISTORICAL QUESTIONS (CANDIDATES) ===\n"
    for idx, result in enumerate(feedback_results, 1):
        candidates_context += f"""
Candidate {idx}:
- seq_id: {result.get('seq_id', 'N/A')}
- table_name: {result.get('table_name', 'N/A')}
- question: {result.get('user_question', 'N/A')}
"""
    
    # Build the full prompt
    selection_prompt = f"""{system_prompt}

{candidates_context}

=== CURRENT QUESTION ===
{user_question}

Analyze step-by-step through the decision process and make your decision. Return only the JSON in <json> tags.
"""
    
    # Retry logic
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"🔍 Attempting feedback selection (attempt {retry_count + 1}/{max_retries})...")
            
            # Call LLM
            llm_response = await self.db_client.call_claude_api_endpoint_async([
                {"role": "user", "content": selection_prompt}
            ])
            
            print("Raw LLM response:", llm_response)
            
            # Extract JSON from response
            json_content = self._extract_json_from_response(llm_response)
            selection_result = json.loads(json_content)
            
            # Validate response structure
            status = selection_result.get('status')
            if status not in ['match_found', 'no_match']:
                raise ValueError(f"Invalid status returned: {status}")
            
            # Handle success cases
            if status == "match_found":
                print(f"✅ Feedback match found: seq_id={selection_result.get('selected_seq_id')}")
                print(f"   Matched question: {selection_result.get('matched_question')}")
                print(f"   Pattern level: {selection_result.get('pattern_match_level')}")
                print(f"   Reasoning: {selection_result.get('reasoning')}")
                
                return {
                    'status': 'match_found',
                    'seq_id': selection_result.get('selected_seq_id'),
                    'question': selection_result.get('matched_question'),
                    'table_name': selection_result.get('table_name'),
                    'reasoning': selection_result.get('reasoning'),
                    'pattern_match_level': selection_result.get('pattern_match_level'),
                    'error': False,
                    'error_message': ''
                }
            
            else:  # status == "no_match"
                print(f"❌ No suitable match found in feedback history")
                print(f"   Reasoning: {selection_result.get('reasoning')}")
                
                return {
                    'status': 'no_match',
                    'seq_id': None,
                    'question': None,
                    'table_name': None,
                    'reasoning': selection_result.get('reasoning'),
                    'pattern_match_level': 'NONE',
                    'error': False,
                    'error_message': ''
                }
        
        except json.JSONDecodeError as e:
            retry_count += 1
            print(f"⚠ JSON parsing failed (attempt {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count < max_retries:
                print(f"🔄 Retrying...")
                await asyncio.sleep(2 ** retry_count)
                continue
            else:
                print(f"❌ All retry attempts exhausted - JSON parsing failed")
                return {
                    'status': 'no_match',
                    'seq_id': None,
                    'question': None,
                    'table_name': None,
                    'reasoning': f"Failed to parse LLM response after {max_retries} attempts: {str(e)}",
                    'pattern_match_level': 'NONE',
                    'error': True,
                    'error_message': f"JSON parsing failed after {max_retries} attempts"
                }
        
        except Exception as e:
            retry_count += 1
            print(f"⚠ Feedback selection attempt {retry_count} failed: {str(e)}")
            
            if retry_count < max_retries:
                print(f"🔄 Retrying... ({retry_count}/{max_retries})")
                await asyncio.sleep(2 ** retry_count)
                continue
            else:
                print(f"❌ All retry attempts exhausted - feedback selection failed")
                return {
                    'status': 'no_match',
                    'seq_id': None,
                    'question': None,
                    'table_name': None,
                    'reasoning': f"Feedback selection failed after {max_retries} attempts: {str(e)}",
                    'pattern_match_level': 'NONE',
                    'error': True,
                    'error_message': f"LLM call failed after {max_retries} attempts: {str(e)}"
                }
    
    # Should never reach here, but just in case
    return {
        'status': 'no_match',
        'seq_id': None,
        'question': None,
        'table_name': None,
        'reasoning': 'Unexpected error in retry logic',
        'pattern_match_level': 'NONE',
        'error': True,
        'error_message': 'Unexpected error in retry logic'
    }
