SQL_FOLLOWUP_VALIDATION_PROMPT = """TASK: Classify the user's follow-up response to determine the next action.

CONTEXT:
- ORIGINAL USER QUESTION: {current_question}
- SELECTED DATASET: {selected_dataset_name}
- OTHER AVAILABLE DATASETS: {other_available_datasets}

CLARIFICATION EXCHANGE:
- YOUR PREVIOUS QUESTION: {sql_followup_question}
- USER'S RESPONSE: {sql_followup_answer}

CLASSIFICATION RULES:

1. **SIMPLE_APPROVAL**: User confirms/accepts OR selects from presented options without adding NEW constraints
   - Affirmative: "yes", "ok", "looks good", "proceed", "that's correct", "go ahead", "sure", "approved", "confirm"
   - Option selection: "option 1", "carrier_id", "revenue", "first one" (responses that pick from YOUR PREVIOUS QUESTION options)
   - Grouping preference: "show separately", "individually", "break it down", "by each" when referring to values ALREADY in the plan
   - Key: Response references content FROM your question OR simple affirmative with NO new additions

2. **APPROVAL_WITH_MODIFICATIONS**: User selects option BUT adds NEW constraints not in your question
   - Examples: "carrier_id, and also filter by Q1 2024", "option 1 but only top 10", "revenue and break down by client"
   - Key: Option selection/approval + NEW criteria that adds filters, time periods, or dimensions NOT already in your question
   - ⚠️ NOT a modification: Asking to show EXISTING values separately/individually (e.g., "show HDP and SP individually" when both already included) → classify as SIMPLE_APPROVAL

3. **DATASET_CHANGE_REQUEST**: User explicitly requests different dataset(s)
   - Must mention dataset name from OTHER AVAILABLE DATASETS
   - Examples: "Use Pharmacy IRIS Claims instead", "Switch to Rx Claims"
   - Key: Explicit dataset name + intent to change

4. **NEW_QUESTION**: User asks a completely different question instead of answering
   - Ignores the clarification and asks something unrelated to it
   - Examples: Asking about different metrics, different time periods not offered, different analysis entirely
   - Key indicator: Question mark, new analytical intent

5. **TOPIC_DRIFT**: User's response is off-topic or nonsensical
   - Response doesn't relate to clarification or business context
   - Includes requests for datasets NOT in available list
   - Examples: Random text, unavailable dataset requests, non-business content

OUTPUT FORMAT (return ONLY this XML, no other text):

<validation>
<classification>[SIMPLE_APPROVAL|APPROVAL_WITH_MODIFICATIONS|DATASET_CHANGE_REQUEST|NEW_QUESTION|TOPIC_DRIFT]</classification>
<modifications>[If APPROVAL_WITH_MODIFICATIONS: describe ONLY the NEW modifications not in previous question. Otherwise: none]</modifications>
<requested_datasets>[If DATASET_CHANGE_REQUEST: comma-separated dataset names. Otherwise: none]</requested_datasets>
<reasoning>[1 sentence explaining classification]</reasoning>
</validation>
"""
