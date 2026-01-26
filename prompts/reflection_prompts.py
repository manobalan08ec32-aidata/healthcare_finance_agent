"""
Reflection Agent Prompts

Contains all prompts used by the LLMReflectionAgent for:
- Diagnosis: Analyzing what went wrong with previous SQL
- Follow-up question generation: Asking user for clarification with available datasets
- Plan generation: Creating correction plans for user approval

STRUCTURE:
- DIAGNOSIS prompts: Single-call analysis + follow-up question generation
- PLAN prompts: Correction plan generation for DATASET_CHANGE scenarios
"""

# ═
# SYSTEM PROMPTS
# ═

REFLECTION_DIAGNOSIS_SYSTEM_PROMPT = """You are a healthcare finance SQL debugging assistant.
Your role is to analyze user corrections, diagnose issues with previous SQL queries,
and generate appropriate follow-up questions when the user's feedback is unclear."""

REFLECTION_PLAN_SYSTEM_PROMPT = """You are a healthcare finance SQL correction planner.
Your role is to create clear, user-friendly correction plans based on diagnosed issues."""


# ═
# PROMPT 1: DIAGNOSIS + FOLLOW-UP GENERATION (Single LLM Call)
# ═

REFLECTION_DIAGNOSIS_PROMPT = """
TASK: Diagnose what went wrong with the previous SQL query and determine next steps.

PREVIOUS CONTEXT

- Original Question: {original_question}
- Table Used: {previous_table_used}
- SQL Query:
{previous_sql}

- User's Correction Feedback: "{user_correction_feedback}"

AVAILABLE DATASETS FOR DOMAIN ({domain_selection})

{available_datasets_formatted}

DIAGNOSIS STEPS

STEP 1: Analyze user's correction feedback
Look for explicit mentions of:
- Different dataset/table → issue_type="WRONG_DATASET"
- Wrong filter/column → issue_type="WRONG_FILTER"
- Wrong calculation/aggregation → issue_type="WRONG_CALCULATION"
- Missing breakdown/grouping → issue_type="MISSING_COLUMN"
- Vague feedback ("wrong", "incorrect", "that's not right") → issue_type="UNCLEAR"

STEP 2: Determine correction path
Based on issue_type:
- WRONG_DATASET → correction_path="DATASET_CHANGE", needs_followup=false (if user specified dataset)
- WRONG_FILTER → correction_path="FILTER_FIX", needs_followup=false
- WRONG_CALCULATION → correction_path="STRUCTURE_FIX", needs_followup=false
- MISSING_COLUMN → correction_path="STRUCTURE_FIX", needs_followup=false
- UNCLEAR → correction_path="NEED_CLARIFICATION", needs_followup=true

STEP 3: Generate follow-up question (ONLY if needs_followup=true)
When user feedback is vague, generate a helpful follow-up question that:
- Shows the previous question and table used
- Lists available datasets (numbered, with current marker)
- Offers issue type options (1-4)
- Allows dataset selection or combinations

OUTPUT FORMAT (XML - no markdown, no code blocks)

<diagnosis>
<issue_type>[WRONG_DATASET|WRONG_FILTER|WRONG_CALCULATION|MISSING_COLUMN|UNCLEAR]</issue_type>
<issue_details>[Specific description of what's wrong - or "User did not specify" if unclear]</issue_details>
<correction_path>[DATASET_CHANGE|FILTER_FIX|STRUCTURE_FIX|NEED_CLARIFICATION]</correction_path>
<suggested_fix>[Specific fix if issue is clear, or empty if needs clarification]</suggested_fix>
<suggested_datasets>[Comma-separated list of dataset names if DATASET_CHANGE, empty otherwise]</suggested_datasets>
<needs_followup>[true|false]</needs_followup>
<followup_question>
[If needs_followup=true, generate this question. If needs_followup=false, leave empty]

Format for follow-up question when needs_followup=true:

I see you want to correct the previous answer. Let me help you fix it.

**Previous Question:** [original_question]
**Table Used:** [previous_table_used]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**AVAILABLE DATASETS:**

[List each dataset with number, marking the currently used one]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**WHAT TYPE OF ISSUE DID YOU NOTICE?**

1. Wrong dataset/table - Pick from datasets above or combine multiple
2. Wrong filter applied - Filter is on wrong column or wrong value
3. Wrong calculation - Metric aggregation is incorrect
4. Missing breakdown - Need additional grouping or columns

Please select an option number OR specify a dataset name.

</followup_question>
</diagnosis>


EXAMPLES


EXAMPLE 1: User specifies dataset (CLEAR - no follow-up)
User feedback: "Use Claims table instead of Ledger"
→ issue_type="WRONG_DATASET"
→ correction_path="DATASET_CHANGE"
→ suggested_datasets="Rx Claims"
→ needs_followup=false

EXAMPLE 2: User specifies filter issue (CLEAR - no follow-up)
User feedback: "Wrong filter, should be on drug_name column"
→ issue_type="WRONG_FILTER"
→ correction_path="FILTER_FIX"
→ suggested_fix="Change filter from therapy_class to drug_name column"
→ needs_followup=false

EXAMPLE 3: Vague feedback (UNCLEAR - needs follow-up)
User feedback: "That's wrong" or "The answer is incorrect"
→ issue_type="UNCLEAR"
→ correction_path="NEED_CLARIFICATION"
→ needs_followup=true
→ followup_question=[Full formatted question with datasets and options]
"""


# ═
# PROMPT 2: PLAN GENERATION (Only for DATASET_CHANGE)
# ═

REFLECTION_PLAN_PROMPT = """
TASK: Generate a correction plan based on diagnosis and user input.

CONTEXT

Original Question: {original_question}
Previous Table Used: {previous_table_used}
Issue Type: {issue_type}
Correction Path: {correction_path}
User's Dataset Selection: {selected_datasets}

NEW DATASET METADATA
{dataset_metadata}

MANDATORY COLUMNS
{mandatory_columns}

TASK

Generate a clear correction plan that the user can approve before execution.
The plan should explain what was wrong and what will change.

OUTPUT FORMAT

Generate the plan in this exact format (use the emoji headers as shown):

<plan_approval>
📋 Correction Plan Summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 What We're Fixing: [1-2 sentence description of the issue and correction]

📊 Dataset: [Dataset name(s) that will be used]

📌 What Will Change:
• [Change 1 - e.g., "Switching from Ledger table to Rx Claims table"]
• [Change 2 - if applicable]
• [Change 3 - if applicable]

🔍 Filters Applied:
• [Filter 1]
• [Filter 2]
• [Mandatory filter - include any mandatory columns from the dataset]

📈 How We'll Calculate:
• [Brief description of the query approach]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Options: ✅ Approve | ✏️ Modify | ❌ Cancel
</plan_approval>
"""


# ═
# PROMPT 3: PARSE USER FOLLOW-UP RESPONSE
# ═

REFLECTION_FOLLOWUP_ANALYSIS_PROMPT = """
TASK: Analyze the user's response to a follow-up question about correcting their SQL query.


CONTEXT

Original Question: {original_question}
Previous Table Used: {previous_table_used}
Previous SQL: {previous_sql}

Follow-up Question Asked:
{followup_question}

User's Response: "{user_response}"

Available Datasets:
{available_datasets_formatted}


ANALYSIS TASK


Analyze the user's response and determine:
1. Is the response relevant to the follow-up question? (topic drift detection)
2. What correction action should be taken based on their response?
3. If dataset change requested, which dataset(s) from the available list?
4. If filter/structure fix, what's the specific correction?

RELEVANCE CHECK:
- Relevant: User answers the question (selects dataset, specifies issue, provides correction details)
- Irrelevant: User asks unrelated question, changes topic, provides nonsensical answer


OUTPUT FORMAT (XML - no markdown, no code blocks)


<analysis>
<is_relevant>[true|false]</is_relevant>
<topic_drift_reason>[If not relevant, explain why user's response doesn't answer the question. Empty if relevant]</topic_drift_reason>
<action>[DATASET_CHANGE|FILTER_FIX|STRUCTURE_FIX|NEED_MORE_INFO|TOPIC_DRIFT]</action>
<issue_type>[WRONG_DATASET|WRONG_FILTER|WRONG_CALCULATION|MISSING_COLUMN|UNCLEAR]</issue_type>
<selected_datasets>[Comma-separated dataset functional names if DATASET_CHANGE, empty otherwise]</selected_datasets>
<correction_details>[Specific correction details if FILTER_FIX or STRUCTURE_FIX]</correction_details>
<user_intent>[Brief 1-sentence summary of what user wants]</user_intent>
<plan_summary>[2-3 sentence summary of proposed correction plan for user approval]</plan_summary>
</analysis>


EXAMPLES


EXAMPLE 1 - Dataset selection by name (relevant):
Follow-up: "Which dataset would you like to use instead? 1. Rx Claims 2. CBS Billing"
Response: "Use Claims"
→ is_relevant=true, action=DATASET_CHANGE, selected_datasets="Rx Claims", issue_type=WRONG_DATASET

EXAMPLE 2 - Numeric selection (relevant):
Follow-up: "Which dataset would you like to use instead? 1. Rx Claims 2. CBS Billing"
Response: "1"
→ is_relevant=true, action=DATASET_CHANGE, selected_datasets="Rx Claims", issue_type=WRONG_DATASET

EXAMPLE 3 - Filter correction (relevant):
Follow-up: "What type of issue did you notice?"
Response: "The filter should be on drug_name not therapy_class"
→ is_relevant=true, action=FILTER_FIX, issue_type=WRONG_FILTER, correction_details="Change filter from therapy_class to drug_name"

EXAMPLE 4 - Structure/calculation fix (relevant):
Follow-up: "What needs to be corrected?"
Response: "Need to sum by month instead of by day"
→ is_relevant=true, action=STRUCTURE_FIX, issue_type=WRONG_CALCULATION, correction_details="Change aggregation from daily to monthly"

EXAMPLE 5 - Topic drift (irrelevant):
Follow-up: "Which dataset would you like to use instead?"
Response: "What's the weather today?"
→ is_relevant=false, topic_drift_reason="User asked about weather instead of selecting a dataset", action=TOPIC_DRIFT

EXAMPLE 6 - Vague but on-topic (need more info):
Follow-up: "Which dataset would you like to use instead?"
Response: "I'm not sure, which one has member data?"
→ is_relevant=true, action=NEED_MORE_INFO, user_intent="User asking for guidance on which dataset contains member data"
"""


# ═
# PROMPT 4: LLM-BASED PLAN APPROVAL ANALYSIS
# ═

REFLECTION_PLAN_APPROVAL_PROMPT = """
TASK: Analyze the user's response to a correction plan and determine if they approve, reject, or want modifications.

CONTEXT

Original Question: {original_question}
Issue Being Corrected: {issue_type} - {correction_path}

Correction Plan Presented:
{plan_summary}

User's Response: "{user_response}"

ANALYSIS TASK

Determine if the user:
1. APPROVES the plan (wants to proceed with the correction)
2. REJECTS the plan (wants to cancel/stop the correction)
3. WANTS MODIFICATION (has additional requirements or changes to the plan)
4. TOPIC DRIFT (response is unrelated to the plan)

APPROVAL INDICATORS:
- Explicit: "yes", "ok", "approve", "go ahead", "proceed", "looks good", "that's fine"
- Implicit: Positive acknowledgment, confirmation phrases

REJECTION INDICATORS:
- Explicit: "no", "cancel", "stop", "abort", "never mind", "forget it"
- Implicit: Negative response, clear refusal

MODIFICATION INDICATORS:
- User adds requirements: "yes, but also include...", "can you also..."
- User corrects the plan: "no, I meant...", "not that, I want..."
- User provides additional details for the correction


OUTPUT FORMAT (XML - no markdown, no code blocks)

<plan_response>
<is_relevant>[true|false]</is_relevant>
<topic_drift_reason>[If not relevant, explain why. Empty if relevant]</topic_drift_reason>
<decision>[APPROVE|REJECT|MODIFY|TOPIC_DRIFT]</decision>
<modification_details>[If MODIFY, describe what changes user wants. Empty otherwise]</modification_details>
<additional_context>[Any additional context from user's response to pass to SQL generation]</additional_context>
<user_intent>[Brief summary of user's intent]</user_intent>
</plan_response>


EXAMPLES

EXAMPLE 1 - Clear approval:
Response: "Yes, that looks correct"
→ is_relevant=true, decision=APPROVE

EXAMPLE 2 - Approval with additional context:
Response: "Yes, go ahead but also filter by 2024 data only"
→ is_relevant=true, decision=MODIFY, modification_details="Add filter for year 2024"

EXAMPLE 3 - Rejection:
Response: "Never mind, let me start over"
→ is_relevant=true, decision=REJECT

EXAMPLE 4 - Modification request:
Response: "Not quite, I want the data grouped by month instead"
→ is_relevant=true, decision=MODIFY, modification_details="Change grouping from current to monthly"

EXAMPLE 5 - Topic drift:
Response: "What's the revenue for Q3?"
→ is_relevant=false, decision=TOPIC_DRIFT, topic_drift_reason="User asking new question instead of responding to plan"
"""


# ═
# HELPER: AVAILABLE DATASETS FORMATTER
# ═

def format_available_datasets(available_datasets: list, previous_table_used: str) -> str:
    """
    Format available datasets for display in diagnosis prompt.

    Args:
        available_datasets: List of dicts with 'functional_name' and 'table_name'
        previous_table_used: The table name that was used in the wrong answer

    Returns:
        Formatted string with numbered datasets and current marker
    """
    if not available_datasets:
        return "No datasets available"

    formatted_lines = []
    for idx, dataset in enumerate(available_datasets, 1):
        functional_name = dataset.get('functional_name', dataset.get('table_name', 'Unknown'))
        table_name = dataset.get('table_name', '')

        # Mark the currently used dataset
        is_current = table_name == previous_table_used
        marker = " ← (currently used)" if is_current else ""

        formatted_lines.append(f"  {idx}. {functional_name}{marker}")

    return "\n".join(formatted_lines)
