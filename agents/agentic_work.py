 %run ./.setup/learner_setup  
import os
from dotenv import load_dotenv
import httpx
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain.vectorstores import Chroma
from langchain_core.tools import tool
from IPython.display import display, Markdown
from tenacity import retry, stop_after_attempt, wait_random_exponential

def get_access_token():
    '''
    Retrieves an Azure AD access token using the Client Credentials flow.
    Uses secrets stored in the workspace (Databricks example).
    Returns the bearer token string.
    '''
    auth = "https://api.uhg.com/oauth2/token"
    scope = "https://api.uhg.com/.default"
    grant_type = "client_credentials"
    with httpx.Client() as client:
        body = {
            "grant_type": grant_type,
            "scope": scope,
            "client_id": dbutils.secrets.get(scope="AIML_Training", key="client_id"),
            "client_secret": dbutils.secrets.get(scope="AIML_Training", key="client_secret"),
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        resp = client.post(auth, headers=headers, data=body, timeout=60)
        return resp.json()["access_token"]

load_dotenv('./Data/UAIS_vars.env')
AZURE_OPENAI_ENDPOINT = os.environ["AZURE_OPENAI_ENDPOINT"]
OPENAI_API_VERSION = os.environ["OPENAI_API_VERSION"]
EMBEDDINGS_DEPLOYMENT_NAME = os.environ["EMBEDDINGS_DEPLOYMENT_NAME"]
MODEL_DEPLOYMENT_NAME = os.environ["MODEL_DEPLOYMENT_NAME"]
PROJECT_ID = os.environ['PROJECT_ID']

chat_client = AzureChatOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=OPENAI_API_VERSION,
    azure_deployment=MODEL_DEPLOYMENT_NAME,
    temperature=0,
    azure_ad_token=get_access_token(),
    default_headers={"projectId": PROJECT_ID}
)

insurance_policies_df = pd.read_json("./Data/insurance_policies.json")
references_codes_df = pd.read_json("./Data/reference_codes.json")
test_records_df = pd.read_json("./Data/test_records.json")
validation_records_df = pd.read_json("./Data/validation_records.json")

display(validation_records_df.head())

def add_age_column(df: pd.DataFrame,
                   dob_col: str = "date_of_birth",
                   dos_col: str = "date_of_service",
                   out_col: str = "age") -> pd.DataFrame:
    df = df.copy()
    dob = pd.to_datetime(df[dob_col], errors="coerce")
    dos = pd.to_datetime(df[dos_col], errors="coerce")
    had_birthday = ( (dos.dt.month > dob.dt.month) | ((dos.dt.month == dob.dt.month) & (dos.dt.day >= dob.dt.day)) )
    age = (dos.dt.year - dob.dt.year) - (~had_birthday).astype("Int64")
    valid_mask = dob.notna() & dos.notna()
    age = age.where(valid_mask)
    age = age.mask((valid_mask) & (dos < dob))
    df[out_col] = age.astype("Int64")
    return df

test_records_df       = add_age_column(test_records_df)
validation_records_df = add_age_column(validation_records_df)


display(test_records_df.head())

def _extract_map_from_df(ref_df: pd.DataFrame, system: str) -> dict:
    if {"code_system", "code", "description"}.issubset(ref_df.columns):
        sub = ref_df.loc[ref_df["code_system"].astype(str) == system, ["code", "description"]].copy()
        sub["code"] = sub["code"].astype(str)
        sub["description"] = sub["description"].astype(str)
        return dict(sub.drop_duplicates(subset=["code"], keep="last").values)
    if system in ref_df.columns:
        ser = ref_df[system].dropna()
        if ser.map(lambda v: isinstance(v, dict)).any():
            combined = {}
            for d in ser[ser.map(lambda v: isinstance(v, dict))]:
                combined.update({str(k): str(v) for k, v in d.items()})
            return combined
        ser = ser.astype(str)
        return {str(idx): str(val) for idx, val in ser.items()}
    return {}

def build_procedure_and_diagnosis_tables(references_codes_df: pd.DataFrame):
    cpt_map   = _extract_map_from_df(references_codes_df, "CPT")
    icd10_map = _extract_map_from_df(references_codes_df, "ICD10")
    procedure_codes_df = (
        pd.DataFrame(list(cpt_map.items()), columns=["procedure_codes", "CPT"])
        .astype({"procedure_codes": "string", "CPT": "string"})
        .sort_values("procedure_codes", kind="stable")
        .reset_index(drop=True)
    )
    diagnosis_codes_df = (
        pd.DataFrame(list(icd10_map.items()), columns=["diagnosis_codes", "ICD10"])
        .astype({"diagnosis_codes": "string", "ICD10": "string"})
        .sort_values("diagnosis_codes", kind="stable")
        .reset_index(drop=True)
    )
    return procedure_codes_df, diagnosis_codes_df

procedure_codes_df, diagnosis_codes_df = build_procedure_and_diagnosis_tables(references_codes_df)


display(procedure_codes_df.head())

def build_procedure_codes_db(procedure_codes_df: pd.DataFrame) -> Dict[str, str]:
    return dict(zip(procedure_codes_df["procedure_codes"].astype(str), procedure_codes_df["CPT"].astype(str)))

def build_diagnosis_codes_db(diagnosis_codes_df: pd.DataFrame) -> Dict[str, str]:
    return dict(zip(diagnosis_codes_df["diagnosis_codes"].astype(str), diagnosis_codes_df["ICD10"].astype(str)))

patient_record_db: Dict[str, Dict[str, Any]] = {}
procedure_codes_db = build_procedure_codes_db(procedure_codes_df)
diagnosis_codes_db = build_diagnosis_codes_db(diagnosis_codes_df)


print(list(procedure_codes_db.items())[:5])

def _parse_date_to_iso(s: Any) -> Optional[str]:
    if s is None or (isinstance(s, float) and pd.isna(s)) or (isinstance(s, str) and not s.strip()):
        return None
    if isinstance(s, pd.Timestamp):
        return s.date().isoformat()
    if isinstance(s, (datetime, )):
        return s.date().isoformat()
    if isinstance(s, str):
        s = s.strip()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except Exception:
                pass
        if s.isdigit() and len(s) == 8:
            try:
                return datetime.strptime(s, "%Y%m%d").date().isoformat()
            except Exception:
                pass
        try:
            return datetime.fromisoformat(s).date().isoformat()
        except Exception:
            return None
    return None

def _normalize_bool(v: Any) -> Optional[bool]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, )):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"true", "t", "yes", "y", "1"}: return True
        if s in {"false", "f", "no", "n", "0"}: return False
    return None

def _split_codes(val: Any) -> List[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)): return []
    if isinstance(val, (list, tuple, set)): return [str(x).strip() for x in val if str(x).strip()]
    if isinstance(val, dict): return [str(k).strip() for k in val.keys() if str(k).strip()]
    if isinstance(val, str): return [p.strip() for p in val.replace(",", " ").split() if p.strip()]
    return []

def _first_non_empty(row: pd.Series, candidates: List[str]) -> Any:
    for c in candidates:
        if c in row and row[c] is not None:
            v = row[c]
            if isinstance(v, float) and pd.isna(v): continue
            if isinstance(v, str) and not v.strip(): continue
            return v
    return None

def _compute_age_completed_years(dob_iso: Optional[str], dos_iso: Optional[str]) -> Optional[int]:
    try:
        if not dob_iso or not dos_iso: return None
        dob = datetime.strptime(dob_iso, "%Y-%m-%d").date()
        dos = datetime.strptime(dos_iso, "%Y-%m-%d").date()
        if dos < dob: return None
        years = dos.year - dob.year
        return years if (dos.month, dos.day) >= (dob.month, dob.day) else years - 1
    except Exception:
        return None

def _to_float_usd(v: Any) -> Optional[float]:
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, str):
        s = v.strip().replace(",", "")
        for sym in ["USD", "usd", "$", "₹"]: s = s.replace(sym, "")
        try: return float(s.strip())
        except Exception: return None
    return None

def build_patient_record_db_from_df(combined_records_df: pd.DataFrame,
                                    key_priority: List[str] = None,
                                    duplicate_policy: str = "last") -> Dict[str, Dict[str, Any]]:
    if key_priority is None:
        key_priority = ["claim_id", "encounter_id", "visit_id", "bill_id", "patient_id"]
    name_cols       = ["name", "patient_name", "full_name"]
    gender_cols     = ["gender", "sex"]
    dob_cols        = ["date_of_birth", "dob", "birth_date"]
    dos_cols        = ["date_of_service", "dos", "service_date", "encounter_date", "visit_date"]
    policy_cols     = ["insurance_policy_id", "policy_id", "policyID", "insurance_policy"]
    billed_cols     = ["billed_amount_usd", "amount_billed_usd", "billed_amount", "total_charge_usd", "total_charge", "amount"]
    preauth_req_cols= ["preauthorization_required", "preauth_required", "prior_auth_required", "authorization_required"]
    preauth_obt_cols= ["preauthorization_obtained", "preauth_obtained", "prior_auth_obtained", "authorization_obtained"]
    diag_code_cols  = ["diagnosis_codes", "diagnoses", "icd10_codes", "icd10"]
    proc_code_cols  = ["procedure_codes", "procedures", "cpt_codes", "cpt"]
    db: Dict[str, Dict[str, Any]] = {}
    for i, row_dict in enumerate(combined_records_df.to_dict(orient="records")):
        row = pd.Series(row_dict)
        record_key = None
        for k in key_priority:
            v = _first_non_empty(row, [k])
            if v is not None:
                record_key = str(v); break
        if record_key is None: record_key = f"REC-{i+1:06d}"
        dob_iso = _parse_date_to_iso(_first_non_empty(row, dob_cols))
        dos_iso = _parse_date_to_iso(_first_non_empty(row, dos_cols))
        age_val = _first_non_empty(row, ["age", "patient_age"])
        try:
            age = int(age_val) if age_val is not None and str(age_val).strip().isdigit() else None
        except Exception:
            age = None
        if age is None: age = _compute_age_completed_years(dob_iso, dos_iso)
        rec = {
            "record_key": record_key,
            "claim_id": _first_non_empty(row, ["claim_id", "encounter_id", "visit_id", "bill_id"]),
            "patient_id": _first_non_empty(row, ["patient_id", "member_id", "beneficiary_id"]),
            "name": _first_non_empty(row, name_cols),
            "gender": _first_non_empty(row, gender_cols),
            "date_of_birth": dob_iso,
            "age": age,
            "date_of_service": dos_iso,
            "billed_amount_usd": _to_float_usd(_first_non_empty(row, billed_cols)),
            "insurance_policy_id": _first_non_empty(row, policy_cols),
            "diagnosis_codes": _split_codes(_first_non_empty(row, diag_code_cols)),
            "procedure_codes": _split_codes(_first_non_empty(row, proc_code_cols)),
            "preauthorization_required": _normalize_bool(_first_non_empty(row, preauth_req_cols)),
            "preauthorization_obtained": _normalize_bool(_first_non_empty(row, preauth_obt_cols)),
        }
        if duplicate_policy == "first" and record_key in db: continue
        db[record_key] = rec
    return db

combined_records_df = pd.concat([validation_records_df, test_records_df], ignore_index=True)
patient_record_db = build_patient_record_db_from_df(combined_records_df)


print(len(patient_record_db))

def build_insurance_policies_db(insurance_policies_df) -> dict:
    db = {}
    for _, row in insurance_policies_df.iterrows():
        policy_id = str(row.get("policy_id"))
        plan_name = row.get("plan_name")
        proc_map = {}
        for r in (row.get("covered_procedures") or []):
            code = str(r.get("procedure_code"))
            proc_map.setdefault(code, []).append({
                "covered_diagnoses": [str(x) for x in r.get("covered_diagnoses", [])],
                "age_range": tuple(r.get("age_range") or (None, None)),
                "gender": (r.get("gender") or "Any"),
                "requires_preauthorization": bool(r.get("requires_preauthorization", False)),
                "notes": r.get("notes", "")
            })
        db[policy_id] = {"plan_name": plan_name, "covered_procedures": proc_map}
    return db

insurance_policies_db = build_insurance_policies_db(insurance_policies_df)


print(list(insurance_policies_db.keys())[:5])

@tool
def summarize_patient_record(record_key: str) -> dict:
    record = patient_record_db.get(record_key)
    if not record:
        return {"error": f"Record {record_key} not found."}
    diagnoses = [f"- {code}: {diagnosis_codes_db.get(code, 'Description not found')}" for code in record.get("diagnosis_codes", [])] or ["- Not provided"]
    procedures = [f"- {code}: {procedure_codes_db.get(code, 'Description not found')}" for code in record.get("procedure_codes", [])] or ["- Not provided"]
    summary = f"""
1) Patient Demographics
- Name: {record.get('name', 'Not provided')}
- Gender: {record.get('gender', 'Not provided')}
- Age: {record.get('age', 'Not provided')}

2) Insurance Policy ID
- {record.get('insurance_policy_id', 'Not provided')}

3) Diagnoses and Descriptions
{chr(10).join(diagnoses)}

4) Procedures and Descriptions
{chr(10).join(procedures)}

5) Preauthorization Status
- Required: {'Yes' if record.get('preauthorization_required') else 'No'}
- Obtained: {'Yes' if record.get('preauthorization_obtained') else 'No'}

6) Billed Amount (in USD)
- {record.get('billed_amount_usd', 'Not provided')}

7) Date of Service
- {record.get('date_of_service', 'Not provided')}
""".strip()
    return {"summary": summary}

rec_out = summarize_patient_record("S010")

@tool
def summarize_policy_guideline(policy_id: str) -> dict:
    policy = insurance_policies_db.get(policy_id)
    if not policy:
        return {"error": f"Policy {policy_id} not found."}
    plan_name = policy.get("plan_name", "Not provided")
    proc_rules = policy.get("covered_procedures", {}) or {}
    def yes_no(v): return "Yes" if bool(v) else "No"
    def age_range_str(rule):
        rng = rule.get("age_range") or (None, None)
        if isinstance(rng, (list, tuple)) and len(rng) == 2:
            lo, hi = rng
        else:
            lo, hi = (None, None)
        if lo is None and hi is None: return "Not specified"
        return f"{'' if lo is None else lo}–{'' if hi is None else hi} years".strip("–")
    entries = []
    for pcode, rules in proc_rules.items():
        pdesc = procedure_codes_db.get(pcode, "Description not found")
        for r in (rules or []):
            diags = r.get("covered_diagnoses", []) or []
            diag_lines = "\n".join(f"  - {d}: {diagnosis_codes_db.get(d, 'Description not found')}" for d in diags) or "  - Not provided"
            entries.append(
                f"- Procedure: {pcode} — {pdesc}\n"
                f"  - Covered Diagnoses and Descriptions:\n"
                f"{diag_lines}\n"
                f"  - Gender Restriction: {r.get('gender', 'Any')}\n"
                f"  - Age Range: {age_range_str(r)}\n"
                f"  - Preauthorization Requirement: {yes_no(r.get('requires_preauthorization', False))}\n"
                f"  - Notes on Coverage: {r.get('notes', 'None') or 'None'}\n"
            )
    summary = f"""
• Policy Details
- Policy ID: {policy_id}
- Plan Name: {plan_name}

• Covered Procedures
{''.join(entries) if entries else '- None listed'}
""".strip()
    return {"summary": summary}

pol_out = summarize_policy_guideline("POL1001")

def check_claim_coverage(record_summary, policy_summary) -> dict:
    rec = record_summary.get("summary") if isinstance(record_summary, dict) else str(record_summary)
    pol = policy_summary.get("summary") if isinstance(policy_summary, dict) else str(policy_summary)
    if "chat_client" not in globals():
        return {"error": "LLM client not configured. Please set `chat_client` and try again."}
    prompt = f"""
You are a coverage analyst. Determine coverage for a single claimed procedure using ONLY the summaries below.

EVALUATION CRITERIA (all must be met to approve):
- Diagnosis: Patient diagnosis code(s) MUST include at least one of the policy-covered diagnoses for the claimed procedure.
- Procedure: The procedure code MUST be explicitly listed in the policy and meet all associated conditions.
- Age: Patient age MUST be within the policy's age range (lower bound inclusive, upper bound exclusive).
- Gender: Patient gender MUST match the policy’s gender requirement (or 'Any').
- Preauthorization: If the policy requires preauthorization, it MUST be obtained.

Constraints:
- Each patient has only one procedure. Evaluate only that procedure and only diagnoses explicitly listed in the patient record.
- If any required information is missing or ambiguous, choose "ROUTE FOR REVIEW".

OUTPUT FORMAT: Return EXACTLY these three sections, in order:

Coverage Review
- Step-by-step analysis for the claimed procedure covering: code match, diagnosis match, age range check, gender check, preauthorization check.

Summary of Findings
- Bullet list stating which requirements were met or not met.

Final Decision
- Either "APPROVE" or "ROUTE FOR REVIEW" plus a one-sentence justification.

PATIENT RECORD SUMMARY:
{rec}

POLICY SUMMARY:
{pol}
""".strip()
    response = chat_client.invoke(prompt)
    return {"summary": response.content}

decision = check_claim_coverage(rec_out, pol_out)


print(decision["summary"])

from langgraph.prebuilt import create_react_agent

tools = [summarize_patient_record, summarize_policy_guideline, check_claim_coverage]
react_agent = create_react_agent(model=chat_client, tools=tools)

SYSTEM_PROMPT = """# SYSTEM — CLAIM COVERAGE AGENT (ReAct)

## Objective
Determine if a patient's single claimed procedure is covered under their insurance policy.

## Allowed Tools (only these three)
1) summarize_patient_record(record_key)
2) summarize_policy_guideline(policy_id)
3) check_claim_coverage(record_summary, policy_summary)

## REQUIRED SEQUENCE
Step 1 — Call summarize_patient_record(record_key)
  - Input: the record key (e.g., "P011").
  - Extract the Policy ID from section "2) Insurance Policy ID" in the returned summary text.
Step 2 — Call summarize_policy_guideline(policy_id)
  - Input: the Policy ID from Step 1.
  - If the Policy ID is missing/ambiguous, STOP and return final output with Decision=ROUTE FOR REVIEW and a clear Reason.
Step 3 — Call check_claim_coverage(record_summary, policy_summary)
  - Inputs: the full text outputs from Steps 1 and 2.

## EVALUATION RULES (must be enforced)
Approve only if ALL conditions are met:
- Diagnosis: Patient diagnosis code(s) include at least one policy-covered diagnosis for the claimed procedure.
- Procedure: The procedure code is explicitly listed in the policy and all associated conditions are satisfied.
- Age: Patient age in range (lower bound inclusive, upper bound exclusive).
- Gender: Matches policy requirement or policy = "Any".
- Preauthorization: If required by policy, it must have been obtained.

Constraints:
- Evaluate only the single procedure and diagnoses listed in the patient summary.
- If any required info is missing or unclear, choose ROUTE FOR REVIEW.

## FINAL OUTPUT (STRICT)
Return ONLY two lines, nothing else:
Decision: APPROVE | ROUTE FOR REVIEW
Reason: <Detailed justification including procedure name and code, diagnosis name and code, and specific policy conditions met or unmet. Clearly explain which criteria were satisfied and which were not, including age, gender, diagnosis, procedure, and preauthorization. End with a conclusive statement about why the claim is approved or routed for review.>

Do NOT reveal chain-of-thought or tool prompts. Be concise and factual.
"""

def run_claim_approval(user_input: str) -> str:
    messages = [("system", SYSTEM_PROMPT), ("user", user_input)]
    result = react_agent.invoke({"messages": messages})
    return result["messages"][-1].content.strip()

patient_ids = (
    test_records_df["patient_id"]
    .dropna()
    .astype(str)
    .drop_duplicates()
    .tolist()
)

rows = []
for pid in patient_ids:
    try:
        resp = run_claim_approval(pid)
    except Exception as e:
        resp = f"ERROR: {e}"
    rows.append({"patient_id": pid, "generated_response": str(resp)})

submission_df = pd.DataFrame(rows, columns=["patient_id", "generated_response"])
submission_df.to_csv("submission.csv", index=False, encoding="utf-8-sig")

display(submission_df.head(10))
print("✅ Saved submission.csv with", len(submission_df), "rows")


patient_ids = ["S001", "S002", "S003", "S004", "S005", "S006", "S007", "S008", "S009", "S010"]  

for pid in patient_ids:
    print("patients id: ",pid, run_claim_approval(pid))
    print("-------------------------------------------")


patient_ids = ["P011", "P012", "P013", "P014", "P015", "P016", "P017", "P018", "P019", "P020"] 

for pid in patient_ids:
    print("patients id: ",pid, run_claim_approval(pid))
    print("-------------------------------------------")
