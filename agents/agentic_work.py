"""
Insurance Claims Coverage Agent - AI Certification Project

This system uses LangGraph ReAct pattern to evaluate insurance claim coverage.
It orchestrates three tools (patient record lookup, policy lookup, coverage evaluation)
to make automated coverage decisions.
"""

# Notebook setup for Databricks environment
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

# ============================================================================
# AUTHENTICATION
# ============================================================================

def get_access_token():
    """
    Retrieves an Azure AD access token using the Client Credentials flow.
    
    Uses secrets stored in the workspace (Databricks example).
    
    Returns:
        str: Bearer token string for API authentication
        
    Raises:
        httpx.HTTPError: If the HTTP request fails
        KeyError: If access_token not found in response
    """
    auth = "https://api.uhg.com/oauth2/token"
    scope = "https://api.uhg.com/.default"
    grant_type = "client_credentials"
    
    try:
        with httpx.Client() as client:
            body = {
                "grant_type": grant_type,
                "scope": scope,
                "client_id": dbutils.secrets.get(scope="AIML_Training", key="client_id"),
                "client_secret": dbutils.secrets.get(scope="AIML_Training", key="client_secret"),
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            resp = client.post(auth, headers=headers, data=body, timeout=60)
            resp.raise_for_status()  # Raise exception for HTTP errors
            return resp.json()["access_token"]
    except httpx.HTTPError as e:
        raise RuntimeError(f"Failed to obtain access token: {e}")
    except KeyError:
        raise RuntimeError("Access token not found in authentication response")

# ============================================================================
# CONFIGURATION
# ============================================================================

# Load environment variables from configuration file
try:
    load_dotenv('./Data/UAIS_vars.env')
    AZURE_OPENAI_ENDPOINT = os.environ["AZURE_OPENAI_ENDPOINT"]
    OPENAI_API_VERSION = os.environ["OPENAI_API_VERSION"]
    EMBEDDINGS_DEPLOYMENT_NAME = os.environ["EMBEDDINGS_DEPLOYMENT_NAME"]
    MODEL_DEPLOYMENT_NAME = os.environ["MODEL_DEPLOYMENT_NAME"]
    PROJECT_ID = os.environ['PROJECT_ID']
except KeyError as e:
    raise RuntimeError(f"Missing required environment variable: {e}")

# Initialize Azure OpenAI chat client
# Temperature=0 for deterministic outputs (important for consistent coverage decisions)
chat_client = AzureChatOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=OPENAI_API_VERSION,
    azure_deployment=MODEL_DEPLOYMENT_NAME,
    temperature=0,  # Deterministic responses for consistency
    azure_ad_token=get_access_token(),
    default_headers={"projectId": PROJECT_ID}
)

# ============================================================================
# DATA LOADING
# ============================================================================

# Load all required data files with error handling
try:
    insurance_policies_df = pd.read_json("./Data/insurance_policies.json")
    references_codes_df = pd.read_json("./Data/reference_codes.json")
    test_records_df = pd.read_json("./Data/test_records.json")
    validation_records_df = pd.read_json("./Data/validation_records.json")
except FileNotFoundError as e:
    raise FileNotFoundError(f"Required data file not found: {e}")
except ValueError as e:
    raise ValueError(f"Invalid JSON format in data file: {e}")

display(validation_records_df.head())

# ============================================================================
# DATA PROCESSING - AGE CALCULATION
# ============================================================================

def add_age_column(df: pd.DataFrame,
                   dob_col: str = "date_of_birth",
                   dos_col: str = "date_of_service",
                   out_col: str = "age") -> pd.DataFrame:
    """
    Calculate patient age at date of service.
    
    Computes age in completed years, accounting for whether birthday has occurred
    by the date of service.
    
    Args:
        df: DataFrame containing patient records
        dob_col: Column name for date of birth
        dos_col: Column name for date of service
        out_col: Output column name for calculated age
        
    Returns:
        pd.DataFrame: Copy of input DataFrame with age column added
        
    Note:
        - Returns None for invalid dates or when DOS < DOB
        - Uses completed years (standard for insurance eligibility)
    """
    df = df.copy()
    
    # Parse dates with error handling (coerce invalid dates to NaT)
    dob = pd.to_datetime(df[dob_col], errors="coerce")
    dos = pd.to_datetime(df[dos_col], errors="coerce")
    
    # Check if birthday has occurred by date of service
    had_birthday = ( (dos.dt.month > dob.dt.month) | ((dos.dt.month == dob.dt.month) & (dos.dt.day >= dob.dt.day)) )
    
    # Calculate age: subtract 1 if birthday hasn't occurred yet
    age = (dos.dt.year - dob.dt.year) - (~had_birthday).astype("Int64")
    
    # Handle invalid cases
    valid_mask = dob.notna() & dos.notna()
    age = age.where(valid_mask)
    age = age.mask((valid_mask) & (dos < dob))  # Set None if DOS before DOB
    
    df[out_col] = age.astype("Int64")
    return df

# Apply age calculation to both datasets
test_records_df       = add_age_column(test_records_df)
validation_records_df = add_age_column(validation_records_df)

display(test_records_df.head())

# ============================================================================
# MEDICAL CODE REFERENCE EXTRACTION
# ============================================================================

def _extract_map_from_df(ref_df: pd.DataFrame, system: str) -> dict:
    """
    Extract medical code mappings (code -> description) from reference DataFrame.
    
    Handles multiple data formats:
    1. Standard format with code_system, code, description columns
    2. System name as column (e.g., CPT column, ICD10 column)
    3. Nested dictionary format within cells
    
    Args:
        ref_df: Reference DataFrame containing medical codes
        system: Code system name (e.g., "CPT" for procedures, "ICD10" for diagnoses)
        
    Returns:
        dict: Mapping of code (str) -> description (str)
    """
    # Format 1: Standard structured format
    if {"code_system", "code", "description"}.issubset(ref_df.columns):
        sub = ref_df.loc[ref_df["code_system"].astype(str) == system, ["code", "description"]].copy()
        sub["code"] = sub["code"].astype(str)
        sub["description"] = sub["description"].astype(str)
        return dict(sub.drop_duplicates(subset=["code"], keep="last").values)
    
    # Format 2: System name as column
    if system in ref_df.columns:
        ser = ref_df[system].dropna()
        # Handle nested dictionary format
        if ser.map(lambda v: isinstance(v, dict)).any():
            combined = {}
            for d in ser[ser.map(lambda v: isinstance(v, dict))]:
                combined.update({str(k): str(v) for k, v in d.items()})
            return combined
        # Handle simple value format
        ser = ser.astype(str)
        return {str(idx): str(val) for idx, val in ser.items()}
    
    return {}

def build_procedure_and_diagnosis_tables(references_codes_df: pd.DataFrame):
    """
    Build structured lookup tables for medical codes.
    
    Creates separate DataFrames for CPT (procedure) and ICD-10 (diagnosis) codes
    with descriptions for human-readable summaries.
    
    Args:
        references_codes_df: DataFrame containing reference code mappings
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (procedure_codes_df, diagnosis_codes_df)
    """
    # Extract CPT and ICD-10 code mappings
    cpt_map   = _extract_map_from_df(references_codes_df, "CPT")
    icd10_map = _extract_map_from_df(references_codes_df, "ICD10")
    
    # Build procedure codes DataFrame
    procedure_codes_df = (
        pd.DataFrame(list(cpt_map.items()), columns=["procedure_codes", "CPT"])
        .astype({"procedure_codes": "string", "CPT": "string"})
        .sort_values("procedure_codes", kind="stable")
        .reset_index(drop=True)
    )
    
    # Build diagnosis codes DataFrame
    diagnosis_codes_df = (
        pd.DataFrame(list(icd10_map.items()), columns=["diagnosis_codes", "ICD10"])
        .astype({"diagnosis_codes": "string", "ICD10": "string"})
        .sort_values("diagnosis_codes", kind="stable")
        .reset_index(drop=True)
    )
    
    return procedure_codes_df, diagnosis_codes_df

procedure_codes_df, diagnosis_codes_df = build_procedure_and_diagnosis_tables(references_codes_df)

display(procedure_codes_df.head())

# ============================================================================
# CODE LOOKUP DATABASES
# ============================================================================

def build_procedure_codes_db(procedure_codes_df: pd.DataFrame) -> Dict[str, str]:
    """
    Create in-memory dictionary for fast procedure code lookups.
    
    Args:
        procedure_codes_df: DataFrame with procedure_codes and CPT columns
        
    Returns:
        Dict[str, str]: Mapping of procedure code -> description
    """
    return dict(zip(procedure_codes_df["procedure_codes"].astype(str), procedure_codes_df["CPT"].astype(str)))

def build_diagnosis_codes_db(diagnosis_codes_df: pd.DataFrame) -> Dict[str, str]:
    """
    Create in-memory dictionary for fast diagnosis code lookups.
    
    Args:
        diagnosis_codes_df: DataFrame with diagnosis_codes and ICD10 columns
        
    Returns:
        Dict[str, str]: Mapping of diagnosis code -> description
    """
    return dict(zip(diagnosis_codes_df["diagnosis_codes"].astype(str), diagnosis_codes_df["ICD10"].astype(str)))

# Initialize global lookup databases (O(1) lookup time)
patient_record_db: Dict[str, Dict[str, Any]] = {}  # Will be populated from records
procedure_codes_db = build_procedure_codes_db(procedure_codes_df)
diagnosis_codes_db = build_diagnosis_codes_db(diagnosis_codes_df)

print(list(procedure_codes_db.items())[:5])

# ============================================================================
# HELPER FUNCTIONS - DATA NORMALIZATION
# ============================================================================

def _parse_date_to_iso(s: Any) -> Optional[str]:
    """
    Parse various date formats into ISO format (YYYY-MM-DD).
    
    Handles multiple input formats:
    - ISO: YYYY-MM-DD
    - US: MM/DD/YYYY
    - European: DD-MM-YYYY
    - Compact: YYYYMMDD
    - Pandas Timestamp and datetime objects
    
    Args:
        s: Date value in various formats
        
    Returns:
        Optional[str]: ISO formatted date (YYYY-MM-DD) or None if parsing fails
    """
    if s is None or (isinstance(s, float) and pd.isna(s)) or (isinstance(s, str) and not s.strip()):
        return None
    if isinstance(s, pd.Timestamp):
        return s.date().isoformat()
    if isinstance(s, (datetime, )):
        return s.date().isoformat()
    if isinstance(s, str):
        s = s.strip()
        # Try common date formats
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except Exception:
                pass
        # Try compact format
        if s.isdigit() and len(s) == 8:
            try:
                return datetime.strptime(s, "%Y%m%d").date().isoformat()
            except Exception:
                pass
        # Try ISO format parsing
        try:
            return datetime.fromisoformat(s).date().isoformat()
        except Exception:
            return None
    return None

def _normalize_bool(v: Any) -> Optional[bool]:
    """
    Normalize various boolean representations to Python bool.
    
    Handles: True/False, 1/0, "true"/"yes"/"1", "false"/"no"/"0"
    
    Args:
        v: Value to normalize
        
    Returns:
        Optional[bool]: Normalized boolean or None if cannot be interpreted
    """
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
    """
    Split and normalize medical codes from various input formats.
    
    Handles: space-separated strings, comma-separated strings, lists, dicts
    
    Args:
        val: Code value in various formats
        
    Returns:
        List[str]: List of normalized code strings
    """
    if val is None or (isinstance(val, float) and pd.isna(val)): return []
    if isinstance(val, (list, tuple, set)): return [str(x).strip() for x in val if str(x).strip()]
    if isinstance(val, dict): return [str(k).strip() for k in val.keys() if str(k).strip()]
    if isinstance(val, str): return [p.strip() for p in val.replace(",", " ").split() if p.strip()]
    return []

def _first_non_empty(row: pd.Series, candidates: List[str]) -> Any:
    """
    Extract first non-empty value from a list of column candidates.
    
    Useful for handling different column naming conventions in data sources.
    
    Args:
        row: Pandas Series (DataFrame row)
        candidates: List of column names to check in order
        
    Returns:
        Any: First non-empty value found, or None
    """
    for c in candidates:
        if c in row and row[c] is not None:
            v = row[c]
            if isinstance(v, float) and pd.isna(v): continue
            if isinstance(v, str) and not v.strip(): continue
            return v
    return None

def _compute_age_completed_years(dob_iso: Optional[str], dos_iso: Optional[str]) -> Optional[int]:
    """
    Calculate age in completed years at date of service.
    
    Standard insurance eligibility age calculation:
    - Age increases on birthday
    - Uses completed years (not fractional)
    
    Args:
        dob_iso: Date of birth in ISO format (YYYY-MM-DD)
        dos_iso: Date of service in ISO format (YYYY-MM-DD)
        
    Returns:
        Optional[int]: Age in completed years, or None if dates invalid
    """
    try:
        if not dob_iso or not dos_iso: return None
        dob = datetime.strptime(dob_iso, "%Y-%m-%d").date()
        dos = datetime.strptime(dos_iso, "%Y-%m-%d").date()
        if dos < dob: return None  # Invalid: service before birth
        years = dos.year - dob.year
        return years if (dos.month, dos.day) >= (dob.month, dob.day) else years - 1
    except Exception:
        return None

def _to_float_usd(v: Any) -> Optional[float]:
    """
    Normalize currency values to float.
    
    Handles: float/int, strings with $, commas, and various currency symbols
    
    Args:
        v: Value to convert
        
    Returns:
        Optional[float]: Normalized float value (USD), or None if conversion fails
    """
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, str):
        s = v.strip().replace(",", "")
        for sym in ["USD", "usd", "$", "₹"]: s = s.replace(sym, "")
        try: return float(s.strip())
        except Exception: return None
    return None

# ============================================================================
# PATIENT RECORD DATABASE CONSTRUCTION
# ============================================================================

def build_patient_record_db_from_df(combined_records_df: pd.DataFrame,
                                    key_priority: List[str] = None,
                                    duplicate_policy: str = "last") -> Dict[str, Dict[str, Any]]:
    """
    Build normalized patient record database from raw DataFrame.
    
    Performs comprehensive data normalization:
    - Extracts patient identifiers (tries multiple field names)
    - Normalizes dates to ISO format
    - Computes age if not present
    - Normalizes demographics, codes, booleans, amounts
    - Handles missing/malformed data gracefully
    
    Args:
        combined_records_df: Raw DataFrame containing patient records
        key_priority: List of column names to try as record keys (in order)
        duplicate_policy: How to handle duplicate keys ("first" or "last")
        
    Returns:
        Dict[str, Dict[str, Any]]: Nested dictionary {record_key: {field: value}}
        
    Key Fields in Output:
        - record_key: Unique identifier
        - claim_id, patient_id: Various IDs
        - name, gender: Demographics
        - date_of_birth, date_of_service: ISO dates
        - age: Calculated age at DOS
        - diagnosis_codes, procedure_codes: Lists of codes
        - preauthorization_required, preauthorization_obtained: Booleans
        - billed_amount_usd: Float
        - insurance_policy_id: Policy identifier
    """
    if key_priority is None:
        key_priority = ["claim_id", "encounter_id", "visit_id", "bill_id", "patient_id"]
    
    # Define possible column names for each field (handles various naming conventions)
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
        
        # Extract record key (try multiple column names in priority order)
        record_key = None
        for k in key_priority:
            v = _first_non_empty(row, [k])
            if v is not None:
                record_key = str(v); break
        if record_key is None: record_key = f"REC-{i+1:06d}"  # Generate key if none found
        
        # Parse dates
        dob_iso = _parse_date_to_iso(_first_non_empty(row, dob_cols))
        dos_iso = _parse_date_to_iso(_first_non_empty(row, dos_cols))
        
        # Extract or calculate age
        age_val = _first_non_empty(row, ["age", "patient_age"])
        try:
            age = int(age_val) if age_val is not None and str(age_val).strip().isdigit() else None
        except Exception:
            age = None
        if age is None: age = _compute_age_completed_years(dob_iso, dos_iso)
        
        # Build normalized record
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
        
        # Handle duplicates according to policy
        if duplicate_policy == "first" and record_key in db: continue
        db[record_key] = rec
    
    return db

# Combine validation and test records, then build database
combined_records_df = pd.concat([validation_records_df, test_records_df], ignore_index=True)
patient_record_db = build_patient_record_db_from_df(combined_records_df)

print(len(patient_record_db))

# ============================================================================
# INSURANCE POLICY DATABASE CONSTRUCTION
# ============================================================================

def build_insurance_policies_db(insurance_policies_df) -> dict:
    """
    Build normalized insurance policy database from DataFrame.
    
    Converts policy DataFrame into nested dictionary structure optimized for
    fast lookup and LLM-friendly formatting.
    
    Args:
        insurance_policies_df: DataFrame containing insurance policies
        
    Returns:
        dict: Nested dictionary {policy_id: {plan_name: ..., covered_procedures: {...}}}
        
    Structure:
        {
            "POL1001": {
                "plan_name": "Basic Health Plan",
                "covered_procedures": {
                    "99213": [  # CPT code
                        {
                            "covered_diagnoses": ["J20.9", "J44.0"],
                            "age_range": (18, 65),
                            "gender": "Any",
                            "requires_preauthorization": False,
                            "notes": "Coverage details"
                        }
                    ]
                }
            }
        }
    """
    db = {}
    for _, row in insurance_policies_df.iterrows():
        policy_id = str(row.get("policy_id"))
        plan_name = row.get("plan_name")
        
        # Build procedure coverage map (group rules by procedure code)
        proc_map = {}
        for r in (row.get("covered_procedures") or []):
            code = str(r.get("procedure_code"))
            # Each procedure can have multiple coverage rules
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

# ============================================================================
# TOOL 1: PATIENT RECORD SUMMARIZATION
# ============================================================================

@tool
def summarize_patient_record(record_key: str) -> dict:
    """
    Generate structured summary of patient's medical claim record.
    
    First step in ReAct workflow - provides patient information needed for
    coverage evaluation. Summary includes demographics, diagnoses, procedures,
    and preauthorization status.
    
    Args:
        record_key: Unique patient record identifier (e.g., "P011", "S001")
        
    Returns:
        dict: Contains "summary" key with formatted text or "error" key if not found
        
    Summary includes:
        1) Patient Demographics (name, gender, age)
        2) Insurance Policy ID
        3) Diagnoses with descriptions
        4) Procedures with descriptions
        5) Preauthorization status
        6) Billed amount
        7) Date of service
    """
    record = patient_record_db.get(record_key)
    if not record:
        return {"error": f"Record {record_key} not found."}
    
    # Build diagnosis list with human-readable descriptions
    diagnoses = [f"- {code}: {diagnosis_codes_db.get(code, 'Description not found')}" for code in record.get("diagnosis_codes", [])] or ["- Not provided"]
    
    # Build procedure list with human-readable descriptions
    procedures = [f"- {code}: {procedure_codes_db.get(code, 'Description not found')}" for code in record.get("procedure_codes", [])] or ["- Not provided"]
    
    # Format structured summary (numbered sections for easy LLM parsing)
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

# ============================================================================
# TOOL 2: POLICY GUIDELINE SUMMARIZATION
# ============================================================================

@tool
def summarize_policy_guideline(policy_id: str) -> dict:
    """
    Generate structured summary of insurance policy coverage rules.
    
    Second step in ReAct workflow - provides policy criteria needed to evaluate
    patient's claim. Lists all conditions that must be met for coverage.
    
    Args:
        policy_id: Insurance policy identifier (e.g., "POL1001")
        
    Returns:
        dict: Contains "summary" key with formatted text or "error" key if not found
        
    Summary includes:
        - Policy details (ID, plan name)
        - Covered procedures with:
          * Covered diagnoses (with descriptions)
          * Gender restrictions
          * Age range requirements
          * Preauthorization requirements
          * Coverage notes
    """
    policy = insurance_policies_db.get(policy_id)
    if not policy:
        return {"error": f"Policy {policy_id} not found."}
    
    plan_name = policy.get("plan_name", "Not provided")
    proc_rules = policy.get("covered_procedures", {}) or {}
    
    # Helper functions for formatting
    def yes_no(v): return "Yes" if bool(v) else "No"
    
    def age_range_str(rule):
        """Format age range for display"""
        rng = rule.get("age_range") or (None, None)
        if isinstance(rng, (list, tuple)) and len(rng) == 2:
            lo, hi = rng
        else:
            lo, hi = (None, None)
        if lo is None and hi is None: return "Not specified"
        return f"{'' if lo is None else lo}–{'' if hi is None else hi} years".strip("–")
    
    # Build procedure coverage entries
    entries = []
    for pcode, rules in proc_rules.items():
        pdesc = procedure_codes_db.get(pcode, "Description not found")
        
        # Each procedure may have multiple coverage rule sets
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

# ============================================================================
# TOOL 3: COVERAGE EVALUATION (LLM-BASED)
# ============================================================================

def check_claim_coverage(record_summary, policy_summary) -> dict:
    """
    Evaluate claim coverage using LLM-based reasoning.
    
    Final step in ReAct workflow - LLM evaluates whether patient's claim meets
    all policy requirements. Uses structured prompting for consistent decisions.
    
    Args:
        record_summary: Output from summarize_patient_record tool
        policy_summary: Output from summarize_policy_guideline tool
        
    Returns:
        dict: Contains "summary" key with LLM's structured evaluation
        
    Evaluation Criteria (ALL must be met):
        1. Diagnosis Match: Patient diagnosis in covered list
        2. Procedure Match: Procedure code explicitly in policy
        3. Age Check: Patient age within policy range
        4. Gender Check: Matches requirement or "Any"
        5. Preauthorization Check: Obtained if required
        
    Output Format:
        - Coverage Review: Step-by-step analysis
        - Summary of Findings: Met/unmet requirements
        - Final Decision: APPROVE or ROUTE FOR REVIEW with justification
    """
    rec = record_summary.get("summary") if isinstance(record_summary, dict) else str(record_summary)
    pol = policy_summary.get("summary") if isinstance(policy_summary, dict) else str(policy_summary)
    
    if "chat_client" not in globals():
        return {"error": "LLM client not configured. Please set `chat_client` and try again."}
    
    # Prompt with clear evaluation criteria and structured output format
    # Few-shot example guides consistent output
    prompt = f"""
You are a coverage analyst. Determine coverage for a single claimed procedure using ONLY the summaries below.

EVALUATION CRITERIA (all must be met to approve):
- Diagnosis: Patient diagnosis code(s) MUST include at least one of the policy-covered diagnoses for the claimed procedure.
- Procedure: The procedure code MUST be explicitly listed in the policy and meet all associated conditions.
- Age: Patient age MUST be within the policy's age range (lower bound inclusive, upper bound exclusive).
- Gender: Patient gender MUST match the policy's gender requirement (or 'Any').
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

FEW-SHOT EXAMPLE:

Example Patient: Age 45, Gender M, Diagnosis J20.9 (Acute bronchitis), Procedure 99213 (Office visit), Preauth not required
Example Policy: Procedure 99213 covered for J20.9, Age 18-65, Gender Any, No preauth required

Expected Output:

Coverage Review
- Procedure 99213 is listed in policy
- Diagnosis J20.9 matches covered diagnosis
- Age 45 within range 18-65
- Gender M matches Any
- Preauth not required

Summary of Findings
- All criteria met

Final Decision
- APPROVE. All requirements satisfied for procedure 99213 with diagnosis J20.9.

NOW EVALUATE THIS CLAIM:

PATIENT RECORD SUMMARY:
{rec}

POLICY SUMMARY:
{pol}
""".strip()
    
    try:
        response = chat_client.invoke(prompt)
        return {"summary": response.content}
    except Exception as e:
        return {"error": f"LLM evaluation failed: {str(e)}"}

decision = check_claim_coverage(rec_out, pol_out)

print(decision["summary"])

# ============================================================================
# REACT AGENT SETUP
# ============================================================================

from langgraph.prebuilt import create_react_agent

# Register all three tools for the agent
# ReAct Pattern: Agent iterates between Reasoning (thinking) and Acting (tool calling)
tools = [summarize_patient_record, summarize_policy_guideline, check_claim_coverage]
react_agent = create_react_agent(model=chat_client, tools=tools)

# System prompt defines agent behavior, tool sequence, and output format
SYSTEM_PROMPT = """# SYSTEM — CLAIM COVERAGE AGENT (ReAct)

## Objective
Determine if a patient's single claimed procedure is covered under their insurance policy.

## Allowed Tools (only these three)
1) summarize_patient_record(record_key)
2) summarize_policy_guideline(policy_id)
3) check_claim_coverage(record_summary, policy_summary)

## REQUIRED SEQUENCE
The agent MUST follow this exact three-step workflow:

Step 1 — Call summarize_patient_record(record_key)
  - Input: the record key (e.g., "P011").
  - Extract the Policy ID from section "2) Insurance Policy ID" in the returned summary text.
  - This provides patient demographics, diagnoses, procedures, and claim details.
  
Step 2 — Call summarize_policy_guideline(policy_id)
  - Input: the Policy ID from Step 1.
  - If the Policy ID is missing/ambiguous, STOP and return final output with Decision=ROUTE FOR REVIEW and a clear Reason.
  - This provides coverage rules: eligible diagnoses, age ranges, gender requirements, preauthorization needs.
  
Step 3 — Call check_claim_coverage(record_summary, policy_summary)
  - Inputs: the full text outputs from Steps 1 and 2.
  - This performs LLM-based evaluation against policy criteria.

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

## ReAct Pattern
This agent uses ReAct (Reasoning + Acting):
1. REASON: Analyze what information needed
2. ACT: Call appropriate tool
3. OBSERVE: Process tool output
4. REPEAT: Until ready to decide
This mimics human analyst workflow.

## FINAL OUTPUT (STRICT)
Return ONLY two lines, nothing else:
Decision: APPROVE | ROUTE FOR REVIEW
Reason: <Detailed justification including procedure name and code, diagnosis name and code, and specific policy conditions met or unmet. Clearly explain which criteria were satisfied and which were not, including age, gender, diagnosis, procedure, and preauthorization. End with a conclusive statement about why the claim is approved or routed for review.>

Do NOT reveal chain-of-thought or tool prompts. Be concise and factual.

## Example Outputs

Example 1 (Approval):
Decision: APPROVE
Reason: Procedure 99213 (Office visit) with diagnosis J20.9 (Acute bronchitis) meets all requirements: diagnosis covered, age 45 within 18-65, gender M matches Any, preauth not required. All criteria satisfied.

Example 2 (Review):
Decision: ROUTE FOR REVIEW
Reason: Procedure 99385 with diagnosis Z00.00 fails age requirement: patient age 72 exceeds maximum 65. Procedure and diagnosis match but age criteria unmet.
"""

# ============================================================================
# AGENT EXECUTION FUNCTION
# ============================================================================

def run_claim_approval(user_input: str) -> str:
    """
    Execute claim approval workflow for a patient.
    
    Orchestrates full ReAct agent workflow:
    1. Agent receives patient ID
    2. Agent autonomously calls three tools in sequence
    3. Agent reasons about outputs and makes decision
    4. Returns structured decision
    
    Args:
        user_input: Patient record identifier (e.g., "P011")
        
    Returns:
        str: Final decision with format "Decision: APPROVE|ROUTE FOR REVIEW\nReason: ..."
        
    Workflow:
        User Input → Tool 1 (Patient) → Tool 2 (Policy) → Tool 3 (Coverage) → Decision
    """
    try:
        messages = [("system", SYSTEM_PROMPT), ("user", user_input)]
        result = react_agent.invoke({"messages": messages})
        return result["messages"][-1].content.strip()
    except Exception as e:
        return f"Decision: ROUTE FOR REVIEW\nReason: System error - {str(e)}. Manual review required."

# ============================================================================
# BATCH PROCESSING
# ============================================================================

# Extract all unique patient IDs from test dataset
patient_ids = (
    test_records_df["patient_id"]
    .dropna()
    .astype(str)
    .drop_duplicates()
    .tolist()
)

# Process all test records in batch
rows = []
for pid in patient_ids:
    try:
        resp = run_claim_approval(pid)
    except Exception as e:
        # Catch errors for individual records to allow batch to continue
        resp = f"ERROR: {e}"
    rows.append({"patient_id": pid, "generated_response": str(resp)})

# Create submission DataFrame and save
submission_df = pd.DataFrame(rows, columns=["patient_id", "generated_response"])
submission_df.to_csv("submission.csv", index=False, encoding="utf-8-sig")

display(submission_df.head(10))
print("✅ Saved submission.csv with", len(submission_df), "rows")

# ============================================================================
# VALIDATION RUNS
# ============================================================================

# Test S-series patients
patient_ids = ["S001", "S002", "S003", "S004", "S005", "S006", "S007", "S008", "S009", "S010"]  

for pid in patient_ids:
    print("patients id: ",pid, run_claim_approval(pid))
    print("-------------------------------------------")

# Test P-series patients
patient_ids = ["P011", "P012", "P013", "P014", "P015", "P016", "P017", "P018", "P019", "P020"] 

for pid in patient_ids:
    print("patients id: ",pid, run_claim_approval(pid))
    print("-------------------------------------------")
