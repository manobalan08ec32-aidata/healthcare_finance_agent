"""
Insurance Claims Coverage Agent using LangGraph ReAct Pattern

This module implements an AI-powered insurance claims processing system that uses
a ReAct (Reasoning + Acting) agent to evaluate whether medical procedures are 
covered under a patient's insurance policy.

Architecture Overview:
---------------------
1. Data Layer: Loads patient records, insurance policies, and medical code references
2. Processing Layer: Normalizes and validates data, builds lookup databases
3. Tool Layer: Three specialized tools for patient records, policy guidelines, and coverage checks
4. Agent Layer: LangGraph ReAct agent orchestrates sequential tool calling with reasoning
5. Evaluation Layer: LLM-based coverage determination with structured output

Key AI/ML Concepts Demonstrated:
--------------------------------
- ReAct Pattern: Iterative reasoning and action taking for complex decision-making
- Tool Calling: LLM orchestrates specialized tools in a logical sequence
- Prompt Engineering: Structured prompts with clear evaluation criteria and output formats
- Few-Shot Learning: Examples guide the LLM to produce consistent, accurate decisions
- Chain-of-Thought: Step-by-step reasoning before final decision

Author: Manobalan
Project: AI Certification - Healthcare Claims Processing
"""

# ============================================================================
# IMPORTS AND DEPENDENCIES
# ============================================================================
# Note: Line below is for Databricks notebook environment setup
# %run ./.setup/learner_setup  

import os
from dotenv import load_dotenv
import httpx
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List

# LangChain components for LLM interaction and agent orchestration
from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
from langchain.vectorstores import Chroma
from langchain_core.tools import tool
from IPython.display import display, Markdown

# Retry logic for handling API failures (imported but can be utilized for robustness)
from tenacity import retry, stop_after_attempt, wait_random_exponential


# ============================================================================
# AUTHENTICATION & CONFIGURATION
# ============================================================================

def get_access_token() -> str:
    """
    Retrieves an Azure AD access token using the Client Credentials OAuth2 flow.
    
    This function authenticates with UHG's API gateway to obtain a bearer token
    that is used for subsequent API calls to Azure OpenAI services. The credentials
    are securely stored in Databricks secrets.
    
    Returns:
        str: Bearer token for API authentication
        
    Raises:
        httpx.HTTPError: If the authentication request fails
        KeyError: If the response doesn't contain an access_token
        
    Note:
        Token expiration is handled by re-calling this function when needed.
        For production, consider implementing token caching with expiry checking.
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
            resp.raise_for_status()  # Raise exception for 4xx/5xx status codes
            return resp.json()["access_token"]
    except httpx.HTTPError as e:
        raise RuntimeError(f"Failed to obtain access token: {e}")
    except KeyError:
        raise RuntimeError("Access token not found in authentication response")


# Load environment variables from configuration file
# These contain Azure OpenAI endpoint, API version, deployment names, and project ID
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
# Temperature=0 ensures deterministic outputs for consistent coverage decisions
chat_client = AzureChatOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_version=OPENAI_API_VERSION,
    azure_deployment=MODEL_DEPLOYMENT_NAME,
    temperature=0,  # Deterministic outputs for consistency in coverage decisions
    azure_ad_token=get_access_token(),
    default_headers={"projectId": PROJECT_ID}
)


# ============================================================================
# DATA LOADING
# ============================================================================

# Load core datasets with error handling
# These JSON files contain patient records, insurance policies, and medical code references
try:
    insurance_policies_df = pd.read_json("./Data/insurance_policies.json")
    references_codes_df = pd.read_json("./Data/reference_codes.json")
    test_records_df = pd.read_json("./Data/test_records.json")
    validation_records_df = pd.read_json("./Data/validation_records.json")
except FileNotFoundError as e:
    raise FileNotFoundError(f"Required data file not found: {e}. Please ensure all data files are in ./Data/ directory.")
except ValueError as e:
    raise ValueError(f"Invalid JSON format in data file: {e}")

# Display sample validation records for quick verification
display(validation_records_df.head())


# ============================================================================
# DATA PROCESSING - AGE CALCULATION
# ============================================================================

def add_age_column(df: pd.DataFrame,
                   dob_col: str = "date_of_birth",
                   dos_col: str = "date_of_service",
                   out_col: str = "age") -> pd.DataFrame:
    """
    Calculate patient age at date of service with proper birthday handling.
    
    This function computes age by determining if the patient has had their birthday
    by the date of service. It handles edge cases like invalid dates and dates of
    service before birth.
    
    Args:
        df: DataFrame containing patient records
        dob_col: Column name for date of birth (default: "date_of_birth")
        dos_col: Column name for date of service (default: "date_of_service")
        out_col: Output column name for calculated age (default: "age")
        
    Returns:
        pd.DataFrame: Copy of input DataFrame with age column added
        
    Logic:
        - Age = (DOS year - DOB year) - 1 if birthday hasn't occurred yet
        - Invalid dates (DOS < DOB) result in None
        - Missing dates result in None
        
    Note:
        Uses completed years (age at last birthday), which is standard for
        insurance eligibility determination.
    """
    df = df.copy()
    
    # Parse dates with error handling (coerce invalid dates to NaT)
    dob = pd.to_datetime(df[dob_col], errors="coerce")
    dos = pd.to_datetime(df[dos_col], errors="coerce")
    
    # Check if birthday has occurred by date of service
    # Birthday occurred if: DOS month > DOB month OR (same month AND DOS day >= DOB day)
    had_birthday = (
        (dos.dt.month > dob.dt.month) | 
        ((dos.dt.month == dob.dt.month) & (dos.dt.day >= dob.dt.day))
    )
    
    # Calculate age: subtract 1 if birthday hasn't occurred yet
    age = (dos.dt.year - dob.dt.year) - (~had_birthday).astype("Int64")
    
    # Handle invalid cases: missing dates or DOS before DOB
    valid_mask = dob.notna() & dos.notna()
    age = age.where(valid_mask)  # Set to None where dates are invalid
    age = age.mask((valid_mask) & (dos < dob))  # Set to None where DOS < DOB
    
    df[out_col] = age.astype("Int64")
    return df


# Apply age calculation to both test and validation datasets
test_records_df = add_age_column(test_records_df)
validation_records_df = add_age_column(validation_records_df)

display(test_records_df.head())


# ============================================================================
# MEDICAL CODE REFERENCE PROCESSING
# ============================================================================

def _extract_map_from_df(ref_df: pd.DataFrame, system: str) -> dict:
    """
    Extract medical code mappings (code -> description) from reference DataFrame.
    
    This helper function handles multiple data formats for medical code references:
    1. Standard format with code_system, code, description columns
    2. Column-based format where system name is a column
    3. Nested dictionary format within cells
    
    Args:
        ref_df: Reference DataFrame containing medical codes
        system: Code system name (e.g., "CPT", "ICD10")
        
    Returns:
        dict: Mapping of code (str) -> description (str)
        
    Note:
        Handles duplicates by keeping the last occurrence, which allows for
        code definition updates in the reference data.
    """
    # Format 1: Standard structured format with code_system column
    if {"code_system", "code", "description"}.issubset(ref_df.columns):
        sub = ref_df.loc[ref_df["code_system"].astype(str) == system, ["code", "description"]].copy()
        sub["code"] = sub["code"].astype(str)
        sub["description"] = sub["description"].astype(str)
        # Keep last occurrence for duplicate codes (allows updates)
        return dict(sub.drop_duplicates(subset=["code"], keep="last").values)
    
    # Format 2: System name as column (e.g., CPT column, ICD10 column)
    if system in ref_df.columns:
        ser = ref_df[system].dropna()
        
        # Handle nested dictionary format in cells
        if ser.map(lambda v: isinstance(v, dict)).any():
            combined = {}
            for d in ser[ser.map(lambda v: isinstance(v, dict))]:
                combined.update({str(k): str(v) for k, v in d.items()})
            return combined
        
        # Handle simple value format
        ser = ser.astype(str)
        return {str(idx): str(val) for idx, val in ser.items()}
    
    # No matching format found
    return {}


def build_procedure_and_diagnosis_tables(references_codes_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build structured lookup tables for medical procedure and diagnosis codes.
    
    Extracts CPT (Current Procedural Terminology) and ICD-10 (International 
    Classification of Diseases) codes from the reference DataFrame and creates
    clean, sorted lookup tables.
    
    Args:
        references_codes_df: DataFrame containing reference code mappings
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: 
            - procedure_codes_df: CPT codes with descriptions
            - diagnosis_codes_df: ICD-10 codes with descriptions
            
    Note:
        These tables are used for:
        1. Validating codes in patient records
        2. Enriching summaries with human-readable descriptions
        3. Supporting the LLM's coverage evaluation
    """
    # Extract code mappings using helper function
    cpt_map = _extract_map_from_df(references_codes_df, "CPT")
    icd10_map = _extract_map_from_df(references_codes_df, "ICD10")
    
    # Build procedure codes DataFrame (CPT)
    procedure_codes_df = (
        pd.DataFrame(list(cpt_map.items()), columns=["procedure_codes", "CPT"])
        .astype({"procedure_codes": "string", "CPT": "string"})
        .sort_values("procedure_codes", kind="stable")  # Stable sort preserves order for ties
        .reset_index(drop=True)
    )
    
    # Build diagnosis codes DataFrame (ICD-10)
    diagnosis_codes_df = (
        pd.DataFrame(list(icd10_map.items()), columns=["diagnosis_codes", "ICD10"])
        .astype({"diagnosis_codes": "string", "ICD10": "string"})
        .sort_values("diagnosis_codes", kind="stable")
        .reset_index(drop=True)
    )
    
    return procedure_codes_df, diagnosis_codes_df


# Build lookup tables for medical codes
procedure_codes_df, diagnosis_codes_df = build_procedure_and_diagnosis_tables(references_codes_df)

display(procedure_codes_df.head())


# ============================================================================
# DATABASE CONSTRUCTION
# ============================================================================

def build_procedure_codes_db(procedure_codes_df: pd.DataFrame) -> Dict[str, str]:
    """
    Create in-memory dictionary for fast procedure code lookups.
    
    Args:
        procedure_codes_df: DataFrame with procedure_codes and CPT columns
        
    Returns:
        Dict[str, str]: Mapping of procedure code -> description
    """
    return dict(zip(
        procedure_codes_df["procedure_codes"].astype(str), 
        procedure_codes_df["CPT"].astype(str)
    ))


def build_diagnosis_codes_db(diagnosis_codes_df: pd.DataFrame) -> Dict[str, str]:
    """
    Create in-memory dictionary for fast diagnosis code lookups.
    
    Args:
        diagnosis_codes_df: DataFrame with diagnosis_codes and ICD10 columns
        
    Returns:
        Dict[str, str]: Mapping of diagnosis code -> description
    """
    return dict(zip(
        diagnosis_codes_df["diagnosis_codes"].astype(str), 
        diagnosis_codes_df["ICD10"].astype(str)
    ))


# Initialize global lookup databases
# These provide O(1) lookup time for code descriptions during summarization
patient_record_db: Dict[str, Dict[str, Any]] = {}  # Will be populated from records
procedure_codes_db = build_procedure_codes_db(procedure_codes_df)
diagnosis_codes_db = build_diagnosis_codes_db(diagnosis_codes_df)

# Verify database construction
print(list(procedure_codes_db.items())[:5])


# ============================================================================
# HELPER FUNCTIONS - DATA NORMALIZATION
# ============================================================================

def _parse_date_to_iso(s: Any) -> Optional[str]:
    """
    Parse various date formats into ISO format (YYYY-MM-DD).
    
    Handles multiple input formats commonly found in healthcare data:
    - ISO format: YYYY-MM-DD
    - US format: MM/DD/YYYY
    - European format: DD-MM-YYYY
    - Compact format: YYYYMMDD
    - Pandas Timestamp objects
    - Python datetime objects
    
    Args:
        s: Date value in various formats (str, datetime, Timestamp, or None)
        
    Returns:
        Optional[str]: ISO formatted date string (YYYY-MM-DD) or None if parsing fails
        
    Note:
        Returns None for invalid dates rather than raising exceptions, allowing
        graceful handling of malformed data in patient records.
    """
    # Handle None and NaN values
    if s is None or (isinstance(s, float) and pd.isna(s)) or (isinstance(s, str) and not s.strip()):
        return None
    
    # Handle Pandas Timestamp
    if isinstance(s, pd.Timestamp):
        return s.date().isoformat()
    
    # Handle Python datetime
    if isinstance(s, (datetime, )):
        return s.date().isoformat()
    
    # Handle string formats
    if isinstance(s, str):
        s = s.strip()
        
        # Try common date formats
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except Exception:
                pass
        
        # Try compact format (YYYYMMDD)
        if s.isdigit() and len(s) == 8:
            try:
                return datetime.strptime(s, "%Y%m%d").date().isoformat()
            except Exception:
                pass
        
        # Try ISO format parsing as fallback
        try:
            return datetime.fromisoformat(s).date().isoformat()
        except Exception:
            return None
    
    return None


def _normalize_bool(v: Any) -> Optional[bool]:
    """
    Normalize various boolean representations to Python bool.
    
    Healthcare data often contains boolean values in multiple formats:
    - Actual booleans: True/False
    - Strings: "true", "yes", "1", "false", "no", "0"
    - Integers: 1/0
    
    Args:
        v: Value to normalize (bool, int, str, or None)
        
    Returns:
        Optional[bool]: Normalized boolean or None if value cannot be interpreted
        
    Note:
        Case-insensitive for string comparisons. Returns None for ambiguous values
        to avoid false assumptions in coverage decisions.
    """
    # Handle None and NaN
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    
    # Already a boolean
    if isinstance(v, bool):
        return v
    
    # Integer representation
    if isinstance(v, (int, )):
        return bool(v)
    
    # String representation (case-insensitive)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"true", "t", "yes", "y", "1"}: 
            return True
        if s in {"false", "f", "no", "n", "0"}: 
            return False
    
    return None


def _split_codes(val: Any) -> List[str]:
    """
    Split and normalize medical codes from various input formats.
    
    Medical codes in patient records may appear as:
    - Space-separated strings: "99213 99214"
    - Comma-separated strings: "99213, 99214"
    - Lists: ["99213", "99214"]
    - Dictionaries: {99213: desc, 99214: desc}
    
    Args:
        val: Code value in various formats (str, list, dict, or None)
        
    Returns:
        List[str]: List of normalized code strings (whitespace trimmed, empty removed)
        
    Note:
        Returns empty list for None/NaN to simplify downstream processing.
        Handles mixed delimiters (commas and spaces) in string inputs.
    """
    # Handle None and NaN
    if val is None or (isinstance(val, float) and pd.isna(val)): 
        return []
    
    # Handle collection types
    if isinstance(val, (list, tuple, set)): 
        return [str(x).strip() for x in val if str(x).strip()]
    
    # Handle dictionary (extract keys as codes)
    if isinstance(val, dict): 
        return [str(k).strip() for k in val.keys() if str(k).strip()]
    
    # Handle string (split on commas and/or spaces)
    if isinstance(val, str): 
        return [p.strip() for p in val.replace(",", " ").split() if p.strip()]
    
    return []


def _first_non_empty(row: pd.Series, candidates: List[str]) -> Any:
    """
    Extract first non-empty value from a list of column candidates.
    
    Used for handling data with multiple possible column names for the same
    information (e.g., different date field names across data sources).
    
    Args:
        row: Pandas Series (DataFrame row)
        candidates: List of column names to check in order
        
    Returns:
        Any: First non-empty value found, or None if all are empty/missing
        
    Note:
        Checks for None, NaN, and empty strings. Order of candidates matters.
    """
    for c in candidates:
        if c in row and row[c] is not None:
            v = row[c]
            # Skip NaN values
            if isinstance(v, float) and pd.isna(v): 
                continue
            # Skip empty strings
            if isinstance(v, str) and not v.strip(): 
                continue
            return v
    return None


def _compute_age_completed_years(dob_iso: Optional[str], dos_iso: Optional[str]) -> Optional[int]:
    """
    Calculate age in completed years at date of service.
    
    This is the standard age calculation used for insurance eligibility:
    - Age increases on the birthday
    - Uses completed years (not fractional age)
    - Validates that DOS >= DOB
    
    Args:
        dob_iso: Date of birth in ISO format (YYYY-MM-DD)
        dos_iso: Date of service in ISO format (YYYY-MM-DD)
        
    Returns:
        Optional[int]: Age in completed years, or None if dates are invalid/missing
        
    Note:
        Returns None if DOS < DOB (invalid case) rather than raising an error,
        allowing the system to route such cases for manual review.
    """
    try:
        if not dob_iso or not dos_iso: 
            return None
        
        # Parse ISO dates
        dob = datetime.strptime(dob_iso, "%Y-%m-%d").date()
        dos = datetime.strptime(dos_iso, "%Y-%m-%d").date()
        
        # Validate DOS >= DOB
        if dos < dob: 
            return None
        
        # Calculate completed years
        years = dos.year - dob.year
        
        # Subtract 1 if birthday hasn't occurred yet
        return years if (dos.month, dos.day) >= (dob.month, dob.day) else years - 1
    except Exception:
        return None


def _to_float_usd(v: Any) -> Optional[float]:
    """
    Normalize currency values to float for billed amounts.
    
    Handles various currency representations:
    - Float/int: Direct conversion
    - String with $ and commas: "$1,234.56" -> 1234.56
    - String without symbols: "1234.56" -> 1234.56
    
    Args:
        v: Value to convert (float, int, str, or None)
        
    Returns:
        Optional[float]: Normalized float value (USD), or None if conversion fails
        
    Note:
        Negative values are allowed (for adjustments/refunds).
        Returns None for unparseable values rather than raising exceptions.
    """
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        # Remove currency symbols and commas
        s = v.strip().replace("$", "").replace(",", "")
        try:
            return float(s)
        except Exception:
            return None
    return None


# ============================================================================
# PATIENT RECORD DATABASE CONSTRUCTION
# ============================================================================

def build_record_db(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Build normalized patient record database from raw DataFrame.
    
    This function performs comprehensive data normalization and validation:
    1. Extracts patient identifiers
    2. Normalizes dates to ISO format
    3. Computes age if not present
    4. Normalizes demographic data (gender)
    5. Splits and validates medical codes
    6. Normalizes boolean fields (preauthorization)
    7. Validates currency amounts
    
    The resulting database is optimized for:
    - Fast lookup by patient ID
    - Consistent data types for LLM processing
    - Graceful handling of missing/malformed data
    
    Args:
        df: Raw DataFrame containing patient records
        
    Returns:
        Dict[str, Dict[str, Any]]: Nested dictionary structure
            {patient_id: {field: normalized_value, ...}}
            
    Key Fields in Output:
        - patient_id: Unique identifier
        - date_of_birth: ISO date (YYYY-MM-DD)
        - date_of_service: ISO date (YYYY-MM-DD)
        - age: Calculated age at DOS (int)
        - gender: Normalized gender (str)
        - diagnosis_codes: List of ICD-10 codes
        - procedure_codes: List of CPT codes
        - preauthorization_required: Boolean
        - preauthorization_obtained: Boolean
        - billed_amount_usd: Float
        - insurance_policy_id: Policy identifier
        
    Note:
        Missing or malformed fields are set to None rather than raising errors,
        allowing the system to process partial records and route edge cases
        appropriately.
    """
    db = {}
    
    for _, row in df.iterrows():
        # Extract patient identifier (try multiple possible column names)
        pid_val = _first_non_empty(row, ["patient_id", "id", "record_id", "patient"])
        if pid_val is None:
            continue  # Skip records without identifiers
        
        pid = str(pid_val).strip()
        if not pid:
            continue
        
        # Parse dates from multiple possible column names
        dob_raw = _first_non_empty(row, ["date_of_birth", "dob", "birth_date"])
        dos_raw = _first_non_empty(row, ["date_of_service", "dos", "service_date"])
        dob_iso = _parse_date_to_iso(dob_raw)
        dos_iso = _parse_date_to_iso(dos_raw)
        
        # Calculate age if not already present in the data
        age_val = row.get("age")
        if age_val is None or (isinstance(age_val, float) and pd.isna(age_val)):
            age_val = _compute_age_completed_years(dob_iso, dos_iso)
        else:
            # Normalize existing age value to int
            try:
                age_val = int(age_val)
            except Exception:
                age_val = None
        
        # Normalize gender (case-insensitive, strip whitespace)
        gender_val = _first_non_empty(row, ["gender", "sex"])
        gender_str = str(gender_val).strip().upper() if gender_val is not None else None
        
        # Extract and split diagnosis codes (ICD-10)
        # Handles multiple possible column names and formats
        diag_raw = _first_non_empty(row, [
            "diagnosis_codes", "diagnosis_code", "icd10", "diagnosis", "diagnoses"
        ])
        diagnosis_list = _split_codes(diag_raw)
        
        # Extract and split procedure codes (CPT)
        proc_raw = _first_non_empty(row, [
            "procedure_codes", "procedure_code", "cpt", "procedure", "procedures"
        ])
        procedure_list = _split_codes(proc_raw)
        
        # Normalize preauthorization flags (boolean)
        preauth_req = _normalize_bool(
            _first_non_empty(row, ["preauthorization_required", "preauth_required", "prior_auth_required"])
        )
        preauth_obt = _normalize_bool(
            _first_non_empty(row, ["preauthorization_obtained", "preauth_obtained", "prior_auth_obtained"])
        )
        
        # Normalize billed amount (currency to float)
        billed_raw = _first_non_empty(row, ["billed_amount", "billed_amount_usd", "amount", "charge"])
        billed_usd = _to_float_usd(billed_raw)
        
        # Extract insurance policy ID
        policy_id = _first_non_empty(row, ["insurance_policy_id", "policy_id", "policy"])
        if policy_id is not None:
            policy_id = str(policy_id).strip()
        
        # Store normalized record in database
        db[pid] = {
            "patient_id": pid,
            "date_of_birth": dob_iso,
            "date_of_service": dos_iso,
            "age": age_val,
            "gender": gender_str,
            "diagnosis_codes": diagnosis_list,
            "procedure_codes": procedure_list,
            "preauthorization_required": preauth_req,
            "preauthorization_obtained": preauth_obt,
            "billed_amount_usd": billed_usd,
            "insurance_policy_id": policy_id,
        }
    
    return db


# Build patient record database from test data
patient_record_db = build_record_db(test_records_df)

print(f"✅ Built patient record database with {len(patient_record_db)} records")


# ============================================================================
# INSURANCE POLICY DATABASE CONSTRUCTION
# ============================================================================

def build_insurance_policies_db(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Build normalized insurance policy database from DataFrame.
    
    Converts policy DataFrame into a dictionary structure optimized for:
    1. Fast lookup by policy ID
    2. Nested structure for coverage rules
    3. Easy serialization for LLM prompts
    
    Args:
        df: DataFrame containing insurance policies
        
    Returns:
        Dict[str, Dict[str, Any]]: Nested dictionary
            {policy_id: {plan_name: ..., covered_procedures: {...}}}
            
    Structure:
        {
            "POL1001": {
                "plan_name": "Basic Health Plan",
                "covered_procedures": {
                    "99213": [  # CPT code
                        {
                            "covered_diagnoses": ["J20.9", "J44.0"],
                            "gender": "Any",
                            "age_range": [18, 65],
                            "requires_preauthorization": False,
                            "notes": "Standard office visit coverage"
                        }
                    ]
                }
            }
        }
        
    Note:
        Each procedure can have multiple coverage rules with different
        eligibility criteria (age, gender, diagnosis).
    """
    db = {}
    
    for _, row in df.iterrows():
        # Extract policy ID
        pid = row.get("policy_id")
        if pid is None or (isinstance(pid, float) and pd.isna(pid)):
            continue
        
        pid = str(pid).strip()
        if not pid:
            continue
        
        # Store policy with all fields
        # to_dict() preserves nested structures (covered_procedures, etc.)
        db[pid] = row.to_dict()
    
    return db


# Build insurance policy database
insurance_policies_db = build_insurance_policies_db(insurance_policies_df)

print(f"✅ Built insurance policy database with {len(insurance_policies_db)} policies")


# ============================================================================
# TOOL 1: PATIENT RECORD SUMMARIZATION
# ============================================================================

@tool
def summarize_patient_record(record_key: str) -> dict:
    """
    Generate a structured summary of a patient's medical claim record.
    
    This tool serves as the first step in the ReAct agent's workflow, providing
    essential patient information in a format optimized for LLM interpretation.
    The summary includes all information needed for coverage determination.
    
    Args:
        record_key: Unique patient record identifier (e.g., "P011", "S001")
        
    Returns:
        dict: Contains "summary" key with formatted text or "error" key if not found
        
    Summary Format:
        1) Patient Demographics: ID, DOB, age, gender
        2) Insurance Policy ID: For policy lookup in next step
        3) Diagnoses and Descriptions: ICD-10 codes with human-readable names
        4) Procedures and Descriptions: CPT codes with descriptions
        5) Preauthorization Status: Required and obtained flags
        6) Billed Amount: In USD
        7) Date of Service: When the procedure occurred
        
    Design Rationale:
        - Numbered sections guide LLM parsing
        - Human-readable descriptions reduce LLM hallucination
        - Explicit "Not provided" for missing data helps LLM handle edge cases
        - Structured format ensures consistent extraction of Policy ID for next tool
        
    Example:
        >>> summarize_patient_record("P011")
        {
            "summary": "1) Patient Demographics\n- ID: P011\n- DOB: 1985-03-15..."
        }
    """
    # Lookup record in database
    record = patient_record_db.get(record_key)
    
    if not record:
        return {"error": f"Patient record {record_key} not found in database."}
    
    # Build diagnosis list with descriptions
    # Format: "CODE: Description" for LLM readability
    diagnoses = []
    for dcode in (record.get("diagnosis_codes") or []):
        # Lookup description in diagnosis_codes_db
        desc = diagnosis_codes_db.get(dcode, "Description not found")
        diagnoses.append(f"- {dcode}: {desc}")
    
    # If no diagnoses, indicate explicitly
    if not diagnoses:
        diagnoses = ["- Not provided"]
    
    # Build procedure list with descriptions
    procedures = []
    for pcode in (record.get("procedure_codes") or []):
        # Lookup description in procedure_codes_db
        desc = procedure_codes_db.get(pcode, "Description not found")
        procedures.append(f"- {pcode}: {desc}")
    
    if not procedures:
        procedures = ["- Not provided"]
    
    # Format structured summary
    # Design: Each section numbered for easy reference in LLM prompts
    summary = f"""
1) Patient Demographics
- Patient ID: {record.get('patient_id', 'Not provided')}
- Date of Birth: {record.get('date_of_birth', 'Not provided')}
- Age: {record.get('age', 'Not provided')} years
- Gender: {record.get('gender', 'Not provided')}

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


# Test the tool
rec_out = summarize_patient_record("S010")


# ============================================================================
# TOOL 2: POLICY GUIDELINE SUMMARIZATION
# ============================================================================

@tool
def summarize_policy_guideline(policy_id: str) -> dict:
    """
    Generate a structured summary of insurance policy coverage rules.
    
    This tool serves as the second step in the ReAct workflow, retrieving the
    policy coverage criteria needed to evaluate the patient's claim. The summary
    explicitly lists all conditions that must be met for coverage approval.
    
    Args:
        policy_id: Insurance policy identifier (e.g., "POL1001")
        
    Returns:
        dict: Contains "summary" key with formatted text or "error" key if not found
        
    Summary Format:
        • Policy Details: Policy ID and plan name
        • Covered Procedures: For each procedure:
            - Procedure code and description
            - Covered diagnoses (ICD-10 codes with descriptions)
            - Gender restrictions
            - Age range requirements
            - Preauthorization requirements
            - Additional notes on coverage
            
    Design Rationale:
        - Bullet points for clear hierarchical structure
        - All eligibility criteria explicitly stated for LLM evaluation
        - Human-readable descriptions reduce confusion
        - "Not specified" for optional criteria (e.g., no age restriction)
        - Multiple rules per procedure supported (different diagnoses/age ranges)
        
    Coverage Rule Structure:
        Each procedure may have multiple rule sets. Coverage is approved if the
        claim matches ANY of the rule sets for that procedure.
        
    Note:
        Handles both list and dictionary formats for covered_procedures field
        to accommodate different data source structures.
        
    Example:
        >>> summarize_policy_guideline("POL1001")
        {
            "summary": "• Policy Details
- Policy ID: POL1001
- Plan Name: ..."
        }
    """
    # Lookup policy in database
    policy = insurance_policies_db.get(policy_id)
    
    if not policy:
        return {"error": f"Policy {policy_id} not found."}
    
    plan_name = policy.get("plan_name", "Not provided")
    proc_rules_raw = policy.get("covered_procedures", [])
    
    # Helper function to format yes/no
    def yes_no(v): 
        return "Yes" if bool(v) else "No"
    
    # Helper function to format age range
    def age_range_str(rule):
        rng = rule.get("age_range") or (None, None)
        if isinstance(rng, (list, tuple)) and len(rng) == 2:
            lo, hi = rng
        else:
            lo, hi = (None, None)
        
        # Handle case where no age restriction
        if lo is None and hi is None: 
            return "Not specified"
        
        # Format range with dash (e.g., "18–65 years")
        return f"{'' if lo is None else lo}–{'' if hi is None else hi} years".strip("–")
    
    # Build procedure coverage entries
    entries = []
    
    # ========================================================================
    # HANDLE MULTIPLE DATA FORMATS FOR COVERED_PROCEDURES
    # ========================================================================
    # Format 1: Dictionary structure {procedure_code: [rules]}
    # Example: {"99213": [{"covered_diagnoses": [...], "age_range": [...]}]}
    if isinstance(proc_rules_raw, dict):
        for pcode, rules in proc_rules_raw.items():
            # Lookup procedure description
            pdesc = procedure_codes_db.get(pcode, "Description not found")
            
            # Each procedure may have multiple coverage rule sets
            for r in (rules or []):
                # Extract covered diagnoses for this rule
                diags = r.get("covered_diagnoses", []) or []
                diag_lines = "
".join(
                    f"  - {d}: {diagnosis_codes_db.get(d, 'Description not found')}" 
                    for d in diags
                ) or "  - Not provided"
                
                # Format this rule entry
                entries.append(
                    f"- Procedure: {pcode} — {pdesc}
"
                    f"  - Covered Diagnoses and Descriptions:
"
                    f"{diag_lines}
"
                    f"  - Gender Restriction: {r.get('gender', 'Any')}
"
                    f"  - Age Range: {age_range_str(r)}
"
                    f"  - Preauthorization Requirement: {yes_no(r.get('requires_preauthorization', False))}
"
                    f"  - Notes on Coverage: {r.get('notes', 'None') or 'None'}
"
                )
    
    # Format 2: List structure [{"procedure_code": "99213", ...rules...}]
    # Each list item is a complete rule with procedure code embedded
    elif isinstance(proc_rules_raw, list):
        for rule in proc_rules_raw:
            if not isinstance(rule, dict):
                continue
            
            # Extract procedure code from the rule (try multiple field names)
            pcode = rule.get("procedure_code") or rule.get("procedure") or rule.get("code")
            if not pcode:
                continue
            
            pcode = str(pcode).strip()
            
            # Lookup procedure description
            pdesc = procedure_codes_db.get(pcode, "Description not found")
            
            # Extract covered diagnoses for this rule
            diags = rule.get("covered_diagnoses", []) or []
            diag_lines = "
".join(
                f"  - {d}: {diagnosis_codes_db.get(d, 'Description not found')}" 
                for d in diags
            ) or "  - Not provided"
            
            # Format this rule entry
            entries.append(
                f"- Procedure: {pcode} — {pdesc}
"
                f"  - Covered Diagnoses and Descriptions:
"
                f"{diag_lines}
"
                f"  - Gender Restriction: {rule.get('gender', 'Any')}
"
                f"  - Age Range: {age_range_str(rule)}
"
                f"  - Preauthorization Requirement: {yes_no(rule.get('requires_preauthorization', False))}
"
                f"  - Notes on Coverage: {rule.get('notes', 'None') or 'None'}
"
            )
    
    else:
        # Unsupported format - log error but continue
        entries = [f"- Error: Unsupported covered_procedures format (type: {type(proc_rules_raw).__name__})"]
    
    # Format final summary
    summary = f"""
• Policy Details
- Policy ID: {policy_id}
- Plan Name: {plan_name}

• Covered Procedures
{''.join(entries) if entries else '- None listed'}
""".strip()
    
    return {"summary": summary}

# Test the tool
pol_out = summarize_policy_guideline("POL1001")


# ============================================================================
# TOOL 3: COVERAGE EVALUATION (LLM-BASED)
# ============================================================================

def check_claim_coverage(record_summary: dict, policy_summary: dict) -> dict:
    """
    Evaluate claim coverage using LLM-based reasoning.
    
    This is the final step in the ReAct workflow where the LLM acts as a coverage
    analyst, systematically evaluating whether the patient's claim meets all policy
    requirements. Uses structured prompting to ensure consistent, accurate decisions.
    
    Args:
        record_summary: Output from summarize_patient_record tool
        policy_summary: Output from summarize_policy_guideline tool
        
    Returns:
        dict: Contains "summary" key with LLM's structured evaluation
        
    Evaluation Criteria (ALL must be met):
        1. Diagnosis Match: Patient's diagnosis codes must include at least one
           covered diagnosis from the policy
        2. Procedure Match: Procedure code must be explicitly listed in policy
        3. Age Check: Patient age must fall within policy's age range
           (lower bound inclusive, upper bound exclusive)
        4. Gender Check: Patient gender must match requirement (or "Any")
        5. Preauthorization Check: If required, must be obtained
        
    Output Format:
        The LLM returns three sections:
        1. Coverage Review: Step-by-step analysis of each criterion
        2. Summary of Findings: Bullet list of met/unmet requirements
        3. Final Decision: APPROVE or ROUTE FOR REVIEW with justification
        
    Design Rationale:
        - Structured output format ensures parseability
        - Step-by-step analysis mimics human reviewer process
        - "ROUTE FOR REVIEW" handles ambiguous cases safely
        - Clear criteria prevent hallucination
        - Few-shot example guides consistent output formatting
        
    Example:
        >>> check_claim_coverage(rec_out, pol_out)
        {
            "summary": "Coverage Review\n- Procedure code 99213 is listed...\n..."
        }
    """
    # Extract summary text from tool outputs
    rec = record_summary.get("summary") if isinstance(record_summary, dict) else str(record_summary)
    pol = policy_summary.get("summary") if isinstance(policy_summary, dict) else str(policy_summary)
    
    # Validate LLM client availability
    if "chat_client" not in globals():
        return {"error": "LLM client not configured. Please set `chat_client` and try again."}
    
    # Construct evaluation prompt with clear instructions and criteria
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

Example Patient Record:
1) Patient Demographics: Age 45, Gender M
2) Insurance Policy ID: POL1001
3) Diagnoses: J20.9 (Acute bronchitis)
4) Procedures: 99213 (Office visit)
5) Preauthorization: Required No, Obtained No

Example Policy:
- Procedure: 99213 — Office visit
  - Covered Diagnoses: J20.9 (Acute bronchitis)
  - Gender Restriction: Any
  - Age Range: 18–65 years
  - Preauthorization Requirement: No

Expected Output:

Coverage Review
- Procedure code 99213 is listed in the policy for office visits.
- Patient diagnosis J20.9 (Acute bronchitis) matches the covered diagnosis J20.9.
- Patient age 45 falls within policy age range 18–65 (inclusive lower, exclusive upper).
- Patient gender M matches policy requirement Any.
- Preauthorization not required by policy.

Summary of Findings
- Procedure match: ✓ Met
- Diagnosis match: ✓ Met
- Age requirement: ✓ Met (45 within 18–65)
- Gender requirement: ✓ Met (M matches Any)
- Preauthorization: ✓ Met (not required)

Final Decision
- APPROVE. All coverage criteria are satisfied for procedure 99213 (Office visit) with diagnosis J20.9 (Acute bronchitis).

NOW EVALUATE THIS CLAIM:

PATIENT RECORD SUMMARY:
{rec}

POLICY SUMMARY:
{pol}
""".strip()
    
    # Call LLM for evaluation
    try:
        response = chat_client.invoke(prompt)
        return {"summary": response.content}
    except Exception as e:
        return {"error": f"LLM evaluation failed: {str(e)}"}


# Test the coverage check
decision = check_claim_coverage(rec_out, pol_out)
print(decision["summary"])


# ============================================================================
# REACT AGENT SETUP
# ============================================================================

from langgraph.prebuilt import create_react_agent

# Register all three tools for the agent
# Tool calling sequence: summarize_patient_record → summarize_policy_guideline → check_claim_coverage
tools = [summarize_patient_record, summarize_policy_guideline, check_claim_coverage]

# Create ReAct agent with LLM and tools
# ReAct Pattern: The agent iterates between Reasoning (thinking) and Acting (tool calling)
react_agent = create_react_agent(model=chat_client, tools=tools)

# System prompt defines the agent's behavior and constraints
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
  - This provides coverage rules: eligible diagnoses, age ranges, gender requirements, and preauthorization needs.
  
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

## ReAct Pattern Explanation
This agent uses the ReAct (Reasoning + Acting) pattern:
1. REASON: Analyze what information is needed next
2. ACT: Call the appropriate tool to gather that information
3. OBSERVE: Process the tool output
4. REASON: Determine if more information is needed or if ready to decide
5. ACT: Either call another tool or provide final answer

This iterative process mirrors how a human analyst would work through a claim.

## FINAL OUTPUT (STRICT)
Return ONLY two lines, nothing else:
Decision: APPROVE | ROUTE FOR REVIEW
Reason: <Detailed justification including procedure name and code, diagnosis name and code, and specific policy conditions met or unmet. Clearly explain which criteria were satisfied and which were not, including age, gender, diagnosis, procedure, and preauthorization. End with a conclusive statement about why the claim is approved or routed for review.>

Do NOT reveal chain-of-thought or tool prompts. Be concise and factual.

## Example Final Output

Example 1 (Approval):
Decision: APPROVE
Reason: Procedure 99213 (Office visit) with diagnosis J20.9 (Acute bronchitis) meets all policy requirements: diagnosis is covered, patient age 45 is within range 18–65, gender M matches Any, and preauthorization is not required. All criteria satisfied.

Example 2 (Review):
Decision: ROUTE FOR REVIEW
Reason: Procedure 99385 (Preventive visit) with diagnosis Z00.00 (General exam) fails age requirement: patient age 72 exceeds policy maximum of 65 (exclusive). Procedure and diagnosis match policy, but age criteria not met.
"""


# ============================================================================
# AGENT EXECUTION FUNCTION
# ============================================================================

def run_claim_approval(user_input: str) -> str:
    """
    Execute the claim approval workflow for a given patient.
    
    This function orchestrates the full ReAct agent workflow:
    1. Initializes conversation with system prompt and user input (patient ID)
    2. Agent autonomously executes three-step tool calling sequence
    3. Agent reasons about tool outputs and makes final decision
    4. Returns structured decision (APPROVE or ROUTE FOR REVIEW)
    
    Args:
        user_input: Patient record identifier (e.g., "P011", "S005")
        
    Returns:
        str: Final agent decision with format:
             "Decision: APPROVE|ROUTE FOR REVIEW\nReason: <detailed justification>"
             
    Workflow:
        User Input → Agent → Tool 1 (Patient Record) → Agent → 
        Tool 2 (Policy) → Agent → Tool 3 (Coverage Check) → Agent → 
        Final Decision
        
    Error Handling:
        - Invalid patient IDs caught by summarize_patient_record tool
        - Missing policies caught by summarize_policy_guideline tool
        - LLM failures caught by check_claim_coverage tool
        - Agent returns "ROUTE FOR REVIEW" for any ambiguity
        
    Example:
        >>> decision = run_claim_approval("P011")
        >>> print(decision)
        Decision: APPROVE
        Reason: Procedure 99213 (Office visit) with diagnosis...
    """
    try:
        # Construct message sequence for agent
        # System message defines behavior, user message provides patient ID
        messages = [("system", SYSTEM_PROMPT), ("user", user_input)]
        
        # Invoke ReAct agent
        # Agent will autonomously call tools in sequence and make decision
        result = react_agent.invoke({"messages": messages})
        
        # Extract final response from message history
        return result["messages"][-1].content.strip()
    
    except Exception as e:
        # Catch any unexpected errors and route for manual review
        return f"Decision: ROUTE FOR REVIEW\nReason: System error occurred during processing - {str(e)}. Requires manual review."


# ============================================================================
# BATCH PROCESSING - TEST SET
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

# Create submission DataFrame
submission_df = pd.DataFrame(rows, columns=["patient_id", "generated_response"])

# Save to CSV for submission
submission_df.to_csv("submission.csv", index=False, encoding="utf-8-sig")

display(submission_df.head(10))
print("✅ Saved submission.csv with", len(submission_df), "rows")


# ============================================================================
# VALIDATION RUNS - SAMPLE PATIENTS
# ============================================================================

# Test on sample patient IDs (S-series)
print("\n" + "="*60)
print("VALIDATION RUN - S-SERIES PATIENTS")
print("="*60 + "\n")

patient_ids = ["S001", "S002", "S003", "S004", "S005", "S006", "S007", "S008", "S009", "S010"]  

for pid in patient_ids:
    print(f"Patient ID: {pid}")
    print(run_claim_approval(pid))
    print("-" * 60 + "\n")


# Test on sample patient IDs (P-series)
print("\n" + "="*60)
print("VALIDATION RUN - P-SERIES PATIENTS")
print("="*60 + "\n")

patient_ids = ["P011", "P012", "P013", "P014", "P015", "P016", "P017", "P018", "P019", "P020"] 

for pid in patient_ids:
    print(f"Patient ID: {pid}")
    print(run_claim_approval(pid))
    print("-" * 60 + "\n")


# ============================================================================
# END OF SCRIPT
# ============================================================================

print("\n✅ Claims processing complete!")
print(f"📊 Processed {len(submission_df)} patient records")
print("📁 Output saved to: submission.csv")
