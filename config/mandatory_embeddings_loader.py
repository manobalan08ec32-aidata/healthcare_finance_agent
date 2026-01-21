import json
from pathlib import Path
from typing import Dict, List
from functools import lru_cache

# Paths to domain-specific JSON files
MANDATORY_EMBEDDINGS_PBM_FILE = Path(__file__).parent / "mandatory_embeddings_pbm.json"
MANDATORY_EMBEDDINGS_PHARMACY_FILE = Path(__file__).parent / "mandatory_embeddings_pharmacy.json"


@lru_cache(maxsize=1)
def _load_mandatory_embeddings_pbm() -> Dict[str, List[str]]:
    """
    Load all mandatory embeddings from PBM Network JSON file.
    Cached permanently - only loads once per process.
    """
    try:
        with open(MANDATORY_EMBEDDINGS_PBM_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        total_embeddings = sum(len(v) for v in data.values())
        print(f"✅ Loaded {total_embeddings} mandatory embeddings for {len(data)} tables from PBM Network JSON")

        return data

    except FileNotFoundError:
        print(f"⚠️  Mandatory embeddings file not found: {MANDATORY_EMBEDDINGS_PBM_FILE}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in mandatory embeddings file: {e}")
        return {}
    except Exception as e:
        print(f"❌ Error loading mandatory embeddings: {e}")
        return {}


@lru_cache(maxsize=1)
def _load_mandatory_embeddings_pharmacy() -> Dict[str, List[str]]:
    """
    Load all mandatory embeddings from Optum Pharmacy JSON file.
    Cached permanently - only loads once per process.
    """
    try:
        with open(MANDATORY_EMBEDDINGS_PHARMACY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        total_embeddings = sum(len(v) for v in data.values())
        print(f"✅ Loaded {total_embeddings} mandatory embeddings for {len(data)} tables from Optum Pharmacy JSON")

        return data

    except FileNotFoundError:
        print(f"⚠️  Mandatory embeddings file not found: {MANDATORY_EMBEDDINGS_PHARMACY_FILE}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in mandatory embeddings file: {e}")
        return {}
    except Exception as e:
        print(f"❌ Error loading mandatory embeddings: {e}")
        return {}


def get_mandatory_embeddings_for_tables(tables_list: List[str], domain_selection: str = 'Optum Pharmacy') -> Dict[str, List[str]]:
    """
    Get mandatory embeddings for specified tables based on domain selection.
    Ultra-fast after first call (cached in memory).

    Args:
        tables_list: List of table names to get embeddings for
        domain_selection: Domain selection ('PBM Network' or 'Optum Pharmacy')

    Returns:
        Dictionary mapping table names to their embeddings
    """
    # Load embeddings based on domain selection (cached)
    if domain_selection == 'PBM Network':
        all_embeddings = _load_mandatory_embeddings_pbm()
    else:  # Default to Optum Pharmacy
        all_embeddings = _load_mandatory_embeddings_pharmacy()

    # Filter to requested tables
    result = {}
    for table in tables_list:
        if table in all_embeddings:
            result[table] = all_embeddings[table]
        else:
            print(f"⚠️  No mandatory embeddings defined for table: {table}")

    return result


def clear_cache():
    """Clear cache to force reload (useful during development)"""
    _load_mandatory_embeddings_pbm.cache_clear()
    _load_mandatory_embeddings_pharmacy.cache_clear()
    print("✅ Mandatory embeddings cache cleared - will reload on next access")


def get_cache_info():
    """Get cache statistics for both domains"""
    return {
        'pbm_network': _load_mandatory_embeddings_pbm.cache_info(),
        'optum_pharmacy': _load_mandatory_embeddings_pharmacy.cache_info()
    }


def list_available_tables(domain_selection: str = 'Optum Pharmacy') -> List[str]:
    """Get list of tables that have mandatory embeddings defined for a domain"""
    if domain_selection == 'PBM Network':
        all_embeddings = _load_mandatory_embeddings_pbm()
    else:
        all_embeddings = _load_mandatory_embeddings_pharmacy()
    return list(all_embeddings.keys())




