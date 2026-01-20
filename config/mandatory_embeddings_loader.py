import json
from pathlib import Path
from typing import Dict, List
from functools import lru_cache

# Path to single JSON file
MANDATORY_EMBEDDINGS_FILE = Path(__file__).parent / "mandatory_embeddings.json"


@lru_cache(maxsize=1)
def _load_mandatory_embeddings_raw() -> Dict[str, List[str]]:
    """
    Load all mandatory embeddings from JSON file.
    Cached permanently - only loads once per process.
    """
    try:
        with open(MANDATORY_EMBEDDINGS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        total_embeddings = sum(len(v) for v in data.values())
        print(f"✅ Loaded {total_embeddings} mandatory embeddings for {len(data)} tables from JSON")
        
        return data
        
    except FileNotFoundError:
        print(f"⚠️  Mandatory embeddings file not found: {MANDATORY_EMBEDDINGS_FILE}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in mandatory embeddings file: {e}")
        return {}
    except Exception as e:
        print(f"❌ Error loading mandatory embeddings: {e}")
        return {}


def get_mandatory_embeddings_for_tables(tables_list: List[str]) -> Dict[str, List[str]]:
    """
    Get mandatory embeddings for specified tables.
    Ultra-fast after first call (cached in memory).
    """
    # Load all embeddings (cached)
    all_embeddings = _load_mandatory_embeddings_raw()
    
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
    _load_mandatory_embeddings_raw.cache_clear()
    print("✅ Mandatory embeddings cache cleared - will reload on next access")


def get_cache_info():
    """Get cache statistics"""
    return _load_mandatory_embeddings_raw.cache_info()


def list_available_tables() -> List[str]:
    """Get list of tables that have mandatory embeddings defined"""
    all_embeddings = _load_mandatory_embeddings_raw()
    return list(all_embeddings.keys())




