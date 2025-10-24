import hashlib
import re
import uuid
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import Counter, defaultdict

from pyspark.sql import SparkSession

feedback_table = "prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking "
processed_table = "prd_optumrx_orxfdmprdsa.rag.approved_examples_processed"
corpus_stats_table = "prd_optumrx_orxfdmprdsa.rag.bm25_corpus_stats"
term_df_table = "prd_optumrx_orxfdmprdsa.rag.term_document_frequency"

# Deduplication thresholds
TIER2_THRESHOLD = 0.95  # High confidence semantic match
GRAY_ZONE_START = 0.92  # Start of gray zone (need SQL check)
VARIANT_VALIDATION_THRESHOLD = 5  # Thumbs-up count to trust existing
TOP_K_CHECK = 20  # Check top 20 similar questions

# Stop words for tokenization
stop_words = {
    'the', 'a', 'an', 'is', 'are', 'was', 'were', 'in', 'on',
    'at', 'to', 'for', 'of', 'with', 'by', 'from', 'as', 'be'
}

def normalize_question(question: str) -> str:
    """Normalize question for exact matching (lowercase + trim)"""
    if not question:
        return ""
    return question.lower().strip()

def generate_hash(text: str) -> str:
    """Generate MD5 hash for deduplication"""
    return hashlib.md5(text.encode()).hexdigest()

def parse_table_names(table_name: str) -> List[str]:
    """
    Parse table names from feedback_monitoring table_name column.

    Handles formats:
    - Single: "sales"
    - Comma-separated: "sales,customers"
    - Space-separated: "sales customers"
    - Mixed: "sales, orders, products"
    """
    if not table_name:
        return []

    # Replace common separators
    table_name = table_name.replace(';', ',').replace('|', ',')

    # Split and clean
    tables = []
    for part in table_name.split(','):
        for table in part.split():
            cleaned = table.strip().lower()
            if cleaned:
                tables.append(cleaned)

    return list(set(tables))  # Remove duplicates

def tokenize(text: str) -> List[str]:
    """Tokenize text for BM25 (remove stop words and punctuation)"""
    if not text:
        return []

    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)

    tokens = [
        word for word in text.split()
        if word and word not in stop_words and len(word) > 1
    ]

    return tokens

def get_term_frequencies(tokens: List[str]) -> Dict[str, int]:
    """Calculate term frequencies for BM25"""
    return dict(Counter(tokens))

def extract_aggregate(sql: str) -> Optional[str]:
    """
    Extract primary aggregate function from SQL.
    Used in Tier 3 for distinguishing SUM vs AVG vs COUNT, etc.
    """
    if not sql:
        return None

    sql_upper = sql.upper()

    # Check in order of priority
    if 'SUM(' in sql_upper:
        return 'SUM'
    elif 'AVG(' in sql_upper:
        return 'AVG'
    elif 'COUNT(' in sql_upper:
        return 'COUNT'
    elif 'MAX(' in sql_upper:
        return 'MAX'
    elif 'MIN(' in sql_upper:
        return 'MIN'

    return None  # No aggregate function
	

def find_similar_top_k(question_text: str, k: int = 20) -> List[Dict]:
    """
    Find top K most similar questions using Databricks Vector Search.
    """
    try:
        from databricks.vector_search.client import VectorSearchClient

        client = VectorSearchClient()
        index = client.get_index(
            endpoint_name="metadata_vectore_search_endpoint",
            index_name="prd_optumrx_orxfdmprdsa.rag.sql_examples_vector_index"
        )

        # Request columns you want (do not include 'score' if not in table)
        results = index.similarity_search(
            query_text=question_text,
            columns=["example_id", "question", "sql_query", "table_name"],
            num_results=k
        )

        matches = []
        if results and results.get('result') and results['result'].get('data_array'):
            # Get column names from manifest (score is always present in data_array)
            cols = [c['name'] for c in results['manifest']['columns']]
            # If 'score' is not in columns, add it for mapping
            if 'score' not in cols:
                cols.append('score')
            for row in results['result']['data_array']:
                row_dict = dict(zip(cols, row))
                matches.append({
                    'example_id': row_dict.get('example_id'),
                    'question': row_dict.get('question'),
                    'sql_query': row_dict.get('sql_query'),
                    'table_name': row_dict.get('table_name'),
                    'similarity': row_dict.get('score', 0.0)
                })
        return matches

    except Exception as e:
        print(f"⚠ Vector search error: {e}")
        return []

# ==================== DEDUPLICATION LOGIC ====================

def process_all_feedback(feedback_list: List[Dict], 
                        existing_examples: Dict[str, Dict]) -> Dict:
    """
    Process all feedback items in memory (minimal DB hits).
    
    Three-tier deduplication:
    1. Exact hash match
    2. High semantic similarity (≥0.95)
    3. Gray zone (0.92-0.95) with aggregate + table validation
    
    Args:
        feedback_list: List of {user_question, sql_generated, table_name}
        existing_examples: Dict of existing examples indexed by hash
        
    Returns:
        Dict with new_inserts, count_increments, variants, errors, stats
    """
    
    results = {
        'new_inserts': [],
        'count_increments': defaultdict(int),
        'variants': [],
        'errors': [],
        'stats': defaultdict(int)
    }
    
    print(f"\nProcessing {len(feedback_list)} feedback items...")
    
    for idx, feedback in enumerate(feedback_list, 1):
        question = feedback['user_question']
        sql = feedback['sql_generated']
        table_name = feedback.get('table_name', '')
        
        # Process single feedback item
        result = _process_single_in_memory(
            question, sql, table_name, existing_examples
        )
        
        # Collect results
        if result['action'] == 'INSERT':
            results['new_inserts'].append(result['data'])
            results['stats']['NEW'] += 1
            
        elif result['action'] == 'INCREMENT':
            results['count_increments'][result['example_id']] += 1
            results['stats'][result['status']] += 1
            
        elif result['action'] == 'VARIANT':
            results['variants'].append(result['data'])
            results['stats']['NEW_VARIANT'] += 1
            
        elif result['action'] == 'ERROR':
            results['errors'].append(result['data'])
            results['stats']['ERROR_SEARCH'] += 1
        
        # Progress indicator
        if idx % 50 == 0 or idx == len(feedback_list):
            print(f"  Progress: {idx}/{len(feedback_list)} processed...")
    
    return results

def _process_single_in_memory(question: str, sql: str, table_name: str,
                               existing_examples: Dict[str, Dict]) -> Dict:
    """
    Process single feedback item through three-tier deduplication.
    All processing in Python memory (no DB hits).
    
    Returns:
        Dict with 'action' (INSERT/INCREMENT/VARIANT/ERROR) and data
    """
    
    # Normalize and hash
    normalized_q = normalize_question(question)
    q_hash = generate_hash(normalized_q)
    
    # ========== TIER 1: EXACT HASH MATCH ==========
    if q_hash in existing_examples:
        exact_match = existing_examples[q_hash]
        
        # Check if SQL is also the same
        if sql.strip().lower() == exact_match['sql_query'].strip().lower():
            # Exact duplicate - increment count
            return {
                'action': 'INCREMENT',
                'example_id': exact_match['example_id'],
                'status': 'EXACT_DUPLICATE'
            }
        else:
            # Same question, different SQL - check if variant
            if exact_match['thumbs_up_count'] >= VARIANT_VALIDATION_THRESHOLD:
                # Existing is well-validated - ignore new variant
                return {
                    'action': 'INCREMENT',
                    'example_id': exact_match['example_id'],
                    'status': 'IGNORED_VARIANT'
                }
            else:
                # Both unvalidated - keep as variant
                return {
                    'action': 'VARIANT',
                    'data': _prepare_insert_data(
                        question, sql, table_name, q_hash + '_variant',
                        variant_of=exact_match['example_id']
                    )
                }
    
    # ========== TIER 2 & 3: SEMANTIC SIMILARITY (TOP 20) ==========
    
    # Get top 20 similar questions (Vector Search auto-generates embedding!)
    similar_matches = find_similar_top_k(question, k=TOP_K_CHECK)
    
    if not similar_matches:
        # No similar matches found - insert as new
        return {
            'action': 'INSERT',
            'data': _prepare_insert_data(question, sql, table_name, q_hash)
        }
    
    # Extract metadata for NEW question (for Tier 3 comparison)
    new_aggregate = extract_aggregate(sql)
    new_tables = set(parse_table_names(table_name))
    
    # Check each of the top 20 candidates
    for candidate in similar_matches:
        similarity = candidate['similarity']
        
        # Stop checking if similarity too low
        if similarity < GRAY_ZONE_START:
            break
        
        # ========== TIER 2: HIGH CONFIDENCE (≥ 0.95) ==========
        if similarity >= TIER2_THRESHOLD:
            # Very high similarity - mark as duplicate
            return {
                'action': 'INCREMENT',
                'example_id': candidate['example_id'],
                'status': 'SEMANTIC_DUPLICATE_HIGH'
            }
        
        # ========== TIER 3: GRAY ZONE (0.92 - 0.95) ==========
        elif GRAY_ZONE_START <= similarity < TIER2_THRESHOLD:
            # Need additional validation: check aggregate + table
            
            candidate_aggregate = extract_aggregate(candidate['sql_query'])
            candidate_tables = set(parse_table_names(candidate.get('table_name', '')))
            
            # Check if aggregates match
            aggregate_match = (new_aggregate == candidate_aggregate)
            
            # Check if tables overlap
            table_overlap = bool(new_tables & candidate_tables)
            
            if aggregate_match and table_overlap:
                # Confirmed duplicate!
                return {
                    'action': 'INCREMENT',
                    'example_id': candidate['example_id'],
                    'status': 'SEMANTIC_DUPLICATE_SQL_CONFIRMED'
                }
            # Else: different aggregate or tables - continue to next candidate
    
    # None of the top 20 matched - insert as new
    return {
        'action': 'INSERT',
        'data': _prepare_insert_data(question, sql, table_name, q_hash)
    }

def _prepare_insert_data(question: str, sql: str, table_name: str,
                        q_hash: str, variant_of: Optional[str] = None) -> Dict:
    """
    Prepare data dict for insertion into processed table.
    
    Note: NO embedding field - Databricks generates it automatically!
    """
    tables = parse_table_names(table_name)
    tokens = tokenize(question)
    term_freq = get_term_frequencies(tokens)
    
    return {
        'example_id': str(uuid.uuid4()),
        'question': question,  # Databricks will auto-embed this!
        'sql_query': sql,
        'question_hash': q_hash,
        'tables_used': tables,
        'table_name': table_name,
        'question_tokens': tokens,
        'term_frequencies': term_freq,
        'thumbs_up_count': 1,
        'variant_of': variant_of,
        'is_variant': variant_of is not None
    }

# ==================== BULK DATABASE OPERATIONS ====================

def bulk_insert_examples(examples: List[Dict]):
    """
    Bulk insert new examples into processed table.
    Single DB operation regardless of number of examples.
    
    Note: All data is serialized to STRING (JSON format for arrays/dicts).
    """
    if not examples:
        return
    
    print(f"\n  Bulk inserting {len(examples)} new examples...")
    
    from pyspark.sql.types import StructType, StructField, StringType,IntegerType,BooleanType,DataType, TimestampType
    
    # All fields are STRING type (no embedding field!)
    schema = StructType([
        StructField("example_id", StringType()),
        StructField("question", StringType()),
        StructField("sql_query", StringType()),
        StructField("question_hash", StringType()),
        StructField("tables_used", StringType()),
        StructField("table_name", StringType()),
        StructField("question_tokens", StringType()),
        StructField("term_frequencies", StringType()),
        StructField("thumbs_up_count", IntegerType()),
        StructField("variant_of", StringType()),
        StructField("is_variant", BooleanType()),
        StructField("created_at", TimestampType()),
        StructField("last_updated_at", TimestampType())
    ])
    
    # Prepare rows - serialize all data to strings
    rows = []
    current_time = datetime.now().isoformat()
    
    for ex in examples:
        rows.append((
            ex['example_id'],
            ex['question'],  # Just text - Databricks will auto-embed!
            ex['sql_query'],
            ex['question_hash'],
            json.dumps(ex['tables_used']),
            ex['table_name'],
            json.dumps(ex['question_tokens']),
            json.dumps(ex['term_frequencies']),
            (ex['thumbs_up_count']),
            ex['variant_of'] if ex['variant_of'] else None,
            (ex['is_variant']),
            current_time,
            current_time
        ))
    
    # Create DataFrame
    df = spark.createDataFrame(rows, schema)
    display(df)
    # Bulk insert
    df.write.format("delta").mode("append").saveAsTable(processed_table)
    
    print(f"  ✓ Inserted {len(examples)} examples")
    print(f"  ✓ Databricks will auto-generate embeddings from question text")

def bulk_update_counts(count_increments: Dict[str, int]):
    """
    Bulk update thumbs_up counts using MERGE.
    Single DB operation for all updates.
    """
    if not count_increments:
        return
    
    print(f"\n  Bulk updating {len(count_increments)} example counts...")
    
    from pyspark.sql.types import StructType, StructField, StringType
    
    # Prepare updates
    updates = [(example_id, str(count)) for example_id, count in count_increments.items()]
    
    schema = StructType([
        StructField("example_id", StringType()),
        StructField("count_increment", StringType())
    ])
    
    updates_df = spark.createDataFrame(updates, schema)
    updates_df.createOrReplaceTempView("count_updates")
    
    # Bulk update using MERGE
    spark.sql(f"""
        MERGE INTO {processed_table} AS target
        USING count_updates AS source
        ON target.example_id = source.example_id
        WHEN MATCHED THEN UPDATE SET
            thumbs_up_count = CAST((CAST(thumbs_up_count AS INT) + CAST(source.count_increment AS INT)) AS STRING),
            last_updated_at = '{datetime.now().isoformat()}'
    """)
    
    print(f"  ✓ Updated {len(count_increments)} examples")

# ==================== BM25 STATISTICS ====================

def update_bm25_statistics():
    """
    Update BM25 corpus statistics for Phase 2 retrieval.
    Computes: total documents (N), average document length, term frequencies.
    """
    print("\nUpdating BM25 statistics...")
    
    # Load all examples
    examples_df = spark.sql(f"""
        SELECT question_tokens, term_frequencies
        FROM {processed_table}
    """)
    
    examples = examples_df.collect()
    
    if not examples:
        print("  ⚠ No examples to compute statistics")
        return
    
    # Compute corpus stats - parse JSON strings
    N = len(examples)
    total_length = 0
    
    for row in examples:
        if row['question_tokens']:
            try:
                tokens = json.loads(row['question_tokens'])
                total_length += len(tokens)
            except:
                pass
    
    avgdl = total_length / N if N > 0 else 0
    
    # Update corpus stats table
    spark.sql(f"DELETE FROM {corpus_stats_table}")
    current_time = datetime.now().isoformat()
    spark.sql(f"""
        INSERT INTO {corpus_stats_table} VALUES
        ('total_documents', '{N}', '{current_time}'),
        ('avg_doc_length', '{avgdl}', '{current_time}')
    """)
    
    # Compute document frequencies - parse JSON strings
    df_counter = Counter()
    for row in examples:
        if row['question_tokens']:
            try:
                tokens = json.loads(row['question_tokens'])
                unique_terms = set(tokens)
                for term in unique_terms:
                    df_counter[term] += 1
            except:
                pass
    
    # Update term frequencies table
    spark.sql(f"DELETE FROM {term_df_table}")
    
    if df_counter:
        current_time = datetime.now().isoformat()
        values = [
            f"('{term}', '{freq}', '{current_time}')"
            for term, freq in df_counter.items()
        ]
        
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            values_str = ','.join(batch)
            spark.sql(f"""
                INSERT INTO {term_df_table} VALUES {values_str}
            """)
    
    print(f"  ✓ BM25 stats: N={N}, avgdl={avgdl:.2f}, unique_terms={len(df_counter)}")
	
def run_batch_job():
        """
        Main batch processing job - runs every 3 hours.
        
        Flow:
        1. Load existing examples into memory (1 DB call)
        2. Load new feedback (1 DB call)
        3. Process all in memory (N vector searches - auto-embedding!)
        4. Bulk write results (2 DB calls)
        5. Update BM25 stats (2-3 DB calls)
        6. Sync vector index (1 API call)
        
        Performance for 1000 items: ~2-3 minutes (FASTER without embedding generation!)
        - Vector searches: ~1000 calls (auto-embedding by Databricks)
        - NO embedding generation in our code
        - DB operations: ~10 calls total
        """
        print("=" * 70)
        print("TEXT-TO-SQL FEEDBACK BATCH PROCESSOR (AUTO-EMBEDDING)")
        print("=" * 70)
        
        start_time = datetime.now()
        
        # ========== STEP 1: LOAD DATA ==========
        print("\n[Step 1/6] Loading existing examples into memory...")
        
        existing_df = spark.sql(f"""
            SELECT example_id, question, sql_query, question_hash, 
                   table_name, thumbs_up_count
            FROM {processed_table}
        """)
        
        # Index by hash for O(1) lookup
        existing_examples = {}
        for row in existing_df.collect():
            row_dict = row.asDict()
            try:
                row_dict['thumbs_up_count'] = int(row_dict['thumbs_up_count'])
            except:
                row_dict['thumbs_up_count'] = 0
            existing_examples[row['question_hash']] = row_dict
        
        print(f"  ✓ Loaded {len(existing_examples)} existing examples")
        
        print("\n[Step 2/6] Loading new feedback...")
        
        feedback_df = spark.sql(f"""
            SELECT DISTINCT user_question, state_info as sql_generated, table_name
            FROM {feedback_table}
            WHERE user_question IS NOT NULL
            AND state_info IS NOT NULL
            AND user_question != ''
            AND state_info != ''
        """)
        
        feedback_list = [row.asDict() for row in feedback_df.collect()]
        total_feedback = len(feedback_list)
        
        print(f"  ✓ Loaded {total_feedback} feedback items")
        
        if total_feedback == 0:
            print("\n⚠ No feedback to process")
            return
        
        # ========== STEP 2: PROCESS IN MEMORY ==========
        print("\n[Step 3/6] Processing feedback (3-tier deduplication)...")
        print(f"  Note: Vector searches use auto-embedding (Databricks generates embeddings)")
        
        results = process_all_feedback(feedback_list, existing_examples)
        
        # ========== STEP 3: BULK WRITE ==========
        print("\n[Step 4/6] Writing results to database...")
        
        # Combine new inserts and variants
        all_new = results['new_inserts'] + results['variants']
        if all_new:
            bulk_insert_examples(all_new)
        else:
            print("  (No new examples to insert)")
        
        # Bulk update counts
        if results['count_increments']:
            bulk_update_counts(results['count_increments'])
        else:
            print("  (No counts to update)")
        
        # ========== STEP 4: BM25 STATISTICS ==========
        print("\n[Step 5/6] Updating BM25 statistics...")
        update_bm25_statistics()
        
        # ========== STEP 5: SYNC VECTOR INDEX ==========
        print("\n[Step 6/6] Syncing vector search index...")
        try:
            from databricks.vector_search.client import VectorSearchClient
            client = VectorSearchClient()
            client.get_index(
                endpoint_name="sql_feedback_endpoint",
                index_name="sql_examples_vector_index"
            ).sync()
            print("  ✓ Vector search index synced")
            print("  ✓ New questions will be auto-embedded by Databricks")
        except Exception as e:
            print(f"  ⚠ Error syncing index: {e}")
        
        # ========== SUMMARY ==========
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 70)
        print("DEDUPLICATION RESULTS")
        print("=" * 70)
        for status, count in sorted(results['stats'].items()):
            if count > 0:
                percentage = (count / total_feedback) * 100
                print(f"  {status:40s}: {count:4d} ({percentage:5.1f}%)")
        
        print("\n" + "=" * 70)
        print("BATCH JOB SUMMARY")
        print("=" * 70)
        print(f"Total processed:        {total_feedback}")
        print(f"New unique examples:    {results['stats']['NEW']}")
        print(f"Duplicates found:       {sum(results['count_increments'].values())}")
        print(f"Variants created:       {results['stats']['NEW_VARIANT']}")
        print(f"Errors:                 {results['stats']['ERROR_SEARCH']}")
        print(f"Duration:               {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"")
        print(f"Performance notes:")
        print(f"  - Vector searches:    ~{total_feedback} (auto-embedding by Databricks)")
        print(f"  - NO embedding API calls in our code (much faster!)")
        print(f"  - DB operations:      ~10 (bulk inserts/updates)")
        print("=" * 70)
