import hashlib
import re
import uuid
import json
from datetime import datetime
from typing import List, Dict, Optional
from collections import Counter, defaultdict
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
FEEDBACK_TABLE = "prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking "
PROCESSED_TABLE = "prd_optumrx_orxfdmprdsa.rag.approved_examples_processed"
CORPUS_STATS_TABLE = "prd_optumrx_orxfdmprdsa.rag.bm25_corpus_stats"
TERM_DF_TABLE = "prd_optumrx_orxfdmprdsa.rag.term_document_frequency"

# Deduplication thresholds
TIER2_THRESHOLD = 0.95  # High confidence semantic match
GRAY_ZONE_START = 0.92  # Start of gray zone (need SQL check)
VARIANT_VALIDATION_THRESHOLD = 5  # Thumbs-up count to trust existing
TOP_K_CHECK = 20  # Check top 20 similar questions

# Stop words for tokenization
STOP_WORDS = {
    'the', 'a', 'an', 'is', 'are', 'was', 'were', 'in', 'on',
    'at', 'to', 'for', 'of', 'with', 'by', 'from', 'as', 'be'
}

def normalize_question(question: str) -> str:
    """Normalize question for exact matching"""
    if not question:
        return ""
    return question.lower().strip()


def generate_hash(text: str) -> str:
    """Generate MD5 hash"""
    return hashlib.md5(text.encode()).hexdigest()


def parse_table_names(table_name: str) -> List[str]:
    """Parse table names from table_name column"""
    if not table_name:
        return []
    
    table_name = table_name.replace(';', ',').replace('|', ',')
    tables = []
    for part in table_name.split(','):
        for table in part.split():
            cleaned = table.strip().lower()
            if cleaned:
                tables.append(cleaned)
    
    return list(set(tables))


def tokenize(text: str) -> List[str]:
    """Tokenize text for BM25"""
    if not text:
        return []
    
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    tokens = [
        word for word in text.split()
        if word and word not in STOP_WORDS and len(word) > 1
    ]
    return tokens


def get_term_frequencies(tokens: List[str]) -> Dict[str, int]:
    """Calculate term frequencies"""
    return dict(Counter(tokens))


def extract_aggregate(sql: str) -> Optional[str]:
    """Extract primary aggregate function from SQL"""
    if not sql:
        return None
    
    sql_upper = sql.upper()
    
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
    
    return None


# ==================== VECTOR SEARCH ====================

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

def process_single_feedback(question: str, sql: str, table_name: str,
                           existing_examples: Dict[str, Dict]) -> Dict:
    """
    Process single feedback through three-tier deduplication
    
    Returns dict with 'action' and 'data'
    """
    
    # Normalize and hash
    normalized_q = normalize_question(question)
    q_hash = generate_hash(normalized_q)
    
    # ========== TIER 1: EXACT HASH MATCH ==========
    if q_hash in existing_examples:
        exact_match = existing_examples[q_hash]
        
        # Check if SQL is the same
        if sql.strip().lower() == exact_match['sql_query'].strip().lower():
            # Exact duplicate - increment count
            return {
                'action': 'INCREMENT',
                'example_id': exact_match['example_id'],
                'status': 'EXACT_DUPLICATE'
            }
        else:
            # Same question, different SQL - still increment (simplified logic)
            return {
                'action': 'INCREMENT',
                'example_id': exact_match['example_id'],
                'status': 'DIFFERENT_SQL'
            }
    
    # ========== TIER 2 & 3: SEMANTIC SIMILARITY (TOP 20) ==========
    
    similar_matches = find_similar_top_k(question, k=TOP_K_CHECK)
    
    if not similar_matches:
        # No similar matches - insert as new
        return {
            'action': 'INSERT',
            'data': prepare_insert_data(question, sql, table_name, q_hash)
        }
    
    # Extract metadata for comparison
    new_aggregate = extract_aggregate(sql)
    new_tables = set(parse_table_names(table_name))
    
    # Check each of top 20
    for candidate in similar_matches:
        similarity = candidate['similarity']
        
        if similarity < GRAY_ZONE_START:
            break
        
        # TIER 2: High confidence (≥ 0.95)
        if similarity >= TIER2_THRESHOLD:
            return {
                'action': 'INCREMENT',
                'example_id': candidate['example_id'],
                'status': 'SEMANTIC_DUPLICATE_HIGH'
            }
        
        # TIER 3: Gray zone (0.92 - 0.95)
        elif GRAY_ZONE_START <= similarity < TIER2_THRESHOLD:
            candidate_aggregate = extract_aggregate(candidate['sql_query'])
            candidate_tables = set(parse_table_names(candidate.get('table_name', '')))
            
            aggregate_match = (new_aggregate == candidate_aggregate)
            table_overlap = bool(new_tables & candidate_tables)
            
            if aggregate_match and table_overlap:
                return {
                    'action': 'INCREMENT',
                    'example_id': candidate['example_id'],
                    'status': 'SEMANTIC_DUPLICATE_SQL_CONFIRMED'
                }
    
    # None matched - insert as new
    return {
        'action': 'INSERT',
        'data': prepare_insert_data(question, sql, table_name, q_hash)
    }


def prepare_insert_data(question: str, sql: str, table_name: str, q_hash: str) -> Dict:
    """Prepare data for insertion"""
    
    tokens = tokenize(question)
    term_freq = get_term_frequencies(tokens)
    
    return {
        'example_id': str(uuid.uuid4()),
        'question': question,
        'sql_query': sql,
        'question_hash': q_hash,
        'table_name': table_name,
        'question_tokens': tokens,
        'term_frequencies': term_freq,
        'thumbs_up_count': 1
    }


def process_all_feedback(spark: SparkSession, feedback_list: List[Dict],
                        existing_examples: Dict[str, Dict]) -> Dict:
    """Process all feedback items"""
    
    results = {
        'new_inserts': [],
        'count_increments': defaultdict(int),
        'stats': defaultdict(int)
    }
    
    print(f"\nProcessing {len(feedback_list)} feedback items...")
    
    for idx, feedback in enumerate(feedback_list, 1):
        question = feedback['user_question']
        sql = feedback['sql_generated']
        table_name = feedback.get('table_name', '')
        
        result = process_single_feedback(question, sql, table_name, existing_examples)
        
        if result['action'] == 'INSERT':
            results['new_inserts'].append(result['data'])
            results['stats']['NEW'] += 1
            
        elif result['action'] == 'INCREMENT':
            results['count_increments'][result['example_id']] += 1
            results['stats'][result['status']] += 1
        
        if idx % 50 == 0 or idx == len(feedback_list):
            print(f"  Progress: {idx}/{len(feedback_list)} processed...")
    
    return results


# ==================== BULK OPERATIONS ====================

def bulk_insert_examples(spark: SparkSession, examples: List[Dict]):
    """Bulk insert new examples"""
    
    if not examples:
        return
    
    print(f"\n  Bulk inserting {len(examples)} new examples...")
    
    # Schema with correct types
    schema = StructType([
        StructField("example_id", StringType()),
        StructField("question", StringType()),
        StructField("sql_query", StringType()),
        StructField("question_hash", StringType()),
        StructField("table_name", StringType()),
        StructField("question_tokens", StringType()),
        StructField("term_frequencies", StringType()),
        StructField("thumbs_up_count", IntegerType()),
        StructField("created_at", TimestampType()),
        StructField("last_updated_at", TimestampType())
    ])
    
    # Prepare rows
    rows = []
    current_time = datetime.now()
    
    for ex in examples:
        rows.append((
            ex['example_id'],
            ex['question'],
            ex['sql_query'],
            ex['question_hash'],
            ex['table_name'],
            json.dumps(ex['question_tokens']),
            json.dumps(ex['term_frequencies']),
            ex['thumbs_up_count'],
            current_time,
            current_time
        ))
    
    # Create DataFrame and insert
    df = spark.createDataFrame(rows, schema)
    df.write.format("delta").mode("append").saveAsTable(PROCESSED_TABLE)
    
    print(f"  ✓ Inserted {len(examples)} examples")


def bulk_update_counts(spark: SparkSession, count_increments: Dict[str, int]):
    """Bulk update thumbs_up counts"""
    
    if not count_increments:
        return
    
    print(f"\n  Bulk updating {len(count_increments)} example counts...")
    
    # Create updates DataFrame
    updates = [(example_id, count) for example_id, count in count_increments.items()]
    
    schema = StructType([
        StructField("example_id", StringType()),
        StructField("count_increment", IntegerType())
    ])
    
    updates_df = spark.createDataFrame(updates, schema)
    updates_df.createOrReplaceTempView("count_updates")
    
    # Bulk update using MERGE
    spark.sql(f"""
        MERGE INTO {PROCESSED_TABLE} AS target
        USING count_updates AS source
        ON target.example_id = source.example_id
        WHEN MATCHED THEN UPDATE SET
            thumbs_up_count = thumbs_up_count + source.count_increment,
            last_updated_at = current_timestamp()
    """)
    
    print(f"  ✓ Updated {len(count_increments)} examples")


# ==================== BM25 STATISTICS ====================

def update_bm25_statistics(spark: SparkSession):
    """Update BM25 corpus statistics"""
    
    print("\nUpdating BM25 statistics...")
    
    examples_df = spark.sql(f"""
        SELECT question_tokens, term_frequencies
        FROM {PROCESSED_TABLE}
    """)
    
    examples = examples_df.collect()
    
    if not examples:
        print("  ⚠ No examples to compute statistics")
        return
    
    # Compute corpus stats
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
    spark.sql(f"DELETE FROM {CORPUS_STATS_TABLE}")
    current_time = datetime.now().isoformat()
    spark.sql(f"""
        INSERT INTO {CORPUS_STATS_TABLE} VALUES
        ('total_documents', '{N}', '{current_time}'),
        ('avg_doc_length', '{avgdl}', '{current_time}')
    """)
    
    # Compute document frequencies
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
    spark.sql(f"DELETE FROM {TERM_DF_TABLE}")
    
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
                INSERT INTO {TERM_DF_TABLE} VALUES {values_str}
            """)
    
    print(f"  ✓ BM25 stats: N={N}, avgdl={avgdl:.2f}, unique_terms={len(df_counter)}")


def run_batch_job():
    """Main batch processing job"""
    
    print("=" * 70)
    print("TEXT-TO-SQL FEEDBACK BATCH PROCESSOR")
    print("=" * 70)
    
    start_time = datetime.now()
    
    # Step 1: Load existing examples
    print("\n[Step 1/6] Loading existing examples...")
    
    existing_df = spark.sql(f"""
        SELECT example_id, question, sql_query, question_hash, 
               table_name, thumbs_up_count
        FROM {PROCESSED_TABLE}
    """)
    
    existing_examples = {}
    for row in existing_df.collect():
        existing_examples[row['question_hash']] = row.asDict()
    
    print(f"  ✓ Loaded {len(existing_examples)} existing examples")
    
    # Step 2: Load new feedback
    print("\n[Step 2/6] Loading new feedback...")
    
    feedback_df = spark.sql(f"""
        SELECT DISTINCT user_question, state_info as sql_generated, table_name
        FROM {FEEDBACK_TABLE}
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
    
    # Step 3: Process feedback
    print("\n[Step 3/6] Processing feedback...")
    results = process_all_feedback(spark, feedback_list, existing_examples)
    
    # Step 4: Bulk write
    print("\n[Step 4/6] Writing results...")
    
    if results['new_inserts']:
        bulk_insert_examples(spark, results['new_inserts'])
    else:
        print("  (No new examples to insert)")
    
    if results['count_increments']:
        bulk_update_counts(spark, results['count_increments'])
    else:
        print("  (No counts to update)")
    
    # Step 5: BM25 statistics
    print("\n[Step 5/6] Updating BM25 statistics...")
    update_bm25_statistics(spark)
    
    # Step 6: Sync vector index
    print("\n[Step 6/6] Syncing vector search index...")
    try:
        from databricks.vector_search.client import VectorSearchClient
        client = VectorSearchClient()
        client.get_index(
            endpoint_name="metadata_vectore_search_endpoint",
            index_name="prd_optumrx_orxfdmprdsa.rag.sql_examples_vector_index"
        ).sync()
        print("  ✓ Vector search index synced")
    except Exception as e:
        print(f"  ⚠ Error syncing index: {e}")
    
    # Summary
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
    print(f"Duration:               {duration:.1f} seconds ({duration/60:.1f} minutes)")
    print("=" * 70)
