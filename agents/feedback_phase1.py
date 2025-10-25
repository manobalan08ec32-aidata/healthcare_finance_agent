import hashlib
import re
import uuid
import json
from datetime import datetime
from typing import List, Dict, Optional
from collections import Counter, defaultdict
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# ----------------------------
# CONFIG (fixed)
# ----------------------------
FEEDBACK_TABLE = "prd_optumrx_orxfdmprdsa.rag.fdmbotfeedback_tracking"  # trimmed whitespace
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

# ----------------------------
# TEXT & SQL PROCESSING HELPERS
# ----------------------------

def normalize_question(question: str) -> str:
    """Normalize question for exact matching"""
    if not question:
        return ""
    q = question.lower().strip()
    q = re.sub(r'\s+', ' ', q)
    return q


def generate_hash(text: str) -> str:
    """Generate MD5 hash"""
    if text is None:
        text = ""
    return hashlib.md5(text.encode('utf-8')).hexdigest()


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
    """Extract primary aggregate function from SQL (simple check)"""
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


def normalize_sql_light(sql: str) -> str:
    """
    Lightweight SQL canonicalizer:
    - remove comments, collapse whitespace, lowercase,
    - normalize simple patterns like COUNT(1) -> COUNT(*)
    - remove trailing semicolons
    """
    if not sql:
        return ""
    s = sql
    # remove single-line comments
    s = re.sub(r'--.*?(\n|$)', ' ', s)
    # remove block comments
    s = re.sub(r'/\*.*?\*/', ' ', s, flags=re.DOTALL)
    s = s.lower()
    # normalize count(1) -> count(*)
    s = re.sub(r'count\s*\(\s*1\s*\)', 'count(*)', s)
    s = s.replace('\n', ' ').replace('\r', ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    if s.endswith(';'):
        s = s[:-1].strip()
    s = re.sub(r'\s*,\s*', ',', s)
    s = re.sub(r'\s*\(\s*', '(', s)
    s = re.sub(r'\s*\)\s*', ')', s)
    return s

# ----------------------------
# VECTOR SEARCH (robust mapping)
# ----------------------------

def find_similar_top_k(question_text: str, k: int = 20) -> List[Dict]:
    """
    Find top K most similar questions using Databricks Vector Search.
    Defensive mapping for manifest/columns.
    """
    try:
        from databricks.vector_search.client import VectorSearchClient

        client = VectorSearchClient()
        index = client.get_index(
            endpoint_name="metadata_vectore_search_endpoint",
            index_name="prd_optumrx_orxfdmprdsa.rag.sql_examples_vector_index"
        )

        results = index.similarity_search(
            query_text=question_text,
            columns=["example_id", "question", "sql_query", "table_name"],
            num_results=k
        )

        matches = []
        if not results or not results.get('result') or not results['result'].get('data_array'):
            return matches

        # Build column mapping defensively
        cols = []
        try:
            if results.get('manifest') and results['manifest'].get('columns'):
                cols = [c.get('name') for c in results['manifest']['columns'] if c.get('name')]
        except Exception:
            cols = []
        # fallback set of names (ensure 'score' last)
        if not cols:
            cols = ["example_id", "question", "sql_query", "table_name", "score"]

        for row in results['result'].get('data_array', []):
            # zip will truncate if row shorter than cols; that's fine
            row_dict = dict(zip(cols, row))
            matches.append({
                'example_id': row_dict.get('example_id'),
                'question': row_dict.get('question'),
                'sql_query': row_dict.get('sql_query'),
                'table_name': row_dict.get('table_name'),
                'similarity': float(row_dict.get('score') or 0.0)
            })
        return matches

    except Exception as e:
        print(f"⚠ Vector search error: {e}")
        return []

# ----------------------------
# DEDUPLICATION LOGIC
# ----------------------------

def prepare_insert_data(question: str, sql: str, table_name: str, q_hash: str) -> Dict:
    """Prepare data for insertion (keeps SQL canonicalization local, not persisted)"""
    tokens = tokenize(question)
    term_freq = get_term_frequencies(tokens)
    sql_norm = normalize_sql_light(sql)
    sql_fp = hashlib.md5(sql_norm.encode('utf-8')).hexdigest() if sql_norm else None

    return {
        'example_id': str(uuid.uuid4()),
        'question': question,
        'sql_query': sql,
        'question_hash': q_hash,
        'table_name': table_name,
        'question_tokens': tokens,
        'term_frequencies': term_freq,
        'thumbs_up_count': 1,
        # keep these for internal logs/decisions but do not assume table columns exist
        'sql_norm': sql_norm,
        'sql_fingerprint': sql_fp
    }


def process_single_feedback(question: str, sql: str, table_name: str,
                           existing_examples: Dict[str, List[Dict]]) -> Dict:
    """
    Process single feedback through three-tier deduplication.
    existing_examples: dict mapping question_hash -> list of candidate rows (dicts).
    """
    normalized_q = normalize_question(question)
    q_hash = generate_hash(normalized_q)

    # Precompute normalized SQL of new item
    new_sql_norm = normalize_sql_light(sql)

    # ========== TIER 1: EXACT HASH MATCH (examine all candidates) ==========
    if q_hash in existing_examples and existing_examples[q_hash]:
        # Try to find exact normalized SQL match among candidates
        for candidate in existing_examples[q_hash]:
            cand_sql = candidate.get('sql_query', '')
            cand_sql_norm = candidate.get('sql_norm') or normalize_sql_light(cand_sql)
            if new_sql_norm and cand_sql_norm and new_sql_norm == cand_sql_norm:
                return {
                    'action': 'INCREMENT',
                    'example_id': candidate['example_id'],
                    'status': 'EXACT_DUPLICATE'
                }
        # No exact SQL match: increment the most-trusted example for that hash
        chosen = sorted(existing_examples[q_hash], key=lambda r: r.get('thumbs_up_count') or 0, reverse=True)[0]
        return {
            'action': 'INCREMENT',
            'example_id': chosen['example_id'],
            'status': 'DIFFERENT_SQL'
        }

    # ========== TIER 2 & 3: SEMANTIC SIMILARITY (TOP K) ==========
    similar_matches = find_similar_top_k(question, k=TOP_K_CHECK)

    if not similar_matches:
        # No similar matches - insert as new
        return {
            'action': 'INSERT',
            'data': prepare_insert_data(question, sql, table_name, q_hash)
        }

    new_aggregate = extract_aggregate(sql)
    new_tables = set([t.lower() for t in (table_name or "").replace(';', ',').replace('|', ',').split(',') if t.strip()])

    for candidate in similar_matches:
        similarity = float(candidate.get('similarity') or 0.0)

        if similarity < GRAY_ZONE_START:
            # Lower than gray zone — remaining candidates likely worse (assuming sorted by score)
            break

        if similarity >= TIER2_THRESHOLD:
            return {
                'action': 'INCREMENT',
                'example_id': candidate['example_id'],
                'status': 'SEMANTIC_DUPLICATE_HIGH'
            }

        # Gray zone logic
        if GRAY_ZONE_START <= similarity < TIER2_THRESHOLD:
            candidate_aggregate = extract_aggregate(candidate.get('sql_query', ''))
            candidate_tables = set([t.lower() for t in (candidate.get('table_name') or '').replace(';', ',').replace('|', ',').split(',') if t.strip()])
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


def process_all_feedback(spark: SparkSession, feedback_list: List[Dict],
                        existing_examples: Dict[str, List[Dict]]) -> Dict:
    """Process all feedback items"""
    results = {
        'new_inserts': [],
        'count_increments': defaultdict(int),
        'stats': defaultdict(int)
    }

    print(f"\nProcessing {len(feedback_list)} feedback items...")

    for idx, feedback in enumerate(feedback_list, 1):
        question = feedback.get('user_question')
        sql = feedback.get('sql_generated') or feedback.get('state_info') or ''
        table_name = feedback.get('table_name', '')

        if not question or not sql:
            results['stats']['SKIPPED_INVALID'] += 1
            continue

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

# ----------------------------
# BULK OPERATIONS
# ----------------------------

def bulk_insert_examples(spark: SparkSession, examples: List[Dict]):
    """Bulk insert new examples"""
    if not examples:
        return

    print(f"\n  Bulk inserting {len(examples)} new examples...")

    # Schema with correct types (question_tokens and term_frequencies stored as JSON strings)
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

    df = spark.createDataFrame(rows, schema)
    df.write.format("delta").mode("append").saveAsTable(PROCESSED_TABLE)

    print(f"  ✓ Inserted {len(examples)} examples")


def bulk_update_counts(spark: SparkSession, count_increments: Dict[str, int]):
    """Bulk update thumbs_up counts"""
    if not count_increments:
        return

    print(f"\n  Bulk updating {len(count_increments)} example counts...")

    updates = [(example_id, count) for example_id, count in count_increments.items()]

    schema = StructType([
        StructField("example_id", StringType()),
        StructField("count_increment", IntegerType())
    ])

    updates_df = spark.createDataFrame(updates, schema)
    updates_df.createOrReplaceTempView("count_updates")

    # Bulk update using MERGE with null-safe addition
    spark.sql(f"""
        MERGE INTO {PROCESSED_TABLE} AS target
        USING count_updates AS source
        ON target.example_id = source.example_id
        WHEN MATCHED THEN UPDATE SET
            thumbs_up_count = coalesce(target.thumbs_up_count, 0) + source.count_increment,
            last_updated_at = current_timestamp()
    """)

    print(f"  ✓ Updated {len(count_increments)} examples")

# ----------------------------
# BM25 STATISTICS (distributed)
# ----------------------------

def update_bm25_statistics(spark: SparkSession):
    """Update BM25 corpus statistics using distributed aggregations"""
    print("\nUpdating BM25 statistics...")

    from pyspark.sql.functions import from_json, col, size, avg, count, explode, collect_set
    from pyspark.sql.types import ArrayType, StringType

    # Read only non-null tokens rows
    df = spark.table(PROCESSED_TABLE).select("example_id", "question_tokens").where("question_tokens IS NOT NULL")

    if df.rdd.isEmpty():
        print("  ⚠ No examples to compute statistics")
        return

    # Parse question_tokens JSON -> array
    parsed = df.withColumn("tokens", from_json(col("question_tokens"), ArrayType(StringType())))

    # Document count (N) and avg document length (avgdl)
    stats_row = parsed.select(size(col("tokens")).alias("len_tokens")).agg(
        count("*").alias("N"),
        avg("len_tokens").alias("avgdl")
    ).collect()[0]

    N = int(stats_row['N'])
    avgdl = float(stats_row['avgdl'] or 0.0)

    # Update corpus stats table (simple approach: delete and insert)
    spark.sql(f"DELETE FROM {CORPUS_STATS_TABLE}")
    current_time = datetime.now().isoformat()
    spark.sql(f"""
        INSERT INTO {CORPUS_STATS_TABLE} VALUES
        ('total_documents', '{N}', '{current_time}'),
        ('avg_doc_length', '{avgdl}', '{current_time}')
    """)

    # Document frequency (df): each token counted once per document
    # explode tokens with example_id, then distinct per doc, then group
    exploded = parsed.select("example_id", explode(col("tokens")).alias("token")).where(col("token").isNotNull())
    # unique terms per document
    unique_per_doc = exploded.select("example_id", "token").distinct()
    df_counts = unique_per_doc.groupBy("token").count()

    # Overwrite TERM_DF_TABLE safely: delete and insert in batches
    spark.sql(f"DELETE FROM {TERM_DF_TABLE}")

    # write df_counts into term table in batches to avoid very large single insert SQL
    # Collecting counts to driver in batches can be heavy — but we'll write via DataFrame to a temp table and then insert
    df_counts.createOrReplaceTempView("term_df_counts_temp")
    # Insert all rows via Spark SQL using INSERT INTO ... SELECT to avoid driver-side formatting
    spark.sql(f"""
        INSERT INTO {TERM_DF_TABLE}
        SELECT token as term, cast(count as string) as document_frequency, '{current_time}' as last_updated
        FROM term_df_counts_temp
    """)

    print(f"  ✓ BM25 stats: N={N}, avgdl={avgdl:.2f}, unique_terms={df_counts.count()}")

# ----------------------------
# MAIN BATCH JOB
# ----------------------------

def run_batch_job(spark: SparkSession):
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

    existing_examples = defaultdict(list)
    for row in existing_df.collect():
        rowd = row.asDict()
        # Ensure stable question_hash (recompute if missing)
        qhash = rowd.get('question_hash') or generate_hash(normalize_question(rowd.get('question','')))
        # compute normalized SQL for quick comparisons
        try:
            rowd['sql_norm'] = normalize_sql_light(rowd.get('sql_query','') or '')
        except Exception:
            rowd['sql_norm'] = normalize_sql_light(rowd.get('sql_query','') or '')
        existing_examples[qhash].append(rowd)

    print(f"  ✓ Loaded {sum(len(v) for v in existing_examples.values())} existing examples grouped into {len(existing_examples)} hashes")

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
    print(f"New unique examples:    {results['stats'].get('NEW', 0)}")
    print(f"Duplicates found:       {sum(results['count_increments'].values())}")
    print(f"Duration:               {duration:.1f} seconds ({duration/60:.1f} minutes)")
    print("=" * 70)
