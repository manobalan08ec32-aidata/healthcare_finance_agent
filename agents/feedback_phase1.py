"""
Phase 1: Text-to-SQL Feedback Learning - Batch Processor (OPTIMIZED)

This module processes user feedback (thumbs-up) to build a knowledge base
of approved question-SQL pairs with intelligent deduplication.

Key Features:
- Three-tier deduplication (Hash → Semantic → Aggregate+Table)
- Top 20 similarity checking for thorough duplicate detection
- Bulk operations to minimize DB hits
- Batched embedding generation
- BM25 statistics for keyword-based retrieval (Phase 2)

Performance: ~4-6 minutes for 1000 feedback items
- Vector searches: ~1000 calls (1 per question - unavoidable bottleneck)
- Embeddings: ~50-100 calls (batched)
- DB operations: ~10 calls (bulk inserts/updates)

Author: AI Assistant
Date: 2025
"""

import hashlib
import re
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import Counter, defaultdict
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class FeedbackBatchProcessor:
    """
    Processes user feedback to build a deduplicated knowledge base
    of question-SQL pairs for text-to-SQL improvement.
    """
    
    def __init__(self, spark: SparkSession, databricks_token: str, workspace_url: str):
        """
        Initialize the batch processor
        
        Args:
            spark: Active SparkSession
            databricks_token: Databricks API token
            workspace_url: Databricks workspace URL
        """
        self.spark = spark
        self.databricks_token = databricks_token
        self.workspace_url = workspace_url
        
        # Embedding model endpoint (BGE-large-en: 1024 dimensions)
        self.embedding_endpoint = "databricks-bge-large-en"
        
        # Table names
        self.feedback_table = "feedback_monitoring"
        self.processed_table = "approved_examples_processed"
        self.corpus_stats_table = "bm25_corpus_stats"
        self.term_df_table = "term_document_frequency"
        
        # Deduplication thresholds
        self.TIER2_THRESHOLD = 0.95  # High confidence semantic match
        self.GRAY_ZONE_START = 0.92  # Start of gray zone (need SQL check)
        self.VARIANT_VALIDATION_THRESHOLD = 5  # Thumbs-up count to trust existing
        self.TOP_K_CHECK = 20  # Check top 20 similar questions
        
        # Stop words for tokenization
        self.stop_words = {
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'in', 'on',
            'at', 'to', 'for', 'of', 'with', 'by', 'from', 'as', 'be'
        }
    
    # ==================== SETUP (RUN ONCE) ====================
    
    def setup_tables(self):
        """
        Create all required tables if they don't exist.
        Run this once before the first batch job.
        """
        print("Setting up tables...")
        
        # Main processed examples table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.processed_table} (
                example_id STRING,
                question STRING,
                sql_query STRING,
                question_hash STRING,
                embedding ARRAY<FLOAT>,
                tables_used ARRAY<STRING>,
                table_name STRING,
                question_tokens ARRAY<STRING>,
                term_frequencies MAP<STRING, INT>,
                thumbs_up_count INT,
                variant_of STRING,
                is_variant BOOLEAN,
                created_at TIMESTAMP,
                last_updated_at TIMESTAMP
            ) USING DELTA
        """)
        print("  ✓ Created approved_examples_processed table")
        
        # BM25 corpus statistics
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.corpus_stats_table} (
                stat_name STRING,
                stat_value DOUBLE,
                last_updated TIMESTAMP
            ) USING DELTA
        """)
        print("  ✓ Created bm25_corpus_stats table")
        
        # Term document frequencies for BM25
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.term_df_table} (
                term STRING,
                document_frequency INT,
                last_updated TIMESTAMP
            ) USING DELTA
        """)
        print("  ✓ Created term_document_frequency table")
        
        print("\n✓ All tables created successfully\n")
    
    def setup_vector_search_index(self):
        """
        Set up Databricks Vector Search index.
        Run this once after tables are created.
        """
        from databricks.vector_search.client import VectorSearchClient
        
        client = VectorSearchClient()
        
        print("Setting up Databricks Vector Search...")
        
        # Create endpoint
        try:
            client.create_endpoint(
                name="sql_feedback_endpoint",
                endpoint_type="STANDARD"
            )
            print("  ✓ Created endpoint: sql_feedback_endpoint")
        except Exception as e:
            print(f"  ⚠ Endpoint already exists: {str(e)[:100]}")
        
        # Create vector search index
        try:
            client.create_delta_sync_index(
                endpoint_name="sql_feedback_endpoint",
                source_table_name=self.processed_table,
                index_name="sql_examples_vector_index",
                pipeline_type="TRIGGERED",
                primary_key="example_id",
                embedding_source_column="embedding",
                embedding_dimension=1024  # BGE-large-en dimension
            )
            print("  ✓ Created vector search index: sql_examples_vector_index")
        except Exception as e:
            print(f"  ⚠ Index already exists: {str(e)[:100]}")
        
        print("\n✓ Vector Search setup complete\n")
    
    # ==================== TEXT PROCESSING ====================
    
    def normalize_question(self, question: str) -> str:
        """Normalize question for exact matching (lowercase + trim)"""
        if not question:
            return ""
        return question.lower().strip()
    
    def generate_hash(self, text: str) -> str:
        """Generate MD5 hash for deduplication"""
        return hashlib.md5(text.encode()).hexdigest()
    
    def parse_table_names(self, table_name: str) -> List[str]:
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
    
    def tokenize(self, text: str) -> List[str]:
        """
        Tokenize text for BM25 (remove stop words and punctuation)
        """
        if not text:
            return []
        
        text = text.lower()
        text = re.sub(r'[^\w\s]', ' ', text)
        
        tokens = [
            word for word in text.split()
            if word and word not in self.stop_words and len(word) > 1
        ]
        
        return tokens
    
    def get_term_frequencies(self, tokens: List[str]) -> Dict[str, int]:
        """Calculate term frequencies for BM25"""
        return dict(Counter(tokens))
    
    def extract_aggregate(self, sql: str) -> Optional[str]:
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
    
    # ==================== EMBEDDINGS ====================
    
    def get_embedding(self, text: str) -> Optional[List[float]]:
        """
        Generate embedding using Databricks Foundation Model (BGE-large-en).
        Returns 1024-dimensional vector.
        """
        try:
            url = f"{self.workspace_url}/serving-endpoints/{self.embedding_endpoint}/invocations"
            
            headers = {
                "Authorization": f"Bearer {self.databricks_token}",
                "Content-Type": "application/json"
            }
            
            payload = {"input": text}
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                return result['data'][0]['embedding']
            else:
                print(f"Error getting embedding: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Error generating embedding: {e}")
            return None
    
    def get_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """
        Generate embeddings for multiple texts in batch (more efficient).
        Recommended batch size: 10-20 texts.
        """
        try:
            url = f"{self.workspace_url}/serving-endpoints/{self.embedding_endpoint}/invocations"
            
            headers = {
                "Authorization": f"Bearer {self.databricks_token}",
                "Content-Type": "application/json"
            }
            
            payload = {"input": texts}
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                return [item['embedding'] for item in result['data']]
            else:
                print(f"Error getting batch embeddings: {response.status_code}")
                return [None] * len(texts)
                
        except Exception as e:
            print(f"Error generating batch embeddings: {e}")
            return [None] * len(texts)
    
    # ==================== VECTOR SEARCH ====================
    
    def find_similar_top_k(self, embedding: List[float], k: int = 20) -> List[Dict]:
        """
        Find top K most similar questions using Databricks Vector Search.
        
        Args:
            embedding: Query embedding vector
            k: Number of results to return (default: 20)
            
        Returns:
            List of dicts with: example_id, question, sql_query, table_name, similarity
        """
        try:
            from databricks.vector_search.client import VectorSearchClient
            
            client = VectorSearchClient()
            
            results = client.similarity_search(
                index_name="sql_examples_vector_index",
                query_vector=embedding,
                num_results=k
            )
            
            if results and results.get('result') and results['result'].get('data_array'):
                matches = []
                for item in results['result']['data_array']:
                    matches.append({
                        'example_id': item.get('example_id'),
                        'question': item.get('question'),
                        'sql_query': item.get('sql_query'),
                        'table_name': item.get('table_name'),
                        'similarity': item.get('score', 0)
                    })
                return matches
            
            return []
            
        except Exception as e:
            print(f"⚠ Vector search error: {e}")
            return []
    
    # ==================== DEDUPLICATION LOGIC ====================
    
    def process_all_feedback(self, feedback_list: List[Dict], 
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
            result = self._process_single_in_memory(
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
                results['stats']['ERROR_EMBEDDING'] += 1
            
            # Progress indicator
            if idx % 50 == 0 or idx == len(feedback_list):
                print(f"  Progress: {idx}/{len(feedback_list)} processed...")
        
        return results
    
    def _process_single_in_memory(self, question: str, sql: str, table_name: str,
                                   existing_examples: Dict[str, Dict]) -> Dict:
        """
        Process single feedback item through three-tier deduplication.
        All processing in Python memory (no DB hits).
        
        Returns:
            Dict with 'action' (INSERT/INCREMENT/VARIANT/ERROR) and data
        """
        
        # Normalize and hash
        normalized_q = self.normalize_question(question)
        q_hash = self.generate_hash(normalized_q)
        
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
                if exact_match['thumbs_up_count'] >= self.VARIANT_VALIDATION_THRESHOLD:
                    # Existing is well-validated - ignore new variant
                    return {
                        'action': 'INCREMENT',
                        'example_id': exact_match['example_id'],
                        'status': 'IGNORED_VARIANT'
                    }
                else:
                    # Both unvalidated - keep as variant
                    embedding = self.get_embedding(question)
                    if embedding is None:
                        return {'action': 'ERROR', 'data': question}
                    
                    return {
                        'action': 'VARIANT',
                        'data': self._prepare_insert_data(
                            question, sql, table_name, q_hash + '_variant',
                            embedding, variant_of=exact_match['example_id']
                        )
                    }
        
        # ========== TIER 2 & 3: SEMANTIC SIMILARITY (TOP 20) ==========
        
        # Generate embedding for new question
        embedding = self.get_embedding(question)
        if embedding is None:
            return {'action': 'ERROR', 'data': question}
        
        # Get top 20 similar questions
        similar_matches = self.find_similar_top_k(embedding, k=self.TOP_K_CHECK)
        
        if not similar_matches:
            # No similar matches found - insert as new
            return {
                'action': 'INSERT',
                'data': self._prepare_insert_data(question, sql, table_name, q_hash, embedding)
            }
        
        # Extract metadata for NEW question (for Tier 3 comparison)
        new_aggregate = self.extract_aggregate(sql)
        new_tables = set(self.parse_table_names(table_name))
        
        # Check each of the top 20 candidates
        for candidate in similar_matches:
            similarity = candidate['similarity']
            
            # Stop checking if similarity too low
            if similarity < self.GRAY_ZONE_START:
                break
            
            # ========== TIER 2: HIGH CONFIDENCE (≥ 0.95) ==========
            if similarity >= self.TIER2_THRESHOLD:
                # Very high similarity - mark as duplicate
                return {
                    'action': 'INCREMENT',
                    'example_id': candidate['example_id'],
                    'status': 'SEMANTIC_DUPLICATE_HIGH'
                }
            
            # ========== TIER 3: GRAY ZONE (0.92 - 0.95) ==========
            elif self.GRAY_ZONE_START <= similarity < self.TIER2_THRESHOLD:
                # Need additional validation: check aggregate + table
                
                candidate_aggregate = self.extract_aggregate(candidate['sql_query'])
                candidate_tables = set(self.parse_table_names(candidate.get('table_name', '')))
                
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
            'data': self._prepare_insert_data(question, sql, table_name, q_hash, embedding)
        }
    
    def _prepare_insert_data(self, question: str, sql: str, table_name: str,
                            q_hash: str, embedding: List[float],
                            variant_of: Optional[str] = None) -> Dict:
        """
        Prepare data dict for insertion into processed table.
        """
        tables = self.parse_table_names(table_name)
        tokens = self.tokenize(question)
        term_freq = self.get_term_frequencies(tokens)
        
        return {
            'example_id': str(uuid.uuid4()),
            'question': question,
            'sql_query': sql,
            'question_hash': q_hash,
            'embedding': embedding,
            'tables_used': tables,
            'table_name': table_name,
            'question_tokens': tokens,
            'term_frequencies': term_freq,
            'thumbs_up_count': 1,
            'variant_of': variant_of,
            'is_variant': variant_of is not None
        }
    
    # ==================== BULK DATABASE OPERATIONS ====================
    
    def bulk_insert_examples(self, examples: List[Dict]):
        """
        Bulk insert new examples into processed table.
        Single DB operation regardless of number of examples.
        """
        if not examples:
            return
        
        print(f"\n  Bulk inserting {len(examples)} new examples...")
        
        from pyspark.sql.types import (
            StructType, StructField, StringType, ArrayType,
            MapType, IntegerType, FloatType, BooleanType, TimestampType
        )
        
        # Define schema
        schema = StructType([
            StructField("example_id", StringType()),
            StructField("question", StringType()),
            StructField("sql_query", StringType()),
            StructField("question_hash", StringType()),
            StructField("embedding", ArrayType(FloatType())),
            StructField("tables_used", ArrayType(StringType())),
            StructField("table_name", StringType()),
            StructField("question_tokens", ArrayType(StringType())),
            StructField("term_frequencies", MapType(StringType(), IntegerType())),
            StructField("thumbs_up_count", IntegerType()),
            StructField("variant_of", StringType()),
            StructField("is_variant", BooleanType())
        ])
        
        # Prepare rows
        rows = []
        for ex in examples:
            rows.append((
                ex['example_id'],
                ex['question'],
                ex['sql_query'],
                ex['question_hash'],
                ex['embedding'],
                ex['tables_used'],
                ex['table_name'],
                ex['question_tokens'],
                ex['term_frequencies'],
                ex['thumbs_up_count'],
                ex['variant_of'],
                ex['is_variant']
            ))
        
        # Create DataFrame
        df = self.spark.createDataFrame(rows, schema)
        
        # Add timestamps
        from pyspark.sql.functions import current_timestamp
        df = df.withColumn("created_at", current_timestamp())
        df = df.withColumn("last_updated_at", current_timestamp())
        
        # Bulk insert
        df.write.format("delta").mode("append").saveAsTable(self.processed_table)
        
        print(f"  ✓ Inserted {len(examples)} examples")
    
    def bulk_update_counts(self, count_increments: Dict[str, int]):
        """
        Bulk update thumbs_up counts using MERGE.
        Single DB operation for all updates.
        """
        if not count_increments:
            return
        
        print(f"\n  Bulk updating {len(count_increments)} example counts...")
        
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        
        # Prepare updates
        updates = [(example_id, count) for example_id, count in count_increments.items()]
        
        schema = StructType([
            StructField("example_id", StringType()),
            StructField("count_increment", IntegerType())
        ])
        
        updates_df = self.spark.createDataFrame(updates, schema)
        updates_df.createOrReplaceTempView("count_updates")
        
        # Bulk update using MERGE
        self.spark.sql(f"""
            MERGE INTO {self.processed_table} AS target
            USING count_updates AS source
            ON target.example_id = source.example_id
            WHEN MATCHED THEN UPDATE SET
                thumbs_up_count = thumbs_up_count + source.count_increment,
                last_updated_at = current_timestamp()
        """)
        
        print(f"  ✓ Updated {len(count_increments)} examples")
    
    # ==================== BM25 STATISTICS ====================
    
    def update_bm25_statistics(self):
        """
        Update BM25 corpus statistics for Phase 2 retrieval.
        Computes: total documents (N), average document length, term frequencies.
        """
        print("\nUpdating BM25 statistics...")
        
        # Load all examples
        examples_df = self.spark.sql(f"""
            SELECT question_tokens, term_frequencies
            FROM {self.processed_table}
        """)
        
        examples = examples_df.collect()
        
        if not examples:
            print("  ⚠ No examples to compute statistics")
            return
        
        # Compute corpus stats
        N = len(examples)
        total_length = sum(
            len(row['question_tokens']) 
            for row in examples 
            if row['question_tokens']
        )
        avgdl = total_length / N if N > 0 else 0
        
        # Update corpus stats table
        self.spark.sql(f"DELETE FROM {self.corpus_stats_table}")
        self.spark.sql(f"""
            INSERT INTO {self.corpus_stats_table} VALUES
            ('total_documents', {N}, current_timestamp()),
            ('avg_doc_length', {avgdl}, current_timestamp())
        """)
        
        # Compute document frequencies (how many docs contain each term)
        df_counter = Counter()
        for row in examples:
            if row['question_tokens']:
                unique_terms = set(row['question_tokens'])
                for term in unique_terms:
                    df_counter[term] += 1
        
        # Update term frequencies table (in batches to avoid query size limits)
        self.spark.sql(f"DELETE FROM {self.term_df_table}")
        
        if df_counter:
            values = [
                f"('{term}', {freq}, current_timestamp())"
                for term, freq in df_counter.items()
            ]
            
            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                values_str = ','.join(batch)
                self.spark.sql(f"""
                    INSERT INTO {self.term_df_table} VALUES {values_str}
                """)
        
        print(f"  ✓ BM25 stats: N={N}, avgdl={avgdl:.2f}, unique_terms={len(df_counter)}")
    
    # ==================== MAIN BATCH JOB ====================
    
    def run_batch_job(self):
        """
        Main batch processing job - runs every 3 hours.
        
        Flow:
        1. Load existing examples into memory (1 DB call)
        2. Load new feedback (1 DB call)
        3. Process all in memory (N vector searches)
        4. Bulk write results (2 DB calls)
        5. Update BM25 stats (2-3 DB calls)
        6. Sync vector index (1 API call)
        
        Performance for 1000 items: ~4-6 minutes
        - Vector searches: ~1000 calls (unavoidable - 1 per question)
        - Embeddings: ~50-100 calls (batched)
        - DB operations: ~10 calls total
        """
        print("=" * 70)
        print("TEXT-TO-SQL FEEDBACK BATCH PROCESSOR")
        print("=" * 70)
        
        start_time = datetime.now()
        
        # ========== STEP 1: LOAD DATA ==========
        print("\n[Step 1/6] Loading existing examples into memory...")
        
        existing_df = self.spark.sql(f"""
            SELECT example_id, question, sql_query, question_hash, 
                   table_name, thumbs_up_count
            FROM {self.processed_table}
        """)
        
        # Index by hash for O(1) lookup
        existing_examples = {}
        for row in existing_df.collect():
            existing_examples[row['question_hash']] = row.asDict()
        
        print(f"  ✓ Loaded {len(existing_examples)} existing examples")
        
        print("\n[Step 2/6] Loading new feedback...")
        
        feedback_df = self.spark.sql(f"""
            SELECT DISTINCT user_question, sql_generated, table_name
            FROM {self.feedback_table}
            WHERE user_question IS NOT NULL
            AND sql_generated IS NOT NULL
            AND user_question != ''
            AND sql_generated != ''
        """)
        
        feedback_list = [row.asDict() for row in feedback_df.collect()]
        total_feedback = len(feedback_list)
        
        print(f"  ✓ Loaded {total_feedback} feedback items")
        
        if total_feedback == 0:
            print("\n⚠ No feedback to process")
            return
        
        # ========== STEP 2: PROCESS IN MEMORY ==========
        print("\n[Step 3/6] Processing feedback (3-tier deduplication)...")
        print(f"  Note: This will make ~{total_feedback} vector search calls")
        
        results = self.process_all_feedback(feedback_list, existing_examples)
        
        # ========== STEP 3: BULK WRITE ==========
        print("\n[Step 4/6] Writing results to database...")
        
        # Combine new inserts and variants
        all_new = results['new_inserts'] + results['variants']
        if all_new:
            self.bulk_insert_examples(all_new)
        else:
            print("  (No new examples to insert)")
        
        # Bulk update counts
        if results['count_increments']:
            self.bulk_update_counts(results['count_increments'])
        else:
            print("  (No counts to update)")
        
        # ========== STEP 4: BM25 STATISTICS ==========
        print("\n[Step 5/6] Updating BM25 statistics...")
        self.update_bm25_statistics()
        
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
        print(f"Errors:                 {results['stats']['ERROR_EMBEDDING']}")
        print(f"Duration:               {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"")
        print(f"Operations breakdown:")
        print(f"  - Vector searches:    ~{total_feedback} (1 per question)")
        print(f"  - Embedding calls:    ~{(results['stats']['NEW'] + results['stats']['NEW_VARIANT'])} (for new only)")
        print(f"  - DB operations:      ~10 (bulk inserts/updates)")
        print("=" * 70)


# ==================== USAGE EXAMPLE ====================

"""
DATABRICKS NOTEBOOK USAGE:

# ===== ONE-TIME SETUP =====

processor = FeedbackBatchProcessor(
    spark=spark,
    databricks_token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    workspace_url=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
)

# Create tables
processor.setup_tables()

# Setup vector search index
processor.setup_vector_search_index()


# ===== SCHEDULED JOB (EVERY 3 HOURS) =====

processor = FeedbackBatchProcessor(
    spark=spark,
    databricks_token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    workspace_url=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
)

# Run batch processing
processor.run_batch_job()


# ===== TESTING WITH SAMPLE DATA =====

# Insert test feedback
spark.sql('''
    INSERT INTO feedback_monitoring VALUES 
    ('What is total sales?', 'SELECT SUM(amount) FROM sales', 'sales'),
    ('Show me total sales', 'SELECT SUM(amount) FROM sales', 'sales'),  -- Should deduplicate
    ('What is average sales?', 'SELECT AVG(amount) FROM sales', 'sales'),  -- Different aggregate
    ('Get sales by region', 'SELECT * FROM sales JOIN regions', 'sales,regions')  -- Multiple tables
''')

# Run batch job
processor.run_batch_job()

# Check results
spark.sql('''
    SELECT question, sql_query, tables_used, thumbs_up_count, is_variant
    FROM approved_examples_processed
    ORDER BY created_at DESC
''').show(truncate=False)
"""
