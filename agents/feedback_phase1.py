# Phase 1: Batch Processing for Text-to-SQL Feedback Learning
# Runs every 3 hours to process feedback and build the knowledge base
# Uses Databricks Foundation Models for embeddings (no OpenAI needed)

import hashlib
import re
import uuid
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import Counter
import math
import requests
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, current_timestamp
from pyspark.sql.types import StringType, ArrayType, MapType, IntegerType, FloatType, BooleanType
import sqlparse


class FeedbackBatchProcessor:
    """
    Main class for processing feedback and building the knowledge base
    Uses Databricks Foundation Models for embeddings
    """
    
    def __init__(self, spark: SparkSession, databricks_token: str, workspace_url: str):
        self.spark = spark
        self.databricks_token = databricks_token
        self.workspace_url = workspace_url
        
        # Embedding model endpoint
        # Using BGE-large-en - good quality, open source
        self.embedding_endpoint = "databricks-bge-large-en"
        
        # Table names
        self.feedback_table = "feedback_monitoring"
        self.processed_table = "approved_examples_processed"
        self.corpus_stats_table = "bm25_corpus_stats"
        self.term_df_table = "term_document_frequency"
        
        # Deduplication thresholds
        self.TIER2_THRESHOLD = 0.95  # High confidence duplicate
        self.GRAY_ZONE_START = 0.92  # Start of gray zone
        self.SQL_SIMILARITY_THRESHOLD = 0.85  # SQL similarity for gray zone
        self.VARIANT_VALIDATION_THRESHOLD = 5  # Thumbs up count to trust existing
        
        # Stop words for tokenization
        self.stop_words = {
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'in', 'on', 
            'at', 'to', 'for', 'of', 'with', 'by', 'from', 'as', 'be'
        }
    
    # ========== SETUP: Create Tables ==========
    
    def setup_tables(self):
        """
        Create all required tables if they don't exist
        Run this once before first batch job
        """
        
        # 1. Processed examples table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.processed_table} (
                example_id STRING,
                question STRING,
                sql_query STRING,
                question_hash STRING,
                embedding ARRAY<FLOAT>,
                tables_used ARRAY<STRING>,
                question_tokens ARRAY<STRING>,
                term_frequencies MAP<STRING, INT>,
                thumbs_up_count INT,
                variant_of STRING,
                is_variant BOOLEAN,
                created_at TIMESTAMP,
                last_updated_at TIMESTAMP
            )
            USING DELTA
        """)
        
        print("✓ Created approved_examples_processed table")
        
        # 2. BM25 corpus stats table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.corpus_stats_table} (
                stat_name STRING,
                stat_value DOUBLE,
                last_updated TIMESTAMP
            )
            USING DELTA
        """)
        
        print("✓ Created bm25_corpus_stats table")
        
        # 3. Term document frequency table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.term_df_table} (
                term STRING,
                document_frequency INT,
                last_updated TIMESTAMP
            )
            USING DELTA
        """)
        
        print("✓ Created term_document_frequency table")
        
        print("\n✓ All tables created successfully")
    
    # ========== NORMALIZATION & HASHING ==========
    
    def normalize_question(self, question: str) -> str:
        """Normalize question for exact matching"""
        if not question:
            return ""
        return question.lower().strip()
    
    def generate_hash(self, text: str) -> str:
        """Generate MD5 hash for deduplication"""
        return hashlib.md5(text.encode()).hexdigest()
    
    def parse_table_names(self, table_name: str) -> List[str]:
        """
        Parse table name(s) from feedback_monitoring
        Handles various formats:
        - Single table: "sales"
        - Comma-separated: "sales,customers"
        - Space-separated: "sales customers"
        - Mixed: "sales, orders, products"
        """
        if not table_name:
            return []
        
        # Replace common separators with comma
        table_name = table_name.replace(';', ',').replace('|', ',')
        
        # Split by comma or whitespace
        tables = []
        for part in table_name.split(','):
            # Further split by whitespace
            for table in part.split():
                cleaned = table.strip().lower()
                if cleaned:
                    tables.append(cleaned)
        
        return list(set(tables))  # Remove duplicates
    
    def normalize_sql(self, sql: str) -> str:
        """Normalize SQL for comparison"""
        if not sql:
            return ""
        
        try:
            # Use sqlparse for proper normalization
            formatted = sqlparse.format(
                sql,
                reindent=True,
                keyword_case='upper',
                strip_comments=True
            )
            
            # Remove extra whitespace
            normalized = re.sub(r'\s+', ' ', formatted).strip().lower()
            return normalized
        except:
            # Fallback to simple normalization
            return re.sub(r'\s+', ' ', sql.lower().strip())
    
    # ========== EMBEDDING GENERATION (DATABRICKS) ==========
    
    def get_embedding(self, text: str) -> Optional[List[float]]:
        """
        Generate embedding using Databricks Foundation Model
        Uses BGE-large-en model (1024 dimensions)
        """
        try:
            # Databricks serving endpoint URL
            url = f"{self.workspace_url}/serving-endpoints/{self.embedding_endpoint}/invocations"
            
            headers = {
                "Authorization": f"Bearer {self.databricks_token}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "input": text
            }
            
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                # BGE returns embeddings in 'data' field
                embedding = result['data'][0]['embedding']
                return embedding
            else:
                print(f"Error getting embedding: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"Error generating embedding: {e}")
            return None
    
    def get_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """
        Generate embeddings for multiple texts in batch (more efficient)
        """
        try:
            url = f"{self.workspace_url}/serving-endpoints/{self.embedding_endpoint}/invocations"
            
            headers = {
                "Authorization": f"Bearer {self.databricks_token}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "input": texts
            }
            
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                embeddings = [item['embedding'] for item in result['data']]
                return embeddings
            else:
                print(f"Error getting batch embeddings: {response.status_code}")
                return [None] * len(texts)
                
        except Exception as e:
            print(f"Error generating batch embeddings: {e}")
            return [None] * len(texts)
    
    # ========== METADATA EXTRACTION ==========
    
    def extract_tables_from_sql(self, sql: str) -> List[str]:
        """
        Extract table names from SQL query
        """
        if not sql:
            return []
        
        tables = []
        
        try:
            # Parse SQL
            parsed = sqlparse.parse(sql)
            
            if not parsed:
                return []
            
            stmt = parsed[0]
            
            # Extract table names
            from_seen = False
            for token in stmt.tokens:
                if from_seen:
                    if isinstance(token, sqlparse.sql.Identifier):
                        tables.append(str(token.get_real_name()).lower())
                    elif isinstance(token, sqlparse.sql.IdentifierList):
                        for identifier in token.get_identifiers():
                            tables.append(str(identifier.get_real_name()).lower())
                
                if token.ttype is sqlparse.tokens.Keyword and token.value.upper() in ('FROM', 'JOIN'):
                    from_seen = True
                elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() in ('WHERE', 'GROUP', 'ORDER', 'LIMIT'):
                    from_seen = False
        
        except Exception as e:
            print(f"Error parsing SQL: {e}")
            return []
        
        return list(set(tables))  # Remove duplicates
    
    def tokenize(self, text: str) -> List[str]:
        """
        Tokenize text for BM25
        """
        if not text:
            return []
        
        # Lowercase
        text = text.lower()
        
        # Remove punctuation
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Split and remove stop words
        tokens = [
            word for word in text.split() 
            if word and word not in self.stop_words and len(word) > 1
        ]
        
        return tokens
    
    def get_term_frequencies(self, tokens: List[str]) -> Dict[str, int]:
        """
        Calculate term frequencies
        """
        return dict(Counter(tokens))
    
    # ========== SQL SIMILARITY ==========
    
    def extract_select_metrics(self, sql: str) -> List[str]:
        """
        Extract aggregation functions and columns from SELECT clause
        """
        if not sql:
            return []
        
        metrics = []
        
        # Extract SELECT clause
        match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not match:
            return []
        
        select_clause = match.group(1)
        
        # Find aggregation functions
        agg_pattern = r'(SUM|AVG|COUNT|MAX|MIN|DISTINCT)\s*\(([^)]+)\)'
        agg_matches = re.findall(agg_pattern, select_clause, re.IGNORECASE)
        
        for func, col in agg_matches:
            metrics.append(func.upper())
            # Extract column name (remove table prefix if exists)
            col_name = col.strip().split('.')[-1].lower()
            metrics.append(col_name)
        
        return metrics
    
    def compute_sql_similarity(self, sql1: str, sql2: str) -> float:
        """
        Compute similarity between two SQL queries
        """
        # Exact match check first
        norm1 = self.normalize_sql(sql1)
        norm2 = self.normalize_sql(sql2)
        
        if norm1 == norm2:
            return 1.0
        
        # Extract components
        metrics1 = self.extract_select_metrics(sql1)
        metrics2 = self.extract_select_metrics(sql2)
        
        tables1 = self.extract_tables_from_sql(sql1)
        tables2 = self.extract_tables_from_sql(sql2)
        
        # Compute overlaps
        if not metrics1 or not metrics2:
            metric_overlap = 0
        else:
            metric_overlap = len(set(metrics1) & set(metrics2)) / max(len(metrics1), len(metrics2))
        
        if not tables1 or not tables2:
            table_overlap = 0
        else:
            table_overlap = len(set(tables1) & set(tables2)) / max(len(tables1), len(tables2))
        
        # Weighted similarity
        sql_similarity = (0.6 * metric_overlap) + (0.4 * table_overlap)
        
        return sql_similarity
    
    # ========== VECTOR SEARCH INTEGRATION ==========
    
    def setup_vector_search_index(self):
        """
        Set up Databricks Vector Search index
        Run this once after tables are created
        """
        from databricks.vector_search.client import VectorSearchClient
        
        client = VectorSearchClient()
        
        print("Setting up Vector Search...")
        
        # Create endpoint if it doesn't exist
        try:
            client.create_endpoint(
                name="sql_feedback_endpoint",
                endpoint_type="STANDARD"
            )
            print("✓ Created endpoint: sql_feedback_endpoint")
        except Exception as e:
            print(f"⚠ Endpoint might already exist: {e}")
        
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
            print("✓ Created vector search index: sql_examples_vector_index")
        except Exception as e:
            print(f"⚠ Index might already exist: {e}")
        
        print("\n✓ Vector Search setup complete")
    
    def sync_vector_search_index(self):
        """
        Sync vector search index after batch updates
        """
        from databricks.vector_search.client import VectorSearchClient
        
        try:
            client = VectorSearchClient()
            client.get_index(
                endpoint_name="sql_feedback_endpoint",
                index_name="sql_examples_vector_index"
            ).sync()
            print("✓ Vector search index synced")
        except Exception as e:
            print(f"⚠ Error syncing index: {e}")
    
    # ========== THREE-TIER DEDUPLICATION ==========
    
    def find_by_hash(self, question_hash: str) -> Optional[Dict]:
        """
        Tier 1: Find exact match by hash
        """
        result = self.spark.sql(f"""
            SELECT example_id, question, sql_query, thumbs_up_count, variant_of
            FROM {self.processed_table}
            WHERE question_hash = '{question_hash}'
        """).first()
        
        if result:
            return result.asDict()
        return None
    
    def find_similar_by_embedding(self, embedding: List[float], threshold: float = 0.92) -> Optional[Dict]:
        """
        Tier 2 & 3: Find similar question by embedding using Vector Search
        """
        try:
            from databricks.vector_search.client import VectorSearchClient
            
            client = VectorSearchClient()
            
            # Search for similar vectors
            results = client.similarity_search(
                index_name="sql_examples_vector_index",
                query_vector=embedding,
                num_results=1
            )
            
            if results and results.get('result') and results['result'].get('data_array'):
                top_match = results['result']['data_array'][0]
                
                similarity = top_match.get('score', 0)
                
                if similarity >= threshold:
                    return {
                        'example_id': top_match.get('example_id'),
                        'question': top_match.get('question'),
                        'sql_query': top_match.get('sql_query'),
                        'similarity': similarity
                    }
            
            return None
            
        except Exception as e:
            print(f"⚠ Vector search error: {e}")
            return None
    
    def process_single_feedback(self, question: str, sql: str, table_name: str) -> Tuple[str, Optional[str]]:
        """
        Process a single feedback item through three-tier deduplication
        
        Args:
            question: User's question
            sql: Generated SQL
            table_name: Table name from feedback_monitoring
        
        Returns: (status, example_id or None)
        """
        
        # Normalize and hash
        normalized_q = self.normalize_question(question)
        q_hash = self.generate_hash(normalized_q)
        
        # ===== TIER 1: EXACT MATCH =====
        exact_match = self.find_by_hash(q_hash)
        
        if exact_match:
            # Found exact question match
            norm_sql_new = self.normalize_sql(sql)
            norm_sql_existing = self.normalize_sql(exact_match['sql_query'])
            
            if norm_sql_new == norm_sql_existing:
                # Exact duplicate - increment count
                self.spark.sql(f"""
                    UPDATE {self.processed_table}
                    SET thumbs_up_count = thumbs_up_count + 1,
                        last_updated_at = current_timestamp()
                    WHERE question_hash = '{q_hash}'
                """)
                return ("EXACT_DUPLICATE", exact_match['example_id'])
            
            else:
                # Same question, different SQL
                if exact_match['thumbs_up_count'] >= self.VARIANT_VALIDATION_THRESHOLD:
                    # Existing is validated - ignore new variant
                    return ("IGNORED_VARIANT", exact_match['example_id'])
                else:
                    # Both unvalidated - keep as variant
                    new_id = str(uuid.uuid4())
                    embedding = self.get_embedding(question)
                    
                    if embedding is None:
                        return ("ERROR_EMBEDDING", None)
                    
                    # Parse table names from feedback + extract from SQL
                    tables = self.parse_table_names(table_name)
                    sql_tables = self.extract_tables_from_sql(sql)
                    tables = list(set(tables + sql_tables))
                    
                    tokens = self.tokenize(question)
                    term_freq = self.get_term_frequencies(tokens)
                    
                    # Prepare data for insertion
                    self._insert_example(
                        new_id, question, sql, f"{q_hash}_variant_{new_id[:8]}",
                        embedding, tables, tokens, term_freq,
                        thumbs_up_count=1, variant_of=exact_match['example_id'], is_variant=True
                    )
                    
                    # Mark parent as having variants
                    self.spark.sql(f"""
                        UPDATE {self.processed_table}
                        SET is_variant = true
                        WHERE example_id = '{exact_match['example_id']}'
                    """)
                    
                    return ("NEW_VARIANT", new_id)
        
        # ===== TIER 2 & 3: SEMANTIC SIMILARITY =====
        # Generate embedding
        embedding = self.get_embedding(question)
        
        if embedding is None:
            return ("ERROR_EMBEDDING", None)
        
        # Find similar using vector search
        similar_match = self.find_similar_by_embedding(embedding, self.GRAY_ZONE_START)
        
        if similar_match:
            similarity = similar_match.get('similarity', 0)
            
            if similarity >= self.TIER2_THRESHOLD:
                # High confidence duplicate
                self.spark.sql(f"""
                    UPDATE {self.processed_table}
                    SET thumbs_up_count = thumbs_up_count + 1,
                        last_updated_at = current_timestamp()
                    WHERE example_id = '{similar_match['example_id']}'
                """)
                return ("SEMANTIC_DUPLICATE_HIGH", similar_match['example_id'])
            
            elif self.GRAY_ZONE_START <= similarity < self.TIER2_THRESHOLD:
                # Gray zone - check SQL similarity
                sql_sim = self.compute_sql_similarity(sql, similar_match['sql_query'])
                
                if sql_sim >= self.SQL_SIMILARITY_THRESHOLD:
                    # SQL confirms duplicate
                    self.spark.sql(f"""
                        UPDATE {self.processed_table}
                        SET thumbs_up_count = thumbs_up_count + 1,
                            last_updated_at = current_timestamp()
                        WHERE example_id = '{similar_match['example_id']}'
                    """)
                    return ("SEMANTIC_DUPLICATE_SQL_CONFIRMED", similar_match['example_id'])
                else:
                    # SQL different - keep as new
                    pass  # Fall through to insertion
        
        # ===== NEW UNIQUE QUESTION =====
        new_id = str(uuid.uuid4())
        
        # Parse table names from feedback + extract from SQL
        tables = self.parse_table_names(table_name)
        sql_tables = self.extract_tables_from_sql(sql)
        tables = list(set(tables + sql_tables))
        
        tokens = self.tokenize(question)
        term_freq = self.get_term_frequencies(tokens)
        
        # Insert new example
        self._insert_example(
            new_id, question, sql, q_hash,
            embedding, tables, tokens, term_freq,
            thumbs_up_count=1, variant_of=None, is_variant=False
        )
        
        return ("NEW", new_id)
    
    def _insert_example(self, example_id: str, question: str, sql: str, q_hash: str,
                       embedding: List[float], tables: List[str], tokens: List[str],
                       term_freq: Dict[str, int], thumbs_up_count: int,
                       variant_of: Optional[str], is_variant: bool):
        """
        Helper method to insert example into processed table
        """
        # Escape single quotes
        question_escaped = question.replace("'", "''")
        sql_escaped = sql.replace("'", "''")
        
        # Format arrays and maps
        embedding_str = ','.join(map(str, embedding))
        tables_str = ','.join([f"'{t}'" for t in tables]) if tables else ''
        tokens_str = ','.join([f"'{t}'" for t in tokens]) if tokens else ''
        term_freq_str = ','.join([f"'{k}',{v}" for k, v in term_freq.items()]) if term_freq else ''
        
        variant_of_str = f"'{variant_of}'" if variant_of else "null"
        
        self.spark.sql(f"""
            INSERT INTO {self.processed_table} VALUES (
                '{example_id}',
                '{question_escaped}',
                '{sql_escaped}',
                '{q_hash}',
                array({embedding_str}),
                array({tables_str}),
                array({tokens_str}),
                map({term_freq_str}),
                {thumbs_up_count},
                {variant_of_str},
                {is_variant},
                current_timestamp(),
                current_timestamp()
            )
        """)
    
    # ========== BM25 STATISTICS ==========
    
    def update_bm25_statistics(self):
        """
        Update BM25 corpus statistics and term document frequencies
        """
        print("\nUpdating BM25 statistics...")
        
        # Get all examples
        examples_df = self.spark.sql(f"""
            SELECT question_tokens, term_frequencies
            FROM {self.processed_table}
        """)
        
        examples = examples_df.collect()
        
        if not examples:
            print("⚠ No examples to compute statistics")
            return
        
        # Compute corpus stats
        N = len(examples)
        total_length = sum(len(row['question_tokens']) for row in examples if row['question_tokens'])
        avgdl = total_length / N if N > 0 else 0
        
        # Update corpus stats table
        self.spark.sql(f"DELETE FROM {self.corpus_stats_table}")
        self.spark.sql(f"""
            INSERT INTO {self.corpus_stats_table} VALUES
            ('total_documents', {N}, current_timestamp()),
            ('avg_doc_length', {avgdl}, current_timestamp())
        """)
        
        # Compute document frequencies
        df_counter = Counter()
        for row in examples:
            if row['question_tokens']:
                unique_terms = set(row['question_tokens'])
                for term in unique_terms:
                    df_counter[term] += 1
        
        # Update term DF table
        self.spark.sql(f"DELETE FROM {self.term_df_table}")
        
        if df_counter:
            values = [f"('{term}', {freq}, current_timestamp())" 
                     for term, freq in df_counter.items()]
            values_str = ','.join(values)
            self.spark.sql(f"""
                INSERT INTO {self.term_df_table} VALUES {values_str}
            """)
        
        print(f"✓ BM25 statistics updated: N={N}, avgdl={avgdl:.2f}, unique_terms={len(df_counter)}")
    
    # ========== MAIN BATCH JOB ==========
    
    def run_batch_job(self):
        """
        Main batch processing job
        """
        print("=" * 70)
        print("STARTING BATCH PROCESSING JOB")
        print("=" * 70)
        
        start_time = datetime.now()
        
        # Extract new feedback
        print("\nExtracting feedback from feedback_monitoring table...")
        
        feedback_df = self.spark.sql(f"""
            SELECT DISTINCT user_question, sql_generated, table_name
            FROM {self.feedback_table}
            WHERE user_question IS NOT NULL 
            AND sql_generated IS NOT NULL
            AND user_question != ''
            AND sql_generated != ''
        """)
        
        feedback_list = feedback_df.collect()
        total_feedback = len(feedback_list)
        
        print(f"Total feedback items to process: {total_feedback}")
        
        if total_feedback == 0:
            print("\n⚠ No feedback to process")
            return
        
        # Statistics tracking
        stats = {
            'EXACT_DUPLICATE': 0,
            'IGNORED_VARIANT': 0,
            'NEW_VARIANT': 0,
            'SEMANTIC_DUPLICATE_HIGH': 0,
            'SEMANTIC_DUPLICATE_SQL_CONFIRMED': 0,
            'NEW': 0,
            'ERROR_EMBEDDING': 0
        }
        
        # Process each feedback
        print("\n" + "=" * 70)
        print("Processing feedback items...")
        print("=" * 70)
        
        for idx, row in enumerate(feedback_list, 1):
            question = row['user_question']
            sql = row['sql_generated']
            table_name = row.get('table_name', '')  # Get table name from feedback
            
            status, example_id = self.process_single_feedback(question, sql, table_name)
            stats[status] += 1
            
            if idx % 10 == 0 or idx == total_feedback:
                print(f"  Progress: {idx}/{total_feedback} processed...")
        
        print("\n" + "=" * 70)
        print("DEDUPLICATION RESULTS:")
        print("=" * 70)
        for status, count in stats.items():
            if count > 0:
                percentage = (count / total_feedback) * 100
                print(f"  {status:40s}: {count:4d} ({percentage:5.1f}%)")
        
        # Update BM25 statistics
        print("\n" + "=" * 70)
        self.update_bm25_statistics()
        
        # Sync vector search index
        print("\n" + "=" * 70)
        print("Syncing vector search index...")
        self.sync_vector_search_index()
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "=" * 70)
        print("BATCH JOB COMPLETE")
        print("=" * 70)
        print(f"Total processed:        {total_feedback}")
        print(f"New unique examples:    {stats['NEW']}")
        print(f"Duplicates found:       {total_feedback - stats['NEW'] - stats['ERROR_EMBEDDING']}")
        print(f"Errors:                 {stats['ERROR_EMBEDDING']}")
        print(f"Duration:               {duration:.2f} seconds")
        print(f"Embeddings generated:   {stats['NEW'] + stats['NEW_VARIANT']}")
        print("=" * 70)


# ========== DATABRICKS NOTEBOOK USAGE ==========

"""
USAGE IN DATABRICKS NOTEBOOK:

# Expected feedback_monitoring schema:
# - user_question: STRING (the question user asked)
# - sql_generated: STRING (the SQL that was generated)
# - table_name: STRING (the table(s) involved - can be single or multiple)
#
# Table name formats supported:
#   Single: "sales"
#   Comma-separated: "sales,customers"
#   Space-separated: "sales customers"
#   Mixed: "sales, orders, products"

# Cell 1: One-time setup
processor = FeedbackBatchProcessor(
    spark=spark,
    databricks_token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    workspace_url=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
)

# Create tables
processor.setup_tables()

# Setup vector search (run once)
processor.setup_vector_search_index()

# Cell 2: Run batch job (schedule this every 3 hours)
processor = FeedbackBatchProcessor(
    spark=spark,
    databricks_token=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
    workspace_url=dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
)

processor.run_batch_job()

# ========== TESTING ==========

# Test with various table name formats
spark.sql('''
    INSERT INTO feedback_monitoring VALUES 
    ('What is total sales?', 'SELECT SUM(amount) FROM sales', 'sales'),
    ('Show sales by customer', 'SELECT * FROM sales JOIN customers', 'sales,customers'),
    ('Get order details', 'SELECT * FROM orders o JOIN products p', 'orders, products'),
    ('Revenue report', 'SELECT * FROM revenue r JOIN sales s', 'revenue sales')
''')

# All will correctly parse multiple tables into tables_used array
"""
