"""
PostgreSQL Storage Adapter for Data Universe
Compatible with the MinerStorage interface but uses PostgreSQL instead of SQLite
"""

import psycopg2
import psycopg2.extras
from psycopg2 import pool
import threading
import datetime as dt
import bittensor as bt
import logging
from typing import Dict, List
from collections import defaultdict
from contextlib import contextmanager

# Setup logger
logger = logging.getLogger(__name__)

# Import Data Universe models
import sys
sys.path.append('./data-universe')
from common.data import (
    CompressedEntityBucket,
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucketId,
    DataLabel,
    DataSource,
    TimeBucket,
)
from common import constants, utils


class PostgreSQLMinerStorage:
    """PostgreSQL backed MinerStorage compatible with Data Universe"""

    def __init__(
        self,
        database="reddit_miner_db",
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
        max_database_size_gb_hint=250,
        min_connections=2,
        max_connections=10,
    ):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database_max_content_size_bytes = utils.gb_to_bytes(max_database_size_gb_hint)

        # Create connection pool
        self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
            min_connections,
            max_connections,
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

        # Initialize database
        self._initialize_database()

        # Locks for thread safety
        self.clearing_space_lock = threading.Lock()
        self.cached_index_refresh_lock = threading.Lock()
        self.cached_index_lock = threading.Lock()
        self.cached_index_4 = None
        self.cached_index_updated = dt.datetime.min

        bt.logging.success(f"PostgreSQL storage initialized: {self.database}@{self.host}")

    @contextmanager
    def get_connection(self):
        """Get connection from pool"""
        conn = self.connection_pool.getconn()
        try:
            yield conn
        finally:
            self.connection_pool.putconn(conn)

    def _initialize_database(self):
        """Create tables and indexes if they don't exist"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Create DataEntity table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS DataEntity (
                        uri TEXT PRIMARY KEY,
                        datetime TIMESTAMP WITH TIME ZONE NOT NULL,
                        timeBucketId INTEGER NOT NULL,
                        source INTEGER NOT NULL,
                        label VARCHAR(140),
                        content BYTEA NOT NULL,
                        contentSizeBytes INTEGER NOT NULL
                    )
                """)

                # Create indexes
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS data_entity_bucket_index
                    ON DataEntity (timeBucketId, source, label, contentSizeBytes)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS data_entity_datetime_index
                    ON DataEntity (datetime)
                """)

                conn.commit()

    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary"""
        if not data_entities:
            return

        added_content_size = sum(entity.content_size_bytes for entity in data_entities)

        if added_content_size > self.database_max_content_size_bytes:
            raise ValueError(
                f"Content size to store: {added_content_size} exceeds configured max: {self.database_max_content_size_bytes}"
            )

        with self.get_connection() as conn:
            with self.clearing_space_lock:
                # Check current size
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COALESCE(SUM(contentSizeBytes), 0) FROM DataEntity")
                    current_content_size = cursor.fetchone()[0]

                    if current_content_size + added_content_size > self.database_max_content_size_bytes:
                        content_bytes_to_clear = max(
                            self.database_max_content_size_bytes // 10,
                            added_content_size
                        )
                        self.clear_content_from_oldest(content_bytes_to_clear)

            # Prepare values for batch insert
            values = []
            for data_entity in data_entities:
                label = None if data_entity.label is None else data_entity.label.value
                time_bucket_id = TimeBucket.from_datetime(data_entity.datetime).id
                values.append((
                    data_entity.uri,
                    data_entity.datetime,
                    time_bucket_id,
                    data_entity.source,  # Already an integer, don't call .value
                    label,
                    psycopg2.Binary(data_entity.content),
                    data_entity.content_size_bytes,
                ))

            # Batch insert with ON CONFLICT UPDATE - track inserts vs updates
            with conn.cursor() as cursor:
                # Get count before insert
                cursor.execute("SELECT COUNT(*) FROM DataEntity")
                count_before = cursor.fetchone()[0]
                
                psycopg2.extras.execute_batch(
                    cursor,
                    """
                    INSERT INTO DataEntity (uri, datetime, timeBucketId, source, label, content, contentSizeBytes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (uri) DO UPDATE SET
                        datetime = EXCLUDED.datetime,
                        timeBucketId = EXCLUDED.timeBucketId,
                        source = EXCLUDED.source,
                        label = EXCLUDED.label,
                        content = EXCLUDED.content,
                        contentSizeBytes = EXCLUDED.contentSizeBytes
                    """,
                    values
                )
                
                # Get count after insert
                cursor.execute("SELECT COUNT(*) FROM DataEntity")
                count_after = cursor.fetchone()[0]
                
                conn.commit()
                
                # Calculate inserts vs updates
                new_inserts = count_after - count_before
                duplicates_updated = len(data_entities) - new_inserts

            # Use both loggers to ensure visibility
            log_msg = f"Stored {len(data_entities)} data entities to PostgreSQL | NEW: {new_inserts}, DUPLICATES: {duplicates_updated}"
            logger.info(log_msg)
            try:
                bt.logging.info(log_msg)
            except:
                pass  # bt.logging might not be initialized

    def list_data_entities_in_data_entity_bucket(
        self, data_entity_bucket_id: DataEntityBucketId
    ) -> List[DataEntity]:
        """Lists from storage all DataEntities matching the provided DataEntityBucketId"""
        label = None if data_entity_bucket_id.label is None else data_entity_bucket_id.label.value

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM DataEntity 
                    WHERE timeBucketId = %s AND source = %s AND label IS NOT DISTINCT FROM %s
                    ORDER BY datetime DESC
                    """,
                    (data_entity_bucket_id.time_bucket.id, data_entity_bucket_id.source, label)
                )

                data_entities = []
                running_size = 0

                for row in cursor:
                    if running_size >= constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES:
                        return data_entities

                    data_entity = DataEntity(
                        uri=row["uri"],
                        datetime=row["datetime"],
                        source=DataSource(row["source"]),
                        content=bytes(row["content"]),
                        content_size_bytes=row["contentsizebytes"],
                        label=DataLabel(value=row["label"]) if row["label"] else None
                    )

                    data_entities.append(data_entity)
                    running_size += row["contentsizebytes"]

                return data_entities

    def refresh_compressed_index(self, time_delta: dt.timedelta):
        """Refreshes the compressed MinerIndex"""
        with self.cached_index_lock:
            if dt.datetime.now() - self.cached_index_updated <= time_delta:
                bt.logging.trace(f"Skipping updating cached index. Already fresher than {time_delta}")
                return
            else:
                bt.logging.info(f"Cached index out of {time_delta} freshness period. Refreshing.")

        with self.cached_index_refresh_lock:
            with self.cached_index_lock:
                if dt.datetime.now() - self.cached_index_updated <= time_delta:
                    bt.logging.trace("After waiting on refresh lock, index was already refreshed")
                    return

            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    oldest_time_bucket_id = TimeBucket.from_datetime(
                        dt.datetime.now() - dt.timedelta(days=constants.DATA_ENTITY_BUCKET_AGE_LIMIT_DAYS)
                    ).id

                    cursor.execute(
                        """
                        SELECT 
                            SUM(contentSizeBytes) AS bucketSize,
                            timeBucketId,
                            source,
                            label
                        FROM DataEntity
                        WHERE timeBucketId >= %s
                        GROUP BY timeBucketId, source, label
                        ORDER BY bucketSize DESC
                        LIMIT %s
                        """,
                        (oldest_time_bucket_id, constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4)
                    )

                    buckets_by_source_by_label = defaultdict(dict)

                    for row in cursor:
                        size = min(row[0], constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES)
                        label = row[3]
                        source = DataSource(row[2])

                        bucket = buckets_by_source_by_label[source].get(
                            label, CompressedEntityBucket(label=label)
                        )
                        bucket.sizes_bytes.append(size)
                        bucket.time_bucket_ids.append(row[1])
                        buckets_by_source_by_label[source][label] = bucket

                    with self.cached_index_lock:
                        self.cached_index_4 = CompressedMinerIndex(
                            sources={
                                source: list(labels_to_buckets.values())
                                for source, labels_to_buckets in buckets_by_source_by_label.items()
                            }
                        )
                        self.cached_index_updated = dt.datetime.now()
                        bt.logging.success(
                            f"Created cached index of {CompressedMinerIndex.size_bytes(self.cached_index_4)} bytes "
                            f"across {CompressedMinerIndex.bucket_count(self.cached_index_4)} buckets"
                        )

    def get_compressed_index(
        self,
        bucket_count_limit=constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4,
    ) -> CompressedMinerIndex:
        """Gets the compressed MinerIndex"""
        self.refresh_compressed_index(
            time_delta=(constants.MINER_CACHE_FRESHNESS + dt.timedelta(minutes=10))
        )

        with self.cached_index_lock:
            return self.cached_index_4

    def list_contents_in_data_entity_buckets(
        self, data_entity_bucket_ids: List[DataEntityBucketId]
    ) -> Dict[DataEntityBucketId, List[bytes]]:
        """Lists contents for each requested DataEntityBucketId"""
        if len(data_entity_bucket_ids) == 0 or len(data_entity_bucket_ids) > constants.BULK_BUCKETS_COUNT_LIMIT:
            return defaultdict(list)

        # Build query conditions
        conditions = []
        params = []
        for bucket_id in data_entity_bucket_ids:
            label = None if bucket_id.label is None else bucket_id.label.value
            conditions.append("(timeBucketId = %s AND label IS NOT DISTINCT FROM %s)")
            params.extend([bucket_id.time_bucket.id, label])

        query = f"""
            SELECT timeBucketId, source, label, content, contentSizeBytes
            FROM DataEntity
            WHERE {' OR '.join(conditions)}
            LIMIT %s
        """
        params.append(constants.BULK_CONTENTS_COUNT_LIMIT)

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)

                buckets_ids_to_contents = defaultdict(list)
                running_size = 0

                for row in cursor:
                    if running_size < constants.BULK_CONTENTS_SIZE_LIMIT_BYTES:
                        data_entity_bucket_id = DataEntityBucketId(
                            time_bucket=TimeBucket(id=row[0]),
                            source=DataSource(row[1]),
                            label=DataLabel(value=row[2]) if row[2] else None
                        )
                        buckets_ids_to_contents[data_entity_bucket_id].append(bytes(row[3]))
                        running_size += row[4]
                    else:
                        break

                return buckets_ids_to_contents

    def clear_content_from_oldest(self, content_bytes_to_clear: int):
        """Deletes entries starting from the oldest until cleared the specified amount"""
        bt.logging.debug(f"Database full. Clearing {content_bytes_to_clear} bytes")

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Find cutoff datetime
                cursor.execute("""
                    SELECT datetime, contentSizeBytes
                    FROM DataEntity
                    ORDER BY datetime ASC
                """)

                running_bytes = 0
                earliest_datetime_to_clear = None

                for row in cursor:
                    running_bytes += row[1]
                    earliest_datetime_to_clear = row[0]
                    if running_bytes >= content_bytes_to_clear:
                        break

                if earliest_datetime_to_clear:
                    cursor.execute(
                        "DELETE FROM DataEntity WHERE datetime <= %s",
                        (earliest_datetime_to_clear,)
                    )
                    conn.commit()
                    bt.logging.info(f"Cleared old data up to {earliest_datetime_to_clear}")

    def close(self):
        """Close all connections in the pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            bt.logging.info("PostgreSQL connection pool closed")
