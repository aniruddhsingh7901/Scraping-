#!/usr/bin/env python3
"""
Test your PostgreSQL data against validator duplicate detection logic.
This simulates what validators will check when they evaluate your miner.

COMPLETE VERSION with all advanced testing features integrated.
"""

import sys
sys.path.append('./data-universe')

import hashlib
import psycopg2
import random
from typing import List, Set, Tuple
from collections import defaultdict
from common.data import DataEntity, DataLabel, DataSource, TimeBucket
from scraping.reddit import utils as reddit_utils
import datetime as dt
import os

# Import your storage
from storage_postgresql import PostgreSQLMinerStorage


class LocalValidatorTest:
    """
    Mimics validator's duplicate detection logic.
    Tests your data the same way validators will check it.
    """
    
    def __init__(self, storage: PostgreSQLMinerStorage):
        self.storage = storage
        self.total_buckets_checked = 0
        self.passed_buckets = 0
        self.failed_buckets = 0
        self.issues = []
    
    def _normalize_uri(self, uri: str) -> str:
        """
        Exact copy of validator's URI normalization.
        From: data-universe/vali_utils/utils.py
        """
        # For Reddit, just pass through (no special normalization)
        # For X/Twitter, normalize twitter.com <-> x.com
        if 'twitter.com' in uri:
            return uri.replace('twitter.com', 'x.com')
        elif 'x.com' in uri:
            return uri  # Already normalized
        return uri
    
    def are_entities_unique(self, entities: List[DataEntity]) -> Tuple[bool, str]:
        """
        Exact copy of validator's uniqueness check.
        From: data-universe/vali_utils/utils.py lines 138-152
        """
        entity_content_hash_set: Set[str] = set()
        uris: Set[str] = set()
        
        for i, entity in enumerate(entities):
            # Hash the content (exact validator logic)
            entity_content_hash = hashlib.sha1(entity.content).hexdigest()
            normalized_uri = self._normalize_uri(entity.uri)
            
            # Check for duplicates
            if entity_content_hash in entity_content_hash_set:
                return False, f"Duplicate content hash at position {i}: {entity_content_hash[:16]}... (URI: {entity.uri})"
            
            if normalized_uri in uris:
                return False, f"Duplicate URI at position {i}: {normalized_uri}"
            
            entity_content_hash_set.add(entity_content_hash)
            uris.add(normalized_uri)
        
        return True, "All entities unique"
    
    def get_all_buckets(self) -> List[dict]:
        """Get list of all buckets in your database"""
        with self.storage.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        timeBucketId,
                        source,
                        label,
                        COUNT(*) as entity_count,
                        SUM(contentSizeBytes) as total_bytes
                    FROM DataEntity
                    GROUP BY timeBucketId, source, label
                    ORDER BY timeBucketId DESC, source, label
                """)
                
                buckets = []
                for row in cursor.fetchall():
                    buckets.append({
                        'time_bucket_id': row[0],
                        'source': row[1],
                        'label': row[2],
                        'entity_count': row[3],
                        'total_bytes': row[4]
                    })
                
                return buckets
    
    def get_bucket_entities(self, time_bucket_id: int, source: int, label: str) -> List[DataEntity]:
        """Fetch all entities from a specific bucket"""
        with self.storage.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT uri, datetime, source, label, content, contentSizeBytes
                    FROM DataEntity
                    WHERE timeBucketId = %s
                    AND source = %s
                    AND label IS NOT DISTINCT FROM %s
                    ORDER BY datetime DESC
                """, (time_bucket_id, source, label))
                
                entities = []
                for row in cursor.fetchall():
                    entity = DataEntity(
                        uri=row[0],
                        datetime=row[1],
                        source=DataSource(row[2]),
                        content=bytes(row[4]),
                        content_size_bytes=row[5],
                        label=DataLabel(value=row[3]) if row[3] else None
                    )
                    entities.append(entity)
                
                return entities
    
    def test_bucket(self, bucket_info: dict) -> Tuple[bool, str]:
        """Test a single bucket (what validator does)"""
        print(f"\n{'='*80}")
        print(f"Testing Bucket:")
        print(f"  Time Bucket: {bucket_info['time_bucket_id']} ({self._format_time_bucket(bucket_info['time_bucket_id'])})")
        print(f"  Source: {DataSource(bucket_info['source']).name}")
        print(f"  Label: {bucket_info['label']}")
        print(f"  Entity Count: {bucket_info['entity_count']}")
        print(f"  Total Bytes: {bucket_info['total_bytes']:,}")
        
        # Fetch entities
        entities = self.get_bucket_entities(
            bucket_info['time_bucket_id'],
            bucket_info['source'],
            bucket_info['label']
        )
        
        if not entities:
            return True, "Empty bucket (skipped)"
        
        # Run validator's uniqueness check
        is_unique, reason = self.are_entities_unique(entities)
        
        if is_unique:
            print(f"  ✅ PASS: {reason}")
            return True, reason
        else:
            print(f"  ❌ FAIL: {reason}")
            return False, reason
    
    def _format_time_bucket(self, bucket_id: int) -> str:
        """Convert time bucket ID to readable date"""
        try:
            # Time bucket 0 = Unix epoch, each bucket = 1 hour
            timestamp = bucket_id * 3600
            dt_obj = dt.datetime.utcfromtimestamp(timestamp)
            return dt_obj.strftime('%Y-%m-%d %H:%M UTC')
        except:
            return f"ID:{bucket_id}"
    
    def test_all_buckets(self):
        """Test all buckets in your database"""
        print("\n" + "="*80)
        print("LOCAL VALIDATOR TEST - Checking All Buckets")
        print("="*80)
        
        buckets = self.get_all_buckets()
        
        if not buckets:
            print("\n⚠️  No data found in database!")
            return
        
        print(f"\nFound {len(buckets)} buckets to test")
        
        for i, bucket in enumerate(buckets, 1):
            self.total_buckets_checked += 1
            
            passed, reason = self.test_bucket(bucket)
            
            if passed:
                self.passed_buckets += 1
            else:
                self.failed_buckets += 1
                self.issues.append({
                    'bucket': bucket,
                    'reason': reason
                })
        
        self._print_summary()
    
    def test_random_sample(self, sample_size: int = 10):
        """Test a random sample of buckets (faster for large databases)"""
        print("\n" + "="*80)
        print(f"LOCAL VALIDATOR TEST - Random Sample ({sample_size} buckets)")
        print("="*80)
        
        buckets = self.get_all_buckets()
        
        if not buckets:
            print("\n⚠️  No data found in database!")
            return
        
        print(f"\nFound {len(buckets)} total buckets")
        
        # Sample random buckets
        sample = random.sample(buckets, min(sample_size, len(buckets)))
        print(f"Testing random sample of {len(sample)} buckets")
        
        for bucket in sample:
            self.total_buckets_checked += 1
            
            passed, reason = self.test_bucket(bucket)
            
            if passed:
                self.passed_buckets += 1
            else:
                self.failed_buckets += 1
                self.issues.append({
                    'bucket': bucket,
                    'reason': reason
                })
        
        self._print_summary()
    
    def test_specific_bucket(self, time_bucket_id: int, source: str, label: str):
        """
        Test one specific bucket by its parameters.
        
        Args:
            time_bucket_id: The time bucket ID (e.g., 478920)
            source: Source name as string ('REDDIT', 'X', 'YOUTUBE')
            label: Label/subreddit/hashtag (e.g., 'python')
        """
        print("\n" + "="*80)
        print(f"LOCAL VALIDATOR TEST - Testing Specific Bucket")
        print("="*80)
        
        try:
            source_enum = DataSource[source.upper()]
        except KeyError:
            print(f"❌ Invalid source: {source}. Must be REDDIT, X, or YOUTUBE")
            return
        
        # Get bucket info
        with self.storage.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        timeBucketId,
                        source,
                        label,
                        COUNT(*) as entity_count,
                        SUM(contentSizeBytes) as total_bytes
                    FROM DataEntity
                    WHERE timeBucketId = %s
                    AND source = %s
                    AND label IS NOT DISTINCT FROM %s
                    GROUP BY timeBucketId, source, label
                """, (time_bucket_id, source_enum, label))
                
                row = cursor.fetchone()
                
                if not row:
                    print(f"⚠️  No data found for bucket:")
                    print(f"   Time Bucket: {time_bucket_id}")
                    print(f"   Source: {source}")
                    print(f"   Label: {label}")
                    return
                
                bucket_info = {
                    'time_bucket_id': row[0],
                    'source': row[1],
                    'label': row[2],
                    'entity_count': row[3],
                    'total_bytes': row[4]
                }
        
        # Test this bucket
        self.total_buckets_checked += 1
        passed, reason = self.test_bucket(bucket_info)
        
        if passed:
            self.passed_buckets += 1
        else:
            self.failed_buckets += 1
            self.issues.append({
                'bucket': bucket_info,
                'reason': reason
            })
        
        self._print_summary()
    
    def find_buckets_with_most_duplicates(self):
        """
        Find buckets that likely have duplicates.
        Shows top 10 worst offenders.
        """
        print("\n" + "="*80)
        print("FINDING BUCKETS WITH MOST DUPLICATES")
        print("="*80)
        
        with self.storage.get_connection() as conn:
            with conn.cursor() as cursor:
                # Find buckets where same content hash appears multiple times
                # Using MD5 (built-in) instead of SHA1 (requires pgcrypto extension)
                cursor.execute("""
                    WITH ContentHashes AS (
                        SELECT 
                            timeBucketId,
                            source,
                            label,
                            MD5(content) as content_hash,
                            COUNT(*) as hash_count
                        FROM DataEntity
                        GROUP BY timeBucketId, source, label, MD5(content)
                        HAVING COUNT(*) > 1
                    )
                    SELECT 
                        timeBucketId,
                        source,
                        label,
                        COUNT(*) as duplicate_hash_count,
                        SUM(hash_count) as total_duplicate_entries
                    FROM ContentHashes
                    GROUP BY timeBucketId, source, label
                    ORDER BY duplicate_hash_count DESC
                    LIMIT 10
                """)
                
                results = cursor.fetchall()
                
                if results:
                    print("\n🚨 Top 10 Buckets with Duplicate Content Hashes:")
                    print(f"{'Rank':<6}{'Time Bucket':<12}{'Source':<10}{'Label':<20}{'Dup Hashes':<12}{'Dup Entries'}")
                    print("-" * 80)
                    
                    for i, row in enumerate(results, 1):
                        time_bucket = row[0]
                        source = DataSource(row[1]).name
                        label = row[2] or "None"
                        dup_hashes = row[3]
                        dup_entries = row[4]
                        
                        print(f"{i:<6}{time_bucket:<12}{source:<10}{label:<20}{dup_hashes:<12}{dup_entries}")
                    
                    print("\n💡 TIP: Test these specific buckets with:")
                    print("   tester.test_specific_bucket(time_bucket_id, 'SOURCE', 'label')")
                else:
                    print("\n✅ No buckets found with duplicate content hashes!")
                    print("Your database looks clean!")
    
    def _print_summary(self):
        """Print test results summary"""
        print("\n" + "="*80)
        print("TEST RESULTS SUMMARY")
        print("="*80)
        
        print(f"\nTotal Buckets Checked: {self.total_buckets_checked}")
        print(f"✅ Passed: {self.passed_buckets} ({self.passed_buckets/self.total_buckets_checked*100:.1f}%)")
        print(f"❌ Failed: {self.failed_buckets} ({self.failed_buckets/self.total_buckets_checked*100:.1f}%)")
        
        if self.failed_buckets > 0:
            print("\n" + "="*80)
            print("❌ FAILED BUCKETS - THESE WOULD FAIL VALIDATOR CHECKS!")
            print("="*80)
            
            for i, issue in enumerate(self.issues, 1):
                bucket = issue['bucket']
                print(f"\n{i}. Bucket:")
                print(f"   Time: {self._format_time_bucket(bucket['time_bucket_id'])}")
                print(f"   Source: {DataSource(bucket['source']).name}")
                print(f"   Label: {bucket['label']}")
                print(f"   Entities: {bucket['entity_count']}")
                print(f"   Reason: {issue['reason']}")
        
        print("\n" + "="*80)
        if self.failed_buckets == 0:
            print("🎉 SUCCESS! All checked buckets would pass validator checks!")
            print("="*80)
        else:
            print("🚨 FAILURE! Some buckets have duplicates and would fail validation!")
            print("You need to fix your storage before submitting to validators.")
            print("="*80)
    
    def find_duplicate_uris_across_buckets(self):
        """Find URIs that appear in multiple buckets (this is OK!)"""
        print("\n" + "="*80)
        print("CROSS-BUCKET ANALYSIS - Finding URIs in Multiple Buckets")
        print("="*80)
        
        with self.storage.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        uri,
                        COUNT(DISTINCT timeBucketId) as bucket_count,
                        COUNT(*) as total_count,
                        ARRAY_AGG(DISTINCT timeBucketId ORDER BY timeBucketId) as buckets
                    FROM DataEntity
                    GROUP BY uri
                    HAVING COUNT(DISTINCT timeBucketId) > 1
                    ORDER BY bucket_count DESC
                    LIMIT 20
                """)
                
                results = cursor.fetchall()
                
                if not results:
                    print("\n✅ No URIs found in multiple buckets (all unique per bucket)")
                else:
                    print(f"\nFound {len(results)} URIs appearing in multiple buckets:")
                    print("(This is OK - same URI in different time buckets is legitimate!)")
                    
                    for uri, bucket_count, total_count, buckets in results:
                        print(f"\nURI: {uri[:80]}...")
                        print(f"  Appears in {bucket_count} different buckets")
                        print(f"  Total entries: {total_count}")
                        print(f"  Buckets: {buckets[:5]}...")  # Show first 5


def main():
    """Run the local validator test with all features"""
    
    # Initialize your storage
    storage = PostgreSQLMinerStorage(
        database=os.getenv("POSTGRES_DB", "reddit_miner_db"),
        user=os.getenv("POSTGRES_USER", "reddit_user"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    
    # Create tester
    tester = LocalValidatorTest(storage)
    
    print("\n" + "="*80)
    print("🔍 LOCAL VALIDATOR TEST SUITE")
    print("="*80)
    print("\nThis script tests your PostgreSQL data against validator logic.")
    print("Choose a test mode:\n")
    print("1. Test ALL buckets (thorough, slower)")
    print("2. Test RANDOM SAMPLE (quick check)")
    print("3. Test SPECIFIC bucket (target one bucket)")
    print("4. Find WORST OFFENDERS (buckets with most duplicates)")
    print("5. Cross-bucket analysis (URIs in multiple buckets)")
    print("6. RUN ALL TESTS (comprehensive)")
    print("="*80)
    
    choice = input("\nEnter choice (1-6) or press Enter for option 2: ").strip()
    
    if not choice:
        choice = "2"
    
    if choice == "1":
        print("\n📊 Running comprehensive test on ALL buckets...")
        tester.test_all_buckets()
    
    elif choice == "2":
        sample_size = input("Sample size (default 20): ").strip()
        sample_size = int(sample_size) if sample_size else 20
        tester.test_random_sample(sample_size=sample_size)
    
    elif choice == "3":
        print("\n📊 Test specific bucket:")
        time_bucket = input("Time Bucket ID (e.g., 478920): ").strip()
        source = input("Source (REDDIT/X/YOUTUBE): ").strip().upper()
        label = input("Label (e.g., python): ").strip()
        
        if time_bucket and source:
            tester.test_specific_bucket(
                time_bucket_id=int(time_bucket),
                source=source,
                label=label if label else None
            )
        else:
            print("❌ Invalid input")
    
    elif choice == "4":
        tester.find_buckets_with_most_duplicates()
    
    elif choice == "5":
        tester.find_duplicate_uris_across_buckets()
    
    elif choice == "6":
        print("\n📊 Running ALL TESTS...\n")
        
        # Test 1: Find worst offenders first
        tester.find_buckets_with_most_duplicates()
        
        # Test 2: Random sample
        print("\n" + "="*80)
        input("Press Enter to continue with random sample test...")
        tester.test_random_sample(sample_size=20)
        
        # Test 3: Cross-bucket analysis
        print("\n" + "="*80)
        input("Press Enter to continue with cross-bucket analysis...")
        tester.find_duplicate_uris_across_buckets()
    
    else:
        print("❌ Invalid choice")
    
    storage.close()


if __name__ == "__main__":
    main()
