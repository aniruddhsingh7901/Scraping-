#!/usr/bin/env python3
"""
migrate_reddit_data.py

Transfer data from reddit.scraped_data table to reddit_miner_db.dataentity table
Converts old format to DataEntity format compatible with Data Universe
"""

import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
import json
import logging
import sys
from typing import List, Dict, Any

# Import Data Universe components
sys.path.append('./data-universe')
from common.data import DataEntity, DataLabel, DataSource
from scraping.reddit.model import RedditContent, RedditDataType
from storage_postgresql import PostgreSQLMinerStorage

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('migrate_reddit_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def connect_to_source_db(host="localhost", port="5432", user="postgres", password="password", database="reddit"):
    """Connect to source database (reddit.scraped_data)"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=10
        )
        logger.info(f"✓ Connected to source database: {database}@{host}:{port}")
        return conn
    except Exception as e:
        logger.error(f"✗ Failed to connect to source database: {e}")
        raise


def get_scraped_data_structure(conn):
    """Get the structure of scraped_data table"""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'scraped_data'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        logger.info(f"scraped_data table structure:")
        for col in columns:
            logger.info(f"  - {col[0]}: {col[1]}")
        return columns


def get_record_count(conn, table_name="scraped_data"):
    """Get total record count from source table"""
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.info(f"Total records in {table_name}: {count:,}")
        return count


def convert_to_reddit_content(row: Dict[str, Any]) -> RedditContent:
    """
    Convert a row from scraped_data to RedditContent format
    Following the pattern from reddit_custom_scraper.py
    """
    # Determine if it's a post or comment based on presence of title or parent_id
    has_title = row.get('title') is not None and row.get('title') != ''
    has_parent = row.get('parent_id') is not None and row.get('parent_id') != ''
    is_post = has_title or not has_parent
    
    # Extract body - ensure it's a string, never None
    body_value = row.get('body') or row.get('content') or row.get('text') or row.get('selftext')
    body = str(body_value) if body_value is not None else ""
    
    # Extract URL - ensure it's a string
    url_value = row.get('url') or row.get('permalink')
    url = str(url_value) if url_value is not None else ""
    
    # Extract username - ensure it's a string
    username_value = row.get('username') or row.get('author')
    username = str(username_value) if username_value is not None else "unknown"
    
    # Extract subreddit - ensure it's a string with 'r/' prefix
    subreddit_value = row.get('subreddit') or row.get('community_name')
    if subreddit_value:
        subreddit = str(subreddit_value)
        # Add 'r/' prefix if not already present
        if not subreddit.startswith('r/'):
            subreddit = f"r/{subreddit}"
    else:
        subreddit = "r/unknown"
    
    # Extract title - only for posts
    title = None
    if is_post and row.get('title'):
        title = str(row.get('title'))
    
    # Extract media - handle JSON strings
    media = None
    media_value = row.get('media')
    if media_value:
        if isinstance(media_value, str):
            try:
                media = json.loads(media_value)
            except:
                pass
        else:
            media = media_value
    
    # Extract datetime
    created_at = row.get('created_at') or row.get('created_utc') or row.get('datetime')
    if created_at is None:
        created_at = datetime.now(timezone.utc)
    
    # Extract mandatory metadata fields with defaults (required by validator)
    # is_nsfw: Default to False if NULL
    is_nsfw_value = row.get('is_nsfw') or row.get('nsfw')
    is_nsfw = bool(is_nsfw_value) if is_nsfw_value is not None else False
    
    # score: Default to 0 if NULL (validator requires non-None)
    score_value = row.get('score')
    score = int(score_value) if score_value is not None else 0
    
    # upvote_ratio: Default to 0.5 if NULL (validator requires non-None)
    upvote_ratio_value = row.get('upvote_ratio')
    upvote_ratio = float(upvote_ratio_value) if upvote_ratio_value is not None else 0.5
    
    # num_comments: Default to 0 if NULL (validator requires non-None)
    num_comments_value = row.get('num_comments') or row.get('comment_count')
    num_comments = int(num_comments_value) if num_comments_value is not None else 0
    
    return RedditContent(
        id=row.get('id') or row.get('reddit_id') or row.get('name') or "unknown",
        url=url,
        username=username,
        communityName=subreddit,
        body=body,
        createdAt=created_at,
        dataType=RedditDataType.POST if is_post else RedditDataType.COMMENT,
        title=title,
        parentId=row.get('parent_id') if not is_post else None,
        media=media,
        is_nsfw=is_nsfw,
        score=score,
        upvote_ratio=upvote_ratio,
        num_comments=num_comments,
    )


def migrate_batch(source_conn, storage: PostgreSQLMinerStorage, offset: int, batch_size: int) -> int:
    """Migrate a batch of records"""
    with source_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        # Fetch batch
        cursor.execute(f"""
            SELECT * FROM scraped_data
            ORDER BY created_at
            LIMIT %s OFFSET %s
        """, (batch_size, offset))
        
        rows = cursor.fetchall()
        
        if not rows:
            return 0
        
        # Convert to DataEntity objects
        data_entities = []
        skipped_nsfw_media = 0
        skipped_errors = 0
        
        for row in rows:
            try:
                # Convert to RedditContent
                reddit_content = convert_to_reddit_content(dict(row))
                
                # Skip NSFW content with media (Data Universe validation rule)
                if reddit_content.is_nsfw and reddit_content.media:
                    logger.debug(f"Skipping NSFW content with media: {reddit_content.id}")
                    skipped_nsfw_media += 1
                    continue
                
                # Validate mandatory fields are not None (validator requirement)
                if reddit_content.is_nsfw is None:
                    raise ValueError(f"is_nsfw is None for {reddit_content.id}")
                if reddit_content.score is None:
                    raise ValueError(f"score is None for {reddit_content.id}")
                if reddit_content.upvote_ratio is None:
                    raise ValueError(f"upvote_ratio is None for {reddit_content.id}")
                if reddit_content.num_comments is None:
                    raise ValueError(f"num_comments is None for {reddit_content.id}")
                
                # Convert to DataEntity
                data_entity = RedditContent.to_data_entity(content=reddit_content)
                data_entities.append(data_entity)
                
            except Exception as e:
                logger.warning(f"Failed to convert row: {e}")
                logger.debug(f"Problematic row: {dict(row)}")
                skipped_errors += 1
                continue
        
        # Store batch
        if data_entities:
            try:
                logger.info(f"Attempting to store {len(data_entities)} entities...")
                storage.store_data_entities(data_entities)
                logger.info(f"✓ STORED batch: {len(data_entities)} records (offset {offset}) | Skipped: {skipped_nsfw_media} NSFW+media, {skipped_errors} errors")
            except Exception as e:
                logger.error(f"✗ FAILED to store batch: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise
        else:
            logger.warning(f"No entities to store in this batch (offset {offset}) | Skipped: {skipped_nsfw_media} NSFW+media, {skipped_errors} errors")
        
        return len(rows)


def main():
    """Main migration process"""
    logger.info("=" * 80)
    logger.info("Reddit Data Migration: scraped_data → dataentity")
    logger.info("=" * 80)
    
    # Configuration - adjust these as needed
    # Using credentials from reddit_scraper_dataentity.py
    SOURCE_HOST = "localhost"
    SOURCE_PORT = "5432"
    SOURCE_USER = "postgres"  # Source database user
    SOURCE_PASSWORD = "postgres"  # Changed from 'password' to 'postgres'
    SOURCE_DATABASE = "reddit"
    
    TARGET_HOST = "localhost"
    TARGET_PORT = "5432"
    TARGET_USER = "reddit_user"  # From reddit_scraper_dataentity.py
    TARGET_PASSWORD = "postgres"  # From reddit_scraper_dataentity.py
    TARGET_DATABASE = "reddit_miner_db"
    
    BATCH_SIZE = 1000  # Process 10000 records at a time
    
    # Connect to source database
    logger.info("Step 1: Connecting to source database...")
    try:
        source_conn = connect_to_source_db(
            host=SOURCE_HOST,
            port=SOURCE_PORT,
            user=SOURCE_USER,
            password=SOURCE_PASSWORD,
            database=SOURCE_DATABASE
        )
    except Exception as e:
        logger.error(f"Cannot proceed without source database connection: {e}")
        return 1
    
    # Get table structure
    logger.info("\nStep 2: Analyzing source table structure...")
    try:
        get_scraped_data_structure(source_conn)
    except Exception as e:
        logger.error(f"Failed to get table structure: {e}")
        # Continue anyway - structure query might fail but data query might work
    
    # Get record count
    logger.info("\nStep 3: Counting source records...")
    try:
        total_records = get_record_count(source_conn)
    except Exception as e:
        logger.error(f"Failed to count records: {e}")
        source_conn.close()
        return 1
    
    if total_records == 0:
        logger.warning("No records found in scraped_data table. Nothing to migrate.")
        source_conn.close()
        return 0
    
    # Initialize target storage
    logger.info("\nStep 4: Connecting to target database...")
    try:
        storage = PostgreSQLMinerStorage(
            database=TARGET_DATABASE,
            user=TARGET_USER,
            password=TARGET_PASSWORD,
            host=TARGET_HOST,
            port=TARGET_PORT,
            max_database_size_gb_hint=250
        )
        logger.info(f"✓ Connected to target database: {TARGET_DATABASE}@{TARGET_HOST}:{TARGET_PORT}")
    except Exception as e:
        logger.error(f"Failed to initialize target storage: {e}")
        source_conn.close()
        return 1
    
    # Migrate data in batches
    logger.info(f"\nStep 5: Migrating data in batches of {BATCH_SIZE}...")
    logger.info("-" * 80)
    
    offset = 0
    total_migrated = 0
    
    try:
        while offset < total_records:
            batch_count = migrate_batch(source_conn, storage, offset, BATCH_SIZE)
            
            if batch_count == 0:
                break
            
            offset += batch_count
            total_migrated += batch_count
            
            progress = (offset / total_records) * 100
            logger.info(f"Progress: {offset:,}/{total_records:,} ({progress:.1f}%)")
            
            # Small delay to avoid overwhelming the database
            import time
            time.sleep(0.1)
    
    except KeyboardInterrupt:
        logger.warning("\nMigration interrupted by user!")
        logger.info(f"Migrated {total_migrated:,} records before interruption")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        source_conn.close()
        storage.close()
    
    # Summary
    logger.info("-" * 80)
    logger.info("Migration Summary:")
    logger.info(f"  Source records: {total_records:,}")
    logger.info(f"  Migrated: {total_migrated:,}")
    logger.info(f"  Success rate: {(total_migrated/total_records*100):.1f}%")
    logger.info("=" * 80)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
