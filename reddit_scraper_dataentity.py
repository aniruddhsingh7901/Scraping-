#!/usr/bin/env python3
"""
reddit_scraper_dataentity.py

Enhanced async Reddit scraper compatible with Data Universe format:
✅ Uses DataEntity format from Data Universe
✅ Stores in PostgreSQL using PostgreSQLMinerStorage
✅ Keeps original scraping logic intact
✅ Compatible with RedditContent model
✅ Robust 429 error handling with automatic retry
✅ Pre-emptive rate limit management
✅ Comprehensive error recovery
"""

import os
import asyncio
import aiohttp
import asyncpraw
from asyncprawcore import Requestor
from asyncprawcore.exceptions import ResponseException, RequestException
from datetime import datetime, timedelta, timezone
import random
import logging
from typing import Optional, Dict, Any, List
import time
import traceback
import json

# Import Data Universe components
import sys
sys.path.append('./data-universe')
from common.data import DataEntity, DataLabel, DataSource
from scraping.reddit.model import RedditContent, RedditDataType, DELETED_USER
from scraping.reddit.utils import normalize_permalink, extract_media_urls
from storage_postgresql import PostgreSQLMinerStorage
import bittensor as bt

# Configuration
RATE_LIMIT_STOP = 50
RATE_LIMIT_SLOW = 100
RETRY_429_WAIT = 60
MAX_RETRIES = 5
SAVE_IMMEDIATELY = True  # Save each post/comment immediately

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('reddit_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configure bittensor logging
bt.logging.set_info(True)


class RateLimitManager:
    """Centralized rate limit management with thread-safe operations"""
    
    def __init__(self):
        self.lock = asyncio.Lock()
        self.last_remaining: Optional[float] = None
        self.last_reset: Optional[float] = None
        self.last_check_time: float = time.time()
        self.consecutive_429s: int = 0
        
    async def update(self, remaining: float, reset: float):
        """Update rate limit information from API response headers"""
        async with self.lock:
            self.last_remaining = remaining
            self.last_reset = reset
            self.last_check_time = time.time()
            logger.info(f"[RATE UPDATE] remaining={remaining}, resets_in={reset}s")
    
    async def should_wait(self) -> tuple[bool, float]:
        """Check if we should wait and return (should_wait, wait_time)"""
        async with self.lock:
            if self.last_remaining is None:
                return False, 0
            
            if self.last_remaining <= RATE_LIMIT_STOP:
                wait_time = (self.last_reset or 60) + 5
                logger.warning(
                    f"[RATE LIMIT] Remaining={self.last_remaining} <= {RATE_LIMIT_STOP}. "
                    f"Pausing for {wait_time}s"
                )
                return True, wait_time
            
            return False, 0
    
    async def get_delay(self) -> float:
        """Get appropriate delay based on current rate limit status"""
        async with self.lock:
            if self.last_remaining is None:
                return random.uniform(2, 5)
            
            if self.last_remaining <= RATE_LIMIT_SLOW:
                return random.uniform(5, 8)
            
            return random.uniform(2, 5)
    
    async def handle_429(self):
        """Handle 429 error with exponential backoff"""
        async with self.lock:
            self.consecutive_429s += 1
            wait_time = min(RETRY_429_WAIT * (2 ** (self.consecutive_429s - 1)), 300)
            logger.error(
                f"[429 ERROR] Rate limit exceeded! Attempt #{self.consecutive_429s}. "
                f"Waiting {wait_time}s before retry"
            )
            return wait_time
    
    async def reset_429_counter(self):
        """Reset consecutive 429 counter on successful request"""
        async with self.lock:
            if self.consecutive_429s > 0:
                logger.info(f"[RECOVERY] Successfully recovered from {self.consecutive_429s} consecutive 429 errors")
                self.consecutive_429s = 0


class TrackingRequestor(Requestor):
    """Custom requestor that tracks rate limits and updates manager"""
    
    def __init__(self, rate_manager: RateLimitManager, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rate_manager = rate_manager
    
    async def request(self, *args, **kwargs):
        response = await super().request(*args, **kwargs)
        headers = response.headers
        
        if "x-ratelimit-remaining" in headers:
            remaining = float(headers.get("x-ratelimit-remaining", 0))
            reset = float(headers.get("x-ratelimit-reset", 0))
            await self.rate_manager.update(remaining, reset)
        
        return response


async def retry_with_backoff(func, *args, max_retries=MAX_RETRIES, **kwargs):
    """Retry function with exponential backoff for various errors"""
    last_exception = None
    
    for attempt in range(1, max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except ResponseException as e:
            last_exception = e
            if hasattr(e, 'response') and e.response.status == 429:
                # Handled separately by rate_manager
                raise
            logger.warning(f"[RETRY {attempt}/{max_retries}] ResponseException: {e}")
            if attempt < max_retries:
                wait = min(2 ** attempt, 60)
                await asyncio.sleep(wait)
        except (RequestException, aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_exception = e
            logger.warning(f"[RETRY {attempt}/{max_retries}] Network error: {type(e).__name__}: {e}")
            if attempt < max_retries:
                wait = min(2 ** attempt, 60)
                await asyncio.sleep(wait)
        except Exception as e:
            last_exception = e
            logger.error(f"[RETRY {attempt}/{max_retries}] Unexpected error: {type(e).__name__}: {e}")
            if attempt < max_retries:
                wait = min(2 ** attempt, 60)
                await asyncio.sleep(wait)
    
    logger.error(f"[RETRY FAILED] All {max_retries} attempts failed")
    raise last_exception


def parse_submission_to_reddit_content(submission) -> Optional[RedditContent]:
    """
    Parse asyncpraw submission to RedditContent
    Keeps original scraping logic, just formats to RedditContent
    """
    try:
        media_urls = extract_media_urls(submission)
        user = submission.author.name if submission.author else DELETED_USER
        
        content = RedditContent(
            id=submission.name,
            url="https://www.reddit.com" + normalize_permalink(submission.permalink),
            username=user,
            communityName=submission.subreddit_name_prefixed,
            body=submission.selftext,
            createdAt=datetime.utcfromtimestamp(submission.created_utc).replace(tzinfo=timezone.utc),
            dataType=RedditDataType.POST,
            title=submission.title,
            parentId=None,
            media=media_urls if media_urls else None,
            is_nsfw=submission.over_18,
            score=getattr(submission, 'score', None),
            upvote_ratio=getattr(submission, 'upvote_ratio', None),
            num_comments=getattr(submission, 'num_comments', None),
        )
        return content
    except Exception as e:
        logger.error(f"Failed to parse submission: {e}")
        return None


def parse_comment_to_reddit_content(comment, submission_nsfw=False) -> Optional[RedditContent]:
    """
    Parse asyncpraw comment to RedditContent
    Keeps original scraping logic, just formats to RedditContent
    """
    try:
        user = comment.author.name if comment.author else DELETED_USER
        
        content = RedditContent(
            id=comment.name,
            url="https://www.reddit.com" + normalize_permalink(comment.permalink),
            username=user,
            communityName=comment.subreddit_name_prefixed,
            body=comment.body,
            createdAt=datetime.utcfromtimestamp(comment.created_utc).replace(tzinfo=timezone.utc),
            dataType=RedditDataType.COMMENT,
            title=None,
            parentId=comment.parent_id,
            media=None,
            is_nsfw=submission_nsfw,  # Comments inherit NSFW from parent submission
            score=getattr(comment, 'score', None),
            upvote_ratio=None,
            num_comments=None,
        )
        return content
    except Exception as e:
        logger.trace(f"Failed to parse comment: {e}")
        return None


async def fetch_and_store_all_comments(
    submission,
    rate_manager: RateLimitManager,
    storage: PostgreSQLMinerStorage,
    subreddit_name: str,
    worker_id: str
) -> int:
    """
    Fetch ALL comments from a submission recursively and store them immediately
    Based on reddit_multi_scraper.py logic
    """
    comment_count = 0
    
    try:
        # Check rate limit before fetching
        should_wait, wait_time = await rate_manager.should_wait()
        if should_wait:
            await asyncio.sleep(wait_time)
        
        # Load submission and expand all comment trees
        await submission.load()
        await submission.comments.replace_more(limit=None)
        
        submission_nsfw = submission.over_18
        
        # Recursive function to walk comment tree
        def walk_comments(comment_list):
            nonlocal comment_count
            for comment in comment_list:
                try:
                    # Parse comment to RedditContent
                    comment_content = parse_comment_to_reddit_content(comment, submission_nsfw)
                    
                    if comment_content:
                        # Convert to DataEntity and save immediately
                        comment_entity = RedditContent.to_data_entity(content=comment_content)
                        storage.store_data_entities([comment_entity])
                        comment_count += 1
                        
                        # Log every 50 comments
                        if comment_count % 50 == 0:
                            logger.info(
                                f"[{worker_id}][r/{subreddit_name}] Saved {comment_count} comments from post {submission.name}"
                            )
                    
                    # Recursively process replies
                    if comment.replies:
                        walk_comments(comment.replies)
                        
                except Exception as e:
                    logger.trace(f"[{worker_id}] Failed to process comment: {e}")
                    continue
        
        # Start walking from top-level comments
        walk_comments(submission.comments)
        
    except Exception as e:
        logger.error(f"[{worker_id}] Failed to fetch comments for post {submission.name}: {e}")
    
    return comment_count


async def scrape_subreddit(
    reddit,
    rate_manager: RateLimitManager,
    storage: PostgreSQLMinerStorage,
    subreddit_name: str,
    days_back: int = 30,
    worker_id: str = "unknown"
):
    """
    Scrape subreddit and store as DataEntity objects
    Keeps original scraping logic intact
    """
    logger.info(f"[{worker_id}][SCRAPE START] r/{subreddit_name} - Historical scrape (last {days_back} days)")
    
    try:
        subreddit = await reddit.subreddit(subreddit_name)
    except Exception as e:
        logger.error(f"[SCRAPE ERROR] Failed to access r/{subreddit_name}: {e}")
        return
    
    cutoff = datetime.utcnow() - timedelta(days=days_back)
    count = 0
    consecutive_errors = 0
    max_consecutive_errors = 10
    
    try:
        async for post in subreddit.new(limit=None):
            # Check if we should pause due to rate limits
            should_wait, wait_time = await rate_manager.should_wait()
            if should_wait:
                await asyncio.sleep(wait_time)
            
            try:
                created = datetime.utcfromtimestamp(post.created_utc)
                if created < cutoff:
                    logger.info(f"[{worker_id}][SCRAPE DONE] r/{subreddit_name} reached {days_back}-day cutoff")
                    break
                
                # Parse to RedditContent
                reddit_content = parse_submission_to_reddit_content(post)
                
                if reddit_content:
                    # Skip NSFW content with media (Data Universe validation rule)
                    if reddit_content.is_nsfw and reddit_content.media:
                        logger.trace(f"Skipping NSFW content with media: {reddit_content.url}")
                        continue
                    
                    # Convert to DataEntity using Data Universe format
                    data_entity = RedditContent.to_data_entity(content=reddit_content)
                    count += 1
                    consecutive_errors = 0
                    
                    await rate_manager.reset_429_counter()
                    
                    # Save post immediately to PostgreSQL
                    storage.store_data_entities([data_entity])
                    
                    logger.info(
                        f"[{worker_id}][r/{subreddit_name}] POST #{count} SCRAPED & SAVED → DB | "
                        f"Title: {post.title[:50]}... | Score: {post.score} | ID: {post.name}"
                    )
                    
                    # Now fetch and store ALL comments from this post with retry
                    try:
                        comment_count = await retry_with_backoff(
                            fetch_and_store_all_comments,
                            post, rate_manager, storage, subreddit_name, worker_id
                        )
                        
                        if comment_count > 0:
                            logger.info(
                                f"[{worker_id}][r/{subreddit_name}] POST {post.name}: "
                                f"Scraped & Saved {comment_count} COMMENTS → DB"
                            )
                    except Exception as e:
                        logger.error(f"[{worker_id}] Failed to fetch comments after retries for {post.name}: {e}")
                
            except ResponseException as e:
                if hasattr(e, 'response') and e.response.status == 429:
                    wait_time = await rate_manager.handle_429()
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[{worker_id}][POST ERROR] r/{subreddit_name}: {e}")
                    consecutive_errors += 1
            
            except Exception as e:
                logger.error(f"[{worker_id}][POST ERROR] r/{subreddit_name}: {type(e).__name__}: {e}")
                consecutive_errors += 1
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error(f"[{worker_id}][SCRAPE ABORT] r/{subreddit_name} - Too many consecutive errors")
                break
            
            delay = await rate_manager.get_delay()
            await asyncio.sleep(delay)
    
    except Exception as e:
        logger.error(f"[{worker_id}][SCRAPE ERROR] r/{subreddit_name} loop failed: {traceback.format_exc()}")
    
    finally:
        logger.info(f"[{worker_id}][SCRAPE COMPLETE] r/{subreddit_name} - Collected & Saved {count} posts to PostgreSQL")


# COMMENTED OUT - STREAM SCRAPING DISABLED
# async def stream_subreddit(
#     reddit,
#     rate_manager: RateLimitManager,
#     storage: PostgreSQLMinerStorage,
#     subreddit_name: str,
#     worker_id: str = "unknown"
# ):
#     """
#     Stream new submissions and store as DataEntity objects
#     Keeps original streaming logic intact
#     """
#     logger.info(f"[{worker_id}][STREAM START] r/{subreddit_name} - Live streaming mode")
#     
#     try:
#         subreddit = await reddit.subreddit(subreddit_name)
#     except Exception as e:
#         logger.error(f"[{worker_id}][STREAM ERROR] Failed to access r/{subreddit_name}: {e}")
#         return
#     
#     consecutive_errors = 0
#     max_consecutive_errors = 20
#     
#     try:
#         async for post in subreddit.stream.submissions(skip_existing=True, pause_after=5):
#             if post is None:
#                 await asyncio.sleep(2)
#                 continue
#             
#             should_wait, wait_time = await rate_manager.should_wait()
#             if should_wait:
#                 await asyncio.sleep(wait_time)
#             
#             try:
#                 reddit_content = parse_submission_to_reddit_content(post)
#                 
#                 if reddit_content:
#                     # Skip NSFW content with media
#                     if reddit_content.is_nsfw and reddit_content.media:
#                         logger.trace(f"Skipping NSFW content with media: {reddit_content.url}")
#                         continue
#                     
#                     data_entity = RedditContent.to_data_entity(content=reddit_content)
#                     consecutive_errors = 0
#                     
#                     await rate_manager.reset_429_counter()
#                     
#                     # Save post immediately to PostgreSQL
#                     storage.store_data_entities([data_entity])
#                     
#                     logger.info(
#                         f"[{worker_id}][STREAM r/{subreddit_name}] NEW POST SCRAPED & SAVED → DB | "
#                         f"Title: {post.title[:50]}... | Score: {post.score} | ID: {post.name}"
#                     )
#                     
#                     # Now fetch and store ALL comments from this post with retry
#                     try:
#                         comment_count = await retry_with_backoff(
#                             fetch_and_store_all_comments,
#                             post, rate_manager, storage, subreddit_name, worker_id
#                         )
#                         
#                         if comment_count > 0:
#                             logger.info(
#                                 f"[{worker_id}][STREAM r/{subreddit_name}] POST {post.name}: "
#                                 f"Scraped & Saved {comment_count} COMMENTS → DB"
#                             )
#                     except Exception as e:
#                         logger.error(f"[{worker_id}] Failed to fetch comments after retries for {post.name}: {e}")
#             
#             except ResponseException as e:
#                 if hasattr(e, 'response') and e.response.status == 429:
#                     wait_time = await rate_manager.handle_429()
#                     await asyncio.sleep(wait_time)
#                     continue
#                 else:
#                     logger.error(f"[{worker_id}][STREAM ERROR] r/{subreddit_name}: {e}")
#                     consecutive_errors += 1
#             
#             except Exception as e:
#                 logger.error(f"[{worker_id}][STREAM ERROR] r/{subreddit_name}: {type(e).__name__}: {e}")
#                 consecutive_errors += 1
#             
#             if consecutive_errors >= max_consecutive_errors:
#                 logger.error(f"[{worker_id}][STREAM ABORT] r/{subreddit_name} - Too many errors")
#                 break
#             
#             delay = await rate_manager.get_delay()
#             await asyncio.sleep(delay)
#     
#     except Exception as e:
#         logger.error(f"[{worker_id}][STREAM FATAL] r/{subreddit_name}: {traceback.format_exc()}")
#     
#     finally:
#         logger.info(f"[{worker_id}][STREAM END] r/{subreddit_name}")


async def main():
    """Main entry point"""
    worker_id = os.getenv("WORKER_ID", "standalone")
    
    logger.info("=" * 80)
    logger.info(f"Reddit DataEntity Scraper Starting [{worker_id}]")
    logger.info("=" * 80)
    
    # Setup proxy (MANDATORY - always required for scraping)
    proxy_env = os.getenv("PROXY")
    
    if not proxy_env:
        logger.error("[PROXY] PROXY environment variable is REQUIRED but not set!")
        logger.error("[PROXY] Cannot proceed without proxy configuration")
        return
    
    try:
        host, port, user, pwd = proxy_env.split(":")
        proxy_url = f"http://{host}:{port}"
        proxy_auth = aiohttp.BasicAuth(user, pwd)
        logger.info(f"[PROXY] ✓ Using MANDATORY proxy: {proxy_url} with auth user={user}")
    except ValueError as e:
        logger.error(f"[PROXY] Invalid PROXY format: {proxy_env}")
        logger.error("[PROXY] Expected format: host:port:user:pass")
        logger.error("[PROXY] Cannot proceed without valid proxy configuration")
        return
    
    # Setup session
    timeout = aiohttp.ClientTimeout(total=120)
    conn = aiohttp.TCPConnector(ssl=False)
    session = aiohttp.ClientSession(timeout=timeout, connector=conn)
    
    if proxy_url:
        original_request = session._request
        async def proxy_request(method, url, **kwargs):
            kwargs["proxy"] = proxy_url
            kwargs["proxy_auth"] = proxy_auth
            return await original_request(method, url, **kwargs)
        session._request = proxy_request
    
    # Initialize storage
    storage = PostgreSQLMinerStorage(
        database=os.getenv("POSTGRES_DB", "reddit_miner_db"),
        user=os.getenv("POSTGRES_USER", "reddit_user"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        max_database_size_gb_hint=250
    )
    
    # Initialize rate limit manager
    rate_manager = RateLimitManager()
    
    # Setup Reddit client
    requestor = TrackingRequestor(rate_manager, session=session, user_agent="DataUniverseScraper/1.0")
    
    reddit = asyncpraw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        username=os.getenv("REDDIT_USERNAME"),
        password=os.getenv("REDDIT_PASSWORD"),
        user_agent="DataUniverseScraper/1.0",
        requestor_class=lambda *a, **kw: requestor,
    )
    
    # Get subreddits
    subreddits_env = os.getenv("SUBREDDITS", "")
    if subreddits_env:
        subreddits = [s.strip() for s in subreddits_env.split(",") if s.strip()]
        logger.info(f"[CONFIG] Using subreddits from ENV: {', '.join(subreddits)}")
    else:
        subreddits = ["python", "machinelearning", "cryptocurrency"]
        logger.info(f"[CONFIG] Using default subreddits: {', '.join(subreddits)}")
    
    reddit_username = os.getenv("REDDIT_USERNAME", "unknown")
    logger.info(f"[{worker_id}][CONFIG] Worker ID: {worker_id}")
    logger.info(f"[{worker_id}][CONFIG] Reddit Account: {reddit_username}")
    logger.info(f"[{worker_id}][CONFIG] PostgreSQL: {os.getenv('POSTGRES_USER')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")
    logger.info(f"[{worker_id}][CONFIG] Assigned Subreddits: {', '.join(subreddits)}")
    
    try:
        # Continuous historical scraping mode (24/7)
        logger.info(f"[{worker_id}][CONTINUOUS MODE] Starting continuous historical scraping (24/7)")
        logger.info(f"[{worker_id}][INFO] Stream scraping is DISABLED - only historical data will be collected")
        
        cycle_count = 0
        while True:
            cycle_count += 1
            logger.info(f"[{worker_id}][CYCLE #{cycle_count}] Starting historical scrape (last 30 days)")
            
            tasks = [
                asyncio.create_task(scrape_subreddit(reddit, rate_manager, storage, s, worker_id=worker_id))
                for s in subreddits
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"[{worker_id}][CYCLE #{cycle_count} ERROR] r/{subreddits[i]} failed: {result}")
            
            logger.info(f"[{worker_id}][CYCLE #{cycle_count}] Historical scrape complete")
            
            # Wait before next cycle to avoid hammering the API
            wait_time = 3600  # 1 hour between cycles
            logger.info(f"[{worker_id}][WAITING] Next cycle in {wait_time}s (1 hour)...")
            await asyncio.sleep(wait_time)
        
        # PHASE 2 COMMENTED OUT - NO STREAM SCRAPING
        # # Phase 2: Live streaming
        # logger.info(f"[{worker_id}][PHASE 2] Starting live streaming mode")
        # stream_tasks = [
        #     asyncio.create_task(stream_subreddit(reddit, rate_manager, storage, s, worker_id=worker_id))
        #     for s in subreddits
        # ]
        # await asyncio.gather(*stream_tasks, return_exceptions=True)
        
    except KeyboardInterrupt:
        logger.info(f"[{worker_id}][SHUTDOWN] Received interrupt signal, shutting down gracefully...")
    except Exception as e:
        logger.error(f"[{worker_id}][FATAL ERROR] {type(e).__name__}: {e}")
    finally:
        await reddit.close()
        await session.close()
        storage.close()
        logger.info(f"[{worker_id}][SHUTDOWN] Cleanup complete")


if __name__ == "__main__":
    asyncio.run(main())
