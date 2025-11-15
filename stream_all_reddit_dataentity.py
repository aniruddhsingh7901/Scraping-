#!/usr/bin/env python3
"""
stream_all_reddit_dataentity.py - Global Reddit Streaming with DataEntity

Streams EVERYTHING from Reddit (r/all) in real-time:
✅ ALL new posts from r/all
✅ ALL new comments from r/all
✅ Compatible with Worker Manager account allocation
✅ Uses DataEntity format from Data Universe
✅ Stores in PostgreSQL using PostgreSQLMinerStorage
✅ Robust rate limit handling
✅ 24/7 continuous operation
"""

import os
import asyncio
import aiohttp
import asyncpraw
from asyncprawcore import Requestor
from asyncprawcore.exceptions import ResponseException, RequestException
from datetime import datetime, timezone
import random
import logging
from typing import Optional, List
import time
import traceback

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
SAVE_IMMEDIATELY = True

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('stream_all_reddit.log'),
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
        self.requests_made: int = 0
        
    async def update(self, remaining: float, reset: float):
        """Update rate limit information from API response headers"""
        async with self.lock:
            self.last_remaining = remaining
            self.last_reset = reset
            self.last_check_time = time.time()
            self.requests_made += 1
            
            if self.requests_made % 500 == 0:
                logger.info(
                    f"[RATE STATS] Requests: {self.requests_made}, "
                    f"Remaining: {remaining}, Resets in: {reset}s"
                )
    
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
                return random.uniform(0.5, 1.5)
            
            if self.last_remaining <= RATE_LIMIT_SLOW:
                return random.uniform(3, 5)
            elif self.last_remaining <= 200:
                return random.uniform(1, 2)
            
            return random.uniform(0.5, 1.5)
    
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


def parse_submission_to_reddit_content(submission) -> Optional[RedditContent]:
    """Parse asyncpraw submission to RedditContent"""
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


def parse_comment_to_reddit_content(comment) -> Optional[RedditContent]:
    """Parse asyncpraw comment to RedditContent"""
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
            is_nsfw=False,  # Comments don't directly have NSFW status
            score=getattr(comment, 'score', None),
            upvote_ratio=None,
            num_comments=None,
        )
        return content
    except Exception as e:
        logger.trace(f"Failed to parse comment: {e}")
        return None


async def stream_all_posts(
    reddit,
    rate_manager: RateLimitManager,
    storage: PostgreSQLMinerStorage,
    worker_id: str = "unknown"
):
    """Stream ALL new posts from r/all"""
    logger.info(f"[{worker_id}][STREAM POSTS] Starting global post stream from r/all")
    
    try:
        all_subreddit = await reddit.subreddit("all")
    except Exception as e:
        logger.error(f"[{worker_id}][ERROR] Cannot access r/all: {e}")
        return
    
    consecutive_errors = 0
    max_consecutive_errors = 20
    post_count = 0
    
    while True:  # Infinite loop for 24/7 streaming
        try:
            async for post in all_subreddit.stream.submissions(skip_existing=True, pause_after=5):
                if post is None:
                    await asyncio.sleep(1)
                    continue
                
                # Check rate limits
                should_wait, wait_time = await rate_manager.should_wait()
                if should_wait:
                    await asyncio.sleep(wait_time)
                
                try:
                    # Parse to RedditContent
                    reddit_content = parse_submission_to_reddit_content(post)
                    
                    if reddit_content:
                        # Skip NSFW content with media (Data Universe validation rule)
                        if reddit_content.is_nsfw and reddit_content.media:
                            logger.trace(f"Skipping NSFW content with media: {reddit_content.url}")
                            continue
                        
                        # Convert to DataEntity using Data Universe format
                        data_entity = RedditContent.to_data_entity(content=reddit_content)
                        consecutive_errors = 0
                        post_count += 1
                        
                        await rate_manager.reset_429_counter()
                        
                        # Save post immediately to PostgreSQL
                        storage.store_data_entities([data_entity])
                        
                        logger.info(
                            f"[{worker_id}][POST #{post_count}] r/{post.subreddit} SAVED → DB | "
                            f"Title: {post.title[:50]}... | Score: {post.score}"
                        )
                
                except ResponseException as e:
                    if hasattr(e, 'response') and e.response.status == 429:
                        wait_time = await rate_manager.handle_429()
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"[{worker_id}][POST ERROR] {e}")
                        consecutive_errors += 1
                
                except Exception as e:
                    logger.error(f"[{worker_id}][POST ERROR] {type(e).__name__}: {e}")
                    consecutive_errors += 1
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"[{worker_id}][STREAM RESTART] Posts - Too many errors, restarting...")
                    await asyncio.sleep(60)
                    consecutive_errors = 0
                    break
                
                delay = await rate_manager.get_delay()
                await asyncio.sleep(delay)
        
        except Exception as e:
            logger.error(f"[{worker_id}][STREAM FATAL] Posts: {traceback.format_exc()}")
            await asyncio.sleep(60)


async def stream_all_comments(
    reddit,
    rate_manager: RateLimitManager,
    storage: PostgreSQLMinerStorage,
    worker_id: str = "unknown"
):
    """Stream ALL new comments from r/all"""
    logger.info(f"[{worker_id}][STREAM COMMENTS] Starting global comment stream from r/all")
    
    try:
        all_subreddit = await reddit.subreddit("all")
    except Exception as e:
        logger.error(f"[{worker_id}][ERROR] Cannot access r/all: {e}")
        return
    
    consecutive_errors = 0
    max_consecutive_errors = 20
    comment_count = 0
    
    while True:  # Infinite loop for 24/7 streaming
        try:
            async for comment in all_subreddit.stream.comments(skip_existing=True, pause_after=5):
                if comment is None:
                    await asyncio.sleep(1)
                    continue
                
                # Check rate limits
                should_wait, wait_time = await rate_manager.should_wait()
                if should_wait:
                    await asyncio.sleep(wait_time)
                
                try:
                    # Parse to RedditContent
                    comment_content = parse_comment_to_reddit_content(comment)
                    
                    if comment_content:
                        # Convert to DataEntity
                        comment_entity = RedditContent.to_data_entity(content=comment_content)
                        consecutive_errors = 0
                        comment_count += 1
                        
                        await rate_manager.reset_429_counter()
                        
                        # Save comment immediately to PostgreSQL
                        storage.store_data_entities([comment_entity])
                        
                        if comment_count % 100 == 0:
                            logger.info(
                                f"[{worker_id}][COMMENT MILESTONE] Total: {comment_count} comments saved to DB"
                            )
                
                except ResponseException as e:
                    if hasattr(e, 'response') and e.response.status == 429:
                        wait_time = await rate_manager.handle_429()
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"[{worker_id}][COMMENT ERROR] {e}")
                        consecutive_errors += 1
                
                except Exception as e:
                    logger.error(f"[{worker_id}][COMMENT ERROR] {type(e).__name__}: {e}")
                    consecutive_errors += 1
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"[{worker_id}][STREAM RESTART] Comments - Too many errors, restarting...")
                    await asyncio.sleep(60)
                    consecutive_errors = 0
                    break
                
                delay = await rate_manager.get_delay()
                await asyncio.sleep(delay)
        
        except Exception as e:
            logger.error(f"[{worker_id}][STREAM FATAL] Comments: {traceback.format_exc()}")
            await asyncio.sleep(60)


async def main():
    """Main entry point - streams everything from r/all"""
    worker_id = os.getenv("WORKER_ID", "stream_all")
    
    logger.info("=" * 80)
    logger.info(f"Global Reddit Streaming Bot Starting [{worker_id}]")
    logger.info("=" * 80)
    logger.info("[MODE] Streaming from r/all (ALL posts + ALL comments)")
    logger.info("[FORMAT] DataEntity format → PostgreSQL")
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
    conn = aiohttp.TCPConnector(ssl=False, limit=100)
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
    requestor = TrackingRequestor(rate_manager, session=session, user_agent="GlobalRedditStream/1.0")
    
    reddit = asyncpraw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        username=os.getenv("REDDIT_USERNAME"),
        password=os.getenv("REDDIT_PASSWORD"),
        user_agent="GlobalRedditStream/1.0",
        requestor_class=lambda *a, **kw: requestor,
    )
    
    reddit_username = os.getenv("REDDIT_USERNAME", "unknown")
    logger.info(f"[{worker_id}][CONFIG] Worker ID: {worker_id}")
    logger.info(f"[{worker_id}][CONFIG] Reddit Account: {reddit_username}")
    logger.info(f"[{worker_id}][CONFIG] PostgreSQL: {os.getenv('POSTGRES_USER')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")
    logger.info(f"[{worker_id}][CONFIG] Streaming from: r/all (entire Reddit)")
    logger.info("=" * 80)
    
    try:
        # Create tasks for posts and comments from r/all
        post_task = asyncio.create_task(stream_all_posts(reddit, rate_manager, storage, worker_id))
        comment_task = asyncio.create_task(stream_all_comments(reddit, rate_manager, storage, worker_id))
        
        # Run both concurrently (will run forever)
        await asyncio.gather(post_task, comment_task, return_exceptions=True)
    
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
