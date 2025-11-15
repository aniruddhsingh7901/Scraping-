#!/usr/bin/env python3
"""
reddit_all_subs_30days_dataentity.py
-------------------------------------
Comprehensive Reddit scraper that:
✅ Fetches ALL subreddit names (100K+)
✅ Gets ALL posts from last 30 days from each subreddit
✅ Gets ALL comments recursively from each post
✅ Uses rate limit checking and cooldown
✅ Stores in DataEntity format to PostgreSQL
✅ Self-manages single account with cooldown
"""

import os
import asyncio
import aiohttp
import asyncpraw
from asyncprawcore import Requestor
from asyncprawcore.exceptions import ResponseException, RequestException
from datetime import datetime, timezone, timedelta
import json
import time
import random
import logging
from typing import Optional, List

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
DAYS_TO_SCRAPE = 30

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('reddit_all_subs_30days.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
bt.logging.set_info(True)


class RateLimitManager:
    """Centralized rate limit management"""
    
    def __init__(self):
        self.lock = asyncio.Lock()
        self.last_remaining: Optional[float] = None
        self.last_reset: Optional[float] = None
        self.last_check_time: float = time.time()
        self.consecutive_429s: int = 0
        self.requests_made: int = 0
        
    async def update(self, remaining: float, reset: float):
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
        async with self.lock:
            if self.last_remaining is None:
                return random.uniform(0.5, 1.5)
            
            if self.last_remaining <= RATE_LIMIT_SLOW:
                return random.uniform(3, 5)
            elif self.last_remaining <= 200:
                return random.uniform(1, 2)
            
            return random.uniform(0.5, 1.5)
    
    async def handle_429(self):
        async with self.lock:
            self.consecutive_429s += 1
            wait_time = min(RETRY_429_WAIT * (2 ** (self.consecutive_429s - 1)), 300)
            logger.error(
                f"[429 ERROR] Rate limit exceeded! Attempt #{self.consecutive_429s}. "
                f"Waiting {wait_time}s before retry"
            )
            return wait_time
    
    async def reset_429_counter(self):
        async with self.lock:
            if self.consecutive_429s > 0:
                logger.info(f"[RECOVERY] Recovered from {self.consecutive_429s} consecutive 429 errors")
                self.consecutive_429s = 0


class TrackingRequestor(Requestor):
    """Custom requestor that tracks rate limits"""
    
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
            is_nsfw=False,
            score=getattr(comment, 'score', None),
            upvote_ratio=None,
            num_comments=None,
        )
        return content
    except Exception as e:
        return None


async def get_all_subreddits(reddit, rate_manager: RateLimitManager):
    """Fetch ALL subreddit names"""
    logger.info("[STEP 1] Fetching ALL subreddit names...")
    
    subs = set()
    
    async def fetch_letter(letter):
        try:
            async for sr in reddit.subreddits.search(letter, limit=None):
                subs.add(sr.display_name)
                
                # Check rate limits
                should_wait, wait_time = await rate_manager.should_wait()
                if should_wait:
                    await asyncio.sleep(wait_time)
                
                delay = await rate_manager.get_delay()
                await asyncio.sleep(delay)
                
        except Exception as e:
            logger.error(f"Error fetching subreddits for letter {letter}: {e}")
    
    letters = "abcdefghijklmnopqrstuvwxyz0123456789"
    tasks = [asyncio.create_task(fetch_letter(c)) for c in letters]
    await asyncio.gather(*tasks)
    
    subs = sorted(subs)
    logger.info(f"[DONE] Found {len(subs)} subreddits total.")
    return subs


async def fetch_all_comments(post, rate_manager: RateLimitManager, storage: PostgreSQLMinerStorage):
    """Fetch all comments for a post recursively and store in DB"""
    try:
        await post.load()
        
        # Check rate limits
        should_wait, wait_time = await rate_manager.should_wait()
        if should_wait:
            await asyncio.sleep(wait_time)
        
        # Replace MoreComments objects (expand full thread)
        await post.comments.replace_more(limit=None)
        
        comment_entities = []
        
        def extract(comment):
            comment_content = parse_comment_to_reddit_content(comment)
            if comment_content:
                comment_entity = RedditContent.to_data_entity(content=comment_content)
                comment_entities.append(comment_entity)
            
            for reply in comment.replies:
                extract(reply)
        
        for c in post.comments:
            extract(c)
        
        # Store all comments in DB
        if comment_entities:
            storage.store_data_entities(comment_entities)
        
        return len(comment_entities)
        
    except Exception as e:
        logger.error(f"Error fetching comments: {e}")
        return 0


async def scrape_subreddit_last30(reddit, subreddit_name: str, rate_manager: RateLimitManager, storage: PostgreSQLMinerStorage):
    """Scrape last 30 days of posts + comments from a subreddit"""
    now = datetime.utcnow()
    cutoff = now - timedelta(days=DAYS_TO_SCRAPE)
    
    post_count = 0
    comment_count = 0
    
    try:
        subreddit = await reddit.subreddit(subreddit_name)
        
        async for post in subreddit.new(limit=None):
            # Check rate limits
            should_wait, wait_time = await rate_manager.should_wait()
            if should_wait:
                await asyncio.sleep(wait_time)
            
            created = datetime.utcfromtimestamp(post.created_utc)
            if created < cutoff:
                break
            
            # Parse and store post
            post_content = parse_submission_to_reddit_content(post)
            if post_content:
                # Skip NSFW content with media
                if post_content.is_nsfw and post_content.media:
                    continue
                
                post_entity = RedditContent.to_data_entity(content=post_content)
                storage.store_data_entities([post_entity])
                post_count += 1
                
                # Fetch all comments for this post
                num_comments = await fetch_all_comments(post, rate_manager, storage)
                comment_count += num_comments
                
                await rate_manager.reset_429_counter()
            
            delay = await rate_manager.get_delay()
            await asyncio.sleep(delay)
        
        logger.info(
            f"[r/{subreddit_name}] Scraped {post_count} posts + {comment_count} comments"
        )
        return post_count, comment_count
        
    except ResponseException as e:
        if hasattr(e, 'response') and e.response.status == 429:
            wait_time = await rate_manager.handle_429()
            await asyncio.sleep(wait_time)
        else:
            logger.error(f"[r/{subreddit_name}] Error: {e}")
        return 0, 0
    except Exception as e:
        logger.error(f"[r/{subreddit_name}] Error: {e}")
        return 0, 0


async def main():
    """Main entry point - Fetch all subreddits and save to JSON"""
    worker_id = os.getenv("WORKER_ID", "subreddit_fetcher")
    output_file = os.getenv("OUTPUT_FILE", "all_subreddits_list.json")
    
    logger.info("="*80)
    logger.info(f"Reddit Subreddit List Fetcher Starting [{worker_id}]")
    logger.info("="*80)
    logger.info("[MODE] Fetch ALL subreddit names and save to JSON file")
    logger.info(f"[OUTPUT] {output_file}")
    logger.info("="*80)
    
    # Setup proxy (MANDATORY)
    proxy_env = os.getenv("PROXY")
    
    if not proxy_env:
        logger.error("[PROXY] PROXY environment variable is REQUIRED!")
        return
    
    try:
        host, port, user, pwd = proxy_env.split(":")
        proxy_url = f"http://{host}:{port}"
        proxy_auth = aiohttp.BasicAuth(user, pwd)
        logger.info(f"[PROXY] Using proxy: {proxy_url} with auth user={user}")
    except ValueError as e:
        logger.error(f"[PROXY] Invalid PROXY format: {proxy_env}")
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
    
    # Initialize rate limit manager
    rate_manager = RateLimitManager()
    
    # Setup Reddit client
    requestor = TrackingRequestor(rate_manager, session=session, user_agent="AllSubreddits30DayScraper/1.0")
    
    reddit = asyncpraw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        username=os.getenv("REDDIT_USERNAME"),
        password=os.getenv("REDDIT_PASSWORD"),
        user_agent="AllSubreddits30DayScraper/1.0",
        requestor_class=lambda *a, **kw: requestor,
    )
    
    reddit_username = os.getenv("REDDIT_USERNAME", "unknown")
    logger.info(f"[{worker_id}][CONFIG] Worker ID: {worker_id}")
    logger.info(f"[{worker_id}][CONFIG] Reddit Account: {reddit_username}")
    logger.info(f"[{worker_id}][CONFIG] PostgreSQL: {os.getenv('POSTGRES_USER')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")
    logger.info("="*80)
    
    try:
        # Fetch ALL subreddits
        subreddits = await get_all_subreddits(reddit, rate_manager)
        
        if not subreddits:
            logger.error("[ERROR] No subreddits found!")
            return
        
        # Save to JSON file
        subreddit_data = {
            "fetch_date": datetime.utcnow().isoformat(),
            "total_count": len(subreddits),
            "subreddits": subreddits
        }
        
        with open(output_file, 'w') as f:
            json.dump(subreddit_data, f, indent=2)
        
        logger.info("="*80)
        logger.info(f"[SUCCESS] Fetched {len(subreddits)} subreddits")
        logger.info(f"[SUCCESS] Saved to {output_file}")
        logger.info("="*80)
        logger.info(f"[NEXT] Run 'pm2 start all_subs_worker_manager.py' to start parallel scraping")
        logger.info("="*80)
    
    except KeyboardInterrupt:
        logger.info(f"[{worker_id}][SHUTDOWN] Received interrupt signal")
    except Exception as e:
        logger.error(f"[{worker_id}][FATAL ERROR] {type(e).__name__}: {e}")
    finally:
        await reddit.close()
        await session.close()
        logger.info(f"[{worker_id}][SHUTDOWN] Cleanup complete")


if __name__ == "__main__":
    asyncio.run(main())
