#!/usr/bin/env python3
"""
reddit_multi_scraper.py

Enhanced async Reddit scraper with:
✅ Robust 429 error handling with automatic retry
✅ Pre-emptive rate limit management
✅ Exponential backoff for transient errors
✅ Comprehensive error recovery
✅ Detailed logging system
✅ Graceful degradation
✅ Two-phase operation: historical scrape + live streaming
"""

import os
import json
import asyncio
import aiohttp
import asyncpraw
from asyncprawcore import Requestor
from asyncprawcore.exceptions import ResponseException, RequestException
from datetime import datetime, timedelta
import random
import logging
from typing import Optional, Dict, Any, List
import time

# Configuration
RATE_LIMIT_STOP = 50  # Pause when remaining <= 50
RATE_LIMIT_SLOW = 100  # Slow down when remaining <= 100
RETRY_429_WAIT = 60  # Initial wait for 429 errors (seconds)
MAX_RETRIES = 5  # Maximum retry attempts
SAVE_INTERVAL_HISTORICAL = 10  # Save every N posts during historical scrape
SAVE_INTERVAL_STREAM = 5  # Save every N posts during streaming

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
                # Slow down when approaching limit
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


def extract_media(submission) -> List[str]:
    """Extract media URLs from submission"""
    urls = []
    if hasattr(submission, "url") and submission.url:
        urls.append(submission.url)
    if getattr(submission, "media", None):
        media = submission.media
        if isinstance(media, dict):
            for v in media.values():
                if isinstance(v, dict) and "url" in v:
                    urls.append(v["url"])
    return list(set(urls))


async def fetch_all_comments(submission, rate_manager: RateLimitManager) -> List[Dict[str, Any]]:
    """Fetch all comments with retry logic and rate limit awareness"""
    
    async def _fetch():
        # Check rate limit before fetching
        should_wait, wait_time = await rate_manager.should_wait()
        if should_wait:
            await asyncio.sleep(wait_time)
        
        await submission.load()
        await submission.comments.replace_more(limit=None)
        all_comments = []
        
        def walk(comment_list):
            for c in comment_list:
                all_comments.append({
                    "id": c.id,
                    "author": str(c.author) if c.author else None,
                    "body": c.body,
                    "score": c.score,
                    "created_utc": c.created_utc,
                    "replies": [r.id for r in c.replies]
                })
                walk(c.replies)
        
        walk(submission.comments)
        return all_comments
    
    return await retry_with_backoff(_fetch)


def save_state(subreddit_name: str, data: Dict[str, Any]):
    """Save data to JSON file with error handling"""
    filename = f"reddit_data_{subreddit_name}.json"
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"[SAVE] {len(data['posts'])} posts → {filename}")
    except Exception as e:
        logger.error(f"[SAVE ERROR] Failed to save {filename}: {e}")


async def scrape_subreddit(reddit, rate_manager: RateLimitManager, subreddit_name: str):
    """Scrape last 30 days of posts from a subreddit with robust error handling"""
    logger.info(f"[SCRAPE START] r/{subreddit_name} - Historical scrape (last 30 days)")
    
    try:
        subreddit = await reddit.subreddit(subreddit_name)
    except Exception as e:
        logger.error(f"[SCRAPE ERROR] Failed to access r/{subreddit_name}: {e}")
        return
    
    data = {
        "subreddit": subreddit_name,
        "fetched_at": datetime.utcnow().isoformat(),
        "posts": [],
    }
    
    cutoff = datetime.utcnow() - timedelta(days=30)
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
                    logger.info(f"[SCRAPE DONE] r/{subreddit_name} reached 30-day cutoff ({created.date()})")
                    break
                
                # Extract post data with retry logic
                try:
                    media_urls = extract_media(post)
                    comments = await fetch_all_comments(post, rate_manager)
                    
                    post_data = {
                        "id": post.id,
                        "title": post.title,
                        "author": str(post.author) if post.author else None,
                        "score": post.score,
                        "created_utc": post.created_utc,
                        "url": post.url,
                        "num_comments": post.num_comments,
                        "media_urls": media_urls,
                        "selftext": post.selftext,
                        "comments": comments,
                    }
                    
                    data["posts"].append(post_data)
                    count += 1
                    consecutive_errors = 0  # Reset error counter on success
                    
                    await rate_manager.reset_429_counter()
                    
                    logger.info(
                        f"[r/{subreddit_name}] POST {count}: {post.title[:60]}... "
                        f"({len(comments)} comments)"
                    )
                    
                    # Save progress periodically
                    if count % SAVE_INTERVAL_HISTORICAL == 0:
                        save_state(subreddit_name, data)
                    
                except ResponseException as e:
                    if hasattr(e, 'response') and e.response.status == 429:
                        wait_time = await rate_manager.handle_429()
                        await asyncio.sleep(wait_time)
                        continue  # Retry this post
                    else:
                        logger.error(f"[POST ERROR] r/{subreddit_name} post {post.id}: {e}")
                        consecutive_errors += 1
                
                except Exception as e:
                    logger.error(f"[POST ERROR] r/{subreddit_name} post {post.id}: {type(e).__name__}: {e}")
                    consecutive_errors += 1
                
                # Check if too many consecutive errors
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(
                        f"[SCRAPE ABORT] r/{subreddit_name} - Too many consecutive errors ({consecutive_errors})"
                    )
                    break
                
                # Smart delay based on rate limit status
                delay = await rate_manager.get_delay()
                await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(f"[POST ITERATION ERROR] r/{subreddit_name}: {type(e).__name__}: {e}")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    break
                await asyncio.sleep(5)
    
    except Exception as e:
        logger.error(f"[SCRAPE ERROR] r/{subreddit_name} loop failed: {type(e).__name__}: {e}")
    
    finally:
        # Save final state
        save_state(subreddit_name, data)
        logger.info(f"[SCRAPE COMPLETE] r/{subreddit_name} - Collected {count} posts")


async def stream_subreddit(reddit, rate_manager: RateLimitManager, subreddit_name: str):
    """Stream new submissions from a subreddit with robust error handling"""
    logger.info(f"[STREAM START] r/{subreddit_name} - Live streaming mode")
    
    try:
        subreddit = await reddit.subreddit(subreddit_name)
    except Exception as e:
        logger.error(f"[STREAM ERROR] Failed to access r/{subreddit_name}: {e}")
        return
    
    data_file = f"reddit_data_{subreddit_name}.json"
    if os.path.exists(data_file):
        try:
            with open(data_file) as f:
                data = json.load(f)
            logger.info(f"[STREAM] Loaded existing data from {data_file}")
        except Exception as e:
            logger.error(f"[STREAM ERROR] Failed to load {data_file}: {e}")
            data = {
                "subreddit": subreddit_name,
                "fetched_at": datetime.utcnow().isoformat(),
                "posts": []
            }
    else:
        data = {
            "subreddit": subreddit_name,
            "fetched_at": datetime.utcnow().isoformat(),
            "posts": []
        }
    
    consecutive_errors = 0
    max_consecutive_errors = 20  # More tolerance for streaming
    
    try:
        async for post in subreddit.stream.submissions(skip_existing=True, pause_after=5):
            if post is None:
                await asyncio.sleep(2)
                continue
            
            # Check rate limits
            should_wait, wait_time = await rate_manager.should_wait()
            if should_wait:
                await asyncio.sleep(wait_time)
            
            try:
                media_urls = extract_media(post)
                comments = await fetch_all_comments(post, rate_manager)
                
                post_data = {
                    "id": post.id,
                    "title": post.title,
                    "author": str(post.author) if post.author else None,
                    "score": post.score,
                    "created_utc": post.created_utc,
                    "url": post.url,
                    "num_comments": post.num_comments,
                    "media_urls": media_urls,
                    "selftext": post.selftext,
                    "comments": comments,
                }
                
                data["posts"].append(post_data)
                consecutive_errors = 0
                
                await rate_manager.reset_429_counter()
                
                logger.info(
                    f"[STREAM r/{subreddit_name}] NEW POST: {post.title[:60]}... "
                    f"({len(comments)} comments)"
                )
                
                if len(data["posts"]) % SAVE_INTERVAL_STREAM == 0:
                    save_state(subreddit_name, data)
            
            except ResponseException as e:
                if hasattr(e, 'response') and e.response.status == 429:
                    wait_time = await rate_manager.handle_429()
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[STREAM ERROR] r/{subreddit_name}: {e}")
                    consecutive_errors += 1
            
            except Exception as e:
                logger.error(f"[STREAM ERROR] r/{subreddit_name}: {type(e).__name__}: {e}")
                consecutive_errors += 1
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error(
                    f"[STREAM ABORT] r/{subreddit_name} - Too many errors ({consecutive_errors})"
                )
                break
            
            delay = await rate_manager.get_delay()
            await asyncio.sleep(delay)
    
    except Exception as e:
        logger.error(f"[STREAM FATAL] r/{subreddit_name}: {type(e).__name__}: {e}")
    
    finally:
        save_state(subreddit_name, data)
        logger.info(f"[STREAM END] r/{subreddit_name}")


async def main():
    """Main entry point with two-phase operation"""
    logger.info("=" * 80)
    logger.info("Reddit Multi-Scraper Starting")
    logger.info("=" * 80)
    
    # Setup proxy
    proxy_env = os.getenv("PROXY")
    proxy_url, proxy_auth = None, None
    
    if proxy_env:
        try:
            host, port, user, pwd = proxy_env.split(":")
            proxy_url = f"http://{host}:{port}"
            proxy_auth = aiohttp.BasicAuth(user, pwd)
            logger.info(f"[PROXY] Using {proxy_url} with auth user={user}")
        except ValueError:
            logger.warning("[PROXY] Invalid PROXY format. Expected host:port:user:pass")
    
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
    
    # Initialize rate limit manager
    rate_manager = RateLimitManager()
    
    # Setup Reddit client with tracking requestor
    requestor = TrackingRequestor(rate_manager, session=session, user_agent="RobustRedditScraper/3.0")
    
    reddit = asyncpraw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        username=os.getenv("REDDIT_USERNAME"),
        password=os.getenv("REDDIT_PASSWORD"),
        user_agent="RobustRedditScraper/3.0",
        requestor_class=lambda *a, **kw: requestor,
    )
    
    # Get subreddits from environment variable (set by worker_manager) or use defaults
    subreddits_env = os.getenv("SUBREDDITS", "")
    if subreddits_env:
        subreddits = [s.strip() for s in subreddits_env.split(",") if s.strip()]
        logger.info(f"[CONFIG] Using subreddits from WORKER: {', '.join(subreddits)}")
    else:
        subreddits = ["python", "machinelearning", "stocks", "india", "wallstreetbets"]
        logger.info(f"[CONFIG] Using default subreddits: {', '.join(subreddits)}")
    
    worker_id = os.getenv("WORKER_ID", "standalone")
    logger.info(f"[CONFIG] Worker ID: {worker_id}")
    
    try:
        # Phase 1: Historical scrape
        logger.info("[PHASE 1] Starting historical scrape (last 30 days)")
        tasks = [
            asyncio.create_task(scrape_subreddit(reddit, rate_manager, s))
            for s in subreddits
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"[PHASE 1 ERROR] r/{subreddits[i]} failed: {result}")
        
        logger.info("[PHASE 1] Historical scrape complete")
        
        # Phase 2: Live streaming
        logger.info("[PHASE 2] Starting live streaming mode")
        stream_tasks = [
            asyncio.create_task(stream_subreddit(reddit, rate_manager, s))
            for s in subreddits
        ]
        await asyncio.gather(*stream_tasks, return_exceptions=True)
        
    except KeyboardInterrupt:
        logger.info("[SHUTDOWN] Received interrupt signal, shutting down gracefully...")
    except Exception as e:
        logger.error(f"[FATAL ERROR] {type(e).__name__}: {e}")
    finally:
        await reddit.close()
        await session.close()
        logger.info("[SHUTDOWN] Cleanup complete")


if __name__ == "__main__":
    asyncio.run(main())
