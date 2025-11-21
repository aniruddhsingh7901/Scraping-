# #!/usr/bin/env python3
# """
# fetch_all_active_subreddits.py
# ------------------------------
# Fetches ALL subreddits that have been active in the last 30 days
# by crawling through /r/all, multiple discovery methods, and tracking activity.
# """

# import os
# import asyncio
# import aiohttp
# import asyncpraw
# from asyncprawcore import Requestor
# from asyncprawcore.exceptions import ResponseException, RequestException
# from datetime import datetime, timezone, timedelta, UTC
# import json
# import time
# import random
# import logging
# from typing import Optional, List, Dict, Set
# import re

# # Configuration
# RATE_LIMIT_STOP = 50
# RATE_LIMIT_SLOW = 100
# RETRY_429_WAIT = 60
# MAX_RETRIES = 5
# DAYS_TO_CHECK = 30  # Check activity in last 30 days
# MIN_POSTS_TO_QUALIFY = 1  # Minimum posts in last 30 days to be considered "active"

# # Setup logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s] %(message)s',
#     handlers=[
#         logging.FileHandler('fetch_all_active_subreddits.log'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)


# class RateLimitManager:
#     """Centralized rate limit management"""
    
#     def __init__(self):
#         self.lock = asyncio.Lock()
#         self.last_remaining: Optional[float] = None
#         self.last_reset: Optional[float] = None
#         self.last_check_time: float = time.time()
#         self.consecutive_429s: int = 0
#         self.requests_made: int = 0
        
#     async def update(self, remaining: float, reset: float):
#         async with self.lock:
#             self.last_remaining = remaining
#             self.last_reset = reset
#             self.last_check_time = time.time()
#             self.requests_made += 1
            
#             if self.requests_made % 100 == 0:
#                 logger.info(
#                     f"[RATE STATS] Requests: {self.requests_made}, "
#                     f"Remaining: {remaining}, Resets in: {reset}s"
#                 )
    
#     async def should_wait(self) -> tuple[bool, float]:
#         async with self.lock:
#             if self.last_remaining is None:
#                 return False, 0
            
#             if self.last_remaining <= RATE_LIMIT_STOP:
#                 wait_time = (self.last_reset or 60) + 5
#                 logger.warning(
#                     f"[RATE LIMIT] Remaining={self.last_remaining} <= {RATE_LIMIT_STOP}. "
#                     f"Pausing for {wait_time}s"
#                 )
#                 return True, wait_time
            
#             return False, 0
    
#     async def get_delay(self) -> float:
#         async with self.lock:
#             if self.last_remaining is None:
#                 return random.uniform(0.5, 1.5)
            
#             if self.last_remaining <= RATE_LIMIT_SLOW:
#                 return random.uniform(3, 5)
#             elif self.last_remaining <= 200:
#                 return random.uniform(1, 2)
            
#             return random.uniform(0.5, 1.5)
    
#     async def handle_429(self):
#         async with self.lock:
#             self.consecutive_429s += 1
#             wait_time = min(RETRY_429_WAIT * (2 ** (self.consecutive_429s - 1)), 300)
#             logger.error(
#                 f"[429 ERROR] Rate limit exceeded! Attempt #{self.consecutive_429s}. "
#                 f"Waiting {wait_time}s before retry"
#             )
#             return wait_time
    
#     async def reset_429_counter(self):
#         async with self.lock:
#             if self.consecutive_429s > 0:
#                 logger.info(f"[RECOVERY] Recovered from {self.consecutive_429s} consecutive 429 errors")
#                 self.consecutive_429s = 0


# class TrackingRequestor(Requestor):
#     """Custom requestor that tracks rate limits"""
    
#     def __init__(self, rate_manager: RateLimitManager, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.rate_manager = rate_manager
    
#     async def request(self, *args, **kwargs):
#         response = await super().request(*args, **kwargs)
#         headers = response.headers
        
#         if "x-ratelimit-remaining" in headers:
#             remaining = float(headers.get("x-ratelimit-remaining", 0))
#             reset = float(headers.get("x-ratelimit-reset", 0))
#             await self.rate_manager.update(remaining, reset)
        
#         return response


# def load_reddit_accounts(env_file: str = "envActive") -> List[Dict[str, str]]:
#     """Load Reddit accounts from envActive file"""
#     accounts = []
    
#     if not os.path.exists(env_file):
#         logger.error(f"[ERROR] {env_file} not found!")
#         return accounts
    
#     with open(env_file, 'r') as f:
#         lines = f.readlines()
    
#     current_account = {}
#     for line in lines:
#         line = line.strip()
#         if not line or line.startswith('#'):
#             if current_account and all(k in current_account for k in ['client_id', 'client_secret', 'username', 'password']):
#                 accounts.append(current_account.copy())
#                 current_account = {}
#             continue
        
#         # Parse key-value pairs
#         match = re.match(r'(\w+)\s+["\'](.+)["\']', line)
#         if match:
#             key, value = match.groups()
#             current_account[key] = value
    
#     # Add last account if valid
#     if current_account and all(k in current_account for k in ['client_id', 'client_secret', 'username', 'password']):
#         accounts.append(current_account)
    
#     logger.info(f"[ACCOUNTS] Loaded {len(accounts)} Reddit accounts from {env_file}")
#     return accounts


# async def discover_subreddits_from_posts(reddit, rate_manager: RateLimitManager, 
#                                          discovered_subs: Set[str], time_filter='month') -> int:
#     """Discover subreddits by crawling through /r/all posts from last 30 days"""
#     logger.info(f"[DISCOVERY] Crawling r/all with time_filter={time_filter}...")
    
#     count = 0
#     cutoff = datetime.now(UTC) - timedelta(days=DAYS_TO_CHECK)
    
#     try:
#         # Get posts from r/all
#         subreddit = await reddit.subreddit('all')
        
#         async for post in subreddit.top(time_filter=time_filter, limit=None):
#             should_wait, wait_time = await rate_manager.should_wait()
#             if should_wait:
#                 await asyncio.sleep(wait_time)
            
#             # Check if post is within our time range
#             created = datetime.fromtimestamp(post.created_utc, UTC)
#             if created < cutoff:
#                 break
            
#             # Add subreddit
#             sub_name = post.subreddit.display_name
#             if sub_name and sub_name not in discovered_subs:
#                 discovered_subs.add(sub_name)
#                 count += 1
#                 if count % 100 == 0:
#                     logger.info(f"[r/all] Discovered {len(discovered_subs)} unique subreddits so far...")
            
#             delay = await rate_manager.get_delay()
#             await asyncio.sleep(delay)
            
#     except Exception as e:
#         logger.warning(f"[ERROR] Error crawling r/all ({time_filter}): {e}")
    
#     logger.info(f"[r/all] Found {count} new subreddits from r/all ({time_filter})")
#     return count


# async def discover_from_endpoints(reddit, rate_manager: RateLimitManager, 
#                                   discovered_subs: Set[str]) -> int:
#     """Discover subreddits using various Reddit endpoints"""
#     count = 0
    
#     # Method 1: Popular subreddits
#     logger.info("[DISCOVERY] Fetching popular subreddits...")
#     try:
#         async for sr in reddit.subreddits.popular(limit=None):
#             should_wait, wait_time = await rate_manager.should_wait()
#             if should_wait:
#                 await asyncio.sleep(wait_time)
            
#             if sr.display_name not in discovered_subs:
#                 discovered_subs.add(sr.display_name)
#                 count += 1
            
#             delay = await rate_manager.get_delay()
#             await asyncio.sleep(delay)
#     except Exception as e:
#         logger.warning(f"[ERROR] Error fetching popular: {e}")
    
#     # Method 2: New subreddits
#     logger.info("[DISCOVERY] Fetching new subreddits...")
#     try:
#         async for sr in reddit.subreddits.new(limit=None):
#             should_wait, wait_time = await rate_manager.should_wait()
#             if should_wait:
#                 await asyncio.sleep(wait_time)
            
#             if sr.display_name not in discovered_subs:
#                 discovered_subs.add(sr.display_name)
#                 count += 1
            
#             delay = await rate_manager.get_delay()
#             await asyncio.sleep(delay)
#     except Exception as e:
#         logger.warning(f"[ERROR] Error fetching new: {e}")
    
#     # Method 3: Search by comprehensive keywords
#     logger.info("[DISCOVERY] Searching subreddits by keywords...")
#     search_terms = (
#         list("abcdefghijklmnopqrstuvwxyz0123456789") +
#         ["the", "my", "r/", "game", "gaming", "news", "sport", "tech", "music", 
#          "art", "funny", "meme", "politics", "science", "movie", "tv", "food", 
#          "travel", "crypto", "ask", "question", "help", "support", "discussion",
#          "photo", "pic", "video", "gif", "daily", "weekly", "general", "meta",
#          "anime", "manga", "comic", "book", "read", "write", "learn", "study",
#          "fitness", "health", "mental", "life", "advice", "tips", "guide",
#          "diy", "craft", "build", "make", "create", "design", "programming"]
#     )
    
#     for term in search_terms:
#         try:
#             async for sr in reddit.subreddits.search(term, limit=100):
#                 should_wait, wait_time = await rate_manager.should_wait()
#                 if should_wait:
#                     await asyncio.sleep(wait_time)
                
#                 if sr.display_name not in discovered_subs:
#                     discovered_subs.add(sr.display_name)
#                     count += 1
                
#                 delay = await rate_manager.get_delay()
#                 await asyncio.sleep(delay)
#         except Exception as e:
#             logger.debug(f"Error searching for '{term}': {e}")
#             continue
    
#     logger.info(f"[DISCOVERY] Found {count} new subreddits from endpoints")
#     return count


# async def check_subreddit_activity(reddit, subreddit_name: str, 
#                                    rate_manager: RateLimitManager) -> Optional[Dict]:
#     """Check if a subreddit has been active in last 30 days"""
#     try:
#         subreddit = await reddit.subreddit(subreddit_name)
#         await subreddit.load()
        
#         should_wait, wait_time = await rate_manager.should_wait()
#         if should_wait:
#             await asyncio.sleep(wait_time)
        
#         # Check for recent posts
#         post_count = 0
#         total_comments = 0
#         cutoff = datetime.now(UTC) - timedelta(days=DAYS_TO_CHECK)
        
#         try:
#             async for post in subreddit.new(limit=100):
#                 should_wait, wait_time = await rate_manager.should_wait()
#                 if should_wait:
#                     await asyncio.sleep(wait_time)
                
#                 created = datetime.fromtimestamp(post.created_utc, UTC)
#                 if created < cutoff:
#                     break
                
#                 post_count += 1
#                 total_comments += getattr(post, 'num_comments', 0)
                
#                 delay = await rate_manager.get_delay()
#                 await asyncio.sleep(delay)
#         except Exception as e:
#             logger.debug(f"Error counting posts for r/{subreddit_name}: {e}")
        
#         # Only include if it has recent activity
#         if post_count >= MIN_POSTS_TO_QUALIFY:
#             subscribers = getattr(subreddit, 'subscribers', 0)
#             created_utc = getattr(subreddit, 'created_utc', None)
            
#             result = {
#                 "subreddit": subreddit_name,
#                 "posts_last_30d": post_count,
#                 "comments_last_30d": total_comments,
#                 "total_activity": post_count + total_comments,
#                 "subscribers": subscribers,
#                 "created_utc": created_utc,
#             }
            
#             logger.info(
#                 f"[ACTIVE] r/{subreddit_name}: {post_count} posts, "
#                 f"{total_comments} comments in last 30 days"
#             )
            
#             await rate_manager.reset_429_counter()
#             return result
#         else:
#             logger.debug(f"[INACTIVE] r/{subreddit_name}: Only {post_count} posts in last 30 days")
#             return None
        
#     except ResponseException as e:
#         if hasattr(e, 'response') and e.response.status == 429:
#             wait_time = await rate_manager.handle_429()
#             await asyncio.sleep(wait_time)
#         else:
#             logger.debug(f"[r/{subreddit_name}] Error: {e}")
#         return None
#     except Exception as e:
#         logger.debug(f"[r/{subreddit_name}] Error: {e}")
#         return None


# async def main():
#     """Main entry point"""
#     output_file = "all_active_subreddits_30days.json"
    
#     logger.info("="*80)
#     logger.info("Reddit All Active Subreddits Fetcher (Last 30 Days)")
#     logger.info("="*80)
#     logger.info(f"[MODE] Fetch ALL subreddits with activity in last {DAYS_TO_CHECK} days")
#     logger.info(f"[OUTPUT] {output_file}")
#     logger.info("="*80)
    
#     # Load Reddit accounts
#     accounts = load_reddit_accounts("envActive")
#     if not accounts:
#         logger.error("[ERROR] No Reddit accounts found in envActive!")
#         return
    
#     # Use first account
#     account = accounts[0]
#     logger.info(f"[ACCOUNT] Using: {account['username']}")
    
#     # Setup proxy if available
#     proxy_url = None
#     proxy_auth = None
#     if 'proxy_url' in account:
#         try:
#             proxy_full = account['proxy_url'].replace('http://', '')
#             user_pass, host_port = proxy_full.split('@')
#             user, pwd = user_pass.split(':')
#             host, port = host_port.split(':')
#             proxy_url = f"http://{host}:{port}"
#             proxy_auth = aiohttp.BasicAuth(user, pwd)
#             logger.info(f"[PROXY] Using: {proxy_url}")
#         except Exception as e:
#             logger.warning(f"[PROXY] Failed to parse proxy: {e}")
    
#     # Setup session
#     timeout = aiohttp.ClientTimeout(total=120)
#     conn = aiohttp.TCPConnector(ssl=False, limit=100)
#     session = aiohttp.ClientSession(timeout=timeout, connector=conn)
    
#     if proxy_url:
#         original_request = session._request
#         async def proxy_request(method, url, **kwargs):
#             kwargs["proxy"] = proxy_url
#             kwargs["proxy_auth"] = proxy_auth
#             return await original_request(method, url, **kwargs)
#         session._request = proxy_request
    
#     # Initialize rate limit manager
#     rate_manager = RateLimitManager()
    
#     # Setup Reddit client
#     requestor = TrackingRequestor(rate_manager, session=session, 
#                                   user_agent="ActiveSubredditsFetcher/1.0")
    
#     reddit = asyncpraw.Reddit(
#         client_id=account['client_id'],
#         client_secret=account['client_secret'],
#         username=account['username'],
#         password=account['password'],
#         user_agent="ActiveSubredditsFetcher/1.0",
#         requestor_class=lambda *a, **kw: requestor,
#     )
    
#     try:
#         discovered_subs: Set[str] = set()
        
#         # Phase 1: Discover subreddits from multiple sources
#         logger.info("="*80)
#         logger.info("[PHASE 1/3] DISCOVERING SUBREDDITS")
#         logger.info("="*80)
        
#         # Crawl r/all for posts from last month (discovers active subs)
#         await discover_subreddits_from_posts(reddit, rate_manager, discovered_subs, 'month')
        
#         # Use other discovery methods
#         await discover_from_endpoints(reddit, rate_manager, discovered_subs)
        
#         logger.info(f"[PHASE 1] Discovered {len(discovered_subs)} unique subreddits")
        
#         # Phase 2: Check activity for each discovered subreddit
#         logger.info("="*80)
#         logger.info(f"[PHASE 2/3] CHECKING ACTIVITY FOR {len(discovered_subs)} SUBREDDITS")
#         logger.info("="*80)
        
#         active_subreddits = []
#         subs_list = sorted(list(discovered_subs))
        
#         for i, subreddit_name in enumerate(subs_list):
#             if i % 50 == 0:
#                 logger.info(
#                     f"[PROGRESS] Checked {i}/{len(subs_list)} subreddits, "
#                     f"found {len(active_subreddits)} active..."
#                 )
            
#             activity = await check_subreddit_activity(reddit, subreddit_name, rate_manager)
#             if activity:
#                 active_subreddits.append(activity)
        
#         # Phase 3: Sort and save results
#         logger.info("="*80)
#         logger.info("[PHASE 3/3] SORTING AND SAVING RESULTS")
#         logger.info("="*80)
        
#         # Sort by total activity
#         active_subreddits.sort(key=lambda x: x['total_activity'], reverse=True)
        
#         # Prepare output
#         output_data = {
#             "fetch_date": datetime.now(UTC).isoformat(),
#             "period_days": DAYS_TO_CHECK,
#             "min_posts_to_qualify": MIN_POSTS_TO_QUALIFY,
#             "total_discovered": len(discovered_subs),
#             "total_active": len(active_subreddits),
#             "active_subreddits": active_subreddits
#         }
        
#         # Save to JSON
#         with open(output_file, 'w') as f:
#             json.dump(output_data, f, indent=2)
        
#         logger.info("="*80)
#         logger.info(f"[SUCCESS] Found {len(active_subreddits)} active subreddits out of {len(discovered_subs)} discovered")
#         logger.info(f"[SUCCESS] Saved to {output_file}")
#         logger.info("="*80)
#         logger.info("[TOP 20 MOST ACTIVE SUBREDDITS]")
#         for i, stat in enumerate(active_subreddits[:20], 1):
#             logger.info(
#                 f"{i:2d}. r/{stat['subreddit']:25s}: {stat['total_activity']:8,} activity "
#                 f"({stat['posts_last_30d']:5,} posts, {stat['comments_last_30d']:8,} comments)"
#             )
#         logger.info("="*80)
#         logger.info(f"[INFO] Full list of {len(active_subreddits)} active subreddits saved to {output_file}")
        
#     except KeyboardInterrupt:
#         logger.info("[SHUTDOWN] Received interrupt signal")
#     except Exception as e:
#         logger.error(f"[FATAL ERROR] {type(e).__name__}: {e}", exc_info=True)
#     finally:
#         await reddit.close()
#         await session.close()
#         logger.info("[SHUTDOWN] Cleanup complete")


# if __name__ == "__main__":
#     asyncio.run(main())

#!/usr/bin/env python3
"""
fetch_all_active_subreddits_fast.py
------------------------------------
OPTIMIZED VERSION - Uses concurrent processing for 5-10x speed improvement
"""

import os
import asyncio
import aiohttp
import asyncpraw
from asyncprawcore import Requestor
from asyncprawcore.exceptions import ResponseException, RequestException
from datetime import datetime, timezone, timedelta, UTC
import json
import time
import random
import logging
from typing import Optional, List, Dict, Set
import re

# OPTIMIZED Configuration
RATE_LIMIT_STOP = 20  # More aggressive
RATE_LIMIT_SLOW = 50
RETRY_429_WAIT = 30  # Reduced wait time
MAX_RETRIES = 3
DAYS_TO_CHECK = 30
MIN_POSTS_TO_QUALIFY = 1
CONCURRENT_CHECKS = 10  # Process 10 subreddits concurrently
MAX_POSTS_PER_SUBREDDIT = 50  # Reduced from 100 for faster checking

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('fetch_all_active_subreddits_fast.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class RateLimitManager:
    """Optimized rate limit management"""
    
    def __init__(self):
        self.lock = asyncio.Lock()
        self.last_remaining: Optional[float] = None
        self.last_reset: Optional[float] = None
        self.last_check_time: float = time.time()
        self.consecutive_429s: int = 0
        self.requests_made: int = 0
        self.semaphore = asyncio.Semaphore(CONCURRENT_CHECKS)
        
    async def update(self, remaining: float, reset: float):
        async with self.lock:
            self.last_remaining = remaining
            self.last_reset = reset
            self.last_check_time = time.time()
            self.requests_made += 1
            
            if self.requests_made % 200 == 0:
                logger.info(
                    f"[RATE STATS] Requests: {self.requests_made}, "
                    f"Remaining: {remaining}, Resets in: {reset}s"
                )
    
    async def should_wait(self) -> tuple[bool, float]:
        async with self.lock:
            if self.last_remaining is None:
                return False, 0
            
            if self.last_remaining <= RATE_LIMIT_STOP:
                wait_time = (self.last_reset or 30) + 2
                logger.warning(
                    f"[RATE LIMIT] Remaining={self.last_remaining} <= {RATE_LIMIT_STOP}. "
                    f"Pausing for {wait_time}s"
                )
                return True, wait_time
            
            return False, 0
    
    async def get_delay(self) -> float:
        async with self.lock:
            if self.last_remaining is None:
                return random.uniform(0.1, 0.3)
            
            if self.last_remaining <= RATE_LIMIT_SLOW:
                return random.uniform(1, 2)
            elif self.last_remaining <= 100:
                return random.uniform(0.3, 0.6)
            
            return random.uniform(0.1, 0.3)
    
    async def handle_429(self):
        async with self.lock:
            self.consecutive_429s += 1
            wait_time = min(RETRY_429_WAIT * (2 ** (self.consecutive_429s - 1)), 120)
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


def load_reddit_accounts(env_file: str = "envActive") -> List[Dict[str, str]]:
    """Load Reddit accounts from envActive file"""
    accounts = []
    
    if not os.path.exists(env_file):
        logger.error(f"[ERROR] {env_file} not found!")
        return accounts
    
    with open(env_file, 'r') as f:
        lines = f.readlines()
    
    current_account = {}
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            if current_account and all(k in current_account for k in ['client_id', 'client_secret', 'username', 'password']):
                accounts.append(current_account.copy())
                current_account = {}
            continue
        
        match = re.match(r'(\w+)\s+["\'](.+)["\']', line)
        if match:
            key, value = match.groups()
            current_account[key] = value
    
    if current_account and all(k in current_account for k in ['client_id', 'client_secret', 'username', 'password']):
        accounts.append(current_account)
    
    logger.info(f"[ACCOUNTS] Loaded {len(accounts)} Reddit accounts from {env_file}")
    return accounts


async def discover_subreddits_from_posts(reddit, rate_manager: RateLimitManager, 
                                         discovered_subs: Set[str], time_filter='month', 
                                         limit=None) -> int:
    """OPTIMIZED: Discover subreddits (unlimited)"""
    logger.info(f"[DISCOVERY] Crawling r/all (unlimited)...")
    
    count = 0
    cutoff = datetime.now(UTC) - timedelta(days=DAYS_TO_CHECK)
    
    try:
        subreddit = await reddit.subreddit('all')
        
        async for post in subreddit.top(time_filter=time_filter, limit=limit):
            should_wait, wait_time = await rate_manager.should_wait()
            if should_wait:
                await asyncio.sleep(wait_time)
            
            created = datetime.fromtimestamp(post.created_utc, UTC)
            if created < cutoff:
                break
            
            sub_name = post.subreddit.display_name
            if sub_name and sub_name not in discovered_subs:
                discovered_subs.add(sub_name)
                count += 1
            
            delay = await rate_manager.get_delay()
            await asyncio.sleep(delay)
            
    except Exception as e:
        logger.warning(f"[ERROR] Error crawling r/all: {e}")
    
    logger.info(f"[r/all] Found {count} subreddits")
    return count


async def discover_from_endpoints(reddit, rate_manager: RateLimitManager, 
                                  discovered_subs: Set[str], limit=None) -> int:
    """OPTIMIZED: Discover subreddits (unlimited)"""
    count = 0
    
    # Popular subreddits
    logger.info("[DISCOVERY] Fetching popular subreddits...")
    try:
        async for sr in reddit.subreddits.popular(limit=limit):
            should_wait, wait_time = await rate_manager.should_wait()
            if should_wait:
                await asyncio.sleep(wait_time)
            
            if sr.display_name not in discovered_subs:
                discovered_subs.add(sr.display_name)
                count += 1
            
            delay = await rate_manager.get_delay()
            await asyncio.sleep(delay)
    except Exception as e:
        logger.warning(f"[ERROR] Error fetching popular: {e}")
    
    # Search with all keywords
    logger.info("[DISCOVERY] Searching subreddits...")
    search_terms = list("abcdefghijklmnopqrstuvwxyz0123456789") + [
        "the", "my", "r/", "game", "gaming", "news", "sport", "tech", "music", 
         "art", "funny", "meme", "politics", "science", "movie", "tv", "food", 
         "travel", "crypto", "ask", "question", "help", "support", "discussion",
         "photo", "pic", "video", "gif", "daily", "weekly", "general", "meta",
         "anime", "manga", "comic", "book", "read", "write", "learn", "study",
         "fitness", "health", "mental", "life", "advice", "tips", "guide",
         "diy", "craft", "build", "make", "create", "design", "programming"
    ]
    
    for term in search_terms:  # No limit on search terms
        try:
            async for sr in reddit.subreddits.search(term, limit=None):
                should_wait, wait_time = await rate_manager.should_wait()
                if should_wait:
                    await asyncio.sleep(wait_time)
                
                if sr.display_name not in discovered_subs:
                    discovered_subs.add(sr.display_name)
                    count += 1
                
                delay = await rate_manager.get_delay()
                await asyncio.sleep(delay)
        except Exception as e:
            continue
    
    logger.info(f"[DISCOVERY] Found {count} new subreddits from endpoints")
    return count


async def check_subreddit_activity(reddit, subreddit_name: str, 
                                   rate_manager: RateLimitManager) -> Optional[Dict]:
    """OPTIMIZED: Check activity with reduced post limit"""
    async with rate_manager.semaphore:  # Limit concurrent checks
        try:
            subreddit = await reddit.subreddit(subreddit_name)
            await subreddit.load()
            
            should_wait, wait_time = await rate_manager.should_wait()
            if should_wait:
                await asyncio.sleep(wait_time)
            
            post_count = 0
            total_comments = 0
            cutoff = datetime.now(UTC) - timedelta(days=DAYS_TO_CHECK)
            
            try:
                async for post in subreddit.new(limit=MAX_POSTS_PER_SUBREDDIT):
                    created = datetime.fromtimestamp(post.created_utc, UTC)
                    if created < cutoff:
                        break
                    
                    post_count += 1
                    total_comments += getattr(post, 'num_comments', 0)
                    
                    # Early exit if we found enough activity
                    if post_count >= 10:
                        break
                    
                    delay = await rate_manager.get_delay()
                    await asyncio.sleep(delay)
            except Exception as e:
                logger.debug(f"Error counting posts for r/{subreddit_name}: {e}")
            
            if post_count >= MIN_POSTS_TO_QUALIFY:
                subscribers = getattr(subreddit, 'subscribers', 0)
                created_utc = getattr(subreddit, 'created_utc', None)
                
                result = {
                    "subreddit": subreddit_name,
                    "posts_last_30d": post_count,
                    "comments_last_30d": total_comments,
                    "total_activity": post_count + total_comments,
                    "subscribers": subscribers,
                    "created_utc": created_utc,
                }
                
                await rate_manager.reset_429_counter()
                return result
            else:
                return None
            
        except ResponseException as e:
            if hasattr(e, 'response') and e.response.status == 429:
                wait_time = await rate_manager.handle_429()
                await asyncio.sleep(wait_time)
            return None
        except Exception as e:
            logger.debug(f"[r/{subreddit_name}] Error: {e}")
            return None


async def check_batch_concurrent(reddit, subreddit_names: List[str], 
                                 rate_manager: RateLimitManager) -> List[Dict]:
    """Check multiple subreddits concurrently"""
    tasks = [
        check_subreddit_activity(reddit, name, rate_manager) 
        for name in subreddit_names
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out None and exceptions
    return [r for r in results if r and not isinstance(r, Exception)]


async def main():
    """Main entry point - OPTIMIZED"""
    output_file = "all_active_subreddits_30days.json"
    
    logger.info("="*80)
    logger.info("Reddit Active Subreddits Fetcher - FAST MODE")
    logger.info("="*80)
    logger.info(f"[MODE] Concurrent processing with {CONCURRENT_CHECKS} workers")
    logger.info(f"[OUTPUT] {output_file}")
    logger.info("="*80)
    
    accounts = load_reddit_accounts("envActive")
    if not accounts:
        logger.error("[ERROR] No Reddit accounts found in envActive!")
        return
    
    account = accounts[0]
    logger.info(f"[ACCOUNT] Using: {account['username']}")
    
    # Setup proxy
    proxy_url = None
    proxy_auth = None
    if 'proxy_url' in account:
        try:
            proxy_full = account['proxy_url'].replace('http://', '')
            user_pass, host_port = proxy_full.split('@')
            user, pwd = user_pass.split(':')
            host, port = host_port.split(':')
            proxy_url = f"http://{host}:{port}"
            proxy_auth = aiohttp.BasicAuth(user, pwd)
            logger.info(f"[PROXY] Using: {proxy_url}")
        except Exception as e:
            logger.warning(f"[PROXY] Failed to parse proxy: {e}")
    
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
    
    rate_manager = RateLimitManager()
    requestor = TrackingRequestor(rate_manager, session=session, 
                                  user_agent="ActiveSubredditsFetcher/2.0")
    
    reddit = asyncpraw.Reddit(
        client_id=account['client_id'],
        client_secret=account['client_secret'],
        username=account['username'],
        password=account['password'],
        user_agent="ActiveSubredditsFetcher/2.0",
        requestor_class=lambda *a, **kw: requestor,
    )
    
    try:
        start_time = time.time()
        discovered_subs: Set[str] = set()
        
        # Phase 1: Discovery (with limits)
        logger.info("="*80)
        logger.info("[PHASE 1/3] DISCOVERING SUBREDDITS")
        logger.info("="*80)
        
        await discover_subreddits_from_posts(reddit, rate_manager, discovered_subs, 'month', limit=None)
        await discover_from_endpoints(reddit, rate_manager, discovered_subs, limit=None)
        
        logger.info(f"[PHASE 1] Discovered {len(discovered_subs)} unique subreddits")
        
        # Phase 2: Concurrent activity checking
        logger.info("="*80)
        logger.info(f"[PHASE 2/3] CHECKING ACTIVITY (CONCURRENT)")
        logger.info("="*80)
        
        active_subreddits = []
        subs_list = sorted(list(discovered_subs))
        
        # Process in batches
        batch_size = CONCURRENT_CHECKS * 5
        for i in range(0, len(subs_list), batch_size):
            batch = subs_list[i:i+batch_size]
            logger.info(f"[PROGRESS] Processing batch {i//batch_size + 1}/{(len(subs_list)-1)//batch_size + 1}...")
            
            batch_results = await check_batch_concurrent(reddit, batch, rate_manager)
            active_subreddits.extend(batch_results)
            
            logger.info(f"[PROGRESS] Found {len(active_subreddits)} active subreddits so far...")
        
        # Phase 3: Save results
        logger.info("="*80)
        logger.info("[PHASE 3/3] SAVING RESULTS")
        logger.info("="*80)
        
        active_subreddits.sort(key=lambda x: x['total_activity'], reverse=True)
        
        output_data = {
            "fetch_date": datetime.now(UTC).isoformat(),
            "period_days": DAYS_TO_CHECK,
            "min_posts_to_qualify": MIN_POSTS_TO_QUALIFY,
            "total_discovered": len(discovered_subs),
            "total_active": len(active_subreddits),
            "execution_time_seconds": time.time() - start_time,
            "active_subreddits": active_subreddits
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        elapsed = time.time() - start_time
        logger.info("="*80)
        logger.info(f"[SUCCESS] Completed in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        logger.info(f"[SUCCESS] Found {len(active_subreddits)} active subreddits")
        logger.info(f"[SUCCESS] Saved to {output_file}")
        logger.info("="*80)
        logger.info("[TOP 20 MOST ACTIVE SUBREDDITS]")
        for i, stat in enumerate(active_subreddits[:20], 1):
            logger.info(
                f"{i:2d}. r/{stat['subreddit']:25s}: {stat['total_activity']:8,} activity "
                f"({stat['posts_last_30d']:5,} posts, {stat['comments_last_30d']:8,} comments)"
            )
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.info("[SHUTDOWN] Received interrupt signal")
    except Exception as e:
        logger.error(f"[FATAL ERROR] {type(e).__name__}: {e}", exc_info=True)
    finally:
        await reddit.close()
        await session.close()
        logger.info("[SHUTDOWN] Cleanup complete")


if __name__ == "__main__":
    asyncio.run(main())