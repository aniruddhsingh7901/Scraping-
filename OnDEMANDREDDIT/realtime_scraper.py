#!/usr/bin/env python3
"""
realtime_scraper.py

Optimized real-time Reddit scraper:
✅ Parallel subreddit processing
✅ Batched comment fetching
✅ DataEntity format output
✅ Rate limit aware
✅ Memory efficient
"""

import asyncio
import aiohttp
import asyncpraw
from asyncprawcore import Requestor, ResponseException
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import logging
import time

# Import Data Universe components
import sys
sys.path.append('./data-universe')
from common.data import DataEntity
from scraping.reddit.model import RedditContent, RedditDataType, DELETED_USER
from scraping.reddit.utils import normalize_permalink, extract_media_urls

from account_pool_manager import RedditAccount

logger = logging.getLogger(__name__)


class TrackingRequestor(Requestor):
    """Track rate limits"""
    
    def __init__(self, account: RedditAccount, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.account = account
    
    async def request(self, *args, **kwargs):
        response = await super().request(*args, **kwargs)
        headers = response.headers
        
        if "x-ratelimit-remaining" in headers:
            remaining = float(headers.get("x-ratelimit-remaining", 0))
            reset = float(headers.get("x-ratelimit-reset", 0))
            self.account.update_rate_limit(remaining, reset)
        
        return response


class RealtimeScraper:
    """Real-time Reddit scraper"""
    
    def __init__(
        self,
        account: RedditAccount,
        max_posts: int = 100,
        include_comments: bool = True,
        max_comments_per_post: int = 500,
        days_back: int = 7
    ):
        self.account = account
        self.max_posts = max_posts
        self.include_comments = include_comments
        self.max_comments_per_post = max_comments_per_post
        self.days_back = days_back
        self.user_agent = f"linux:ondemandreddit.scraper:1.0.0 (by u/{self.account.username})"
        
        self.stats = {
            'posts_scraped': 0,
            'comments_scraped': 0,
            'errors': 0
        }
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.reddit: Optional[asyncpraw.Reddit] = None
    
    async def initialize(self):
        """Initialize Reddit client"""
        # Setup proxy
        try:
            proxy_url = self.account.proxy_url
            
            # Parse proxy_url format: http://user:pass@host:port
            if "@" in proxy_url:
                # Format: http://user:pass@host:port
                auth_part = proxy_url.split("@")[0].replace("http://", "")
                host_part = proxy_url.split("@")[1]
                user, pwd = auth_part.split(":")
                proxy_url_clean = f"http://{host_part}"
                proxy_auth = aiohttp.BasicAuth(user, pwd)
            else:
                # No auth
                proxy_url_clean = proxy_url
                proxy_auth = None
            
            logger.info(f"[SCRAPER] Using proxy: {proxy_url_clean}")
            
        except Exception as e:
            logger.error(f"[SCRAPER] Proxy parsing error: {e}")
            raise
        
        # Setup session
        timeout = aiohttp.ClientTimeout(total=120)
        conn = aiohttp.TCPConnector(ssl=False)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=conn)
        
        # Apply proxy to all requests
        if proxy_url_clean:
            original_request = self.session._request
            async def proxy_request(method, url, **kwargs):
                kwargs["proxy"] = proxy_url_clean
                if proxy_auth:
                    kwargs["proxy_auth"] = proxy_auth
                return await original_request(method, url, **kwargs)
            self.session._request = proxy_request
        
        # Setup Reddit client
        requestor = TrackingRequestor(
            self.account,
            session=self.session,
            user_agent=self.user_agent
        )
        
        self.reddit = asyncpraw.Reddit(
            client_id=self.account.client_id,
            client_secret=self.account.client_secret,
            username=self.account.username,
            password=self.account.password,
            user_agent=self.user_agent,
            requestor_class=lambda *a, **kw: requestor,
        )
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.reddit:
            await self.reddit.close()
        if self.session:
            await self.session.close()
    
    def parse_submission(self, submission) -> Optional[RedditContent]:
        """Parse submission to RedditContent"""
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
    
    def parse_comment(self, comment, submission_nsfw=False) -> Optional[RedditContent]:
        """Parse comment to RedditContent"""
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
                is_nsfw=submission_nsfw,
                score=getattr(comment, 'score', None),
                upvote_ratio=None,
                num_comments=None,
            )
            return content
        except Exception as e:
            return None
    
    async def fetch_comments_paginated(self, submission) -> List[RedditContent]:
        """Fetch comments with pagination limit"""
        comments = []
        
        try:
            await submission.load()
            
            # Only expand top 10 "more" links (prevents blocking)
            await submission.comments.replace_more(limit=10)
            
            # Limit to max_comments_per_post
            comment_list = submission.comments.list()[:self.max_comments_per_post]
            
            for comment in comment_list:
                try:
                    comment_content = self.parse_comment(comment, submission.over_18)
                    if comment_content:
                        comments.append(comment_content)
                except:
                    continue
            
            self.stats['comments_scraped'] += len(comments)
            
        except Exception as e:
            logger.error(f"Error fetching comments: {e}")
            self.stats['errors'] += 1
        
        return comments
    
    async def scrape_subreddit(self, subreddit_name: str) -> List[DataEntity]:
        """Scrape single subreddit"""
        entities = []
        
        try:
            subreddit = await self.reddit.subreddit(subreddit_name)
            cutoff = datetime.utcnow() - timedelta(days=self.days_back)
            
            count = 0
            async for post in subreddit.new(limit=self.max_posts):
                try:
                    created = datetime.utcfromtimestamp(post.created_utc)
                    if created < cutoff:
                        break
                    
                    # Parse post
                    post_content = self.parse_submission(post)
                    if post_content:
                        # Skip NSFW with media
                        if post_content.is_nsfw and post_content.media:
                            continue
                        
                        # Convert to DataEntity
                        post_entity = RedditContent.to_data_entity(content=post_content)
                        entities.append(post_entity)
                        self.stats['posts_scraped'] += 1
                        count += 1
                        
                        # Fetch comments if enabled
                        if self.include_comments:
                            comments = await self.fetch_comments_paginated(post)
                            for comment in comments:
                                comment_entity = RedditContent.to_data_entity(content=comment)
                                entities.append(comment_entity)
                        
                        # Rate limit delay
                        await asyncio.sleep(0.5)
                    
                    if count >= self.max_posts:
                        break
                        
                except ResponseException as e:
                    if hasattr(e, 'response'):
                        if e.response.status == 429:
                            logger.warning(f"Rate limited on r/{subreddit_name}")
                            await asyncio.sleep(60)
                        elif e.response.status == 403:
                            logger.error(f"Forbidden (403) on r/{subreddit_name}; marking account {self.account.username} as forbidden")
                            try:
                                self.account.mark_forbidden()
                            except Exception:
                                pass
                            self.stats['errors'] += 1
                            break
                        else:
                            self.stats['errors'] += 1
                    else:
                        self.stats['errors'] += 1
                except Exception as e:
                    logger.error(f"Error processing post: {e}")
                    if "403" in str(e):
                        try:
                            self.account.mark_forbidden()
                        except Exception:
                            pass
                    self.stats['errors'] += 1
            
            logger.info(f"[SCRAPER] r/{subreddit_name}: {count} posts, {self.stats['comments_scraped']} comments")
            
        except Exception as e:
            logger.error(f"Error scraping r/{subreddit_name}: {e}")
            self.stats['errors'] += 1
        
        return entities

    async def search_keyword_in_subreddit(self, subreddit_name: str, keyword: str) -> List[DataEntity]:
        """Search a subreddit for a keyword and return matching posts (and optionally comments)."""
        entities: List[DataEntity] = []
        try:
            subreddit = await self.reddit.subreddit(subreddit_name)

            # Map days_back to Reddit time_filter for search API
            if self.days_back <= 1:
                time_filter = "day"
            elif self.days_back <= 7:
                time_filter = "week"
            elif self.days_back <= 30:
                time_filter = "month"
            else:
                time_filter = "year"

            cutoff = datetime.utcnow() - timedelta(days=self.days_back)
            count = 0

            async for post in subreddit.search(
                query=keyword, sort="new", time_filter=time_filter, limit=self.max_posts
            ):
                try:
                    created = datetime.utcfromtimestamp(post.created_utc)
                    if created < cutoff:
                        continue

                    post_content = self.parse_submission(post)
                    if post_content:
                        # Skip NSFW with media
                        if post_content.is_nsfw and post_content.media:
                            continue

                        # Convert to DataEntity
                        post_entity = RedditContent.to_data_entity(content=post_content)
                        entities.append(post_entity)
                        self.stats["posts_scraped"] += 1
                        count += 1

                        # Fetch comments if enabled
                        if self.include_comments:
                            comments = await self.fetch_comments_paginated(post)
                            for comment in comments:
                                comment_entity = RedditContent.to_data_entity(content=comment)
                                entities.append(comment_entity)

                        # Rate limit delay
                        await asyncio.sleep(0.5)

                    if count >= self.max_posts:
                        break

                except ResponseException as e:
                    if hasattr(e, "response"):
                        if e.response.status == 429:
                            logger.warning(f"Rate limited while searching '{keyword}' on r/{subreddit_name}")
                            await asyncio.sleep(60)
                        elif e.response.status == 403:
                            logger.error(
                                f"Forbidden (403) while searching '{keyword}' on r/{subreddit_name}; "
                                f"marking account {self.account.username} as forbidden"
                            )
                            try:
                                self.account.mark_forbidden()
                            except Exception:
                                pass
                            self.stats["errors"] += 1
                            break
                        else:
                            self.stats["errors"] += 1
                    else:
                        self.stats["errors"] += 1
                except Exception as e:
                    logger.error(f"Error processing search result: {e}")
                    if "403" in str(e):
                        try:
                            self.account.mark_forbidden()
                        except Exception:
                            pass
                    self.stats["errors"] += 1

            logger.info(f"[SEARCH] r/{subreddit_name} '{keyword}': {count} posts, {self.stats['comments_scraped']} comments")

        except Exception as e:
            logger.error(f"Error searching '{keyword}' in r/{subreddit_name}: {e}")
            self.stats["errors"] += 1

        return entities

    async def search_keywords(self, subreddit_names: List[str], keywords: List[str]) -> Dict[str, Any]:
        """Search multiple subreddits for multiple keywords."""
        await self.initialize()
        try:
            # Create tasks for each (subreddit, keyword) pair
            tasks = [
                self.search_keyword_in_subreddit(subreddit, keyword)
                for subreddit in subreddit_names
                for keyword in keywords
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Combine results
            all_entities: List[DataEntity] = []
            for result in results:
                if isinstance(result, list):
                    all_entities.extend(result)

            # Convert to serializable dicts
            entities_dict = [entity.to_json_dict() for entity in all_entities]

            return {
                "data_entities": entities_dict,
                "stats": self.stats,
            }
        finally:
            await self.cleanup()

    async def scrape_subreddits(self, subreddit_names: List[str]) -> Dict[str, Any]:
        """Scrape multiple subreddits in parallel"""
        await self.initialize()
        try:
            # Scrape all subreddits in parallel
            tasks = [self.scrape_subreddit(name) for name in subreddit_names]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Combine results
            all_entities: List[DataEntity] = []
            for result in results:
                if isinstance(result, list):
                    all_entities.extend(result)

            # Convert DataEntity objects to dicts for JSON serialization
            entities_dict = [entity.to_json_dict() for entity in all_entities]

            return {
                "data_entities": entities_dict,
                "stats": self.stats
            }
        finally:
            await self.cleanup()

    async def scrape_subreddits_streaming(self, subreddit_names: List[str]):
        """Scrape subreddits with streaming response (yields entities as they arrive)"""
        await self.initialize()
        try:
            for subreddit_name in subreddit_names:
                entities = await self.scrape_subreddit(subreddit_name)

                # Yield each entity as it's scraped
                for entity in entities:
                    data = entity.to_json_dict()
                    data["subreddit"] = subreddit_name
                    yield data
        finally:
            await self.cleanup()
