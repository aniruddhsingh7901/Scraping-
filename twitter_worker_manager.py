"""
High-Volume Twitter Worker Manager
Manages multiple workers for 20M+ tweets/day scraping using twscrape

Features:
- Dynamic worker scaling based on account availability
- Hashtag rotation and keyword expansion
- PostgreSQL storage integration
- Continuous 30-day rolling scraping
"""

import asyncio
import traceback
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set
import json
import sys
import time

sys.path.append('/root/Algo-test-script')
sys.path.append('/root/Algo-test-script/twscrape')
sys.path.append('/root/Algo-test-script/data-universe')

import bittensor as bt
from twscrape import API
from data_universe.scraping.x.twscrape_miner import TwscrapeXMiner
from common.data import DataEntity, DataLabel
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig


class TwitterWorkerManager:
    """
    Manages multiple workers for high-volume Twitter scraping.
    
    Architecture:
    - Each worker handles one hashtag/keyword at a time
    - Workers rotate through hashtag list
    - Automatic scaling based on account availability (2/3 of accounts = workers)
    - PostgreSQL storage for all scraped data
    """
    
    def __init__(
        self,
        db_path: str = "twitter_accounts.db",
        postgres_config: Dict = None,
        hashtags_file: str = None
    ):
        """
        Initialize the worker manager.
        
        Args:
            db_path: Path to twscrape accounts database
            postgres_config: PostgreSQL connection config
            hashtags_file: Path to file containing hashtags/keywords (one per line)
        """
        self.db_path = db_path
        self.postgres_config = postgres_config or self._default_postgres_config()
        self.hashtags_file = hashtags_file
        
        # Worker management
        self.workers = []
        self.active_workers = 0
        self.max_workers = 0
        
        # Scraping state
        self.hashtags: List[str] = []
        self.current_hashtag_index = 0
        self.scraped_tweet_ids: Set[int] = set()
        
        # Statistics
        self.stats = {
            'total_scraped': 0,
            'total_stored': 0,
            'start_time': None,
            'errors': 0,
            'workers_spawned': 0
        }
        
    def _default_postgres_config(self) -> Dict:
        """Default PostgreSQL configuration."""
        return {
            'host': 'localhost',
            'port': 5432,
            'database': 'twitter_data',
            'user': 'postgres',
            'password': 'password'
        }
    
    async def initialize(self):
        """Initialize the worker manager."""
        bt.logging.info("Initializing Twitter Worker Manager...")
        
        # Initialize API to check accounts
        api = API(pool=self.db_path)
        account_stats = await api.pool.stats()
        
        total_accounts = account_stats.get('active', 0)
        bt.logging.info(f"Found {total_accounts} active Twitter accounts")
        
        if total_accounts == 0:
            bt.logging.error("No active accounts found! Please add accounts first.")
            return False
        
        # Calculate optimal workers (2/3 of accounts, minimum 1)
        self.max_workers = max(1, int(total_accounts * 2 / 3))
        bt.logging.success(f"Will spawn {self.max_workers} concurrent workers")
        
        # Load hashtags
        if not await self._load_hashtags():
            bt.logging.error("Failed to load hashtags")
            return False
        
        # Initialize PostgreSQL
        if not self._init_postgres():
            bt.logging.error("Failed to initialize PostgreSQL")
            return False
        
        self.stats['start_time'] = datetime.now(timezone.utc)
        return True
    
    async def _load_hashtags(self) -> bool:
        """Load hashtags from file or use defaults."""
        if self.hashtags_file:
            try:
                with open(self.hashtags_file, 'r') as f:
                    self.hashtags = [line.strip() for line in f if line.strip()]
                bt.logging.info(f"Loaded {len(self.hashtags)} hashtags from {self.hashtags_file}")
                return True
            except Exception as e:
                bt.logging.error(f"Error loading hashtags: {e}")
                return False
        else:
            # Default popular hashtags for crypto/tech
            self.hashtags = [
                "#bitcoin", "#ethereum", "#crypto", "#blockchain", "#web3",
                "#defi", "#nft", "#ai", "#artificialintelligence", "#machinelearning",
                "#technology", "#innovation", "#startup", "#programming", "#coding",
                "#python", "#javascript", "#react", "#nodejs", "#developer"
            ]
            bt.logging.info(f"Using {len(self.hashtags)} default hashtags")
            return True
    
    def _init_postgres(self) -> bool:
        """Initialize PostgreSQL connection and create tables."""
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cur = conn.cursor()
            
            # Create table for tweets
            cur.execute("""
                CREATE TABLE IF NOT EXISTS twitter_data (
                    id SERIAL PRIMARY KEY,
                    tweet_id BIGINT UNIQUE,
                    uri TEXT NOT NULL,
                    username TEXT NOT NULL,
                    content TEXT NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    hashtags TEXT[],
                    media_urls TEXT[],
                    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    content_size_bytes INTEGER,
                    source TEXT DEFAULT 'twscrape',
                    CONSTRAINT unique_tweet_id UNIQUE (tweet_id)
                )
            """)
            
            # Create index on timestamp for 30-day queries
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_twitter_timestamp 
                ON twitter_data (timestamp DESC)
            """)
            
            # Create index on hashtags for filtering
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_twitter_hashtags 
                ON twitter_data USING GIN (hashtags)
            """)
            
            conn.commit()
            cur.close()
            conn.close()
            
            bt.logging.success("PostgreSQL initialized successfully")
            return True
            
        except Exception as e:
            bt.logging.error(f"PostgreSQL initialization error: {e}")
            return False
    
    async def _get_next_hashtag(self) -> str:
        """Get next hashtag from rotation with expansion strategy."""
        if not self.hashtags:
            return "#trending"
        
        # Get base hashtag
        base_hashtag = self.hashtags[self.current_hashtag_index % len(self.hashtags)]
        self.current_hashtag_index += 1
        
        # Expansion strategy: also search as keyword without #
        # This increases coverage by finding tweets that mention the term
        # but might not have used it as a hashtag
        
        return base_hashtag
    
    async def _expand_search_query(self, hashtag: str) -> List[str]:
        """
        Expand search strategy for better coverage.
        
        Returns list of search variations:
        1. Original hashtag (e.g., #bitcoin)
        2. Keyword without # (e.g., bitcoin)
        3. Related variations if applicable
        """
        queries = []
        
        # Original hashtag
        queries.append(hashtag)
        
        # Keyword version (without #)
        if hashtag.startswith('#'):
            keyword = hashtag[1:]
            queries.append(keyword)
        
        return queries
    
    async def worker_task(self, worker_id: int):
        """
        Individual worker task that continuously scrapes.
        
        Each worker:
        1. Gets next hashtag from rotation
        2. Scrapes tweets from last 30 days
        3. Stores in PostgreSQL
        4. Repeats
        """
        bt.logging.info(f"Worker {worker_id} started")
        self.active_workers += 1
        self.stats['workers_spawned'] += 1
        
        # Create scraper instance for this worker
        scraper = TwscrapeXMiner(db_path=self.db_path)
        
        try:
            while True:
                # Get next hashtag
                hashtag = await self._get_next_hashtag()
                bt.logging.info(f"Worker {worker_id}: Processing {hashtag}")
                
                # Get search query variations
                queries = await self._expand_search_query(hashtag)
                
                for query in queries:
                    try:
                        # Build scrape config for last 30 days
                        scrape_config = ScrapeConfig(
                            entity_limit=1000,  # Per query limit
                            date_range=DateRange(
                                start=datetime.now(timezone.utc) - timedelta(days=30),
                                end=datetime.now(timezone.utc),
                            ),
                            labels=[DataLabel(value=query)],
                        )
                        
                        # Scrape tweets
                        entities = await scraper.scrape(scrape_config)
                        
                        if entities:
                            # Filter duplicates
                            new_entities = await self._filter_duplicates(entities)
                            
                            # Store in PostgreSQL
                            stored_count = await self._store_entities(new_entities)
                            
                            self.stats['total_scraped'] += len(entities)
                            self.stats['total_stored'] += stored_count
                            
                            bt.logging.success(
                                f"Worker {worker_id}: {query} - "
                                f"Scraped {len(entities)}, Stored {stored_count} new tweets"
                            )
                        else:
                            bt.logging.debug(f"Worker {worker_id}: No tweets for {query}")
                        
                        # Small delay between queries to avoid overwhelming
                        await asyncio.sleep(2)
                        
                    except Exception as e:
                        bt.logging.error(f"Worker {worker_id} error on {query}: {e}")
                        self.stats['errors'] += 1
                        await asyncio.sleep(5)
                
                # Log progress every 10 hashtags
                if self.current_hashtag_index % 10 == 0:
                    await self._log_progress()
                
        except Exception as e:
            bt.logging.error(f"Worker {worker_id} crashed: {traceback.format_exc()}")
            self.stats['errors'] += 1
        finally:
            self.active_workers -= 1
            bt.logging.info(f"Worker {worker_id} stopped")
    
    async def _filter_duplicates(self, entities: List[DataEntity]) -> List[DataEntity]:
        """Filter out already-scraped tweets."""
        from scraping.x.model import XContent
        
        new_entities = []
        for entity in entities:
            try:
                content = XContent.from_data_entity(entity)
                # Extract tweet ID from URL
                import re
                match = re.search(r'/status/(\d+)', content.url)
                if match:
                    tweet_id = int(match.group(1))
                    if tweet_id not in self.scraped_tweet_ids:
                        self.scraped_tweet_ids.add(tweet_id)
                        new_entities.append(entity)
            except:
                continue
        
        return new_entities
    
    async def _store_entities(self, entities: List[DataEntity]) -> int:
        """Store DataEntities in PostgreSQL."""
        if not entities:
            return 0
        
        from scraping.x.model import XContent
        
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cur = conn.cursor()
            
            batch_data = []
            for entity in entities:
                try:
                    content = XContent.from_data_entity(entity)
                    
                    # Extract tweet ID
                    import re
                    match = re.search(r'/status/(\d+)', content.url)
                    tweet_id = int(match.group(1)) if match else None
                    
                    if tweet_id:
                        batch_data.append((
                            tweet_id,
                            content.url,
                            content.username,
                            content.text,
                            content.timestamp,
                            content.tweet_hashtags,
                            content.media or [],
                            entity.content_size_bytes
                        ))
                except Exception as e:
                    bt.logging.debug(f"Skipping entity: {e}")
                    continue
            
            if batch_data:
                # Batch insert with conflict handling
                execute_batch(cur, """
                    INSERT INTO twitter_data 
                    (tweet_id, uri, username, content, timestamp, hashtags, media_urls, content_size_bytes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tweet_id) DO NOTHING
                """, batch_data, page_size=1000)
                
                conn.commit()
                stored = cur.rowcount
            else:
                stored = 0
            
            cur.close()
            conn.close()
            
            return stored
            
        except Exception as e:
            bt.logging.error(f"PostgreSQL storage error: {traceback.format_exc()}")
            return 0
    
    async def _log_progress(self):
        """Log current progress statistics."""
        if self.stats['start_time']:
            elapsed = (datetime.now(timezone.utc) - self.stats['start_time']).total_seconds()
            rate_per_hour = (self.stats['total_stored'] / elapsed) * 3600 if elapsed > 0 else 0
            rate_per_day = rate_per_hour * 24
            
            bt.logging.info(
                f"\n{'='*60}\n"
                f"PROGRESS REPORT\n"
                f"{'='*60}\n"
                f"Active Workers: {self.active_workers}/{self.max_workers}\n"
                f"Total Scraped: {self.stats['total_scraped']:,}\n"
                f"Total Stored (unique): {self.stats['total_stored']:,}\n"
                f"Errors: {self.stats['errors']}\n"
                f"Elapsed Time: {elapsed/3600:.2f} hours\n"
                f"Rate: {rate_per_hour:.0f} tweets/hour ({rate_per_day:.0f} tweets/day)\n"
                f"Target: 20,000,000 tweets/day\n"
                f"Progress: {(rate_per_day/20_000_000)*100:.2f}% of target\n"
                f"{'='*60}\n"
            )
    
    async def start(self):
        """Start the worker manager."""
        if not await self.initialize():
            bt.logging.error("Initialization failed")
            return
        
        bt.logging.success(f"Starting {self.max_workers} workers...")
        
        # Spawn workers
        tasks = []
        for i in range(self.max_workers):
            task = asyncio.create_task(self.worker_task(i))
            tasks.append(task)
            # Stagger worker starts
            await asyncio.sleep(1)
        
        # Monitor workers
        try:
            # Log progress every 5 minutes
            while True:
                await asyncio.sleep(300)
                await self._log_progress()
                
        except KeyboardInterrupt:
            bt.logging.info("Shutting down workers...")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        
        bt.logging.info("Worker manager stopped")


async def main():
    """Main entry point."""
    bt.logging.set_trace(True)
    
    # Configuration
    postgres_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'twitter_data',
        'user': 'postgres',
        'password': 'postgres'  # Change this!
    }
    
    manager = TwitterWorkerManager(
        db_path="twitter_accounts.db",
        postgres_config=postgres_config,
        hashtags_file="hashtags.txt"  # Optional: provide your own list
    )
    
    await manager.start()


if __name__ == "__main__":
    asyncio.run(main())
