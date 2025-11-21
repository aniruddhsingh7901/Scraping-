"""
Parallel Twitter Worker Manager
- Compatible with TwscrapeXMinerParallel from twscrape_miner.py
- Uses parallel scraping for 100M+ tweets/day capability
- Stores in PostgreSQL with batch processing
"""

import asyncio
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone, timedelta
import sys

sys.path.append('/root/Algo-test-script/twscrape')
sys.path.append('/root/Algo-test-script/data-universe')

import bittensor as bt
from common.data import DataEntity, DataLabel, TimeBucket
from common.date_range import DateRange
from scraping.scraper import ScrapeConfig
from scraping.x.twscrape_miner import TwscrapeXMinerParallel


class ParallelTwitterWorkerManager:
    """
    Worker manager using parallel scraping architecture.
    
    Compatible with TwscrapeXMinerParallel for high-volume scraping.
    """
    
    def __init__(
        self,
        db_path: str = "twitter_accounts.db",
        max_concurrent_tweets: int = 100,
        max_concurrent_replies: int = 50,
        batch_size: int = 100,
        worker_count: int = 50,
        api_rate_limit: int = 500
    ):
        """Initialize parallel worker manager."""
        # Initialize parallel scraper
        self.scraper = TwscrapeXMinerParallel(
            db_path=db_path,
            max_concurrent_tweets=max_concurrent_tweets,
            max_concurrent_replies=max_concurrent_replies,
            batch_size=batch_size,
            worker_count=worker_count,
            api_rate_limit=api_rate_limit
        )
        
        self.postgres_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'reddit_miner_db',
            'user': 'postgres',
            'password': 'postgres'
        }
        self.hashtags = []
        self.stats = {
            'scraped': 0,
            'stored': 0,
            'hashtags_processed': 0,
            'errors': 0
        }
    
    def load_hashtags(self):
        """Load hashtags from file."""
        try:
            with open("hashtags.txt", 'r') as f:
                self.hashtags = [line.strip() for line in f if line.strip() and not line.startswith('#@')]
            bt.logging.info(f"Loaded {len(self.hashtags)} hashtags")
            return True
        except Exception as e:
            bt.logging.error(f"Error loading hashtags: {e}")
            return False
    
    def store_entities(self, entities):
        """Store entities in PostgreSQL using batch processing."""
        if not entities:
            return 0
        
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cur = conn.cursor()
            
            thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
            batch_data = []
            
            for entity in entities:
                try:
                    entity_dt = entity.datetime if entity.datetime.tzinfo else entity.datetime.replace(tzinfo=timezone.utc)
                    if entity_dt < thirty_days_ago:
                        continue
                    
                    time_bucket_id = TimeBucket.from_datetime(entity.datetime).id
                    label = entity.label.value if entity.label else None
                    
                    batch_data.append((
                        entity.uri,
                        entity.datetime,
                        time_bucket_id,
                        2,  # source=2 for X
                        label,
                        entity.content,
                        entity.content_size_bytes
                    ))
                except Exception as e:
                    bt.logging.debug(f"Error preparing entity: {e}")
                    continue
            
            if batch_data:
                execute_batch(cur, """
                    INSERT INTO dataentity 
                    (uri, datetime, timebucketid, source, label, content, contentsizebytes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (uri) DO UPDATE SET
                        datetime = EXCLUDED.datetime,
                        content = EXCLUDED.content
                """, batch_data, page_size=1000)
                
                conn.commit()
                stored = len(batch_data)
            else:
                stored = 0
            
            cur.close()
            conn.close()
            return stored
        except Exception as e:
            bt.logging.error(f"Storage error: {e}")
            return 0
    
    async def scrape_hashtag(self, hashtag: str, limit: int = 1000):
        """
        Scrape one hashtag using parallel scraper.
        
        Args:
            hashtag: Hashtag to scrape (with or without #)
            limit: Maximum tweets to scrape
            
        Returns:
            List[DataEntity]: Scraped entities
        """
        try:
            # Ensure hashtag has #
            if not hashtag.startswith('#'):
                hashtag = f"#{hashtag}"
            
            # Create scrape config
            scrape_config = ScrapeConfig(
                entity_limit=limit,
                date_range=DateRange(
                    start=datetime.now(timezone.utc) - timedelta(days=30),
                    end=datetime.now(timezone.utc),
                ),
                labels=[DataLabel(value=hashtag)],
            )
            
            bt.logging.info(f"Starting parallel scrape for {hashtag}")
            
            # Use parallel scraper
            entities = await self.scraper.scrape(scrape_config)
            
            bt.logging.success(f"Parallel scraped {len(entities)} entities for {hashtag}")
            return entities
            
        except Exception as e:
            bt.logging.error(f"Error scraping {hashtag}: {e}")
            self.stats['errors'] += 1
            return []
    
    async def scrape_multiple_hashtags_parallel(self, hashtags: list, limit_per_hashtag: int = 1000):
        """
        Scrape multiple hashtags in parallel.
        
        This allows scraping different hashtags simultaneously for even more throughput.
        
        Args:
            hashtags: List of hashtags to scrape
            limit_per_hashtag: Limit per hashtag
            
        Returns:
            Dict mapping hashtag to entities
        """
        bt.logging.info(f"Starting parallel scrape of {len(hashtags)} hashtags")
        
        # Create tasks for all hashtags
        tasks = []
        for hashtag in hashtags:
            task = self.scrape_hashtag(hashtag, limit=limit_per_hashtag)
            tasks.append(task)
        
        # Run all hashtag scrapes in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Organize results
        hashtag_entities = {}
        for hashtag, result in zip(hashtags, results):
            if isinstance(result, Exception):
                bt.logging.error(f"Error scraping {hashtag}: {result}")
                hashtag_entities[hashtag] = []
            else:
                hashtag_entities[hashtag] = result
        
        return hashtag_entities
    
    async def run(self, parallel_hashtag_batches: int = 5):
        """
        Main loop with parallel processing.
        
        Args:
            parallel_hashtag_batches: Number of hashtags to scrape in parallel at once
        """
        bt.logging.info("Starting PARALLEL Twitter Worker Manager...")
        
        # Check account health
        try:
            stats = await self.scraper.get_account_stats()
            bt.logging.info(f"Account pool stats: {stats}")
        except Exception as e:
            bt.logging.warning(f"Could not check account health: {str(e)}")
        
        if not self.load_hashtags():
            return
        
        # Process hashtags in parallel batches
        total_hashtags = len(self.hashtags)
        
        for i in range(0, total_hashtags, parallel_hashtag_batches):
            batch = self.hashtags[i:i + parallel_hashtag_batches]
            batch_num = (i // parallel_hashtag_batches) + 1
            total_batches = (total_hashtags + parallel_hashtag_batches - 1) // parallel_hashtag_batches
            
            bt.logging.info(f"\n=== BATCH {batch_num}/{total_batches} ===")
            bt.logging.info(f"Scraping {len(batch)} hashtags in parallel: {batch}")
            
            try:
                # Scrape all hashtags in batch SIMULTANEOUSLY
                hashtag_results = await self.scrape_multiple_hashtags_parallel(
                    batch, 
                    limit_per_hashtag=1000
                )
                
                # Store results for each hashtag
                for hashtag, entities in hashtag_results.items():
                    stored = self.store_entities(entities)
                    
                    self.stats['scraped'] += len(entities)
                    self.stats['stored'] += stored
                    self.stats['hashtags_processed'] += 1
                    
                    bt.logging.info(f"{hashtag}: {len(entities)} scraped, {stored} stored")
                
                # Progress update
                bt.logging.success(
                    f"\nProgress: {self.stats['hashtags_processed']}/{total_hashtags} hashtags\n"
                    f"Total scraped: {self.stats['scraped']:,}\n"
                    f"Total stored: {self.stats['stored']:,}\n"
                    f"Errors: {self.stats['errors']}"
                )
                
                # Small delay between batches
                await asyncio.sleep(2)
                
            except KeyboardInterrupt:
                bt.logging.warning("Interrupted by user")
                break
            except Exception as e:
                bt.logging.error(f"Error processing batch: {e}")
                self.stats['errors'] += 1
                continue
        
        bt.logging.success(
            f"\n=== FINAL RESULTS ===\n"
            f"Hashtags processed: {self.stats['hashtags_processed']}/{total_hashtags}\n"
            f"Total scraped: {self.stats['scraped']:,}\n"
            f"Total stored: {self.stats['stored']:,}\n"
            f"Errors: {self.stats['errors']}"
        )


async def main():
    """Main entry point with configurable parallelism."""
    bt.logging.set_trace(True)
    
    # Configure for high-volume parallel scraping
    manager = ParallelTwitterWorkerManager(
        db_path="twitter_accounts.db",
        max_concurrent_tweets=100,      # 100 tweets processed simultaneously
        max_concurrent_replies=50,      # 50 reply fetches at once
        batch_size=100,                 # Batch size for processing
        worker_count=50,                # 50 worker threads
        api_rate_limit=500              # 500 concurrent API calls max
    )
    
    # Run with 5 hashtags scraped in parallel at a time
    await manager.run(parallel_hashtag_batches=5)


if __name__ == "__main__":
    asyncio.run(main())
