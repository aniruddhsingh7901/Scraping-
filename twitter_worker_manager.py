"""
Simple Twitter Scraper
- Takes accounts from twitter_accounts.db (twscrape pool - handles rotation automatically)
- Takes hashtags from hashtags.txt  
- Scrapes sequentially
- Stores in PostgreSQL
"""

import asyncio
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone, timedelta
from contextlib import aclosing
import sys

sys.path.append('/root/Algo-test-script/twscrape')
sys.path.append('/root/Algo-test-script/data-universe')

import bittensor as bt
from twscrape import API
from twscrape.login import login, LoginConfig
from common.data import TimeBucket
from scraping.x.model import XContent


class TwitterScraper:
    """Simple sequential scraper."""
    
    def __init__(self):
        self.api = API(pool="twitter_accounts.db")  # twscrape handles account rotation
        self.postgres_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'reddit_miner_db',
            'user': 'postgres',
            'password': 'postgres'
        }
        self.hashtags = []
        self.stats = {'scraped': 0, 'stored': 0}
    
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
    
    def _convert_tweet(self, tweet):
        """Convert tweet to XContent."""
        try:
            # Check required fields
            if not (hasattr(tweet, 'user') and hasattr(tweet.user, 'username') and
                    hasattr(tweet, 'rawContent') and hasattr(tweet, 'url') and
                    hasattr(tweet, 'date')):
                return None
            
            hashtags = [f"#{tag}" for tag in (tweet.hashtags if hasattr(tweet, 'hashtags') and tweet.hashtags else [])]
            
            return XContent(
                username=f"@{tweet.user.username}",
                text=tweet.rawContent,
                url=tweet.url,
                timestamp=tweet.date,
                tweet_hashtags=hashtags,
            )
        except:
            return None
    
    async def relogin_all_accounts(self):
        """Re-login all accounts to refresh CSRF tokens and cookies."""
        try:
            # Get all accounts from the pool
            accounts = await self.api.pool.get_all()
            bt.logging.info(f"Re-logging in {len(accounts)} accounts to fix CSRF errors...")
            
            success_count = 0
            failed_count = 0
            
            for account in accounts:
                try:
                    bt.logging.info(f"Re-logging in: {account.username}")
                    
                    # Reset account to inactive state first
                    account.active = False
                    account.error_msg = None
                    await self.api.pool.save(account)
                    
                    # Perform fresh login
                    login_config = LoginConfig(email_first=False, manual=False)
                    await login(account, cfg=login_config)
                    
                    # Save the refreshed account
                    await self.api.pool.save(account)
                    
                    bt.logging.success(f"Successfully re-logged in: {account.username}")
                    success_count += 1
                    await asyncio.sleep(2)  # Delay between logins
                    
                except Exception as e:
                    failed_count += 1
                    bt.logging.error(f"Failed to re-login {account.username}: {str(e)}")
                    continue
            
            bt.logging.success(f"Re-login complete: {success_count} successful, {failed_count} failed")
            return success_count > 0
            
        except Exception as e:
            bt.logging.error(f"Error during account re-login: {str(e)}")
            return False
    
    async def scrape_hashtag(self, hashtag, limit=100, retry_count=0):
        """Scrape one hashtag with automatic retry on CSRF errors."""
        clean_tag = hashtag.lstrip('#')
        start_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
        query = f"{clean_tag} since:{start_date}"
        
        bt.logging.info(f"Scraping: {query}")
        
        entities = []
        try:
            async with aclosing(self.api.search(query, limit=limit)) as gen:
                async for tweet in gen:
                    if hasattr(tweet, 'retweetedTweet') and tweet.retweetedTweet:
                        continue
                    
                    x_content = self._convert_tweet(tweet)
                    if x_content:
                        entity = XContent.to_data_entity(content=x_content)
                        entities.append(entity)
            
            bt.logging.success(f"Scraped {len(entities)} tweets for {hashtag}")
            return entities
            
        except Exception as e:
            error_msg = str(e)
            # Check for CSRF/auth errors (403 status, error 353, csrf mentions)
            if ("353" in error_msg or "403" in error_msg or 
                "csrf" in error_msg.lower() or "matching csrf cookie" in error_msg.lower()):
                
                if retry_count < 2:
                    bt.logging.warning(f"CSRF/Auth error detected. Attempting to re-login accounts... (attempt {retry_count + 1}/2)")
                    relogin_success = await self.relogin_all_accounts()
                    
                    if relogin_success:
                        await asyncio.sleep(3)  # Wait before retry
                        bt.logging.info(f"Retrying scrape for {hashtag}")
                        return await self.scrape_hashtag(hashtag, limit, retry_count + 1)
                    else:
                        bt.logging.error(f"Re-login failed, cannot retry")
                else:
                    bt.logging.error(f"Failed after {retry_count} re-login attempts")
            else:
                bt.logging.error(f"Error: {error_msg}")
            
            return []
    
    def store_entities(self, entities):
        """Store in PostgreSQL."""
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
                except:
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
    
    async def run(self):
        """Main loop."""
        bt.logging.info("Starting scraper...")
        
        # Check account health at startup
        try:
            accounts = await self.api.pool.get_all()
            active_accounts = [acc for acc in accounts if acc.active]
            bt.logging.info(f"Found {len(accounts)} accounts ({len(active_accounts)} active)")
            
            if len(active_accounts) == 0:
                bt.logging.warning("No active accounts found. Running re-login...")
                await self.relogin_all_accounts()
        except Exception as e:
            bt.logging.warning(f"Could not check account health: {str(e)}")
        
        if not self.load_hashtags():
            return
        
        for i, hashtag in enumerate(self.hashtags, 1):
            try:
                bt.logging.info(f"[{i}/{len(self.hashtags)}] {hashtag}")
                
                entities = await self.scrape_hashtag(hashtag, limit=1000)
                stored = self.store_entities(entities)
                
                self.stats['scraped'] += len(entities)
                self.stats['stored'] += stored
                
                bt.logging.info(f"Stored: {stored}")
                
                if i % 10 == 0:
                    bt.logging.info(f"\nTotal: {self.stats['stored']:,} tweets stored\n")
                
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                break
            except Exception as e:
                bt.logging.error(f"Error: {e}")
                continue
        
        bt.logging.success(f"Done! Stored {self.stats['stored']:,} tweets")


async def main():
    bt.logging.set_trace(True)
    scraper = TwitterScraper()
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
