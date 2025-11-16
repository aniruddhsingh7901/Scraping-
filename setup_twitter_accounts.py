"""
Twitter Account Setup Script
Helps set up twscrape account pool for high-volume scraping
"""

import asyncio
import sys
sys.path.append('/root/Algo-test-script/twscrape')

from twscrape import API
import bittensor as bt


async def setup_accounts_from_file(filepath: str, db_path: str = "twitter_accounts.db"):
    """
    Load accounts from a file and add them to twscrape pool.
    
    File format (one account per line):
    username:password:email:email_password:proxy:cookies
    
    OR for cookie-only accounts (more stable):
    username:password:email:email_password::cookies
    
    Args:
        filepath: Path to accounts file
        db_path: Path to SQLite database for account storage
    """
    bt.logging.set_trace(True)
    bt.logging.info("Setting up Twitter accounts...")
    
    api = API(pool=db_path)
    
    try:
        # Read accounts file
        with open(filepath, 'r') as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        bt.logging.info(f"Found {len(lines)} accounts in {filepath}")
        
        # Add accounts
        for i, line in enumerate(lines, 1):
            try:
                parts = line.split(':')
                if len(parts) < 4:
                    bt.logging.error(f"Line {i}: Invalid format (need at least username:password:email:email_password)")
                    continue
                
                username = parts[0].strip()
                password = parts[1].strip()
                email = parts[2].strip()
                email_password = parts[3].strip()
                proxy = parts[4].strip() if len(parts) > 4 and parts[4].strip() else None
                cookies = parts[5].strip() if len(parts) > 5 and parts[5].strip() else None
                
                bt.logging.info(f"Adding account {i}/{len(lines)}: {username}")
                
                await api.pool.add_account(
                    username=username,
                    password=password,
                    email=email,
                    email_password=email_password,
                    proxy=proxy,
                    cookies=cookies
                )
                
            except Exception as e:
                bt.logging.error(f"Line {i}: Error adding account: {e}")
                continue
        
        # Show account stats
        stats = await api.pool.stats()
        bt.logging.success(f"\n{'='*60}")
        bt.logging.success(f"Account Setup Complete!")
        bt.logging.success(f"{'='*60}")
        bt.logging.success(f"Total accounts: {stats.get('total', 0)}")
        bt.logging.success(f"Active accounts: {stats.get('active', 0)}")
        bt.logging.success(f"Inactive accounts: {stats.get('inactive', 0)}")
        
        # If some accounts need login
        inactive = stats.get('inactive', 0)
        if inactive > 0:
            bt.logging.info(f"\n{inactive} accounts need to be logged in")
            bt.logging.info("Run: python setup_twitter_accounts.py login")
        
    except FileNotFoundError:
        bt.logging.error(f"File not found: {filepath}")
    except Exception as e:
        bt.logging.error(f"Setup error: {e}")


async def login_accounts(db_path: str = "twitter_accounts.db", manual: bool = False):
    """
    Login to all inactive accounts.
    
    Args:
        db_path: Path to SQLite database
        manual: If True, manually enter email verification codes
    """
    bt.logging.set_trace(True)
    bt.logging.info("Logging in to accounts...")
    
    api = API(pool=db_path)
    
    try:
        if manual:
            bt.logging.info("Manual mode: You will be prompted for email verification codes")
            # TODO: Implement manual login
            bt.logging.warning("Manual mode not fully implemented yet")
        
        result = await api.pool.login_all()
        
        bt.logging.success(f"\n{'='*60}")
        bt.logging.success(f"Login Complete!")
        bt.logging.success(f"{'='*60}")
        bt.logging.success(f"Total: {result['total']}")
        bt.logging.success(f"Success: {result['success']}")
        bt.logging.success(f"Failed: {result['failed']}")
        
    except Exception as e:
        bt.logging.error(f"Login error: {e}")


async def show_account_stats(db_path: str = "twitter_accounts.db"):
    """Show current account statistics."""
    bt.logging.set_trace(True)
    
    api = API(pool=db_path)
    
    try:
        stats = await api.pool.stats()
        accounts_info = await api.pool.accounts_info()
        
        print(f"\n{'='*80}")
        print(f"TWITTER ACCOUNT POOL STATUS")
        print(f"{'='*80}")
        print(f"Total Accounts: {stats.get('total', 0)}")
        print(f"Active Accounts: {stats.get('active', 0)}")
        print(f"Inactive Accounts: {stats.get('inactive', 0)}")
        print(f"\n{'='*80}")
        print(f"INDIVIDUAL ACCOUNT STATUS")
        print(f"{'='*80}")
        print(f"{'Username':<20} {'Active':<10} {'Logged In':<12} {'Total Req':<12} {'Last Used':<20}")
        print(f"{'-'*80}")
        
        for acc in accounts_info[:20]:  # Show first 20
            last_used = acc['last_used'].strftime('%Y-%m-%d %H:%M') if acc['last_used'] else 'Never'
            print(f"{acc['username']:<20} {str(acc['active']):<10} {str(acc['logged_in']):<12} "
                  f"{acc['total_req']:<12} {last_used:<20}")
        
        if len(accounts_info) > 20:
            print(f"... and {len(accounts_info)-20} more accounts")
        
        print(f"{'='*80}\n")
        
        # Show locked accounts by operation
        locked_ops = {k: v for k, v in stats.items() if k.startswith('locked_')}
        if locked_ops:
            print(f"LOCKED ACCOUNTS BY OPERATION")
            print(f"{'='*80}")
            for op, count in locked_ops.items():
                op_name = op.replace('locked_', '')
                print(f"{op_name:<30} {count} accounts locked")
            print(f"{'='*80}\n")
        
    except Exception as e:
        bt.logging.error(f"Error: {e}")


async def reset_locks(db_path: str = "twitter_accounts.db"):
    """Reset all account locks (use if accounts are stuck)."""
    bt.logging.set_trace(True)
    bt.logging.info("Resetting all account locks...")
    
    api = API(pool=db_path)
    
    try:
        await api.pool.reset_locks()
        bt.logging.success("All locks reset successfully")
    except Exception as e:
        bt.logging.error(f"Error: {e}")


async def test_scraping(db_path: str = "twitter_accounts.db"):
    """Test scraping with current account pool."""
    bt.logging.set_trace(True)
    bt.logging.info("Testing scraping capabilities...")
    
    api = API(pool=db_path)
    
    try:
        # Test search
        bt.logging.info("Testing search for '#bitcoin'...")
        tweets = []
        count = 0
        async for tweet in api.search("#bitcoin", limit=10):
            tweets.append(tweet)
            count += 1
            if count >= 10:
                break
        
        bt.logging.success(f"Successfully scraped {len(tweets)} tweets")
        
        if tweets:
            bt.logging.info("\nSample tweet:")
            tweet = tweets[0]
            print(f"  User: @{tweet.user.username}")
            print(f"  Date: {tweet.date}")
            print(f"  Text: {tweet.rawContent[:100]}...")
            print(f"  Likes: {tweet.likeCount}, Retweets: {tweet.retweetCount}")
        
    except Exception as e:
        bt.logging.error(f"Test failed: {e}")


def print_usage():
    """Print usage instructions."""
    print("""
Twitter Account Setup Script
============================

Commands:
  setup <file>         Add accounts from file
  login               Login to all inactive accounts
  login --manual      Login with manual email verification
  stats               Show account statistics
  test                Test scraping with current accounts
  reset-locks         Reset all account locks
  
File Format for 'setup':
  username:password:email:email_password:proxy:cookies
  
  OR for cookie-only (recommended):
  username:password:email:email_password::cookies
  
Examples:
  python setup_twitter_accounts.py setup accounts.txt
  python setup_twitter_accounts.py login
  python setup_twitter_accounts.py stats
  python setup_twitter_accounts.py test
  
Environment Variables:
  TWS_PROXY           Global proxy for all accounts
  TWS_WAIT_EMAIL_CODE Timeout for email verification (default: 30s)
    """)


async def main():
    """Main entry point."""
    import sys
    
    if len(sys.argv) < 2:
        print_usage()
        return
    
    command = sys.argv[1].lower()
    
    if command == "setup":
        if len(sys.argv) < 3:
            print("Error: Missing accounts file")
            print("Usage: python setup_twitter_accounts.py setup <accounts_file>")
            return
        await setup_accounts_from_file(sys.argv[2])
    
    elif command == "login":
        manual = "--manual" in sys.argv
        await login_accounts(manual=manual)
    
    elif command == "stats":
        await show_account_stats()
    
    elif command == "test":
        await test_scraping()
    
    elif command == "reset-locks":
        await reset_locks()
    
    else:
        print(f"Unknown command: {command}")
        print_usage()


if __name__ == "__main__":
    asyncio.run(main())
