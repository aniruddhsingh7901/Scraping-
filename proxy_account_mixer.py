"""
Proxy-Account Mixer for Twitter Scraping
Automatically assigns and rotates proxies across accounts

Features:
- Loads 5000+ proxies from separate file
- Assigns proxies to accounts (round-robin or random)
- Updates account pool with proxy assignments
- Supports re-mixing for proxy rotation
"""

import asyncio
import random
import sys
sys.path.append('/root/Algo-test-script/twscrape')

from twscrape import API
import bittensor as bt


class ProxyAccountMixer:
    """
    Manages proxy assignment and rotation for Twitter accounts.
    
    twscrape DOES NOT automatically rotate proxies itself.
    - Each account has ONE proxy assigned
    - twscrape rotates ACCOUNTS (which indirectly rotates proxies)
    - This script helps manage proxy assignments
    """
    
    def __init__(self, db_path: str = "twitter_accounts.db"):
        self.db_path = db_path
        self.api = API(pool=db_path)
        self.proxies = []
        self.accounts_data = []
        
    async def load_proxies(self, proxy_file: str):
        """
        Load proxies from file.
        
        Supports multiple formats (one per line):
        - socks5://user:pass@ip:port
        - http://user:pass@ip:port
        - ip:port (converted to socks5)
        - ip:port:username:password (YOUR FORMAT - auto-converted)
        """
        try:
            with open(proxy_file, 'r') as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            self.proxies = []
            for line in lines:
                # Support different formats
                if line.startswith('socks') or line.startswith('http'):
                    # Already in correct format
                    self.proxies.append(line)
                else:
                    # Count colons to determine format
                    parts = line.split(':')
                    
                    if len(parts) == 4:
                        # Format: IP:PORT:USERNAME:PASSWORD
                        ip, port, username, password = parts
                        proxy_url = f"socks5://{username}:{password}@{ip}:{port}"
                        self.proxies.append(proxy_url)
                    elif len(parts) == 2:
                        # Format: IP:PORT (no auth)
                        self.proxies.append(f"socks5://{line}")
                    else:
                        bt.logging.warning(f"Skipping invalid proxy format: {line}")
            
            bt.logging.info(f"Loaded {len(self.proxies)} proxies from {proxy_file}")
            return True
            
        except FileNotFoundError:
            bt.logging.error(f"Proxy file not found: {proxy_file}")
            return False
        except Exception as e:
            bt.logging.error(f"Error loading proxies: {e}")
            return False
    
    async def load_accounts(self, accounts_file: str):
        """
        Load account data from file.
        
        Format (one per line):
        username:password:email:email_password:ct0_token:auth_token
        """
        try:
            with open(accounts_file, 'r') as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            self.accounts_data = []
            for line in lines:
                parts = line.split(':')
                if len(parts) >= 6:
                    account = {
                        'username': parts[0].strip(),
                        'password': parts[1].strip(),
                        'email': parts[2].strip(),
                        'email_password': parts[3].strip(),
                        'ct0': parts[4].strip() if parts[4].strip() else None,
                        'auth_token': parts[5].strip() if parts[5].strip() else None,
                    }
                    self.accounts_data.append(account)
            
            bt.logging.info(f"Loaded {len(self.accounts_data)} accounts from {accounts_file}")
            return True
            
        except FileNotFoundError:
            bt.logging.error(f"Accounts file not found: {accounts_file}")
            return False
        except Exception as e:
            bt.logging.error(f"Error loading accounts: {e}")
            return False
    
    async def mix_and_assign(self, strategy: str = "round-robin"):
        """
        Mix accounts with proxies and add to account pool.
        
        Strategies:
        - round-robin: Assign proxies sequentially
        - random: Assign proxies randomly
        - balanced: Distribute proxies evenly across accounts
        
        Args:
            strategy: Assignment strategy
        """
        if not self.proxies:
            bt.logging.error("No proxies loaded!")
            return False
        
        if not self.accounts_data:
            bt.logging.error("No accounts loaded!")
            return False
        
        bt.logging.info(f"Mixing {len(self.accounts_data)} accounts with {len(self.proxies)} proxies...")
        bt.logging.info(f"Strategy: {strategy}")
        
        successful = 0
        failed = 0
        
        for i, account in enumerate(self.accounts_data):
            try:
                # Select proxy based on strategy
                if strategy == "round-robin":
                    proxy = self.proxies[i % len(self.proxies)]
                elif strategy == "random":
                    proxy = random.choice(self.proxies)
                elif strategy == "balanced":
                    # Distribute evenly
                    proxy_index = int((i / len(self.accounts_data)) * len(self.proxies))
                    proxy = self.proxies[proxy_index]
                else:
                    proxy = self.proxies[i % len(self.proxies)]
                
                # Build cookies string if tokens provided
                cookies = None
                if account['ct0'] and account['auth_token']:
                    cookies = f"ct0={account['ct0']}; auth_token={account['auth_token']}"
                
                # Add account to pool
                await self.api.pool.add_account(
                    username=account['username'],
                    password=account['password'],
                    email=account['email'],
                    email_password=account['email_password'],
                    proxy=proxy,
                    cookies=cookies
                )
                
                successful += 1
                if (i + 1) % 10 == 0:
                    bt.logging.info(f"Progress: {i+1}/{len(self.accounts_data)} accounts processed")
                
            except Exception as e:
                bt.logging.error(f"Failed to add account {account['username']}: {e}")
                failed += 1
        
        bt.logging.success(f"\n{'='*60}")
        bt.logging.success(f"Mixing Complete!")
        bt.logging.success(f"{'='*60}")
        bt.logging.success(f"Successful: {successful}")
        bt.logging.success(f"Failed: {failed}")
        bt.logging.success(f"Total Proxies: {len(self.proxies)}")
        bt.logging.success(f"Proxy:Account Ratio: {len(self.proxies)/len(self.accounts_data):.1f}:1")
        
        return successful > 0
    
    async def update_account_proxies(self, strategy: str = "random"):
        """
        Update existing accounts with new proxy assignments.
        Useful for proxy rotation without re-adding accounts.
        
        Args:
            strategy: Assignment strategy (random recommended for rotation)
        """
        if not self.proxies:
            bt.logging.error("No proxies loaded!")
            return False
        
        bt.logging.info("Updating account proxies...")
        
        # Get all existing accounts
        accounts = await self.api.pool.get_all()
        
        updated = 0
        for account in accounts:
            try:
                # Select new proxy
                if strategy == "random":
                    new_proxy = random.choice(self.proxies)
                else:
                    new_proxy = self.proxies[updated % len(self.proxies)]
                
                # Update proxy
                account.proxy = new_proxy
                await self.api.pool.save(account)
                
                updated += 1
                
            except Exception as e:
                bt.logging.error(f"Failed to update {account.username}: {e}")
        
        bt.logging.success(f"Updated proxies for {updated} accounts")
        return updated > 0
    
    async def show_proxy_distribution(self):
        """Show current proxy distribution across accounts."""
        accounts = await self.api.pool.get_all()
        
        proxy_counts = {}
        no_proxy = 0
        
        for account in accounts:
            if account.proxy:
                proxy_counts[account.proxy] = proxy_counts.get(account.proxy, 0) + 1
            else:
                no_proxy += 1
        
        print(f"\n{'='*80}")
        print(f"PROXY DISTRIBUTION")
        print(f"{'='*80}")
        print(f"Total Accounts: {len(accounts)}")
        print(f"Accounts with Proxy: {len(accounts) - no_proxy}")
        print(f"Accounts without Proxy: {no_proxy}")
        print(f"Unique Proxies: {len(proxy_counts)}")
        print(f"\nTop 10 Most Used Proxies:")
        print(f"{'-'*80}")
        
        sorted_proxies = sorted(proxy_counts.items(), key=lambda x: x[1], reverse=True)
        for proxy, count in sorted_proxies[:10]:
            # Mask proxy for security
            masked = proxy[:20] + "..." + proxy[-10:] if len(proxy) > 30 else proxy
            print(f"{masked:<40} {count} accounts")
        
        print(f"{'='*80}\n")


async def main():
    """Main entry point."""
    import sys
    
    bt.logging.set_trace(True)
    
    if len(sys.argv) < 2:
        print("""
Proxy-Account Mixer
===================

Commands:
  mix <accounts_file> <proxy_file> [strategy]    Mix accounts with proxies and add to pool
  rotate <proxy_file> [strategy]                 Rotate proxies for existing accounts
  stats                                          Show proxy distribution
  
Strategies:
  round-robin    Assign proxies sequentially (default)
  random         Assign proxies randomly
  balanced       Distribute proxies evenly
  
File Formats:
  accounts_file: username:password:email:email_password:ct0:auth_token
  proxy_file:    socks5://user:pass@ip:port (one per line)
  
Examples:
  python proxy_account_mixer.py mix accounts.txt proxies.txt
  python proxy_account_mixer.py mix accounts.txt proxies.txt random
  python proxy_account_mixer.py rotate proxies.txt random
  python proxy_account_mixer.py stats
        """)
        return
    
    command = sys.argv[1].lower()
    mixer = ProxyAccountMixer()
    
    if command == "mix":
        if len(sys.argv) < 4:
            print("Error: Missing required arguments")
            print("Usage: python proxy_account_mixer.py mix <accounts_file> <proxy_file> [strategy]")
            return
        
        accounts_file = sys.argv[2]
        proxy_file = sys.argv[3]
        strategy = sys.argv[4] if len(sys.argv) > 4 else "round-robin"
        
        if await mixer.load_accounts(accounts_file):
            if await mixer.load_proxies(proxy_file):
                await mixer.mix_and_assign(strategy)
    
    elif command == "rotate":
        if len(sys.argv) < 3:
            print("Error: Missing proxy file")
            print("Usage: python proxy_account_mixer.py rotate <proxy_file> [strategy]")
            return
        
        proxy_file = sys.argv[2]
        strategy = sys.argv[3] if len(sys.argv) > 3 else "random"
        
        if await mixer.load_proxies(proxy_file):
            await mixer.update_account_proxies(strategy)
    
    elif command == "stats":
        await mixer.show_proxy_distribution()
    
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())
