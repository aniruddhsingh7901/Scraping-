#!/usr/bin/env python3
"""
Manual CSRF Error Fix Script with Proxy Rotation
Run this script when you encounter CSRF errors (403, error 353)
It will re-login all accounts using rotating proxies to avoid detection.
"""

import asyncio
import sys
import random

sys.path.append('/root/Algo-test-script/twscrape')

from twscrape import API
from twscrape.login import login, LoginConfig
from twscrape.logger import logger


def load_proxies():
    """Load proxies from file."""
    try:
        with open('/root/Algo-test-script/proxies.txt', 'r') as f:
            proxies = []
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Format: host:port:user:pass
                    parts = line.split(':')
                    if len(parts) == 4:
                        host, port, user, password = parts
                        proxy = f"socks5://{user}:{password}@{host}:{port}"
                        proxies.append(proxy)
            print(f"Loaded {len(proxies)} proxies")
            return proxies
    except Exception as e:
        print(f"Error loading proxies: {e}")
        return []


async def fix_csrf_errors_with_proxy_rotation():
    """Re-login all accounts with rotating proxies to avoid detection."""
    print("=" * 60)
    print("CSRF Error Fix Tool - With Proxy Rotation")
    print("=" * 60)
    
    # Load proxies
    proxies = load_proxies()
    if not proxies:
        print("No proxies found! Cannot proceed without proxies.")
        return
    
    # Shuffle proxies for random rotation
    random.shuffle(proxies)
    
    api = API(pool="twitter_accounts.db")
    
    try:
        # Get all accounts
        accounts = await api.pool.get_all()
        print(f"\nFound {len(accounts)} accounts")
        
        if not accounts:
            print("No accounts found in database!")
            return
        
        # Show account status
        print("\nAccount Status:")
        for acc in accounts:
            status = "✓ Active" if acc.active else "✗ Inactive"
            error = f" - Error: {acc.error_msg[:50]}..." if acc.error_msg else ""
            print(f"  {acc.username}: {status}{error}")
        
        # Confirm action
        print(f"\nThis will re-login all {len(accounts)} accounts using {len(proxies)} rotating proxies.")
        print("Each account will use a different proxy to avoid detection.")
        response = input("Continue? (y/n): ").strip().lower()
        
        if response != 'y':
            print("Cancelled.")
            return
        
        print("\nStarting re-login process with proxy rotation...\n")
        
        success_count = 0
        failed_count = 0
        proxy_index = 0
        
        for i, account in enumerate(accounts, 1):
            try:
                # Rotate to next proxy
                new_proxy = proxies[proxy_index % len(proxies)]
                proxy_index += 1
                
                print(f"[{i}/{len(accounts)}] Re-logging in: {account.username}")
                print(f"  Using proxy: {new_proxy.split('@')[1] if '@' in new_proxy else new_proxy}")
                
                # Update account with new proxy
                account.proxy = new_proxy
                account.active = False
                account.error_msg = None
                await api.pool.save(account)
                
                # Perform fresh login with the new proxy
                login_config = LoginConfig(email_first=False, manual=False)
                await login(account, cfg=login_config)
                
                # Save refreshed account
                await api.pool.save(account)
                
                print(f"  ✓ Success: {account.username}")
                success_count += 1
                
                # Random delay between logins (3-7 seconds) to look more human
                if i < len(accounts):
                    delay = random.uniform(3, 7)
                    print(f"  Waiting {delay:.1f}s before next login...")
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                failed_count += 1
                error_str = str(e)
                if '403' in error_str or '401' in error_str:
                    print(f"  ✗ Failed: {account.username} - Auth error (may need manual verification)")
                else:
                    print(f"  ✗ Failed: {account.username} - {error_str[:80]}")
                
                # Even on failure, wait a bit before trying next account
                if i < len(accounts):
                    await asyncio.sleep(random.uniform(2, 4))
                continue
        
        print("\n" + "=" * 60)
        print(f"Re-login Complete:")
        print(f"  ✓ Successful: {success_count}")
        print(f"  ✗ Failed: {failed_count}")
        print(f"  Proxies rotated: {min(proxy_index, len(proxies))} different proxies used")
        print("=" * 60)
        
        if success_count > 0:
            print("\n✓ Accounts have been refreshed with new proxies.")
            print("  You can now restart your scraper.")
        else:
            print("\n✗ All re-logins failed.")
            print("  Possible reasons:")
            print("  - Accounts may be suspended/locked")
            print("  - Proxies may be blocked by Twitter")
            print("  - IP/proxy reputation issues")
            
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()


async def show_account_status():
    """Show current account status."""
    api = API(pool="twitter_accounts.db")
    
    try:
        accounts = await api.pool.get_all()
        
        print("\n" + "=" * 60)
        print("Account Status Report")
        print("=" * 60)
        
        if not accounts:
            print("No accounts found in database!")
            return
        
        active = [acc for acc in accounts if acc.active]
        inactive = [acc for acc in accounts if not acc.active]
        
        print(f"\nTotal Accounts: {len(accounts)}")
        print(f"Active: {len(active)}")
        print(f"Inactive: {len(inactive)}")
        
        print("\nDetailed Status:")
        for acc in accounts:
            status = "✓ Active" if acc.active else "✗ Inactive"
            last_used = f" (Last used: {acc.last_used})" if acc.last_used else ""
            proxy_info = f" Proxy: {acc.proxy.split('@')[1] if acc.proxy and '@' in acc.proxy else 'None'}"
            error = f"\n    Error: {acc.error_msg}" if acc.error_msg else ""
            print(f"  {acc.username}: {status}{last_used}{proxy_info}{error}")
        
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError: {e}")


async def main():
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        await show_account_status()
    else:
        await fix_csrf_errors_with_proxy_rotation()


if __name__ == "__main__":
    print("\nTwitter Account CSRF Fix Tool - With Proxy Rotation\n")
    asyncio.run(main())
