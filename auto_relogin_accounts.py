"""
Automatic Account Re-login Script for Twitter Scraper
Monitors inactive accounts and attempts re-login with cookie-based authentication

Usage:
    python auto_relogin_accounts.py
"""

import asyncio
import sys
sys.path.append('/root/Algo-test-script/twscrape')

from twscrape import API, AccountsPool
from twscrape.login import LoginConfig
import bittensor as bt


class AccountRecovery:
    """Handles automatic re-authentication of failed accounts."""
    
    def __init__(self, db_path="twitter_accounts.db"):
        self.db_path = db_path
        login_config = LoginConfig(email_first=False, manual=False)
        self.pool = AccountsPool(db_path, login_config=login_config)
    
    async def check_inactive_accounts(self):
        """Get list of inactive accounts."""
        accounts = await self.pool.get_all()
        inactive = [acc for acc in accounts if not acc.active]
        return inactive
    
    async def relogin_account(self, account):
        """
        Attempt to re-login a single account.
        Since your accounts have cookies, this will use cookie-based auth.
        """
        try:
            bt.logging.info(f"Attempting re-login for {account.username}")
            
            # If account has cookies, mark it active directly
            if "ct0" in account.cookies and "auth_token" in account.cookies:
                account.active = True
                account.error_msg = None
                await self.pool.save(account)
                bt.logging.success(f"✓ Reactivated {account.username} (cookie-based)")
                return True
            else:
                # Need full login
                success = await self.pool.login(account)
                if success:
                    bt.logging.success(f"✓ Re-logged in {account.username}")
                else:
                    bt.logging.error(f"✗ Failed to re-login {account.username}")
                return success
                
        except Exception as e:
            bt.logging.error(f"✗ Error re-logging {account.username}: {e}")
            return False
    
    async def relogin_all_inactive(self):
        """Re-login all inactive accounts."""
        bt.logging.info("Checking for inactive accounts...")
        
        inactive = await self.check_inactive_accounts()
        
        if not inactive:
            bt.logging.info("✓ No inactive accounts found. All accounts are active!")
            return
        
        bt.logging.warning(f"Found {len(inactive)} inactive account(s)")
        
        for acc in inactive:
            error_msg = acc.error_msg if acc.error_msg else "Unknown error"
            bt.logging.info(f"  - {acc.username}: {error_msg}")
        
        bt.logging.info("\nAttempting to re-login inactive accounts...")
        
        success_count = 0
        failed_count = 0
        
        for account in inactive:
            result = await self.relogin_account(account)
            if result:
                success_count += 1
            else:
                failed_count += 1
            
            # Small delay between logins
            await asyncio.sleep(2)
        
        bt.logging.info("\n" + "="*60)
        bt.logging.success(f"Re-login Complete!")
        bt.logging.info(f"✓ Success: {success_count}")
        bt.logging.info(f"✗ Failed: {failed_count}")
        bt.logging.info("="*60)
    
    async def show_account_status(self):
        """Display current account status."""
        accounts = await self.pool.get_all()
        
        active = [acc for acc in accounts if acc.active]
        inactive = [acc for acc in accounts if not acc.active]
        
        print("\n" + "="*60)
        print("ACCOUNT STATUS")
        print("="*60)
        print(f"Total Accounts: {len(accounts)}")
        print(f"Active: {len(active)}")
        print(f"Inactive: {len(inactive)}")
        
        if inactive:
            print("\nInactive Accounts:")
            for acc in inactive:
                error = acc.error_msg if acc.error_msg else "Unknown"
                print(f"  - {acc.username}: {error}")
        
        print("="*60 + "\n")


async def main():
    bt.logging.set_trace(True)
    
    recovery = AccountRecovery()
    
    # Show current status
    await recovery.show_account_status()
    
    # Attempt re-login for inactive accounts
    await recovery.relogin_all_inactive()
    
    # Show final status
    await recovery.show_account_status()


if __name__ == "__main__":
    asyncio.run(main())
