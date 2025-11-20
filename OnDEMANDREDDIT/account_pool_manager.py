#!/usr/bin/env python3
"""
account_pool_manager.py

Manages pool of Reddit accounts with:
✅ Round-robin assignment
✅ Health tracking
✅ Auto-failover
✅ Rate limit awareness
✅ Forbidden account detection
"""

import asyncio
import logging
from typing import List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class RedditAccount:
    """Reddit account with tracking"""
    client_id: str
    client_secret: str
    username: str
    password: str
    proxy_url: str
    
    # Tracking fields
    status: str = "healthy"  # healthy, rate_limited, forbidden, error
    requests_made: int = 0
    errors: int = 0
    last_used: Optional[datetime] = None
    rate_limit_remaining: Optional[float] = None
    rate_limit_reset: Optional[datetime] = None
    forbidden_at: Optional[datetime] = None
    rate_limited_at: Optional[datetime] = None
    
    def is_healthy(self) -> bool:
        """Check if account is healthy and usable"""
        if self.status == "forbidden":
            return False
        
        if self.status == "rate_limited":
            # Check if rate limit has expired
            if self.rate_limited_at:
                elapsed = (datetime.utcnow() - self.rate_limited_at).total_seconds()
                if elapsed > 600:  # 10 minutes
                    self.status = "healthy"
                    return True
            return False
        
        # Too many errors
        if self.errors > 5:
            return False
        
        return self.status == "healthy"
    
    def mark_forbidden(self):
        """Mark account as forbidden/banned"""
        self.status = "forbidden"
        self.forbidden_at = datetime.utcnow()
        logger.error(f"Account {self.username} marked as FORBIDDEN")
    
    def mark_rate_limited(self):
        """Mark account as rate limited"""
        self.status = "rate_limited"
        self.rate_limited_at = datetime.utcnow()
        logger.warning(f"Account {self.username} rate limited")
    
    def update_rate_limit(self, remaining: float, reset_seconds: float):
        """Update rate limit info"""
        self.rate_limit_remaining = remaining
        self.rate_limit_reset = datetime.utcnow() + timedelta(seconds=reset_seconds)


class AccountPoolManager:
    """Manages pool of Reddit accounts"""
    
    def __init__(self, accounts_file: str = "envActive"):
        self.accounts_file = accounts_file
        self.accounts: List[RedditAccount] = []
        self.current_index = 0
        self.lock = asyncio.Lock()
    
    async def load_accounts(self) -> int:
        """Load accounts from envActive file"""
        accounts = []
        
        try:
            with open(self.accounts_file, 'r') as f:
                lines = f.readlines()
            
            current_account = {}
            for line in lines:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split('\t')
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip().strip('"')
                    
                    # Skip PostgreSQL config
                    if key.startswith('POSTGRES_'):
                        continue
                    
                    if key in ['client_id', 'client_secret', 'username', 'password', 'proxy_url']:
                        current_account[key] = value
                        
                        # Complete account
                        if len(current_account) == 5:
                            accounts.append(RedditAccount(**current_account))
                            current_account = {}
            
            async with self.lock:
                self.accounts = accounts
                self.current_index = 0
            
            logger.info(f"[POOL] Loaded {len(accounts)} accounts from {self.accounts_file}")
            return len(accounts)
            
        except FileNotFoundError:
            logger.error(f"[POOL] {self.accounts_file} not found")
            return 0
        except Exception as e:
            logger.error(f"[POOL] Error loading accounts: {e}")
            return 0
    
    def get_healthy_accounts(self) -> List[RedditAccount]:
        """Get list of healthy accounts"""
        return [acc for acc in self.accounts if acc.is_healthy()]
    
    def get_next_account(self) -> Optional[RedditAccount]:
        """Get next healthy account (round-robin)"""
        healthy = self.get_healthy_accounts()
        
        if not healthy:
            logger.error("[POOL] No healthy accounts available!")
            return None
        
        # Round-robin selection
        account = healthy[self.current_index % len(healthy)]
        self.current_index += 1
        
        return account
    
    async def cleanup(self):
        """Cleanup resources"""
        logger.info("[POOL] Cleaning up account pool...")