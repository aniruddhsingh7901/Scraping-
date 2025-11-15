#!/usr/bin/env python3
"""
stream_all_worker_manager.py

Manages workers for streaming r/all (all of Reddit):
✅ Allocates FREE accounts from envActive (not used by main worker_manager.py)
✅ Spawns 2-4 workers running stream_all_reddit_dataentity.py
✅ Each worker streams posts AND comments from r/all
✅ Stores data in same PostgreSQL database as DataEntity format
✅ Automatic failure recovery and account rotation
✅ Independent from subreddit-specific scrapers
"""

import os
import json
import asyncio
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
import signal
import sys

# Configuration
ACCOUNTS_FILE = "envActive"
TRACKING_FILE = "stream_all_tracking.json"
WORKER_TRACKING_FILE = "worker_tracking.json"  # Main worker manager tracking
PROXY_FILE = "proxy.txt"
NUM_STREAM_WORKERS = 1  # Number of workers to allocate for streaming r/all (1 account as per allocation)
HEALTH_CHECK_INTERVAL = 30  # Check worker health every 30 seconds
FORBIDDEN_ERROR_KEYWORDS = ["403", "forbidden", "suspended", "banned", "401"]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('stream_all_worker_manager.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Account:
    """Reddit account with credentials"""
    client_id: str
    client_secret: str
    username: str
    password: str
    proxy_url: str
    
    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass
class WorkerStatus:
    """Worker status tracking"""
    worker_id: str
    account_username: str
    status: str  # running, failed, stopped
    start_time: float
    last_health_check: float
    process_pid: Optional[int]
    total_runtime: float
    errors_count: int
    last_error: Optional[str]
    
    def to_dict(self) -> Dict:
        return {
            "worker_id": self.worker_id,
            "account_username": self.account_username,
            "status": self.status,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "uptime_minutes": round((time.time() - self.start_time) / 60, 2),
            "last_health_check": datetime.fromtimestamp(self.last_health_check).isoformat(),
            "process_pid": self.process_pid,
            "total_runtime_minutes": round(self.total_runtime / 60, 2),
            "errors_count": self.errors_count,
            "last_error": self.last_error
        }


class StreamWorker:
    """Represents a streaming worker process"""
    
    def __init__(self, worker_id: str, account: Account):
        self.worker_id = worker_id
        self.account = account
        self.process: Optional[asyncio.subprocess.Process] = None
        self.status = WorkerStatus(
            worker_id=worker_id,
            account_username=account.username,
            status="initialized",
            start_time=time.time(),
            last_health_check=time.time(),
            process_pid=None,
            total_runtime=0,
            errors_count=0,
            last_error=None
        )
        self.work_start_time = time.time()
    
    def _load_postgres_config(self) -> Dict[str, str]:
        """Load PostgreSQL configuration from envActive file"""
        config = {}
        try:
            with open(ACCOUNTS_FILE, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split('\t')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip().strip('"')
                        if key.startswith('POSTGRES_'):
                            config[key] = value
        except Exception as e:
            logger.warning(f"[WORKER {self.worker_id}] Could not load PostgreSQL config: {e}")
        return config
    
    async def start(self) -> bool:
        """Start the worker process"""
        try:
            # Load PostgreSQL credentials from envActive
            postgres_config = self._load_postgres_config()
            
            # Create environment variables for this worker
            env = os.environ.copy()
            env.update({
                "REDDIT_CLIENT_ID": self.account.client_id,
                "REDDIT_CLIENT_SECRET": self.account.client_secret,
                "REDDIT_USERNAME": self.account.username,
                "REDDIT_PASSWORD": self.account.password,
                "PROXY": self.account.proxy_url,  # Already in host:port:user:pass format
                "WORKER_ID": self.worker_id,
                # PostgreSQL credentials
                "POSTGRES_DB": postgres_config.get("POSTGRES_DB", "reddit_miner_db"),
                "POSTGRES_USER": postgres_config.get("POSTGRES_USER", "reddit_user"),
                "POSTGRES_PASSWORD": postgres_config.get("POSTGRES_PASSWORD", "postgres"),
                "POSTGRES_HOST": postgres_config.get("POSTGRES_HOST", "localhost"),
                "POSTGRES_PORT": postgres_config.get("POSTGRES_PORT", "5432")
            })
            
            # Start the stream_all scraper process
            # Note: Using DEVNULL instead of PIPE to prevent buffer overflow causing process death
            self.process = await asyncio.create_subprocess_exec(
                sys.executable, "stream_all_reddit_dataentity.py",
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            
            self.status.process_pid = self.process.pid
            self.status.status = "running"
            self.status.last_health_check = time.time()
            
            logger.info(
                f"[WORKER {self.worker_id}] Started with account {self.account.username}, "
                f"PID {self.process.pid}, streaming r/all (posts + comments)"
            )
            return True
            
        except Exception as e:
            self.status.status = "failed"
            self.status.errors_count += 1
            self.status.last_error = str(e)
            logger.error(f"[WORKER {self.worker_id}] Failed to start: {e}")
            return False
    
    async def stop(self):
        """Stop the worker process gracefully"""
        if self.process and self.process.returncode is None:
            try:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), timeout=10)
                logger.info(f"[WORKER {self.worker_id}] Stopped gracefully")
            except asyncio.TimeoutError:
                self.process.kill()
                logger.warning(f"[WORKER {self.worker_id}] Force killed after timeout")
            except Exception as e:
                logger.error(f"[WORKER {self.worker_id}] Error stopping: {e}")
        
        self.status.status = "stopped"
        self.status.total_runtime += time.time() - self.work_start_time
    
    async def is_healthy(self) -> bool:
        """Check if worker process is still running"""
        if not self.process:
            return False
        
        if self.process.returncode is not None:
            # Process has exited
            self.status.status = "failed"
            self.status.errors_count += 1
            self.status.last_error = f"Process exited with code {self.process.returncode}"
            logger.error(f"[WORKER {self.worker_id}] Process died with code {self.process.returncode}")
            return False
        
        self.status.last_health_check = time.time()
        return True
    
    async def restart(self, new_account: Optional[Account] = None):
        """Restart worker with same or new account"""
        logger.info(f"[WORKER {self.worker_id}] Restarting...")
        await self.stop()
        
        if new_account:
            old_username = self.account.username
            self.account = new_account
            self.status.account_username = new_account.username
            logger.info(f"[WORKER {self.worker_id}] Replaced account {old_username} → {new_account.username}")
        
        self.work_start_time = time.time()
        return await self.start()


class StreamAllWorkerManager:
    """Manages stream_all workers"""
    
    def __init__(self):
        self.workers: Dict[str, StreamWorker] = {}
        self.available_accounts: List[Account] = []
        self.used_accounts: List[Account] = []
        self.available_proxies: List[str] = []
        self.shutdown_flag = False
        self.forbidden_accounts: List[str] = []
        self.tracking_data = {
            "manager_start_time": datetime.now().isoformat(),
            "total_workers_spawned": 0,
            "total_replacements": 0,
            "forbidden_accounts_count": 0
        }
    
    def load_proxies(self) -> List[str]:
        """Load proxies from proxy.txt file"""
        import random
        proxies = []
        try:
            with open(PROXY_FILE, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    # Format: host:port:user:pass - keep original format
                    if line.count(':') == 3:
                        proxies.append(line)
            logger.info(f"[PROXY] Loaded {len(proxies)} proxies from {PROXY_FILE}")
            return proxies
        except FileNotFoundError:
            logger.error(f"[PROXY] {PROXY_FILE} not found")
            return []
        except Exception as e:
            logger.error(f"[PROXY] Error loading proxies: {e}")
            return []
    
    def assign_random_proxy_to_account(self, account: Account) -> Account:
        """Assign a random proxy to an account"""
        import random
        if self.available_proxies:
            account.proxy_url = random.choice(self.available_proxies)
        return account
    
    def check_for_forbidden_error(self, error_msg: str) -> bool:
        """Check if error message indicates forbidden/suspended account"""
        if not error_msg:
            return False
        error_lower = error_msg.lower()
        return any(keyword in error_lower for keyword in FORBIDDEN_ERROR_KEYWORDS)
    
    def load_all_accounts(self) -> List[Account]:
        """Load all accounts from envActive file"""
        accounts = []
        
        try:
            with open(ACCOUNTS_FILE, 'r') as f:
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
                    
                    # Skip PostgreSQL configuration lines
                    if key.startswith('POSTGRES_'):
                        continue
                    
                    # Only collect Reddit account credentials
                    if key in ['client_id', 'client_secret', 'username', 'password', 'proxy_url']:
                        current_account[key] = value
                        
                        # Check if we have a complete account
                        if len(current_account) == 5:  # All fields present
                            accounts.append(Account(**current_account))
                            current_account = {}
            
            logger.info(f"[CONFIG] Loaded {len(accounts)} total accounts from {ACCOUNTS_FILE}")
            return accounts
            
        except FileNotFoundError:
            logger.error(f"[CONFIG] {ACCOUNTS_FILE} not found")
            return []
        except Exception as e:
            logger.error(f"[CONFIG] Error loading accounts: {e}")
            return []
    
    def get_used_accounts_by_main_manager(self) -> List[str]:
        """Get list of account usernames used by main worker_manager.py"""
        try:
            if os.path.exists(WORKER_TRACKING_FILE):
                with open(WORKER_TRACKING_FILE, 'r') as f:
                    tracking = json.load(f)
                
                used = []
                # Get accounts from active workers
                for worker_info in tracking.get('workers', {}).values():
                    username = worker_info.get('account_username')
                    if username:
                        used.append(username)
                
                # Get reserve accounts
                for username in tracking.get('reserves', {}).get('accounts', []):
                    used.append(username)
                
                logger.info(f"[CONFIG] Main worker manager using {len(used)} accounts")
                return used
        except Exception as e:
            logger.warning(f"[CONFIG] Could not read main worker tracking: {e}")
        
        return []
    
    def get_free_accounts(self, num_needed: int) -> List[Account]:
        """Get free accounts not used by main worker manager"""
        all_accounts = self.load_all_accounts()
        used_usernames = self.get_used_accounts_by_main_manager()
        
        # Filter out used accounts
        free_accounts = [
            acc for acc in all_accounts 
            if acc.username not in used_usernames
        ]
        
        logger.info(
            f"[ACCOUNTS] Total: {len(all_accounts)}, "
            f"Used by main manager: {len(used_usernames)}, "
            f"Free: {len(free_accounts)}"
        )
        
        # Return requested number of accounts
        selected = free_accounts[:num_needed]
        remaining = free_accounts[num_needed:]
        
        return selected, remaining
    
    async def spawn_workers(self):
        """Spawn stream_all worker processes"""
        # Load proxies first
        self.available_proxies = self.load_proxies()
        
        selected_accounts, remaining_accounts = self.get_free_accounts(NUM_STREAM_WORKERS)
        
        if len(selected_accounts) < 1:
            logger.error("[ERROR] No free accounts available for stream_all workers!")
            return False
        
        # Assign random proxies to accounts
        for account in selected_accounts:
            account = self.assign_random_proxy_to_account(account)
        
        for account in remaining_accounts:
            account = self.assign_random_proxy_to_account(account)
        
        self.used_accounts = selected_accounts
        self.available_accounts = remaining_accounts
        
        logger.info(
            f"[ACCOUNTS] Allocating {len(selected_accounts)} accounts for r/all streaming, "
            f"{len(remaining_accounts)} remaining as reserves"
        )
        
        # Create and start workers
        for i, account in enumerate(selected_accounts):
            worker_id = f"stream_all_{i+1}"
            worker = StreamWorker(worker_id, account)
            
            success = await worker.start()
            if success:
                self.workers[worker_id] = worker
                self.tracking_data["total_workers_spawned"] += 1
            else:
                logger.error(f"[ERROR] Failed to start {worker_id}")
        
        logger.info(f"[MANAGER] Spawned {len(self.workers)} stream_all workers successfully")
        self.save_tracking_data()
        return True
    
    async def replace_failed_worker(self, worker_id: str, is_forbidden: bool = False) -> bool:
        """Replace a failed worker with a reserve account"""
        if worker_id not in self.workers:
            return False
        
        failed_worker = self.workers[worker_id]
        
        # If account is forbidden, add to forbidden list
        if is_forbidden:
            if failed_worker.account.username not in self.forbidden_accounts:
                self.forbidden_accounts.append(failed_worker.account.username)
                self.tracking_data["forbidden_accounts_count"] += 1
                logger.error(
                    f"[FORBIDDEN] Account {failed_worker.account.username} marked as forbidden. "
                    f"Total forbidden: {len(self.forbidden_accounts)}"
                )
        
        if not self.available_accounts:
            logger.error(f"[REPLACE] No reserve accounts available for {worker_id}")
            # Try to restart with same account
            return await self.workers[worker_id].restart()
        
        # Get a reserve account and assign random proxy
        reserve_account = self.available_accounts.pop(0)
        reserve_account = self.assign_random_proxy_to_account(reserve_account)
        
        # Add failed account back to reserves only if not forbidden
        if not is_forbidden:
            self.available_accounts.append(failed_worker.account)
        
        # Restart with new account
        success = await failed_worker.restart(reserve_account)
        
        if success:
            self.tracking_data["total_replacements"] += 1
            logger.info(
                f"[REPLACE] Successfully replaced {worker_id} with account {reserve_account.username}. "
                f"Reserves remaining: {len(self.available_accounts)}"
            )
        
        self.save_tracking_data()
        return success
    
    async def health_check_loop(self):
        """Continuously monitor worker health and detect forbidden accounts"""
        while not self.shutdown_flag:
            try:
                for worker_id, worker in list(self.workers.items()):
                    is_healthy = await worker.is_healthy()
                    
                    if not is_healthy:
                        # Check if error indicates forbidden account
                        is_forbidden = self.check_for_forbidden_error(worker.status.last_error)
                        
                        if is_forbidden:
                            logger.error(
                                f"[HEALTH] {worker_id} has FORBIDDEN error: {worker.status.last_error}"
                            )
                        else:
                            logger.warning(f"[HEALTH] {worker_id} is unhealthy, attempting replacement")
                        
                        await self.replace_failed_worker(worker_id, is_forbidden=is_forbidden)
                
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
                
            except Exception as e:
                logger.error(f"[HEALTH] Error in health check: {e}")
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
    
    def save_tracking_data(self):
        """Save tracking data to JSON file"""
        try:
            tracking = {
                "timestamp": datetime.now().isoformat(),
                "manager_info": self.tracking_data,
                "mode": "r/all streaming (posts + comments)",
                "workers": {
                    worker_id: worker.status.to_dict()
                    for worker_id, worker in self.workers.items()
                },
                "reserves": {
                    "count": len(self.available_accounts),
                    "accounts": [acc.username for acc in self.available_accounts]
                },
                "forbidden": {
                    "count": len(self.forbidden_accounts),
                    "accounts": self.forbidden_accounts
                },
                "configuration": {
                    "num_stream_workers": NUM_STREAM_WORKERS,
                    "active_workers": len(self.workers),
                    "reserve_accounts": len(self.available_accounts),
                    "available_proxies": len(self.available_proxies)
                }
            }
            
            with open(TRACKING_FILE, 'w') as f:
                json.dump(tracking, f, indent=2)
                
        except Exception as e:
            logger.error(f"[TRACKING] Error saving tracking data: {e}")
    
    async def tracking_loop(self):
        """Periodically save tracking data"""
        while not self.shutdown_flag:
            try:
                self.save_tracking_data()
                await asyncio.sleep(60)  # Save every minute
            except Exception as e:
                logger.error(f"[TRACKING] Error in tracking loop: {e}")
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("[SHUTDOWN] Initiating graceful shutdown...")
        self.shutdown_flag = True
        
        for worker_id, worker in self.workers.items():
            await worker.stop()
        
        self.save_tracking_data()
        logger.info("[SHUTDOWN] All workers stopped")
    
    async def run(self):
        """Main run loop"""
        logger.info("="*80)
        logger.info("Stream All Worker Manager Starting")
        logger.info("Mode: r/all streaming (posts + comments) → PostgreSQL DataEntity")
        logger.info("="*80)
        
        # Initial setup
        success = await self.spawn_workers()
        if not success:
            logger.error("[FATAL] Failed to spawn workers, exiting")
            return
        
        # Start management loops
        tasks = [
            asyncio.create_task(self.health_check_loop()),
            asyncio.create_task(self.tracking_loop())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("[MANAGER] Tasks cancelled, shutting down")
        except Exception as e:
            logger.error(f"[MANAGER] Fatal error: {e}")
        finally:
            await self.shutdown()


async def main():
    """Entry point"""
    manager = StreamAllWorkerManager()
    
    # Handle signals for graceful shutdown
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("[SIGNAL] Received shutdown signal")
        asyncio.create_task(manager.shutdown())
        loop.stop()
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await manager.run()
    except KeyboardInterrupt:
        logger.info("[INTERRUPT] Keyboard interrupt received")
        await manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
