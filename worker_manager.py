#!/usr/bin/env python3
"""
worker_manager.py

Intelligent Worker Manager for 24/7 Reddit Scraping:
✅ Monitors scraping_config.json and envActive for accounts
✅ Uses N-4 accounts (keeps 4 as reserve)
✅ Spins up N-4 workers running reddit_multi_scraper.py in parallel
✅ Distributes 1-5 subreddits per worker
✅ Real-time worker replacement on failures
✅ Job rotation at configurable intervals
✅ Scheduled rest periods for workers
✅ Comprehensive JSON tracking and logging
"""

import os
import json
import asyncio
import time
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import signal
import sys

# Configuration
CONFIG_FILE = "scraping_config.json"
ACCOUNTS_FILE = "envActive"
TRACKING_FILE = "worker_tracking.json"
PROXY_FILE = "proxy.txt"
ACCOUNTS_FOR_STREAM_ALL = 1  # Reserve 1 account for stream_all manager
ACCOUNTS_FOR_ALL_SUBS_30DAYS = 1  # Reserve 1 account for comprehensive 30-day scraper
ALLOCATION_RATIO = 2/3  # Allocate 2/3 of remaining accounts to reddit-scraper
MIN_SUBREDDITS_PER_WORKER = 1
MAX_SUBREDDITS_PER_WORKER = 5
ENABLE_ALL_SUBS_30DAYS_SCRAPER = True  # Enable/disable comprehensive scraper
WORK_DURATION_MINUTES = 60  # Work for 60 minutes
REST_DURATION_MINUTES = 5   # Rest for 5 minutes
LONG_WORK_DURATION_MINUTES = 360  # 6 hours
LONG_REST_DURATION_MINUTES = 10   # 10 minutes rest
HEALTH_CHECK_INTERVAL = 30  # Check worker health every 30 seconds
ACCOUNT_POOL_CHECK_INTERVAL = 1800  # Check account pool every 30 minutes
CONFIG_RELOAD_INTERVAL = 300  # Reload config every 5 minutes
JOB_ROTATION_INTERVAL = 3600  # Rotate jobs every 1 hour
FORBIDDEN_ERROR_KEYWORDS = ["403", "forbidden", "suspended", "banned", "401"]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('worker_manager.log'),
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
    subreddits: List[str]
    status: str  # running, resting, failed, stopped
    start_time: float
    last_health_check: float
    process_pid: Optional[int]
    total_runtime: float
    errors_count: int
    last_error: Optional[str]
    assigned_subreddits_count: int
    
    def to_dict(self) -> Dict:
        return {
            "worker_id": self.worker_id,
            "account_username": self.account_username,
            "subreddits": self.subreddits,
            "status": self.status,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "uptime_minutes": round((time.time() - self.start_time) / 60, 2),
            "last_health_check": datetime.fromtimestamp(self.last_health_check).isoformat(),
            "process_pid": self.process_pid,
            "total_runtime_minutes": round(self.total_runtime / 60, 2),
            "errors_count": self.errors_count,
            "last_error": self.last_error,
            "assigned_subreddits_count": self.assigned_subreddits_count
        }


class Worker:
    """Represents a scraping worker process"""
    
    def __init__(self, worker_id: str, account: Account, subreddits: List[str]):
        self.worker_id = worker_id
        self.account = account
        self.subreddits = subreddits
        self.process: Optional[asyncio.subprocess.Process] = None
        self.status = WorkerStatus(
            worker_id=worker_id,
            account_username=account.username,
            subreddits=subreddits,
            status="initialized",
            start_time=time.time(),
            last_health_check=time.time(),
            process_pid=None,
            total_runtime=0,
            errors_count=0,
            last_error=None,
            assigned_subreddits_count=len(subreddits)
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
                "SUBREDDITS": ",".join(self.subreddits),
                # PostgreSQL credentials
                "POSTGRES_DB": postgres_config.get("POSTGRES_DB", "reddit_miner_db"),
                "POSTGRES_USER": postgres_config.get("POSTGRES_USER", "reddit_user"),
                "POSTGRES_PASSWORD": postgres_config.get("POSTGRES_PASSWORD", "postgres"),
                "POSTGRES_HOST": postgres_config.get("POSTGRES_HOST", "localhost"),
                "POSTGRES_PORT": postgres_config.get("POSTGRES_PORT", "5432")
            })
            
            # Start the scraper process
            self.process = await asyncio.create_subprocess_exec(
                sys.executable, "reddit_scraper_dataentity.py",
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            self.status.process_pid = self.process.pid
            self.status.status = "running"
            self.status.last_health_check = time.time()
            
            logger.info(
                f"[WORKER {self.worker_id}] Started with account {self.account.username}, "
                f"PID {self.process.pid}, subreddits: {', '.join(self.subreddits)}"
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


class WorkerManager:
    """Manages all scraping workers"""
    
    def __init__(self):
        self.workers: Dict[str, Worker] = {}
        self.active_accounts: List[Account] = []
        self.reserve_accounts: List[Account] = []
        self.available_proxies: List[str] = []
        self.subreddits: List[str] = []
        self.shutdown_flag = False
        self.forbidden_accounts: List[str] = []  # Track forbidden account usernames
        self.comprehensive_scraper: Optional[Worker] = None  # Dedicated comprehensive scraper
        self.tracking_data = {
            "manager_start_time": datetime.now().isoformat(),
            "total_workers_spawned": 0,
            "total_replacements": 0,
            "total_job_rotations": 0,
            "total_rest_periods": 0,
            "current_cycle": 0,
            "forbidden_accounts_count": 0,
            "account_pool_checks": 0,
            "comprehensive_scraper_enabled": ENABLE_ALL_SUBS_30DAYS_SCRAPER
        }
    
    def load_proxies(self) -> List[str]:
        """Load proxies from proxy.txt file"""
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
        """Assign a random proxy to an account (keeps host:port:user:pass format)"""
        if self.available_proxies:
            # Store in original format: host:port:user:pass
            account.proxy_url = random.choice(self.available_proxies)
        return account
    
    def load_accounts(self) -> Tuple[List[Account], int]:
        """Load accounts from envActive file with smart 2/3 allocation"""
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
                            # Skip forbidden accounts
                            if current_account['username'] not in self.forbidden_accounts:
                                accounts.append(Account(**current_account))
                            else:
                                logger.warning(f"[CONFIG] Skipping forbidden account: {current_account['username']}")
                            current_account = {}
            
            total_loaded = len(accounts)
            
            # Reserve accounts for specialized scrapers
            reserved_accounts = ACCOUNTS_FOR_STREAM_ALL
            if ENABLE_ALL_SUBS_30DAYS_SCRAPER:
                reserved_accounts += ACCOUNTS_FOR_ALL_SUBS_30DAYS
            
            available_for_allocation = total_loaded - reserved_accounts
            
            # Allocate 2/3 of remaining accounts to reddit-scraper (this manager)
            num_for_reddit_scraper = int(available_for_allocation * ALLOCATION_RATIO)
            
            # Rest go to reserve pool for reallocation
            accounts_for_main = accounts[:num_for_reddit_scraper]
            
            logger.info(f"[CONFIG] Loaded {total_loaded} total accounts from {ACCOUNTS_FILE}")
            logger.info(f"[ALLOCATION] 2/3 allocation: {num_for_reddit_scraper} accounts for reddit-scraper")
            logger.info(f"[ALLOCATION] {ACCOUNTS_FOR_STREAM_ALL} account reserved for stream_all")
            if ENABLE_ALL_SUBS_30DAYS_SCRAPER:
                logger.info(f"[ALLOCATION] {ACCOUNTS_FOR_ALL_SUBS_30DAYS} account reserved for comprehensive 30-day scraper")
            logger.info(f"[ALLOCATION] {total_loaded - num_for_reddit_scraper - reserved_accounts} accounts in reserve pool")
            
            return accounts_for_main, len(accounts_for_main)
            
        except FileNotFoundError:
            logger.error(f"[CONFIG] {ACCOUNTS_FILE} not found")
            return [], 0
        except Exception as e:
            logger.error(f"[CONFIG] Error loading accounts: {e}")
            return [], 0
    
    def load_subreddits(self) -> List[str]:
        """Load subreddits from scraping_config.json"""
        try:
            with open(CONFIG_FILE, 'r') as f:
                subreddits = json.load(f)
            
            # Clean subreddit names (remove r/ prefix if present)
            cleaned = [s.replace('r/', '').strip() for s in subreddits]
            logger.info(f"[CONFIG] Loaded {len(cleaned)} subreddits from {CONFIG_FILE}")
            return cleaned
            
        except FileNotFoundError:
            logger.error(f"[CONFIG] {CONFIG_FILE} not found")
            return []
        except Exception as e:
            logger.error(f"[CONFIG] Error loading subreddits: {e}")
            return []
    
    def distribute_subreddits(self, subreddits: List[str], num_workers: int) -> List[List[str]]:
        """Distribute subreddits across workers (1-5 per worker)"""
        if num_workers == 0:
            return []
        
        # Shuffle for better distribution
        shuffled = subreddits.copy()
        random.shuffle(shuffled)
        
        # Calculate subreddits per worker
        total_subs = len(shuffled)
        subs_per_worker = max(MIN_SUBREDDITS_PER_WORKER, min(MAX_SUBREDDITS_PER_WORKER, total_subs // num_workers))
        
        distributions = []
        start_idx = 0
        
        for i in range(num_workers):
            # Last worker gets remaining subreddits
            if i == num_workers - 1:
                distributions.append(shuffled[start_idx:])
            else:
                end_idx = start_idx + subs_per_worker
                distributions.append(shuffled[start_idx:end_idx])
                start_idx = end_idx
        
        # Log distribution
        for i, dist in enumerate(distributions):
            logger.info(f"[DISTRIBUTION] Worker {i+1}: {len(dist)} subreddits - {', '.join(dist[:3])}{'...' if len(dist) > 3 else ''}")
        
        return distributions
    
    async def spawn_workers(self):
        """Spawn worker processes based on available accounts"""
        # Load proxies first
        self.available_proxies = self.load_proxies()
        
        all_accounts, total_count = self.load_accounts()
        
        if total_count < 2:
            logger.error(f"[ERROR] Not enough accounts! Need at least 2, have {total_count}")
            return False
        
        # Load subreddits first to determine optimal worker count
        self.subreddits = self.load_subreddits()
        if not self.subreddits:
            logger.error("[ERROR] No subreddits configured")
            return False
        
        # Calculate optimal number of workers
        num_workers = min(total_count, len(self.subreddits))
        
        # Split into active and reserve
        self.active_accounts = all_accounts[:num_workers]
        self.reserve_accounts = all_accounts[num_workers:]
        
        # Assign random proxies to active accounts
        for account in self.active_accounts:
            account = self.assign_random_proxy_to_account(account)
        
        # Assign random proxies to reserve accounts
        for account in self.reserve_accounts:
            account = self.assign_random_proxy_to_account(account)
        
        logger.info(
            f"[ACCOUNTS] Using {num_workers} workers, keeping {len(self.reserve_accounts)} in reserve "
            f"(subreddits: {len(self.subreddits)})"
        )
        
        # Distribute work
        distributions = self.distribute_subreddits(self.subreddits, num_workers)
        
        # Create and start workers
        for i, (account, subs) in enumerate(zip(self.active_accounts, distributions)):
            worker_id = f"worker_{i+1}"
            worker = Worker(worker_id, account, subs)
            
            success = await worker.start()
            if success:
                self.workers[worker_id] = worker
                self.tracking_data["total_workers_spawned"] += 1
            else:
                logger.error(f"[ERROR] Failed to start {worker_id}")
        
        logger.info(f"[MANAGER] Spawned {len(self.workers)} workers successfully")
        
        # Spawn comprehensive scraper if enabled
        if ENABLE_ALL_SUBS_30DAYS_SCRAPER:
            await self.spawn_comprehensive_scraper()
        
        self.save_tracking_data()
        self.update_envactive_status()
        return True
    
    async def spawn_comprehensive_scraper(self):
        """Spawn the comprehensive 30-day scraper with dedicated account"""
        try:
            # Get all accounts to find the dedicated one
            all_accounts_full = []
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
                    
                    if key.startswith('POSTGRES_'):
                        continue
                    
                    if key in ['client_id', 'client_secret', 'username', 'password', 'proxy_url']:
                        current_account[key] = value
                        
                        if len(current_account) == 5:
                            if current_account['username'] not in self.forbidden_accounts:
                                all_accounts_full.append(Account(**current_account))
                            current_account = {}
            
            # Get the account reserved for comprehensive scraper (last account after stream_all)
            if len(all_accounts_full) < (ACCOUNTS_FOR_STREAM_ALL + ACCOUNTS_FOR_ALL_SUBS_30DAYS):
                logger.error("[COMPREHENSIVE] Not enough accounts for comprehensive scraper")
                return False
            
            # The account is after stream_all reserve
            comp_account_index = -(ACCOUNTS_FOR_STREAM_ALL + ACCOUNTS_FOR_ALL_SUBS_30DAYS) + ACCOUNTS_FOR_STREAM_ALL
            comp_account = all_accounts_full[comp_account_index]
            
            # Assign random proxy
            comp_account = self.assign_random_proxy_to_account(comp_account)
            
            # Load PostgreSQL config
            postgres_config = {}
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
                            postgres_config[key] = value
            
            # Create environment for comprehensive scraper
            env = os.environ.copy()
            env.update({
                "REDDIT_CLIENT_ID": comp_account.client_id,
                "REDDIT_CLIENT_SECRET": comp_account.client_secret,
                "REDDIT_USERNAME": comp_account.username,
                "REDDIT_PASSWORD": comp_account.password,
                "PROXY": comp_account.proxy_url,  # Already in host:port:user:pass format
                "WORKER_ID": "comprehensive_30days",
                "POSTGRES_DB": postgres_config.get("POSTGRES_DB", "reddit_miner_db"),
                "POSTGRES_USER": postgres_config.get("POSTGRES_USER", "reddit_user"),
                "POSTGRES_PASSWORD": postgres_config.get("POSTGRES_PASSWORD", "postgres"),
                "POSTGRES_HOST": postgres_config.get("POSTGRES_HOST", "localhost"),
                "POSTGRES_PORT": postgres_config.get("POSTGRES_PORT", "5432")
            })
            
            # Create worker for comprehensive scraper
            self.comprehensive_scraper = Worker("comprehensive_30days", comp_account, ["ALL_SUBREDDITS"])
            
            # Start the process
            self.comprehensive_scraper.process = await asyncio.create_subprocess_exec(
                sys.executable, "reddit_all_subs_30days_dataentity.py",
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL
            )
            
            self.comprehensive_scraper.status.process_pid = self.comprehensive_scraper.process.pid
            self.comprehensive_scraper.status.status = "running"
            self.comprehensive_scraper.status.last_health_check = time.time()
            
            logger.info(
                f"[COMPREHENSIVE] Started comprehensive 30-day scraper with account {comp_account.username}, "
                f"PID {self.comprehensive_scraper.process.pid}"
            )
            
            self.tracking_data["total_workers_spawned"] += 1
            return True
            
        except Exception as e:
            logger.error(f"[COMPREHENSIVE] Failed to start comprehensive scraper: {e}")
            return False
    
    def check_for_forbidden_error(self, error_msg: str) -> bool:
        """Check if error message indicates forbidden/suspended account"""
        if not error_msg:
            return False
        error_lower = error_msg.lower()
        return any(keyword in error_lower for keyword in FORBIDDEN_ERROR_KEYWORDS)
    
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
        
        if not self.reserve_accounts:
            logger.error(f"[REPLACE] No reserve accounts available for {worker_id}")
            return False
        
        # Get a reserve account and assign random proxy
        reserve_account = self.reserve_accounts.pop(0)
        reserve_account = self.assign_random_proxy_to_account(reserve_account)
        
        # Add failed account back to reserves only if not forbidden
        if not is_forbidden:
            self.reserve_accounts.append(failed_worker.account)
        
        # Restart with new account
        success = await failed_worker.restart(reserve_account)
        
        if success:
            self.tracking_data["total_replacements"] += 1
            logger.info(
                f"[REPLACE] Successfully replaced {worker_id} with account {reserve_account.username}. "
                f"Reserves remaining: {len(self.reserve_accounts)}"
            )
        
        self.save_tracking_data()
        self.update_envactive_status()
        return success
    
    async def rotate_jobs(self):
        """Rotate subreddit assignments across workers"""
        logger.info("[ROTATION] Starting job rotation...")
        
        # Shuffle subreddits
        random.shuffle(self.subreddits)
        
        # Redistribute
        distributions = self.distribute_subreddits(self.subreddits, len(self.workers))
        
        # Update each worker
        for (worker_id, worker), new_subs in zip(self.workers.items(), distributions):
            old_subs = worker.subreddits
            worker.subreddits = new_subs
            worker.status.subreddits = new_subs
            worker.status.assigned_subreddits_count = len(new_subs)
            
            logger.info(
                f"[ROTATION] {worker_id}: {len(old_subs)} → {len(new_subs)} subreddits. "
                f"New: {', '.join(new_subs[:3])}{'...' if len(new_subs) > 3 else ''}"
            )
            
            # Restart worker with new job
            await worker.restart()
        
        self.tracking_data["total_job_rotations"] += 1
        self.save_tracking_data()
    
    async def rest_workers(self, duration_minutes: int):
        """Give all workers a rest period"""
        logger.info(f"[REST] Starting {duration_minutes}-minute rest period for all workers")
        
        for worker_id, worker in self.workers.items():
            await worker.stop()
            worker.status.status = "resting"
        
        self.tracking_data["total_rest_periods"] += 1
        self.save_tracking_data()
        
        await asyncio.sleep(duration_minutes * 60)
        
        logger.info("[REST] Rest period complete, resuming workers")
        
        for worker_id, worker in self.workers.items():
            await worker.start()
    
    async def health_check_loop(self):
        """Continuously monitor worker health and detect forbidden accounts"""
        while not self.shutdown_flag:
            try:
                # Check regular workers
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
                
                # Check comprehensive scraper
                if ENABLE_ALL_SUBS_30DAYS_SCRAPER and self.comprehensive_scraper:
                    is_healthy = await self.comprehensive_scraper.is_healthy()
                    if not is_healthy:
                        logger.warning("[HEALTH] Comprehensive scraper is unhealthy, attempting restart")
                        await self.comprehensive_scraper.restart()
                
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
                
            except Exception as e:
                logger.error(f"[HEALTH] Error in health check: {e}")
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
    
    async def config_reload_loop(self):
        """Periodically reload configuration"""
        while not self.shutdown_flag:
            try:
                await asyncio.sleep(CONFIG_RELOAD_INTERVAL)
                
                # Reload accounts - use same allocation logic as initial spawn
                all_accounts, total_count = self.load_accounts()
                if total_count > 0:
                    # Only update if reserve count increased
                    new_reserve_count = total_count - len(self.workers)
                    if new_reserve_count > len(self.reserve_accounts):
                        logger.info(f"[CONFIG] Reserve accounts increased: {len(self.reserve_accounts)} → {new_reserve_count}")
                
                # Reload subreddits
                new_subreddits = self.load_subreddits()
                if new_subreddits and new_subreddits != self.subreddits:
                    logger.info(f"[CONFIG] Detected subreddit changes: {len(self.subreddits)} → {len(new_subreddits)}")
                    self.subreddits = new_subreddits
                    # Will be applied on next rotation
                
            except Exception as e:
                logger.error(f"[CONFIG] Error reloading config: {e}")
    
    async def work_rest_cycle_loop(self):
        """Manage work/rest cycles"""
        cycle_count = 0
        
        while not self.shutdown_flag:
            try:
                cycle_count += 1
                self.tracking_data["current_cycle"] = cycle_count
                
                # Decide on work/rest pattern
                if cycle_count % 6 == 0:  # Every 6 cycles (6 hours)
                    logger.info(f"[CYCLE {cycle_count}] Long cycle: {LONG_WORK_DURATION_MINUTES} min work + {LONG_REST_DURATION_MINUTES} min rest")
                    await asyncio.sleep(LONG_WORK_DURATION_MINUTES * 60)
                    await self.rest_workers(LONG_REST_DURATION_MINUTES)
                else:
                    logger.info(f"[CYCLE {cycle_count}] Regular cycle: {WORK_DURATION_MINUTES} min work + {REST_DURATION_MINUTES} min rest")
                    await asyncio.sleep(WORK_DURATION_MINUTES * 60)
                    await self.rest_workers(REST_DURATION_MINUTES)
                
            except Exception as e:
                logger.error(f"[CYCLE] Error in work/rest cycle: {e}")
    
    async def job_rotation_loop(self):
        """Periodically rotate jobs"""
        while not self.shutdown_flag:
            try:
                await asyncio.sleep(JOB_ROTATION_INTERVAL)
                await self.rotate_jobs()
            except Exception as e:
                logger.error(f"[ROTATION] Error in job rotation: {e}")
    
    def update_envactive_status(self):
        """Update envActive file with current account status annotations"""
        try:
            # Read current envActive
            with open(ACCOUNTS_FILE, 'r') as f:
                lines = f.readlines()
            
            # Build status info
            active_usernames = [w.account.username for w in self.workers.values()]
            reserve_usernames = [acc.username for acc in self.reserve_accounts]
            
            # Update header with status
            new_lines = []
            for line in lines:
                if line.startswith('#'):
                    # Update existing header
                    if 'envActive last update' in line:
                        total_active = len(active_usernames)
                        total_reserve = len(reserve_usernames)
                        total_forbidden = len(self.forbidden_accounts)
                        new_lines.append(
                            f"# envActive last update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                            f"active: {total_active} | reserve: {total_reserve} | forbidden: {total_forbidden}\n"
                        )
                    else:
                        new_lines.append(line)
                else:
                    new_lines.append(line)
            
            # Write back
            with open(ACCOUNTS_FILE, 'w') as f:
                f.writelines(new_lines)
            
            logger.info(
                f"[STATUS] Updated envActive: {len(active_usernames)} active, "
                f"{len(reserve_usernames)} reserve, {len(self.forbidden_accounts)} forbidden"
            )
            
        except Exception as e:
            logger.error(f"[STATUS] Error updating envActive: {e}")
    
    def save_tracking_data(self):
        """Save tracking data to JSON file"""
        try:
            tracking = {
                "timestamp": datetime.now().isoformat(),
                "manager_info": self.tracking_data,
                "workers": {
                    worker_id: worker.status.to_dict()
                    for worker_id, worker in self.workers.items()
                },
                "comprehensive_scraper": {
                    "enabled": ENABLE_ALL_SUBS_30DAYS_SCRAPER,
                    "status": self.comprehensive_scraper.status.to_dict() if self.comprehensive_scraper else None
                },
                "reserves": {
                    "count": len(self.reserve_accounts),
                    "accounts": [acc.username for acc in self.reserve_accounts]
                },
                "forbidden": {
                    "count": len(self.forbidden_accounts),
                    "accounts": self.forbidden_accounts
                },
                "configuration": {
                    "total_subreddits": len(self.subreddits),
                    "active_workers": len(self.workers),
                    "reserve_accounts": len(self.reserve_accounts),
                    "available_proxies": len(self.available_proxies)
                }
            }
            
            with open(TRACKING_FILE, 'w') as f:
                json.dump(tracking, f, indent=2)
                
        except Exception as e:
            logger.error(f"[TRACKING] Error saving tracking data: {e}")
    
    async def account_pool_check_loop(self):
        """Check account pool every 30 minutes and refresh active accounts"""
        while not self.shutdown_flag:
            try:
                await asyncio.sleep(ACCOUNT_POOL_CHECK_INTERVAL)
                
                logger.info("[ACCOUNT POOL] Starting periodic account pool check...")
                self.tracking_data["account_pool_checks"] += 1
                
                # Reload accounts from envActive
                all_accounts, total_count = self.load_accounts()
                
                if total_count > 0:
                    # Calculate how many we should have
                    available_for_allocation = total_count - ACCOUNTS_FOR_STREAM_ALL
                    num_for_reddit_scraper = int(available_for_allocation * ALLOCATION_RATIO)
                    new_accounts_for_main = all_accounts[:num_for_reddit_scraper]
                    new_reserves = all_accounts[num_for_reddit_scraper:]
                    
                    # Assign proxies to new reserves
                    for account in new_reserves:
                        account = self.assign_random_proxy_to_account(account)
                    
                    # Update reserves if we have new accounts
                    if len(new_reserves) > len(self.reserve_accounts):
                        added = len(new_reserves) - len(self.reserve_accounts)
                        logger.info(
                            f"[ACCOUNT POOL] Added {added} new accounts to reserve pool. "
                            f"Total reserves: {len(new_reserves)}"
                        )
                        self.reserve_accounts = new_reserves
                    
                    self.update_envactive_status()
                    
                    logger.info(
                        f"[ACCOUNT POOL] Check complete. Active: {len(self.workers)}, "
                        f"Reserve: {len(self.reserve_accounts)}, Forbidden: {len(self.forbidden_accounts)}"
                    )
                
            except Exception as e:
                logger.error(f"[ACCOUNT POOL] Error in pool check: {e}")
    
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
        
        # Stop regular workers
        for worker_id, worker in self.workers.items():
            await worker.stop()
        
        # Stop comprehensive scraper
        if self.comprehensive_scraper:
            await self.comprehensive_scraper.stop()
            logger.info("[SHUTDOWN] Comprehensive scraper stopped")
        
        self.save_tracking_data()
        logger.info("[SHUTDOWN] All workers stopped")
    
    async def run(self):
        """Main run loop"""
        logger.info("="*80)
        logger.info("Worker Manager Starting - 24/7 Reddit Scraping System")
        logger.info("="*80)
        
        # Initial setup
        success = await self.spawn_workers()
        if not success:
            logger.error("[FATAL] Failed to spawn workers, exiting")
            return
        
        # Start all management loops
        tasks = [
            asyncio.create_task(self.health_check_loop()),
            asyncio.create_task(self.config_reload_loop()),
            asyncio.create_task(self.work_rest_cycle_loop()),
            asyncio.create_task(self.job_rotation_loop()),
            asyncio.create_task(self.account_pool_check_loop()),
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
    manager = WorkerManager()
    
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
