#!/usr/bin/env python3
"""
all_subs_worker_manager.py

Worker Manager for reddit_all_subs_30days scraping with 1/4 account allocation:
✅ Allocates 1/4 of available accounts for comprehensive 30-day scraping
✅ Fetches ALL subreddit names first
✅ Distributes 5 subreddits per worker for parallel scraping
✅ Manages worker lifecycle with health checks
✅ Re-fetches subreddit list every 24 hours
✅ Rotates pending subreddits in next cycle
✅ Comprehensive tracking and logging
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

import aiohttp
import asyncpraw
from asyncprawcore import Requestor

# Configuration
ACCOUNTS_FILE = "envActive"
TRACKING_FILE = "all_subs_tracking.json"
PROXY_FILE = "proxy.txt"
SUBREDDITS_JSON_FILE = "all_subreddits_list.json"  # Read subreddits from this file
SUBREDDITS_PER_WORKER = 5  # Each worker handles 5 subreddits
ALLOCATION_RATIO = 1/4  # Allocate 1/4 of available accounts
ACCOUNTS_FOR_STREAM_ALL = 1  # Reserve for stream_all
ACCOUNTS_FOR_REDDIT_WORKER = 2/3  # Reserve for reddit_worker_manager
HEALTH_CHECK_INTERVAL = 30  # Check worker health every 30 seconds
SUBREDDIT_REFRESH_INTERVAL = 86400  # Re-fetch subreddits every 24 hours
CYCLE_INTERVAL = 3600  # Rotate to pending subreddits every 1 hour

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('all_subs_worker_manager.log'),
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
            "subreddits": self.subreddits,
            "status": self.status,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "uptime_minutes": round((time.time() - self.start_time) / 60, 2),
            "last_health_check": datetime.fromtimestamp(self.last_health_check).isoformat(),
            "process_pid": self.process_pid,
            "total_runtime_minutes": round(self.total_runtime / 60, 2),
            "errors_count": self.errors_count,
            "last_error": self.last_error
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
            postgres_config = self._load_postgres_config()
            
            # Create environment variables for this worker
            env = os.environ.copy()
            env.update({
                "REDDIT_CLIENT_ID": self.account.client_id,
                "REDDIT_CLIENT_SECRET": self.account.client_secret,
                "REDDIT_USERNAME": self.account.username,
                "REDDIT_PASSWORD": self.account.password,
                "PROXY": self.account.proxy_url,
                "WORKER_ID": self.worker_id,
                "SUBREDDITS": ",".join(self.subreddits),  # Pass subreddit list
                "POSTGRES_DB": postgres_config.get("POSTGRES_DB", "reddit_miner_db"),
                "POSTGRES_USER": postgres_config.get("POSTGRES_USER", "reddit_user"),
                "POSTGRES_PASSWORD": postgres_config.get("POSTGRES_PASSWORD", "postgres"),
                "POSTGRES_HOST": postgres_config.get("POSTGRES_HOST", "localhost"),
                "POSTGRES_PORT": postgres_config.get("POSTGRES_PORT", "5432")
            })
            
            # Start the scraper process (use reddit_all_subs_30days_dataentity.py for all-subs scraping)
            self.process = await asyncio.create_subprocess_exec(
                sys.executable, "reddit_all_subs_30days_dataentity.py",
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
            self.status.status = "failed"
            self.status.errors_count += 1
            self.status.last_error = f"Process exited with code {self.process.returncode}"
            logger.error(f"[WORKER {self.worker_id}] Process died with code {self.process.returncode}")
            return False
        
        self.status.last_health_check = time.time()
        return True
    
    async def restart(self, new_subreddits: Optional[List[str]] = None):
        """Restart worker with same or new subreddit assignment"""
        logger.info(f"[WORKER {self.worker_id}] Restarting...")
        await self.stop()
        
        if new_subreddits:
            self.subreddits = new_subreddits
            self.status.subreddits = new_subreddits
        
        self.work_start_time = time.time()
        return await self.start()


class AllSubsWorkerManager:
    """Manages all comprehensive 30-day scraping workers"""
    
    def __init__(self):
        self.workers: Dict[str, Worker] = {}
        self.available_accounts: List[Account] = []
        self.available_proxies: List[str] = []
        self.all_subreddits: List[str] = []
        self.active_subreddits: List[str] = []  # Currently being scraped
        self.pending_subreddits: List[str] = []  # Waiting for next cycle
        self.shutdown_flag = False
        self.last_subreddit_fetch = 0
        self.tracking_data = {
            "manager_start_time": datetime.now().isoformat(),
            "total_workers_spawned": 0,
            "total_cycles_completed": 0,
            "subreddit_refreshes": 0,
            "total_subreddits_discovered": 0,
            "current_cycle": 0
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
        if self.available_proxies:
            account.proxy_url = random.choice(self.available_proxies)
        return account
    
    def load_accounts(self) -> Tuple[List[Account], int]:
        """Load accounts from envActive file with 1/4 allocation - NO CONFLICT with reddit-worker-manager"""
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
                    
                    if key.startswith('POSTGRES_'):
                        continue
                    
                    if key in ['client_id', 'client_secret', 'username', 'password', 'proxy_url']:
                        current_account[key] = value
                        
                        if len(current_account) == 5:
                            accounts.append(Account(**current_account))
                            current_account = {}
            
            total_loaded = len(accounts)
            
            # ALLOCATION STRATEGY TO AVOID CONFLICTS:
            # Stream_all takes: accounts[0:1] (1 account)
            # Reddit-worker-manager takes: accounts[1:1+num_reddit_workers] (2/3 of remaining)
            # All_subs (this manager) takes: AFTER reddit-worker-manager (1/4 of remaining)
            
            # Reserve for stream_all
            reserved_for_stream = ACCOUNTS_FOR_STREAM_ALL  # 1 account
            
            # Calculate reddit-worker-manager allocation (2/3 of remaining)
            available_after_stream = total_loaded - reserved_for_stream
            num_for_reddit_worker = int(available_after_stream * ACCOUNTS_FOR_REDDIT_WORKER)  # 2/3
            
            # Calculate all_subs allocation (1/4 of remaining)
            num_for_all_subs = int(available_after_stream * ALLOCATION_RATIO)  # 1/4
            
            # Get accounts allocated for all_subs AFTER reddit-worker-manager
            start_idx = reserved_for_stream + num_for_reddit_worker
            end_idx = start_idx + num_for_all_subs
            accounts_for_all_subs = accounts[start_idx:end_idx]
            
            logger.info(f"[CONFIG] Loaded {total_loaded} total accounts from {ACCOUNTS_FILE}")
            logger.info(f"[ALLOCATION] Account distribution:")
            logger.info(f"  - Stream_all: accounts[0:{reserved_for_stream}] = {reserved_for_stream} account(s)")
            logger.info(f"  - Reddit-worker-manager: accounts[{reserved_for_stream}:{reserved_for_stream + num_for_reddit_worker}] = {num_for_reddit_worker} accounts")
            logger.info(f"  - All_subs (this): accounts[{start_idx}:{end_idx}] = {num_for_all_subs} accounts")
            logger.info(f"  - Remaining/Reserve: {total_loaded - end_idx} accounts")
            
            return accounts_for_all_subs, len(accounts_for_all_subs)
            
        except FileNotFoundError:
            logger.error(f"[CONFIG] {ACCOUNTS_FILE} not found")
            return [], 0
        except Exception as e:
            logger.error(f"[CONFIG] Error loading accounts: {e}")
            return [], 0
    
    def load_subreddits_from_json(self) -> List[str]:
        """Load subreddits from JSON file"""
        try:
            if not os.path.exists(SUBREDDITS_JSON_FILE):
                logger.error(f"[SUBREDDITS] {SUBREDDITS_JSON_FILE} not found!")
                logger.error(f"[SUBREDDITS] Please run: python3 reddit_all_subs_30days_dataentity.py first")
                return []
            
            with open(SUBREDDITS_JSON_FILE, 'r') as f:
                data = json.load(f)
            
            subreddits = data.get("subreddits", [])
            fetch_date = data.get("fetch_date", "unknown")
            
            logger.info(f"[SUBREDDITS] Loaded {len(subreddits)} subreddits from {SUBREDDITS_JSON_FILE}")
            logger.info(f"[SUBREDDITS] Fetch date: {fetch_date}")
            
            return subreddits
            
        except Exception as e:
            logger.error(f"[SUBREDDITS] Error loading from JSON: {e}")
            return []
    
    def distribute_subreddits(self, subreddits: List[str], num_workers: int) -> Tuple[List[List[str]], List[str]]:
        """
        Distribute subreddits across workers (5 per worker)
        Returns: (active_distributions, pending_subreddits)
        """
        if num_workers == 0:
            return [], subreddits
        
        # Shuffle for better distribution
        shuffled = subreddits.copy()
        random.shuffle(shuffled)
        
        # Calculate how many subreddits can be actively scraped
        total_active = num_workers * SUBREDDITS_PER_WORKER
        
        active_subs = shuffled[:total_active]
        pending_subs = shuffled[total_active:]
        
        # Distribute active subreddits
        distributions = []
        for i in range(num_workers):
            start_idx = i * SUBREDDITS_PER_WORKER
            end_idx = start_idx + SUBREDDITS_PER_WORKER
            distributions.append(active_subs[start_idx:end_idx])
        
        logger.info(f"[DISTRIBUTION] Distributing {len(active_subs)} subreddits to {num_workers} workers")
        logger.info(f"[DISTRIBUTION] {len(pending_subs)} subreddits pending for next cycle")
        
        for i, dist in enumerate(distributions):
            logger.info(f"[DISTRIBUTION] Worker {i+1}: {len(dist)} subreddits - {', '.join(dist)}")
        
        return distributions, pending_subs
    
    async def fetch_and_create_subreddit_list(self) -> bool:
        """Fetch ALL subreddit names from Reddit and save to JSON"""
        logger.info("[FETCH] Subreddit list file not found, fetching from Reddit...")
        logger.info("[FETCH] This may take 30-60 minutes to complete...")
        
        if not self.available_accounts:
            logger.error("[FETCH] No accounts available to fetch subreddits")
            return False
        
        account = self.available_accounts[0]
        
        # Setup proxy
        proxy_env = account.proxy_url
        try:
            host, port, user, pwd = proxy_env.split(":")
            proxy_url = f"http://{host}:{port}"
            proxy_auth = aiohttp.BasicAuth(user, pwd)
        except ValueError:
            logger.error(f"[FETCH] Invalid proxy format: {proxy_env}")
            return False
        
        # Setup session
        timeout = aiohttp.ClientTimeout(total=120)
        conn = aiohttp.TCPConnector(ssl=False, limit=100)
        session = aiohttp.ClientSession(timeout=timeout, connector=conn)
        
        if proxy_url:
            original_request = session._request
            async def proxy_request(method, url, **kwargs):
                kwargs["proxy"] = proxy_url
                kwargs["proxy_auth"] = proxy_auth
                return await original_request(method, url, **kwargs)
            session._request = proxy_request
        
        # Setup Reddit client
        reddit = asyncpraw.Reddit(
            client_id=account.client_id,
            client_secret=account.client_secret,
            username=account.username,
            password=account.password,
            user_agent="AllSubsFetcher/1.0",
            requestor_kwargs={"session": session}
        )
        
        try:
            logger.info("[FETCH] Starting to fetch all subreddit names...")
            subreddits = set()
            
            # Fetch subreddits by searching common letters/numbers
            letters = "abcdefghijklmnopqrstuvwxyz0123456789"
            
            async def fetch_letter(letter):
                try:
                    async for sr in reddit.subreddits.search(letter, limit=None):
                        subreddits.add(sr.display_name)
                        if len(subreddits) % 1000 == 0:
                            logger.info(f"[FETCH] Discovered {len(subreddits)} subreddits so far...")
                        await asyncio.sleep(random.uniform(0.5, 1.5))
                except Exception as e:
                    logger.error(f"[FETCH] Error fetching for letter {letter}: {e}")
            
            tasks = [asyncio.create_task(fetch_letter(c)) for c in letters]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            subreddit_list = sorted(list(subreddits))
            
            if not subreddit_list:
                logger.error("[FETCH] No subreddits found!")
                return False
            
            # Save to JSON file
            subreddit_data = {
                "fetch_date": datetime.now().isoformat(),
                "total_count": len(subreddit_list),
                "subreddits": subreddit_list
            }
            
            with open(SUBREDDITS_JSON_FILE, 'w') as f:
                json.dump(subreddit_data, f, indent=2)
            
            logger.info(f"[FETCH] Successfully fetched {len(subreddit_list)} subreddits")
            logger.info(f"[FETCH] Saved to {SUBREDDITS_JSON_FILE}")
            
            return True
            
        except Exception as e:
            logger.error(f"[FETCH] Failed to fetch subreddits: {e}")
            return False
        finally:
            await reddit.close()
            await session.close()
    
    async def spawn_workers(self) -> bool:
        """Spawn worker processes based on available accounts"""
        # Load proxies
        self.available_proxies = self.load_proxies()
        
        # Load accounts (1/4 allocation)
        self.available_accounts, total_count = self.load_accounts()
        
        if total_count == 0:
            logger.error("[ERROR] No accounts allocated for all_subs scraper")
            return False
        
        # Assign proxies to accounts
        for account in self.available_accounts:
            account = self.assign_random_proxy_to_account(account)
        
        # Check if subreddit list exists, if not create it
        if not os.path.exists(SUBREDDITS_JSON_FILE):
            logger.info("[INIT] Subreddit list file not found, will fetch automatically...")
            success = await self.fetch_and_create_subreddit_list()
            if not success:
                logger.error("[ERROR] Failed to fetch subreddit list")
                return False
        
        # Load subreddits from JSON file
        self.all_subreddits = self.load_subreddits_from_json()
        self.last_subreddit_fetch = time.time()
        self.tracking_data["total_subreddits_discovered"] = len(self.all_subreddits)
        
        if not self.all_subreddits:
            logger.error("[ERROR] No subreddits loaded from file")
            return False
        
        # Distribute subreddits
        distributions, self.pending_subreddits = self.distribute_subreddits(
            self.all_subreddits, 
            total_count
        )
        
        self.active_subreddits = [sub for dist in distributions for sub in dist]
        
        # Create and start workers
        for i, (account, subs) in enumerate(zip(self.available_accounts, distributions)):
            worker_id = f"all_subs_worker_{i+1}"
            worker = Worker(worker_id, account, subs)
            
            success = await worker.start()
            if success:
                self.workers[worker_id] = worker
                self.tracking_data["total_workers_spawned"] += 1
            else:
                logger.error(f"[ERROR] Failed to start {worker_id}")
        
        logger.info(f"[MANAGER] Spawned {len(self.workers)} workers successfully")
        logger.info(f"[MANAGER] Active: {len(self.active_subreddits)} subreddits, Pending: {len(self.pending_subreddits)} subreddits")
        
        self.save_tracking_data()
        return True
    
    async def rotate_to_pending(self):
        """Rotate to pending subreddits in next cycle"""
        if not self.pending_subreddits:
            logger.info("[ROTATION] No pending subreddits, restarting from beginning")
            self.pending_subreddits = self.all_subreddits.copy()
        
        logger.info(f"[ROTATION] Starting rotation to pending subreddits ({len(self.pending_subreddits)} pending)")
        
        # Redistribute subreddits
        distributions, new_pending = self.distribute_subreddits(
            self.pending_subreddits,
            len(self.workers)
        )
        
        # Update active and pending lists
        self.active_subreddits = [sub for dist in distributions for sub in dist]
        self.pending_subreddits = new_pending
        
        # Restart workers with new assignments
        for (worker_id, worker), new_subs in zip(self.workers.items(), distributions):
            logger.info(f"[ROTATION] {worker_id}: assigned {len(new_subs)} new subreddits - {', '.join(new_subs)}")
            await worker.restart(new_subreddits=new_subs)
        
        self.tracking_data["current_cycle"] += 1
        self.save_tracking_data()
    
    async def refresh_subreddit_list(self):
        """Re-load subreddit list from JSON every 24 hours"""
        logger.info("[REFRESH] Re-loading subreddit list from JSON file...")
        
        new_subreddits = self.load_subreddits_from_json()
        
        if new_subreddits:
            old_count = len(self.all_subreddits)
            self.all_subreddits = new_subreddits
            self.tracking_data["subreddit_refreshes"] += 1
            self.tracking_data["total_subreddits_discovered"] = len(self.all_subreddits)
            self.last_subreddit_fetch = time.time()
            
            # Combine with pending
            self.pending_subreddits = list(set(self.all_subreddits) - set(self.active_subreddits))
            
            logger.info(
                f"[REFRESH] Subreddit list updated: {old_count} → {len(self.all_subreddits)} "
                f"(+{len(self.all_subreddits) - old_count})"
            )
            logger.info(f"[REFRESH] Updated pending list: {len(self.pending_subreddits)} subreddits")
            
            self.save_tracking_data()
        else:
            logger.error("[REFRESH] Failed to refresh subreddit list from JSON")
    
    async def health_check_loop(self):
        """Continuously monitor worker health"""
        while not self.shutdown_flag:
            try:
                for worker_id, worker in list(self.workers.items()):
                    is_healthy = await worker.is_healthy()
                    
                    if not is_healthy:
                        logger.warning(f"[HEALTH] {worker_id} is unhealthy, attempting restart")
                        await worker.restart()
                
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
                
            except Exception as e:
                logger.error(f"[HEALTH] Error in health check: {e}")
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)
    
    async def rotation_loop(self):
        """Periodically rotate to pending subreddits"""
        while not self.shutdown_flag:
            try:
                await asyncio.sleep(CYCLE_INTERVAL)
                await self.rotate_to_pending()
                self.tracking_data["total_cycles_completed"] += 1
                
            except Exception as e:
                logger.error(f"[ROTATION] Error in rotation: {e}")
    
    async def subreddit_refresh_loop(self):
        """Periodically refresh subreddit list"""
        while not self.shutdown_flag:
            try:
                await asyncio.sleep(SUBREDDIT_REFRESH_INTERVAL)
                await self.refresh_subreddit_list()
                
            except Exception as e:
                logger.error(f"[REFRESH] Error in refresh: {e}")
    
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
                "subreddit_stats": {
                    "total_subreddits": len(self.all_subreddits),
                    "active_subreddits": len(self.active_subreddits),
                    "pending_subreddits": len(self.pending_subreddits),
                    "last_refresh": datetime.fromtimestamp(self.last_subreddit_fetch).isoformat() if self.last_subreddit_fetch > 0 else None
                },
                "configuration": {
                    "subreddits_per_worker": SUBREDDITS_PER_WORKER,
                    "allocation_ratio": ALLOCATION_RATIO,
                    "active_workers": len(self.workers),
                    "cycle_interval_minutes": CYCLE_INTERVAL / 60,
                    "refresh_interval_hours": SUBREDDIT_REFRESH_INTERVAL / 3600
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
                await asyncio.sleep(60)
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
        logger.info("All Subs Worker Manager Starting - 1/4 Account Allocation")
        logger.info("="*80)
        
        success = await self.spawn_workers()
        if not success:
            logger.error("[FATAL] Failed to spawn workers, exiting")
            return
        
        # Start all management loops
        tasks = [
            asyncio.create_task(self.health_check_loop()),
            asyncio.create_task(self.rotation_loop()),
            asyncio.create_task(self.subreddit_refresh_loop()),
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
    manager = AllSubsWorkerManager()
    
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
