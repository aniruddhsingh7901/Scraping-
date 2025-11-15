#!/usr/bin/env python3
"""
account_pool.py

Comprehensive Reddit account management system that:
✅ Parses accounts from .env file
✅ Verifies account credentials with Reddit API
✅ Tests and assigns proxies (roundrobin or random)
✅ Publishes active accounts to envActive file
✅ Runs continuous verification loop (every 1 hour by default)
✅ Supports one-time manual verification via CLI
✅ Handles proxy rotation and retry logic
"""

import os
import time
import asyncio
import random
import argparse
from typing import List, Dict, Optional, Tuple

import aiohttp
import asyncpraw

REQUIRED_KEYS = ("client_id", "client_secret", "username", "password", "proxy_url")


def parse_env_accounts(path: str = ".env") -> List[Dict[str, str]]:
    """
    Parse a non-standard .env that contains repeated blocks of:
      client_id     "..."
      client_secret "..."
      username      "..."
      password      "..."
      proxy_url     "http://user:pass@host:port"

    Lines may be tab or space separated and values quoted. We accumulate a block
    once all REQUIRED_KEYS are seen, then reset. Returns only well-formed blocks.
    """
    accounts: List[Dict[str, str]] = []
    current: Dict[str, str] = {}

    if not os.path.exists(path):
        return accounts

    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            # normalize tabs to spaces
            line = line.replace("\t", " ").strip()

            # Support "KEY=value" and "key    \"value\"" formats
            if "=" in line and not line.lstrip().startswith(("http://", "https://")):
                key, val = line.split("=", 1)
            else:
                parts = line.split(None, 1)
                if len(parts) != 2:
                    continue
                key, val = parts

            key = key.strip()
            val = val.strip().strip('"').strip("'")

            if key in {"client_id", "client_secret", "username", "password", "proxy_url"}:
                current[key] = val

            if all(k in current for k in REQUIRED_KEYS):
                accounts.append({k: current[k] for k in REQUIRED_KEYS})
                current = {}

    return accounts


def parse_proxy_file(path: str) -> List[str]:
    """
    Parse a proxy file with lines in the format:
        host:port:user:pass
    Returns a list of http proxy URLs like:
        http://user:pass@host:port
    """
    proxies: List[str] = []
    if not os.path.exists(path):
        return proxies
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) != 4:
                continue
            host, port, user, pwd = parts
            proxies.append(f"http://{user}:{pwd}@{host}:{port}")
    return proxies


class _ProxyEnv:
    """
    Context manager to temporarily set HTTP(S)_PROXY env variables for a given proxy.
    This is used so aiohttp with trust_env=True will pick up per-account proxy.
    """

    def __init__(self, proxy: Optional[str]) -> None:
        self.proxy = proxy
        self.prev_http = None
        self.prev_https = None
        self.prev_http_lc = None
        self.prev_https_lc = None
        self.prev_no_proxy = None
        self.had_http = False
        self.had_https = False
        self.had_http_lc = False
        self.had_https_lc = False
        self.had_no_proxy = False

    def __enter__(self):
        self.prev_http = os.environ.get("HTTP_PROXY")
        self.prev_https = os.environ.get("HTTPS_PROXY")
        self.prev_http_lc = os.environ.get("http_proxy")
        self.prev_https_lc = os.environ.get("https_proxy")
        self.prev_no_proxy = os.environ.get("NO_PROXY")
        self.had_http = "HTTP_PROXY" in os.environ
        self.had_https = "HTTPS_PROXY" in os.environ
        self.had_http_lc = "http_proxy" in os.environ
        self.had_https_lc = "https_proxy" in os.environ
        self.had_no_proxy = "NO_PROXY" in os.environ

        if self.proxy:
            os.environ["HTTP_PROXY"] = self.proxy
            os.environ["HTTPS_PROXY"] = self.proxy
            os.environ["http_proxy"] = self.proxy
            os.environ["https_proxy"] = self.proxy
            os.environ.pop("NO_PROXY", None)
        else:
            # Ensure proxies are not set for "no proxy" case
            os.environ.pop("HTTP_PROXY", None)
            os.environ.pop("HTTPS_PROXY", None)
            os.environ.pop("http_proxy", None)
            os.environ.pop("https_proxy", None)
            os.environ.pop("NO_PROXY", None)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        def _restore(var, had, prev):
            if had:
                if prev is not None:
                    os.environ[var] = prev
                else:
                    os.environ.pop(var, None)
            else:
                os.environ.pop(var, None)

        _restore("HTTP_PROXY", self.had_http, self.prev_http)
        _restore("HTTPS_PROXY", self.had_https, self.prev_https)
        _restore("http_proxy", self.had_http_lc, self.prev_http_lc)
        _restore("https_proxy", self.had_https_lc, self.prev_https_lc)
        _restore("NO_PROXY", self.had_no_proxy, self.prev_no_proxy)


async def verify_account(
    account: Dict[str, str],
    timeout: int = 20,
    proxy_override: Optional[str] = None,
    proxies: Optional[List[str]] = None,
    max_proxy_attempts: int = 3,
    verbose: bool = False,
) -> Tuple[str, bool, str]:
    """
    Attempt to authenticate with asyncpraw and fetch the current user.
    Returns (username, success, message).

    Proxy behavior:
      - Try proxy_override first if provided.
      - Then try alternative proxies from 'proxies' up to max_proxy_attempts.
      - On 401 (invalid credentials) do not retry with other proxies.
      - On 403/network errors, rotate to next proxy.
    """
    username = account.get("username", "<unknown>")

    # Build proxy attempt list
    attempts: List[Optional[str]] = []
    if proxy_override:
        attempts.append(proxy_override)
    extras: List[str] = []
    if proxies:
        extras = [p for p in proxies if p != proxy_override]
        random.shuffle(extras)
    attempts.extend(extras)
    if max_proxy_attempts > 0:
        attempts = attempts[:max_proxy_attempts]
    if not attempts:
        attempts = [proxy_override or account.get("proxy_url")]

    last_err = ""
    for idx, proxy in enumerate(attempts, start=1):
        with _ProxyEnv(proxy or account.get("proxy_url")):
            session = aiohttp.ClientSession(
                trust_env=True,
                timeout=aiohttp.ClientTimeout(total=timeout + 5),
                connector=aiohttp.TCPConnector(force_close=True),
            )
            reddit = asyncpraw.Reddit(
                client_id=account["client_id"],
                client_secret=account["client_secret"],
                username=account["username"],
                password=account["password"],
                user_agent=f"account-pool:v1 (by /u/{username})",
                requestor_kwargs={"session": session},
            )
            try:
                async def _do_check():
                    me = await reddit.user.me()
                    return me is not None
                ok = await asyncio.wait_for(_do_check(), timeout=timeout)
                if ok:
                    via = f"ok via proxy {proxy}" if proxy else "ok (no proxy)"
                    return username, True, via
                else:
                    return username, False, "failed: me() returned None"
            except Exception as e:
                msg = f"{type(e).__name__}: {e}" if verbose else f"{type(e).__name__}"
                last_err = msg
                # Classify error
                lower = str(e).lower()
                if "401" in lower or "invalid_grant" in lower:
                    # Invalid credentials; no point trying other proxies
                    return username, False, msg
                # Otherwise try next proxy (403, connection issues, etc.)
                # fall through to next attempt after closing resources
            finally:
                try:
                    await reddit.close()
                finally:
                    await session.close()
    return username, False, last_err or "error: proxy attempts exhausted"


def _format_account_block(acc: Dict[str, str]) -> str:
    """
    Write a block in the same loose format the parser accepts: key<TAB>"value"
    """
    lines = []
    for key in REQUIRED_KEYS:
        val = acc.get(key, "")
        # Quote values to be safe, keep proxy_url as given
        lines.append(f"{key}\t\"{val}\"")
    return "\n".join(lines)


def _atomic_write(path: str, content: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)


async def build_active_accounts(
    accounts: List[Dict[str, str]],
    per_account_timeout: int = 20,
    proxies: Optional[List[str]] = None,
    strategy: str = "roundrobin",
    verbose: bool = False,
) -> List[Dict[str, str]]:
    """
    Verify each account. If proxies are provided, assign one per account using the given strategy:
      - "roundrobin" (default): proxies[(idx-1) % len(proxies)]
      - "random": random choice per account
    When an account is ACTIVE, publish it with the chosen proxy via the 'proxy_url' field.
    """
    active: List[Dict[str, str]] = []
    try:
        max_attempts = int(os.getenv("PROXY_MAX_ATTEMPTS", "3"))
    except ValueError:
        max_attempts = 3
    for idx, acc in enumerate(accounts, start=1):
        chosen_proxy: Optional[str] = None
        if proxies:
            if strategy.lower() == "random":
                chosen_proxy = random.choice(proxies)
            else:
                chosen_proxy = proxies[(idx - 1) % len(proxies)]

        username, ok, msg = await verify_account(
            acc,
            timeout=per_account_timeout,
            proxy_override=chosen_proxy,
            proxies=proxies,
            max_proxy_attempts=max_attempts,
            verbose=verbose,
        )
        status = "ACTIVE" if ok else "INACTIVE"
        print(f"[{idx}/{len(accounts)}] {username}: {status} - {msg}")
        if ok:
            acc_copy = acc.copy()
            if chosen_proxy:
                acc_copy["proxy_url"] = chosen_proxy
            active.append(acc_copy)
        # small jitter between verifications to avoid burst pattern per proxy/IP
        await asyncio.sleep(random.uniform(0.5, 1.5))
    return active


async def check_and_publish(
    env_path: str = ".env",
    out_path: str = "envActive",
    per_account_timeout: int = 20,
    verbose: bool = False,
) -> Tuple[int, int]:
    """
    Parse .env, verify each account, and publish active accounts to out_path.
    Returns (total_parsed, active_count).
    """
    accounts = parse_env_accounts(env_path)
    total = len(accounts)
    print(f"Loaded accounts from {env_path}: {total}")

    if total == 0:
        # Publish empty active list to signal no available accounts
        banner = f"# envActive last update: {time.strftime('%Y-%m-%d %H:%M:%S')} (no accounts found)\n"
        _atomic_write(out_path, banner)
        return 0, 0

    # Load proxies from file if provided
    proxy_file = os.getenv("PROXY_FILE", "proxy.txt")
    proxies = parse_proxy_file(proxy_file) if os.path.exists(proxy_file) else []
    if proxies:
        print(f"Loaded proxies from {proxy_file}: {len(proxies)}")
    assignment = os.getenv("PROXY_ASSIGNMENT", "roundrobin")

    active_accounts = await build_active_accounts(
        accounts,
        per_account_timeout=per_account_timeout,
        proxies=proxies if proxies else None,
        strategy=assignment,
        verbose=verbose,
    )
    active_count = len(active_accounts)

    # Compose envActive content
    banner = f"# envActive last update: {time.strftime('%Y-%m-%d %H:%M:%S')} | active {active_count}/{total}\n"
    body_blocks = []
    for acc in active_accounts:
        body_blocks.append(_format_account_block(acc))
        body_blocks.append("")  # blank line between blocks
    content = banner + "\n".join(body_blocks).rstrip() + "\n"

    _atomic_write(out_path, content)
    print(f"Published {active_count} active accounts to {out_path}")
    return total, active_count


async def run_loop():
    """
    Periodically (every 1 hour by default) refresh envActive from .env.
    Override interval with env ACCOUNT_POLL_INTERVAL (seconds).
    """
    env_path = os.getenv("POOL_SOURCE", ".env")
    out_path = os.getenv("POOL_TARGET", "envActive")
    try:
        interval = int(os.getenv("ACCOUNT_POLL_INTERVAL", "3600"))  # Default: 1 hour (3600s)
    except ValueError:
        interval = 3600

    print(f"Account pool started. Source={env_path} Target={out_path} Interval={interval}s ({interval/60:.1f} min)")

    # Initial run
    try:
        await check_and_publish(env_path=env_path, out_path=out_path)
    except Exception as e:
        print(f"Initial publish error: {e}")

    # Periodic loop
    while True:
        try:
            await asyncio.sleep(interval)
            print(f"\n{'='*60}")
            print(f"Starting periodic verification at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            await check_and_publish(env_path=env_path, out_path=out_path)
        except Exception as e:
            print(f"Periodic publish error: {e}")


async def run_verification_once(
    accounts: List[Dict[str, str]],
    max_accounts: Optional[int],
    timeout: int,
    verbose: bool
) -> Tuple[int, int, List[Tuple[str, bool, str]]]:
    """Run one-time verification (for CLI mode)"""
    results: List[Tuple[str, bool, str]] = []
    total = len(accounts) if max_accounts is None else min(max_accounts, len(accounts))
    active = 0
    
    # Load proxies
    proxy_file = os.getenv("PROXY_FILE", "proxy.txt")
    proxies = parse_proxy_file(proxy_file) if os.path.exists(proxy_file) else []
    if proxies:
        print(f"Loaded {len(proxies)} proxies from {proxy_file}")
    
    for i in range(total):
        acc = accounts[i]
        username, ok, msg = await verify_account(
            acc,
            timeout=timeout,
            proxies=proxies if proxies else None,
            max_proxy_attempts=3,
            verbose=verbose
        )
        results.append((username, ok, msg))
        if ok:
            active += 1
        status = "ACTIVE" if ok else "INACTIVE"
        print(f"[{i+1}/{total}] {username}: {status} - {msg}")
    return total, active, results


def main():
    """
    Main entry point supporting both daemon mode and CLI mode.
    
    Daemon mode (default): Runs continuous loop checking accounts every hour
    CLI mode: One-time verification with options
    """
    parser = argparse.ArgumentParser(
        description="Reddit Account Pool Manager - Verify and manage Reddit accounts"
    )
    parser.add_argument(
        "--mode",
        choices=["daemon", "verify"],
        default="daemon",
        help="Operation mode: 'daemon' for continuous monitoring, 'verify' for one-time check"
    )
    parser.add_argument(
        "--env",
        default=".env",
        help="Path to the .env file (default: .env)"
    )
    parser.add_argument(
        "--output",
        default="envActive",
        help="Path to output active accounts file (default: envActive)"
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Verify only the first N accounts (verify mode only)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Per-account verification timeout in seconds (default: 20)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose error details for failures"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Verification interval in seconds for daemon mode (default: 3600 = 1 hour)"
    )
    
    args = parser.parse_args()
    
    # Set environment variables from args
    os.environ["POOL_SOURCE"] = args.env
    os.environ["POOL_TARGET"] = args.output
    os.environ["ACCOUNT_POLL_INTERVAL"] = str(args.interval)
    
    if args.mode == "daemon":
        print(f"Starting in DAEMON mode")
        print(f"  Source: {args.env}")
        print(f"  Output: {args.output}")
        print(f"  Interval: {args.interval}s ({args.interval/60:.1f} minutes)")
        print(f"  Press Ctrl+C to stop")
        print()
        try:
            asyncio.run(run_loop())
        except KeyboardInterrupt:
            print("\nStopping daemon...")
    
    else:  # verify mode
        print(f"Starting in VERIFY mode (one-time check)")
        print(f"  Source: {args.env}")
        print()
        
        accounts = parse_env_accounts(args.env)
        print(f"Parsed well-formed accounts: {len(accounts)}")
        
        if not accounts:
            print("No accounts found.")
            return
        
        # Print usernames summary
        usernames = [a.get("username", "<unknown>") for a in accounts]
        print(f"Usernames: {', '.join(usernames[:10])}")
        if len(usernames) > 10:
            print(f"  ... and {len(usernames) - 10} more")
        print()
        
        # Run verification
        total, active, _ = asyncio.run(
            run_verification_once(accounts, args.max, args.timeout, args.verbose)
        )
        print()
        print("="*60)
        print(f"Verification summary: {active} active / {total} checked")
        print("="*60)


if __name__ == "__main__":
    main()
