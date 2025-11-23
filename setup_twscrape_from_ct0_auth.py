#!/usr/bin/env python3
"""
setup_twscrape_from_ct0_auth.py

Populate a twscrape accounts SQLite DB from a file formatted as:
username:password:email:email_password:ct0:auth:proxy

Notes:
- proxy is optional; keep the trailing colon if empty
- cookies are constructed as "auth_token=<auth>; ct0=<ct0>"
- Uses the local twscrape package at ./twscrape (no pip install needed)
"""

import asyncio
import argparse
import logging
import os
import sys

# Ensure local twscrape package is importable
REPO_ROOT = "/root/Algo-test-script"
TWSCRAPE_DIR = os.path.join(REPO_ROOT, "twscrape")
if TWSCRAPE_DIR not in sys.path:
    sys.path.insert(0, TWSCRAPE_DIR)

from twscrape import API  # noqa: E402

log = logging.getLogger("setup_twscrape_from_ct0_auth")


async def import_accounts(file_path: str, db_path: str, login_after: bool) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    log.info(f"Importing accounts from {file_path}")
    log.info(f"Target twscrape DB: {db_path}")

    # Initialize API with the target DB
    api = API(pool=db_path)

    # Read accounts file
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_lines = f.read().splitlines()
    except FileNotFoundError:
        log.error(f"File not found: {file_path}")
        return

    # Parse lines, skip comments/empties
    lines = [ln.strip() for ln in raw_lines if ln.strip() and not ln.strip().startswith("#")]
    log.info(f"Found {len(lines)} non-empty account lines")

    added, skipped = 0, 0
    for idx, line in enumerate(lines, 1):
        try:
            # Split into first 7 fields to preserve proxy that may contain colons in credentials
            parts = line.split(":", 6)
            if len(parts) < 6:
                skipped += 1
                log.error(f"Line {idx}: invalid format (need at least 6 fields): {line}")
                continue

            username = parts[0].strip()
            password = parts[1].strip()
            email = parts[2].strip()
            email_password = parts[3].strip()
            ct0 = parts[4].strip()
            auth = parts[5].strip()
            proxy = parts[6].strip() if len(parts) >= 7 and parts[6].strip() else None

            if not username or not email or not ct0 or not auth:
                skipped += 1
                log.error(f"Line {idx}: missing required values (username/email/ct0/auth): {line}")
                continue

            cookies = f"auth_token={auth}; ct0={ct0}"
            log.info(f"Adding {idx}/{len(lines)} user={username} proxy={'set' if proxy else 'none'} cookies=set")

            await api.pool.add_account(
                username=username,
                password=password,
                email=email,
                email_password=email_password,
                proxy=proxy,
                cookies=cookies,
            )
            added += 1

        except Exception as e:
            skipped += 1
            log.error(f"Line {idx}: error adding account: {e}")

    log.info(f"Imported: {added}, Skipped: {skipped}")

    # Show brief stats
    try:
        stats = await api.pool.stats()
        log.info(f"DB Stats: total={stats.get('total', 0)} active={stats.get('active', 0)} inactive={stats.get('inactive', 0)}")
    except Exception as e:
        log.warning(f"Could not fetch stats: {e}")

    # Optional login
    if login_after:
        log.info("Logging in to inactive accounts...")
        try:
            res = await api.pool.login_all()
            log.info(f"Login result: total={res.get('total')} success={res.get('success')} failed={res.get('failed')}")
        except Exception as e:
            log.error(f"Login failed: {e}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate twscrape DB from ct0/auth file")
    parser.add_argument(
        "--file",
        default=os.path.join(REPO_ROOT, "OnDEMANDX", "twscrape_accounts_ct0_auth.txt"),
        help="Path to ct0/auth accounts file (default: OnDEMANDX/twscrape_accounts_ct0_auth.txt)",
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("TWSCRAPE_DB", os.path.join(REPO_ROOT, "accounts_x2.db")),
        help="Path to twscrape SQLite DB (default: $TWSCRAPE_DB or /root/Algo-test-script/accounts_x2.db)",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="Login to all inactive accounts after import",
    )
    return parser.parse_args()


async def amain():
    args = parse_args()
    await import_accounts(args.file, args.db, args.login)


if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        print("Interrupted")
