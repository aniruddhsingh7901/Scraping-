#!/usr/bin/env python3
"""
Normalize proxy strings stored in a twscrape SQLite DB.

Fixes accounts.proxy values like:
- "5.249.178.32:5432:uae2k:ddfivl8d"  -> "http://uae2k:ddfivl8d@5.249.178.32:5432"
- "5.249.178.32:5432"                 -> "http://5.249.178.32:5432"

Leaves proxies that already start with http:// or https:// unchanged.

Usage:
  python3 tools/normalize_twscrape_proxies.py --db /root/Algo-test-script/accounts_x2.db
"""

import argparse
import sqlite3
import sys
import os


def normalize(proxy: str) -> str:
    p = proxy.strip()
    if not p:
        return p
    lower = p.lower()
    if lower.startswith("http://") or lower.startswith("https://"):
        return p

    # Attempt to split into components
    parts = p.split(":")
    # host:port:user:pass
    if len(parts) >= 4:
        host, port, user, password = parts[0], parts[1], parts[2], parts[3]
        # Guard: port must be digits
        if port.isdigit():
            return f"http://{user}:{password}@{host}:{port}"
    # host:port
    if len(parts) >= 2 and parts[1].isdigit():
        host, port = parts[0], parts[1]
        return f"http://{host}:{port}"

    # Fallback: leave unchanged
    return p


def main():
    ap = argparse.ArgumentParser(description="Normalize twscrape proxies in DB")
    ap.add_argument("--db", required=True, help="Path to twscrape SQLite DB (e.g., /root/Algo-test-script/accounts_x2.db)")
    args = ap.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # verify table/column
    try:
        cur.execute("PRAGMA table_info(accounts)")
        cols = [r["name"] for r in cur.fetchall()]
        if "username" not in cols or "proxy" not in cols:
            print("accounts table missing expected columns 'username' and/or 'proxy'", file=sys.stderr)
            sys.exit(2)
    except Exception as e:
        print(f"Failed to introspect DB: {e}", file=sys.stderr)
        sys.exit(3)

    cur.execute("SELECT username, proxy FROM accounts")
    rows = cur.fetchall()

    changed = 0
    total = 0
    for r in rows:
        total += 1
        username = r["username"]
        proxy = r["proxy"]
        if proxy is None:
            continue
        new_proxy = normalize(str(proxy))
        if new_proxy != proxy:
            cur.execute("UPDATE accounts SET proxy = ? WHERE username = ?", (new_proxy, username))
            changed += 1

    conn.commit()
    conn.close()

    print(f"Scanned {total} accounts, updated {changed} proxy values.")
    if changed > 0:
        print("Restart x-orchestrator to apply changes:\n"
              "  pm2 restart x-orchestrator --update-env\n"
              "Then tail logs:\n"
              "  pm2 logs x-orchestrator --lines 200")


if __name__ == "__main__":
    main()
