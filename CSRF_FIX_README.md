# CSRF Error Fix Documentation

## Problem
CSRF error 353: "This request requires a matching csrf cookie and header"

This error occurs when Twitter account sessions expire or become invalid, causing 403 errors during scraping.

## Solution Overview

The solution includes:
1. **Automatic CSRF error handling** in the main scraper (twitter_worker_manager.py)
2. **Manual fix script** with proxy rotation (fix_csrf_accounts.py)

## How It Works

### Main Scraper (twitter_worker_manager.py)
- Detects CSRF errors automatically during scraping
- Allows twscrape to rotate to the next available account
- Tracks CSRF error count for monitoring
- No forced re-login (to avoid triggering Twitter's anti-bot detection)

### Manual Fix Script (fix_csrf_accounts.py)
When you need to refresh all account sessions:

```bash
./fix_csrf_accounts.py
```

**Key Features:**
- Loads 500 proxies from `/root/Algo-test-script/proxies.txt`
- Rotates proxies for each account login (different IP per account)
- Random delays (3-7 seconds) between logins to mimic human behavior
- Shuffles proxy list for randomization

**How Proxy Rotation Avoids Detection:**
- Each account uses a different proxy IP
- Twitter cannot detect pattern of same IP logging into multiple accounts
- Random delays make behavior look more natural
- Proxies are residential (harder to detect as proxies)

## Usage

### Check Account Status
```bash
./fix_csrf_accounts.py status
```

Shows:
- Total accounts
- Active vs inactive count
- Each account's status, proxy, and last used time

### Fix CSRF Errors
```bash
./fix_csrf_accounts.py
```

This will:
1. Load 500 proxies
2. Show current account status
3. Ask for confirmation
4. Re-login each account with a different proxy
5. Report success/failure count

**Expected behavior:**
- Each account gets assigned a new proxy from the pool
- Login attempts are spaced out with random delays
- Successfully logged-in accounts can resume scraping

### Run the Scraper
```bash
python twitter_worker_manager.py
```

The scraper will:
- Use accounts with valid sessions
- Automatically detect CSRF errors
- Let twscrape rotate to next account
- Track error count in stats

## Proxy Format

File: `/root/Algo-test-script/proxies.txt`

Format: `host:port:username:password`

Example:
```
5.249.176.154:5432:uae2k:ddfivl8d
5.249.176.206:5432:uae2k:ddfivl8d
```

The script converts these to: `socks5://username:password@host:port`

## When to Use Manual Fix

Use `fix_csrf_accounts.py` when:
- Most/all accounts showing CSRF errors
- Scraper can't find any active accounts
- After accounts have been idle for a long time
- After Twitter API changes/updates

**Don't use too frequently:**
- Multiple re-login attempts can trigger suspicion
- Let natural rotation work during normal operation
- Only fix when necessary

## Troubleshooting

### All logins fail (403/401 errors)
**Possible causes:**
- Accounts suspended/locked by Twitter
