# Account Monitoring Commands

## Quick Status Checks

### Check all active accounts
```bash
sqlite3 twitter_accounts.db "SELECT username, active, last_used FROM accounts WHERE active = 1;"
```

### Count active vs inactive accounts
```bash
sqlite3 twitter_accounts.db "SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active,
    SUM(CASE WHEN active = 0 THEN 1 ELSE 0 END) as inactive
FROM accounts;"
```

### Check inactive accounts with error messages
```bash
sqlite3 twitter_accounts.db "SELECT username, active, error_msg FROM accounts WHERE active = 0;"
```

### Check all accounts with proxy status
```bash
sqlite3 twitter_accounts.db "SELECT 
    username, 
    active,
    CASE WHEN proxy IS NULL OR proxy = '' THEN 'NO PROXY' ELSE 'HAS PROXY' END as proxy_status 
FROM accounts;"
```

### Check all accounts with cookie status
```bash
sqlite3 twitter_accounts.db "SELECT 
    username, 
    active,
    CASE WHEN cookies LIKE '%ct0%' AND cookies LIKE '%auth_token%' THEN 'HAS COOKIES' ELSE 'NO COOKIES' END as cookie_status
FROM accounts;"
```

### Full status overview
```bash
sqlite3 twitter_accounts.db "SELECT 
    username, 
    active,
    CASE WHEN proxy IS NULL OR proxy = '' THEN 'NO PROXY' ELSE 'HAS PROXY' END as proxy_status,
    CASE WHEN cookies LIKE '%ct0%' AND cookies LIKE '%auth_token%' THEN 'HAS COOKIES' ELSE 'NO COOKIES' END as cookie_status,
    last_used,
    error_msg
FROM accounts;"
```

### Pretty formatted table
```bash
sqlite3 -header -column twitter_accounts.db "SELECT 
    username, 
    CASE WHEN active = 1 THEN 'ACTIVE' ELSE 'INACTIVE' END as status,
    error_msg
FROM accounts;"
```

## Detailed Monitoring

### Check account with most requests
```bash
sqlite3 twitter_accounts.db "SELECT username, active, stats FROM accounts ORDER BY length(stats) DESC LIMIT 5;"
```

### Check recently used accounts
```bash
sqlite3 -header -column twitter_accounts.db "SELECT username, active, last_used FROM accounts ORDER BY last_used DESC LIMIT 10;"
```

### Check accounts that have never been used
```bash
sqlite3 twitter_accounts.db "SELECT username, active FROM accounts WHERE last_used IS NULL;"
```

### Export full account info to CSV
```bash
sqlite3 -header -csv twitter_accounts.db "SELECT username, active, error_msg, last_used FROM accounts;" > accounts_status.csv
```

## Quick Count Commands

### Just count active accounts
```bash
sqlite3 twitter_accounts.db "SELECT COUNT(*) FROM accounts WHERE active = 1;"
```

### Just count inactive accounts
```bash
sqlite3 twitter_accounts.db "SELECT COUNT(*) FROM accounts WHERE active = 0;"
```

### Count total accounts
```bash
sqlite3 twitter_accounts.db "SELECT COUNT(*) FROM accounts;"
```

## Python Script Alternative

### Quick check using Python
```bash
python -c "
import sys
sys.path.append('/root/Algo-test-script/twscrape')
import asyncio
from twscrape import AccountsPool

async def check():
    pool = AccountsPool('twitter_accounts.db')
    accounts = await pool.get_all()
    active = [a for a in accounts if a.active]
    inactive = [a for a in accounts if not a.active]
    
    print(f'\nTotal: {len(accounts)}')
    print(f'Active: {len(active)}')
    print(f'Inactive: {len(inactive)}')
    
    if inactive:
        print('\nInactive accounts:')
        for a in inactive:
            print(f'  - {a.username}: {a.error_msg}')

asyncio.run(check())
"
```

### Or use the auto_relogin script in check-only mode
```bash
# Just shows status without attempting recovery
python auto_relogin_accounts.py 2>&1 | grep -A 20 "ACCOUNT STATUS"
```

## One-Liner Status Commands

### Single line summary
```bash
echo "Active: $(sqlite3 twitter_accounts.db 'SELECT COUNT(*) FROM accounts WHERE active = 1;') | Inactive: $(sqlite3 twitter_accounts.db 'SELECT COUNT(*) FROM accounts WHERE active = 0;') | Total: $(sqlite3 twitter_accounts.db 'SELECT COUNT(*) FROM accounts;')"
```

### List just active account names
```bash
sqlite3 twitter_accounts.db "SELECT username FROM accounts WHERE active = 1;"
```

### List just inactive account names
```bash
sqlite3 twitter_accounts.db "SELECT username FROM accounts WHERE active = 0;"
```

## Monitoring Script

Create a quick status check script:

```bash
cat > check_accounts.sh << 'EOF'
#!/bin/bash
echo "======================================"
echo "Twitter Account Status Check"
echo "======================================"
echo ""
sqlite3 -header -column twitter_accounts.db "
SELECT 
    COUNT(*) as Total,
    SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as Active,
    SUM(CASE WHEN active = 0 THEN 1 ELSE 0 END) as Inactive
FROM accounts;
"
echo ""
echo "Inactive Accounts:"
sqlite3 twitter_accounts.db "SELECT username, error_msg FROM accounts WHERE active = 0;"
echo "======================================"
EOF

chmod +x check_accounts.sh
```

Then run: `./check_accounts.sh`

## Watch Mode (Auto-refresh every 5 seconds)

```bash
watch -n 5 "sqlite3 -header -column twitter_accounts.db 'SELECT username, CASE WHEN active = 1 THEN \"ACTIVE\" ELSE \"INACTIVE\" END as status, error_msg FROM accounts;'"
```

Press Ctrl+C to stop.
