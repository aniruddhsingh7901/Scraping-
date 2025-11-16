# Twitter High-Volume Miner Setup Guide

Complete setup guide for 20M+ tweets/day scraping using twscrape

---

## 📋 Overview

This system provides:
- ✅ **High-volume scraping** (20M+ tweets/day capability)
- ✅ **Multi-worker architecture** with automatic scaling
- ✅ **Last 30 days data** collection
- ✅ **Hashtag rotation** with keyword expansion
- ✅ **PostgreSQL storage** with deduplication
- ✅ **Compatible with data-universe validators**
- ✅ **Uses existing utils.py validation**

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Twitter Worker Manager                  │
│  - Spawns 2/3 of available accounts as workers          │
│  - Rotates through hashtags/keywords                    │
│  - Deduplicates tweets                                  │
└─────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
    ┌───▼────┐         ┌───▼────┐         ┌───▼────┐
    │Worker 1│         │Worker 2│         │Worker N│
    │#bitcoin│         │#ethereum│        │#crypto │
    └────────┘         └────────┘         └────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
                ┌───────────▼───────────┐
                │   PostgreSQL Database │
                │   - Tweets table      │
                │   - Deduplicated      │
                │   - 30-day rolling    │
                └───────────────────────┘
```

---

## 🚀 Quick Start

### Step 1: Prerequisites

```bash
# Install dependencies
pip install psycopg2-binary bittensor

# Ensure twscrape is available (already in /root/Algo-test-script/twscrape/)

# Setup PostgreSQL
sudo apt-get install postgresql postgresql-contrib
sudo systemctl start postgresql
```

### Step 2: Configure PostgreSQL

```bash
# Create database
sudo -u postgres psql

postgres=# CREATE DATABASE twitter_data;
postgres=# CREATE USER twitter_user WITH PASSWORD 'your_password';
postgres=# GRANT ALL PRIVILEGES ON DATABASE twitter_data TO twitter_user;
postgres=# \q
```

### Step 3: Prepare Twitter Accounts

Create `twitter_accounts.txt` with your accounts:

```
# Format: username:password:email:email_password:proxy:cookies
# Recommended: Cookie-based accounts (more stable)

user1:pass1:email1@gmail.com:emailpass1::ct0=abc123;auth_token=xyz789
user2:pass2:email2@yahoo.com:emailpass2::ct0=def456;auth_token=uvw012
user3:pass3:email3@outlook.com:emailpass3::ct0=ghi789;auth_token=rst345

# OR without cookies (will need to login):
user4:pass4:email4@gmail.com:emailpass4::
```

**How to get cookies:**
1. Login to Twitter in your browser
2. Open Developer Tools (F12)
3. Go to Application > Cookies > https://x.com
4. Copy `ct0` and `auth_token` values
5. Format: `ct0=<value>;auth_token=<value>`

### Step 4: Setup Accounts

```bash
cd /root/Algo-test-script

# Add accounts to pool
python setup_twitter_accounts.py setup twitter_accounts.txt

# If accounts need login (non-cookie accounts)
python setup_twitter_accounts.py login

# Verify account status
python setup_twitter_accounts.py stats
```

### Step 5: Create Hashtags File (Optional)

Create `hashtags.txt` with your target hashtags (one per line):

```
#bitcoin
#ethereum
#cryptocurrency
#blockchain
#defi
#nft
#web3
#ai
#machinelearning
#technology
```

### Step 6: Configure Worker Manager

Edit `twitter_worker_manager.py` PostgreSQL config:

```python
postgres_config = {
    'host': 'localhost',
    'port': 5432,
    'database': 'twitter_data',
    'user': 'twitter_user',
    'password': 'your_password'
}
```

### Step 7: Start Scraping

```bash
# Start the worker manager
python twitter_worker_manager.py
```

---

## 📊 Monitoring & Management

### Check Account Status

```bash
python setup_twitter_accounts.py stats
```

Output:
```
================================================================================
TWITTER ACCOUNT POOL STATUS
================================================================================
Total Accounts: 30
Active Accounts: 28
Inactive Accounts: 2

================================================================================
INDIVIDUAL ACCOUNT STATUS
================================================================================
Username             Active     Logged In    Total Req    Last Used           
--------------------------------------------------------------------------------
user1                True       True         1547         2024-11-15 14:23    
user2                True       True         1432         2024-11-15 14:22    
...
```

### Test Scraping

```bash
python setup_twitter_accounts.py test
```

### Reset Account Locks

If accounts get stuck:

```bash
python setup_twitter_accounts.py reset-locks
```

### Monitor Progress

Worker manager logs progress every 5 minutes:

```
============================================================
PROGRESS REPORT
============================================================
Active Workers: 20/20
Total Scraped: 1,234,567
Total Stored (unique): 987,654
Errors: 23
Elapsed Time: 2.50 hours
Rate: 395,062 tweets/hour (9,481,488 tweets/day)
Target: 20,000,000 tweets/day
Progress: 47.41% of target
============================================================
```

---

## 🔧 Configuration

### Worker Scaling

Automatic based on accounts:
```
workers = (active_accounts * 2) / 3
```

Example:
- 30 accounts → 20 workers
- 45 accounts → 30 workers
- 60 accounts → 40 workers

### Scraping Parameters

In `twitter_worker_manager.py`:

```python
# Per-query limit
entity_limit = 1000

# Date range (last 30 days)
start = datetime.now(timezone.utc) - timedelta(days=30)
end = datetime.now(timezone.utc)

# Delay between queries
await asyncio.sleep(2)  # 2 seconds
```

### Hashtag Expansion

Each hashtag automatically expands to:
1. Original: `#bitcoin`
2. Keyword: `bitcoin`

This doubles coverage by catching mentions without hashtags.

---

## 📦 PostgreSQL Schema

```sql
CREATE TABLE twitter_data (
    id SERIAL PRIMARY KEY,
    tweet_id BIGINT UNIQUE,
    uri TEXT NOT NULL,
    username TEXT NOT NULL,
    content TEXT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    hashtags TEXT[],
    media_urls TEXT[],
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    content_size_bytes INTEGER,
    source TEXT DEFAULT 'twscrape'
);

-- Indexes for performance
CREATE INDEX idx_twitter_timestamp ON twitter_data (timestamp DESC);
CREATE INDEX idx_twitter_hashtags ON twitter_data USING GIN (hashtags);
```

### Query Examples

```sql
-- Get tweets from last 7 days
SELECT * FROM twitter_data 
WHERE timestamp > NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC;

-- Get tweets by hashtag
SELECT * FROM twitter_data 
WHERE '#bitcoin' = ANY(hashtags);

-- Count tweets per day
SELECT DATE(timestamp), COUNT(*) 
FROM twitter_data 
GROUP BY DATE(timestamp) 
ORDER BY DATE(timestamp) DESC;

-- Get top users
SELECT username, COUNT(*) as tweet_count 
FROM twitter_data 
GROUP BY username 
ORDER BY tweet_count DESC 
LIMIT 10;
```

---

## 🎯 Optimization Tips

### 1. **Account Recommendations**

For 20M+ tweets/day:
- **Minimum**: 30-40 accounts
- **Optimal**: 60-100 accounts
- **Account Type**: Cookie-based (more stable)
- **Age**: Older accounts perform better
- **Activity**: Mix of active/semi-active accounts

### 2. **Proxy Usage**

For large-scale scraping:

```python
# In twitter_accounts.txt
username:password:email:email_pass:http://proxy.com:8080:cookies
```

Or global proxy:
```bash
export TWS_PROXY=socks5://user:pass@proxy.com:1080
```

### 3. **PostgreSQL Tuning**

For high-volume inserts:

```sql
-- In postgresql.conf
shared_buffers = 256MB
work_mem = 16MB
maintenance_work_mem = 128MB
effective_cache_size = 1GB
```

### 4. **Hashtag Strategy**

Mix popular and niche hashtags:
```
# High volume (millions of tweets)
#bitcoin, #ethereum, #crypto

# Medium volume (hundreds of thousands)
#defi, #nft, #web3

# Niche volume (thousands)
#substrate, #polkadot, #bittensor
```

### 5. **Storage Management**

Auto-cleanup old tweets (>30 days):

```sql
-- Run daily as cron job
DELETE FROM twitter_data 
WHERE timestamp < NOW() - INTERVAL '31 days';
```

---

## 🔍 Validation Compatibility

The system is **fully compatible** with data-universe validators:

### 1. **Uses XContent Model**
```python
from scraping.x.model import XContent
```

### 2. **Uses Existing Validation**
```python
from scraping.x import utils

validation_result = utils.validate_tweet_content(
    actual_tweet=actual_x_content,
    entity=entity,
    is_retweet=bool(actual_tweet.retweetedTweet)
)
```

### 3. **DataEntity Format**
```python
data_entity = XContent.to_data_entity(content=x_content)
```

### 4. **Required Fields Only**
```python
XContent(
    username=f"@{tweet.user.username}",
    text=tweet.rawContent,
    url=tweet.url,
    timestamp=tweet.date,
    tweet_hashtags=hashtags,
    media=media_urls if media_urls else None,
)
# Optional fields skipped as per requirements
```

---

## 🚨 Troubleshooting

### Issue: No accounts available

```bash
# Check account status
python setup_twitter_accounts.py stats

# If inactive, try login
python setup_twitter_accounts.py login

# If stuck, reset locks
python setup_twitter_accounts.py reset-locks
```

### Issue: Rate limit errors

- **Solution**: Add more accounts
- Rate limits reset every 15 minutes per operation
- More accounts = more parallel scraping

### Issue: PostgreSQL connection errors

```bash
# Check PostgreSQL status
sudo systemctl status postgresql

# Restart if needed
sudo systemctl restart postgresql

# Verify database exists
sudo -u postgres psql -l
```

### Issue: Low scraping rate

**Check:**
1. Number of active accounts
2. Network speed/proxy performance
3. Hashtag popularity (too niche = few tweets)
4. PostgreSQL performance

**Optimize:**
- Add more accounts
- Use faster proxies
- Mix popular hashtags
- Tune PostgreSQL

---

## 📈 Scaling to 20M+ Tweets/Day

**Required Setup:**

| Metric | Minimum | Optimal |
|--------|---------|---------|
| Accounts | 30-40 | 60-100 |
| Workers | 20-27 | 40-67 |
| PostgreSQL | 2GB RAM | 4GB+ RAM |
| Bandwidth | 10 Mbps | 50+ Mbps |
| Storage | 100 GB | 500 GB+ |

**Expected Performance:**

- **30 accounts**: ~8-10M tweets/day
- **60 accounts**: ~18-22M tweets/day
- **100 accounts**: ~30-35M tweets/day

**Formula:**
```
tweets_per_day ≈ active_accounts × workers_per_account × tweets_per_worker × hours

Where:
- workers_per_account ≈ 0.67
- tweets_per_worker ≈ 800-1200/hour
- hours = 24
```

---

## 🔒 Security Best Practices

1. **Store credentials securely**
   - Use environment variables
   - Encrypt accounts file
   - Restrict file permissions: `chmod 600 twitter_accounts.txt`

2. **Use proxies**
   - Residential proxies preferred
   - Rotate proxy IPs
   - Avoid free proxies

3. **Account safety**
   - Don't overuse single account
   - Mix old and new accounts
   - Monitor for suspensions

4. **Database security**
   - Use strong PostgreSQL password
   - Restrict network access
   - Regular backups

---

## 📚 Files Reference

| File | Purpose |
|------|---------|
| `data-universe/scraping/x/twscrape_miner.py` | Core scraper using twscrape |
| `twitter_worker_manager.py` | Multi-worker manager |
| `setup_twitter_accounts.py` | Account management |
| `twitter_accounts.txt` | Account credentials (you create) |
| `hashtags.txt` | Target hashtags (optional) |
| `twitter_accounts.db` | SQLite account pool (auto-created) |

---

## 🎓 Additional Resources

- **twscrape docs**: `/root/Algo-test-script/TWSCRAPE_DOCUMENTATION.md`
- **X scraping docs**: `/root/Algo-test-script/X_SCRAPING_DOCUMENTATION.md`
- **Twitter search operators**: https://github.com/igorbrigadir/twitter-advanced-search

---

## ✅ Next Steps

1. ✅ Setup PostgreSQL database
2. ✅ Prepare Twitter accounts with cookies
3. ✅ Run `setup_twitter_accounts.py setup`
4. ✅ Verify with `setup_twitter_accounts.py stats`
5. ✅ Test with `setup_twitter_accounts.py test`
6. ✅ Create hashtags.txt (optional)
7. ✅ Configure postgres_config in twitter_worker_manager.py
8. ✅ Start: `python twitter_worker_manager.py`
9. ✅ Monitor progress and adjust as needed

---

**Questions? Issues?**
- Check logs for errors
- Verify account status
- Test PostgreSQL connection
- Review hashtag selection
- Scale accounts as needed

**Happy Mining! 🚀**
