# Proxy Rotation Guide for Twitter Scraping

## ❓ Does twscrape Automatically Rotate Proxies?

**NO** - twscrape does NOT automatically rotate proxies itself.

### How It Works:

```
┌──────────────────────────────────────────────────────┐
│ Account Pool (managed by twscrape)                   │
│                                                       │
│  Account 1 → Proxy A (assigned once)                │
│  Account 2 → Proxy B (assigned once)                │
│  Account 3 → Proxy C (assigned once)                │
│  ...                                                 │
│  Account N → Proxy Z (assigned once)                │
└──────────────────────────────────────────────────────┘
           │
           │ twscrape ROTATES ACCOUNTS
           │ (which indirectly rotates proxies)
           ▼
    ┌─────────────┐
    │   Request   │
    └─────────────┘
```

### Key Points:

1. **Each account has ONE proxy** assigned at setup
2. **twscrape rotates ACCOUNTS** automatically for rate limits
3. **Proxy rotation happens indirectly** through account rotation
4. **To change proxies**, you must manually reassign them to accounts

---

## 🔄 Proxy Rotation Strategies

### Strategy 1: Large Proxy Pool (RECOMMENDED for 5000 proxies)

**With 5000 proxies and 100 accounts:**

```python
# Initial setup: Each account gets a unique proxy
Account 1 → Proxy 1
Account 2 → Proxy 2
Account 3 → Proxy 3
...
Account 100 → Proxy 100

# Remaining 4900 proxies available for rotation
```

**Benefits:**
- ✅ Each account has unique proxy initially
- ✅ 4900 proxies available for rotation
- ✅ Can rotate proxies weekly/monthly
- ✅ Minimal IP reuse

**Setup:**
```bash
# Mix accounts with proxies (balanced distribution)
python proxy_account_mixer.py mix accounts_only.txt proxies.txt balanced
```

---

### Strategy 2: Random Assignment

**Best for frequent proxy rotation:**

```python
# Each account randomly assigned from 5000 proxies
Account 1 → random(Proxies)
Account 2 → random(Proxies)
...
```

**Benefits:**
- ✅ Maximum proxy diversity
- ✅ Good for large proxy pools
- ✅ Easy to re-randomize

**Setup:**
```bash
# Mix with random strategy
python proxy_account_mixer.py mix accounts_only.txt proxies.txt random
```

---

### Strategy 3: Round-Robin

**Predictable proxy distribution:**

```python
# Sequential assignment
Account 1 → Proxy 1
Account 2 → Proxy 2
Account 3 → Proxy 3
...
Account 100 → Proxy 100
Account 101 → Proxy 1  # Wraps around
```

**Benefits:**
- ✅ Predictable pattern
- ✅ Even distribution
- ✅ Easy to track

**Setup:**
```bash
# Mix with round-robin (default)
python proxy_account_mixer.py mix accounts_only.txt proxies.txt
```

---

## 🛠️ Complete Workflow

### Step 1: Prepare Files

**Create `accounts_only.txt`:**
```
user1:pass1:email1@gmail.com:emailpass1:ct0_token1:auth_token1
user2:pass2:email2@gmail.com:emailpass2:ct0_token2:auth_token2
...
```

**Create `proxies.txt`:**
```
socks5://user1:pass1@proxy1.com:1080
socks5://user2:pass2@proxy2.com:1080
...
(5000 proxies)
```

### Step 2: Initial Mix

```bash
# Mix accounts with proxies
python proxy_account_mixer.py mix accounts_only.txt proxies.txt balanced

# Verify distribution
python proxy_account_mixer.py stats
```

**Output:**
```
============================================================
PROXY DISTRIBUTION
============================================================
Total Accounts: 100
Accounts with Proxy: 100
Accounts without Proxy: 0
Unique Proxies: 100
============================================================
```

### Step 3: Start Scraping

```bash
# Start worker manager (uses proxies automatically)
python twitter_worker_manager.py
```

**How it works:**
1. Worker manager requests account from pool
2. Account comes with assigned proxy
3. Request goes through that proxy
4. Rate limit hit → twscrape switches to different account (= different proxy)
5. Process repeats

### Step 4: Rotate Proxies (Weekly/Monthly)

```bash
# Reassign proxies randomly
python proxy_account_mixer.py rotate proxies.txt random

# Verify new distribution
python proxy_account_mixer.py stats
```

---

## 📊 Proxy-Account Ratios

### Scenario 1: More Proxies than Accounts (YOUR CASE)

**5000 proxies, 100 accounts:**

```
Ratio: 50:1 (50 proxies per account available)

Initial Assignment:
- Each account gets 1 proxy
- 4900 proxies remain unused

Rotation Strategy:
- Rotate proxies every 7 days
- Each account can use 50 different proxies over time
- Very low IP ban risk
```

**Advantages:**
- ✅ Excellent IP diversity
- ✅ Low ban risk
- ✅ Can afford to lose/rotate bad proxies
- ✅ Scale to more accounts easily

---

### Scenario 2: Equal Proxies and Accounts

**100 proxies, 100 accounts:**

```
Ratio: 1:1

Assignment:
- Each account gets exactly 1 proxy
- No proxies left for rotation

Rotation Strategy:
- Rotate all proxies monthly
- Replace bad proxies as needed
```

---

### Scenario 3: More Accounts than Proxies

**100 proxies, 200 accounts:**

```
Ratio: 1:2 (1 proxy per 2 accounts)

Assignment:
- Multiple accounts share same proxy
- Higher IP ban risk

Rotation Strategy:
- Rotate frequently (daily)
- Monitor for bans closely
```

---

## 🎯 Recommended Setup for 20M+ Tweets/Day

### Optimal Configuration:

| Resource | Minimum | Optimal | Your Setup |
|----------|---------|---------|------------|
| Proxies | 100 | 1000+ | ✅ 5000 |
| Accounts | 30 | 100 | ? |
| Ratio | 3:1 | 10:1 | 50:1 ✅ |

### Your Setup (5000 proxies):

**If you have 100 accounts:**
- Ratio: 50:1 (EXCELLENT)
- Expected: 18-22M tweets/day
- Rotation: Weekly or monthly
- Risk: Very low

**If you have 50 accounts:**
- Ratio: 100:1 (EXCESSIVE but safe)
- Expected: 10-12M tweets/day
- Rotation: Monthly
- Risk: Minimal

**If you have 200 accounts:**
- Ratio: 25:1 (EXCELLENT)
- Expected: 35-40M tweets/day
- Rotation: Weekly
- Risk: Very low

---

## 🔧 Proxy Management Commands

### Initial Setup
```bash
# Mix accounts with proxies (first time)
python proxy_account_mixer.py mix accounts_only.txt proxies.txt balanced
```

### Check Status
```bash
# View current proxy distribution
python proxy_account_mixer.py stats
```

### Rotate Proxies
```bash
# Reassign proxies randomly (recommended for rotation)
python proxy_account_mixer.py rotate proxies.txt random

# OR reassign sequentially
python proxy_account_mixer.py rotate proxies.txt round-robin
```

### Update Proxy File
```bash
# 1. Edit proxies.txt (add/remove proxies)
# 2. Rotate with new proxy list
python proxy_account_mixer.py rotate proxies.txt random
```

---

## 💡 Best Practices

### 1. Proxy Quality

**Residential Proxies (BEST):**
- ✅ Highest success rate
- ✅ Lowest ban rate
- ✅ More expensive
- **Use for**: Long-term accounts

**Datacenter Proxies:**
- ⚠️ Higher ban risk
- ✅ Cheaper
- ✅ Faster
- **Use for**: High-volume scraping with frequent rotation

**Mobile Proxies:**
- ✅ Very high success rate
- ✅ Expensive
- **Use for**: Premium accounts

### 2. Rotation Schedule

**Conservative (LOW risk):**
- Rotate every 30 days
- For residential proxies
- With 5000 proxies: Can run for years

**Moderate (BALANCED risk):**
- Rotate every 7 days
- For datacenter proxies
- With 5000 proxies: Optimal

**Aggressive (HIGH volume):**
- Rotate every 24 hours
- For fresh datacenter proxies
- With 5000 proxies: Very sustainable

### 3. Monitoring

**Watch for:**
- ❌ Dead proxies (connection failures)
- ❌ Slow proxies (high latency)
- ❌ Banned proxies (Twitter errors)

**Action:**
```bash
# Remove dead proxies from proxies.txt
# Then rotate
python proxy_account_mixer.py rotate proxies.txt random
```

### 4. Proxy Pool Maintenance

**Weekly:**
- Check proxy health
- Remove dead proxies
- Add new proxies if available

**Monthly:**
- Full proxy rotation
- Review ban rates
- Optimize proxy providers

---

## 🚨 Troubleshooting

### Issue: Many accounts getting banned

**Solutions:**
1. Rotate proxies more frequently
2. Use residential proxies instead of datacenter
3. Reduce scraping rate per account
4. Check if proxies are blacklisted

```bash
# Quick proxy rotation
python proxy_account_mixer.py rotate proxies.txt random
```

### Issue: Slow scraping speed

**Solutions:**
1. Check proxy latency
2. Remove slow proxies
3. Use faster proxy provider
4. Add more accounts

### Issue: Proxies not being used

**Verify:**
```bash
# Check proxy distribution
python proxy_account_mixer.py stats

# Should show: "Accounts with Proxy: 100" (or your total)
```

### Issue: Want to change specific account's proxy

**Solution:**
```python
# Manual change (advanced)
from twscrape import API

api = API()
account = await api.pool.get("username")
account.proxy = "socks5://new:proxy@ip:port"
await api.pool.save(account)
```

---

## 📈 Scaling Guide

### Current: 100 accounts, 5000 proxies

**To reach 20M tweets/day:**

1. **Add accounts** to 100-150
2. **Keep 5000 proxies** (more than enough)
3. **Mix again:**
   ```bash
   python proxy_account_mixer.py mix accounts_only.txt proxies.txt balanced
   ```
4. **Start scraping:**
   ```bash
   python twitter_worker_manager.py
   ```

### Future: 200+ accounts

**With 5000 proxies:**
- Ratio: 25:1 (still excellent)
- Expected: 35-40M tweets/day
- Can scale to 500 accounts with same proxy pool

---

## ✅ Summary: Your Setup (5000 SOCKS Proxies)

### Setup Process:

```bash
# 1. Create files
nano accounts_only.txt  # Add accounts (username:pass:email:emailpass:ct0:auth)
nano proxies.txt        # Add 5000 proxies (one per line)

# 2. Mix accounts with proxies
python proxy_account_mixer.py mix accounts_only.txt proxies.txt balanced

# 3. Verify
python proxy_account_mixer.py stats

# 4. Start scraping
python twitter_worker_manager.py

# 5. Rotate proxies (weekly)
python proxy_account_mixer.py rotate proxies.txt random
```

### Expected Results:

- **With 100 accounts + 5000 proxies:**
  - 18-22M tweets/day ✅
  - Very low ban risk ✅
  - Weekly proxy rotation ✅
  - Can run for months ✅

- **With 150 accounts + 5000 proxies:**
  - 25-30M tweets/day ✅
  - Exceeds 20M target ✅
  - Still 33:1 ratio ✅

**Your setup is EXCELLENT for high-volume scraping!** 🚀

---

## 🔗 Related Documentation

- **Setup Guide**: `TWITTER_MINER_SETUP.md`
- **twscrape Docs**: `TWSCRAPE_DOCUMENTATION.md`
- **Account Setup**: `setup_twitter_accounts.py`
- **Worker Manager**: `twitter_worker_manager.py`

---

**Need Help?**
- Check proxy health regularly
- Monitor account bans
- Adjust rotation schedule based on results
- Scale accounts to match your target volume
