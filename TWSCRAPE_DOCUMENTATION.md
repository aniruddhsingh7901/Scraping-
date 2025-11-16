# twscrape - Twitter GraphQL API Implementation

## 📁 Overview

**twscrape** is a Python library that provides a complete Twitter/X GraphQL API implementation with SNScrape-compatible data models. It's designed for efficient, large-scale Twitter data collection with automatic account rotation to handle rate limits.

**Location**: `/root/Algo-test-script/twscrape/`

---

## 🏗️ Architecture

### Directory Structure
```
twscrape/
├── readme.md                    # Main documentation
├── pyproject.toml              # Package configuration
├── LICENSE                     # MIT License
├── Makefile                    # Build automation
├── twscrape/                   # Main package
│   ├── __init__.py            # Package exports
│   ├── api.py                 # Main API interface
│   ├── account.py             # Account model
│   ├── accounts_pool.py       # Account management
│   ├── models.py              # Data models (Tweet, User, etc.)
│   ├── db.py                  # SQLite database operations
│   ├── login.py               # Login flow implementation
│   ├── queue_client.py        # Request queue management
│   ├── imap.py                # Email verification via IMAP
│   ├── logger.py              # Logging configuration
│   ├── cli.py                 # Command-line interface
│   ├── utils.py               # Utility functions
│   └── xclid.py               # X-Client-Id generation
├── tests/                      # Test suite
└── examples/                   # Usage examples
```

---

## 🔑 Core Components

### 1. **API Class** (`api.py`)

Main interface for Twitter data scraping.

#### GraphQL Operations Available:
```python
# Search Operations
OP_SearchTimeline              # Search tweets with queries
OP_GenericTimelineById         # Trends timeline

# User Operations
OP_UserByRestId                # Get user by ID
OP_UserByScreenName            # Get user by username
OP_Followers                   # Get followers
OP_BlueVerifiedFollowers       # Get verified followers
OP_Following                   # Get following
OP_UserCreatorSubscriptions    # Get subscriptions

# Tweet Operations
OP_TweetDetail                 # Get tweet details & replies
OP_UserTweets                  # Get user's tweets
OP_UserTweetsAndReplies        # Get tweets + replies
OP_UserMedia                   # Get user's media tweets
OP_Retweeters                  # Get retweeters
OP_Bookmarks                   # Get bookmarks

# List Operations
OP_ListLatestTweetsTimeline    # Get list timeline
```

#### Key Methods:

**Search Methods:**
```python
async def search(q: str, limit=-1, kv: KV = None) -> AsyncGenerator[Tweet]
async def search_raw(q: str, limit=-1, kv: KV = None) -> AsyncGenerator[Response]
async def search_user(q: str, limit=-1, kv: KV = None) -> AsyncGenerator[User]
```

**User Methods:**
```python
async def user_by_id(uid: int, kv: KV = None) -> User | None
async def user_by_login(login: str, kv: KV = None) -> User | None
async def followers(uid: int, limit=-1, kv: KV = None) -> AsyncGenerator[User]
async def following(uid: int, limit=-1, kv: KV = None) -> AsyncGenerator[User]
async def verified_followers(uid: int, limit=-1, kv: KV = None) -> AsyncGenerator[User]
async def subscriptions(uid: int, limit=-1, kv: KV = None) -> AsyncGenerator[User]
```

**Tweet Methods:**
```python
async def tweet_details(twid: int, kv: KV = None) -> Tweet | None
async def tweet_replies(twid: int, limit=-1, kv: KV = None) -> AsyncGenerator[Tweet]
async def user_tweets(uid: int, limit=-1, kv: KV = None) -> AsyncGenerator[Tweet]
async def user_tweets_and_replies(uid: int, limit=-1, kv: KV = None) -> AsyncGenerator[Tweet]
async def user_media(uid: int, limit=-1, kv: KV = None) -> AsyncGenerator[Tweet]
async def retweeters(twid: int, limit=-1, kv: KV = None) -> AsyncGenerator[User]
```

**Other Methods:**
```python
async def list_timeline(list_id: int, limit=-1, kv: KV = None) -> AsyncGenerator[Tweet]
async def trends(trend_id: TrendId, limit=-1, kv: KV = None) -> AsyncGenerator[Trend]
async def bookmarks(limit=-1, kv: KV = None) -> AsyncGenerator[Tweet]
```

#### GraphQL Features Configuration:
```python
GQL_FEATURES = {
    "articles_preview_enabled": False,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "communities_web_enable_tweet_community_results_fetch": True,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    # ... 30+ features
}
```

---

### 2. **Account Management** (`accounts_pool.py`)

Manages Twitter account pool with automatic rotation and rate limit handling.

#### AccountsPool Class:

**Initialization:**
```python
class AccountsPool:
    def __init__(
        db_file="accounts.db",
        login_config: LoginConfig | None = None,
        raise_when_no_account=False
    )
```

**Account Operations:**
```python
# Add accounts
async def add_account(username, password, email, email_password, 
                     user_agent=None, proxy=None, cookies=None, mfa_code=None)
async def load_from_file(filepath: str, line_format: str)

# Login operations
async def login(account: Account) -> bool
async def login_all(usernames: list[str] | None = None) -> dict
async def relogin(usernames: str | list[str])
async def relogin_failed()

# Account management
async def delete_accounts(usernames: str | list[str])
async def delete_inactive()
async def set_active(username: str, active: bool)
async def get(username: str) -> Account
async def get_all() -> list[Account]

# Queue management
async def get_for_queue(queue: str) -> Account | None
async def get_for_queue_or_wait(queue: str) -> Account | None
async def lock_until(username: str, queue: str, unlock_at: int, req_count=0)
async def unlock(username: str, queue: str, req_count=0)
async def reset_locks()

# Statistics
async def stats() -> dict
async def accounts_info() -> list[AccountInfo]
async def next_available_at(queue: str) -> str | None
```

#### Account Model (`account.py`):
```python
@dataclass
class Account(JSONTrait):
    username: str
    password: str
    email: str
    email_password: str
    user_agent: str
    active: bool
    locks: dict           # Rate limit locks per operation
    stats: dict           # Request statistics per operation
    headers: dict         # Request headers
    cookies: dict         # Session cookies
    proxy: str | None
    mfa_code: str | None
    last_used: datetime | None
    error_msg: str | None
    _tx: str | None
```

#### Rate Limit Management:
- **15-minute windows** per operation endpoint
- **Automatic account rotation** when rate limits hit
- **Queue-based locking** system
- **Configurable wait behavior** (raise exception or wait)

---

### 3. **Data Models** (`models.py`)

SNScrape-compatible data structures for parsed Twitter data.

#### Tweet Model:
```python
@dataclass
class Tweet(JSONTrait):
    # Basic fields
    id: int
    id_str: str
    url: str
    date: datetime
    user: User
    lang: str
    rawContent: str
    
    # Engagement metrics
    replyCount: int
    retweetCount: int
    likeCount: int
    quoteCount: int
    bookmarkedCount: int
    viewCount: int | None
    
    # Thread information
    conversationId: int
    conversationIdStr: str
    inReplyToTweetId: int | None
    inReplyToTweetIdStr: str | None
    inReplyToUser: UserRef | None
    
    # Content elements
    hashtags: list[str]
    cashtags: list[str]
    mentionedUsers: list[UserRef]
    links: list[TextLink]
    media: Media
    
    # Related tweets
    retweetedTweet: Optional[Tweet]
    quotedTweet: Optional[Tweet]
    
    # Location
    place: Optional[Place]
    coordinates: Optional[Coordinates]
    
    # Source
    source: str | None
    sourceUrl: str | None
    sourceLabel: str | None
    
    # Card/Poll
    card: Union[None, SummaryCard, PollCard, BroadcastCard, AudiospaceCard]
    
    # Flags
    possibly_sensitive: bool | None
    
    _type: str = "snscrape.modules.twitter.Tweet"
```

#### User Model:
```python
@dataclass
class User(JSONTrait):
    # Identity
    id: int
    id_str: str
    url: str
    username: str
    displayname: str
    rawDescription: str
    
    # Stats
    created: datetime
    followersCount: int
    friendsCount: int
    statusesCount: int
    favouritesCount: int
    listedCount: int
    mediaCount: int
    
    # Profile
    location: str
    profileImageUrl: str
    profileBannerUrl: str | None
    
    # Verification
    protected: bool | None
    verified: bool | None
    blue: bool | None
    blueType: str | None
    
    # Additional
    descriptionLinks: list[TextLink]
    pinnedIds: list[int]
    
    _type: str = "snscrape.modules.twitter.User"
```

#### Media Models:
```python
@dataclass
class Media(JSONTrait):
    photos: list[MediaPhoto]
    videos: list[MediaVideo]
    animated: list[MediaAnimated]

@dataclass
class MediaPhoto(JSONTrait):
    url: str

@dataclass
class MediaVideo(JSONTrait):
    thumbnailUrl: str
    variants: list[MediaVideoVariant]
    duration: int
    views: int | None

@dataclass
class MediaVideoVariant(JSONTrait):
    contentType: str
    bitrate: int
    url: str
```

#### Card Models:
```python
@dataclass
class SummaryCard(Card):
    title: str
    description: str
    vanityUrl: str
    url: str
    photo: MediaPhoto | None
    video: MediaVideo | None

@dataclass
class PollCard(Card):
    options: list[PollOption]
    finished: bool

@dataclass
class BroadcastCard(Card):
    title: str
    url: str
    photo: MediaPhoto | None

@dataclass
class AudiospaceCard(Card):
    url: str
```

#### Trend Model:
```python
@dataclass
class Trend(JSONTrait):
    id: Optional[str]
    rank: Optional[str | int]
    name: str
    trend_url: TrendUrl
    trend_metadata: TrendMetadata
    grouped_trends: list[GroupedTrend]
```

---

### 4. **Login System** (`login.py`)

Handles Twitter authentication flow including 2FA and email verification.

#### Login Flow:
```python
async def login(acc: Account, cfg: LoginConfig | None = None) -> Account
```

**Login Steps:**
1. Get guest token
2. Login initiate
3. Enter username (alternate identifier if needed)
4. Enter password
5. Handle challenges:
   - Two-factor authentication
   - Email verification (via IMAP)
   - Duplication check
6. Login success

#### LoginConfig:
```python
@dataclass
class LoginConfig:
    # Email verification settings
    email_first: bool = False
    manual_email_code: bool = False
    wait_email_code: int = 30  # seconds
```

#### Email Verification (IMAP):
```python
async def imap_get_email_code(
    email: str, 
    password: str, 
    imap_domain: str, 
    min_timestamp: datetime | None,
    timeout: int = 30
) -> str
```

**Supported Email Providers:**
- Gmail, Yahoo, Outlook, iCloud
- Custom IMAP domains can be added

---

### 5. **Queue Client** (`queue_client.py`)

Manages request queuing, rate limiting, and error handling.

#### QueueClient Class:
```python
class QueueClient:
    def __init__(
        pool: AccountsPool, 
        queue: str, 
        debug=False, 
        proxy: str | None = None
    )
    
    async def get(url: str, params: ReqParams = None) -> Response | None
    async def req(method: str, url: str, params: ReqParams = None) -> Response | None
```

**Features:**
- **Automatic account selection** from pool
- **Rate limit detection** and handling
- **15-minute lock windows** per operation
- **X-Client-Id header generation** for requests
- **Error handling** with account marking
- **Request/response logging** in debug mode

#### Error Handling:
- `HandledError` - Known errors (rate limits, etc.)
- `AbortReqError` - Abort request without retrying
- Automatic account deactivation on repeated failures

---

### 6. **Database** (`db.py`)

SQLite-based persistent storage for accounts and state.

#### Database Schema:
```sql
CREATE TABLE accounts (
    username TEXT PRIMARY KEY,
    password TEXT,
    email TEXT,
    email_password TEXT,
    user_agent TEXT,
    active INTEGER,
    locks TEXT,        -- JSON object
    stats TEXT,        -- JSON object
    headers TEXT,      -- JSON object
    cookies TEXT,      -- JSON object
    proxy TEXT,
    mfa_code TEXT,
    last_used TEXT,
    error_msg TEXT,
    _tx TEXT
);
```

#### DB Operations:
```python
async def execute(db_path: str, qs: str, params: dict | None = None)
async def fetchone(db_path: str, qs: str, params: dict | None = None)
async def fetchall(db_path: str, qs: str, params: dict | None = None)
async def executemany(db_path: str, qs: str, params: list[dict])
```

#### Migrations:
- **v1-v4 migrations** for schema evolution
- **Automatic migration** on first run
- **Version checking** to ensure compatibility

---

### 7. **X-Client-Id Generation** (`xclid.py`)

Generates required X-Client-Transaction-Id headers for Twitter API requests.

#### XClIdGen Class:
```python
class XClIdGen:
    async def create(clt: httpx.AsyncClient | None = None) -> "XClIdGen"
    def calc(method: str, path: str) -> str
```

**Process:**
1. Fetch Twitter's main.js bundle
2. Parse animation key bytes (vk_bytes)
3. Extract cubic curve parameters
4. Generate transaction IDs using animation interpolation

**Caching:**
- Cached per account username
- Fresh generation on demand
- Stored in XClIdGenStore

---

## 🔧 Usage Examples

### Basic Setup & Usage

```python
import asyncio
from twscrape import API, gather

async def main():
    api = API()  # Uses accounts.db by default
    
    # Add accounts (choose one method)
    
    # Method 1: Cookie-based (more stable)
    cookies = "abc=12; ct0=xyz"
    await api.pool.add_account(
        "user1", "pass1", "email@example.com", "email_pass1",
        cookies=cookies
    )
    
    # Method 2: Login-based (requires IMAP email)
    await api.pool.add_account(
        "user2", "pass2", "email2@example.com", "email_pass2"
    )
    await api.pool.login_all()
    
    # Search tweets
    tweets = await gather(api.search("bitcoin", limit=100))
    for tweet in tweets:
        print(f"{tweet.user.username}: {tweet.rawContent}")
    
    # Get user info
    user = await api.user_by_login("elonmusk")
    print(f"{user.displayname} has {user.followersCount} followers")
    
    # Get user's tweets
    async for tweet in api.user_tweets(user.id, limit=50):
        print(f"{tweet.date}: {tweet.rawContent}")
    
    # Get tweet details
    tweet = await api.tweet_details(1234567890)
    print(f"Likes: {tweet.likeCount}, Retweets: {tweet.retweetCount}")
    
    # Get followers
    async for follower in api.followers(user.id, limit=100):
        print(f"@{follower.username}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Advanced Search

```python
# Search with filters
queries = [
    "bitcoin lang:en",              # Language filter
    "ethereum since:2024-01-01",    # Date filter
    "#crypto min_faves:100",        # Minimum likes
    "from:elonmusk -filter:retweets", # Exclude retweets
    "to:elonmusk filter:replies",   # Only replies
]

for query in queries:
    tweets = await gather(api.search(query, limit=20))
    print(f"Found {len(tweets)} tweets for: {query}")
```

### Using Different Search Products

```python
# Latest tweets (default)
await gather(api.search("elon musk", limit=20))

# Top tweets
await gather(api.search("elon musk", limit=20, kv={"product": "Top"}))

# Media tweets
await gather(api.search("elon musk", limit=20, kv={"product": "Media"}))

# People search
await gather(api.search_user("elon musk", limit=20))
```

### Breaking from Loops Correctly

```python
from contextlib import aclosing

async with aclosing(api.search("bitcoin")) as gen:
    async for tweet in gen:
        print(tweet.rawContent)
        if tweet.likeCount > 1000:
            break  # Properly releases account lock
```

### Using Proxies

```python
# Option 1: Per-account proxy
await api.pool.add_account(
    "user1", "pass1", "email@example.com", "email_pass1",
    proxy="http://user:pass@proxy.com:8080"
)

# Option 2: Global proxy
api = API(proxy="socks5://user:pass@127.0.0.1:1080")

# Option 3: Environment variable
# TWS_PROXY=socks5://user:pass@127.0.0.1:1080

# Option 4: Change proxy dynamically
api.proxy = "http://newproxy.com:8080"
user = await api.user_by_login("elonmusk")
api.proxy = None  # Disable proxy
```

---

## 🖥️ CLI Usage

### Account Management

```bash
# Add accounts from file
twscrape add_accounts accounts.txt username:password:email:email_password:_:cookies

# Login to accounts
twscrape login_accounts
twscrape login_accounts --manual  # Manual email verification

# List accounts
twscrape accounts

# Re-login specific accounts
twscrape relogin user1 user2

# Re-login all failed accounts
twscrape relogin_failed

# Use different database
twscrape --db test-accounts.db accounts
```

### Data Collection

```bash
# Search tweets
twscrape search "bitcoin" --limit=100 > tweets.jsonl
twscrape search "ethereum lang:en" --limit=50 --raw > raw_tweets.json

# User operations
twscrape user_by_login elonmusk
twscrape user_by_id 44196397
twscrape user_tweets 44196397 --limit=100
twscrape user_tweets_and_replies 44196397 --limit=50
twscrape user_media 44196397 --limit=20

# Tweet operations
twscrape tweet_details 1234567890
twscrape tweet_replies 1234567890 --limit=20
twscrape retweeters 1234567890 --limit=50

# Follow relationships
twscrape followers 44196397 --limit=100
twscrape following 44196397 --limit=100
twscrape verified_followers 44196397 --limit=50
twscrape subscriptions 44196397 --limit=20

# Trends
twscrape trends trending
twscrape trends news
twscrape trends sport
```

### Account File Format

Example `accounts.txt`:
```
user1:pass1:email1@gmail.com:emailpass1::cookie_string_1
user2:pass2:email2@yahoo.com:emailpass2::cookie_string_2
```

Format tokens:
- `username` - Required
- `password` - Required
- `email` - Required
- `email_password` - Required for IMAP verification
- `_` - Skip column (e.g., user_agent)
- `cookies` - Optional cookie string

---

## ⚙️ Configuration

### Environment Variables

```bash
# Global proxy
TWS_PROXY=socks5://user:pass@127.0.0.1:1080

# Email verification timeout (seconds)
TWS_WAIT_EMAIL_CODE=30

# Raise exception when no accounts available (instead of waiting)
TWS_RAISE_WHEN_NO_ACCOUNT=true
```

### Rate Limits

**Current Twitter Rate Limits (Updated regularly):**
- Reset every **15 minutes** per endpoint
- Limits vary by:
  - Account age
  - Verification status
  - Previous activity

**Known Limits:**
- `user_tweets` & `user_tweets_and_replies`: ~3200 tweets max
- Search: Variable based on account
- Followers/Following: Pagination-based

### Proxy Configuration

**Proxy Priority:**
1. `api.proxy` (highest)
2. Environment variable `TWS_PROXY`
3. Per-account proxy (lowest)

**Supported Proxy Types:**
- HTTP/HTTPS
- SOCKS4/SOCKS5

---

## 🔍 Advanced Features

### Parallel Scraping

```python
import asyncio
from twscrape import API, gather

async def scrape_multiple_users(usernames):
    api = API()
    
    tasks = []
    for username in usernames:
        task = api.user_by_login(username)
        tasks.append(task)
    
    users = await asyncio.gather(*tasks)
    return users

# Run
usernames = ["elonmusk", "BillGates", "JeffBezos"]
users = asyncio.run(scrape_multiple_users(usernames))
```

### Raw Response Access

```python
# All methods have _raw version
async for response in api.search_raw("bitcoin"):
    print(response.status_code)
    print(response.headers)
    data = response.json()
    # Process raw GraphQL response
```

### Custom GraphQL Features

```python
# Override default features
custom_features = {
    "longform_notetweets_consumption_enabled": False,
    "custom_feature": True,
}

tweets = await gather(api.search(
    "bitcoin",
    limit=20,
    kv={"features": custom_features}
))
```

### Account Statistics

```python
# Get pool statistics
stats = await api.pool.stats()
print(f"Total accounts: {stats['total']}")
print(f"Active accounts: {stats['active']}")
print(f"Locked accounts: {stats['locked_SearchTimeline']}")

# Get detailed account info
accounts_info = await api.pool.accounts_info()
for acc in accounts_info:
    print(f"{acc['username']}: {acc['total_req']} requests, active={acc['active']}")
```

### Manual Account Lock Management

```python
# Reset all locks
await api.pool.reset_locks()

# Lock account for specific operation
await api.pool.lock_until("user1", "SearchTimeline", utc.ts() + 900)

# Unlock account
await api.pool.unlock("user1", "SearchTimeline")
```

---

## 🚨 Error Handling

### Common Exceptions

```python
from twscrape.accounts_pool import NoAccountError
from httpx import HTTPStatusError

try:
    tweets = await gather(api.search("bitcoin", limit=100))
except NoAccountError:
    print("No accounts available. Add more accounts or wait.")
except HTTPStatusError as e:
    print(f"HTTP error: {e.response.status_code}")
except Exception as e:
    print(f"Error: {e}")
```

### Account Status Monitoring

```python
# Check account status
accounts = await api.pool.get_all()
for acc in accounts:
    if not acc.active:
        print(f"Inactive: {acc.username} - {acc.error_msg}")
    
    # Check locks
    for queue, lock_time in acc.locks.items():
        print(f"{acc.username} locked on {queue} until {lock_time}")
```

### Automatic Recovery

- **Rate limit errors**: Account automatically locked for 15 min
- **Login errors**: Account marked inactive, can be relogged
- **Network errors**: Retried with exponential backoff
- **No account available**: Waits for next available (or raises exception)

---

## 📊 Data Export

### Export to JSON

```python
import json

tweets = await gather(api.search("bitcoin", limit=100))

# Export as JSON Lines
with open("tweets.jsonl", "w") as f:
    for tweet in tweets:
        f.write(tweet.json() + "\n")

# Export as JSON array
with open("tweets.json", "w") as f:
    json.dump([t.dict() for t in tweets], f, indent=2, default=str)
```

### Export to CSV

```python
import csv

tweets = await gather(api.search("bitcoin", limit=100))

with open("tweets.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["ID", "Username", "Date", "Content", "Likes", "Retweets"])
    
    for tweet in tweets:
        writer.writerow([
            tweet.id,
            tweet.user.username,
            tweet.date,
            tweet.rawContent,
            tweet.likeCount,
            tweet.retweetCount
        ])
```

---

## 🔐 Security & Best Practices

### Account Safety

1. **Use cookie-based accounts** for better stability
2. **Rotate accounts regularly** to distribute load
3. **Use proxies** to avoid IP bans
4. **Monitor account health** via `accounts_info()`
5. **Don't overuse** a single account

### Data Collection Ethics

1. **Respect rate limits** - Don't try to bypass them
2. **Follow Twitter ToS** - Understand usage restrictions
3. **Private data** - Respect protected accounts
4. **Bulk collection** - Use responsibly

### Cookie Management

```python
# Extract cookies from browser
cookies = "ct0=xyz123; auth_token=abc456; ..."

# Or as JSON
cookies = '{"ct0": "xyz123", "auth_token": "abc456"}'

# Or as base64
import base64
cookies_b64 = base64.b64encode(cookies.encode()).decode()
```

---

## 🎯 Key Features Summary

### ✅ **Advantages**

1. **Async/Await** - High-performance concurrent operations
2. **Automatic Rate Limit Handling** - Smart account rotation
3. **Cookie Support** - More stable than login-based
4. **SNScrape Models** - Compatible with existing tools
5. **GraphQL API** - Direct Twitter API access
6. **Raw Responses** - Full control over data
7. **Proxy Support** - Per-account or global
8. **CLI Interface** - Easy command-line usage
9. **Email Verification** - IMAP-based 2FA
10. **Persistent Storage** - SQLite-based account management

### ⚠️ **Limitations**

1. **Account Requirements** - Needs valid Twitter accounts
2. **Rate Limits** - Subject to Twitter's restrictions
3. **Email IMAP** - Not all providers supported
4. **Ban Risk** - Heavy usage may result in account suspension
5. **Data Limits** - Some endpoints limited (e.g., 3200 tweets)

---

## 📦 Dependencies

```toml
[tool.poetry.dependencies]
python = "^3.11"
httpx = {extras = ["http2", "socks"], version = "^0.25.2"}
fake-useragent = "^1.5.1"
beautifulsoup4 = "^4.12.2"
aiosqlite = "^0.19.0"
```

---

## 🔗 Integration with Algo-test-script

The twscrape library is used within the main scraping system but as a **separate module**. It's not directly integrated with the `data-universe/scraping/x/` modules but can be used as an alternative or supplementary scraping method.

**Potential Use Cases:**
- Backup scraping when Apify actors are unavailable
- Direct GraphQL API access for specific operations
- Account pool management for distributed scraping
- Real-time data collection with lower latency

---

## 📚 References

- **GitHub**: https://github.com/vladkens/twscrape
- **PyPI**: https://pypi.org/project/twscrape
- **SNScrape Compatibility**: Data models match SNScrape format
- **Twitter GraphQL**: Uses official Twitter GraphQL endpoints

---

**Documentation Generated**: 2025-11-15  
**Library Version**: Latest from repository  
**Python Requirement**: 3.11+
