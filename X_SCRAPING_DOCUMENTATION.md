# X (Twitter) Scraping System - Complete Documentation

## 📁 Directory Structure
```
/root/Algo-test-script/data-universe/scraping/x/
├── __init__.py (empty initialization file)
├── model.py (Data model definitions)
├── utils.py (Validation and utility functions)
├── apidojo_scraper.py (Primary scraper - Apify Actor based)
├── microworlds_scraper.py (Alternative scraper - Apify Actor based)
└── quacker_url_scraper.py (Backup URL-based scraper - not actively used)
```

---

## 🔧 Available Scraping Methods

### 1. **ApiDojoTwitterScraper** (PRIMARY)
**File**: `apidojo_scraper.py`  
**Actor ID**: `61RPP7dywgiy0JPD0`  
**Status**: ✅ Active and preferred

#### Capabilities:
- ✅ **Scraping**: Search-based scraping with hashtags, keywords, usernames
- ✅ **Validation**: URL-based validation with engagement filtering
- ✅ **On-Demand Scraping**: Supports URL lookups and custom queries
- ✅ **Engagement Filtering**: Filters spam accounts and low-engagement tweets
- ✅ **Concurrent Processing**: Supports up to 20 concurrent validations

#### Key Features:
- Advanced engagement metrics extraction
- Spam account detection (min 50 followers, 30+ day old account)
- Low engagement filtering (min 50 views)
- Enhanced metadata extraction (user profiles, engagement counts)
- Shadowban detection capability

#### Methods:
```python
async def scrape(scrape_config: ScrapeConfig, allow_low_engagement: bool = False) -> List[DataEntity]
async def validate(entities: List[DataEntity], allow_low_engagement: bool = False) -> List[ValidationResult]
async def on_demand_scrape(usernames, keywords, url, keyword_mode, start_datetime, end_datetime, limit) -> List[DataEntity]
```

---

### 2. **MicroworldsTwitterScraper** (ALTERNATIVE)
**File**: `microworlds_scraper.py`  
**Actor ID**: `heLL6fUofdPgRXZie`  
**Status**: ✅ Active alternative

#### Capabilities:
- ✅ **Scraping**: Search-based scraping
- ✅ **Validation**: URL-based validation
- ❌ **On-Demand Scraping**: Not implemented
- ⚠️ **Engagement Filtering**: Basic support
- ✅ **Concurrent Processing**: Supports up to 20 concurrent validations

#### Key Features:
- Simpler data structure (fewer enhanced fields)
- Different API response format
- Good for basic tweet collection
- Less comprehensive metadata

---

### 3. **QuackerUrlScraper** (BACKUP)
**File**: `quacker_url_scraper.py`  
**Actor ID**: `KVJr35xjTw2XyvMeK`  
**Status**: ⚠️ Not actively used

#### Capabilities:
- ❌ **Scraping**: Not implemented
- ✅ **Validation**: URL-based validation only
- ❌ **On-Demand Scraping**: Not supported
- ❌ **Engagement Filtering**: No retweet detection

#### Notes:
- Designed as fallback during APIDojo outages
- Limited field extraction
- No engagement metrics
- Timestamp format: `%Y-%m-%dT%H:%M:%S.%fZ`

---

## 📦 Storage & Data Models

### **XContent Model** (`model.py`)
The core data structure for X/Twitter content.

#### ✅ **REQUIRED FIELDS** (Must always be provided):
```python
username: str               # Twitter username (with or without @)
text: str                   # Tweet text content
url: str                    # Tweet URL (x.com or twitter.com)
timestamp: dt.datetime      # Tweet timestamp (obfuscated to minute)
tweet_hashtags: List[str]   # Hashtags in order (default: [])
```

#### 🔧 **OPTIONAL BASIC FIELDS**:
```python
media: Optional[List[str]]              # Media URLs (images, videos)
```

#### 🌟 **ENHANCED OPTIONAL FIELDS**:

**User Information:**
```python
user_id: Optional[str]                  # Twitter user ID
user_display_name: Optional[str]        # Display name (not @username)
user_verified: Optional[bool]           # Legacy verified status
user_blue_verified: Optional[bool]      # Twitter Blue verification
user_description: Optional[str]         # Bio/description
user_location: Optional[str]            # User location
profile_image_url: Optional[str]        # Profile picture URL
cover_picture_url: Optional[str]        # Banner/cover image URL
```

**Tweet Metadata (Non-dynamic):**
```python
tweet_id: Optional[str]                 # Tweet ID
is_reply: Optional[bool]                # Is reply to another tweet
is_quote: Optional[bool]                # Is quote tweet
language: Optional[str]                 # Tweet language code
in_reply_to_username: Optional[str]     # Username being replied to
quoted_tweet_id: Optional[str]          # ID of quoted tweet
conversation_id: Optional[str]          # Conversation thread ID
in_reply_to_user_id: Optional[str]      # User ID being replied to
```

**Dynamic Engagement Metrics (Can change over time):**
```python
like_count: Optional[int]               # Number of likes
retweet_count: Optional[int]            # Number of retweets
reply_count: Optional[int]              # Number of replies
quote_count: Optional[int]              # Number of quote tweets
view_count: Optional[int]               # Number of views
bookmark_count: Optional[int]           # Number of bookmarks
```

**User Profile Metrics (Bi-directional):**
```python
user_followers_count: Optional[int]     # Follower count
user_following_count: Optional[int]     # Following count
```

#### ⚠️ **FORBIDDEN FIELDS**:
```python
model_config: Dict[str, str]            # Must be None or {"extra": "ignore"}
                                        # Miners will be penalized for setting this
```

---

### **DataEntity Model** (`common/data.py`)
Standardized storage format for all scraped content.

```python
uri: str                        # Unique resource identifier (tweet URL)
datetime: dt.datetime           # When content was created
source: DataSource              # DataSource.X (value: 2)
label: Optional[DataLabel]      # First hashtag or None
content: bytes                  # JSON-encoded XContent
content_size_bytes: int         # Size in bytes
```

#### Conversion Methods:
```python
# Convert XContent to DataEntity
data_entity = XContent.to_data_entity(content=x_content)

# Convert DataEntity back to XContent
x_content = XContent.from_data_entity(data_entity)
```

---

## ✅ Validators & Content Checking

### **Validation Flow** (`utils.py`)

#### 1️⃣ **URL Validation**
```python
is_valid_twitter_url(url: str) -> bool
```
- ✅ Must be valid URL format
- ✅ After Dec 27, 2024: Only `x.com` accepted
- ✅ Before deadline: Both `twitter.com` and `x.com` accepted

#### 2️⃣ **Spam Account Detection**
```python
is_spam_account(author_data: dict) -> bool
```
**Filters out:**
- Accounts with < 50 followers
- Accounts younger than 30 days
- Accounts missing creation date

#### 3️⃣ **Low Engagement Detection**
```python
is_low_engagement_tweet(tweet_data: dict) -> bool
```
**Filters out:**
- Tweets with < 50 views

#### 4️⃣ **Required Field Validation**
All required fields must:
- ✅ Not be `None`
- ✅ Match between submitted and actual tweet
- ✅ Pass field-specific checks:
  - **username**: Case-insensitive, @ sign normalized
  - **url**: Normalized (twitter.com → x.com)
  - **tweet_hashtags**: Subset check with 2.5x tolerance
  - **text**: Exact match (after sanitization)

#### 5️⃣ **Optional Field Validation**
Optional fields:
- ⚠️ Skipped if either value is `None`
- ✅ Must match exactly if both values exist
- ✅ Validated fields include:
  - User info (ID, display name, verified status)
  - Tweet metadata (ID, reply/quote status, language)
  - User profile (description, location, images)

#### 6️⃣ **Dynamic Engagement Validation**
```python
validate_engagement_metrics(submitted_tweet, actual_tweet, entity)
```

**Increasing-Only Metrics** (like_count, retweet_count, etc.):
- ✅ Must not decrease significantly (5% tolerance for spam removal)
- ✅ Can increase with age-based tolerance
- ✅ Tolerance calculation:
  - Fresh tweets (< 1hr): 100% increase allowed
  - Recent (< 6hr): 75% increase allowed
  - Day-old: 50% increase allowed
  - Week-old: 30% increase allowed
  - Older: 20% increase allowed

**Bi-directional Metrics** (followers/following):
- ✅ Percentage-based validation with logarithmic scaling
- ✅ Smaller accounts: Higher percentage tolerance
- ✅ Age-based multipliers:
  - < 24hr: 1.0x (standard)
  - < 1 week: 1.5x
  - < 1 month: 2.5x
  - > 1 month: 4.0x (max 500% tolerance)

**Field-specific multipliers:**
```python
{
    "like_count": 1.0,           # Baseline
    "retweet_count": 1.3,        # Higher volatility
    "reply_count": 1.5,          # High volatility
    "quote_count": 1.4,          # Moderate-high
    "view_count": 3.0,           # Highest volatility (10-100:1 ratio)
    "bookmark_count": 0.8,       # Lower volatility
    "user_followers_count": 2.0, # High volatility
    "user_following_count": 1.5, # Moderate volatility
}
```

#### 7️⃣ **Timestamp Validation**
```python
validate_timestamp(tweet_to_verify, actual_tweet, entity)
```
- ✅ Timestamps must be obfuscated to the minute
- ✅ Penalizes miners who submit non-obfuscated timestamps

#### 8️⃣ **Media Validation**
```python
validate_media_content(tweet_to_verify, actual_tweet, entity)
```
- ✅ Required if actual tweet has media
- ✅ URLs must match exactly (sorted for comparison)
- ❌ Rejects fake media claims

#### 9️⃣ **Retweet Filtering**
- ❌ Retweets are NOT eligible (as of July 6, 2024)
- ✅ All retweets are rejected during validation

#### 🔟 **DataEntity Field Validation**
Final checks:
- ✅ URI normalization and matching
- ✅ Datetime accuracy
- ✅ Source type (DataSource.X)
- ✅ Label correctness (first hashtag)
- ✅ Content size bytes accuracy

---

## 🛠️ Utility Functions (`utils.py`)

### **Text Processing:**
```python
sanitize_scraped_tweet(text: str) -> str
- Removes leading @mentions
- Strips t.co image links

extract_hashtags(text: str) -> List[str]
- Extracts hashtags in order
- Includes cashtags ($TAG) as hashtags
- Removes duplicates while preserving order

extract_user(url: str) -> str
- Extracts @username from tweet URL
```

### **URL Handling:**
```python
normalize_url(url: str) -> str
- Converts twitter.com → x.com for comparison
- After deadline: No normalization needed

remove_at_sign_from_username(username: str) -> str
- Removes @ prefix from usernames
```

### **Validation Helpers:**
```python
are_hashtags_valid(submitted, actual) -> bool
- Checks subset relationship
- Allows up to 2.5x extra hashtags

_validate_model_config(model_config) -> bool
- Ensures no extra data in model_config
- Only allows None or {"extra": "ignore"}
```

---

## 📋 Configuration Files Needed

### **Required:**
None - The X scraping system is self-contained

### **Optional Dependencies:**
```python
# From common module
from common.data import DataEntity, DataLabel, DataSource
from common.constants import (
    X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE,
    NO_TWITTER_URLS_DATE,
    MAX_LABEL_LENGTH
)
from common.date_range import DateRange
from common.protocol import KeywordMode

# From scraping module
from scraping.scraper import ScrapeConfig, Scraper, ValidationResult
from scraping.apify import ActorRunner, RunConfig
from scraping import utils

# From validation utilities
from vali_utils.on_demand import utils as on_demand_utils
```

---

## 🔍 Search & Query Capabilities

### **Search Query Format** (ApiDojo):
```python
query = "since:2024-01-01_00:00:00_UTC until:2024-01-01_23:59:59_UTC (from:user1 OR from:user2) (#hashtag1 OR #hashtag2)"
```

### **Search Parameters:**
- ✅ Date ranges (since/until)
- ✅ Usernames (from:username) with OR logic
- ✅ Hashtags with OR logic
- ✅ Keywords (exact or partial)
- ✅ Combination queries

### **On-Demand Scraping Options:**
```python
usernames: List[str]          # Target users (OR logic)
keywords: List[str]           # Search keywords
keyword_mode: "any" | "all"   # OR vs AND logic
url: str                      # Direct tweet URL
start_datetime: dt.datetime   # Start of time range
end_datetime: dt.datetime     # End of time range
limit: int                    # Max results (default: 100)
```

---

## 🚨 Important Validation Rules

### **Content Rules:**
1. ✅ **No Retweets** - Rejected since July 6, 2024
2. ✅ **URL Format** - Only x.com after December 27, 2024
3. ✅ **Timestamp Obfuscation** - Must be rounded to minute
4. ✅ **Hashtag Order** - First hashtag becomes DataLabel
5. ✅ **Media Required** - Must include if present in original
6. ✅ **No Spam Accounts** - Min 50 followers, 30+ days old
7. ✅ **Minimum Engagement** - Min 50 views required
8. ✅ **Model Config** - Must be None or {"extra": "ignore"}

### **Engagement Tolerance:**
- Recent tweets: Higher tolerance (more volatile)
- Old tweets: Lower tolerance (stabilized)
- Views: 3x multiplier (most volatile)
- Bookmarks: 0.8x multiplier (most stable)

### **Validation Flow:**
```
1. Check URL validity
2. Scrape actual tweet (via Apify)
3. Check spam/engagement filters (if enabled)
4. Validate required fields
5. Validate optional fields (if present)
6. Validate engagement metrics (if present)
7. Check timestamp obfuscation
8. Check media content
9. Verify DataEntity consistency
10. Return ValidationResult
```

---

## 📊 Data Flow Summary

```
┌─────────────────────────────────────────────┐
│  1. SCRAPING                                │
│     ├── ApiDojoTwitterScraper (primary)     │
│     ├── MicroworldsTwitterScraper (alt)     │
│     └── QuackerUrlScraper (backup)          │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  2. PARSING                                 │
│     ├── Extract user info                   │
│     ├── Extract hashtags/media              │
│     ├── Extract engagement metrics          │
│     └── Sanitize text content               │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  3. VALIDATION (utils.py)                   │
│     ├── Required field checks               │
│     ├── Optional field checks               │
│     ├── Engagement metric validation        │
│     ├── Spam/low-engagement filtering       │
│     └── Timestamp obfuscation check         │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  4. STORAGE (XContent → DataEntity)         │
│     ├── Convert to DataEntity               │
│     ├── Encode content as bytes             │
│     ├── Calculate content_size_bytes        │
│     └── Set label from first hashtag        │
└─────────────────────────────────────────────┘
```

---

## 🎯 Key Takeaways

### **For Miners:**
1. Use **ApiDojoTwitterScraper** for best results
2. Always include **required fields**
3. Include **enhanced fields** for better validation
4. Obfuscate **timestamps to the minute**
5. Avoid **spam accounts** and **low-engagement tweets**
6. Do NOT set **model_config** field
7. Include **media URLs** if present
8. Use **x.com** URLs only (after Dec 27, 2024)

### **For Validators:**
1. **ApiDojoTwitterScraper.validate()** is primary method
2. Engagement filtering **can be disabled** for on-demand API
3. Concurrent validation **limited to 20** simultaneous requests
4. Validation uses **time-based tolerance** for engagement
5. Spam detection requires **50+ followers, 30+ day account**
6. Low engagement threshold: **50+ views**

### **For Developers:**
1. **Three scraper options** available
2. **XContent model** has extensive optional fields
3. **Validation is comprehensive** with multiple stages
4. **Engagement metrics** use sophisticated tolerance calculations
5. **Backward compatibility** maintained for optional fields
6. **DataEntity** is the storage abstraction layer

---

## 📞 Testing & Examples

### **Test Scraping:**
```bash
cd /root/Algo-test-script/data-universe/scraping/x
python apidojo_scraper.py  # Runs test_multi_thread_validate, test_scrape, test_validate
```

### **Test Functions Available:**
- `test_scrape()` - Tests basic scraping
- `test_validate()` - Tests validation with sample entities
- `test_shadowban_detection()` - Tests shadowban detection
- `test_multi_thread_validate()` - Tests concurrent validation

---

## 🔗 Related Files

- `/root/Algo-test-script/data-universe/common/data.py` - DataEntity model
- `/root/Algo-test-script/data-universe/common/constants.py` - Configuration constants
- `/root/Algo-test-script/data-universe/scraping/scraper.py` - Base scraper interface
- `/root/Algo-test-script/data-universe/scraping/apify.py` - Apify actor runner
- `/root/Algo-test-script/data-universe/storage/` - Storage implementations

---

**Documentation Generated**: 2025-11-15  
**System Version**: Enhanced Format with Engagement Metrics  
**Compatibility Expiration**: Check `X_ENHANCED_FORMAT_COMPATIBILITY_EXPIRATION_DATE`
