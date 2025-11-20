# Reddit Scraper API — Access & Usage Guide (v1.0.0)

This document explains how a single external tester can securely access and test the Reddit Scraper API, along with full usage instructions, example requests, streaming, and troubleshooting.

Service entrypoint:
- App module: OnDEMANDREDDIT/reddit_scraper_api.py (FastAPI + Uvicorn)
- Default port: 8000
- Accounts source: OnDEMANDREDDIT/envActive
- Logs: scraper_api.log (in the working directory where the server is started)

--------------------------------------------------------------------------------

1) Quick Start (Server Admin)

Prerequisites:
- Python 3.10+ recommended
- Network access for tester (via SSH tunnel or IP allowlist)
- A populated envActive file with Reddit accounts and proxy configuration (already present)

Steps:
1. Create and activate a virtual environment (optional but recommended)
   - python3 -m venv venv
   - source venv/bin/activate

2. Install dependencies
   - pip install -r OnDEMANDREDDIT/requirements_api.txt

3. Start the API server
   Option A (recommended for single-user testing — bind to localhost):
   - uvicorn OnDEMANDREDDIT.reddit_scraper_api:app --host 127.0.0.1 --port 8000 --workers 1

   Option B (binds to all interfaces — only do this with IP allowlisting or reverse proxy):
   - python OnDEMANDREDDIT/reddit_scraper_api.py
     (This runs Uvicorn with host=0.0.0.0, port=8000)

4. Verify health locally
   - curl -s http://127.0.0.1:8000/health | jq

5. Share access with the tester using one of the secure methods below.

--------------------------------------------------------------------------------

2) Secure Access for One Tester

Use one of these approaches to grant access to a single person:

A. SSH Port Forwarding (Preferred — no firewall changes)
- Keep the API bound to 127.0.0.1 on the server.
- Tester runs this from their machine:
  - ssh -N -L 8000:127.0.0.1:8000 user@your.server.ip
- Tester can then access locally at:
  - http://localhost:8000

B. IP Allowlisting (UFW) for port 8000
- Bind API to 0.0.0.0 or use Option B run mode.
- On the server (replace TESTER_IP):
  - sudo ufw allow from TESTER_IP to any port 8000 proto tcp
  - sudo ufw deny 8000/tcp   # Deny from all others if needed
- Tester can access directly at:
  - http://your.server.ip:8000

C. Reverse Proxy with Basic Auth (Nginx)
- Put Nginx in front of the FastAPI service and enable HTTP Basic Auth.
- Example Nginx snippet:
  - location / {
      proxy_pass http://127.0.0.1:8000;
      proxy_set_header Host $host;
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
- Create a password file with htpasswd and restrict to the single tester.

Recommendation: For a one-person private test, SSH port forwarding is simplest and safest.

--------------------------------------------------------------------------------

3) Endpoints Overview

Base URL: http://localhost:8000 (or your server IP/host)

- GET /health
  - Returns service health + account pool status summary.

- GET /accounts
  - Returns details for loaded accounts, request/error counts, rate-limit info.

- POST /accounts/reload
  - Reloads accounts from OnDEMANDREDDIT/envActive without restarting the server.

- POST /scrape
  - Scrapes one or more subreddits and returns a structured response including stats and data entities.

- POST /scrape/stream
  - Streams entities as they are scraped (server-sent events, text/event-stream).

--------------------------------------------------------------------------------

4) Request/Response Schemas

POST /scrape — JSON request body:
{
  "subreddits": ["python", "machinelearning"],  // required, list of subreddit names (no "r/" prefix required)
  "max_posts": 100,                              // int, 1..1000 (per subreddit)
  "include_comments": true,                      // bool
  "max_comments_per_post": 500,                  // int, 0..1000
  "days_back": 7                                 // int, 1..30
}

Response (200):
{
  "status": "success",
  "data": [ DataEntity JSON dicts ... ],
  "stats": {
    "posts_scraped": int,
    "comments_scraped": int,
    "subreddits_processed": int,
    "duration_seconds": float,
    "account_used": "username",
    "errors_count": int
  },
  "message": "Successfully scraped ..."
}

DataEntity JSON dict (example):
{
  "uri": "https://www.reddit.com/r/Python/comments/....",
  "datetime": "2025-11-20T15:36:37+00:00",
  "source": 1,                // 1=Reddit (DataSource.REDDIT)
  "label": "r/python",        // subreddit label (lowercased)
  "content": "{\"id\": \"t3_...\", ... }",  // serialized RedditContent JSON (string)
  "content_size_bytes": 1234
}

POST /scrape/stream — JSON request body:
- Same as /scrape.
- SSE stream lines look like: data: { ...entity json... }

--------------------------------------------------------------------------------

5) Tester Usage Examples

Use curl (or any HTTP client). If using SSH tunnel, access via http://localhost:8000.

Health:
- curl -s http://localhost:8000/health | jq

Accounts:
- curl -s http://localhost:8000/accounts | jq

Scrape (single subreddit):
- curl -s -X POST http://localhost:8000/scrape \
  -H "Content-Type: application/json" \
  -d '{
        "subreddits": ["python"],
        "max_posts": 10,
        "include_comments": true,
        "max_comments_per_post": 50,
        "days_back": 7
      }' | jq

Scrape (multiple):
- curl -s -X POST http://localhost:8000/scrape \
  -H "Content-Type: application/json" \
  -d '{
        "subreddits": ["python", "machinelearning", "datascience"],
        "max_posts": 5,
        "include_comments": true,
        "max_comments_per_post": 20,
        "days_back": 3
      }' | jq

Streaming (server-sent events):
- curl -N -X POST http://localhost:8000/scrape/stream \
  -H "Content-Type: application/json" \
  -d '{
        "subreddits": ["python"],
        "max_posts": 5,
        "include_comments": false,
        "days_back": 1
      }'
Notes:
- Use -N (no-buffer) to see streamed events in real-time.
- Each event line starts with: data: {json}\n\n

Python (requests) example:
- See OnDEMANDREDDIT/test_api.py for a complete test suite.

--------------------------------------------------------------------------------

6) Behavior Notes

- NSFW Handling: Posts marked NSFW with media are skipped to avoid unsafe content propagation.
- Rate Limiting:
  - The scraper tracks Reddit rate limit headers and updates the current account’s rate-limit state.
  - If 429 is detected, the scraper backs off automatically.
- Errors/Exceptions:
  - 503 if the account pool is not initialized or no healthy accounts are available.
  - 500 with message if scraping fails (e.g., proxy issues, Reddit errors).
- Accounts:
  - Loaded from OnDEMANDREDDIT/envActive at startup.
  - Reload on the fly via POST /accounts/reload.

--------------------------------------------------------------------------------

7) Operational Tips

- Running as a service:
  - Consider a systemd unit to supervise the API for production-like stability.
  - Example ExecStart (localhost bind):
    - /path/to/venv/bin/uvicorn OnDEMANDREDDIT.reddit_scraper_api:app --host 127.0.0.1 --port 8000 --workers 1

- Logs:
  - Check scraper_api.log for runtime status, errors, and account pool events.

- Performance:
  - max_posts and include_comments significantly impact runtime and payload size.
  - The streaming endpoint (/scrape/stream) can be used to process data incrementally.

- Security for One Tester:
  - Prefer SSH port forwarding: keeps the API bound to localhost and not exposed to the internet.
  - If exposing the port, restrict by IP (UFW) or add Basic Auth at a reverse proxy.

--------------------------------------------------------------------------------

8) Troubleshooting

- HTTP 503: "Account pool not initialized"
  - Ensure the server is fully started and envActive is present and valid.
  - Call POST /accounts/reload to reload accounts without restart.

- HTTP 500: Generic scraping failure
  - Check scraper_api.log for stack traces.
  - Common causes: proxy misconfiguration, network blocks, banned/forbidden accounts.

- HTTP 403 / Forbidden or 429 / Too Many Requests
  - The account may be forbidden or rate-limited; the pool adjusts states accordingly.
  - Try again later or switch to a different account.

- Streaming shows no events
  - Ensure -N is used in curl.
  - Verify payload has reasonable limits (e.g., small max_posts).
  - Confirm the subreddits have recent posts within days_back.

- Old error: "'int' object has no attribute 'value'"
  - Fixed by serializing DataEntity via to_json_dict to avoid enum/value mismatches.
  - Ensure the deployed code includes the latest OnDEMANDREDDIT/realtime_scraper.py.

--------------------------------------------------------------------------------

9) Change Accounts During Runtime

- POST /accounts/reload
  - Reloads accounts from envActive. Response includes old and new counts.
- GET /accounts
  - Verify which accounts are healthy and track request/error counts.

--------------------------------------------------------------------------------

10) Version

- API Version: 1.0.0
- Module: OnDEMANDREDDIT/reddit_scraper_api.py
- Last verified test: see OnDEMANDREDDIT/test_api.py output

If you need to onboard a second tester or enable authentication, consider adding a simple API key header check in FastAPI or put Nginx/Caddy in front with Basic Auth and TLS.
