# Manual Setup: Twitter Cookies and Headers for twscrape (per-account)

This guide shows how to manually inject the required cookies and headers into `twitter_accounts.db` so an account becomes active (like your working account `aniruddh62469`), without triggering the login flow.

Works for: an existing row in `accounts` table.

Key points:
- Cookies required: `auth_token` and `ct0`
- Headers required (minimal):  
  - `authorization` (fixed Bearer token used by twscrape)  
  - `content-type` = `application/json`  
  - `x-twitter-active-user` = `yes`  
  - `x-twitter-client-language` = `en`  
  - `x-csrf-token` = same value as `ct0` cookie  
- `user-agent` is taken from the `user_agent` column (keep consistent with how you obtained the cookies)

---

## 0) Prerequisites

- Database file: `twitter_accounts.db`
- The `accounts` row for the username already exists (created earlier)
- You have values for:
  - USERNAME (e.g., `aniruddh4354`)
  - AUTH_TOKEN (e.g., `bc00b3df...`)
  - CT0 (long CSRF token you provided)

---

## 1) Backup the DB

Always make a backup before manual edits.

```bash
cp twitter_accounts.db "twitter_accounts.db.bak_$(date +%s)"
```

---

## 2) Verify the account row exists

```bash
sqlite3 -header -csv twitter_accounts.db \
"SELECT username, active, error_msg, user_agent FROM accounts WHERE username='USERNAME';"
```

You should see one row for that `USERNAME`.

---

## 3) Update cookies, headers, activate, and clear error

Replace placeholders:
- `USERNAME` &rarr; the account username
- `AUTH_TOKEN` &rarr; the auth_token cookie
- `CT0` &rarr; the ct0 cookie

```bash
sqlite3 twitter_accounts.db "
UPDATE accounts
SET
  active = 1,
  error_msg = NULL,
  cookies = json_object(
    'auth_token', 'AUTH_TOKEN',
    'ct0', 'CT0'
  ),
  headers = json_object(
    'authorization', 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    'content-type', 'application/json',
    'x-twitter-active-user', 'yes',
    'x-twitter-client-language', 'en',
    'x-csrf-token', 'CT0'
  )
WHERE username = 'USERNAME';
"
```

Notes:
- The `authorization` bearer token is fixed by twscrape (`account.py`); do not change it.
- `x-csrf-token` must equal exactly the value of `ct0` cookie.
- `user-agent` is read from the `user_agent` column when building the client.

---

## 4) (Optional) Clear locks and stats for the account

If the account was previously locked due to rate/queue windows:

```bash
sqlite3 twitter_accounts.db "
UPDATE accounts
SET locks = json_object(),
    stats = json_object()
WHERE username = 'USERNAME';
"
```

---

## 5) Verify the update

```bash
sqlite3 -header -csv twitter_accounts.db "
SELECT
  username,
  active,
  error_msg,
  json_extract(cookies, '$.auth_token') AS auth_token,
  json_extract(cookies, '$.ct0')        AS ct0,
  json_extract(headers, '$.x-csrf-token') AS x_csrf,
  user_agent
FROM accounts
WHERE username='USERNAME';
"
```

You should see:
- `active` = 1
- `error_msg` empty/null
- `x_csrf` equals `ct0`
- Your `user_agent` preserved

---

## 6) (Optional) Update user-agent for this account

If you want to set a specific UA string (ideally matching the browser session you took cookies from):

```bash
sqlite3 twitter_accounts.db "
UPDATE accounts
SET user_agent = 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1'
WHERE username = 'USERNAME';
"
```

---

## Example (using your provided tokens for aniruddh4354)

Replace with your actual values if changed later.

```bash
sqlite3 twitter_accounts.db "
UPDATE accounts
SET
  active = 1,
  error_msg = NULL,
  cookies = json_object(
    'auth_token', 'bc00b3df9ad1e81e2b5e6872434f0302007ff57f',
    'ct0', 'a388f6646c3bf09cb6723eb370717c8605f579e52492d8e425d6f81e267178450c778dabe3375b014d8663ac4cf6d82651bb32ff816b5e095fe52cb568e02e251333702ecdeb0d9a44bd99b88c77f335'
  ),
  headers = json_object(
    'authorization', 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
    'content-type', 'application/json',
    'x-twitter-active-user', 'yes',
    'x-twitter-client-language', 'en',
    'x-csrf-token', 'a388f6646c3bf09cb6723eb370717c8605f579e52492d8e425d6f81e267178450c778dabe3375b014d8663ac4cf6d82651bb32ff816b5e095fe52cb568e02e251333702ecdeb0d9a44bd99b88c77f335'
  )
WHERE username = 'aniruddh4354';
"
```

Then verify:

```bash
sqlite3 -header -csv twitter_accounts.db "
SELECT username, active, error_msg,
       json_extract(cookies,'$.auth_token') AS auth_token,
       json_extract(cookies,'$.ct0') AS ct0,
       json_extract(headers,'$.x-csrf-token') AS x_csrf
FROM accounts
WHERE username='aniruddh4354';
"
```

---

## Adding a brand-new account with cookies (if needed)

If the account row does not exist yet, prefer adding via CLI including cookies:

1) Create a file `accounts_with_cookies.txt`:

```
USERNAME:PASSWORD:EMAIL:EMAIL_PASSWORD:auth_token=YOUR_AUTH;ct0=YOUR_CT0
```

2) Add it:

```bash
python -m twscrape.cli add_accounts accounts_with_cookies.txt "username:password:email:email_password:cookies"
```

The pool code will parse cookies and set `active = true` automatically when `ct0` exists.

---

## Troubleshooting error (32) Could not authenticate you

- Tokens invalid/expired: obtain fresh `auth_token` and `ct0` from a valid browser session for that account (same IP/region/UA if possible).
- Ensure no extra spaces in tokens.  
- Ensure `x-csrf-token` equals the `ct0` value exactly.  
- `authorization` must remain the fixed Bearer from twscrape code.  
- If the account still gets disabled by the queue, re-check tokens or relogin with saved credentials when possible.

---

## What twscrape will send after this setup

From `twscrape/twscrape/account.py` (`make_client`):
- It merges saved `cookies` and `headers` and then ensures:
  - `authorization` is the fixed Bearer token
  - `user-agent` comes from the `user_agent` column
  - `content-type` = `application/json`
  - `x-twitter-active-user` = `yes`
  - `x-twitter-client-language` = `en`
  - If `ct0` exists in cookies, `x-csrf-token` is set to that value

This matches the behavior of your working account.
