#!/usr/bin/env python3
"""
reddit_gravity_orchestrator.py

24/7 PM2-friendly orchestrator that:
- Reads gravity/reddit.json (produced by monitor-gravity.js)
- Converts items into scrape jobs for Reddit:
    * label: "r/subreddit" (subreddit-targeted)
    * keyword: "..."       (search)
    * post_start_datetime / post_end_datetime (ISO 8601, optional)
- Rotates through ACTIVE Reddit accounts from envActive (via AccountPoolManager)
- Executes scraping with OnDEMANDREDDIT.realtime_scraper.RealtimeScraper
- Supports on-demand jobs via a local JSON queue file
- Writes results as JSON to OnDEMANDREDDIT/logs/

Environment variables:
  GRAVITY_REDDIT_JSON  Path to gravity/reddit.json (default: gravity/reddit.json)
  ENVACTIVE_PATH       Path to envActive file (default: envActive)
  LOOP_INTERVAL_SEC    Orchestrator loop interval (default: 120)
  MAX_CONCURRENCY      Max concurrent jobs per loop (default: 3)
  MAX_POSTS_PER_JOB    Per-job max posts for subreddit-new or search (default: 50)
  INCLUDE_COMMENTS     "1" or "0" to include comments per post (default: "1")
  DAYS_BACK_DEFAULT    Default days back for time window if missing (default: 7)
  ONDEMAND_FILE        Path to local on-demand queue (default: OnDEMANDREDDIT/ondemand_requests.json)
  OUTPUT_DIR           Base output dir (default: OnDEMANDREDDIT/logs/reddit_scrapes)
  ONDEMAND_OUTPUT_DIR  Output dir for ondemand results (default: OnDEMANDREDDIT/logs/ondemand)
"""

import os
import sys
import json
import time
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

# Resolve repository root and add to sys.path so imports work under PM2
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_UNIVERSE_DIR = os.path.join(REPO_ROOT, "data-universe")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if DATA_UNIVERSE_DIR not in sys.path:
    sys.path.insert(0, DATA_UNIVERSE_DIR)

from common.data import DataEntity  # noqa: E402
from account_pool_manager import AccountPoolManager, RedditAccount  # noqa: E402
from realtime_scraper import RealtimeScraper  # noqa: E402
from storage_postgresql import PostgreSQLMinerStorage  # noqa: E402

logger = logging.getLogger("reddit_gravity_orchestrator")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Config via env
GRAVITY_REDDIT_JSON = os.getenv("GRAVITY_REDDIT_JSON", "gravity/reddit.json")
ENVACTIVE_PATH = os.getenv("ENVACTIVE_PATH", "envActive")
LOOP_INTERVAL_SEC = int(os.getenv("LOOP_INTERVAL_SEC", "120"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "3"))
MAX_POSTS_PER_JOB = int(os.getenv("MAX_POSTS_PER_JOB", "50"))
INCLUDE_COMMENTS = os.getenv("INCLUDE_COMMENTS", "1") == "1"
DAYS_BACK_DEFAULT = int(os.getenv("DAYS_BACK_DEFAULT", "7"))
ONDEMAND_FILE = os.getenv("ONDEMAND_FILE", "OnDEMANDREDDIT/ondemand_requests.json")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "OnDEMANDREDDIT/logs/reddit_scrapes")
ONDEMAND_OUTPUT_DIR = os.getenv("ONDEMAND_OUTPUT_DIR", "OnDEMANDREDDIT/logs/ondemand")
# Control artifact writing (disable file output to save disk)
WRITE_JOB_FILES = os.getenv("WRITE_JOB_FILES", "0") == "1"
WRITE_ONDEMAND_FILES = os.getenv("WRITE_ONDEMAND_FILES", "0") == "1"

# PostgreSQL storage configuration
PG_DATABASE = os.getenv("PG_DATABASE", "reddit_miner_db")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")


def _ensure_dirs() -> None:
    if WRITE_JOB_FILES:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    if WRITE_ONDEMAND_FILES:
        os.makedirs(ONDEMAND_OUTPUT_DIR, exist_ok=True)


def parse_iso8601(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        # Support 'Z' suffix
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def job_within_window(job: Dict[str, Any], ref: datetime) -> bool:
    params = job.get("params", {}) or {}
    start = parse_iso8601(params.get("post_start_datetime"))
    end = parse_iso8601(params.get("post_end_datetime"))

    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    if start and ref < start:
        return False
    if end and ref > end:
        return False
    return True


def load_gravity_reddit_jobs(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        jobs: List[Dict[str, Any]] = []
        for item in data:
            params = item.get("params", {}) or {}
            if params.get("platform", "").lower() != "reddit":
                continue
            jobs.append(item)
        return jobs
    except FileNotFoundError:
        logger.warning(f"Gravity reddit file not found: {path}")
        return []
    except Exception as e:
        logger.error(f"Failed to read {path}: {e}")
        return []


def extract_subreddit(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    s = label.strip()
    if s.lower().startswith("r/"):
        return s[2:]
    # Accept full prefixed form "r/sub"
    return s


def entity_in_window(entity: DataEntity, start: Optional[datetime], end: Optional[datetime]) -> bool:
    dt = entity.datetime
    if start and dt < start:
        return False
    if end and dt > end:
        return False
    return True


async def run_job(
    job: Dict[str, Any],
    account: RedditAccount,
    max_posts: int,
    include_comments: bool,
    storage: PostgreSQLMinerStorage,
) -> Dict[str, Any]:
    """
    Run a single gravity reddit job using one account.
    Returns dict with:
      { "job_id": ..., "count": int, "path": output_file, "errors": int }
    """
    job_id = job.get("id", "unknown")
    params = job.get("params", {}) or {}
    label = params.get("label")
    keyword = params.get("keyword")
    sub = extract_subreddit(label)

    start_dt = parse_iso8601(params.get("post_start_datetime"))
    end_dt = parse_iso8601(params.get("post_end_datetime"))

    # Fallback to DAYS_BACK_DEFAULT if window unspecified
    days_back = DAYS_BACK_DEFAULT
    if start_dt and end_dt:
        try:
            # If both given, compute days_back ceiling
            delta = end_dt - start_dt
            days_back = max(1, min(30, int(delta.total_seconds() // 86400) + 1))
        except Exception:
            pass

    scraper = RealtimeScraper(
        account=account,
        max_posts=max_posts,
        include_comments=include_comments,
        max_comments_per_post=500,
        days_back=days_back,
    )

    entities: List[DataEntity] = []
    errors = 0
    start_t = time.monotonic()

    try:
        await scraper.initialize()

        if sub and keyword:
            # Search within a subreddit by keyword
            results = await scraper.search_keyword_in_subreddit(sub, keyword)
            entities.extend(results)
        elif sub:
            # Scrape recent posts/comments from subreddit
            results = await scraper.scrape_subreddit(sub)
            entities.extend(results)
        elif keyword:
            # Global-ish search: attempt r/all for the keyword
            results = await scraper.search_keyword_in_subreddit("all", keyword)
            entities.extend(results)
        else:
            # Nothing actionable
            logger.info(f"[{job_id}] No label or keyword, skipping.")
    except Exception as e:
        logger.error(f"[{job_id}] job failed: {e}")
        errors += 1
    finally:
        try:
            await scraper.cleanup()
        except Exception:
            pass

    # Filter entities by time window if provided
    if start_dt and start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt and end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    if start_dt or end_dt:
        filtered = [e for e in entities if entity_in_window(e, start_dt, end_dt)]
    else:
        filtered = entities

    # Store to PostgreSQL (preserve protocol: DataEntity as-is)
    try:
        if filtered:
            storage.store_data_entities(filtered)
    except Exception as e:
        logger.error(f"[{job_id}] DB store failed: {e}")

    # Serialize to JSON
    out = [e.to_json_dict() for e in filtered]
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = None
    if WRITE_JOB_FILES:
        out_path = os.path.join(OUTPUT_DIR, f"{job_id}_{ts}.json")
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({"job_id": job_id, "count": len(out), "entities": out}, f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"[{job_id}] failed to write output: {e}")
    out_path_str = out_path if out_path else "(disabled)"

    duration = time.monotonic() - start_t
    logger.info(
        f"[JOB] id={job_id} account={account.username} "
        f"label={sub or 'all'} keyword={keyword or '-'} "
        f"window=({start_dt.isoformat() if start_dt else '-'} .. {end_dt.isoformat() if end_dt else '-'}) "
        f"posts={scraper.stats.get('posts_scraped', 0)} comments={scraper.stats.get('comments_scraped', 0)} "
        f"stored={len(filtered)} errors={errors + scraper.stats.get('errors', 0)} "
        f"took={duration:.2f}s file={out_path_str}"
    )
    return {
        "job_id": job_id,
        "count": len(out),
        "path": out_path,
        "errors": errors + scraper.stats.get("errors", 0),
        "stored": len(filtered),
        "account": account.username,
        "stats": dict(scraper.stats),
        "duration_sec": duration,
    }


async def process_ondemand_file(pool: AccountPoolManager, storage: PostgreSQLMinerStorage) -> Optional[str]:
    """
    If an on-demand file exists, process and rename it to .processed-TS.
    Expected schema:
      {
        "requests": [
          {
            "subreddit": "r/cryptocurrency" | null,
            "keywords": ["bitcoin","tao"] | null,
            "usernames": ["spez"] | null,
            "limit": 100,
            "days_back": 7
          },
          ...
        ]
      }
    Writes output JSON into ONDEMAND_OUTPUT_DIR and returns output path.
    """
    if not os.path.exists(ONDEMAND_FILE):
        return None
    try:
        with open(ONDEMAND_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        logger.error(f"[ONDEMAND] failed reading {ONDEMAND_FILE}: {e}")
        return None

    requests = payload.get("requests", []) or []
    if not requests:
        # Rename to mark processed empty
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        new_name = f"{ONDEMAND_FILE}.processed-{ts}"
        try:
            os.replace(ONDEMAND_FILE, new_name)
        except Exception:
            pass
        return None

    account = pool.get_next_account()
    if not account:
        logger.error("[ONDEMAND] no healthy accounts available.")
        return None

    scraper = RealtimeScraper(
        account=account,
        max_posts=MAX_POSTS_PER_JOB,
        include_comments=INCLUDE_COMMENTS,
        max_comments_per_post=500,
        days_back=DAYS_BACK_DEFAULT,
    )

    all_entities: List[DataEntity] = []
    try:
        await scraper.initialize()
        for idx, req in enumerate(requests):
            sub = extract_subreddit(req.get("subreddit"))
            kws = req.get("keywords")
            limit = int(req.get("limit") or MAX_POSTS_PER_JOB)
            days_back = int(req.get("days_back") or DAYS_BACK_DEFAULT)
            logger.info(f"[ONDEMAND] req#{idx+1}: subreddit={sub or 'all'} keywords={kws} limit={limit} days_back={days_back} account={account.username}")

            scraper.max_posts = limit
            scraper.days_back = days_back

            if sub and kws:
                for kw in kws:
                    all_entities.extend(await scraper.search_keyword_in_subreddit(sub, kw))
            elif sub:
                all_entities.extend(await scraper.scrape_subreddit(sub))
            elif kws:
                # Search in r/all for each keyword
                for kw in kws:
                    all_entities.extend(await scraper.search_keyword_in_subreddit("all", kw))
            else:
                logger.info("[ONDEMAND] request missing subreddit and keywords; skipped.")
    except Exception as e:
        logger.error(f"[ONDEMAND] failed: {e}")
    finally:
        try:
            await scraper.cleanup()
        except Exception:
            pass

    # Store on-demand results to PostgreSQL as well
    try:
        if all_entities:
            storage.store_data_entities(all_entities)
    except Exception as e:
        logger.error(f"[ONDEMAND] DB store failed: {e}")

    out = [e.to_json_dict() for e in all_entities]
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = None
    if WRITE_ONDEMAND_FILES:
        out_path = os.path.join(ONDEMAND_OUTPUT_DIR, f"ondemand_{ts}.json")
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({"count": len(out), "entities": out}, f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"[ONDEMAND] failed to write output: {e}")

    # Mark processed
    try:
        new_name = f"{ONDEMAND_FILE}.processed-{ts}"
        os.replace(ONDEMAND_FILE, new_name)
    except Exception:
        pass

    out_path_str = out_path if out_path else "(disabled)"
    logger.info(f"[ONDEMAND] wrote {len(out)} entities -> {out_path_str}")
    logger.info(f"[ONDEMAND] summary: requests={len(requests)} total_entities={len(all_entities)} stored_to_db={len(all_entities)}")
    return out_path


async def process_ondemand_requests(pool: AccountPoolManager, storage: PostgreSQLMinerStorage, requests: List[Dict[str, Any]], persist: bool = False) -> List[DataEntity]:
    """
    Direct on-demand processing for Reddit (without using the ondemand_requests.json file).
    Each request item schema:
      {
        "subreddit": "r/cryptocurrency" | null,
        "keywords": ["bitcoin","tao"] | null,
        "usernames": ["spez"] | null,
        "limit": 100,
        "days_back": 7
      }
    Returns the list of DataEntity objects produced. Also stores to PostgreSQL.
    """
    all_entities: List[DataEntity] = []
    if not requests:
        return all_entities

    # Get an account
    account = pool.get_next_account()
    if not account:
        logger.error("[ONDEMAND.DIRECT] no healthy accounts available.")
        return all_entities

    scraper = RealtimeScraper(
        account=account,
        max_posts=MAX_POSTS_PER_JOB,
        include_comments=INCLUDE_COMMENTS,
        max_comments_per_post=500,
        days_back=DAYS_BACK_DEFAULT,
    )

    try:
        await scraper.initialize()
        for idx, req in enumerate(requests):
            sub = extract_subreddit(req.get("subreddit"))
            kws = req.get("keywords")
            limit = int(req.get("limit") or MAX_POSTS_PER_JOB)
            days_back = int(req.get("days_back") or DAYS_BACK_DEFAULT)

            logger.info(
                f"[ONDEMAND.DIRECT] req#{idx+1}: subreddit={sub or 'all'} keywords={kws} "
                f"limit={limit} days_back={days_back} account={account.username}"
            )

            scraper.max_posts = limit
            scraper.days_back = days_back

            if sub and kws:
                for kw in kws:
                    all_entities.extend(await scraper.search_keyword_in_subreddit(sub, kw))
            elif sub:
                all_entities.extend(await scraper.scrape_subreddit(sub))
            elif kws:
                for kw in kws:
                    all_entities.extend(await scraper.search_keyword_in_subreddit("all", kw))
            else:
                logger.info("[ONDEMAND.DIRECT] request missing subreddit and keywords; skipped.")
    except Exception as e:
        logger.error(f"[ONDEMAND.DIRECT] failed: {e}")
    finally:
        try:
            await scraper.cleanup()
        except Exception:
            pass

    # Store to PostgreSQL (optional live-only control)
    if persist:
        try:
            if all_entities:
                storage.store_data_entities(all_entities)
        except Exception as e:
            logger.error(f"[ONDEMAND.DIRECT] DB store failed: {e}")

    return all_entities


async def orchestrate_once(pool: AccountPoolManager, storage: PostgreSQLMinerStorage) -> None:
    """
    Single iteration:
      - Load gravity reddit jobs
      - Filter by active window
      - Execute up to MAX_CONCURRENCY jobs using round-robin accounts
      - Process on-demand requests (if any)
    """
    # Process on-demand first (higher priority)
    try:
        await process_ondemand_file(pool, storage)
    except Exception as e:
        logger.error(f"[LOOP] ondemand processing failed: {e}")

    jobs = load_gravity_reddit_jobs(GRAVITY_REDDIT_JSON)
    if not jobs:
        logger.info("No reddit jobs found in gravity; sleeping...")
        return

    ref = now_utc()
    active_jobs = [j for j in jobs if job_within_window(j, ref)]
    if not active_jobs:
        logger.info("No reddit jobs active at this time; sleeping...")
        return

    # Prepare concurrency controls
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results: List[Dict[str, Any]] = []

    async def _worker(job: Dict[str, Any]):
        async with sem:
            acc = pool.get_next_account()
            if not acc:
                logger.error("No healthy accounts available for job; skipped.")
                return
            res = await run_job(
                job=job,
                account=acc,
                max_posts=MAX_POSTS_PER_JOB,
                include_comments=INCLUDE_COMMENTS,
                storage=storage,
            )
            results.append(res)

    # Limit total jobs per loop to avoid overload
    # Strategy: take up to MAX_CONCURRENCY * 2 jobs per iteration
    budget = MAX_CONCURRENCY * 2
    batch = active_jobs[:budget]
    tasks = [asyncio.create_task(_worker(j)) for j in batch]
    await asyncio.gather(*tasks, return_exceptions=True)

    # Summarize
    total = sum(r.get("count", 0) for r in results)
    logger.info(f"[SUMMARY] processed {len(results)} jobs, total entities {total}")
    for r in results:
        try:
            logger.info(
                f"[SUMMARY] job={r.get('job_id')} account={r.get('account')} "
                f"stored={r.get('stored')} errors={r.get('errors')} "
                f"posts={r.get('stats', {}).get('posts_scraped', 0)} "
                f"comments={r.get('stats', {}).get('comments_scraped', 0)} "
                f"took={r.get('duration_sec'):.2f}s"
            )
        except Exception:
            pass


async def main():
    _ensure_dirs()
    pool = AccountPoolManager(accounts_file=ENVACTIVE_PATH)
    # initial load
    await pool.load_accounts()

    # Initialize PostgreSQL storage (protocol-compatible)
    storage = PostgreSQLMinerStorage(
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )

    logger.info("Reddit Gravity Orchestrator started.")
    logger.info(f"Gravity: {GRAVITY_REDDIT_JSON} | envActive: {ENVACTIVE_PATH}")
    logger.info(f"Loop interval: {LOOP_INTERVAL_SEC}s | Max concurrency: {MAX_CONCURRENCY}")

    # Periodic loop
    while True:
        try:
            # Reload accounts each loop in case envActive changed
            await pool.load_accounts()
            await orchestrate_once(pool, storage)
        except Exception as e:
            logger.error(f"[LOOP] error: {e}")
        await asyncio.sleep(LOOP_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting orchestrator...")
