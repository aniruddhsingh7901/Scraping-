#!/usr/bin/env python3
"""
x_gravity_orchestrator.py

24/7 PM2-friendly orchestrator for X (Twitter):
- Reads gravity/x.json (produced by monitor-gravity.js)
- Converts items into scrape jobs for X using ApiDojoTwitterScraper (Apify Actor)
    * label: "@username" or "#hashtag" or plain keyword
    * keyword: "..." (string or list)
    * post_start_datetime / post_end_datetime (ISO 8601, optional)
- Executes scraping without local X accounts (uses Apify actor)
- Supports on-demand jobs via a local JSON queue file
- Writes results to PostgreSQL via storage_postgresql.PostgreSQLMinerStorage
- Optional JSON artifacts are DISABLED by default to save disk

Environment variables:
  GRAVITY_X_JSON        Path to gravity/x.json (default: gravity/x.json)
  LOOP_INTERVAL_SEC     Orchestrator loop interval (default: 120)
  MAX_CONCURRENCY       Max concurrent jobs per loop (default: 3)
  MAX_ITEMS_PER_JOB     Per-job max items to scrape (default: 150)
  DAYS_BACK_DEFAULT     Default days back for time window if missing (default: 7)
  WRITE_JOB_FILES       "1" to write per-job artifacts (default: "0")
  WRITE_ONDEMAND_FILES  "1" to write ondemand artifacts (default: "0")
  ONDEMAND_FILE         Path to local on-demand queue (default: OnDEMANDX/ondemand_requests.json)
  OUTPUT_DIR            Base output dir for job artifacts (default: OnDEMANDX/logs/x_scrapes)
  ONDEMAND_OUTPUT_DIR   Output dir for ondemand artifacts (default: OnDEMANDX/logs/ondemand)

PostgreSQL storage environment variables:
  PG_DATABASE           DB name (default: reddit_miner_db)
  PG_USER               DB user (default: postgres)
  PG_PASSWORD           DB password (default: postgres)
  PG_HOST               Host (default: localhost)
  PG_PORT               Port (default: 5432)

Notes on twscrape:
- This orchestrator uses Apify actor (ApiDojo) and does NOT require twscrape accounts.
- If you also run twscrape elsewhere, you can run another instance by pointing it to a different SQLite DB file:
  python3 -m twscrape.cli --db accounts_x2.db ...
  Using a separate --db avoids lock contention and keeps instances independent.
"""

import os
import sys
import json
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
# Ensure local twscrape package import works without pip install
TWSCRAPE_PKG_DIR = os.path.join(REPO_ROOT, "twscrape")
if TWSCRAPE_PKG_DIR not in sys.path:
    sys.path.insert(0, TWSCRAPE_PKG_DIR)

# Data Universe imports
from common.data import DataEntity, DataLabel  # noqa: E402
from common.date_range import DateRange  # noqa: E402

# Scraper (Apify actor based)
from scraping.scraper import ScrapeConfig  # noqa: E402
from twscrape_scraper import TwscrapeTwitterScraper  # noqa: E402

# PostgreSQL storage adapter (protocol-compatible)
from storage_postgresql import PostgreSQLMinerStorage  # noqa: E402

logger = logging.getLogger("x_gravity_orchestrator")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Config via env
GRAVITY_X_JSON = os.getenv("GRAVITY_X_JSON", "gravity/x.json")
LOOP_INTERVAL_SEC = int(os.getenv("LOOP_INTERVAL_SEC", "120"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "3"))
MAX_ITEMS_PER_JOB = int(os.getenv("MAX_ITEMS_PER_JOB", "150"))
DAYS_BACK_DEFAULT = int(os.getenv("DAYS_BACK_DEFAULT", "7"))

# Control artifact writing (disabled by default to save disk)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "OnDEMANDX/logs/x_scrapes")
ONDEMAND_OUTPUT_DIR = os.getenv("ONDEMAND_OUTPUT_DIR", "OnDEMANDX/logs/ondemand")
WRITE_JOB_FILES = os.getenv("WRITE_JOB_FILES", "0") == "1"
WRITE_ONDEMAND_FILES = os.getenv("WRITE_ONDEMAND_FILES", "0") == "1"

# On-demand queue file (JSON)
ONDEMAND_FILE = os.getenv("ONDEMAND_FILE", "OnDEMANDX/ondemand_requests.json")

# PostgreSQL storage configuration
PG_DATABASE = os.getenv("PG_DATABASE", "reddit_miner_db")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")

# twscrape configuration (separate DB to avoid conflicts with other instances)
TWSCRAPE_DB = os.getenv("TWSCRAPE_DB", "accounts_x2.db")  # provide absolute path if desired
TWSCRAPE_PROXY = os.getenv("TWSCRAPE_PROXY")  # optional, e.g. http://host:port


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


def load_gravity_x_jobs(path: str) -> List[Dict[str, Any]]:
    """
    Load and filter jobs where params.platform == 'x'
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        jobs: List[Dict[str, Any]] = []
        for item in data:
            params = item.get("params", {}) or {}
            if params.get("platform", "").lower() != "x":
                continue
            jobs.append(item)
        return jobs
    except FileNotFoundError:
        logger.warning(f"Gravity X file not found: {path}")
        return []
    except Exception as e:
        logger.error(f"Failed to read {path}: {e}")
        return []


def entity_in_window(entity: DataEntity, start: Optional[datetime], end: Optional[datetime]) -> bool:
    dt = entity.datetime
    if start and dt < start:
        return False
    if end and dt > end:
        return False
    return True


def labels_from_job(params: Dict[str, Any]) -> List[DataLabel]:
    labels: List[DataLabel] = []
    label = params.get("label")
    if label:
        try:
            labels.append(DataLabel(value=str(label)))
        except Exception:
            pass
    keyword = params.get("keyword")
    if isinstance(keyword, list):
        for kw in keyword:
            try:
                labels.append(DataLabel(value=str(kw)))
            except Exception:
                pass
    elif isinstance(keyword, str) and keyword.strip():
        try:
            labels.append(DataLabel(value=keyword))
        except Exception:
            pass
    return labels


async def run_job(
    job: Dict[str, Any],
    storage: PostgreSQLMinerStorage,
    scraper: TwscrapeTwitterScraper,
) -> Dict[str, Any]:
    """
    Run a single gravity X job using ApiDojoTwitterScraper (Apify).
    Returns dict with:
      {
        "job_id": ...,
        "count": int,
        "path": output_file or None,
        "errors": int,
        "stored": int,
        "stats": {},
        "duration_sec": float
      }
    """
    job_id = job.get("id", "unknown")
    params = job.get("params", {}) or {}

    # Time window
    start_dt = parse_iso8601(params.get("post_start_datetime"))
    end_dt = parse_iso8601(params.get("post_end_datetime"))

    # Derive default window if missing: last DAYS_BACK_DEFAULT days
    if not end_dt:
        end_dt = now_utc()
    if not start_dt:
        start_dt = end_dt - timedelta(days=DAYS_BACK_DEFAULT)

    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    labels = labels_from_job(params)
    limit = int(params.get("limit") or MAX_ITEMS_PER_JOB)

    # Build scrape config
    cfg = ScrapeConfig(
        entity_limit=limit,
        date_range=DateRange(start=start_dt, end=end_dt),
        labels=labels if labels else None,
    )

    # scraper provided by orchestrator
    entities: List[DataEntity] = []
    errors = 0
    start_t = asyncio.get_event_loop().time()

    try:
        # Allow low engagement for gravity jobs? False by default to keep quality
        results = await scraper.scrape(cfg, allow_low_engagement=False)
        entities.extend(results)
    except Exception as e:
        logger.error(f"[{job_id}] job failed: {e}")
        errors += 1

    # Filter to exact window (scraper should already respect it, but be strict)
    filtered = [e for e in entities if entity_in_window(e, start_dt, end_dt)]

    # Store to PostgreSQL (preserve protocol: DataEntity as-is)
    try:
        if filtered:
            storage.store_data_entities(filtered)
    except Exception as e:
        logger.error(f"[{job_id}] DB store failed: {e}")

    # Optional JSON artifact
    out: List[Dict[str, Any]] = [e.to_json_dict() for e in filtered]
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path: Optional[str] = None
    if WRITE_JOB_FILES:
        out_path = os.path.join(OUTPUT_DIR, f"{job_id}_{ts}.json")
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({"job_id": job_id, "count": len(out), "entities": out}, f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"[{job_id}] failed to write output: {e}")

    out_path_str = out_path if out_path else "(disabled)"
    duration = asyncio.get_event_loop().time() - start_t

    # Logging
    label_dbg = [l.value for l in labels] if labels else []
    logger.info(
        f"[JOB] id={job_id} labels={label_dbg} "
        f"window=({start_dt.isoformat()} .. {end_dt.isoformat()}) "
        f"stored={len(filtered)} errors={errors} took={duration:.2f}s file={out_path_str}"
    )

    return {
        "job_id": job_id,
        "count": len(out),
        "path": out_path,
        "errors": errors,
        "stored": len(filtered),
        "stats": {},
        "duration_sec": duration,
    }


async def process_ondemand_file(storage: PostgreSQLMinerStorage, scraper: TwscrapeTwitterScraper) -> Optional[str]:
    """
    If an on-demand file exists, process and rename it to .processed-TS.
    Expected schema:
      {
        "requests": [
          {
            "usernames": ["elonmusk","binance"] | null,
            "keywords": ["bitcoin","tao"] | null,
            "url": "https://x.com/.../status/...",
            "keyword_mode": "any"|"all",
            "start_datetime": ISO8601,
            "end_datetime": ISO8601,
            "limit": 100
          },
          ...
        ]
      }
    Writes optional output JSON into ONDEMAND_OUTPUT_DIR and returns output path (or None).
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

    # scraper provided by orchestrator
    all_entities: List[DataEntity] = []

    for idx, req in enumerate(requests):
        usernames = req.get("usernames")
        keywords = req.get("keywords")
        url = req.get("url")
        keyword_mode = req.get("keyword_mode") or "all"
        limit = int(req.get("limit") or 100)
        start_dt = parse_iso8601(req.get("start_datetime"))
        end_dt = parse_iso8601(req.get("end_datetime"))

        logger.info(
            f"[ONDEMAND] req#{idx+1}: usernames={usernames} keywords={keywords} "
            f"mode={keyword_mode} limit={limit} start={start_dt} end={end_dt} url={url}"
        )
        try:
            ents = await scraper.on_demand_scrape(
                usernames=usernames,
                keywords=keywords,
                url=url,
                keyword_mode=keyword_mode,
                start_datetime=start_dt,
                end_datetime=end_dt,
                limit=limit,
            )
            all_entities.extend(ents)
        except Exception as e:
            logger.error(f"[ONDEMAND] request failed: {e}")

    # Store on-demand results to PostgreSQL
    try:
        if all_entities:
            storage.store_data_entities(all_entities)
    except Exception as e:
        logger.error(f"[ONDEMAND] DB store failed: {e}")

    # Optional artifact
    out = [e.to_json_dict() for e in all_entities]
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path: Optional[str] = None
    if WRITE_ONDEMAND_FILES:
        out_path = os.path.join(ONDEMAND_OUTPUT_DIR, f"ondemand_{ts}.json")
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({"count": len(out), "entities": out}, f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"[ONDEMAND] failed to write output: {e}")

    out_path_str = out_path if out_path else "(disabled)"
    logger.info(f"[ONDEMAND] wrote {len(out)} entities -> {out_path_str}")
    logger.info(f"[ONDEMAND] summary: requests={len(requests)} total_entities={len(all_entities)} stored_to_db={len(all_entities)}")

    # Mark processed
    try:
        new_name = f"{ONDEMAND_FILE}.processed-{ts}"
        os.replace(ONDEMAND_FILE, new_name)
    except Exception:
        pass

    return out_path


async def process_ondemand_requests(storage: PostgreSQLMinerStorage, requests: List[Dict[str, Any]], persist: bool = False) -> List[DataEntity]:
    """
    Direct on-demand processing for X (without using the ondemand_requests.json file).
    Each request item schema:
      {
        "usernames": ["elonmusk","binance"] | null,
        "keywords": ["bitcoin","tao"] | null,
        "url": "https://x.com/.../status/...",
        "keyword_mode": "any"|"all",
        "start_datetime": ISO8601,
        "end_datetime": ISO8601,
        "limit": 100
      }
    Returns the list of DataEntity objects produced. Also stores to PostgreSQL.
    """
    all_entities: List[DataEntity] = []
    # Initialize twscrape instance for direct processing
    scraper = TwscrapeTwitterScraper(
        db_path=TWSCRAPE_DB,
        proxy=TWSCRAPE_PROXY or None,
        debug=False,
        raise_when_no_account=True,
    )
    if not requests:
        return all_entities

    def _parse_iso8601(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s)
        except Exception:
            return None

    for idx, req in enumerate(requests):
        try:
            usernames = req.get("usernames")
            keywords = req.get("keywords")
            url = req.get("url")
            keyword_mode = req.get("keyword_mode") or "all"
            limit = int(req.get("limit") or MAX_ITEMS_PER_JOB)
            start_dt = _parse_iso8601(req.get("start_datetime"))
            end_dt = _parse_iso8601(req.get("end_datetime"))

            logger.info(
                f"[ONDEMAND.DIRECT] req#{idx+1}: usernames={usernames} keywords={keywords} "
                f"mode={keyword_mode} limit={limit} start={start_dt} end={end_dt} url={url}"
            )

            ents = await scraper.on_demand_scrape(
                usernames=usernames,
                keywords=keywords,
                url=url,
                keyword_mode=keyword_mode,
                start_datetime=start_dt,
                end_datetime=end_dt,
                limit=limit,
            )
            all_entities.extend(ents)
        except Exception as e:
            logger.error(f"[ONDEMAND.DIRECT] request failed: {e}")

    # Store to PostgreSQL (optional live-only control)
    if persist:
        try:
            if all_entities:
                storage.store_data_entities(all_entities)
        except Exception as e:
            logger.error(f"[ONDEMAND.DIRECT] DB store failed: {e}")

    return all_entities


async def orchestrate_once(storage: PostgreSQLMinerStorage, scraper: TwscrapeTwitterScraper) -> None:
    """
    Single iteration:
      - Load gravity x jobs
      - Filter by active window
      - Execute up to MAX_CONCURRENCY jobs
      - Process on-demand requests (if any)
    """
    # Process on-demand first (higher priority)
    try:
        await process_ondemand_file(storage, scraper)
    except Exception as e:
        logger.error(f"[LOOP] ondemand processing failed: {e}")

    jobs = load_gravity_x_jobs(GRAVITY_X_JSON)
    if not jobs:
        logger.info("No X jobs found in gravity; sleeping...")
        return

    ref = now_utc()
    active_jobs = [j for j in jobs if job_within_window(j, ref)]
    if not active_jobs:
        logger.info("No X jobs active at this time; sleeping...")
        return

    # Prepare concurrency controls
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results: List[Dict[str, Any]] = []

    async def _worker(job: Dict[str, Any]):
        async with sem:
            res = await run_job(job=job, storage=storage, scraper=scraper)
            results.append(res)

    # Limit total jobs per loop to avoid overload
    budget = MAX_CONCURRENCY * 2
    batch = active_jobs[:budget]
    tasks = [asyncio.create_task(_worker(j)) for j in batch]
    await asyncio.gather(*tasks, return_exceptions=True)

    # Summarize
    total = sum(r.get("stored", 0) for r in results)
    logger.info(f"[SUMMARY] processed {len(results)} X jobs, total entities stored {total}")
    for r in results:
        try:
            logger.info(
                f"[SUMMARY] job={r.get('job_id')} stored={r.get('stored')} "
                f"errors={r.get('errors')} took={r.get('duration_sec'):.2f}s"
            )
        except Exception:
            pass


async def main():
    _ensure_dirs()

    # Initialize PostgreSQL storage (protocol-compatible)
    storage = PostgreSQLMinerStorage(
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )

    # Initialize twscrape against a dedicated accounts DB (separate from other running instance)
    scraper = TwscrapeTwitterScraper(
        db_path=TWSCRAPE_DB,
        proxy=TWSCRAPE_PROXY or None,
        debug=False,
        raise_when_no_account=True,
    )

    logger.info("X Gravity Orchestrator started.")
    logger.info(f"Gravity X: {GRAVITY_X_JSON}")
    logger.info(f"Loop interval: {LOOP_INTERVAL_SEC}s | Max concurrency: {MAX_CONCURRENCY}")
    logger.info(f"twscrape DB: {TWSCRAPE_DB} | proxy: {TWSCRAPE_PROXY or '(none)'}")

    # Periodic loop
    while True:
        try:
            await orchestrate_once(storage, scraper)
        except Exception as e:
            logger.error(f"[LOOP] error: {e}")
        await asyncio.sleep(LOOP_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting orchestrator...")
