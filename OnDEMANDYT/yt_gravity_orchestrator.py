#!/usr/bin/env python3
"""
yt_gravity_orchestrator.py

24/7 PM2-friendly orchestrator for YouTube:
- Reads gravity/yt.json (produced by monitor-gravity.js)
- Converts items into scrape jobs for YouTube using YouTubeTranscriptScraper
  • Labels use unified channel format e.g. "#ytc_c_fireship" (see youtube_custom_scraper)
  • post_start_datetime / post_end_datetime (ISO 8601, optional)
  • limit per job (MAX_VIDEOS_PER_JOB)
- Rotates YouTube Data API keys across jobs to avoid quota hotspots
- Uses pytube/pytubefix first for transcripts; falls back to youtube-transcript-api (in scraper)
- Supports on-demand jobs via a local JSON queue file
- Writes DataEntity to PostgreSQL via storage_postgresql.PostgreSQLMinerStorage
- Optional JSON artifacts are DISABLED by default to save disk

Environment variables:
  GRAVITY_YT_JSON        Path to gravity/yt.json (default: gravity/yt.json)
  LOOP_INTERVAL_SEC      Orchestrator loop interval (default: 120)
  MAX_CONCURRENCY        Max concurrent jobs per loop (default: 3)
  MAX_VIDEOS_PER_JOB     Per-job max videos to scrape (default: 50)
  DAYS_BACK_DEFAULT      Default days back for time window if missing (default: 7)
  WRITE_JOB_FILES        "1" to write per-job artifacts (default: "0")
  WRITE_ONDEMAND_FILES   "1" to write ondemand artifacts (default: "0")
  ONDEMAND_FILE          Path to local on-demand queue (default: OnDEMANDYT/ondemand_requests.json)
  OUTPUT_DIR             Base output dir for job artifacts (default: OnDEMANDYT/logs/yt_scrapes)
  ONDEMAND_OUTPUT_DIR    Output dir for ondemand artifacts (default: OnDEMANDYT/logs/ondemand)

YouTube API keys:
  YT_KEYS_FILE           Path to a file with one API key per line (default: OnDEMANDYT/youtube_api_keys.txt)
                         Keys are rotated round-robin across jobs. Invalid/failed keys are retried next loop.
  YOUTUBE_API_KEY        If set, used as a single key (YT_KEYS_FILE ignored). Orchestrator sets this per job
                         before constructing the YouTubeTranscriptScraper instance (scraper reads at init).

PostgreSQL storage environment variables:
  PG_DATABASE            DB name (default: reddit_miner_db)
  PG_USER                DB user (default: postgres)
  PG_PASSWORD            DB password (default: postgres)
  PG_HOST                Host (default: localhost)
  PG_PORT                Port (default: 5432)
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

# Resolve repository root and add to sys.path so imports work under PM2
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_UNIVERSE_DIR = os.path.join(REPO_ROOT, "data-universe")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if DATA_UNIVERSE_DIR not in sys.path:
    sys.path.insert(0, DATA_UNIVERSE_DIR)

# Data Universe imports
from common.data import DataEntity, DataLabel  # noqa: E402
from common.date_range import DateRange  # noqa: E402

# Scraper (custom YouTube scraper using pytube/youtube_transcript_api + YouTube Data API)
from scraping.scraper import ScrapeConfig  # noqa: E402
from scraping.youtube.youtube_custom_scraper import YouTubeTranscriptScraper  # noqa: E402

# PostgreSQL storage adapter (protocol-compatible)
from storage_postgresql import PostgreSQLMinerStorage  # noqa: E402

logger = logging.getLogger("yt_gravity_orchestrator")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Config via env
GRAVITY_YT_JSON = os.getenv("GRAVITY_YT_JSON", "gravity/yt.json")
LOOP_INTERVAL_SEC = int(os.getenv("LOOP_INTERVAL_SEC", "120"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "3"))
MAX_VIDEOS_PER_JOB = int(os.getenv("MAX_VIDEOS_PER_JOB", "50"))
DAYS_BACK_DEFAULT = int(os.getenv("DAYS_BACK_DEFAULT", "7"))

# Control artifact writing (disabled by default to save disk)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "OnDEMANDYT/logs/yt_scrapes")
ONDEMAND_OUTPUT_DIR = os.getenv("ONDEMAND_OUTPUT_DIR", "OnDEMANDYT/logs/ondemand")
WRITE_JOB_FILES = os.getenv("WRITE_JOB_FILES", "0") == "1"
WRITE_ONDEMAND_FILES = os.getenv("WRITE_ONDEMAND_FILES", "0") == "1"
# Allow bypassing gravity time windows (treat all YouTube jobs as active)
IGNORE_TIME_WINDOWS = os.getenv("IGNORE_TIME_WINDOWS", "0") == "1"

# On-demand queue file (JSON)
ONDEMAND_FILE = os.getenv("ONDEMAND_FILE", "OnDEMANDYT/ondemand_requests.json")

# PostgreSQL storage configuration
PG_DATABASE = os.getenv("PG_DATABASE", "reddit_miner_db")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")

# YouTube API keys rotation
YT_KEYS_FILE = os.getenv("YT_KEYS_FILE", "OnDEMANDYT/youtube_api_keys.txt")
ENV_SINGLE_KEY = os.getenv("YOUTUBE_API_KEY")  # If provided, use single key only


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
    # If override is enabled, treat all YouTube jobs as active
    if IGNORE_TIME_WINDOWS:
        return True
    params = job.get("params", {}) or {}
    start = parse_iso8601(params.get("post_start_datetime"))
    end = parse_iso8601(params.get("post_end_datetime"))

    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    # Consider expired windows as active; run_job will slide window to last DAYS_BACK_DEFAULT days
    if start and ref < start:
        return False
    return True


def load_gravity_yt_jobs(path: str) -> List[Dict[str, Any]]:
    """
    Expect gravity/yt.json items like:
    [
      {
        "id": "default_yt_1",
        "params": {
          "platform": "youtube",
          "label": "#ytc_c_fireship",   # unified channel label format
          "post_start_datetime": "...", # optional
          "post_end_datetime": "...",   # optional
          "limit": 50                   # optional per-job override
        }
      },
      ...
    ]
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        jobs: List[Dict[str, Any]] = []
        for item in data:
            params = item.get("params", {}) or {}
            if params.get("platform", "").lower() != "youtube":
                continue
            jobs.append(item)
        return jobs
    except FileNotFoundError:
        logger.warning(f"Gravity YouTube file not found: {path}")
        return []
    except Exception as e:
        logger.error(f"Failed to read {path}: {e}")
        return []


def labels_from_job(params: Dict[str, Any]) -> List[DataLabel]:
    labels: List[DataLabel] = []
    label = params.get("label")
    if label:
        try:
            labels.append(DataLabel(value=str(label)))
        except Exception:
            pass
    # Support keyword list for future expansion if needed; primary source is unified channel labels
    kws = params.get("keywords")
    if isinstance(kws, list):
        for kw in kws:
            try:
                labels.append(DataLabel(value=str(kw)))
            except Exception:
                pass
    elif isinstance(kws, str) and kws.strip():
        try:
            labels.append(DataLabel(value=kws))
        except Exception:
            pass
    return labels


class YTKeyRotator:
    def __init__(self, keys_file: str, env_single_key: Optional[str]):
        self.single_key = env_single_key.strip() if env_single_key else None
        self.keys: List[str] = []
        self.idx = 0
        if not self.single_key:
            self._load_file(keys_file)

    def _load_file(self, path: str):
        try:
            with open(path, "r", encoding="utf-8") as f:
                self.keys = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
        except FileNotFoundError:
            logger.warning(f"YouTube keys file not found: {path}")
            self.keys = []
        except Exception as e:
            logger.error(f"Failed to read keys file {path}: {e}")
            self.keys = []

    def next_key(self) -> Optional[str]:
        if self.single_key:
            return self.single_key
        if not self.keys:
            return None
        k = self.keys[self.idx % len(self.keys)]
        self.idx += 1
        return k


async def run_job(
    job: Dict[str, Any],
    storage: PostgreSQLMinerStorage,
    key_rotator: YTKeyRotator,
) -> Dict[str, Any]:
    """
    Run a single gravity YouTube job.
    Returns dict with:
      {
        "job_id": ...,
        "count": int,
        "path": output_file or None,
        "errors": int,
        "stored": int,
        "duration_sec": float
      }
    """
    job_id = job.get("id", "unknown")
    params = job.get("params", {}) or {}
    labels = labels_from_job(params)

    # Time window
    start_dt = parse_iso8601(params.get("post_start_datetime"))
    end_dt = parse_iso8601(params.get("post_end_datetime"))

    # Derive default window if missing: last DAYS_BACK_DEFAULT days
    if not end_dt:
        end_dt = now_utc()
    if not start_dt:
        start_dt = end_dt - timedelta(days=DAYS_BACK_DEFAULT)

    # If ignoring windows, or the job window is expired, use a fresh window ending now
    if IGNORE_TIME_WINDOWS or (end_dt and end_dt < now_utc()):
        end_dt = now_utc()
        start_dt = end_dt - timedelta(days=DAYS_BACK_DEFAULT)

    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    limit = int(params.get("limit") or MAX_VIDEOS_PER_JOB)

    # Build scrape config
    cfg = ScrapeConfig(
        entity_limit=limit,
        date_range=DateRange(start=start_dt, end=end_dt),
        labels=labels if labels else None,
    )

    # Pick API key for this job, set env so scraper picks it on init
    key = key_rotator.next_key()
    if key:
        os.environ["YOUTUBE_API_KEY"] = key

    scraper = YouTubeTranscriptScraper()

    entities: List[DataEntity] = []
    errors = 0
    start_t = asyncio.get_event_loop().time()

    try:
        results = await scraper.scrape(cfg)
        entities.extend(results)
    except Exception as e:
        logger.error(f"[{job_id}] job failed: {e}")
        errors += 1

    # Store to PostgreSQL (preserve protocol: DataEntity as-is)
    stored = 0
    try:
        if entities:
            storage.store_data_entities(entities)
            stored = len(entities)
    except Exception as e:
        logger.error(f"[{job_id}] DB store failed: {e}")

    # Optional JSON artifact
    out = [e.to_json_dict() for e in entities]
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

    label_dbg = [l.value for l in labels] if labels else []
    logger.info(
        f"[JOB] id={job_id} labels={label_dbg} "
        f"window=({start_dt.isoformat()} .. {end_dt.isoformat()}) "
        f"stored={stored} errors={errors} took={duration:.2f}s file={out_path_str}"
    )

    return {
        "job_id": job_id,
        "count": len(out),
        "path": out_path,
        "errors": errors,
        "stored": stored,
        "duration_sec": duration,
    }


async def process_ondemand_file(storage: PostgreSQLMinerStorage, key_rotator: YTKeyRotator) -> Optional[str]:
    """
    If an on-demand file exists, process and rename it to .processed-TS.
    Expected schema:
      {
        "requests": [
          {
            "labels": ["#ytc_c_fireship", "#ytc_c_ted"],  # unified labels
            "start_datetime": ISO8601,
            "end_datetime": ISO8601,
            "limit": 20
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

    all_entities: List[DataEntity] = []
    for idx, req in enumerate(requests):
        labels_raw = req.get("labels") or []
        labels = []
        for v in labels_raw:
            try:
                labels.append(DataLabel(value=str(v)))
            except Exception:
                pass

        limit = int(req.get("limit") or MAX_VIDEOS_PER_JOB)
        start_dt = parse_iso8601(req.get("start_datetime"))
        end_dt = parse_iso8601(req.get("end_datetime"))
        if not end_dt:
            end_dt = now_utc()
        if not start_dt:
            start_dt = end_dt - timedelta(days=DAYS_BACK_DEFAULT)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)

        logger.info(
            f"[ONDEMAND] req#{idx+1}: labels={[l.value for l in labels]} "
            f"limit={limit} start={start_dt} end={end_dt}"
        )

        cfg = ScrapeConfig(
            entity_limit=limit,
            date_range=DateRange(start=start_dt, end=end_dt),
            labels=labels if labels else None,
        )

        key = key_rotator.next_key()
        if key:
            os.environ["YOUTUBE_API_KEY"] = key

        scraper = YouTubeTranscriptScraper()
        try:
            ents = await scraper.scrape(cfg)
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


async def process_ondemand_requests(storage: PostgreSQLMinerStorage, requests: List[Dict[str, Any]], persist: bool = False, key_rotator: Optional[YTKeyRotator] = None) -> List[DataEntity]:
    """
    Process a list of on-demand requests (without touching any files).
    Each request item schema:
      {
        "labels": ["#ytc_c_fireship", "#ytc_c_ted"] | null,
        "start_datetime": ISO8601 | null,
        "end_datetime": ISO8601 | null,
        "limit": int | null
      }
    Returns the list of DataEntity objects produced. Also stores to PostgreSQL.
    """
    all_entities: List[DataEntity] = []

    if not requests:
        return all_entities

    for idx, req in enumerate(requests):
        # Labels -> DataLabel
        labels_raw = req.get("labels") or []
        labels: List[DataLabel] = []
        for v in labels_raw:
            try:
                labels.append(DataLabel(value=str(v)))
            except Exception:
                pass

        # Window
        start_dt = parse_iso8601(req.get("start_datetime"))
        end_dt = parse_iso8601(req.get("end_datetime"))
        if not end_dt:
            end_dt = now_utc()
        if not start_dt:
            start_dt = end_dt - timedelta(days=DAYS_BACK_DEFAULT)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)

        limit = int(req.get("limit") or MAX_VIDEOS_PER_JOB)

        logger.info(
            f"[ONDEMAND.DIRECT] req#{idx+1}: labels={[l.value for l in labels]} "
            f"limit={limit} start={start_dt} end={end_dt}"
        )

        # Build config
        cfg = ScrapeConfig(
            entity_limit=limit,
            date_range=DateRange(start=start_dt, end=end_dt),
            labels=labels if labels else None,
        )

        # Rotate/select API key
        if key_rotator:
            key = key_rotator.next_key()
            if key:
                os.environ["YOUTUBE_API_KEY"] = key

        # Scrape
        scraper = YouTubeTranscriptScraper()
        try:
            ents = await scraper.scrape(cfg)
            all_entities.extend(ents)
        except Exception as e:
            logger.error(f"[ONDEMAND.DIRECT] request failed: {e}")

    # Store to PostgreSQL (optional)
    if persist:
        try:
            if all_entities:
                storage.store_data_entities(all_entities)
        except Exception as e:
            logger.error(f"[ONDEMAND.DIRECT] DB store failed: {e}")

    return all_entities


async def orchestrate_once(storage: PostgreSQLMinerStorage, key_rotator: YTKeyRotator) -> None:
    """
    Single iteration:
      - Load gravity YouTube jobs
      - Filter by active window
      - Execute up to MAX_CONCURRENCY jobs
      - Process on-demand requests (if any)
    """
    # Process on-demand first (higher priority)
    try:
        await process_ondemand_file(storage, key_rotator)
    except Exception as e:
        logger.error(f"[LOOP] ondemand processing failed: {e}")

    jobs = load_gravity_yt_jobs(GRAVITY_YT_JSON)
    if not jobs:
        logger.info("No YouTube jobs found in gravity; sleeping...")
        return

    ref = now_utc()
    active_jobs = [j for j in jobs if job_within_window(j, ref)]
    if not active_jobs:
        logger.info("No YouTube jobs active at this time; sleeping...")
        return

    # Prepare concurrency controls
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results: List[Dict[str, Any]] = []

    async def _worker(job: Dict[str, Any]):
        async with sem:
            res = await run_job(job=job, storage=storage, key_rotator=key_rotator)
            results.append(res)

    # Limit total jobs per loop to avoid overload
    budget = MAX_CONCURRENCY * 2
    batch = active_jobs[:budget]
    tasks = [asyncio.create_task(_worker(j)) for j in batch]
    await asyncio.gather(*tasks, return_exceptions=True)

    # Summarize
    total = sum(r.get("stored", 0) for r in results)
    logger.info(f"[SUMMARY] processed {len(results)} YouTube jobs, total entities stored {total}")
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

    # Initialize YouTube keys rotator
    key_rotator = YTKeyRotator(keys_file=YT_KEYS_FILE, env_single_key=ENV_SINGLE_KEY)

    logger.info("YouTube Gravity Orchestrator started.")
    logger.info(f"Gravity YT: {GRAVITY_YT_JSON}")
    logger.info(f"Loop interval: {LOOP_INTERVAL_SEC}s | Max concurrency: {MAX_CONCURRENCY} | Max/videos/job: {MAX_VIDEOS_PER_JOB}")
    logger.info(f"Keys: {'single env key' if ENV_SINGLE_KEY else f'file {YT_KEYS_FILE}'}")

    # Periodic loop
    while True:
        try:
            await orchestrate_once(storage, key_rotator)
        except Exception as e:
            logger.error(f"[LOOP] error: {e}")
        await asyncio.sleep(LOOP_INTERVAL_SEC)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting orchestrator...")
