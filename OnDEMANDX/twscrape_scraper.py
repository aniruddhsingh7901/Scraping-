#!/usr/bin/env python3
"""
twscrape_scraper.py

Thin wrapper around twscrape to provide a simple scraping interface compatible
with our orchestrator needs. It:
- Opens a specific twscrape SQLite DB (separate from any other running instance)
- Builds Twitter/X advanced search queries from labels/usernames/keywords and time windows
- Converts twscrape Tweet objects into DataEntity with rich JSON content similar to our Apify-based payload

Env:
- None required here; caller should pass db_path (path to twscrape SQLite file) and optional proxy.
"""

import re
import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
import datetime as dt

from common.data import DataEntity, DataLabel, DataSource
from common.date_range import DateRange

# twscrape local package import (ensure orchestrator added repo/twscrape to sys.path)
from twscrape.api import API
from twscrape.models import Tweet as TwTweet

log = logging.getLogger("twscrape_scraper")


def _iso8601(dt_obj: dt.datetime) -> str:
    # Always timezone-aware ISO string
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj.astimezone(dt.timezone.utc).isoformat()


def _date_clause(start: Optional[dt.datetime], end: Optional[dt.datetime]) -> List[str]:
    # Twitter web search typically accepts date granularity by day; however, the
    # format "YYYY-MM-DD_HH:MM:SS_UTC" also appears in some tooling. We'll keep
    # the richer format to maximize compatibility, falling back to dates.
    fmt = "%Y-%m-%d_%H:%M:%S_UTC"
    out: List[str] = []
    if start:
        z = start.astimezone(dt.timezone.utc)
        out.append(f"since:{z.strftime(fmt)}")
    if end:
        z = end.astimezone(dt.timezone.utc)
        out.append(f"until:{z.strftime(fmt)}")
    return out


def _build_query(
    labels: Optional[List[DataLabel]],
    keywords: Optional[List[str]],
    start: Optional[dt.datetime],
    end: Optional[dt.datetime],
    keyword_mode: str = "any",  # "any"|"all"
) -> str:
    parts: List[str] = []

    # time window
    parts.extend(_date_clause(start, end))

    # split usernames vs general keywords from labels
    usernames: List[str] = []
    label_keywords: List[str] = []
    for lb in (labels or []):
        v = lb.value.strip()
        if v.startswith("@"):
            usernames.append(v.lstrip("@"))
        else:
            label_keywords.append(v)

    if usernames:
        # from:user OR from:user2
        parts.append("(" + " OR ".join([f"from:{u}" for u in usernames]) + ")")

    all_keywords: List[str] = []
    if keywords:
        all_keywords.extend(keywords)
    all_keywords.extend(label_keywords)

    if all_keywords:
        quoted = [f'"{kw}"' if " " in kw else kw for kw in all_keywords]
        if keyword_mode == "all":
            parts.append("(" + " AND ".join(quoted) + ")")
        else:
            parts.append("(" + " OR ".join(quoted) + ")")

    # fallback term if query is empty
    if not parts:
        parts.append("e")

    return " ".join(parts)


def _tweet_to_entity(tw: TwTweet, label: Optional[DataLabel]) -> DataEntity:
    # Build content payload similar to our XContent JSON used previously
    payload: Dict[str, Any] = {
        "username": f"@{tw.user.username}",
        "display_name": tw.user.displayname,
        "text": tw.rawContent,
        "url": tw.url,
        "timestamp": _iso8601(tw.date),
        "tweet_hashtags": ["#" + x for x in (tw.hashtags or [])],
        "cashtags": ["$" + x for x in (tw.cashtags or [])],
        "language": tw.lang,
        "in_reply_to_tweet_id": tw.inReplyToTweetId,
        "in_reply_to_user": (tw.inReplyToUser.dict() if tw.inReplyToUser else None),
        "conversation_id": tw.conversationId,
        "like_count": tw.likeCount,
        "retweet_count": tw.retweetCount,
        "reply_count": tw.replyCount,
        "quote_count": tw.quoteCount,
        "view_count": tw.viewCount,
        "media": {
            "photos": [m.url for m in (tw.media.photos if tw.media else [])],
            "videos": [
                {
                    "thumbnail": m.thumbnailUrl,
                    "duration_ms": m.duration,
                    "variants": [{"bitrate": v.bitrate, "url": v.url, "contentType": v.contentType} for v in m.variants],
                }
                for m in (tw.media.videos if tw.media else [])
            ],
            "animated": [{"thumbnail": m.thumbnailUrl, "url": m.videoUrl} for m in (tw.media.animated if tw.media else [])],
        },
        "possibly_sensitive": tw.possibly_sensitive,
        "card_type": getattr(tw.card, "_type", None) if tw.card else None,
        "source_label": tw.sourceLabel,
        "source_url": tw.sourceUrl,
        "place": tw.place.dict() if tw.place else None,
        "coordinates": tw.coordinates.dict() if tw.coordinates else None,
    }

    content_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return DataEntity(
        uri=tw.url,
        datetime=tw.date if tw.date.tzinfo else tw.date.replace(tzinfo=dt.timezone.utc),
        source=DataSource.X,
        label=label,
        content=content_bytes,
        content_size_bytes=len(content_bytes),
    )


class TwscrapeTwitterScraper:
    def __init__(
        self,
        db_path: str,
        proxy: Optional[str] = None,
        debug: bool = False,
        raise_when_no_account: bool = True,
    ):
        # Create API bound to a specific accounts DB and optional proxy
        self.api = API(pool=db_path, proxy=proxy, debug=debug, raise_when_no_account=raise_when_no_account)

    async def scrape(self, scrape_config, allow_low_engagement: bool = False) -> List[DataEntity]:
        """
        Scrape tweets using twscrape search timeline.
        scrape_config: ScrapeConfig(entity_limit:int, date_range:DateRange, labels:List[DataLabel]|None)
        """
        start = scrape_config.date_range.start
        end = scrape_config.date_range.end
        labels = scrape_config.labels or []
        # Build a unified query. No separate keywords array in ScrapeConfig, so labels carry hashtags/usernames/keywords.
        q = _build_query(labels=labels, keywords=None, start=start, end=end, keyword_mode="any")

        out: List[DataEntity] = []
        label_for_entity: Optional[DataLabel] = labels[0] if labels else None

        try:
            cnt = 0
            async for tw in self.api.search(q, limit=scrape_config.entity_limit or -1):
                # tw is twscrape.models.Tweet
                ent = _tweet_to_entity(tw, label_for_entity)
                # strict window filter
                if (start and ent.datetime < start) or (end and ent.datetime > end):
                    continue
                out.append(ent)
                cnt += 1
                if scrape_config.entity_limit and cnt >= scrape_config.entity_limit:
                    break
        except Exception as e:
            log.error(f"[twscrape] scrape failed: {e}")

        return out

    async def on_demand_scrape(
        self,
        usernames: List[str] = None,
        keywords: List[str] = None,
        url: str = None,
        keyword_mode: str = "all",
        start_datetime: dt.datetime = None,
        end_datetime: dt.datetime = None,
        limit: int = 100,
    ) -> List[DataEntity]:
        """
        On-demand scraping using twscrape.
        - If url is provided, fetch that tweet via tweet_details
        - Otherwise perform a search query combining usernames and keywords within [start_datetime, end_datetime]
        """
        usernames = usernames or []
        keywords = keywords or []

        # URL direct fetch
        if url:
            m = re.search(r"/status/(\\d+)", url)
            if not m:
                log.error(f"[twscrape] invalid tweet URL: {url}")
                return []
            twid = int(m.group(1))
            try:
                tw = await self.api.tweet_details(twid)
                if not tw:
                    return []
                # choose a label if any keyword present; else username label
                label: Optional[DataLabel] = None
                if keywords:
                    try:
                        label = DataLabel(value=keywords[0])
                    except Exception:
                        label = None
                elif usernames:
                    try:
                        label = DataLabel(value="@" + usernames[0])
                    except Exception:
                        label = None
                return [_tweet_to_entity(tw, label)]
            except Exception as e:
                log.error(f"[twscrape] tweet_details failed: {e}")
                return []

        # Build labels list from usernames (prefix @) plus treat keywords as labels for display
        labels: List[DataLabel] = []
        for u in usernames:
            try:
                labels.append(DataLabel(value="@" + u.lstrip("@")))
            except Exception:
                pass
        for kw in keywords:
            try:
                labels.append(DataLabel(value=str(kw)))
            except Exception:
                pass

        q = _build_query(
            labels=labels,
            keywords=None,  # already part of labels
            start=start_datetime,
            end=end_datetime,
            keyword_mode=keyword_mode or "all",
        )

        out: List[DataEntity] = []
        chosen_label: Optional[DataLabel] = labels[0] if labels else None

        try:
            cnt = 0
            async for tw in self.api.search(q, limit=limit or -1):
                ent = _tweet_to_entity(tw, chosen_label)
                if (start_datetime and ent.datetime < start_datetime) or (end_datetime and ent.datetime > end_datetime):
                    continue
                out.append(ent)
                cnt += 1
                if limit and cnt >= limit:
                    break
        except Exception as e:
            log.error(f"[twscrape] on_demand scrape failed: {e}")

