"""
FTL Collector — FencingTimeLive data collection.

Discovered FTL API (via browser network analysis):
  events/results/data/{event_id}          → JSON: all fencers + placement + fencer GUIDs
  pools/results/data/{event_id}/{pool_id} → JSON: all fencers' aggregate pool stats
                                            (v, m, ts, tr, ind, prediction, place)
  events/results/{event_id}               → HTML: contains pool links (for pool_id_seed discovery)

NOTE: Individual pool bouts and DE bouts are loaded via socket.io (not standard HTTP).
      We collect aggregate pool stats (V, L, TS, TR, indicator) per event via the JSON API.
      Individual bout data is a Phase 2 addition requiring a headless browser or socket.io client.

FTL login will be required from 2026-04-14 — monitor and add auth when needed.
"""

import re
import time
import logging
from typing import Optional
import httpx
from bs4 import BeautifulSoup

from database.client import get_write_client

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────
FTL_BASE        = "https://www.fencingtimelive.com"
REQUEST_DELAY   = 1.5   # seconds between requests — be polite
REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-GB,en;q=0.9",
}


# ── HTTP helpers ───────────────────────────────────────────────

def _get_json(url: str) -> Optional[list | dict]:
    """Fetch a URL and return parsed JSON, or None on failure."""
    try:
        time.sleep(REQUEST_DELAY)
        r = httpx.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"JSON fetch failed {url}: {e}")
        return None


def _get_html(url: str) -> Optional[BeautifulSoup]:
    """Fetch a URL and return parsed HTML, or None on failure."""
    try:
        time.sleep(REQUEST_DELAY)
        r = httpx.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        logger.error(f"HTML fetch failed {url}: {e}")
        return None


# ── Name matching ──────────────────────────────────────────────

def _name_matches(ftl_name: str, athlete_ftl_name: str) -> bool:
    """
    Word-set matching: all words in athlete's stored name must appear in the FTL row name.
    Handles formats like 'PANGA Daniel', 'PANGA Daniel J', 'Daniel PANGA'.
    """
    row_words    = set(ftl_name.upper().split())
    target_words = set(athlete_ftl_name.upper().split())
    return target_words.issubset(row_words)


# ── Pool ID discovery ──────────────────────────────────────────

def discover_pool_id_seed(ftl_event_id: str) -> Optional[str]:
    """
    Fetch the events/results/{event_id} HTML page and extract any pool_id link.
    Returns the first pool_id found, or None.

    This gives us a pool_id 'seed' we can then use with pools/results/data/
    to fetch full pool standings for all fencers.
    """
    url = f"{FTL_BASE}/events/results/{ftl_event_id}"
    soup = _get_html(url)
    if not soup:
        return None

    # Pool links are embedded in the HTML: /pools/scores/{event_id}/{pool_id}
    for tag in soup.find_all(href=True):
        m = re.search(r"/pools/scores/[A-F0-9]{32}/([A-F0-9]{32})", tag["href"], re.I)
        if m:
            return m.group(1).upper()

    # Also check raw HTML text (sometimes in JS vars)
    pattern = re.compile(
        r"pools/scores/[A-F0-9]{32}/([A-F0-9]{32})", re.I
    )
    m = pattern.search(soup.get_text())
    return m.group(1).upper() if m else None


# ── Event results (placements + fencer IDs) ────────────────────

def fetch_event_results(ftl_event_id: str) -> list[dict]:
    """
    Fetch /events/results/data/{event_id}.
    Returns a list of dicts: {id, name, place, clubs, country, ...}
    """
    url = f"{FTL_BASE}/events/results/data/{ftl_event_id}"
    data = _get_json(url)
    return data if isinstance(data, list) else []


def get_fencer_placement(ftl_event_id: str, name_ftl: str) -> tuple[Optional[int], int]:
    """
    Return (placement, field_size) for an athlete at an event.
    placement is None if not found.
    """
    results = fetch_event_results(ftl_event_id)
    field_size = len(results)
    for entry in results:
        if entry.get("name") and _name_matches(entry["name"], name_ftl):
            try:
                placement = int(entry.get("place", 0))
            except (ValueError, TypeError):
                placement = None
            return placement, field_size
    return None, field_size


# ── Pool aggregate stats ───────────────────────────────────────

def fetch_pool_stats(ftl_event_id: str, pool_id_seed: str, name_ftl: str) -> Optional[dict]:
    """
    Fetch /pools/results/data/{event_id}/{pool_id}.
    Returns the fencer's aggregate pool stats dict, or None if not found.

    The endpoint returns all fencers in the event (not just one pool),
    so any valid pool_id works as the second path segment.

    Returned dict fields:
      pool_v    — victories
      pool_l    — losses
      pool_ts   — touches scored
      pool_tr   — touches received
      pool_ind  — indicator (ts - tr)
      advanced_to_de — True if prediction == "Advanced"
    """
    url = f"{FTL_BASE}/pools/results/data/{ftl_event_id}/{pool_id_seed}"
    data = _get_json(url)
    if not isinstance(data, list):
        return None

    for entry in data:
        if entry.get("name") and _name_matches(entry["name"], name_ftl):
            v = int(entry.get("v", 0))
            m = int(entry.get("m", 0))
            ts = int(entry.get("ts", 0))
            tr = int(entry.get("tr", 0))
            return {
                "pool_v":          v,
                "pool_l":          m - v,
                "pool_ts":         ts,
                "pool_tr":         tr,
                "pool_ind":        ts - tr,
                "advanced_to_de":  entry.get("prediction", "").lower() == "advanced",
            }

    logger.warning(f"'{name_ftl}' not found in pool data for event {ftl_event_id}")
    return None


# ── Main collection pipeline ───────────────────────────────────

def collect_athlete(athlete_id: str, name_ftl: str, force: bool = False) -> dict:
    """
    Full collection run for one athlete.

    Reads events from Supabase where ftl_event_id IS NOT NULL.
    For each event:
      1. Fetches placement + field_size from events/results/data/{event_id}
      2. Discovers pool_id_seed from events/results/{event_id} (if not cached)
      3. Fetches pool aggregate stats from pools/results/data/
      4. Updates the event row in Supabase

    NOTE: U10 and earlier events will be collected as their FTL event IDs
    are added to the events table — the collector handles all age categories
    automatically since it searches by athlete name regardless of category.

    Returns a summary dict with counts.
    """
    db = get_write_client()
    summary = {"events_updated": 0, "events_skipped": 0, "errors": []}

    # Load all events for this athlete that have a FTL event ID
    res = db.table("events")\
        .select("id, ftl_event_id, pool_id_seed, event_name, date")\
        .eq("athlete_id", athlete_id)\
        .not_.is_("ftl_event_id", "null")\
        .execute()

    if not res.data:
        logger.info(f"No events with ftl_event_id found for athlete {athlete_id}")
        return summary

    logger.info(f"Processing {len(res.data)} events for '{name_ftl}'")

    for event_row in res.data:
        event_db_id    = event_row["id"]
        ftl_event_id   = event_row["ftl_event_id"]
        pool_id_seed   = event_row.get("pool_id_seed")
        event_name     = event_row.get("event_name", "?")

        try:
            update = {}

            # 1. Get placement + field_size
            placement, field_size = get_fencer_placement(ftl_event_id, name_ftl)
            if placement is not None:
                update["placement"]   = placement
                update["field_size"]  = field_size
                logger.info(f"  {event_name}: placed {placement}/{field_size}")
            else:
                logger.warning(f"  {event_name}: fencer not found in event results")

            # 2. Discover pool_id_seed if not cached
            if not pool_id_seed:
                pool_id_seed = discover_pool_id_seed(ftl_event_id)
                if pool_id_seed:
                    update["pool_id_seed"] = pool_id_seed
                    logger.info(f"  {event_name}: discovered pool_id_seed={pool_id_seed}")

            # 3. Fetch pool aggregate stats
            if pool_id_seed:
                pool_stats = fetch_pool_stats(ftl_event_id, pool_id_seed, name_ftl)
                if pool_stats:
                    update.update(pool_stats)
                    logger.info(
                        f"  {event_name}: pool V{pool_stats['pool_v']}"
                        f"/L{pool_stats['pool_l']} "
                        f"TS{pool_stats['pool_ts']}-TR{pool_stats['pool_tr']} "
                        f"{'→DE' if pool_stats['advanced_to_de'] else '→OUT'}"
                    )

            # 4. Write updates to Supabase
            if update:
                db.table("events").update(update).eq("id", event_db_id).execute()
                summary["events_updated"] += 1
            else:
                summary["events_skipped"] += 1

        except Exception as e:
            logger.error(f"Error processing event {ftl_event_id}: {e}")
            summary["errors"].append(f"{event_name}: {e}")

    # Update last_refreshed timestamp
    db.table("athletes").update(
        {"last_refreshed": "now()"}
    ).eq("id", athlete_id).execute()

    return summary


def collect_all_athletes() -> dict:
    """Run collect_athlete for every active athlete in the database."""
    db = get_write_client()
    athletes = db.table("athletes")\
        .select("id, name_ftl")\
        .eq("active", True)\
        .not_.is_("name_ftl", "null")\
        .execute()

    totals = {"athletes": 0, "events_updated": 0, "errors": []}
    for athlete in (athletes.data or []):
        result = collect_athlete(athlete["id"], athlete["name_ftl"])
        totals["athletes"] += 1
        totals["events_updated"] += result["events_updated"]
        totals["errors"].extend(result["errors"])

    return totals
