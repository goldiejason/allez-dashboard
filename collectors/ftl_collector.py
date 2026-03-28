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

Auth (required from 2026-04-14):
  FTL requires a free account to access tournament results.
  Set FTL_USERNAME and FTL_PASSWORD in .env — the collector handles login automatically.

  Login flow (reverse-engineered from /js/login.*.js):
    1. GET /account/login        → session cookie + CSRF token in <meta name='csrf_token'>
    2. POST /login               → body: {username, password}, header: x-csrf-token
                                   response body = redirect URL on success, error text on failure
    3. GET {redirect URL}        → establishes full authenticated session
    4. All subsequent requests   → httpx.Client preserves session cookies automatically
"""

import os
import re
import time
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from database.client import get_write_client

load_dotenv()

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

# ── Module-level authenticated client ─────────────────────────
# Created once at first use, reused across all requests.
# httpx.Client maintains a cookie jar automatically.
_client: Optional[httpx.Client] = None


def _login(client: httpx.Client) -> bool:
    """
    Log in to FTL using credentials from the environment.

    Flow (from /js/login.*.js):
      1. GET /account/login  — establish session cookie + extract CSRF token
      2. POST /login         — submit credentials with CSRF header
      3. Response body       — redirect URL on success, error string on failure
      4. GET redirect URL    — follow to fully establish the session

    Returns True on success, False on failure.
    """
    username = os.getenv("FTL_USERNAME", "").strip()
    password = os.getenv("FTL_PASSWORD", "").strip()

    if not username or not password:
        logger.warning(
            "FTL_USERNAME or FTL_PASSWORD not set in .env — "
            "running unauthenticated. This will break on 2026-04-14."
        )
        return False

    login_page_url = f"{FTL_BASE}/account/login"
    post_url       = f"{FTL_BASE}/login"

    try:
        # Step 1: GET login page — sets session cookie + gives us the CSRF token
        time.sleep(REQUEST_DELAY)
        resp = client.get(login_page_url)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        meta = soup.find("meta", {"name": "csrf_token"})
        if not meta:
            logger.error("FTL: <meta name='csrf_token'> not found on login page")
            return False
        csrf_token = meta.get("content", "")
        if not csrf_token:
            logger.error("FTL: CSRF token is empty")
            return False

        # Step 2: POST credentials — response body is redirect URL on success
        time.sleep(REQUEST_DELAY)
        post_resp = client.post(
            post_url,
            data={"username": username, "password": password},
            headers={"x-csrf-token": csrf_token},
            follow_redirects=False,
        )
        post_resp.raise_for_status()

        redirect_target = post_resp.text.strip()

        # Detect failure: success yields a URL path, failure yields an error string
        if not (redirect_target.startswith("/") or redirect_target.startswith("http")):
            logger.error(
                f"FTL login failed — server said: {redirect_target[:120]}"
            )
            return False

        # Step 3: Follow the redirect to fully establish the authenticated session
        time.sleep(REQUEST_DELAY)
        redir_full = (
            redirect_target
            if redirect_target.startswith("http")
            else f"{FTL_BASE}{redirect_target}"
        )
        client.get(redir_full, follow_redirects=True)

        logger.info("FTL login successful")
        return True

    except Exception as e:
        logger.error(f"FTL login error: {e}")
        return False


def _get_client() -> httpx.Client:
    """
    Return the shared authenticated httpx.Client, creating and logging in if needed.

    If credentials are absent or login fails, returns an unauthenticated client
    (which will continue to work until 2026-04-14).
    """
    global _client
    if _client is None:
        new_client = httpx.Client(
            headers=HEADERS,
            follow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        _login(new_client)   # mutates the client's cookie jar in place
        _client = new_client
    return _client


def _is_auth_redirect(resp: httpx.Response) -> bool:
    """
    Detect whether a response is actually a redirect to the login page —
    indicating the session has expired mid-run.
    """
    return "/account/login" in str(resp.url)


def _reauth_and_retry(url: str, is_json: bool) -> Optional[httpx.Response]:
    """
    Re-authenticate and retry a single request after a session expiry.
    Returns the new response or None on failure.
    """
    global _client
    logger.warning("FTL session expired — re-authenticating")
    _client = None  # force a fresh login on next _get_client() call
    new_client = _get_client()
    try:
        time.sleep(REQUEST_DELAY)
        resp = new_client.get(url)
        if _is_auth_redirect(resp):
            logger.error("FTL re-authentication failed — still redirecting to login")
            return None
        return resp
    except Exception as e:
        logger.error(f"FTL retry failed: {e}")
        return None


# ── HTTP helpers ───────────────────────────────────────────────

def _get_json(url: str) -> Optional[list | dict]:
    """Fetch a URL and return parsed JSON, or None on failure."""
    try:
        client = _get_client()
        time.sleep(REQUEST_DELAY)
        r = client.get(url)
        r.raise_for_status()

        # Session expiry returns a 200 redirect to login — detect and reauth
        if _is_auth_redirect(r):
            r = _reauth_and_retry(url, is_json=True)
            if r is None:
                return None
            r.raise_for_status()

        return r.json()
    except Exception as e:
        logger.error(f"JSON fetch failed {url}: {e}")
        return None


def _get_html(url: str) -> Optional[BeautifulSoup]:
    """Fetch a URL and return parsed HTML, or None on failure."""
    try:
        client = _get_client()
        time.sleep(REQUEST_DELAY)
        r = client.get(url)
        r.raise_for_status()

        if _is_auth_redirect(r):
            r = _reauth_and_retry(url, is_json=False)
            if r is None:
                return None
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
        {"last_refreshed": datetime.now(timezone.utc).isoformat()}
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
