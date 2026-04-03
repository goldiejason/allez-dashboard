"""
FTL Collector — FencingTimeLive data collection.

Discovered FTL API (via browser network analysis):
  events/results/data/{event_id}          → JSON: all fencers + placement + fencer GUIDs
  pools/results/data/{event_id}/{pool_id} → JSON: all fencers' aggregate pool stats
                                            (v, m, ts, tr, ind, prediction, place)
  events/results/{event_id}               → HTML: contains pool links (for pool_id_seed discovery)

Pool bout collection (Phase 2 — implemented):
  pools/scores/{event_id}/{pool_id}  → HTML: full bout matrix for ALL pools in the event.
  Despite early notes suggesting socket.io, completed events render the full bout matrix
  as plain HTML — one request returns every pool. Individual bouts are parsed directly
  from the table: cell[opp_pos + 1] gives the bout result (e.g. "V5" or "D3") for the
  fencer in that row vs the opponent at that column position.

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
from core.identity import IdentityResolver

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

# Matches a pool bout cell: "V5" (victory, 5 scored) or "D3" (defeat, 3 scored)
_BOUT_CELL = re.compile(r'^([VD])(\d+)$')

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
# Module-level IdentityResolver — instantiated once per process.
# The DB client is injected lazily on first call that needs alias persistence.
_resolver: Optional[IdentityResolver] = None


def _get_resolver() -> IdentityResolver:
    """Return (or lazily create) the module-level IdentityResolver."""
    global _resolver
    if _resolver is None:
        try:
            db = get_write_client()
        except Exception:
            db = None
        _resolver = IdentityResolver(db)
    return _resolver


def _name_matches(ftl_name: str, athlete_ftl_name: str) -> bool:
    """
    Backward-compatible shim that delegates to IdentityResolver.

    Uses Strategy 1 (exact word-set) — the same logic as before — so existing
    call sites that pass a bare True/False check are unaffected.  The resolver
    is used where richer matching (fuzzy, alias cache) is needed.
    """
    from core.identity import match_word_set
    return match_word_set(athlete_ftl_name, ftl_name)


def _resolve_name(
    ftl_name: str,
    athlete_ftl_name: str,
    source: str = "ftl",
    context: str = "",
) -> bool:
    """
    Full resolver-backed name match used in placement and pool lookups.

    Returns True if any strategy (alias, word-set, surname, fuzzy) confirms
    the FTL name corresponds to athlete_ftl_name.
    """
    resolver = _get_resolver()
    result   = resolver.find_in_list(
        target     = athlete_ftl_name,
        candidates = [ftl_name],
        source     = source,
        context    = context,
    )
    return result is not None


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
    # Use full resolver for placement lookup — handles fuzzy/alias variants
    resolver   = _get_resolver()
    names      = [e.get("name", "") for e in results]
    match      = resolver.find_in_list(
        target     = name_ftl,
        candidates = names,
        source     = "ftl",
        context    = f"event:{ftl_event_id}",
    )
    if match is not None:
        entry = results[match.index]
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

    # Use resolver for richer matching (fuzzy + alias cache)
    resolver = _get_resolver()
    names    = [e.get("name", "") for e in data]
    match    = resolver.find_in_list(
        target     = name_ftl,
        candidates = names,
        source     = "ftl",
        context    = f"pool_stats:{ftl_event_id}",
    )
    if match is not None:
        entry = data[match.index]
        v  = int(entry.get("v",  0))
        m  = int(entry.get("m",  0))
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


# ── Pool bout collection ───────────────────────────────────────
#
# FTL pool scores API (reverse-engineered via browser network analysis):
#
#   GET /pools/scores/{event_id}/{pool_id_seed}
#       Returns an HTML page with an inline JS array:  var ids = ["GUID1", "GUID2", ...];
#       One GUID per pool in the event.
#
#   GET /pools/scores/{event_id}/{pool_id_seed}/{pool_id}?dbut=true
#       Returns an HTML fragment for ONE specific pool — the full bout matrix.
#       Cell format: "V5" = victory 5 scored, "D3" = defeat 3 scored.
#       Cell index formula (verified): row_i[opp_pos + 1] = fencer i's bout vs opp_pos.
#
# No socket.io, no Playwright. Pure authenticated HTTP.


def _discover_pool_ids(ftl_event_id: str, pool_id_seed: str) -> list[str]:
    """
    Fetch the pool scores landing page and extract all pool IDs from
    the inline JS  var ids = [...]  array.
    """
    url = f"{FTL_BASE}/pools/scores/{ftl_event_id}/{pool_id_seed}"
    soup = _get_html(url)
    if not soup:
        return []
    for script in soup.find_all("script"):
        text = script.string or ""
        if "var ids" in text:
            m = re.search(r"var ids\s*=\s*\[([\s\S]*?)\]", text)
            if m:
                return re.findall(r"[A-F0-9]{32}", m.group(1), re.I)
    return []


def _parse_pool_fragment(soup: BeautifulSoup, pool_number: int) -> Optional[dict]:
    """
    Parse the HTML fragment from /pools/scores/.../pool_id?dbut=true.
    Returns {pool_number, fencers, rows} or None if no valid table found.
    """
    table = next(
        (t for t in soup.find_all("table") if re.search(r'[VD]\d', t.get_text())),
        None,
    )
    if not table:
        return None

    fencers: dict[int, dict] = {}
    row_cells: dict[int, list[str]] = {}

    for row in table.find_all("tr")[1:]:  # skip header
        cells = [td.get_text("\n", strip=True) for td in row.find_all(["td", "th"])]
        if len(cells) < 3:
            continue
        lines = [ln.strip() for ln in cells[0].split("\n") if ln.strip()]
        if not lines:
            continue
        fencer_name = lines[0]
        club, country = "", "GBR"
        if len(lines) > 1:
            club_raw = lines[1]
            if "/" in club_raw:
                cp, ctp = club_raw.rsplit("/", 1)
                club, country = cp.strip(), ctp.strip() or "GBR"
            else:
                club = club_raw.strip()
        try:
            pos = int(cells[1])
        except (ValueError, IndexError):
            continue
        fencers[pos] = {"name": fencer_name, "club": club, "country": country}
        row_cells[pos] = cells

    return {"pool_number": pool_number, "fencers": fencers, "rows": row_cells} if fencers else None


def _extract_bouts_from_pool(pool: dict, name_ftl: str) -> Optional[list[dict]]:
    """
    Find the athlete in a parsed pool and extract their individual bouts.
    Returns a list of bout dicts, or None if the athlete is not in this pool.

    For athlete at position i vs opponent at position j:
      ts  = numeric part of row_i[j+1]   (touches our athlete scored)
      tr  = numeric part of row_j[i+1]   (touches opponent scored = we received)
      result = True if the cell starts with "V"
    """
    fencers, row_cells = pool["fencers"], pool["rows"]
    # Use resolver for richer matching in pool bout extraction
    resolver   = _get_resolver()
    pos_keys   = sorted(fencers.keys())
    fencer_names = [fencers[p]["name"] for p in pos_keys]
    match      = resolver.find_in_list(
        target     = name_ftl,
        candidates = fencer_names,
        source     = "ftl_pool",
        context    = f"pool:{pool.get('pool_number', '?')}",
    )
    our_pos = pos_keys[match.index] if match is not None else None
    if our_pos is None:
        return None

    our_row = row_cells[our_pos]
    bouts, bout_order = [], 0

    for opp_pos in sorted(fencers):
        if opp_pos == our_pos:
            continue
        bout_order += 1
        cell_idx = opp_pos + 1
        if cell_idx >= len(our_row):
            continue
        m = _BOUT_CELL.match(our_row[cell_idx].strip())
        if not m:
            continue
        ts, result = int(m.group(2)), m.group(1) == "V"
        tr = 0
        opp_row = row_cells.get(opp_pos, [])
        m2 = _BOUT_CELL.match(opp_row[our_pos + 1].strip()) if our_pos + 1 < len(opp_row) else None
        if m2:
            tr = int(m2.group(2))
        opp = fencers[opp_pos]
        bouts.append({
            "pool_number":      pool["pool_number"],
            "bout_order":       bout_order,
            "opponent_name":    opp["name"],
            "opponent_club":    opp["club"],
            "opponent_country": opp["country"] or "GBR",
            "ts": ts, "tr": tr, "result": result,
        })
    return bouts


def collect_pool_bouts_for_event(
    db,
    event_db_id: str,
    ftl_event_id: str,
    pool_id_seed: str,
    name_ftl: str,
) -> dict:
    """
    Collect individual pool bouts for one event and write them to Supabase.

    1. Skip if bouts already exist (idempotent on re-runs)
    2. Fetch landing page → extract all pool IDs from var ids = [...]
    3. Fetch each pool fragment (?dbut=true) until the athlete is found
    4. Parse bout matrix → write to pool_bouts table
    """
    existing = db.table("pool_bouts").select("id").eq("event_id", event_db_id).limit(1).execute()
    if existing.data:
        return {"inserted": 0, "skipped": True, "error": None}

    pool_ids = _discover_pool_ids(ftl_event_id, pool_id_seed)
    if not pool_ids:
        return {"inserted": 0, "skipped": False, "error": "No pool IDs found in page JS"}

    surname = name_ftl.split()[0].upper()

    for pool_num, pool_id in enumerate(pool_ids, 1):
        url = f"{FTL_BASE}/pools/scores/{ftl_event_id}/{pool_id_seed}/{pool_id}?dbut=true"
        soup = _get_html(url)
        if not soup or surname not in soup.get_text().upper():
            continue
        pool = _parse_pool_fragment(soup, pool_num)
        if not pool:
            continue
        bouts = _extract_bouts_from_pool(pool, name_ftl)
        if bouts is None:
            continue
        rows = [{**b, "event_id": event_db_id} for b in bouts]
        try:
            db.table("pool_bouts").insert(rows).execute()
        except Exception as exc:
            logger.error(
                f"  pool_bouts insert failed for event {event_db_id} "
                f"(pool {pool_num}): {exc}"
            )
            return {"inserted": 0, "skipped": False, "error": str(exc)}
        return {"inserted": len(bouts), "skipped": False, "error": None}

    return {"inserted": 0, "skipped": False, "error": f"'{name_ftl}' not found in any of {len(pool_ids)} pools"}


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
        # Still stamp last_refreshed so the dashboard shows an accurate
        # "Data last updated" time even when there is nothing to collect yet.
        db.table("athletes").update(
            {"last_refreshed": datetime.now(timezone.utc).isoformat()}
        ).eq("id", athlete_id).execute()
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

            # 4. Write updates to Supabase (before pool bouts so event_db_id is valid)
            if update:
                db.table("events").update(update).eq("id", event_db_id).execute()
                summary["events_updated"] += 1
            else:
                summary["events_skipped"] += 1

            # 5. Collect individual pool bouts (idempotent — skips if already present)
            if pool_id_seed:
                bout_result = collect_pool_bouts_for_event(
                    db, event_db_id, ftl_event_id, pool_id_seed, name_ftl
                )
                if bout_result["error"]:
                    logger.warning(f"  {event_name}: pool bouts — {bout_result['error']}")
                elif not bout_result["skipped"]:
                    logger.info(f"  {event_name}: pool bouts inserted={bout_result['inserted']}")

            # 5b. Aggregate-stat fallback: if the FTL pools/results/data endpoint
            #     returned nothing (pool_stats is None) but we have bout rows for
            #     this event — either just inserted or previously stored — compute
            #     pool_v/l/ts/tr/ind from those bouts and write them now.
            #     This handles events where FTL publishes bout scores but not the
            #     aggregate summary (observed on some LPJS club-circuit events).
            if pool_id_seed and not pool_stats:
                stored_bouts = (
                    db.table("pool_bouts")
                      .select("ts, tr, result")
                      .eq("event_id", event_db_id)
                      .execute()
                      .data or []
                )
                if stored_bouts:
                    v  = sum(1 for b in stored_bouts if b["result"])
                    l  = len(stored_bouts) - v
                    ts = sum(b["ts"] for b in stored_bouts)
                    tr = sum(b["tr"] for b in stored_bouts)
                    agg = {
                        "pool_v":   v,
                        "pool_l":   l,
                        "pool_ts":  ts,
                        "pool_tr":  tr,
                        "pool_ind": ts - tr,
                    }
                    db.table("events").update(agg).eq("id", event_db_id).execute()
                    logger.info(
                        f"  {event_name}: pool stats computed from bouts — "
                        f"V{v}/L{l} TS{ts}-TR{tr} Ind{ts - tr:+d}"
                    )
                    summary["events_updated"] += 1

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


# ── FTL-first weekend event discovery ─────────────────────────
#
# UK Ratings publishes competition history with a 3-7 day lag, so
# weekend events will not appear until mid-week.  FTL has results
# live during the competition itself.  This function scans FTL for
# recent UK tournaments, checks whether any roster athletes competed,
# and creates / links the event rows in the DB — so the dashboard
# shows the result the same weekend it happened.
#
# Called automatically at the start of every weekend refresh.
# Can also be run manually:
#   python -c "from collectors.ftl_collector import discover_recent_ftl_events; discover_recent_ftl_events()"

def discover_recent_ftl_events(days_back: int = 7, dry_run: bool = False) -> dict:
    """
    Scan FTL for GBR tournaments held in the last `days_back` days.
    For each event, check if any active roster athlete competed and, if so,
    create or link the event row in the DB with ftl_event_id set.

    Returns {"tournaments_scanned", "events_linked", "errors"}.
    """
    import re as _re
    from datetime import date as _Date, timedelta as _td

    today     = _Date.today()
    from_date = (today - _td(days=days_back)).isoformat()
    to_date   = today.isoformat()

    logger.info(
        f"FTL recent discovery: GBR tournaments {from_date} → {to_date}"
        + (" [DRY-RUN]" if dry_run else "")
    )

    db = get_write_client()

    # ── Load active roster ─────────────────────────────────────────
    roster = db.table("athletes")\
        .select("id, name_display, name_ftl, weapon")\
        .eq("active", True)\
        .not_.is_("name_ftl", "null")\
        .execute().data or []

    if not roster:
        logger.warning("  No active athletes with name_ftl — skipping discovery")
        return {"tournaments_scanned": 0, "events_linked": 0, "errors": []}

    # Surname → [athlete, ...] for O(1) first-pass filtering
    by_surname: dict[str, list] = {}
    for a in roster:
        sur = a["name_ftl"].split()[0].upper()
        by_surname.setdefault(sur, []).append(a)

    summary: dict = {"tournaments_scanned": 0, "events_linked": 0, "errors": []}

    # ── Search FTL for recent UK tournaments ───────────────────────
    search_url = (
        f"{FTL_BASE}/tournaments/search/data/advanced"
        f"?from={from_date}&to={to_date}&country=GBR"
    )
    tournaments = _get_json(search_url)
    if not isinstance(tournaments, list):
        logger.warning("  FTL tournament search returned no data — skipping")
        return summary

    logger.info(f"  {len(tournaments)} GBR tournament(s) in window")
    summary["tournaments_scanned"] = len(tournaments)

    # ── Cache existing DB tournaments keyed by ftl_tournament_id ──
    # Supabase PostgREST defaults to a 1 000-row page limit; explicitly
    # requesting 10 000 rows ensures the full table is loaded regardless of
    # how many tournaments accumulate over time.
    existing_t: dict[str, str] = {
        row["ftl_tournament_id"]: row["id"]
        for row in (
            db.table("tournaments")
              .select("id, ftl_tournament_id")
              .not_.is_("ftl_tournament_id", "null")
              .limit(10000)
              .execute().data or []
        )
    }

    for t in tournaments:
        ftl_tid = t.get("id", "")
        t_name  = t.get("name", "")
        t_start = (t.get("start") or "")[:10]

        if not ftl_tid or not t_name:
            continue

        # ── Fetch event schedule ───────────────────────────────────
        sched = _get_html(f"{FTL_BASE}/tournaments/eventSchedule/{ftl_tid}")
        if not sched:
            continue

        ftl_events = []
        for tr in sched.find_all("tr", id=_re.compile(r"^ev_", _re.I)):
            eid   = tr["id"][3:].upper()
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            ename = cells[1] if len(cells) >= 2 else ""
            if eid and ename:
                ftl_events.append({"ftl_event_id": eid, "name": ename})

        if not ftl_events:
            continue

        # ── For each event, check participant list ─────────────────
        for fe in ftl_events:
            ftl_eid = fe["ftl_event_id"]
            fe_name = fe["name"]

            participants = _get_json(
                f"{FTL_BASE}/events/results/data/{ftl_eid}"
            )
            if not isinstance(participants, list) or not participants:
                continue

            # Match participants against roster
            matched: list[dict] = []
            seen_ids: set[str] = set()
            for p in participants:
                p_name = (p.get("name") or "").strip()
                if not p_name:
                    continue
                sur = p_name.split()[0].upper()
                for athlete in by_surname.get(sur, []):
                    if (athlete["id"] not in seen_ids
                            and _resolve_name(p_name, athlete["name_ftl"], source="ftl", context="roster_match")):
                        matched.append(athlete)
                        seen_ids.add(athlete["id"])

            if not matched:
                continue

            names = ", ".join(a["name_display"] for a in matched)
            logger.info(
                f"  ✓ '{t_name}' / '{fe_name}' — "
                f"{len(matched)} athlete(s): {names}"
            )

            if dry_run:
                continue

            # ── Ensure tournament row exists ───────────────────────
            db_tid = existing_t.get(ftl_tid)
            if not db_tid:
                try:
                    res = db.table("tournaments").insert({
                        "name":              t_name,
                        "ftl_tournament_id": ftl_tid,
                        "date_start":        t_start or None,
                        "country":           "GBR",
                    }).execute()
                    if not res.data:
                        raise RuntimeError(
                            f"Tournament insert for '{t_name}' returned no data — "
                            "possible RLS policy or unique-constraint violation"
                        )
                    db_tid = res.data[0]["id"]
                    existing_t[ftl_tid] = db_tid
                    logger.info(f"    Created tournament '{t_name}' → {db_tid[:8]}…")
                except Exception as exc:
                    # 23505 = unique constraint violation.
                    # Fallback strategy (two attempts):
                    #   1. Look up by ftl_tournament_id (covers concurrent inserts)
                    #   2. Look up by (name, date_start) — covers the case where
                    #      the row was manually inserted with a different ftl_tournament_id
                    #      than what FTL's search API returns.  If found, patch the
                    #      ftl_tournament_id so future runs hit the cache correctly.
                    exc_str = str(exc)
                    if "23505" in exc_str or "duplicate" in exc_str.lower():
                        found_via_fallback = False
                        # Attempt 1: look up by ftl_tournament_id
                        try:
                            existing = db.table("tournaments")\
                                .select("id")\
                                .eq("ftl_tournament_id", ftl_tid)\
                                .execute().data
                            if existing:
                                db_tid = existing[0]["id"]
                                existing_t[ftl_tid] = db_tid
                                logger.info(
                                    f"    Tournament '{t_name}' already exists "
                                    f"(ftl_id match) → reusing {db_tid[:8]}…"
                                )
                                found_via_fallback = True
                        except Exception:
                            pass

                        # Attempt 2: look up by (name, date_start) — handles
                        # mismatched ftl_tournament_id from manual inserts
                        if not found_via_fallback and t_start:
                            try:
                                existing = db.table("tournaments")\
                                    .select("id")\
                                    .eq("name", t_name)\
                                    .eq("date_start", t_start)\
                                    .execute().data
                                if existing:
                                    db_tid = existing[0]["id"]
                                    existing_t[ftl_tid] = db_tid
                                    # Patch the stored ftl_tournament_id so the cache
                                    # hits on future runs
                                    db.table("tournaments").update(
                                        {"ftl_tournament_id": ftl_tid}
                                    ).eq("id", db_tid).execute()
                                    logger.info(
                                        f"    Tournament '{t_name}' found by (name, date_start) "
                                        f"→ reusing {db_tid[:8]}… and patching ftl_tournament_id"
                                    )
                                    found_via_fallback = True
                            except Exception as lookup_exc:
                                logger.error(
                                    f"    (name, date_start) fallback failed for '{t_name}': {lookup_exc}"
                                )

                        if not found_via_fallback:
                            logger.error(
                                f"    Duplicate error — cannot find '{t_name}' by ftl_id or (name, date_start)"
                            )
                            summary["errors"].append(exc_str)
                            continue
                    else:
                        logger.error(f"    Failed to create tournament '{t_name}': {exc}")
                        summary["errors"].append(exc_str)
                        continue

            # ── Ensure event row per matched athlete ───────────────
            for athlete in matched:
                aid = athlete["id"]
                try:
                    existing_ev = db.table("events")\
                        .select("id, ftl_event_id, date")\
                        .eq("athlete_id", aid)\
                        .eq("tournament_id", db_tid)\
                        .execute().data

                    if existing_ev:
                        ev = existing_ev[0]
                        stored_eid = ev.get("ftl_event_id")
                        if stored_eid and stored_eid == ftl_eid:
                            # Already linked with the correct canonical ID.
                            # Backfill date if it is currently null and we have t_start.
                            if t_start and not ev.get("date"):
                                db.table("events").update(
                                    {"date": t_start}
                                ).eq("id", ev["id"]).execute()
                                logger.info(
                                    f"    {athlete['name_display']}: "
                                    f"backfilled date={t_start} on already-linked event"
                                )
                            else:
                                logger.debug(
                                    f"    {athlete['name_display']}: "
                                    f"event already linked — skip"
                                )
                        elif stored_eid and stored_eid != ftl_eid:
                            # Stored ID differs from the FTL canonical ID — correct it
                            db.table("events").update({
                                "ftl_event_id": ftl_eid,
                                "date":         t_start or None,
                            }).eq("id", ev["id"]).execute()
                            logger.info(
                                f"    {athlete['name_display']}: "
                                f"corrected ftl_event_id "
                                f"({stored_eid[:8]}… → {ftl_eid[:8]}…)"
                            )
                            summary["events_linked"] += 1
                        else:
                            # Link FTL ID onto existing UK-Ratings-created row
                            db.table("events").update({
                                "ftl_event_id": ftl_eid,
                                "date":         t_start or None,
                            }).eq("id", ev["id"]).execute()
                            logger.info(
                                f"    {athlete['name_display']}: "
                                f"linked ftl_event_id on existing row"
                            )
                            summary["events_linked"] += 1
                    else:
                        # Create a brand-new event row from FTL data
                        db.table("events").insert({
                            "athlete_id":    aid,
                            "tournament_id": db_tid,
                            "event_name":    fe_name,
                            "ftl_event_id":  ftl_eid,
                            "date":          t_start or None,
                        }).execute()
                        logger.info(
                            f"    {athlete['name_display']}: "
                            f"created event '{fe_name}'"
                        )
                        summary["events_linked"] += 1

                except Exception as exc:
                    logger.error(
                        f"    Error linking event for "
                        f"{athlete['name_display']}: {exc}"
                    )
                    summary["errors"].append(str(exc))

    logger.info(
        f"FTL discovery complete — "
        f"tournaments_scanned={summary['tournaments_scanned']}, "
        f"events_linked={summary['events_linked']}, "
        f"errors={len(summary['errors'])}"
    )
    return summary
