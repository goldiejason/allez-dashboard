"""
UK Ratings Collector — event history, DE bouts, and annual stats.

Data source: https://www.ukratings.co.uk/tourneys/athleteex/{weapon_code}/{uk_ratings_id}/None

Tables scraped (located by header content, not by fixed index):
  Competition history  → headers contain "Final Position" + "Event NIF"
  Win/Loss Opponent    → headers contain "Win/Loss" + "Opponent"
  Annual pool/DE W/L   → year-labelled <th> columns + "Pool Victories" row labels

Collection flow per athlete:
  1. collect_athlete_events    → creates/updates tournaments and events rows
  2. collect_athlete_de_bouts  → upserts de_bouts rows (skips BYEs)
  3. collect_annual_stats      → upserts annual_stats rows

UK Ratings round numbers represent tableau size (field remaining at that round):
  64 → T64,  32 → T32,  16 → T16,  8 → QF,  4 → SF,  2 → F,  3 → Bronze
"""

import os
import re
import time
import logging
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

from database.client import get_write_client

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────
UK_BASE       = "https://www.ukratings.co.uk"
WEAPON_CODES  = {"foil": 34, "epee": 35, "sabre": 36}
REQUEST_DELAY = 1.5   # seconds — be polite
REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# UK Ratings numeric round → our de_bouts.round text label
ROUND_MAP: dict[int, str] = {
    64: "T64", 32: "T32", 16: "T16",
    8: "QF", 4: "SF", 3: "Bronze", 2: "F",
}

# Minimum fuzzy ratio (0–1) for tournament name matching
FUZZY_THRESHOLD = 0.72


# ── HTTP / Auth ─────────────────────────────────────────────────────

# Module-level authenticated session — created once, reused across all athletes
_session: Optional[requests.Session] = None


def _login() -> Optional[requests.Session]:
    """
    Log in to UK Ratings using credentials from the environment.

    Flow:
      1. GET /login/   — sets csrftoken cookie + extract csrfmiddlewaretoken from form
      2. POST /login/  — submit useremail + password + csrf token
      3. Verify sessionid cookie is present → logged in
    """
    username = os.getenv("UK_RATINGS_USERNAME", "").strip()
    password = os.getenv("UK_RATINGS_PASSWORD", "").strip()

    if not username or not password:
        logger.error(
            "UK_RATINGS_USERNAME or UK_RATINGS_PASSWORD not set in .env — cannot log in"
        )
        return None

    login_url = f"{UK_BASE}/login/"
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # Step 1: GET login page — sets csrftoken cookie + gives us the form token
        resp = session.get(login_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
        if not csrf_input:
            logger.error(f"csrfmiddlewaretoken not found on login page (url={resp.url})")
            return None
        csrf_token = csrf_input.get("value", "")

        # Step 2: POST credentials
        resp = session.post(
            login_url,
            data={
                "useremail": username,
                "password": password,
                "csrfmiddlewaretoken": csrf_token,
                "next": "/",
            },
            headers={"Referer": login_url},
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        resp.raise_for_status()

        # Step 3: Verify — Django sets sessionid on successful login
        if "sessionid" not in session.cookies:
            logger.error(
                "Login failed — no sessionid cookie set. "
                "Check UK_RATINGS_USERNAME / UK_RATINGS_PASSWORD in .env"
            )
            return None

        logger.info("UK Ratings login successful")
        return session

    except Exception as e:
        logger.error(f"UK Ratings login error: {e}")
        return None


def _get_session() -> Optional[requests.Session]:
    """Return the cached authenticated session, creating it if needed."""
    global _session
    if _session is None:
        _session = _login()
    return _session


def _fetch_athlete_page(uk_ratings_id: int, weapon_code: int) -> Optional[BeautifulSoup]:
    url = f"{UK_BASE}/tourneys/athleteex/{weapon_code}/{uk_ratings_id}/None"
    logger.info(f"Fetching UK Ratings page: {url}")
    try:
        time.sleep(REQUEST_DELAY)
        session = _get_session()
        if session is None:
            logger.error("No authenticated session available — skipping fetch")
            return None
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        # If the site bounced us to logout, the session has expired — re-login once
        if "/logout/" in r.url:
            logger.warning("Session expired — attempting re-login")
            global _session
            _session = None
            session = _get_session()
            if session is None:
                return None
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        logger.error(f"UK Ratings fetch failed for ID {uk_ratings_id}: {e}")
        return None


# ── HTML parsers ───────────────────────────────────────────────────

def _parse_competition_history(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the competition history table — past results with placement data.

    We locate it by content: the correct table has <th> headers containing
    "Event NIF" and "Final Position".  Some athlete pages render an additional
    upcoming-events table (headers: "Start Date", "Event Name", "Difficulty")
    BEFORE the history table, which means the old hardcoded tables[1] index
    picked up the wrong table and stored date strings ("11.04.2026") as
    event_names.  Content-based detection resolves this for all athletes.

    Each <tr onclick="window.location='/tourneys/tourneydetail/{id}'"> row:
      td[0] = tournament name
      td[1] = event name (age category + weapon)
      td[3] = "placement of field_size"
      td[5] = season code ("U26", "U25", ...)

    Returns list of dicts with keys:
      uk_tourney_id, tournament_name, event_name, placement, field_size, season
    """
    comp_table = None
    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        ths_lower = [th.get_text(strip=True).lower() for th in header_row.find_all("th")]
        # The competition history table uniquely contains "final position" and "nif"
        if any("final position" in h for h in ths_lower) and any("nif" in h for h in ths_lower):
            comp_table = table
            break

    if comp_table is None:
        logger.warning("Competition history table not found on UK Ratings page")
        return []

    results = []
    for row in comp_table.find_all("tr", onclick=True):
        onclick = row.get("onclick", "")
        id_match = re.search(r"/tourneys/tourneydetail/(\d+)", onclick)
        if not id_match:
            continue

        uk_tourney_id = int(id_match.group(1))
        cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
        if len(cells) < 4:
            continue

        tournament_name = cells[0].strip()
        event_name      = cells[1].strip()
        placement_str   = cells[3].strip()  # "11 of 19"
        season          = cells[5].strip() if len(cells) > 5 else ""

        placement, field_size = _parse_placement(placement_str)

        results.append({
            "uk_tourney_id":   uk_tourney_id,
            "tournament_name": tournament_name,
            "event_name":      event_name,
            "placement":       placement,
            "field_size":      field_size,
            "season":          season,
        })

    logger.info(f"Parsed {len(results)} competition history rows")
    return results


def _parse_de_bouts(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the Win/Loss Opponent table — DE bouts.

    The table position varies per athlete depending on how many stat
    tables the page renders before it.  We locate it by content:
    looking for a table whose first row has <th> values containing
    "Win/Loss" and "Opponent", with at least one data row.

    Each row has 2 <td> separated internally by <br>:
      Cell 0: result<br>score           e.g. "Lost<br>12 - 4"
      Cell 1: opponent<br>tournament<br>DE Round: N

    Returns list of dicts (BYEs excluded):
      result, ts, tr, opponent_name, tournament_name, round_text
    """
    de_table = None
    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        ths = [th.get_text(strip=True).lower() for th in header_row.find_all("th")]
        if any("win" in h for h in ths) and any("opponent" in h for h in ths):
            # Only use this table if it has actual data rows
            if len(table.find_all("tr")) > 1:
                de_table = table
                break

    if de_table is None:
        logger.info("Win/Loss Opponent table not found on UK Ratings page (athlete may have no DE data)")
        return []

    results = []
    for row in de_table.find_all("tr")[1:]:  # skip header
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        # BeautifulSoup get_text(separator="\n") converts <br> to newlines cleanly
        c0 = [p.strip() for p in cells[0].get_text(separator="\n").split("\n") if p.strip()]
        c1 = [p.strip() for p in cells[1].get_text(separator="\n").split("\n") if p.strip()]

        if not c0 or not c1:
            continue

        result_str = c0[0]  # "Won" or "Lost"
        score_str  = c0[1] if len(c0) > 1 else ""
        opponent   = c1[0] if c1 else ""
        tournament = c1[1] if len(c1) > 1 else ""
        round_str  = c1[2] if len(c1) > 2 else ""

        # Skip BYEs
        if "BYE" in opponent.upper():
            continue

        result = result_str.strip().lower() == "won"

        # Parse score
        ts, tr = None, None
        sm = re.search(r"(\d+)\s*-\s*(\d+)", score_str)
        if sm:
            ts, tr = int(sm.group(1)), int(sm.group(2))

        # Parse round number → text label
        rm = re.search(r"DE Round:\s*(\d+)", round_str, re.I)
        if not rm:
            continue
        round_num  = int(rm.group(1))
        round_text = ROUND_MAP.get(round_num, f"T{round_num}")

        if ts is None or tr is None:
            logger.debug(f"Skipping bout with no score: {opponent} @ {tournament} ({round_text})")
            continue

        results.append({
            "result":          result,
            "ts":              ts,
            "tr":              tr,
            "opponent_name":   opponent,
            "tournament_name": tournament,
            "round_text":      round_text,
        })

    logger.info(f"Parsed {len(results)} scored DE bouts (BYEs excluded)")
    return results


def _parse_annual_stats(soup: BeautifulSoup) -> dict:
    """
    Parse annual pool/DE W/L totals from the UK Ratings athlete page.

    Page structure: stats table has year-labelled <th> columns
    (e.g. "2022", "2023", "2024") and row-type labels in the first <td>
    of each data row ("Pool Victories", "Pool Losses", "DE Victories",
    "DE Losses").  The old header-keyword approach never matched because
    <th> values are years, not descriptive labels.

    Returns dict keyed by year int: {pool_w, pool_l, de_w, de_l}
    """
    YEAR_RE = re.compile(r"^(20\d{2})$")

    for table in soup.find_all("table"):
        # Use only the first <tr> for column headers
        header_row = table.find("tr")
        if not header_row:
            continue
        ths = [th.get_text(strip=True) for th in header_row.find_all("th")]

        # Identify year-indexed columns: th must be exactly "20XX"
        year_cols: dict[int, int] = {}   # year_int → column_index
        for col_idx, th_text in enumerate(ths):
            m = YEAR_RE.match(th_text)
            if m:
                year_cols[int(m.group(1))] = col_idx

        if not year_cols:
            continue

        # Confirm the table contains pool stats by checking first-column td labels
        first_labels = []
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if cells:
                first_labels.append(cells[0].get_text(strip=True))

        if not any(
            lbl in ("Pool Victories", "Pool Losses")
            for lbl in first_labels
        ):
            continue

        # Extract each row's values indexed by year column
        row_data: dict[str, dict[int, int]] = {}
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            label = cells[0].get_text(strip=True)
            if not label:
                continue
            row_data[label] = {}
            for year, col_idx in year_cols.items():
                if col_idx < len(cells):
                    raw = cells[col_idx].get_text(strip=True)
                    m = re.search(r"\d+", raw)
                    row_data[label][year] = int(m.group()) if m else 0
                else:
                    row_data[label][year] = 0

        pool_w_map = row_data.get("Pool Victories", {})
        pool_l_map = row_data.get("Pool Losses", {})
        de_w_map   = row_data.get("DE Victories", {})
        de_l_map   = row_data.get("DE Losses", {})

        stats: dict[int, dict] = {}
        for year in year_cols:
            pool_w = pool_w_map.get(year, 0)
            pool_l = pool_l_map.get(year, 0)
            de_w   = de_w_map.get(year, 0)
            de_l   = de_l_map.get(year, 0)
            # Skip years where all values are zero (e.g. "Prior 2022" with dashes)
            if pool_w or pool_l or de_w or de_l:
                stats[year] = {
                    "pool_w": pool_w,
                    "pool_l": pool_l,
                    "de_w":   de_w,
                    "de_l":   de_l,
                }

        if stats:
            # Return from the first valid table (the "All weapons" summary)
            return stats

    return {}


# ── Name normalization + matching ──────────────────────────────────

def _normalize_tourney_name(name: str) -> str:
    """
    Normalize a tournament name for fuzzy comparison.

    Handles:
    - Stripping category suffixes:  "FCL LPJS November 2024 U10G, U12G..."  → "FCL LPJS November 2024"
    - "Event N - Name" → "Name Event N"
    - Dashes: – → -
    - Year formats: "2024-2025" and "24/25" are treated as similar
    """
    # Strip trailing category codes like "U10G, U12G, U17G, U10B, U12B"
    name = re.sub(r"\s+U\d+[A-Z](?:,\s*U\d+[A-Z])*\s*$", "", name)
    # "Event N - Name" → "Name – Event N"
    m = re.match(r"^Event\s+(\d+)\s*[-–]\s*(.+)$", name.strip())
    if m:
        name = f"{m.group(2).strip()} – Event {m.group(1)}"
    # Normalise dashes
    name = name.replace("–", "-").replace("—", "-")
    # Normalise slashes and year-range formats
    name = re.sub(r"\b(\d{2})/(\d{2})\b", r"\1-\2", name)  # "24/25" → "24-25"
    name = re.sub(r"\b(20\d{2})-(20\d{2})\b", lambda m:
                  f"{m.group(1)[2:]}-{m.group(2)[2:]}", name)  # "2024-2025" → "24-25"
    return name.strip().lower()


def _normalize_event_name(name: str) -> str:
    """Normalise UK Ratings event name to match our DB conventions."""
    name = name.replace("Mens", "Men's").replace("Womens", "Women's")
    name = name.replace("Mixed/Men's", "Men's").replace("Men's/Mixed", "Men's")
    # "U-14 Mens Foil" → "U-14 Men's Foil"
    return name.strip()


def _fuzzy_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _find_col(headers: list[str], candidates: list[str]) -> Optional[int]:
    for i, h in enumerate(headers):
        for c in candidates:
            if c in h:
                return i
    return None


def _parse_placement(text: str) -> tuple[Optional[int], Optional[int]]:
    """Parse "11 of 19" → (11, 19).  Returns (None, None) on failure."""
    m = re.match(r"(\d+)\s+of\s+(\d+)", text.strip())
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


# ── Tournament + event matching / creation ─────────────────────────

def _load_tournaments(db) -> dict[str, dict]:
    """Load all tournaments from DB.  Returns {normalised_name: row_dict}."""
    rows = db.table("tournaments").select("id, name, country, date_start").execute().data or []
    return {_normalize_tourney_name(r["name"]): r for r in rows}


def _match_tournament(ukr_name: str, tourney_map: dict[str, dict]) -> Optional[str]:
    """
    Try to match a UK Ratings tournament name to an existing tournament ID.

    Returns tournament_id (uuid str) or None.

    Strategy:
    1. Exact normalised match (year-compatible only)
    2. Fuzzy match above threshold (year-compatible candidates only)
    3. Substring containment fallback (year-compatible only)

    Year guard: if the UK Ratings tournament name contains a 4-digit year
    (e.g. "LPJS London Foil 2026"), candidates whose date_start year differs
    by more than 1 are excluded.  This prevents historic events from being
    misfiled under a newer same-named tournament and vice-versa.
    """
    norm = _normalize_tourney_name(ukr_name)

    # Extract year hint from the raw UKR name ("LPJS London Foil 2026" → 2026)
    _ym = re.search(r"\b(20\d{2})\b", ukr_name)
    ukr_year: Optional[int] = int(_ym.group(1)) if _ym else None

    def _year_ok(row: dict) -> bool:
        """True if the candidate's date_start year is compatible with ukr_year."""
        if ukr_year is None:
            return True  # no year extractable from UKR name — allow any match
        date_start = row.get("date_start") or ""
        if not date_start:
            return True  # DB row has no date — allow match rather than block
        try:
            db_year = int(str(date_start)[:4])
            return abs(db_year - ukr_year) <= 1
        except (ValueError, TypeError):
            return True

    # 1. Exact normalised match
    if norm in tourney_map:
        row = tourney_map[norm]
        if _year_ok(row):
            return row["id"]
        logger.debug(
            f"  Exact name match for '{ukr_name}' rejected — "
            f"year mismatch (ukr={ukr_year}, db={str(row.get('date_start') or '')[:4]})"
        )

    # 2. Fuzzy match — only among year-compatible candidates
    best_score, best_id = 0.0, None
    for db_norm, row in tourney_map.items():
        if not _year_ok(row):
            continue
        score = _fuzzy_ratio(norm, db_norm)
        if score > best_score:
            best_score, best_id = score, row["id"]

    if best_score >= FUZZY_THRESHOLD:
        logger.debug(f"  Fuzzy match '{ukr_name}' → score={best_score:.2f}")
        return best_id

    # 3. Substring containment (handles abbreviated names) — year-compatible only
    for db_norm, row in tourney_map.items():
        if not _year_ok(row):
            continue
        if norm in db_norm or db_norm in norm:
            logger.debug(f"  Substring match '{ukr_name}' → '{row['name']}'")
            return row["id"]

    return None


def _create_tournament(db, ukr_name: str) -> str:
    """Insert a new tournament row and return its id."""
    # Clean up the name for storage (remove category suffix)
    clean_name = re.sub(r"\s+U\d+[A-Z](?:,\s*U\d+[A-Z])*\s*$", "", ukr_name).strip()

    res = db.table("tournaments").insert({
        "name":           clean_name,
        "country":        "GBR",
        "is_international": False,
    }).execute()
    new_id = res.data[0]["id"]
    logger.info(f"  Created tournament '{clean_name}' → {new_id}")
    return new_id


def _match_or_create_event(
    db,
    athlete_id:     str,
    ukr_tourney_id: int,
    tournament_id:  str,
    event_name_raw: str,
    placement:      Optional[int],
    field_size:     Optional[int],
) -> Optional[str]:
    """
    Find or create an event row.  Returns event_id (uuid str) or None on error.

    Lookup order:
    1. events.uk_ratings_tourney_id = ukr_tourney_id  (fastest — already linked)
    2. (athlete_id, tournament_id, normalised_event_name)  (first-run match)
    3. Create new row if not found
    """
    event_name = _normalize_event_name(event_name_raw)

    # 1. Already linked by uk_ratings_tourney_id
    res = db.table("events")\
        .select("id, placement, field_size")\
        .eq("athlete_id", athlete_id)\
        .eq("uk_ratings_tourney_id", ukr_tourney_id)\
        .execute()

    if res.data:
        existing = res.data[0]
        # Update placement/field_size if UK Ratings has data and DB doesn't
        updates = {}
        if placement and not existing.get("placement"):
            updates["placement"] = placement
        if field_size and not existing.get("field_size"):
            updates["field_size"] = field_size
        if updates:
            db.table("events").update(updates).eq("id", existing["id"]).execute()
        return existing["id"]

    # 2. Match by (athlete_id, tournament_id, event_name)
    res = db.table("events")\
        .select("id")\
        .eq("athlete_id", athlete_id)\
        .eq("tournament_id", tournament_id)\
        .eq("event_name", event_name)\
        .execute()

    if res.data:
        event_id = res.data[0]["id"]
        # Stamp the UK Ratings ID so future runs use path 1
        upd = {"uk_ratings_tourney_id": ukr_tourney_id}
        if placement:
            upd["placement"] = placement
        if field_size:
            upd["field_size"] = field_size
        db.table("events").update(upd).eq("id", event_id).execute()
        logger.debug(f"  Linked existing event {event_id} → uk_ratings_tourney_id={ukr_tourney_id}")
        return event_id

    # 3. Create new event
    try:
        row = {
            "athlete_id":          athlete_id,
            "tournament_id":       tournament_id,
            "event_name":          event_name,
            "uk_ratings_tourney_id": ukr_tourney_id,
        }
        if placement:
            row["placement"] = placement
        if field_size:
            row["field_size"] = field_size

        res = db.table("events").insert(row).execute()
        new_id = res.data[0]["id"]
        logger.info(f"  Created event '{event_name}' (uk_id={ukr_tourney_id}) → {new_id}")
        return new_id
    except Exception as e:
        logger.error(f"  Failed to create event '{event_name}': {e}")
        return None


# ── Main collection functions ──────────────────────────────────────

def collect_athlete_events(
    athlete_id:     str,
    uk_ratings_id:  int,
    weapon_code:    int,
    soup:           Optional[BeautifulSoup] = None,
) -> dict:
    """
    Scrape competition history from UK Ratings and sync to DB.

    For each competition:
    - Match or create a tournament row
    - Match or create an event row (with uk_ratings_tourney_id set)
    - Update placement/field_size if newly discovered

    Returns summary: {events_upserted, events_created, tournaments_created, errors[]}
    """
    summary = {"events_upserted": 0, "events_created": 0, "tournaments_created": 0, "errors": []}

    if soup is None:
        soup = _fetch_athlete_page(uk_ratings_id, weapon_code)
    if soup is None:
        summary["errors"].append("Failed to fetch athlete page")
        return summary

    competitions = _parse_competition_history(soup)
    if not competitions:
        logger.warning(f"No competition history found for UK ID {uk_ratings_id}")
        return summary

    db = get_write_client()
    tourney_map = _load_tournaments(db)  # normalised_name → {id, name}

    for comp in competitions:
        try:
            ukr_id    = comp["uk_tourney_id"]
            ukr_tname = comp["tournament_name"]
            ukr_ename = comp["event_name"]

            # Match or create tournament
            tournament_id = _match_tournament(ukr_tname, tourney_map)
            if tournament_id is None:
                tournament_id = _create_tournament(db, ukr_tname)
                # Re-add to the in-memory map so later comps in same session can find it
                clean = re.sub(r"\s+U\d+[A-Z](?:,\s*U\d+[A-Z])*\s*$", "", ukr_tname).strip()
                tourney_map[_normalize_tourney_name(clean)] = {"id": tournament_id, "name": clean}
                summary["tournaments_created"] += 1

            event_id = _match_or_create_event(
                db,
                athlete_id,
                ukr_id,
                tournament_id,
                ukr_ename,
                comp["placement"],
                comp["field_size"],
            )

            if event_id:
                summary["events_upserted"] += 1
            else:
                summary["errors"].append(f"Could not match/create event: {ukr_tname} / {ukr_ename}")

        except Exception as e:
            logger.error(f"Error processing competition {comp}: {e}")
            summary["errors"].append(str(e))

    logger.info(
        f"Events sync complete: {summary['events_upserted']} upserted, "
        f"{summary['tournaments_created']} tournaments created"
    )
    return summary


def collect_athlete_de_bouts(
    athlete_id:    str,
    uk_ratings_id: int,
    weapon_code:   int,
    soup:          Optional[BeautifulSoup] = None,
) -> dict:
    """
    Scrape DE bouts from UK Ratings Table 10 and upsert into de_bouts.

    Bouts are matched to events via the tournament name appearing in Table 1.
    BYE rows are always skipped (no scores to record).

    Returns summary: {inserted, skipped_no_event, skipped_duplicate, errors[]}
    """
    summary = {"inserted": 0, "skipped_no_event": 0, "skipped_duplicate": 0, "errors": []}

    if soup is None:
        soup = _fetch_athlete_page(uk_ratings_id, weapon_code)
    if soup is None:
        summary["errors"].append("Failed to fetch athlete page")
        return summary

    de_bouts = _parse_de_bouts(soup)
    if not de_bouts:
        logger.info(f"No DE bouts found for UK ID {uk_ratings_id}")
        return summary

    db = get_write_client()

    # Build tournament_name → event_id map for this athlete
    # Use uk_ratings_tourney_id linkage where available, then fall back to tournament name
    events_res = db.table("events")\
        .select("id, uk_ratings_tourney_id")\
        .eq("athlete_id", athlete_id)\
        .not_.is_("uk_ratings_tourney_id", "null")\
        .execute()

    # We also need tournament names — fetch from the competition history
    competition_history = _parse_competition_history(soup)

    # Build: tournament_name (normalised) → event_id
    tourney_to_event: dict[str, str] = {}
    event_id_by_ukr: dict[int, str] = {
        row["uk_ratings_tourney_id"]: row["id"]
        for row in (events_res.data or [])
    }

    for comp in competition_history:
        event_id = event_id_by_ukr.get(comp["uk_tourney_id"])
        if event_id:
            norm = _normalize_tourney_name(comp["tournament_name"])
            tourney_to_event[norm] = event_id

    # Insert DE bouts
    for bout in de_bouts:
        try:
            norm = _normalize_tourney_name(bout["tournament_name"])
            event_id = tourney_to_event.get(norm)

            if event_id is None:
                logger.warning(
                    f"No event found for DE bout: {bout['opponent_name']} "
                    f"@ '{bout['tournament_name']}' ({bout['round_text']})"
                )
                summary["skipped_no_event"] += 1
                continue

            # Attempt insert — unique constraint (event_id, round, opponent_name)
            # prevents duplicates; ON CONFLICT DO NOTHING via upsert count check
            existing = db.table("de_bouts")\
                .select("id")\
                .eq("event_id", event_id)\
                .eq("round", bout["round_text"])\
                .eq("opponent_name", bout["opponent_name"])\
                .execute()

            if existing.data:
                summary["skipped_duplicate"] += 1
                continue

            db.table("de_bouts").insert({
                "event_id":      event_id,
                "round":         bout["round_text"],
                "opponent_name": bout["opponent_name"],
                "ts":            bout["ts"],
                "tr":            bout["tr"],
                "result":        bout["result"],
            }).execute()

            summary["inserted"] += 1
            logger.debug(
                f"  DE bout: {bout['result'] and 'W' or 'L'} "
                f"{bout['ts']}-{bout['tr']} vs {bout['opponent_name']} ({bout['round_text']})"
            )

        except Exception as e:
            logger.error(f"Error inserting DE bout {bout}: {e}")
            summary["errors"].append(str(e))

    logger.info(
        f"DE bouts sync: {summary['inserted']} inserted, "
        f"{summary['skipped_duplicate']} duplicates skipped, "
        f"{summary['skipped_no_event']} no-event skipped"
    )
    return summary


def collect_annual_stats(
    athlete_id:    str,
    uk_ratings_id: int,
    weapon:        str,
    soup:          Optional[BeautifulSoup] = None,
) -> dict:
    """Fetch and upsert annual pool/DE W/L totals."""
    weapon_code = WEAPON_CODES.get(weapon.lower())
    if weapon_code is None:
        logger.error(f"Unknown weapon: {weapon}")
        return {}

    if soup is None:
        soup = _fetch_athlete_page(uk_ratings_id, weapon_code)
    if soup is None:
        return {}

    stats = _parse_annual_stats(soup)
    if not stats:
        logger.warning(f"No annual stats found for UK ID {uk_ratings_id}")
        return {}

    db = get_write_client()
    rows = [
        {
            "athlete_id": athlete_id,
            "year":       year,
            "pool_w":     data.get("pool_w", 0),
            "pool_l":     data.get("pool_l", 0),
            "de_w":       data.get("de_w", 0),
            "de_l":       data.get("de_l", 0),
        }
        for year, data in stats.items()
    ]
    db.table("annual_stats").upsert(rows, on_conflict="athlete_id,year").execute()
    logger.info(f"Upserted {len(rows)} annual stat rows")
    return stats


def collect_athlete(
    athlete_id:    str,
    uk_ratings_id: int,
    weapon:        str,
) -> dict:
    """
    Full UK Ratings collection run for one athlete.

    Fetches the athlete page once, then runs:
      1. Event history sync
      2. DE bout sync
      3. Annual stats sync

    Returns combined summary dict.
    """
    weapon_code = WEAPON_CODES.get(weapon.lower())
    if weapon_code is None:
        return {"error": f"Unknown weapon: {weapon}"}

    logger.info(f"Starting UK Ratings collection for athlete {athlete_id} (UK ID {uk_ratings_id})")

    # Single page fetch reused by all three collectors
    soup = _fetch_athlete_page(uk_ratings_id, weapon_code)
    if soup is None:
        return {"error": "Failed to fetch UK Ratings page"}

    events_summary   = collect_athlete_events(athlete_id, uk_ratings_id, weapon_code, soup=soup)
    de_bouts_summary = collect_athlete_de_bouts(athlete_id, uk_ratings_id, weapon_code, soup=soup)
    annual_summary   = collect_annual_stats(athlete_id, uk_ratings_id, weapon, soup=soup)

    # Update last_refreshed on athlete record
    db = get_write_client()
    db.table("athletes").update({"last_refreshed": datetime.now(timezone.utc).isoformat()}).eq("id", athlete_id).execute()

    return {
        "events":      events_summary,
        "de_bouts":    de_bouts_summary,
        "annual_years": len(annual_summary),
    }


def collect_all_athletes() -> dict:
    """
    Run collect_athlete for every athlete with uk_ratings_id and uk_ratings_weapon_code set.
    """
    db = get_write_client()
    athletes = db.table("athletes")\
        .select("id, name_display, uk_ratings_id, uk_ratings_weapon_code, weapon")\
        .not_.is_("uk_ratings_id", "null")\
        .not_.is_("uk_ratings_weapon_code", "null")\
        .eq("active", True)\
        .execute()

    totals = {"athletes": 0, "events_upserted": 0, "de_bouts_inserted": 0, "errors": []}

    for athlete in (athletes.data or []):
        logger.info(f"Collecting {athlete['name_display']}")
        result = collect_athlete(
            athlete_id=athlete["id"],
            uk_ratings_id=athlete["uk_ratings_id"],
            weapon=athlete["weapon"],
        )
        totals["athletes"] += 1
        totals["events_upserted"]   += result.get("events", {}).get("events_upserted", 0)
        totals["de_bouts_inserted"] += result.get("de_bouts", {}).get("inserted", 0)
        if result.get("error"):
            totals["errors"].append(f"{athlete['name_display']}: {result['error']}")

    logger.info(f"Finished: {totals}")
    return totals
