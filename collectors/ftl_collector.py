"""
FTL Collector — FencingTimeLive data collection.

For a given athlete (identified by their FTL fencer ID), this module:
  1. Fetches their complete event history from their FTL profile page
  2. For each event, fetches pool bouts (ts, tr, opponent, result)
  3. For each event, fetches DE bouts from the tableau
  4. Writes everything to Supabase

FTL URL patterns:
  Fencer profile:  https://www.fencingtimelive.com/fencers/results/{FENCER_ID}
  Event pools:     https://www.fencingtimelive.com/pools/scores/{TOURNAMENT_ID}/{POOL_ID}
  DE tableau:      https://www.fencingtimelive.com/tableaux/results/{TOURNAMENT_ID}/{EVENT_ID}
"""

import re
import time
import logging
from datetime import date
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
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}


# ── HTTP helpers ───────────────────────────────────────────────

def _get(url: str) -> Optional[BeautifulSoup]:
    """Fetch a URL and return parsed HTML, or None on failure."""
    try:
        time.sleep(REQUEST_DELAY)
        r = httpx.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        r.raise_for_status()
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


# ── Name matching ──────────────────────────────────────────────

def _name_matches(ftl_row_name: str, athlete_ftl_name: str) -> bool:
    """
    Check whether the name in an FTL table row matches the athlete's canonical FTL name.

    FTL stores names in various formats:
      "PANGA Daniel", "PANGA Daniel J", "Daniel PANGA"

    We store the canonical FTL name (e.g. "PANGA Daniel") in athletes.name_ftl.
    We do a normalised word-set comparison so minor variations still match.
    """
    row_words  = set(ftl_row_name.upper().split())
    target_words = set(athlete_ftl_name.upper().split())
    # All words in the stored name must appear in the row name
    return target_words.issubset(row_words)


# ── Pool bout extraction ───────────────────────────────────────

def extract_pool_bouts(tournament_id: str, pool_id: str, athlete_ftl_name: str) -> list[dict]:
    """
    Fetch a single pool score sheet from FTL and extract this athlete's bouts.

    Returns a list of dicts:
      {opponent_name, opponent_club, opponent_country, ts, tr, result, bout_order}
    """
    url = f"{FTL_BASE}/pools/scores/{tournament_id}/{pool_id}"
    soup = _get(url)
    if not soup:
        return []

    bouts = []
    # FTL pool tables: rows are fencers, columns include opponent scores
    # Each score cell contains "V5" (victory, 5 touches) or "D3" (defeat, 3 touches)
    table = soup.find("table", class_=re.compile(r"pool", re.I))
    if not table:
        logger.warning(f"No pool table found at {url}")
        return []

    rows = table.find_all("tr")
    fencer_names = []

    # First pass: collect all fencer names from the row headers
    for row in rows:
        header = row.find("th") or (row.find_all("td")[0] if row.find_all("td") else None)
        if header:
            name_text = header.get_text(separator=" ", strip=True)
            fencer_names.append(name_text)

    # Find our athlete's row
    our_row_idx = None
    for i, name in enumerate(fencer_names):
        if _name_matches(name, athlete_ftl_name):
            our_row_idx = i
            break

    if our_row_idx is None:
        logger.warning(f"Could not find '{athlete_ftl_name}' in pool at {url}")
        return []

    our_row = rows[our_row_idx]
    cells = our_row.find_all("td")

    bout_order = 0
    for j, cell in enumerate(cells):
        text = cell.get_text(strip=True)
        # Cells contain "V5", "D3", "V" (victory by withdrawal), "D" etc.
        if j == our_row_idx:
            continue  # diagonal (self)
        match = re.match(r"^([VD])(\d*)$", text, re.I)
        if not match:
            continue
        won = match.group(1).upper() == "V"
        score_str = match.group(2)
        ts = int(score_str) if score_str else (5 if won else 0)
        # Opponent score is in the transposed cell (their row, our column)
        opponent_name = fencer_names[j] if j < len(fencer_names) else "Unknown"
        # Get opponent's score in this bout (their row, column = our_row_idx)
        opp_row = rows[j]
        opp_cells = opp_row.find_all("td")
        tr = 0
        if our_row_idx < len(opp_cells):
            opp_text = opp_cells[our_row_idx].get_text(strip=True)
            opp_match = re.match(r"^[VD](\d*)$", opp_text, re.I)
            if opp_match and opp_match.group(1):
                tr = int(opp_match.group(1))

        bout_order += 1
        bouts.append({
            "opponent_name":    opponent_name.strip(),
            "opponent_club":    None,
            "opponent_country": "GBR",
            "ts":               ts,
            "tr":               tr,
            "result":           won,
            "bout_order":       bout_order,
        })

    return bouts


# ── DE bout extraction ─────────────────────────────────────────

def extract_de_bouts(tournament_id: str, event_id: str, athlete_ftl_name: str) -> list[dict]:
    """
    Fetch the DE tableau for an event and extract this athlete's DE bouts.

    Returns a list of dicts:
      {round, opponent_name, opponent_country, ts, tr, result}
    """
    url = f"{FTL_BASE}/tableaux/results/{tournament_id}/{event_id}"
    soup = _get(url)
    if not soup:
        return []

    bouts = []
    # FTL tableau: find all bout rows containing this athlete's name
    bout_rows = soup.find_all("tr", class_=re.compile(r"bout", re.I))

    round_labels = ["T256", "T128", "T64", "T32", "T16", "QF", "SF", "Bronze", "F"]

    for row in bout_rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        names_in_row = [c.get_text(strip=True) for c in cells]
        our_name_idx = None
        for idx, name in enumerate(names_in_row):
            if _name_matches(name, athlete_ftl_name):
                our_name_idx = idx
                break
        if our_name_idx is None:
            continue

        # Determine which fencer is which
        opp_idx = 1 if our_name_idx == 0 else 0
        opp_name = names_in_row[opp_idx] if opp_idx < len(names_in_row) else "Unknown"

        # Scores are typically in the middle cells
        score_cells = [c for c in cells if re.match(r"^\d+$", c.get_text(strip=True))]
        ts = int(score_cells[our_name_idx].get_text(strip=True)) if len(score_cells) > our_name_idx else 0
        tr = int(score_cells[opp_idx].get_text(strip=True)) if len(score_cells) > opp_idx else 0

        # Round label — try to extract from row class or nearby header
        round_label = "Unknown"
        row_class = " ".join(row.get("class", []))
        for label in round_labels:
            if label.lower() in row_class.lower():
                round_label = label
                break

        bouts.append({
            "round":            round_label,
            "opponent_name":    opp_name.strip(),
            "opponent_club":    None,
            "opponent_country": "GBR",
            "ts":               ts,
            "tr":               tr,
            "result":           ts > tr,
        })

    return bouts


# ── Main collection entry point ────────────────────────────────

def collect_athlete(athlete_id: str, ftl_fencer_id: str, name_ftl: str, force: bool = False) -> dict:
    """
    Full collection run for one athlete.

    1. Fetches their FTL fencer profile (all events)
    2. For each event: fetches pool bouts + DE bouts
    3. Upserts everything into Supabase

    Returns a summary dict with counts.
    """
    db = get_write_client()
    summary = {"events": 0, "pool_bouts": 0, "de_bouts": 0, "errors": []}

    profile_url = f"{FTL_BASE}/fencers/results/{ftl_fencer_id}"
    logger.info(f"Fetching FTL profile for {name_ftl}: {profile_url}")
    soup = _get(profile_url)
    if not soup:
        summary["errors"].append(f"Could not fetch FTL profile: {profile_url}")
        return summary

    # Parse event list from profile page
    # FTL profile lists events in a table: date | tournament | event | place
    event_rows = soup.select("table tr")[1:]  # skip header

    for row in event_rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        try:
            date_text       = cells[0].get_text(strip=True)
            tournament_name = cells[1].get_text(strip=True)
            event_name      = cells[2].get_text(strip=True)
            placement_text  = cells[3].get_text(strip=True)

            # Parse date (FTL format varies: "Jan 27, 2024" or "2024-01-27")
            event_date = _parse_date(date_text)
            placement  = _parse_placement(placement_text)

            # Extract tournament and event IDs from links
            links = row.find_all("a", href=True)
            ftl_tournament_id, ftl_event_id = None, None
            for link in links:
                href = link["href"]
                t_match = re.search(r"/tableaux/results/([A-F0-9]+)/([A-F0-9]+)", href, re.I)
                if t_match:
                    ftl_tournament_id = t_match.group(1)
                    ftl_event_id      = t_match.group(2)

            # Upsert tournament
            tournament_db_id = None
            if ftl_tournament_id:
                t_res = db.table("tournaments").upsert(
                    {"name": tournament_name, "ftl_tournament_id": ftl_tournament_id},
                    on_conflict="ftl_tournament_id"
                ).execute()
                if t_res.data:
                    tournament_db_id = t_res.data[0]["id"]

            # Upsert event
            e_res = db.table("events").upsert({
                "athlete_id":     athlete_id,
                "tournament_id":  tournament_db_id,
                "event_name":     event_name,
                "date":           str(event_date) if event_date else None,
                "placement":      placement,
                "ftl_event_id":   ftl_event_id,
            }, on_conflict="athlete_id,tournament_id,event_name").execute()

            if not e_res.data:
                continue

            event_db_id = e_res.data[0]["id"]
            summary["events"] += 1

            # Collect pool bouts (need pool IDs — found on event detail page)
            if ftl_tournament_id and ftl_event_id:
                pool_ids = _discover_pool_ids(ftl_tournament_id, ftl_event_id)
                for pool_id in pool_ids:
                    bouts = extract_pool_bouts(ftl_tournament_id, pool_id, name_ftl)
                    for bout in bouts:
                        bout["event_id"] = event_db_id
                    if bouts:
                        db.table("pool_bouts").upsert(bouts).execute()
                        summary["pool_bouts"] += len(bouts)

                # Collect DE bouts
                de_bouts = extract_de_bouts(ftl_tournament_id, ftl_event_id, name_ftl)
                for bout in de_bouts:
                    bout["event_id"] = event_db_id
                if de_bouts:
                    db.table("de_bouts").upsert(de_bouts).execute()
                    summary["de_bouts"] += len(de_bouts)

        except Exception as e:
            logger.error(f"Error processing event row: {e}")
            summary["errors"].append(str(e))

    # Update last_refreshed timestamp
    db.table("athletes").update(
        {"last_refreshed": "now()"}
    ).eq("id", athlete_id).execute()

    return summary


# ── Helpers ────────────────────────────────────────────────────

def _parse_date(text: str):
    """Try to parse various FTL date formats into a Python date."""
    import datetime
    formats = ["%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%d %b %Y", "%d/%m/%Y"]
    clean = re.sub(r"\s+", " ", text.strip())
    for fmt in formats:
        try:
            return datetime.datetime.strptime(clean, fmt).date()
        except ValueError:
            pass
    return None


def _parse_placement(text: str) -> Optional[int]:
    """Extract integer placement from text like '3rd', '12th', '1st (tied)'."""
    m = re.search(r"\d+", text)
    return int(m.group()) if m else None


def _discover_pool_ids(tournament_id: str, event_id: str) -> list[str]:
    """
    Fetch the event detail page to find all pool IDs for this event.
    Returns list of pool ID strings.
    """
    # FTL event page lists pools with links to each pool score sheet
    url = f"{FTL_BASE}/events/results/{tournament_id}/{event_id}"
    soup = _get(url)
    if not soup:
        return []

    pool_ids = []
    for link in soup.find_all("a", href=True):
        m = re.search(r"/pools/scores/[A-F0-9]+/([A-F0-9]+)", link["href"], re.I)
        if m:
            pool_ids.append(m.group(1))

    return list(dict.fromkeys(pool_ids))  # deduplicate, preserve order
