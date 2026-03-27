"""
UK Ratings Collector — annual pool W/L totals only.

This is intentionally narrow: we only take annual W/L totals from UK Ratings
because that is the only reliable data it provides that FTL does not.
Everything else (event dates, pool bouts, DE bouts) comes from FTL.

UK Ratings athlete page: https://www.ukratings.co.uk/tourneys/athleteex/{WEAPON_ID}/{UK_ID}/None
  Weapon IDs: Foil=34, Epee=35, Sabre=36
"""

import re
import time
import logging
import httpx
from bs4 import BeautifulSoup

from database.client import get_write_client

logger = logging.getLogger(__name__)

UK_BASE         = "https://www.ukratings.co.uk"
WEAPON_IDS      = {"foil": 34, "epee": 35, "sabre": 36}
REQUEST_DELAY   = 1.5
REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


def fetch_annual_stats(athlete_id: str, uk_ratings_id: int, weapon: str) -> dict:
    """
    Fetch annual pool W/L totals for an athlete from UK Ratings.
    Upserts results into annual_stats table.

    Returns dict: {year: {pool_w, pool_l, de_w, de_l}, ...}
    """
    weapon_id = WEAPON_IDS.get(weapon.lower())
    if not weapon_id:
        logger.error(f"Unknown weapon: {weapon}")
        return {}

    url = f"{UK_BASE}/tourneys/athleteex/{weapon_id}/{uk_ratings_id}/None"
    logger.info(f"Fetching UK Ratings stats: {url}")

    try:
        time.sleep(REQUEST_DELAY)
        r = httpx.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"UK Ratings request failed: {e}")
        return {}

    soup = BeautifulSoup(r.text, "lxml")
    stats = _parse_annual_stats(soup)

    if not stats:
        logger.warning(f"No annual stats found for UK ID {uk_ratings_id}")
        return {}

    # Upsert into Supabase
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
    logger.info(f"Upserted {len(rows)} annual stat rows for athlete {athlete_id}")

    return stats


def _parse_annual_stats(soup: BeautifulSoup) -> dict:
    """
    Parse the UK Ratings athlete page and extract annual pool/DE W/L totals.

    The page has a table with rows per year showing: Year | Pool W | Pool L | DE W | DE L
    (exact column positions vary — we match by header text).
    """
    stats = {}

    # Find all tables
    tables = soup.find_all("table")
    for table in tables:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not any("pool" in h or "win" in h or "w/l" in h for h in headers):
            continue

        # Map header positions
        col_year   = _find_col(headers, ["year", "season"])
        col_pool_w = _find_col(headers, ["pool w", "pool wins", "pw"])
        col_pool_l = _find_col(headers, ["pool l", "pool losses", "pl"])
        col_de_w   = _find_col(headers, ["de w", "de wins", "dw", "direct w"])
        col_de_l   = _find_col(headers, ["de l", "de losses", "dl", "direct l"])

        if col_year is None:
            continue

        for row in table.find_all("tr")[1:]:  # skip header row
            cells = row.find_all("td")
            if not cells:
                continue
            try:
                year_text = cells[col_year].get_text(strip=True) if col_year < len(cells) else ""
                year_m = re.search(r"20\d{2}", year_text)
                if not year_m:
                    continue
                year = int(year_m.group())

                def _int(idx):
                    if idx is None or idx >= len(cells):
                        return 0
                    t = cells[idx].get_text(strip=True)
                    m = re.search(r"\d+", t)
                    return int(m.group()) if m else 0

                stats[year] = {
                    "pool_w": _int(col_pool_w),
                    "pool_l": _int(col_pool_l),
                    "de_w":   _int(col_de_w),
                    "de_l":   _int(col_de_l),
                }
            except (IndexError, ValueError):
                continue

    return stats


def _find_col(headers: list[str], candidates: list[str]) -> int | None:
    """Find the index of a column by matching header text against candidates."""
    for i, h in enumerate(headers):
        for c in candidates:
            if c in h:
                return i
    return None


def collect_all_athletes():
    """Fetch UK Ratings annual stats for every athlete with a uk_ratings_id."""
    db = get_write_client()
    athletes = db.table("athletes").select(
        "id, name_display, uk_ratings_id, weapon"
    ).not_.is_("uk_ratings_id", "null").execute().data

    logger.info(f"Collecting UK Ratings stats for {len(athletes)} athletes")
    for athlete in athletes:
        fetch_annual_stats(
            athlete_id=athlete["id"],
            uk_ratings_id=athlete["uk_ratings_id"],
            weapon=athlete["weapon"],
        )
