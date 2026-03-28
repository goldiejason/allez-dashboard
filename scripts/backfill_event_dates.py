"""
Backfill event dates from UK Ratings tournament detail pages.

UK Ratings competition history doesn't include dates, but each tournament's
detail page shows them in DD.MM.YYYY format.  This script:

  1. Loads all events that have uk_ratings_tourney_id set but no date
  2. Deduplicates — fetches each tournament page only once
  3. Extracts the start date from the page
  4. Stamps it on every event that belongs to that tournament

Run from the project root:
    python scripts/backfill_event_dates.py

Safe to re-run — only events with date IS NULL are touched.
"""

import os
import re
import sys
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from bs4 import BeautifulSoup
from collectors.ukratings_collector import _get_session, UK_BASE, REQUEST_DELAY, REQUEST_TIMEOUT
from database.client import get_write_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")  # DD.MM.YYYY


def _parse_date(text: str) -> str | None:
    """
    Extract first date from text and return as YYYY-MM-DD, or None.

    UK Ratings uses DD.MM.YYYY but some pages appear to have MM.DD.YYYY.
    If the parsed 'month' value exceeds 12 we swap day and month.
    """
    m = DATE_RE.search(text)
    if not m:
        return None
    d, mo, year = int(m.group(1)), int(m.group(2)), m.group(3)
    if mo > 12:
        # Likely MM.DD.YYYY — swap
        d, mo = mo, d
    if mo > 12 or d > 31:
        return None
    return f"{year}-{mo:02d}-{d:02d}"


def fetch_tourney_date(tourney_id: int) -> str | None:
    """Fetch a tournament detail page and return the start date as YYYY-MM-DD."""
    url = f"{UK_BASE}/tourneys/tourneydetail/{tourney_id}"
    session = _get_session()
    if session is None:
        logger.error("No authenticated session — cannot fetch tournament dates")
        return None
    try:
        time.sleep(REQUEST_DELAY)
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # The date sits in <h5 class="navbar-brand text-uppercase">
        # e.g. "03.09.2024 - 03.09.2024"
        # Targeting this element avoids false matches from JWT tokens in table cells.
        h5 = soup.find("h5", class_="navbar-brand")
        text = h5.get_text(" ", strip=True) if h5 else ""
        logger.info(f"  tourney {tourney_id} h5 text: {text!r}")
        date = _parse_date(text)

        if date:
            logger.info(f"  tourney {tourney_id} → {date}")
        else:
            logger.warning(f"  tourney {tourney_id} → no date found (h5 text: {text!r})")
        return date
    except Exception as e:
        logger.error(f"  tourney {tourney_id} fetch failed: {e}")
        return None


def main():
    db = get_write_client()

    # Load all events missing a date but with a uk_ratings_tourney_id
    res = db.table("events")\
        .select("id, uk_ratings_tourney_id, event_name")\
        .is_("date", "null")\
        .not_.is_("uk_ratings_tourney_id", "null")\
        .execute()

    events = res.data or []
    if not events:
        logger.info("No events with missing dates found — nothing to do")
        return

    logger.info(f"Found {len(events)} events missing dates")

    # Deduplicate — group event IDs by tourney ID
    tourney_to_events: dict[int, list[str]] = {}
    for ev in events:
        tid = ev["uk_ratings_tourney_id"]
        tourney_to_events.setdefault(tid, []).append(ev["id"])

    logger.info(f"Fetching dates for {len(tourney_to_events)} unique tournaments")

    updated, failed, skipped = 0, 0, 0

    for tourney_id, event_ids in tourney_to_events.items():
        date = fetch_tourney_date(tourney_id)
        if not date:
            logger.warning(f"  Skipping {len(event_ids)} event(s) for tourney {tourney_id} — no date")
            failed += len(event_ids)
            continue

        # Stamp date on all events for this tournament
        db.table("events")\
            .update({"date": date})\
            .in_("id", event_ids)\
            .execute()

        updated += len(event_ids)
        logger.info(f"  tourney {tourney_id} → {date}  ({len(event_ids)} event(s) updated)")

    logger.info(
        f"\nBackfill complete — "
        f"updated={updated}, failed={failed}, skipped={skipped}"
    )


if __name__ == "__main__":
    main()
