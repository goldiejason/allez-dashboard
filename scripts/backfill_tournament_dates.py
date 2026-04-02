"""
Backfill null tournament date_start values from FTL, then propagate to events.

Phase 1 — FTL fetch (network required):
  For every tournament where date_start IS NULL and ftl_tournament_id IS NOT NULL,
  query FTL's tournament search API using the tournament name to locate the record
  by ID and retrieve its start date.  Writes date_start back to the tournaments row.

Phase 2 — DB-only propagation (no network):
  For every event where date IS NULL, if the linked tournament now has a known
  date_start, copy tournament.date_start → event.date.
  This covers both the Phase 1 discoveries and tournaments that already had dates
  but whose events were never stamped.

Usage:
  python scripts/backfill_tournament_dates.py            # dry run — prints only
  python scripts/backfill_tournament_dates.py --apply    # write to DB
"""

import os
import sys
import time
import logging
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client
import httpx

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
db = create_client(SUPABASE_URL, SUPABASE_KEY)

FTL_BASE        = "https://www.fencingtimelive.com"
REQUEST_DELAY   = 1.2
REQUEST_TIMEOUT = 20

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── FTL HTTP client ───────────────────────────────────────────────────

_ftl_client: httpx.Client | None = None


def _get_ftl_client() -> httpx.Client:
    global _ftl_client
    if _ftl_client is None:
        client = httpx.Client(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            follow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        _ftl_login(client)
        _ftl_client = client
    return _ftl_client


def _ftl_login(client: httpx.Client) -> None:
    """Attempt FTL login using environment credentials. Silent on failure."""
    username = os.getenv("FTL_USERNAME", "").strip()
    password = os.getenv("FTL_PASSWORD", "").strip()
    if not username or not password:
        logger.warning("FTL_USERNAME/FTL_PASSWORD not set — running unauthenticated")
        return
    try:
        from bs4 import BeautifulSoup
        time.sleep(REQUEST_DELAY)
        resp = client.get(f"{FTL_BASE}/account/login")
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        meta = soup.find("meta", {"name": "csrf_token"})
        if not meta:
            logger.warning("FTL login: CSRF token not found on login page")
            return
        csrf = meta.get("content", "")
        time.sleep(REQUEST_DELAY)
        post = client.post(
            f"{FTL_BASE}/login",
            data={"username": username, "password": password},
            headers={"x-csrf-token": csrf},
            follow_redirects=False,
        )
        redirect = post.text.strip()
        if redirect.startswith("/") or redirect.startswith("http"):
            redir = redirect if redirect.startswith("http") else f"{FTL_BASE}{redirect}"
            time.sleep(REQUEST_DELAY)
            client.get(redir, follow_redirects=True)
            logger.info("FTL login successful")
        else:
            logger.warning(f"FTL login failed — server response: {redirect[:100]}")
    except Exception as exc:
        logger.warning(f"FTL login error: {exc}")


def _ftl_get_start_date(ftl_tid: str, t_name: str) -> str | None:
    """
    Retrieve the start date for an FTL tournament by querying the search API
    and matching by ID.

    Strategy: search by the first two words of the tournament name to reduce
    result set, then match by ID.  Returns YYYY-MM-DD or None.
    """
    client = _get_ftl_client()
    # Use two words for the search to avoid overly broad result sets
    words = t_name.split()[:2]
    tname_q = " ".join(words)
    search_url = (
        f"{FTL_BASE}/tournaments/search/data/advanced"
        f"?tname={tname_q}&country=GBR"
    )
    try:
        time.sleep(REQUEST_DELAY)
        resp = client.get(search_url)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            for item in data:
                if (item.get("id") or "").upper() == ftl_tid.upper():
                    start = (item.get("start") or "")[:10]
                    return start or None
        logger.debug(f"  ID {ftl_tid[:8]}… not found in search results for '{tname_q}'")
    except Exception as exc:
        logger.debug(f"  FTL search error for {ftl_tid}: {exc}")

    # Fallback: broader search without name filter
    search_url_all = (
        f"{FTL_BASE}/tournaments/search/data/advanced?country=GBR"
    )
    try:
        time.sleep(REQUEST_DELAY)
        resp = client.get(search_url_all)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            for item in data:
                if (item.get("id") or "").upper() == ftl_tid.upper():
                    start = (item.get("start") or "")[:10]
                    return start or None
    except Exception as exc:
        logger.debug(f"  FTL broad search error: {exc}")

    return None


# ── Phase 1: FTL date lookup for undated tournaments ─────────────────

def phase1_ftl(apply: bool) -> dict[str, str]:
    """
    For each tournament with ftl_tournament_id but null date_start,
    fetch the start date from FTL and optionally write it back.

    Returns {tournament_id: date_start} for all successfully resolved entries
    (whether applied or not) — so Phase 2 can propagate even in dry-run.
    """
    logger.info("Phase 1: FTL lookup for tournaments with null date_start")

    undated = (
        db.table("tournaments")
          .select("id, name, ftl_tournament_id")
          .is_("date_start", "null")
          .not_.is_("ftl_tournament_id", "null")
          .limit(10000)
          .execute()
          .data or []
    )
    logger.info(f"  Tournaments to look up: {len(undated)}")

    if not undated:
        logger.info("  Nothing to fetch.")
        return {}

    resolved: dict[str, str] = {}
    failed = 0

    for t in undated:
        ftl_tid = t["ftl_tournament_id"]
        t_name  = t["name"]
        start   = _ftl_get_start_date(ftl_tid, t_name)

        if start:
            logger.info(f"  ✓ '{t_name}' → {start}")
            resolved[t["id"]] = start
            if apply:
                db.table("tournaments").update(
                    {"date_start": start}
                ).eq("id", t["id"]).execute()
        else:
            logger.warning(
                f"  ✗ '{t_name}' (ftl_id={ftl_tid[:8]}…) — start date not found on FTL"
            )
            failed += 1

    logger.info(
        f"  Phase 1 complete: "
        f"{'updated' if apply else 'would update'} {len(resolved)} tournaments, "
        f"{failed} not resolvable"
    )
    return resolved


# ── Phase 2: propagate tournament dates to events ─────────────────────

def phase2_propagate(apply: bool, extra_dates: dict[str, str] | None = None) -> int:
    """
    Copy tournament.date_start → event.date for all null-date events whose
    linked tournament has a known date_start.

    extra_dates: {tournament_id: date_start} for dates just resolved in Phase 1
    (used so dry runs still report what would happen even before writing to DB).

    Returns the number of events updated (or that would be updated).
    """
    logger.info("Phase 2: propagating tournament dates to linked events")

    null_events = (
        db.table("events")
          .select("id, tournament_id")
          .is_("date", "null")
          .limit(10000)
          .execute()
          .data or []
    )
    logger.info(f"  Events with null date: {len(null_events)}")

    if not null_events:
        logger.info("  Nothing to propagate.")
        return 0

    # Load all tournaments with a known date_start
    dated_tourneys = (
        db.table("tournaments")
          .select("id, date_start")
          .not_.is_("date_start", "null")
          .limit(10000)
          .execute()
          .data or []
    )
    date_map: dict[str, str] = {t["id"]: t["date_start"][:10] for t in dated_tourneys}

    # Merge in any just-resolved dates (for dry runs where DB wasn't updated yet)
    if extra_dates:
        for tid, ds in extra_dates.items():
            date_map[tid] = ds[:10]

    updated = 0
    no_date = 0
    for ev in null_events:
        tid = ev.get("tournament_id")
        if not tid:
            continue
        ds = date_map.get(tid)
        if not ds:
            no_date += 1
            continue
        if apply:
            db.table("events").update({"date": ds}).eq("id", ev["id"]).execute()
        updated += 1

    logger.info(
        f"  Phase 2 complete: "
        f"{'updated' if apply else 'would update'} {updated} events "
        f"({no_date} events have a tournament with no date — genuinely unknown)"
    )
    return updated


# ── Main ──────────────────────────────────────────────────────────────

def main(apply: bool) -> None:
    mode = "APPLY" if apply else "DRY RUN"
    print(f"\n{'=' * 64}")
    print(f"backfill_tournament_dates.py  [{mode}]")
    print("=" * 64)

    resolved = phase1_ftl(apply)
    events_fixed = phase2_propagate(apply, extra_dates=resolved)

    print(f"\nSummary:")
    print(f"  Tournaments with date_start backfilled: {len(resolved)}")
    print(f"  Events with date backfilled:            {events_fixed}")
    if not apply:
        print("\n  Re-run with --apply to write these changes to the database.")
    print("=" * 64)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--apply", action="store_true", help="Write updates to DB")
    args = parser.parse_args()
    main(apply=args.apply)
