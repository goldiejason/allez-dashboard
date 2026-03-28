"""
scripts/discover_ftl_names.py — Auto-discover name_ftl for athletes

For each athlete missing name_ftl, finds any event they have with ftl_event_id,
fetches that event's results from FTL, and fuzzy-matches their display name
against FTL participant names to find the exact FTL spelling.

FTL typically formats names as "SURNAME Firstname" (uppercase surname), but
spellings can vary — this script finds the canonical version rather than guessing.

Usage:
  python scripts/discover_ftl_names.py           # dry-run, show proposed names
  python scripts/discover_ftl_names.py --apply   # write name_ftl to DB
"""

import sys
import os
import time
import logging
import argparse
from pathlib import Path
from typing import Optional

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv
load_dotenv()

from database.client import get_write_client

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

FTL_BASE        = "https://www.fencingtimelive.com"
REQUEST_DELAY   = 1.2
REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, */*",
    "Accept-Language": "en-GB,en;q=0.9",
}


def _get_json(url: str) -> Optional[list]:
    try:
        time.sleep(REQUEST_DELAY)
        r = httpx.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                      follow_redirects=True)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"GET {url} failed: {e}")
        return None


def _name_score(ftl_name: str, display_name: str) -> float:
    """
    Score how well ftl_name matches display_name.

    Strategy: all words in the display name must appear in the FTL name
    (case-insensitive). Returns fraction of display words matched — 1.0
    means every word in the display name was found in the FTL name.

    Examples:
      display="Daniel Polyakov"  ftl="POLYAKOV Daniel"    → 1.0  ✓
      display="Daniel Polyakov"  ftl="POLYAKOV Daniel J." → 1.0  ✓
      display="Bo Dickinson"     ftl="DICKINSON Bo"       → 1.0  ✓
      display="Daniel Polyakov"  ftl="JONES Daniel"       → 0.5  (only first name)
    """
    ftl_words     = set(ftl_name.upper().split())
    display_words = set(display_name.upper().split())
    if not display_words:
        return 0.0
    matched = display_words & ftl_words
    return len(matched) / len(display_words)


def find_ftl_name(display_name: str, ftl_event_id: str) -> Optional[str]:
    """
    Fetch event results and return the best-matching FTL name for display_name,
    or None if no confident match found (score < 1.0).
    """
    url  = f"{FTL_BASE}/events/results/data/{ftl_event_id}"
    data = _get_json(url)
    if not isinstance(data, list):
        return None

    best_name  = None
    best_score = 0.0

    for entry in data:
        ftl_name = (entry.get("name") or "").strip()
        if not ftl_name:
            continue
        score = _name_score(ftl_name, display_name)
        if score > best_score:
            best_score = score
            best_name  = ftl_name

    # Only accept a full match (all display-name words found in FTL name)
    if best_score >= 1.0:
        return best_name

    # Partial match — log for information but don't auto-accept
    if best_score > 0.0 and best_name:
        logger.warning(
            f"  Partial match ({best_score:.0%}) for '{display_name}': "
            f"best FTL candidate was '{best_name}' — skipped, needs manual check"
        )
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Auto-discover name_ftl for athletes from FTL event results"
    )
    parser.add_argument("--apply", action="store_true",
                        help="Write discovered names to DB (default: dry-run)")
    parser.add_argument("--name", type=str, default=None,
                        help="Only process athletes whose name contains this string")
    args = parser.parse_args()

    db = get_write_client()

    # Load athletes missing name_ftl
    q = db.table("athletes").select("id, name_display, name_ftl")
    if not args.name:
        q = q.is_("name_ftl", "null")
    else:
        q = q.ilike("name_display", f"%{args.name}%")
    athletes = q.order("name_display").execute().data or []

    if not athletes:
        logger.info("No athletes missing name_ftl — nothing to do.")
        return

    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(f"Mode: {mode}")
    logger.info(f"Athletes to process: {len(athletes)}")
    logger.info("")

    found    = []
    not_found = []

    for athlete in athletes:
        aid          = athlete["id"]
        display_name = athlete["name_display"]
        current_ftl  = athlete.get("name_ftl")

        logger.info(f"── {display_name}")

        # Find any event for this athlete that has ftl_event_id
        events = db.table("events")\
            .select("id, ftl_event_id, event_name")\
            .eq("athlete_id", aid)\
            .not_.is_("ftl_event_id", "null")\
            .limit(5)\
            .execute().data or []

        if not events:
            logger.info(f"   No events with ftl_event_id — cannot auto-discover")
            not_found.append({"name": display_name, "reason": "no matched events"})
            continue

        # Try each event until we find a confident name match
        ftl_name = None
        for ev in events:
            ftl_name = find_ftl_name(display_name, ev["ftl_event_id"])
            if ftl_name:
                logger.info(
                    f"   ✓ '{display_name}' → '{ftl_name}'  "
                    f"(from event: {ev['event_name']})"
                )
                break

        if not ftl_name:
            logger.warning(f"   ✗ No confident FTL name found")
            not_found.append({"name": display_name, "reason": "not found in event results"})
            continue

        found.append({"id": aid, "name_display": display_name, "name_ftl": ftl_name})

        if args.apply:
            db.table("athletes").update(
                {"name_ftl": ftl_name}
            ).eq("id", aid).execute()

    # ── Summary ─────────────────────────────────────────────────────────────
    print()
    print("FTL NAME DISCOVERY", f"({'APPLIED' if args.apply else 'DRY-RUN — nothing written'})")
    print("─" * 62)
    print(f"  Discovered:  {len(found)}")
    print(f"  Not found:   {len(not_found)}")
    print()

    if found:
        print("  Proposed name_ftl values:")
        for r in found:
            print(f"    {r['name_display']:<30} →  {r['name_ftl']}")
        print()

    if not_found:
        print("  Need manual name_ftl entry:")
        for r in not_found:
            print(f"    {r['name']:<30}  ({r['reason']})")
        print()

    if not args.apply and found:
        print("  ▶  Run with --apply to write to database.")


if __name__ == "__main__":
    main()
