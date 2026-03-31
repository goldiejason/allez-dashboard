"""
Phase 2 canary test — run this on the Mac to verify pool bout collection.

Tests against Daniel Panga only:
  1. Finds his first event with a FTL event ID and pool_id_seed
  2. Fetches the pool scores page
  3. Parses the bout matrix
  4. Prints extracted bouts (does NOT write to Supabase unless --write is passed)
  5. Cross-checks: ts + tr totals should be consistent, results should add up

Usage:
  python scripts/test_pool_bouts.py          # dry run (no DB write)
  python scripts/test_pool_bouts.py --write  # write bouts to Supabase
"""

import sys
import os
import logging
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

from database.client import get_read_client, get_write_client
from collectors.ftl_collector import (
    _get_html,
    _discover_pool_ids,
    _parse_pool_fragment,
    _extract_bouts_from_pool,
    FTL_BASE,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="Write bouts to Supabase")
    args = parser.parse_args()

    db = get_read_client()

    # Find Daniel Panga
    athletes = db.table("athletes").select("id, name_display, name_ftl").ilike("name_display", "%panga%").execute().data
    if not athletes:
        logger.error("Daniel Panga not found in athletes table")
        sys.exit(1)

    athlete = athletes[0]
    logger.info(f"Athlete: {athlete['name_display']} | name_ftl={athlete['name_ftl']}")

    # Get events with pool_id_seed
    events = (
        db.table("events")
        .select("id, ftl_event_id, pool_id_seed, event_name, date, pool_v, pool_l")
        .eq("athlete_id", athlete["id"])
        .not_.is_("ftl_event_id", "null")
        .not_.is_("pool_id_seed", "null")
        .order("date", desc=True)
        .limit(3)
        .execute()
        .data
    )

    if not events:
        logger.error("No events with both ftl_event_id and pool_id_seed found")
        sys.exit(1)

    logger.info(f"Found {len(events)} events with pool seeds — testing most recent")

    for event in events:
        logger.info(
            f"\n{'='*60}\n"
            f"Event : {event['event_name']}\n"
            f"Date  : {event['date']}\n"
            f"FTL ID: {event['ftl_event_id']}\n"
            f"Agg   : V{event['pool_v']} / L{event['pool_l']}"
        )

        url = f"{FTL_BASE}/pools/scores/{event['ftl_event_id']}/{event['pool_id_seed']}"
        logger.info(f"URL   : {url}")

        # Step 1: discover all pool IDs from the landing page JS
        pool_ids = _discover_pool_ids(event["ftl_event_id"], event["pool_id_seed"])
        logger.info(f"Found {len(pool_ids)} pool(s) in event")

        # Step 2: search through pool fragments for this athlete
        surname = athlete["name_ftl"].split()[0].upper()
        bouts = None
        found_pool_num = None
        for pool_num, pool_id in enumerate(pool_ids, 1):
            frag_url = f"{FTL_BASE}/pools/scores/{event['ftl_event_id']}/{event['pool_id_seed']}/{pool_id}?dbut=true"
            soup = _get_html(frag_url)
            if not soup or surname not in soup.get_text().upper():
                continue
            pool = _parse_pool_fragment(soup, pool_num)
            if not pool:
                continue
            bouts = _extract_bouts_from_pool(pool, athlete["name_ftl"])
            if bouts is not None:
                found_pool_num = pool_num
                break

        if bouts is None:

            logger.warning(f"Athlete '{athlete['name_ftl']}' not found in any of {len(pool_ids)} pools")
            continue

        logger.info(f"Found athlete in pool #{found_pool_num}")

        # Print extracted bouts
        wins = sum(1 for b in bouts if b["result"])
        losses = len(bouts) - wins
        total_ts = sum(b["ts"] for b in bouts)
        total_tr = sum(b["tr"] for b in bouts)

        logger.info(f"\nExtracted {len(bouts)} bouts — W{wins}/L{losses}  TS={total_ts} TR={total_tr} Ind={total_ts-total_tr}")
        logger.info(f"{'RESULT':<8} {'TS':>3} {'TR':>3}  {'OPPONENT':<30} {'CLUB'}")
        logger.info("-" * 75)
        for b in bouts:
            r = "WIN" if b["result"] else "LOSS"
            logger.info(f"{r:<8} {b['ts']:>3} {b['tr']:>3}  {b['opponent_name']:<30} {b['opponent_club']}")

        # Cross-check against aggregate stats
        if event["pool_v"] is not None:
            match = (wins == event["pool_v"] and losses == event["pool_l"])
            logger.info(
                f"\nCross-check vs aggregate: W{wins}==V{event['pool_v']} L{losses}==L{event['pool_l']} → "
                f"{'PASS ✓' if match else 'MISMATCH ✗'}"
            )

        # Write if requested
        if args.write:
            db_write = get_write_client()
            rows = [{**b, "event_id": event["id"]} for b in bouts]
            # Delete existing first (clean re-run)
            db_write.table("pool_bouts").delete().eq("event_id", event["id"]).execute()
            db_write.table("pool_bouts").insert(rows).execute()
            logger.info(f"Written {len(rows)} bouts to Supabase for event {event['id']}")
        else:
            logger.info("\n(Dry run — pass --write to write to Supabase)")


if __name__ == "__main__":
    main()
