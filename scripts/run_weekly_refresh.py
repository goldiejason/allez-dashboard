"""
Weekly refresh script — run by GitHub Actions every Saturday and Sunday night.

For each active athlete:
  - Skip if refreshed within the last 12 hours (respects on-demand button clicks)
  - Otherwise collect FTL pool/DE data, then run the full UK Ratings
    collection (event history, DE bouts, annual stats) for athletes
    who have a uk_ratings_id set.
"""

import logging
import sys
import os
from datetime import datetime, timezone, timedelta

# Ensure the project root is on sys.path so sibling packages resolve correctly
# regardless of which directory the script is invoked from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.client import get_write_client, get_read_client
from collectors.ftl_collector import collect_athlete, discover_recent_ftl_events
from collectors.ukratings_collector import collect_athlete as collect_ukratings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

STALE_AFTER_HOURS = 12  # skip athlete if refreshed within this many hours
# 12 hours: prevents the on-demand dashboard button from re-running the same day,
# but allows both the Saturday and Sunday scheduled runs to execute independently.

# Set FORCE_REFRESH=true in the environment to bypass the staleness check and
# refresh every active athlete regardless of when they were last updated.
FORCE_REFRESH = os.getenv("FORCE_REFRESH", "false").strip().lower() == "true"


def should_skip(last_refreshed: str | None) -> bool:
    if FORCE_REFRESH:
        return False
    if not last_refreshed:
        return False
    try:
        refreshed_at = datetime.fromisoformat(last_refreshed.replace("Z", "+00:00"))
        age = datetime.now(timezone.utc) - refreshed_at
        return age < timedelta(hours=STALE_AFTER_HOURS)
    except Exception:
        return False


def main():
    if FORCE_REFRESH:
        logger.info("FORCE_REFRESH=true — staleness check bypassed for all athletes")

    # ── Step 1: FTL-first event discovery ─────────────────────────
    # Scan FTL for UK tournaments in the last 7 days and create/link any
    # events where our athletes competed.  This ensures same-weekend events
    # appear in the dashboard before UK Ratings publishes its results
    # (UK Ratings typically has a 3-7 day publishing lag).
    logger.info("── Step 1: FTL recent event discovery")
    try:
        disc = discover_recent_ftl_events(days_back=7)
        logger.info(
            f"Discovery: {disc['tournaments_scanned']} tournaments scanned, "
            f"{disc['events_linked']} events linked, "
            f"{len(disc['errors'])} errors"
        )
    except Exception as exc:
        logger.error(f"FTL discovery step failed: {exc}")

    # ── Step 2: Per-athlete FTL + UK Ratings refresh ───────────────
    logger.info("── Step 2: Per-athlete FTL + UK Ratings refresh")
    db_read = get_read_client()

    athletes = db_read.table("athletes").select(
        "id, name_display, ftl_fencer_id, name_ftl, uk_ratings_id, weapon, last_refreshed, active"
    ).eq("active", True).limit(10000).execute().data

    logger.info(f"Starting weekly refresh for {len(athletes)} active athletes")
    skipped, refreshed, errored = 0, 0, 0

    for athlete in athletes:
        name = athlete["name_display"]

        if should_skip(athlete.get("last_refreshed")):
            logger.info(f"SKIP  {name} — refreshed within last {STALE_AFTER_HOURS} hours")
            skipped += 1
            continue

        if not athlete.get("name_ftl"):
            logger.warning(f"SKIP  {name} — no name_ftl configured")
            skipped += 1
            continue

        logger.info(f"START {name}")
        try:
            summary = collect_athlete(
                athlete_id=athlete["id"],
                name_ftl=athlete["name_ftl"] or athlete["name_display"],
            )
            logger.info(
                f"FTL   {name} — "
                f"events_updated={summary['events_updated']}, "
                f"events_skipped={summary['events_skipped']}, "
                f"errors={len(summary['errors'])}"
            )

            if athlete.get("uk_ratings_id") and athlete.get("weapon"):
                ukr = collect_ukratings(
                    athlete_id=athlete["id"],
                    uk_ratings_id=athlete["uk_ratings_id"],
                    weapon=athlete["weapon"],
                )
                logger.info(
                    f"UKR   {name} — "
                    f"events={ukr.get('events', {}).get('events_upserted', 0)}, "
                    f"de_bouts={ukr.get('de_bouts', {}).get('inserted', 0)}, "
                    f"annual_years={ukr.get('annual_years', 0)}"
                )

            refreshed += 1

        except Exception as e:
            logger.error(f"ERROR {name}: {e}")
            errored += 1

    logger.info(
        f"Weekly refresh complete — "
        f"refreshed={refreshed}, skipped={skipped}, errors={errored}"
    )


if __name__ == "__main__":
    main()
