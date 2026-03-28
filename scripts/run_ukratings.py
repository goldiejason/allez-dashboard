"""
One-off UK Ratings collection script.

Usage
-----
# Collect a single athlete by display name (partial match, case-insensitive):
  python scripts/run_ukratings.py --name "Daniel Panga"

# Collect ALL athletes that have a uk_ratings_id in the DB:
  python scripts/run_ukratings.py --all

# Dry-run: just list who would be collected, no network calls:
  python scripts/run_ukratings.py --all --dry-run

Run from the project root (allez-dashboard/):
  cd allez-dashboard
  python scripts/run_ukratings.py --name "Daniel Panga"
"""

import argparse
import logging
import os
import sys

# Allow running from the project root: python scripts/run_ukratings.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _load_athletes(db, name_filter: str | None) -> list[dict]:
    q = (
        db.table("athletes")
        .select("id, name_display, uk_ratings_id, uk_ratings_weapon_code, weapon")
        .not_.is_("uk_ratings_id", "null")
        .not_.is_("uk_ratings_weapon_code", "null")
        .eq("active", True)
    )
    rows = q.execute().data or []

    if name_filter:
        # Match all words in the filter against the display name (order-independent)
        words = name_filter.strip().lower().split()
        rows = [r for r in rows if all(w in r["name_display"].lower() for w in words)]
        if not rows:
            logger.error(f"No athlete found matching '{name_filter}' with a UK Ratings ID")
            sys.exit(1)

    return rows


def main():
    parser = argparse.ArgumentParser(description="Run UK Ratings data collection")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--name",  metavar="NAME", help="Partial display name of the athlete to collect")
    group.add_argument("--all",   action="store_true",  help="Collect all athletes with a uk_ratings_id")
    parser.add_argument("--dry-run", action="store_true", help="List athletes without making any changes")
    args = parser.parse_args()

    from database.client import get_write_client
    from collectors.ukratings_collector import collect_athlete

    db = get_write_client()
    athletes = _load_athletes(db, args.name if args.name else None)

    if not athletes:
        logger.warning("No athletes to collect")
        return

    logger.info(f"{'DRY RUN — ' if args.dry_run else ''}Collecting {len(athletes)} athlete(s)")
    for a in athletes:
        logger.info(f"  · {a['name_display']}  (uk_ratings_id={a['uk_ratings_id']})")

    if args.dry_run:
        logger.info("Dry run complete — no changes made")
        return

    total_events = 0
    total_de     = 0
    errors       = []

    for athlete in athletes:
        name = athlete["name_display"]
        logger.info(f"━━━ START {name} ━━━")
        try:
            result = collect_athlete(
                athlete_id=athlete["id"],
                uk_ratings_id=athlete["uk_ratings_id"],
                weapon=athlete["weapon"],
            )

            ev  = result.get("events",   {})
            de  = result.get("de_bouts", {})
            ann = result.get("annual_years", 0)

            logger.info(
                f"DONE  {name} — "
                f"events_upserted={ev.get('events_upserted', 0)}, "
                f"tournaments_created={ev.get('tournaments_created', 0)}, "
                f"de_bouts_inserted={de.get('inserted', 0)}, "
                f"de_no_event={de.get('skipped_no_event', 0)}, "
                f"annual_years={ann}"
            )

            ev_errors = ev.get("errors", []) + de.get("errors", [])
            if ev_errors:
                logger.warning(f"  {len(ev_errors)} non-fatal error(s):")
                for e in ev_errors:
                    logger.warning(f"    - {e}")

            total_events += ev.get("events_upserted", 0)
            total_de     += de.get("inserted", 0)

        except Exception as exc:
            logger.error(f"FATAL error for {name}: {exc}", exc_info=True)
            errors.append(f"{name}: {exc}")

    logger.info(
        f"\n{'━' * 40}\n"
        f"Collection complete\n"
        f"  Athletes processed : {len(athletes)}\n"
        f"  Events upserted    : {total_events}\n"
        f"  DE bouts inserted  : {total_de}\n"
        f"  Fatal errors       : {len(errors)}"
    )
    if errors:
        for e in errors:
            logger.error(f"  ✗ {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
