"""
Sweep for events misfiled under the wrong tournament year by the UK Ratings collector.

For each event row where uk_ratings_tourney_id IS NOT NULL, check whether the
event's date year matches the linked tournament's date_start year.  If the
mismatch is greater than 1, find the correct same-named tournament for that
year and re-file the event under it (updating tournament_id).

Usage:
  python scripts/refile_wrong_year_events.py           # dry run — prints changes only
  python scripts/refile_wrong_year_events.py --apply   # write updates to DB
"""

import os
import sys
import re
import argparse
from difflib import SequenceMatcher

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
db = create_client(SUPABASE_URL, SUPABASE_KEY)

FUZZY_THRESHOLD = 0.72


# ── helpers ────────────────────────────────────────────────────────

def _norm(name: str) -> str:
    """Lightweight normalisation for tournament name comparison."""
    name = name.lower().strip()
    name = re.sub(r"\b(20\d{2})\b", "", name)   # strip 4-digit years
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _fuzzy(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _year(date_str) -> int | None:
    """Extract 4-digit year from a date string or return None."""
    if not date_str:
        return None
    try:
        return int(str(date_str)[:4])
    except (ValueError, TypeError):
        return None


# ── main ───────────────────────────────────────────────────────────

def main(apply: bool):
    print(f"\n{'[DRY RUN]' if not apply else '[APPLY MODE]'} Sweeping for year-misfiled events...\n")

    # Load all events that were linked by UK Ratings (have uk_ratings_tourney_id)
    events_raw = (
        db.table("events")
          .select("id, athlete_id, tournament_id, date, event_name, uk_ratings_tourney_id")
          .not_.is_("uk_ratings_tourney_id", "null")
          .limit(10000)
          .execute()
          .data or []
    )
    print(f"Events with uk_ratings_tourney_id: {len(events_raw)}")

    # Load all tournaments
    tourneys_raw = (
        db.table("tournaments")
          .select("id, name, date_start")
          .limit(10000)
          .execute()
          .data or []
    )
    tourney_by_id = {t["id"]: t for t in tourneys_raw}
    print(f"Tournaments in DB: {len(tourneys_raw)}\n")

    # Identify misfiled events
    misfiled = []
    for ev in events_raw:
        ev_year  = _year(ev.get("date"))
        t        = tourney_by_id.get(ev["tournament_id"])
        if not t or not ev_year:
            continue
        t_year = _year(t.get("date_start"))
        if not t_year:
            continue
        if abs(ev_year - t_year) > 1:
            misfiled.append({
                "event_id":        ev["id"],
                "event_name":      ev["event_name"],
                "event_date":      ev.get("date"),
                "ev_year":         ev_year,
                "current_tourney": t,
                "current_t_year":  t_year,
            })

    print(f"Misfiled events (year gap > 1): {len(misfiled)}")
    if not misfiled:
        print("Nothing to re-file.  All clear.")
        return

    print()

    # Build lookup: for each misfiled event, find the correct tournament
    # Strategy: same normalised name as the current linked tournament,
    # but with date_start year matching ev_year (within 1).
    updates = []
    no_match = []

    for entry in misfiled:
        cur_t    = entry["current_tourney"]
        ev_year  = entry["ev_year"]
        cur_norm = _norm(cur_t["name"])

        # Find best candidate among all tournaments with year-compatible date_start
        best_score, best_t = 0.0, None
        for t in tourneys_raw:
            if t["id"] == cur_t["id"]:
                continue
            t_year = _year(t.get("date_start"))
            if not t_year:
                continue
            if abs(t_year - ev_year) > 1:
                continue
            score = _fuzzy(cur_norm, _norm(t["name"]))
            if score > best_score:
                best_score, best_t = score, t

        if best_t and best_score >= FUZZY_THRESHOLD:
            updates.append({
                "event_id":       entry["event_id"],
                "event_name":     entry["event_name"],
                "event_date":     entry["event_date"],
                "ev_year":        ev_year,
                "from_tourney":   cur_t,
                "to_tourney":     best_t,
                "match_score":    best_score,
            })
        else:
            no_match.append({**entry, "best_score": best_score, "best_t": best_t})

    # Report planned updates
    if updates:
        print(f"{'─'*70}")
        print(f"Events to re-file: {len(updates)}")
        print(f"{'─'*70}")
        for u in updates:
            print(
                f"  [{u['event_date']}] {u['event_name']!r}\n"
                f"    FROM: {u['from_tourney']['name']!r} "
                f"(date_start={u['from_tourney'].get('date_start','?')[:10]}, "
                f"id={u['from_tourney']['id'][:8]}...)\n"
                f"      TO: {u['to_tourney']['name']!r} "
                f"(date_start={u['to_tourney'].get('date_start','?')[:10]}, "
                f"id={u['to_tourney']['id'][:8]}...) "
                f"score={u['match_score']:.2f}\n"
            )
        print(f"{'─'*70}\n")

    if no_match:
        print(f"Events with no suitable target tournament found: {len(no_match)}")
        for n in no_match:
            best_label = (
                f"closest={n['best_t']['name']!r} score={n['best_score']:.2f}"
                if n["best_t"] else "no candidates"
            )
            print(
                f"  [{n['event_date']}] {n['event_name']!r} "
                f"(ev_year={n['ev_year']}, "
                f"current={n['current_tourney']['name']!r}) — {best_label}"
            )
        print()

    # Apply
    if apply and updates:
        print("Applying updates...")
        ok, err = 0, 0
        for u in updates:
            try:
                db.table("events")\
                  .update({"tournament_id": u["to_tourney"]["id"]})\
                  .eq("id", u["event_id"])\
                  .execute()
                ok += 1
                print(f"  ✓ {u['event_name']} → {u['to_tourney']['name']}")
            except Exception as e:
                err += 1
                print(f"  ✗ {u['event_name']}: {e}")
        print(f"\nDone: {ok} updated, {err} errors.")
    elif not apply:
        print("Re-run with --apply to write these changes to the database.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write updates to DB")
    args = parser.parse_args()
    main(apply=args.apply)
