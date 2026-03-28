"""
scripts/discover_ftl_events.py — FTL Event Discovery

Automatically finds FTL event IDs for tournaments/events in the database by:
  1. Searching FTL's tournament API by name + date range
  2. Matching tournament names using Jaccard word-overlap scoring
  3. Parsing the eventSchedule HTML to extract per-event GUIDs
  4. Matching events by normalised (weapon, gender, age) tuple

Discovered FTL API:
  /tournaments/search/data/advanced?tname=...&country=GBR&from=...&to=...
    → JSON list: [{id, name, location, dates, start}, ...]

  /tournaments/eventSchedule/{tournament_id}
    → HTML page; each event is a <tr id="ev_{event_guid}"> row with
      cells: [start_time, event_name, status]

  Once ftl_event_id is populated, the existing ftl_collector.py handles
  placement + pool stats collection automatically.

Usage:
  python scripts/discover_ftl_events.py              # dry-run by default
  python scripts/discover_ftl_events.py --apply      # write matches to DB
  python scripts/discover_ftl_events.py --limit 5    # process 5 tournaments
  python scripts/discover_ftl_events.py --name "British Youth"  # one tournament
  python scripts/discover_ftl_events.py --apply --min-score 0.6  # lower threshold
"""

import sys
import os
import re
import time
import csv
import logging
import argparse
import unicodedata
from datetime import date as Date
from pathlib import Path
from typing import Optional
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
load_dotenv()

from database.client import get_write_client

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s  %(message)s",
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════

FTL_BASE   = "https://www.fencingtimelive.com"
REQUEST_DELAY   = 1.2   # seconds between FTL requests — be polite
REQUEST_TIMEOUT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/json,*/*",
    "Accept-Language": "en-GB,en;q=0.9",
}

# Minimum Jaccard score to accept a tournament name match
DEFAULT_TOURNAMENT_THRESHOLD = 0.65


# ═══════════════════════════════════════════════════════════════
# Event name normalisation
# ═══════════════════════════════════════════════════════════════

def _ascii(text: str) -> str:
    """Strip Unicode accents so 'Épée' → 'Epee', 'É' → 'E', etc."""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def _weapon(text: str) -> Optional[str]:
    # Normalise accents first so "Épée" matches "epee"
    m = re.search(r'\b(epee|foil|sabre|saber)\b', _ascii(text), re.I)
    if not m:
        return None
    return m.group(1).lower().replace("saber", "sabre")


def _gender(text: str) -> Optional[str]:
    """
    Handles many conventions:
      men / mens / man / boys / m / m.  → 'men'
      women / womens / woman / girls / w / w.  → 'women'
      mixed                              → 'mixed'
      Mixed/Men's (FTL BYC 2025+ format) → 'men'
      Mixed/Women's                      → 'women'
    """
    # Handle compound "Mixed/Men's" / "Mixed/Women's" before word splitting
    # (FTL uses this for what UK Ratings calls plain "Men's" / "Women's")
    if re.search(r'mixed/men', text, re.I):
        return "men"
    if re.search(r'mixed/wom', text, re.I):
        return "women"
    words = re.sub(r"[^\w\s]", " ", text.lower()).split()
    for w in words:
        if w in ("men", "mens", "man", "boys", "boy"):
            return "men"
        if w in ("women", "womens", "woman", "girls", "girl"):
            return "women"
        if w == "mixed":
            return "mixed"
    # Single-letter abbreviations ("M." / "W.") — check raw text
    if re.search(r'\bm\b', text, re.I) and not re.search(r'\bw\b', text, re.I):
        return "men"
    if re.search(r'\bw\b', text, re.I) and not re.search(r'\bm\b', text, re.I):
        return "women"
    return None


# Canonical age strings
_AGE_CANON = {
    "cadet":   "u16",
    "junior":  "u20",
    "senior":  "senior",
    "veteran": "veteran",
    "vet":     "veteran",
}

def _age(text: str) -> Optional[str]:
    """
    Recognises: u10/u-10/under10/under 10 … u23, cadet, junior, senior, veteran.
    Returns canonical strings like 'u14', 'u16', 'u20', 'senior', 'veteran'.

    UK Ratings uses odd age groups (U-11, U-13, U-15) for beginner sub-brackets
    that run within the standard U12/U14/U16 events on FTL.  Map them upward:
        U-11 / Under 11 → u12
        U-13 / Under 13 → u14
        U-15 / Under 15 → u16
        U-17 / Under 17 → u18
        U-19 / Under 19 → u20
    """
    t = text.lower()
    for word, canon in _AGE_CANON.items():
        if re.search(r'\b' + word + r'\b', t):
            return canon
    m = re.search(
        r'\b(?:u-?|under[\s-]?)(10|11|12|13|14|15|16|17|18|19|20|23)\b', t
    )
    if m:
        n = int(m.group(1))
        # Round odd ages up to the nearest even FTL bracket
        if n % 2 == 1:
            n += 1
        return f"u{n}"
    return None


def normalise_event(name: str) -> tuple:
    """Return (weapon, gender, age) — all three or some may be None."""
    return (_weapon(name), _gender(name), _age(name))


def event_norm_complete(norm: tuple) -> bool:
    return all(v is not None for v in norm)


# ═══════════════════════════════════════════════════════════════
# Tournament name fuzzy matching
# ═══════════════════════════════════════════════════════════════

def _strip_year(name: str) -> str:
    """Remove standalone year or year-range (2023, 2023/24, 2023-24)."""
    cleaned = re.sub(r'\b20\d{2}(?:[/\-]\d{2,4})?\b', '', name)
    return re.sub(r'\s{2,}', ' ', cleaned).strip().strip('-').strip()


def _tokenise(name: str) -> set:
    """Lowercase, strip punctuation, return word set (words > 2 chars)."""
    words = re.sub(r"[^\w\s]", " ", name.lower()).split()
    return {w for w in words if len(w) > 2}


def jaccard(a: str, b: str) -> float:
    ta = _tokenise(_strip_year(a))
    tb = _tokenise(_strip_year(b))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


# ═══════════════════════════════════════════════════════════════
# FTL HTTP helpers
# ═══════════════════════════════════════════════════════════════

def _get(url: str, params: dict = None) -> Optional[httpx.Response]:
    try:
        time.sleep(REQUEST_DELAY)
        r = httpx.get(
            url, headers=HEADERS, params=params,
            timeout=REQUEST_TIMEOUT, follow_redirects=True
        )
        r.raise_for_status()
        return r
    except Exception as e:
        logger.error(f"GET {url} failed: {e}")
        return None


def search_ftl_tournaments(name: str, date_start: str,
                           is_international: bool = False) -> list[dict]:
    """
    Search FTL for tournaments matching name near the given year.
    Returns list of {id, name, location, dates, start}.
    """
    year = int(date_start[:4]) if date_start and len(date_start) >= 4 else 2024
    params = {
        "tname": _strip_year(name),
        "from":  f"{year - 1}-01-01",
        "to":    f"{year + 1}-12-31",
    }
    if not is_international:
        params["country"] = "GBR"

    r = _get(f"{FTL_BASE}/tournaments/search/data/advanced", params=params)
    if not r:
        return []
    try:
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def fetch_event_schedule(ftl_tournament_id: str) -> list[dict]:
    """
    Fetch /tournaments/eventSchedule/{id} and parse event rows.
    Returns list of {ftl_event_id, name}.
    """
    url = f"{FTL_BASE}/tournaments/eventSchedule/{ftl_tournament_id}"
    r = _get(url)
    if not r:
        return []
    soup = BeautifulSoup(r.text, "lxml")
    events = []
    for tr in soup.find_all("tr", id=re.compile(r"^ev_", re.I)):
        eid = tr["id"][3:]  # strip "ev_"
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        # cells: [start_time, event_name, status_text]
        ename = cells[1] if len(cells) >= 2 else ""
        if ename:
            events.append({"ftl_event_id": eid.upper(), "name": ename})
    return events


# ═══════════════════════════════════════════════════════════════
# Matching logic
# ═══════════════════════════════════════════════════════════════

def match_tournament(db_tournament: dict,
                     threshold: float) -> Optional[dict]:
    """
    Find the best FTL tournament match for a DB tournament.
    Returns enriched FTL tournament dict (with _score) or None.
    """
    name         = db_tournament["name"]
    date_start   = db_tournament.get("date_start") or ""
    is_intl      = db_tournament.get("is_international", False)

    candidates = search_ftl_tournaments(name, date_start, is_intl)

    # If GBR search returned nothing, retry without country filter
    if not candidates and not is_intl:
        logger.debug(f"  Retrying without country filter for '{name}'")
        candidates = search_ftl_tournaments(name, date_start, is_international=True)

    if not candidates:
        return None

    scored = [(c, jaccard(name, c["name"])) for c in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)

    # Keep only candidates that meet the threshold
    above = [(c, s) for c, s in scored if s >= threshold]
    if not above:
        return None

    # When multiple candidates share the top Jaccard score (e.g. "British
    # Youth Championships 2023" vs "... 2024" both strip to the same words),
    # break the tie by choosing the tournament whose start date is closest to
    # the DB tournament's date_start.
    best_score = above[0][1]
    tied = [(c, s) for c, s in above if s == best_score]

    if len(tied) > 1 and date_start and len(date_start) >= 10:
        try:
            db_d = Date.fromisoformat(date_start[:10])
            def _date_gap(item):
                ftl_start = item[0].get("start", "")[:10]
                if not ftl_start:
                    return 9999
                return abs((db_d - Date.fromisoformat(ftl_start)).days)
            tied.sort(key=_date_gap)
        except Exception:
            pass   # fall through to first result if date parsing fails

    best, score = tied[0]
    best["_score"] = score
    return best


def match_events(db_events: list[dict],
                 ftl_events: list[dict]) -> list[dict]:
    """
    Match DB events to FTL events by normalised (weapon, gender, age).

    Matching strategy:
      1. Exact (weapon, gender, age) — safe, always applied.
      2. Partial weapon+gender — ONLY accepted if there is exactly one FTL event
         with that weapon+gender combination at this tournament. If multiple age
         groups exist for the same weapon+gender (e.g. U12/U14/U16/U18 all have
         "Boys Foil") the match is ambiguous and marked as "ambiguous" rather
         than guessing.

    Returns list of match result dicts.
    """
    # Build FTL lookup: norm_tuple → ftl_event
    ftl_by_norm: dict[tuple, dict] = {}
    for fe in ftl_events:
        norm = normalise_event(fe["name"])
        if event_norm_complete(norm):
            ftl_by_norm[norm] = fe

    # Pre-compute weapon+gender → list of FTL events (to detect ambiguity)
    ftl_by_wg: dict[tuple, list[dict]] = {}
    for norm_t, fe in ftl_by_norm.items():
        wg = (norm_t[0], norm_t[1])   # (weapon, gender)
        ftl_by_wg.setdefault(wg, []).append(fe)

    results = []
    for dbe in db_events:
        norm = normalise_event(dbe.get("event_name") or "")
        match = None
        match_quality = "none"

        # 1. Exact match (all 3 components)
        if event_norm_complete(norm):
            match = ftl_by_norm.get(norm)
            if match:
                match_quality = "exact"

        # 2. Partial fallback — only if weapon+gender is unambiguous
        if not match and norm[0] and norm[1]:
            weapon, gender, age = norm
            candidates = ftl_by_wg.get((weapon, gender), [])
            if len(candidates) == 1:
                # Only one FTL event with this weapon+gender → safe to match
                match = candidates[0]
                match_quality = "weapon+gender"
            elif len(candidates) > 1:
                # Multiple age groups → ambiguous, flag for manual review
                match_quality = "ambiguous"

        results.append({
            "db_event_id":    dbe["id"],
            "event_name":     dbe.get("event_name", ""),
            "norm":           norm,
            "ftl_event_id":   match["ftl_event_id"] if match else None,
            "ftl_event_name": match["name"] if match else None,
            "match_quality":  match_quality,
        })
    return results


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Discover FTL event IDs for tournaments in the DB"
    )
    parser.add_argument(
        "--apply",      action="store_true",
        help="Write matches to DB (default: dry-run, show only)"
    )
    parser.add_argument(
        "--name",       type=str, default=None,
        help="Only process tournaments whose name contains this string"
    )
    parser.add_argument(
        "--limit",      type=int, default=0,
        help="Process at most N tournaments"
    )
    parser.add_argument(
        "--min-score",  type=float, default=DEFAULT_TOURNAMENT_THRESHOLD,
        help=f"Minimum Jaccard score for tournament match (default {DEFAULT_TOURNAMENT_THRESHOLD})"
    )
    parser.add_argument(
        "--apply-partial",  action="store_true",
        help="Also apply weapon+gender partial matches (review CSV first)"
    )
    parser.add_argument(
        "--include-matched", action="store_true",
        help="Re-process tournaments that already have ftl_tournament_id"
    )
    parser.add_argument(
        "--csv",        default="unmatched_ftl_events.csv",
        help="Path to write unmatched events CSV"
    )
    args = parser.parse_args()

    db = get_write_client()

    # ── Load tournaments ─────────────────────────────────────────
    q = db.table("tournaments").select(
        "id, name, date_start, ftl_tournament_id, is_international"
    )
    if not args.include_matched:
        q = q.is_("ftl_tournament_id", "null")
    if args.name:
        q = q.ilike("name", f"%{args.name}%")
    result = q.order("date_start", desc=True).execute()
    tournaments = result.data or []

    if args.limit:
        tournaments = tournaments[:args.limit]

    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(f"Mode: {mode}")
    logger.info(f"Tournaments to process: {len(tournaments)}")
    logger.info(f"Tournament match threshold: {args.min_score}")
    logger.info("")

    totals = {
        "t_matched": 0, "t_failed": 0,
        "e_matched": 0, "e_partial": 0, "e_failed": 0,
    }
    unmatched_rows = []

    for t in tournaments:
        t_name = t["name"]
        t_date = (t.get("date_start") or "")[:10]
        logger.info(f"── {t_name} ({t_date})")

        # ── Step 1: Find FTL tournament ──────────────────────────
        if t.get("ftl_tournament_id") and args.include_matched:
            # Already matched — skip search, use existing ID
            ftl_tid  = t["ftl_tournament_id"]
            ftl_name = "(already matched)"
            score    = 1.0
            logger.info(f"   Tournament: already matched → {ftl_tid}")
        else:
            ftl_t = match_tournament(t, threshold=args.min_score)
            if not ftl_t:
                logger.warning(f"   ✗ No FTL tournament match found")
                totals["t_failed"] += 1
                continue
            ftl_tid  = ftl_t["id"]
            ftl_name = ftl_t["name"]
            score    = ftl_t["_score"]
            logger.info(
                f"   Tournament: '{ftl_name}'  score={score:.2f}  id={ftl_tid[:8]}…"
            )

        # ── Step 2: Load unmatched DB events ─────────────────────
        evq = db.table("events")\
            .select("id, event_name, ftl_event_id")\
            .eq("tournament_id", t["id"])\
            .is_("ftl_event_id", "null")
        db_events = evq.execute().data or []

        if not db_events:
            logger.info(f"   Events: all already matched — skipping event step")
            if args.apply and not t.get("ftl_tournament_id"):
                db.table("tournaments").update(
                    {"ftl_tournament_id": ftl_tid}
                ).eq("id", t["id"]).execute()
            totals["t_matched"] += 1
            continue

        # Deduplicate events by event_name (same tournament may have many
        # athletes each linked to the same event)
        seen_names: dict[str, dict] = {}
        for e in db_events:
            ename = e.get("event_name") or ""
            if ename not in seen_names:
                seen_names[ename] = e
        unique_events = list(seen_names.values())
        logger.info(
            f"   Events: {len(db_events)} rows → {len(unique_events)} unique names"
        )

        # ── Step 3: Fetch FTL event schedule ─────────────────────
        ftl_events = fetch_event_schedule(ftl_tid)
        logger.info(f"   FTL schedule: {len(ftl_events)} events")
        for fe in ftl_events:
            logger.debug(f"     FTL event: {fe['name']} → {fe['ftl_event_id'][:8]}…")

        # ── Step 4: Match events ──────────────────────────────────
        matches = match_events(unique_events, ftl_events)

        for m in matches:
            quality = m["match_quality"]
            icon = {"exact": "✓", "weapon+gender": "~", "ambiguous": "?", "none": "✗"}.get(quality, "✗")

            if quality == "exact":
                totals["e_matched"] += 1
                logger.info(
                    f"   {icon} '{m['event_name']}' ({m['norm']}) "
                    f"→ '{m['ftl_event_name']}'"
                )
            elif quality == "weapon+gender":
                totals["e_partial"] += 1
                logger.info(
                    f"   {icon} '{m['event_name']}' ({m['norm']}) "
                    f"→ '{m['ftl_event_name']}' [partial — unambiguous]"
                )
                # Partial match goes to CSV for review even if it will be applied
                unmatched_rows.append({
                    "review_action":       "VERIFY — partial match (weapon+gender only)",
                    "db_tournament":       t_name,
                    "db_tournament_date":  t_date,
                    "db_event_name":       m["event_name"],
                    "normalised":          str(m["norm"]),
                    "proposed_ftl_event":  m["ftl_event_name"],
                    "proposed_ftl_event_id": m["ftl_event_id"],
                    "ftl_tournament_id":   ftl_tid,
                    "ftl_tournament_name": ftl_name,
                    "ftl_events_available": " | ".join(fe["name"] for fe in ftl_events),
                })
            elif quality == "ambiguous":
                totals["e_failed"] += 1
                candidates = " | ".join(fe["name"] for fe in ftl_events
                                        if _weapon(fe["name"]) == m["norm"][0]
                                        and _gender(fe["name"]) == m["norm"][1])
                logger.warning(
                    f"   {icon} '{m['event_name']}' (norm={m['norm']}) "
                    f"— ambiguous: multiple age groups ({candidates})"
                )
                unmatched_rows.append({
                    "review_action":       "MANUAL — ambiguous age group",
                    "db_tournament":       t_name,
                    "db_tournament_date":  t_date,
                    "db_event_name":       m["event_name"],
                    "normalised":          str(m["norm"]),
                    "proposed_ftl_event":  "",
                    "proposed_ftl_event_id": "",
                    "ftl_tournament_id":   ftl_tid,
                    "ftl_tournament_name": ftl_name,
                    "ftl_events_available": " | ".join(fe["name"] for fe in ftl_events),
                })
            else:  # none
                totals["e_failed"] += 1
                logger.warning(
                    f"   ✗ '{m['event_name']}' (norm={m['norm']}) — no FTL match"
                )
                unmatched_rows.append({
                    "review_action":       "MANUAL — no FTL match found",
                    "db_tournament":       t_name,
                    "db_tournament_date":  t_date,
                    "db_event_name":       m["event_name"],
                    "normalised":          str(m["norm"]),
                    "proposed_ftl_event":  "",
                    "proposed_ftl_event_id": "",
                    "ftl_tournament_id":   ftl_tid,
                    "ftl_tournament_name": ftl_name,
                    "ftl_events_available": " | ".join(fe["name"] for fe in ftl_events),
                })

        totals["t_matched"] += 1

        # ── Step 5: Apply if requested ────────────────────────────
        if args.apply:
            # Update tournament
            if not t.get("ftl_tournament_id"):
                db.table("tournaments").update(
                    {"ftl_tournament_id": ftl_tid}
                ).eq("id", t["id"]).execute()

            # Decide which match qualities to apply
            # "exact"         → always apply
            # "weapon+gender" → apply only if --apply-partial flag is set
            # "ambiguous"/"none" → never auto-apply
            apply_qualities = {"exact"}
            if args.apply_partial:
                apply_qualities.add("weapon+gender")

            name_to_ftl = {
                m["event_name"]: m["ftl_event_id"]
                for m in matches
                if m["ftl_event_id"] and m["match_quality"] in apply_qualities
            }
            applied = 0
            for ev in db_events:
                ftl_eid = name_to_ftl.get(ev.get("event_name") or "")
                if ftl_eid:
                    db.table("events").update(
                        {"ftl_event_id": ftl_eid}
                    ).eq("id", ev["id"]).execute()
                    applied += 1
            skipped_partial = sum(
                1 for m in matches
                if m["match_quality"] == "weapon+gender" and not args.apply_partial
            )
            logger.info(
                f"   Applied: {applied}/{len(db_events)} event rows"
                + (f"  ({skipped_partial} partial skipped — re-run with --apply-partial to include)"
                   if skipped_partial else "")
            )

    # ── Final summary ─────────────────────────────────────────────
    print("\n" + "═" * 62)
    print(f"FTL DISCOVERY {'(DRY-RUN — nothing written)' if not args.apply else '(APPLIED)'}")
    print("─" * 62)
    print(f"  Tournaments matched:     {totals['t_matched']}")
    print(f"  Tournaments not found:   {totals['t_failed']}")
    print(f"  Events matched (exact):  {totals['e_matched']}")
    print(f"  Events matched (partial):{totals['e_partial']}")
    print(f"  Events unmatched:        {totals['e_failed']}")

    if unmatched_rows:
        csv_path = Path(args.csv)
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(unmatched_rows[0].keys()))
            writer.writeheader()
            writer.writerows(unmatched_rows)
        n_verify  = sum(1 for r in unmatched_rows if r["review_action"].startswith("VERIFY"))
        n_manual  = sum(1 for r in unmatched_rows if r["review_action"].startswith("MANUAL"))
        print(f"\n  Review CSV → {csv_path}")
        if n_verify:
            print(f"    VERIFY rows ({n_verify}): partial matches — check 'proposed_ftl_event'.")
            print(f"    If correct, re-run with --apply --apply-partial.")
        if n_manual:
            print(f"    MANUAL rows ({n_manual}): no auto-match found.")
            print(f"    Fill in 'proposed_ftl_event_id' and run: UPDATE events SET ftl_event_id=… WHERE …")

    if not args.apply:
        print("\n  ▶  Run with --apply to write exact matches to the database.")
    print("═" * 62)


if __name__ == "__main__":
    main()
