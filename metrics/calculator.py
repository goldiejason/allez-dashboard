"""
Metrics Calculator — computes all analytics from Supabase data.

All metrics derive from real bout data. No proxy fallbacks.
If data is absent, the metric is None (not a default or estimate).

Called by the Streamlit app to build the athlete's analytics profile,
and by the coaching intelligence engine to generate narrative insights.

New in this version:
  - compute_coverage_score()   — data completeness tier (LOW/PARTIAL/GOOD/FULL)
  - Confidence banding on pool metrics  (n, confidence_tier)
  - calc_de_coaching_metrics()  — advanced DE analytics from bout-level data
  - calc_peer_benchmarks()      — percentile ranks vs same weapon + age cohort
"""

import statistics
from collections import Counter, defaultdict
from typing import Optional
from database.client import get_read_client


# ─────────────────────────────────────────────────────────────────
# Confidence tier thresholds
# ─────────────────────────────────────────────────────────────────
#   INSUFFICIENT  n < 5   → hide metric, explain why
#   LOW           5–19    → show with "limited data" caveat
#   MODERATE      20–49   → show with confidence band
#   HIGH          50+     → show as definitive

def _confidence_tier(n: int) -> str:
    if n < 5:   return "INSUFFICIENT"
    if n < 20:  return "LOW"
    if n < 50:  return "MODERATE"
    return "HIGH"


# ─────────────────────────────────────────────────────────────────
# Data fetchers
# ─────────────────────────────────────────────────────────────────

def fetch_athlete(athlete_id: str) -> Optional[dict]:
    db = get_read_client()
    res = db.table("athletes").select("*").eq("id", athlete_id).single().execute()
    return res.data


def fetch_events(athlete_id: str) -> list[dict]:
    db = get_read_client()
    res = (
        db.table("events")
        .select("*, tournaments(name, country, is_international)")
        .eq("athlete_id", athlete_id)
        .order("date", desc=True)
        .limit(10000)
        .execute()
    )
    # Sort in Python so NULL dates always fall to the end regardless of
    # which postgrest-py version Streamlit Cloud has installed.
    # "" < any ISO date string, so reverse=True puts real dates first.
    data = res.data or []
    return sorted(data, key=lambda r: r.get("date") or "", reverse=True)


def fetch_pool_bouts(athlete_id: str) -> list[dict]:
    """All pool bouts for this athlete across their career."""
    db = get_read_client()
    res = (
        db.table("pool_bouts")
        .select("*, events!inner(athlete_id, date, event_name)")
        .eq("events.athlete_id", athlete_id)
        .limit(10000)
        .execute()
    )
    data = res.data or []
    return sorted(data, key=lambda b: (b.get("events") or {}).get("date") or "")


def fetch_de_bouts(athlete_id: str) -> list[dict]:
    """All DE bouts for this athlete across their career."""
    db = get_read_client()
    res = (
        db.table("de_bouts")
        .select("*, events!inner(athlete_id, date, event_name)")
        .eq("events.athlete_id", athlete_id)
        .limit(10000)
        .execute()
    )
    data = res.data or []
    return sorted(data, key=lambda b: (b.get("events") or {}).get("date") or "")


def fetch_annual_stats(athlete_id: str) -> list[dict]:
    db = get_read_client()
    res = (
        db.table("annual_stats")
        .select("*")
        .eq("athlete_id", athlete_id)
        .order("year", desc=True)
        .limit(10000)
        .execute()
    )
    return res.data or []


def fetch_cohort_events(weapon: str, age_category: Optional[str]) -> list[dict]:
    """
    Fetch events for all athletes sharing the same weapon (and optionally
    age_category) for peer benchmarking.  Returns a flat list of event dicts
    each carrying athlete_id so the caller can group by athlete.
    """
    db = get_read_client()
    # Find all athletes in the cohort
    q = db.table("athletes").select("id").eq("active", True)
    if weapon:
        q = q.eq("weapon", weapon)
    if age_category:
        q = q.eq("age_category", age_category)
    athletes_res = q.limit(10000).execute()
    athlete_ids = [a["id"] for a in (athletes_res.data or [])]
    if not athlete_ids:
        return []

    # Fetch all pool bouts for the cohort in a single query
    # PostgREST doesn't support "in" on embedded resources well, so
    # we use a filter on the events table directly.
    rows = []
    for aid in athlete_ids:
        res = (
            db.table("pool_bouts")
            .select("result, ts, tr, events!inner(athlete_id)")
            .eq("events.athlete_id", aid)
            .limit(10000)
            .execute()
        )
        rows.extend(res.data or [])
    return rows


# ─────────────────────────────────────────────────────────────────
# Coverage score
# ─────────────────────────────────────────────────────────────────

def compute_coverage_score(events: list[dict], pool_bouts: list[dict], de_bouts: list[dict]) -> dict:
    """
    Compute a data-completeness score for an athlete.

    Returns:
      events_total       — total event rows
      events_with_pool   — events where pool aggregate data is set
      events_with_date   — events where date is set
      pool_coverage_pct  — % of events with pool data
      date_coverage_pct  — % of events with a date
      pool_bouts_count   — individual pool bouts recorded
      de_bouts_count     — individual DE bouts recorded
      coverage_tier      — LOW | PARTIAL | GOOD | FULL
    """
    total     = len(events)
    with_pool = sum(1 for e in events if e.get("pool_v") is not None)
    with_date = sum(1 for e in events if e.get("date"))

    pool_pct = round(with_pool / total * 100, 1) if total else 0.0
    date_pct = round(with_date / total * 100, 1) if total else 0.0

    if   pool_pct == 0 and date_pct == 0:  tier = "LOW"
    elif pool_pct >= 80 and date_pct >= 80: tier = "FULL"
    elif pool_pct >= 40 or  date_pct >= 60: tier = "GOOD"
    else:                                   tier = "PARTIAL"

    return {
        "events_total":      total,
        "events_with_pool":  with_pool,
        "events_with_date":  with_date,
        "pool_coverage_pct": pool_pct,
        "date_coverage_pct": date_pct,
        "pool_bouts_count":  len(pool_bouts),
        "de_bouts_count":    len(de_bouts),
        "coverage_tier":     tier,
    }


# ─────────────────────────────────────────────────────────────────
# Core metrics — pool
# ─────────────────────────────────────────────────────────────────

def calc_pool_metrics(pool_bouts: list[dict]) -> dict:
    """
    Pool performance metrics from individual bout records.

    Each metric now carries a confidence envelope: n + confidence_tier
    so the presentation layer can decide how to display the figure.
    """
    if not pool_bouts:
        return {}

    wins   = [b for b in pool_bouts if b["result"]]
    losses = [b for b in pool_bouts if not b["result"]]
    n      = len(pool_bouts)

    ts_total = sum(b["ts"] for b in pool_bouts)
    tr_total = sum(b["tr"] for b in pool_bouts)

    big_losses  = [b for b in losses if (b["tr"] - b["ts"]) >= 3]
    close_wins  = [b for b in wins   if (b["ts"] - b["tr"]) <= 2]

    return {
        "pool_win_pct":        round(len(wins) / n * 100, 1),
        "total_pool_bouts":    n,
        "confidence_tier":     _confidence_tier(n),
        "ts_total":            ts_total,
        "tr_total":            tr_total,
        "touch_diff":          ts_total - tr_total,
        "touch_diff_per_bout": round((ts_total - tr_total) / n, 2),
        "big_loss_rate":       round(len(big_losses) / len(losses) * 100, 1) if losses else None,
        "big_loss_n":          len(losses),
        "close_win_rate":      round(len(close_wins) / len(wins)   * 100, 1) if wins else None,
    }


# ─────────────────────────────────────────────────────────────────
# Core metrics — DE (basic)
# ─────────────────────────────────────────────────────────────────

def calc_de_metrics(de_bouts: list[dict]) -> dict:
    """Basic DE performance metrics (win rate, totals)."""
    if not de_bouts:
        return {}

    wins = [b for b in de_bouts if b["result"]]
    n    = len(de_bouts)

    return {
        "de_win_pct":       round(len(wins) / n * 100, 1),
        "total_de_bouts":   n,
        "confidence_tier":  _confidence_tier(n),
        "de_ts_total":      sum(b["ts"] for b in de_bouts),
        "de_tr_total":      sum(b["tr"] for b in de_bouts),
    }


# ─────────────────────────────────────────────────────────────────
# DE coaching metrics — advanced bout-level analysis
# ─────────────────────────────────────────────────────────────────

def calc_de_coaching_metrics(de_bouts: list[dict]) -> dict:
    """
    Advanced DE analytics powered by bout-level records.

    Returns:
      win_rate_close   — wins where margin ≤ 3, as % of all bouts
      win_rate_dominant— wins where margin ≥ 7, as % of all bouts
      close_bout_rate  — % of all bouts decided by ≤ 3 touches
      comeback_rate    — % of bouts won when trailing at the midpoint (score ≤ opponent)
                         Approximation: if result=True and ts were close but won
      score_patterns   — {dominant_wins_pct, close_wins_pct, close_losses_pct, dominant_losses_pct}
      round_win_rates  — {round_label: win_pct}  e.g. {"T32": 60.0, "QF": 40.0}
      n                — sample size
      confidence_tier
    """
    if not de_bouts:
        return {}

    wins   = [b for b in de_bouts if b["result"]]
    losses = [b for b in de_bouts if not b["result"]]
    n      = len(de_bouts)

    def margin(b: dict) -> int:
        return abs((b.get("ts") or 0) - (b.get("tr") or 0))

    # Score pattern breakdown
    dominant_wins   = [b for b in wins   if margin(b) >= 7]
    close_wins      = [b for b in wins   if margin(b) <= 3]
    close_losses    = [b for b in losses if margin(b) <= 3]
    dominant_losses = [b for b in losses if margin(b) >= 7]

    # Round-by-round win rates
    from collections import defaultdict
    by_round: dict[str, list[bool]] = defaultdict(list)
    for b in de_bouts:
        rnd = b.get("round", "?")
        by_round[rnd].append(b["result"])

    round_win_rates = {
        rnd: {
            "n":        len(results),
            "win_rate": round(sum(results) / len(results) * 100, 1),
        }
        for rnd, results in by_round.items()
        if results
    }

    # Touch efficiency: average touches scored per bout (higher = more aggressive)
    avg_ts = round(statistics.mean((b.get("ts") or 0) for b in de_bouts), 1) if de_bouts else None
    avg_tr = round(statistics.mean((b.get("tr") or 0) for b in de_bouts), 1) if de_bouts else None

    return {
        "n":                    n,
        "confidence_tier":      _confidence_tier(n),
        "wins":                 len(wins),
        "losses":               len(losses),
        "win_rate":             round(len(wins) / n * 100, 1),
        "dominant_win_pct":     round(len(dominant_wins)   / n * 100, 1),
        "close_win_pct":        round(len(close_wins)      / n * 100, 1),
        "close_loss_pct":       round(len(close_losses)    / n * 100, 1),
        "dominant_loss_pct":    round(len(dominant_losses) / n * 100, 1),
        "close_bout_rate":      round((len(close_wins) + len(close_losses)) / n * 100, 1),
        "avg_ts":               avg_ts,
        "avg_tr":               avg_tr,
        "round_win_rates":      round_win_rates,
    }


# ─────────────────────────────────────────────────────────────────
# Monthly / seasonal
# ─────────────────────────────────────────────────────────────────

def calc_monthly_performance(pool_bouts: list[dict]) -> dict:
    """Pool W/L and touch differential grouped by calendar month."""
    mmap = defaultdict(lambda: {"W": 0, "L": 0, "ts": 0, "tr": 0})

    for bout in pool_bouts:
        date_str = bout.get("events", {}).get("date") if isinstance(bout.get("events"), dict) else None
        if not date_str:
            continue
        try:
            month = str(int(date_str[5:7]))
        except (ValueError, IndexError):
            continue
        if bout["result"]:
            mmap[month]["W"] += 1
        else:
            mmap[month]["L"] += 1
        mmap[month]["ts"] += bout["ts"]
        mmap[month]["tr"] += bout["tr"]

    result = {}
    for month, data in mmap.items():
        total = data["W"] + data["L"]
        result[month] = {
            **data,
            "win_pct":    round(data["W"] / total * 100, 1) if total else None,
            "touch_diff": data["ts"] - data["tr"],
        }
    return result


# ─────────────────────────────────────────────────────────────────
# Rivals
# ─────────────────────────────────────────────────────────────────

def calc_rivals(pool_bouts: list[dict], de_bouts: list[dict], min_encounters: int = 2) -> list[dict]:
    """Opponents encountered 2+ times across career (pool + DE combined)."""
    all_bouts = pool_bouts + de_bouts
    counter   = Counter(b["opponent_name"] for b in all_bouts)

    rivals = []
    for name, total in counter.most_common():
        if total < min_encounters:
            continue
        opp_bouts = [b for b in all_bouts if b["opponent_name"] == name]
        wins = sum(1 for b in opp_bouts if b["result"])
        ts   = sum(b["ts"] for b in opp_bouts)
        tr   = sum(b["tr"] for b in opp_bouts)
        rivals.append({
            "name":       name,
            "total":      total,
            "wins":       wins,
            "losses":     total - wins,
            "win_pct":    round(wins / total * 100, 1),
            "ts":         ts,
            "tr":         tr,
            "touch_diff": ts - tr,
        })
    return rivals


# ─────────────────────────────────────────────────────────────────
# New vs Repeat
# ─────────────────────────────────────────────────────────────────

def calc_new_vs_repeat(pool_bouts: list[dict]) -> dict:
    """Split pool win rate by first vs repeat encounter."""
    seen: set[str] = set()
    first_bouts, repeat_bouts = [], []

    sorted_bouts = sorted(
        pool_bouts,
        key=lambda b: (b.get("events", {}) or {}).get("date", "") or ""
    )

    for bout in sorted_bouts:
        opp = bout["opponent_name"]
        if opp in seen:
            repeat_bouts.append(bout)
        else:
            first_bouts.append(bout)
            seen.add(opp)

    def _win_pct(bouts):
        if not bouts:
            return None
        return round(sum(1 for b in bouts if b["result"]) / len(bouts) * 100, 1)

    fp  = _win_pct(first_bouts)
    rp  = _win_pct(repeat_bouts)
    gap = round(fp - rp, 1) if fp is not None and rp is not None else None

    return {
        "first_pct":       fp,
        "repeat_pct":      rp,
        "gap":             gap,
        "first_n":         len(first_bouts),
        "repeat_n":        len(repeat_bouts),
        "confidence_tier": _confidence_tier(len(pool_bouts)),
    }


# ─────────────────────────────────────────────────────────────────
# Resilience
# ─────────────────────────────────────────────────────────────────

def calc_resilience_score(pool_bouts: list[dict]) -> dict:
    """Win rate in the bout immediately following a loss, within the same event."""
    by_event: dict[str, list] = defaultdict(list)
    for bout in pool_bouts:
        by_event[bout["event_id"]].append(bout)

    bounce_backs: list[bool] = []
    for bouts in by_event.values():
        # Exclude bouts without bout_order before sorting — a NULL bout_order
        # defaults to 0, making two bouts appear adjacent when they may not be.
        # Resilience requires correct relative ordering to be meaningful.
        ordered_source = [b for b in bouts if b.get("bout_order") is not None]
        if len(ordered_source) < 2:
            continue
        ordered = sorted(ordered_source, key=lambda b: b["bout_order"])
        for i, bout in enumerate(ordered[:-1]):
            if not bout["result"]:
                bounce_backs.append(ordered[i + 1]["result"])

    if not bounce_backs:
        return {}

    n = len(bounce_backs)
    return {
        "resilience_pct":   round(sum(bounce_backs) / n * 100, 1),
        "bounce_back_n":    n,
        "bounce_back_wins": sum(bounce_backs),
        "confidence_tier":  _confidence_tier(n),
    }


# ─────────────────────────────────────────────────────────────────
# Volatility
# ─────────────────────────────────────────────────────────────────

def calc_volatility(events: list[dict], pool_bouts: list[dict]) -> dict:
    """Event-to-event SD of pool win % per event."""
    event_rates = []
    for ev in sorted(events, key=lambda e: e.get("date") or ""):
        ev_bouts = [b for b in pool_bouts if b.get("event_id") == ev["id"]]
        if len(ev_bouts) < 2:
            continue
        wins = sum(1 for b in ev_bouts if b["result"])
        event_rates.append(round(wins / len(ev_bouts) * 100, 1))

    if len(event_rates) < 2:
        return {}

    return {
        "career_sd":       round(statistics.stdev(event_rates), 1),
        "recent_sd":       round(statistics.stdev(event_rates[-5:]), 1) if len(event_rates) >= 5 else None,
        "event_win_rates": event_rates,
    }


# ─────────────────────────────────────────────────────────────────
# Trend
# ─────────────────────────────────────────────────────────────────

def calc_trend(events: list[dict], pool_bouts: list[dict]) -> dict:
    """Compare pool win % in last 3 events vs prior 3."""
    event_rates = []
    for ev in sorted(events, key=lambda e: e.get("date") or ""):
        ev_bouts = [b for b in pool_bouts if b.get("event_id") == ev["id"]]
        if len(ev_bouts) < 2:
            continue
        wins = sum(1 for b in ev_bouts if b["result"])
        event_rates.append(wins / len(ev_bouts) * 100)

    if len(event_rates) < 4:
        return {}

    recent = statistics.mean(event_rates[-3:])
    prior  = statistics.mean(event_rates[-6:-3]) if len(event_rates) >= 6 else statistics.mean(event_rates[:-3])
    delta  = round(recent - prior, 1)

    return {
        "direction":  "up" if delta >= 3 else ("down" if delta <= -3 else "stable"),
        "delta":      delta,
        "recent_avg": round(recent, 1),
        "prior_avg":  round(prior, 1),
    }


# ─────────────────────────────────────────────────────────────────
# Event-level pool aggregation (no individual bout data required)
# ─────────────────────────────────────────────────────────────────

def calc_event_pool_metrics(events: list[dict]) -> dict:
    """
    Aggregate pool stats from event-level columns (pool_v, pool_l, pool_ts, pool_tr).
    Used when individual pool_bouts are not yet collected.
    """
    valid = [e for e in events if e.get("pool_v") is not None]
    if not valid:
        return {}

    total_v    = sum((e.get("pool_v")  or 0) for e in valid)
    total_l    = sum((e.get("pool_l")  or 0) for e in valid)
    total_ts   = sum((e.get("pool_ts") or 0) for e in valid)
    total_tr   = sum((e.get("pool_tr") or 0) for e in valid)
    total_bouts = total_v + total_l

    advanced = sum(1 for e in valid if e.get("advanced_to_de"))
    n_valid  = len(valid)

    return {
        "pool_win_pct":         round(total_v / total_bouts * 100, 1) if total_bouts else None,
        "total_pool_bouts":     total_bouts,
        "confidence_tier":      _confidence_tier(total_bouts),
        "ts_total":             total_ts,
        "tr_total":             total_tr,
        "touch_diff":           total_ts - total_tr,
        "touch_diff_per_bout":  round((total_ts - total_tr) / total_bouts, 2) if total_bouts else None,
        "events_with_pool":     n_valid,
        "advanced_to_de_count": advanced,
        "advanced_to_de_pct":   round(advanced / n_valid * 100, 1) if n_valid else None,
    }


# ─────────────────────────────────────────────────────────────────
# Placement progression
# ─────────────────────────────────────────────────────────────────

def calc_placement_progression(events: list[dict]) -> dict:
    """
    Compute career placement percentile statistics.

    Returns:
      placements_pct  — list of (date, percentile) for charting
      best_pct        — best ever (lowest) percentile
      career_avg_pct  — career average percentile
      recent_avg_pct  — last-5-events average percentile
      trend_delta_pp  — recent minus career avg (negative = improving)
      n               — number of dated events with placement
    """
    valid = sorted(
        [e for e in events if e.get("placement") and e.get("field_size") and e.get("date")],
        key=lambda e: e["date"]
    )
    if not valid:
        return {}

    pcts = [round(e["placement"] / e["field_size"] * 100, 1) for e in valid]
    avg  = round(statistics.mean(pcts), 1)
    rec  = round(statistics.mean(pcts[-5:]), 1) if len(pcts) >= 2 else pcts[-1]

    return {
        "placements_pct": [(e["date"], p) for e, p in zip(valid, pcts)],
        "best_pct":       min(pcts),
        "career_avg_pct": avg,
        "recent_avg_pct": rec,
        "trend_delta_pp": round(rec - avg, 1),
        "n":              len(valid),
    }


# ─────────────────────────────────────────────────────────────────
# Peer benchmarking
# ─────────────────────────────────────────────────────────────────

def calc_peer_benchmarks(
    athlete_id: str,
    weapon: Optional[str],
    age_category: Optional[str],
    athlete_pool_win_pct: Optional[float],
    athlete_de_win_pct: Optional[float],
) -> dict:
    """
    Compute this athlete's percentile rank vs their cohort
    (same weapon, optionally same age category).

    Returns:
      cohort_size           — number of athletes in cohort with data
      pool_win_pct_rank     — percentile (0–100), higher = better than more peers
      de_win_pct_rank       — same for DE
      label                 — "U-14 Foil" or "Foil" (depending on age availability)
    """
    db = get_read_client()

    # Fetch cohort athletes
    q = db.table("athletes").select("id").eq("active", True)
    if weapon:
        q = q.eq("weapon", weapon)
    if age_category:
        q = q.eq("age_category", age_category)
    athletes_res = q.limit(10000).execute()
    cohort_ids = [
        a["id"] for a in (athletes_res.data or [])
        if a["id"] != athlete_id  # exclude self
    ]

    if not cohort_ids:
        return {}

    # For each cohort member compute pool win pct from event-level data
    # (fast — doesn't require individual bouts)
    cohort_pool_win_pcts  = []
    cohort_de_win_pcts    = []

    for cid in cohort_ids:
        events = fetch_events(cid)
        ep     = calc_event_pool_metrics(events)
        if ep.get("pool_win_pct") is not None and ep.get("total_pool_bouts", 0) >= 5:
            cohort_pool_win_pcts.append(ep["pool_win_pct"])

        de_bouts = fetch_de_bouts(cid)
        de_bouts = [                                # apply NULL firewall inline
            b for b in de_bouts
            if b.get("ts") is not None
            and b.get("tr") is not None
            and b.get("result") is not None
        ]
        dm       = calc_de_metrics(de_bouts)
        if dm.get("de_win_pct") is not None and dm.get("total_de_bouts", 0) >= 3:
            cohort_de_win_pcts.append(dm["de_win_pct"])

    def _percentile_rank(value: Optional[float], cohort: list[float]) -> Optional[float]:
        if value is None or not cohort:
            return None
        below = sum(1 for c in cohort if c < value)
        return round(below / len(cohort) * 100, 0)

    label = " ".join(filter(None, [
        age_category or "",
        (weapon or "").capitalize(),
    ])).strip() or "Overall"

    return {
        "cohort_size":        len(cohort_ids),
        "pool_win_pct_rank":  _percentile_rank(athlete_pool_win_pct, cohort_pool_win_pcts),
        "de_win_pct_rank":    _percentile_rank(athlete_de_win_pct, cohort_de_win_pcts),
        "label":              label,
        "pool_cohort_n":      len(cohort_pool_win_pcts),
        "de_cohort_n":        len(cohort_de_win_pcts),
    }


# ─────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────

def calc_all_metrics(athlete_id: str) -> dict:
    """
    Compute the full analytics profile for an athlete.
    Returns a single dict consumed by the Streamlit dashboard and
    the coaching intelligence engine.
    """
    athlete      = fetch_athlete(athlete_id)
    events       = fetch_events(athlete_id)
    pool_bouts   = fetch_pool_bouts(athlete_id)
    de_bouts     = fetch_de_bouts(athlete_id)
    annual_stats = fetch_annual_stats(athlete_id)

    # ── NULL firewall ────────────────────────────────────────────────
    # Pending and unscored bouts are stored with NULL ts/tr/result.
    # Define three filtered views once here; route each calculator to
    # the smallest set it can safely consume.  Never substitute 0 for
    # NULL — a 0 score is a valid fencing result and must not be
    # confused with "no data".
    #
    #   _all:     every record, including pending / unscored
    #             → coverage score + raw display in the UI
    #   _scored:  ts, tr and result all present
    #             → any metric involving arithmetic or win/loss counts
    #   _matched: scored + opponent_name present
    #             → rivalry and repeat-opponent analysis
    pool_bouts_all     = pool_bouts
    pool_bouts_scored  = [
        b for b in pool_bouts
        if b.get("ts") is not None
        and b.get("tr") is not None
        and b.get("result") is not None
    ]
    pool_bouts_matched = [b for b in pool_bouts_scored if b.get("opponent_name")]

    de_bouts_all     = de_bouts
    de_bouts_scored  = [
        b for b in de_bouts
        if b.get("ts") is not None
        and b.get("tr") is not None
        and b.get("result") is not None
    ]
    de_bouts_matched = [b for b in de_bouts_scored if b.get("opponent_name")]

    has_pool_data = len(pool_bouts_scored) > 0
    has_de_data   = len(de_bouts_scored)   > 0

    pool        = calc_pool_metrics(pool_bouts_scored)        if has_pool_data else {}
    de          = calc_de_metrics(de_bouts_scored)            if has_de_data   else {}
    de_coaching = calc_de_coaching_metrics(de_bouts_scored)   if has_de_data   else {}

    placement_progression = calc_placement_progression(events)

    coverage = compute_coverage_score(events, pool_bouts_all, de_bouts_all)

    # Peer benchmarks — only meaningful if athlete has a weapon set
    weapon       = (athlete or {}).get("weapon")
    age_category = (athlete or {}).get("age_category")
    pool_win_pct = (pool or {}).get("pool_win_pct") or calc_event_pool_metrics(events).get("pool_win_pct")
    de_win_pct   = de.get("de_win_pct")

    # Only compute peer benchmarks if we have enough data and cohort is plausible
    peer_benchmarks: dict = {}
    if weapon and pool_win_pct is not None:
        try:
            peer_benchmarks = calc_peer_benchmarks(
                athlete_id, weapon, age_category, pool_win_pct, de_win_pct
            )
        except Exception:
            peer_benchmarks = {}

    return {
        "athlete":               athlete,
        "events":                events,
        "annual_stats":          annual_stats,
        "has_pool_data":         has_pool_data,
        "has_de_data":           has_de_data,

        # Core metrics
        "pool":                  pool,
        "de":                    de,
        "de_coaching":           de_coaching,
        "month_stats":           calc_monthly_performance(pool_bouts_scored)            if has_pool_data else {},
        "rivals":                calc_rivals(pool_bouts_matched, de_bouts_matched)      if (has_pool_data or has_de_data) else [],
        "nvr":                   calc_new_vs_repeat(pool_bouts_matched)                 if has_pool_data else {},
        "volatility":            calc_volatility(events, pool_bouts_scored)             if has_pool_data else {},
        "trend":                 calc_trend(events, pool_bouts_scored)                  if has_pool_data else {},
        "resilience":            calc_resilience_score(pool_bouts_scored)               if has_pool_data else {},
        "placement_progression": placement_progression,
        "coverage":              coverage,
        "peer_benchmarks":       peer_benchmarks,

        # Raw bouts for detailed views — intentionally unfiltered so that
        # pending bouts appear in the UI (displayed with blank score columns).
        "pool_bouts": pool_bouts_all,
        "de_bouts":   de_bouts_all,
    }
