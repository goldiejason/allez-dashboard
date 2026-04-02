"""
Metrics Calculator — computes all analytics from Supabase data.

All metrics derive from real bout data. No proxy fallbacks.
If data is absent, the metric is None (not a default or estimate).

Called by the Streamlit app to build the athlete's analytics profile.
"""

import statistics
from collections import Counter, defaultdict
from typing import Optional
from database.client import get_read_client


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
        .order("date", desc=True, nullslast=True)
        .limit(10000)
        .execute()
    )
    return res.data or []


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
    # Sort in Python — PostgREST can't order by embedded resource columns
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


# ─────────────────────────────────────────────────────────────────
# Core metrics
# ─────────────────────────────────────────────────────────────────

def calc_pool_metrics(pool_bouts: list[dict]) -> dict:
    """
    Pool performance metrics from individual bout records.

    Returns:
      pool_win_pct, total_pool_bouts, ts_total, tr_total,
      touch_diff, touch_diff_per_bout,
      big_loss_rate (bouts lost by 3+ touches as % of losses),
      close_win_rate (bouts won by 1-2 touches as % of wins)
    """
    if not pool_bouts:
        return {}

    wins   = [b for b in pool_bouts if b["result"]]
    losses = [b for b in pool_bouts if not b["result"]]

    ts_total = sum(b["ts"] for b in pool_bouts)
    tr_total = sum(b["tr"] for b in pool_bouts)

    big_losses  = [b for b in losses if (b["tr"] - b["ts"]) >= 3]
    close_wins  = [b for b in wins   if (b["ts"] - b["tr"]) <= 2]

    return {
        "pool_win_pct":        round(len(wins) / len(pool_bouts) * 100, 1),
        "total_pool_bouts":    len(pool_bouts),
        "ts_total":            ts_total,
        "tr_total":            tr_total,
        "touch_diff":          ts_total - tr_total,
        "touch_diff_per_bout": round((ts_total - tr_total) / len(pool_bouts), 2),
        "big_loss_rate":       round(len(big_losses) / len(losses) * 100, 1) if losses else None,
        "close_win_rate":      round(len(close_wins) / len(wins)   * 100, 1) if wins else None,
    }


def calc_de_metrics(de_bouts: list[dict]) -> dict:
    """DE performance metrics."""
    if not de_bouts:
        return {}

    wins   = [b for b in de_bouts if b["result"]]
    losses = [b for b in de_bouts if not b["result"]]

    return {
        "de_win_pct":       round(len(wins) / len(de_bouts) * 100, 1),
        "total_de_bouts":   len(de_bouts),
        "de_ts_total":      sum(b["ts"] for b in de_bouts),
        "de_tr_total":      sum(b["tr"] for b in de_bouts),
    }


def calc_monthly_performance(pool_bouts: list[dict]) -> dict:
    """
    Pool W/L and touch differential grouped by calendar month.
    Returns: {month_num_str: {W, L, ts, tr, win_pct, touch_diff}}
    """
    mmap = defaultdict(lambda: {"W": 0, "L": 0, "ts": 0, "tr": 0})

    for bout in pool_bouts:
        date_str = bout.get("events", {}).get("date") if isinstance(bout.get("events"), dict) else None
        if not date_str:
            continue
        try:
            month = str(int(date_str[5:7]))  # "YYYY-MM-DD" → "M"
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


def calc_rivals(pool_bouts: list[dict], de_bouts: list[dict], min_encounters: int = 2) -> list[dict]:
    """
    Opponents encountered 2+ times across career (pool + DE combined).
    Returns list sorted by encounter count (most common first).
    """
    all_bouts = pool_bouts + de_bouts
    counter = Counter(b["opponent_name"] for b in all_bouts)

    rivals = []
    for name, total in counter.most_common():
        if total < min_encounters:
            continue
        opp_bouts = [b for b in all_bouts if b["opponent_name"] == name]
        wins = sum(1 for b in opp_bouts if b["result"])
        ts   = sum(b["ts"] for b in opp_bouts)
        tr   = sum(b["tr"] for b in opp_bouts)
        rivals.append({
            "name":     name,
            "total":    total,
            "wins":     wins,
            "losses":   total - wins,
            "win_pct":  round(wins / total * 100, 1),
            "ts":       ts,
            "tr":       tr,
            "touch_diff": ts - tr,
        })
    return rivals


def calc_new_vs_repeat(pool_bouts: list[dict]) -> dict:
    """
    Split pool bout win rate by whether this is the first encounter or a repeat.
    Encounters are tracked chronologically — first occurrence per career = "new".

    Returns: {first_pct, repeat_pct, gap, first_n, repeat_n}
    """
    seen: set[str] = set()
    first_bouts, repeat_bouts = [], []

    # Sort by event date ascending for correct chronological tracking
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

    fp = _win_pct(first_bouts)
    rp = _win_pct(repeat_bouts)
    gap = round(fp - rp, 1) if fp is not None and rp is not None else None

    return {
        "first_pct":  fp,
        "repeat_pct": rp,
        "gap":        gap,
        "first_n":    len(first_bouts),
        "repeat_n":   len(repeat_bouts),
    }


def calc_resilience_score(pool_bouts: list[dict]) -> dict:
    """
    Resilience: win rate in the bout immediately following a loss, within the same event.

    Shows whether an athlete can bounce back after being beaten in a pool.
    A high score (>60%) indicates psychological resilience; a low score (<40%) suggests
    losses have a snowball effect.

    Returns:
      resilience_pct   — % of bouts that follow a loss and are won
      bounce_back_n    — total number of bouts that follow a loss (sample size)
      bounce_back_wins — how many of those were wins
    """
    from collections import defaultdict
    by_event: dict[str, list] = defaultdict(list)
    for bout in pool_bouts:
        by_event[bout["event_id"]].append(bout)

    bounce_backs: list[bool] = []
    for bouts in by_event.values():
        ordered = sorted(bouts, key=lambda b: b.get("bout_order", 0))
        for i, bout in enumerate(ordered[:-1]):
            if not bout["result"]:                    # this bout was a loss
                bounce_backs.append(ordered[i + 1]["result"])

    if not bounce_backs:
        return {}

    return {
        "resilience_pct":   round(sum(bounce_backs) / len(bounce_backs) * 100, 1),
        "bounce_back_n":    len(bounce_backs),
        "bounce_back_wins": sum(bounce_backs),
    }


def calc_volatility(events: list[dict], pool_bouts: list[dict]) -> dict:
    """
    Event-to-event volatility: standard deviation of pool win % per event.
    Returns: {career_sd, recent_sd (last 5 events)}
    """
    # Build per-event pool win %
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
        "career_sd": round(statistics.stdev(event_rates), 1),
        "recent_sd": round(statistics.stdev(event_rates[-5:]), 1) if len(event_rates) >= 5 else None,
        "event_win_rates": event_rates,
    }


def calc_trend(events: list[dict], pool_bouts: list[dict]) -> dict:
    """
    Simple trend: compare pool win % in last 3 events vs prior 3 events.
    Returns: {direction: "up"/"down"/"stable", delta}
    """
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
        "direction": "up" if delta >= 3 else ("down" if delta <= -3 else "stable"),
        "delta":     delta,
        "recent_avg": round(recent, 1),
        "prior_avg":  round(prior, 1),
    }


# ─────────────────────────────────────────────────────────────────
# Event-level pool aggregation (no individual bout data required)
# ─────────────────────────────────────────────────────────────────

def calc_event_pool_metrics(events: list[dict]) -> dict:
    """
    Aggregate pool stats from event-level columns (pool_v, pool_l, pool_ts, pool_tr).
    Used when individual pool_bouts are not yet collected (Phase 1 data).

    Returns a subset of the pool metrics dict so the dashboard UI is consistent.
    """
    valid = [e for e in events if e.get("pool_v") is not None]
    if not valid:
        return {}

    # Use .get() with 0-fallback: pool_v filters to non-null, but pool_l / pool_ts /
    # pool_tr could be null in partially-ingested rows.  Direct e["key"] access
    # raises TypeError when the value is None; `or 0` is the correct neutral element.
    total_v  = sum((e.get("pool_v")  or 0) for e in valid)
    total_l  = sum((e.get("pool_l")  or 0) for e in valid)
    total_ts = sum((e.get("pool_ts") or 0) for e in valid)
    total_tr = sum((e.get("pool_tr") or 0) for e in valid)
    total_bouts = total_v + total_l

    advanced = sum(1 for e in valid if e.get("advanced_to_de"))

    return {
        "pool_win_pct":         round(total_v / total_bouts * 100, 1) if total_bouts else None,
        "total_pool_bouts":     total_bouts,
        "ts_total":             total_ts,
        "tr_total":             total_tr,
        "touch_diff":           total_ts - total_tr,
        "touch_diff_per_bout":  round((total_ts - total_tr) / total_bouts, 2) if total_bouts else None,
        "events_with_pool":     len(valid),
        "advanced_to_de_count": advanced,
        "advanced_to_de_pct":   round(advanced / len(valid) * 100, 1) if valid else None,
    }


# ─────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────

def calc_all_metrics(athlete_id: str) -> dict:
    """
    Compute the full analytics profile for an athlete.
    Returns a single dict consumed by the Streamlit dashboard.
    """
    athlete      = fetch_athlete(athlete_id)
    events       = fetch_events(athlete_id)
    pool_bouts   = fetch_pool_bouts(athlete_id)
    de_bouts     = fetch_de_bouts(athlete_id)
    annual_stats = fetch_annual_stats(athlete_id)

    has_pool_data = len(pool_bouts) > 0
    has_de_data   = len(de_bouts) > 0

    return {
        "athlete":       athlete,
        "events":        events,
        "annual_stats":  annual_stats,
        "has_pool_data": has_pool_data,
        "has_de_data":   has_de_data,

        # Core metrics — None if no data
        "pool":       calc_pool_metrics(pool_bouts)   if has_pool_data else {},
        "de":         calc_de_metrics(de_bouts)        if has_de_data   else {},
        "month_stats": calc_monthly_performance(pool_bouts) if has_pool_data else {},
        "rivals":     calc_rivals(pool_bouts, de_bouts) if (has_pool_data or has_de_data) else [],
        "nvr":        calc_new_vs_repeat(pool_bouts)   if has_pool_data else {},
        "volatility":  calc_volatility(events, pool_bouts)    if has_pool_data else {},
        "trend":       calc_trend(events, pool_bouts)        if has_pool_data else {},
        "resilience":  calc_resilience_score(pool_bouts)     if has_pool_data else {},

        # Raw bouts available for detailed views
        "pool_bouts": pool_bouts,
        "de_bouts":   de_bouts,
    }
