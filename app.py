"""
Allez Fencing Dashboard — main Streamlit app.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timezone

from database.client import get_read_client
from metrics.calculator import calc_all_metrics
from collectors.ftl_collector import collect_athlete
from collectors.ukratings_collector import collect_athlete as collect_athlete_ukratings
from intelligence.engine import CoachingEngine

st.set_page_config(
    page_title="Allez Fencing Dashboard",
    page_icon="🤺",
    layout="wide",
)


# ─── Display helpers ────────────────────────────────────────────
def _coerce(v, fmt="{}"):
    """
    Safely format a value for display, returning '—' when v is None.

    Unlike the `v or '—'` idiom, this correctly distinguishes None from zero:
      _coerce(0, "{}%")  → "0%"    (zero is a valid metric, not missing data)
      _coerce(None, "{}%") → "—"   (no data available)

    Args:
        v:   The value to format (int, float, str, or None).
        fmt: A str.format-style template, e.g. "{}%" or "{:+.1f}".
    """
    return fmt.format(v) if v is not None else "—"


# ═══════════════════════════════════════════════════════════════
# Render functions — defined before use
# ═══════════════════════════════════════════════════════════════

def _render_event_history(events: list[dict]):
    if not events:
        st.info("No event history available.")
        return

    # ── Build table ──────────────────────────────────────────────
    rows = []
    for ev in sorted(events, key=lambda e: e.get("date") or "", reverse=True):
        t = ev.get("tournaments") or {}
        flag = "🌍" if t.get("is_international") else "🇬🇧"
        pool_pct = None
        if ev.get("pool_v") is not None and (ev.get("pool_v", 0) + (ev.get("pool_l") or 0)) > 0:
            bouts = (ev.get("pool_v") or 0) + (ev.get("pool_l") or 0)
            pool_pct = f"{ev['pool_v']}/{bouts}  ({round(ev['pool_v']/bouts*100)}%)"
        place = ev.get("placement")
        field = ev.get("field_size")
        place_str = f"{place}/{field}" if place and field else (str(place) if place else "—")
        rows.append({
            "Date":        ev.get("date") or "—",
            "Tournament":  t.get("name", "—"),
            "Event":       ev.get("event_name", "—"),
            "Ctry":        flag,
            "Place":       place_str,
            "Pool V/L":    pool_pct or "—",
            "TS–TR":       (
                f"{ev['pool_ts']}–{ev['pool_tr']}"
                if (ev.get("pool_ts") is not None and ev.get("pool_tr") is not None)
                else "—"
            ),
            "Ind":         _coerce(ev.get("pool_ind"), "{:+d}"),
            "→DE":         "✅" if ev.get("advanced_to_de") else ("❌" if ev.get("pool_v") is not None else "—"),
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, hide_index=True, use_container_width=True)

    # ── Placement trend chart ────────────────────────────────────
    # Exclude rows with unknown date or placement — "—" strings as Plotly x-axis
    # labels collapse the time axis into an unintelligible categorical sequence.
    chart_rows = [
        r for r in rows
        if r["Date"] != "—" and r["Place"] != "—" and "/" in r["Place"]
    ]
    if len(chart_rows) >= 3:
        dates   = [r["Date"] for r in chart_rows]
        # compute percentile rank (lower = better)
        def _pct(place_str):
            parts = place_str.split("/")
            return round(int(parts[0]) / int(parts[1]) * 100, 1)
        pcts    = [_pct(r["Place"]) for r in chart_rows]
        labels  = [r["Tournament"].split("–")[0].strip()[:25] for r in chart_rows]

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates, y=pcts,
            mode="lines+markers",
            marker=dict(size=9, color="#1f77b4"),
            line=dict(color="#1f77b4", width=2),
            hovertext=[f"{r['Tournament']}<br>{r['Event']}<br>Place {r['Place']}" for r in chart_rows],
            hoverinfo="text",
            name="Placement %ile",
        ))
        fig.update_layout(
            title="Placement Percentile Over Time (lower = better)",
            yaxis_title="Percentile rank (%)",
            yaxis=dict(autorange="reversed", range=[0, 105]),
            xaxis_title="",
            height=320,
            margin=dict(t=45, b=30),
        )
        st.plotly_chart(fig, use_container_width=True)


def _render_annual_stats(annual_stats: list[dict]):
    if not annual_stats:
        return
    df = pd.DataFrame(annual_stats)[["year", "pool_w", "pool_l", "de_w", "de_l"]]
    df.columns = ["Year", "Pool W", "Pool L", "DE W", "DE L"]
    st.dataframe(df, hide_index=True)


def _render_pool_tab(pool: dict, pool_bouts: list, volatility: dict, resilience: dict, events: list[dict]):
    if not pool:
        # Fall back to event-level pool data
        from metrics.calculator import calc_event_pool_metrics
        event_pool = calc_event_pool_metrics(events)
        if not event_pool:
            st.info("No pool data available yet.")
            return
        st.caption("_Aggregated from event pool totals — individual bout breakdown available after Phase 2._")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Pool Win %",        _coerce(event_pool.get("pool_win_pct"),        "{}%"))
        c2.metric("Touch Diff",         _coerce(event_pool.get("touch_diff")))
        c3.metric("Touch Diff / Bout",  _coerce(event_pool.get("touch_diff_per_bout")))
        c4.metric("Advanced to DE %",  _coerce(event_pool.get("advanced_to_de_pct"),  "{}%"))

        # Pool win% per event bar chart
        valid = sorted(
            [e for e in events if e.get("pool_v") is not None],
            key=lambda e: e.get("date") or ""
        )
        if valid:
            labels  = [e.get("tournaments", {}).get("name", "")[:20] if e.get("tournaments") else e.get("event_name","")[:20] for e in valid]
            bouts   = [(e.get("pool_v") or 0) + (e.get("pool_l") or 0) for e in valid]
            win_pct = [round(e["pool_v"] / b * 100, 1) if b else 0 for e, b in zip(valid, bouts)]
            fig = go.Figure(go.Bar(
                x=list(range(len(labels))), y=win_pct,
                text=[f"{w}%" for w in win_pct],
                textposition="outside",
                marker_color=["#2ca02c" if w >= 50 else "#d62728" for w in win_pct],
                hovertext=labels,
                hoverinfo="text+y",
            ))
            fig.update_layout(
                title="Pool Win % per Event",
                yaxis_title="Win %", yaxis_range=[0, 110],
                xaxis=dict(tickvals=list(range(len(labels))), ticktext=labels, tickangle=-40),
                height=380, margin=dict(t=45, b=120),
            )
            st.plotly_chart(fig, use_container_width=True)
        return

    # ── Individual bout-level data available ────────────────────
    pool_n    = pool.get("total_pool_bouts", 0)
    pool_tier = pool.get("confidence_tier", "")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Win %",               _coerce(pool.get("pool_win_pct"),        "{}%"),
              help=f"Based on {pool_n} recorded pool bouts ({pool_tier.lower()} confidence)")
    c2.metric("Touch Diff / Bout",   _coerce(pool.get("touch_diff_per_bout")),
              help=f"Average (TS − TR) per bout · {pool_n} bouts")
    c3.metric("Big Loss Rate",        f"{pool.get('big_loss_rate')}%" if pool.get('big_loss_rate') is not None else "—",
              help="% of losses conceded by 3+ touches. Calculated from recorded bout-level data only.")
    res_pct  = resilience.get("resilience_pct") if resilience else None
    res_n    = resilience.get("bounce_back_n", 0) if resilience else 0
    res_tier = resilience.get("confidence_tier", "") if resilience else ""
    c4.metric("Resilience Score",     f"{res_pct}%" if res_pct is not None else "—",
              help=f"Win rate immediately following a loss · {res_n} qualifying bouts ({res_tier.lower()} confidence). >60% = strong bounce-back.")

    # Sample-size caveat — displayed prominently when data is sparse
    if pool_tier in ("INSUFFICIENT", "LOW"):
        st.warning(
            f"⚠️  **Low sample size** — only {pool_n} pool bouts recorded "
            f"({pool_tier.lower()} confidence). "
            "Metrics above should be treated as indicative only. "
            "Run a full data refresh to improve coverage."
        )

    if volatility:
        recent_sd = volatility.get("recent_sd")
        recent_sd_str = (
            f"SD {recent_sd}% recent 5 events"
            if recent_sd is not None
            else "fewer than 5 scored events"
        )
        st.caption(
            f"Consistency: SD {_coerce(volatility.get('career_sd'))}% career · {recent_sd_str}"
        )

    # ── Per-event touch diff chart ───────────────────────────────
    from collections import defaultdict
    ev_map: dict = defaultdict(lambda: {"ts": 0, "tr": 0, "date": "", "name": ""})
    for b in pool_bouts:
        ev = b.get("events") or {}
        eid = b["event_id"]
        ev_map[eid]["ts"]   += (b["ts"] or 0)
        ev_map[eid]["tr"]   += (b["tr"] or 0)
        ev_map[eid]["date"]  = ev.get("date") or ""
        ev_map[eid]["name"]  = ev.get("event_name", eid[:8])

    ev_sorted = sorted(ev_map.values(), key=lambda x: x["date"])
    if len(ev_sorted) >= 2:
        ev_labels = [e["name"][:22] for e in ev_sorted]
        ev_diff   = [e["ts"] - e["tr"] for e in ev_sorted]
        colors    = ["#2ca02c" if d > 0 else "#d62728" for d in ev_diff]
        fig2 = go.Figure(go.Bar(
            x=list(range(len(ev_labels))), y=ev_diff,
            marker_color=colors,
            text=[f"{'+' if d > 0 else ''}{d}" for d in ev_diff],
            textposition="outside",
            hovertext=ev_labels,
            hoverinfo="text+y",
        ))
        fig2.add_hline(y=0, line_color="grey", line_width=1)
        fig2.update_layout(
            title="Touch Differential per Event",
            yaxis_title="TS − TR",
            xaxis=dict(tickvals=list(range(len(ev_labels))), ticktext=ev_labels, tickangle=-40),
            height=360, margin=dict(t=45, b=120),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # ── Individual bouts expandable ──────────────────────────────
    with st.expander(f"All pool bouts ({len(pool_bouts)} recorded)"):
        bout_rows = []
        for b in sorted(pool_bouts,
                         key=lambda x: ((x.get("events") or {}).get("date") or "", x.get("bout_order", 0)),
                         reverse=True):
            ev = b.get("events") or {}
            bout_rows.append({
                "Date":     ev.get("date") or "—",
                "Event":    ev.get("event_name", "—"),
                "Opponent": b.get("opponent_name", "—"),
                "Club":     b.get("opponent_club", "—"),
                "Result":   "✅ W" if b["result"] is True else ("❌ L" if b["result"] is False else "—"),
                "TS":       b.get("ts"),
                "TR":       b.get("tr"),
                "Diff":     (b["ts"] - b["tr"]) if b.get("ts") is not None and b.get("tr") is not None else None,
            })
        st.dataframe(pd.DataFrame(bout_rows), hide_index=True, use_container_width=True)


def _render_de_tab(de: dict, de_bouts: list):
    if not de:
        st.info("No DE bout data yet. DE results are collected from UK Ratings — run a refresh or wait for the weekend automation.")
        return
    c1, c2 = st.columns(2)
    c1.metric("DE Win %",  _coerce(de.get("de_win_pct"),       "{}%"))
    c2.metric("DE Bouts",  _coerce(de.get("total_de_bouts")))

    if de_bouts:
        # DE bouts are stored winner-centric: ts = winner's score, tr = loser's score.
        # For a loss bout, swap so the display always reads from the athlete's perspective:
        # "My Score" = touches scored by this athlete, "Opp Score" = touches conceded.
        rows = []
        for b in de_bouts:
            won   = b.get("result", False)
            ts    = b.get("ts")
            tr    = b.get("tr")
            my_score  = ts if won else tr
            opp_score = tr if won else ts
            rows.append({
                "Round":       b.get("round"),
                "Opponent":    b.get("opponent_name"),
                "Result":      "✅ W" if won else "❌ L",
                "My Score":    my_score,
                "Opp Score":   opp_score,
            })
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
        st.caption("Scores shown from this athlete's perspective: My Score = touches scored, Opp Score = touches conceded.")


def _render_rivals_tab(rivals: list):
    if not rivals:
        st.info("Not enough data yet to identify repeat opponents (requires individual bout data from Phase 2).")
        return

    # ── Summary metrics ──────────────────────────────────────────
    above_50 = sum(1 for r in rivals if r["win_pct"] >= 50)
    c1, c2, c3 = st.columns(3)
    c1.metric("Repeat Opponents",    len(rivals))
    c2.metric("Winning Record vs",   f"{above_50} of {len(rivals)}")
    c3.metric("Most Faced",          rivals[0]["name"].split()[0] if rivals else "—",
              help=f"{rivals[0]['name']} — {rivals[0]['total']} bouts" if rivals else "")

    # ── Win % bar chart ──────────────────────────────────────────
    # Show top 15 by encounter count to keep chart readable
    display = rivals[:15]
    names    = [r["name"] for r in display]
    win_pcts = [r["win_pct"] for r in display]
    totals   = [r["total"] for r in display]
    colors   = ["#2ca02c" if w >= 50 else "#d62728" for w in win_pcts]
    hover    = [f"{r['name']}<br>{r['wins']}W / {r['losses']}L<br>Touch Diff: {r['touch_diff']:+d}" for r in display]

    fig = go.Figure(go.Bar(
        x=win_pcts,
        y=names,
        orientation="h",
        marker_color=colors,
        text=[f"{w}%  ({t} bouts)" for w, t in zip(win_pcts, totals)],
        textposition="outside",
        hovertext=hover,
        hoverinfo="text",
    ))
    fig.add_vline(x=50, line_color="grey", line_dash="dash", line_width=1)
    fig.update_layout(
        title="Win % vs Repeat Opponents",
        xaxis_title="Win %", xaxis_range=[0, 118],
        yaxis=dict(autorange="reversed"),
        height=max(320, len(display) * 38),
        margin=dict(t=45, l=180, r=60, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Full table ───────────────────────────────────────────────
    rows = [{
        "Opponent":       r["name"],
        "Bouts":          r["total"],
        "W":              r["wins"],
        "L":              r["losses"],
        "Win %":          f"{r['win_pct']}%",
        "Touch Diff":     f"{r['touch_diff']:+d}",
        "Diff / Bout":    f"{round(r['touch_diff'] / r['total'], 1):+.1f}",
    } for r in rivals]
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def _render_monthly_tab(month_stats: dict):
    if not month_stats:
        st.info("Monthly breakdown requires individual pool bout records (Phase 2).")
        return
    month_names = {
        "1":"Jan","2":"Feb","3":"Mar","4":"Apr","5":"May","6":"Jun",
        "7":"Jul","8":"Aug","9":"Sep","10":"Oct","11":"Nov","12":"Dec"
    }
    sorted_months = sorted(month_stats.items(), key=lambda x: int(x[0]))
    labels   = [month_names.get(m, m) for m, _ in sorted_months]
    win_pcts = [d.get("win_pct") for _, d in sorted_months]
    bouts_n  = [d.get("W", 0) + d.get("L", 0) for _, d in sorted_months]
    fig = go.Figure(go.Bar(
        x=labels, y=win_pcts,
        marker_color="#1f77b4",
        text=[f"{w}%" for w in win_pcts],
        textposition="outside",
        hovertext=[f"{l}: {w}% win rate ({n} bouts)" for l, w, n in zip(labels, win_pcts, bouts_n)],
        hoverinfo="text",
    ))
    fig.update_layout(
        title="Pool Win % by Month", yaxis_title="Win %",
        yaxis_range=[0, 110], height=350
    )
    st.plotly_chart(fig, use_container_width=True)
    # Caveat: months with few bouts are unreliable — flag them
    sparse_months = [labels[i] for i, n in enumerate(bouts_n) if n < 10]
    if sparse_months:
        st.caption(
            f"⚠️ Low sample months (fewer than 10 bouts): {', '.join(sparse_months)}. "
            "Win rates in these months should be treated as indicative rather than conclusive."
        )


def _render_coaching_tab(metrics: dict):
    """
    Coaching Intelligence tab — powered by CoachingEngine.

    Renders a full analytical report: coverage quality, rule-based insights
    sorted by priority, placement progression, and data-driven priorities.
    """
    from metrics.calculator import calc_event_pool_metrics

    events     = metrics.get("events", []) or []
    athlete    = metrics.get("athlete") or {}
    name       = athlete.get("name_display", "This athlete")
    event_pool = calc_event_pool_metrics(events)
    pool       = metrics.get("pool", {}) or {}
    effective_pool = pool or event_pool

    # ── Empty state ──────────────────────────────────────────────
    if not events:
        st.info(
            "No competition history recorded for this athlete yet. "
            "Click **Refresh Data Now** in the sidebar to pull their history from "
            "FencingTimeLive and UK Ratings."
        )
        return

    if not effective_pool and not metrics.get("de", {}):
        st.info(
            "Coaching intelligence requires at least one event with pool or DE data. "
            "Run a full refresh to collect bout-level results."
        )
        return

    # ── Generate coaching report ──────────────────────────────────
    engine = CoachingEngine()
    report = engine.generate(metrics, athlete_name=name)

    # ── Coverage quality banner ───────────────────────────────────
    coverage = metrics.get("coverage", {}) or {}
    tier     = coverage.get("coverage_tier", "")
    if tier in ("LOW", "INSUFFICIENT"):
        st.warning(report.coverage_note)
    elif tier == "PARTIAL":
        st.info(report.coverage_note)
    else:
        st.success(report.coverage_note)

    # ── Summary narrative ─────────────────────────────────────────
    if report.summary:
        st.subheader("Coaching Summary")
        st.write(report.summary)

    # ── Top priorities ────────────────────────────────────────────
    if report.priorities:
        st.subheader("Top Priorities")
        for i, p in enumerate(report.priorities, 1):
            st.write(f"**{i}.** {p}")

    st.divider()

    # ── Full insight breakdown ────────────────────────────────────
    st.subheader("Detailed Analysis")

    severity_icons = {"STRENGTH": "✅", "CONCERN": "⚠️", "INFO": "ℹ️"}
    category_labels = {
        "pool": "Pool Phase",
        "de": "Direct Elimination",
        "mental": "Mental & Tactical",
        "trend": "Form & Trend",
        "benchmark": "Peer Benchmark",
        "coverage": "Data Quality",
    }

    # Group insights by category
    from collections import defaultdict
    by_cat: dict = defaultdict(list)
    for ins in report.insights:
        by_cat[ins.category].append(ins)

    cat_order = ["pool", "de", "mental", "trend", "benchmark", "coverage"]
    for cat in cat_order:
        if cat not in by_cat:
            continue
        cat_label = category_labels.get(cat, cat.title())
        st.markdown(f"##### {cat_label}")
        for ins in by_cat[cat]:
            icon = severity_icons.get(ins.severity, "•")
            with st.expander(f"{icon} {ins.headline}"):
                st.write(ins.detail)
                st.caption(f"_Confidence basis: {ins.data_ref}_")
        st.write("")

    # ── Placement Progression ─────────────────────────────────────
    st.divider()
    st.subheader("Placement Progression")
    valid_events = sorted(
        [e for e in events if e.get("placement") and e.get("field_size")],
        key=lambda e: e.get("date") or ""
    )
    if len(valid_events) >= 3:
        pcts       = [round(e["placement"] / e["field_size"] * 100, 1) for e in valid_events]
        avg_pct    = round(sum(pcts) / len(pcts), 1)
        recent_n   = min(5, len(pcts))
        recent_pct = round(sum(pcts[-recent_n:]) / recent_n, 1)
        # delta = career_avg minus recent: negative = improvement (lower %ile = better)
        delta_val  = round(avg_pct - recent_pct, 1)
        c1, c2, c3 = st.columns(3)
        c1.metric("Best Placement %ile", f"{min(pcts)}%",  help="Lower is better (1% = top of field)")
        c2.metric("Career Avg %ile",     f"{avg_pct}%",    help="Lower is better")
        c3.metric(
            f"Recent {recent_n} Avg %ile",
            f"{recent_pct}%",
            delta=f"{delta_val:+.1f}pp vs career",
            delta_color="inverse",   # lower %ile = better performance = green delta
        )
        st.caption(
            "Percentile = place ÷ field × 100. Lower is better (1% = top of field, 100% = last place). "
            "A negative delta means recent results are stronger than career average."
        )
    elif valid_events:
        st.caption(f"Only {len(valid_events)} events with placement data — need at least 3 for progression chart.")
    else:
        st.info("No placement data available yet.")

    # ── Advanced to DE detail ─────────────────────────────────────
    st.divider()
    st.subheader("Pool Phase Detail")
    if event_pool:
        adv     = event_pool.get("advanced_to_de_pct")
        adv_n   = event_pool.get("advanced_to_de_count", 0)
        # CI-1 fix: denominator = events_with_pool (events where pool data exists),
        # not total events — using total events would understate the advance rate for
        # Phase 1 athletes who have pool V/L flags recorded on the event row.
        total_n = event_pool.get("events_with_pool", 0)
        if adv is not None and total_n and total_n > 0:
            st.metric(
                "Advanced to DE",
                f"{adv}%",
                help=f"{adv_n} of {total_n} events with pool data — Phase 1 event flags only; "
                     "may undercount if pool data not fully collected.",
            )
            st.caption(
                "Denominator = events where pool V/L is recorded. "
                "Events without pool data are excluded from this calculation."
            )

    win_pct = effective_pool.get("pool_win_pct")
    if win_pct is not None:
        bar = "🟩" * int(win_pct // 10) + "⬜" * (10 - int(win_pct // 10))
        st.write(f"**Pool Win Rate:** {win_pct}%  {bar}")

    blr = pool.get("big_loss_rate")
    if blr is not None:
        # CI-2 fix: add explicit caveat about big loss rate calculation basis
        severity = "High" if blr > 40 else ("Moderate" if blr > 20 else "Low")
        st.write(f"**Big Loss Rate:** {blr}% ({severity})")
        st.caption(
            "Big Loss Rate = proportion of pool bout defeats where the margin was 3+ touches. "
            "Calculated from individual bout records (Phase 2 data only). "
            "A high rate erodes indicator score and adversely affects seeding."
        )

    # ── Footer disclaimer ─────────────────────────────────────────
    st.divider()
    st.caption(
        "**Data disclaimer:** All insights are derived exclusively from recorded competition data "
        "in the Allez Fencing database. No proxy values, assumptions, or invented figures are used. "
        "Insights marked INSUFFICIENT or LOW confidence reflect small sample sizes and should be "
        "treated as directional rather than definitive. For coaching decisions, always "
        "contextualise with direct athlete observation."
    )


# ═══════════════════════════════════════════════════════════════
# Main app
# ═══════════════════════════════════════════════════════════════

# ─── Athlete selector ───────────────────────────────────────────
@st.cache_data(ttl=300)
def load_athlete_list():
    db = get_read_client()
    return db.table("athletes").select(
        "id, name_display, weapon, age_category, last_refreshed, name_ftl, "
        "ftl_fencer_id, uk_ratings_id"
    ).eq("active", True).order("name_display").limit(10000).execute().data or []


athletes = load_athlete_list()

if not athletes:
    st.warning("No athletes found. Add athletes to the database first.")
    st.stop()

athlete_names = [a["name_display"] for a in athletes]
athlete_map   = {a["name_display"]: a for a in athletes}

selected_name = st.sidebar.selectbox("Select Athlete", athlete_names)
selected      = athlete_map[selected_name]
athlete_id    = selected["id"]

# ─── On-demand refresh button ───────────────────────────────────
last_refreshed = selected.get("last_refreshed")
if last_refreshed:
    refreshed_dt = datetime.fromisoformat(last_refreshed.replace("Z", "+00:00"))
    age_hours = (datetime.now(timezone.utc) - refreshed_dt).total_seconds() / 3600
    st.sidebar.caption(f"Data last updated {int(age_hours)}h ago")
else:
    st.sidebar.caption("Data not yet collected")

if st.sidebar.button("🔄 Refresh Data Now"):
    name_ftl  = selected.get("name_ftl") or selected_name
    uk_id     = selected.get("uk_ratings_id")
    uk_weapon = selected.get("weapon")

    with st.status(f"Refreshing {selected_name}...", expanded=True) as _status:
        # ── FTL pool data ──────────────────────────────────────────
        st.write("Collecting FTL pool bouts...")
        try:
            ftl_summary = collect_athlete(athlete_id=athlete_id, name_ftl=name_ftl)
            st.write(
                f"FTL: {ftl_summary.get('events_updated', 0)} events updated, "
                f"{ftl_summary.get('events_skipped', 0)} skipped, "
                f"{len(ftl_summary.get('errors', []))} errors."
            )
        except Exception as _e:
            st.write(f"FTL collection failed: {_e}")

        # ── UK Ratings DE bouts + annual stats ────────────────────
        if uk_id and uk_weapon:
            st.write("Collecting UK Ratings DE bouts and annual stats...")
            try:
                ukr_summary = collect_athlete_ukratings(
                    athlete_id=athlete_id,
                    uk_ratings_id=uk_id,
                    weapon=uk_weapon,
                )
                st.write(
                    f"UK Ratings: {ukr_summary.get('annual_years', 0)} annual year(s) updated."
                )
            except Exception as _e:
                st.write(f"UK Ratings collection failed: {_e}")
        else:
            st.write("UK Ratings: no uk_ratings_id configured for this athlete — skipped.")

        _status.update(label="Refresh complete ✓", state="complete")

    st.cache_data.clear()
    st.rerun()


# ─── Load metrics ───────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_metrics(aid: str):
    return calc_all_metrics(aid)


metrics  = load_metrics(athlete_id)
athlete  = metrics["athlete"]
events   = metrics.get("events", [])
pool     = metrics.get("pool", {})
de       = metrics.get("de", {})

# ─── Header ─────────────────────────────────────────────────────
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    weapon_emoji = {"foil": "🤺", "epee": "⚔️", "sabre": "🗡️"}.get(
        (athlete or {}).get("weapon", ""), "🤺"
    )
    st.title(f"{weapon_emoji} {selected_name}")
    if athlete:
        st.caption(
            f"{athlete.get('age_category') or ''} · "
            f"{athlete.get('weapon', '').capitalize()} · "
            f"{athlete.get('club') or 'Allez Fencing'}"
        )

# ─── No data state ───────────────────────────────────────────────
has_event_pool_data = any(e.get("pool_v") is not None for e in events)

if not events:
    # Distinguish between "not yet collected" and "not configured at all"
    # so the message directs the right person to take the right action.
    has_source_ids = any([
        selected.get("name_ftl"),
        selected.get("ftl_fencer_id"),
        selected.get("uk_ratings_id"),
    ])
    if has_source_ids:
        st.info(
            "No events collected yet for this athlete. "
            "Click **Refresh Data Now** in the sidebar to pull their history "
            "from FencingTimeLive and UK Ratings."
        )
    else:
        st.warning(
            "This athlete has not yet been configured for data collection. "
            "They have no FTL name, FTL fencer ID, or UK Ratings ID set. "
            "Clicking Refresh will not fetch any data until these are populated. "
            "Contact your administrator to set up their competition data profile "
            "in the Supabase athletes table."
        )
    st.stop()

# ─── Summary KPI row ────────────────────────────────────────────
from metrics.calculator import calc_event_pool_metrics
# event_pool_kpi: always the event-level aggregation (has advanced_to_de_pct)
# pool_kpi:       prefer bout-level pool when available (higher fidelity for win% / touch diff)
event_pool_kpi = calc_event_pool_metrics(events)
pool_kpi = pool if pool else event_pool_kpi

# Placement stats
placed_events = [e for e in events if e.get("placement") and e.get("field_size")]

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Events Competed",     len(events))
k2.metric("Pool Win %",         _coerce(pool_kpi.get("pool_win_pct"),              "{}%") if pool_kpi       else "—")
k3.metric("Touch Diff / Bout",  _coerce(pool_kpi.get("touch_diff_per_bout"))          if pool_kpi       else "—")
k4.metric("Advanced to DE %",   _coerce(event_pool_kpi.get("advanced_to_de_pct"),    "{}%") if event_pool_kpi else "—")
k5.metric("Best Place",
          min((e["placement"] for e in placed_events), default=None) or "—")

st.divider()

# ─── Tabs ───────────────────────────────────────────────────────
tab_events, tab_pool, tab_de, tab_rivals, tab_monthly, tab_coaching = st.tabs([
    "📅 Event History",
    "🏊 Pool Performance",
    "⚔️ DE Performance",
    "👥 Rivals",
    "📆 Monthly Trends",
    "🧠 Coaching Intelligence",
])

with tab_events:
    _render_event_history(events)
    _render_annual_stats(metrics.get("annual_stats", []))

with tab_pool:
    _render_pool_tab(pool, metrics.get("pool_bouts", []), metrics.get("volatility", {}), metrics.get("resilience", {}), events)

with tab_de:
    _render_de_tab(de, metrics.get("de_bouts", []))

with tab_rivals:
    _render_rivals_tab(metrics.get("rivals", []))

with tab_monthly:
    _render_monthly_tab(metrics.get("month_stats", {}))

with tab_coaching:
    _render_coaching_tab(metrics)
