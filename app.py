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

st.set_page_config(
    page_title="Allez Fencing Dashboard",
    page_icon="🤺",
    layout="wide",
)


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
        if ev.get("pool_v") is not None and (ev.get("pool_v", 0) + ev.get("pool_l", 0)) > 0:
            bouts = ev["pool_v"] + ev["pool_l"]
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
            "TS–TR":       f"{ev['pool_ts']}–{ev['pool_tr']}" if ev.get("pool_ts") is not None else "—",
            "Ind":         ev.get("pool_ind"),
            "→DE":         "✅" if ev.get("advanced_to_de") else ("❌" if ev.get("pool_v") is not None else "—"),
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, hide_index=True, use_container_width=True)

    # ── Placement trend chart ────────────────────────────────────
    chart_rows = [
        r for r in rows
        if r["Place"] != "—" and "/" in r["Place"]
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
        c1.metric("Pool Win %",         f"{event_pool.get('pool_win_pct') or '—'}%")
        c2.metric("Touch Diff",          event_pool.get("touch_diff") if event_pool.get("touch_diff") is not None else "—")
        c3.metric("Touch Diff / Bout",   event_pool.get("touch_diff_per_bout") or "—")
        c4.metric("Advanced to DE %",   f"{event_pool.get('advanced_to_de_pct') or '—'}%")

        # Pool win% per event bar chart
        valid = sorted(
            [e for e in events if e.get("pool_v") is not None],
            key=lambda e: e.get("date") or ""
        )
        if valid:
            labels  = [e.get("tournaments", {}).get("name", "")[:20] if e.get("tournaments") else e.get("event_name","")[:20] for e in valid]
            bouts   = [e["pool_v"] + e["pool_l"] for e in valid]
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
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Win %",               f"{pool.get('pool_win_pct')}%")
    c2.metric("Touch Diff / Bout",    pool.get("touch_diff_per_bout"))
    c3.metric("Big Loss Rate",        f"{pool.get('big_loss_rate')}%" if pool.get('big_loss_rate') is not None else "—",
              help="% of losses conceded by 3+ touches")
    res_pct = resilience.get("resilience_pct") if resilience else None
    c4.metric("Resilience Score",     f"{res_pct}%" if res_pct is not None else "—",
              help="Win rate in the bout immediately following a loss (same event). >60% = strong bounce-back.")

    if volatility:
        recent_sd = volatility.get("recent_sd")
        recent_sd_str = (
            f"SD {recent_sd}% recent 5 events"
            if recent_sd is not None
            else "fewer than 5 scored events"
        )
        st.caption(
            f"Consistency: SD {volatility.get('career_sd')}% career · {recent_sd_str}"
        )

    # ── Per-event touch diff chart ───────────────────────────────
    from collections import defaultdict
    ev_map: dict = defaultdict(lambda: {"ts": 0, "tr": 0, "date": "", "name": ""})
    for b in pool_bouts:
        ev = b.get("events") or {}
        eid = b["event_id"]
        ev_map[eid]["ts"]   += b["ts"]
        ev_map[eid]["tr"]   += b["tr"]
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
                "Result":   "✅ W" if b["result"] else "❌ L",
                "TS":       b["ts"],
                "TR":       b["tr"],
                "Diff":     b["ts"] - b["tr"],
            })
        st.dataframe(pd.DataFrame(bout_rows), hide_index=True, use_container_width=True)


def _render_de_tab(de: dict, de_bouts: list):
    if not de:
        st.info("No DE bout data yet. DE results are collected from UK Ratings — run a refresh or wait for the weekend automation.")
        return
    c1, c2 = st.columns(2)
    c1.metric("DE Win %",  f"{de.get('de_win_pct')}%")
    c2.metric("DE Bouts",   de.get("total_de_bouts"))

    if de_bouts:
        rows = [{
            "Round":    b.get("round"),
            "Opponent": b.get("opponent_name"),
            "TS":       b.get("ts"),
            "TR":       b.get("tr"),
            "Result":   "✅ W" if b.get("result") else "❌ L",
        } for b in de_bouts]
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def _render_rivals_tab(rivals: list):
    if not rivals:
        st.info("Not enough data yet to identify repeat opponents (requires individual bout data from Phase 2).")
        return

    # ── Summary metrics ──────────────────────────────────────────
    above_50 = sum(1 for r in rivals if r["win_pct"] >= 50)
    c1, c2, c3 = st.columns(3)
    c1.metric("Repeat Opponents",    len(rivals))
    c2.metric("Winning Record vs",   f"{above_50} of {len(rivals)}")
    c3.metric("Most Faced",          rivals[0]["name"].split()[-1] if rivals else "—",
              help=f"{rivals[0]['total']} bouts" if rivals else "")

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
    fig = go.Figure(go.Bar(x=labels, y=win_pcts, marker_color="#1f77b4"))
    fig.update_layout(
        title="Pool Win % by Month", yaxis_title="Win %",
        yaxis_range=[0, 100], height=350
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_coaching_tab(metrics: dict):
    pool   = metrics.get("pool", {})
    de     = metrics.get("de", {})
    trend  = metrics.get("trend", {})
    nvr    = metrics.get("nvr", {})
    events = metrics.get("events", [])

    from metrics.calculator import calc_event_pool_metrics
    event_pool = calc_event_pool_metrics(events) if not pool else {}

    effective_pool = pool or event_pool

    if not effective_pool:
        st.info("Coaching intelligence requires bout-level data.")
        return

    st.subheader("Performance Summary")

    # ── Pool headline ─────────────────────────────────────────────
    win_pct = effective_pool.get("pool_win_pct")
    if win_pct is not None:
        bar = "🟩" * int(win_pct // 10) + "⬜" * (10 - int(win_pct // 10))
        st.write(f"**Pool Win Rate:** {win_pct}%  {bar}")

    # ── Advanced to DE ───────────────────────────────────────────
    if event_pool.get("advanced_to_de_pct") is not None:
        adv = event_pool["advanced_to_de_pct"]
        adv_n = event_pool.get("advanced_to_de_count", "?")
        total_n = event_pool.get("events_with_pool", "?")
        st.write(f"**Advanced to DE:** {adv}%  ({adv_n} of {total_n} events)")

    # ── Touch differential ───────────────────────────────────────
    td = effective_pool.get("touch_diff")
    tdpb = effective_pool.get("touch_diff_per_bout")
    if td is not None:
        icon = "📈" if td > 0 else "📉"
        st.write(f"**Touch Differential:** {'+' if td >= 0 else ''}{td} total  ({'+' if (tdpb or 0) >= 0 else ''}{tdpb} per bout)  {icon}")

    # ── Trend (only if individual bout data available) ───────────
    if trend:
        direction = trend.get("direction", "stable")
        delta     = trend.get("delta", 0)
        icon2 = "📈" if direction == "up" else ("📉" if direction == "down" else "➡️")
        st.write(
            f"{icon2} **Trend:** Pool win rate is **{direction}** "
            f"({'+' if delta >= 0 else ''}{delta}pp vs prior events)"
        )

    # ── New vs Repeat ────────────────────────────────────────────
    if nvr and nvr.get("gap") is not None:
        gap = nvr["gap"]
        fp  = nvr.get("first_pct")
        rp  = nvr.get("repeat_pct")
        st.write(
            f"**New vs Repeat Opponents:** {fp}% vs new, "
            f"{rp}% vs repeat  ({abs(gap)}pp {'advantage' if gap > 0 else 'disadvantage'} "
            f"vs first-time opponents)"
        )

    # ── Big loss rate ────────────────────────────────────────────
    if pool.get("big_loss_rate") is not None:
        blr = pool["big_loss_rate"]
        severity = "High" if blr > 40 else ("Moderate" if blr > 20 else "Low")
        st.write(f"**Big Loss Rate:** {blr}% of losses by 3+ touches ({severity})")

    # ── Placement progression ─────────────────────────────────────
    st.divider()
    st.subheader("Placement Progression")
    valid_events = sorted(
        [e for e in events if e.get("placement") and e.get("field_size")],
        key=lambda e: e.get("date") or ""
    )
    if valid_events:
        pcts = [round(e["placement"] / e["field_size"] * 100, 1) for e in valid_events]
        avg_pct = round(sum(pcts) / len(pcts), 1)
        recent_pct = round(sum(pcts[-5:]) / min(5, len(pcts)), 1)
        c1, c2, c3 = st.columns(3)
        c1.metric("Best Placement %ile", f"{min(pcts)}%")
        c2.metric("Career Avg %ile",     f"{avg_pct}%", help="Lower = better")
        c3.metric("Recent 5 Avg %ile",   f"{recent_pct}%",
                  delta=f"{round(avg_pct - recent_pct, 1)}pp vs career",
                  delta_color="normal")
        st.caption("Percentile = place ÷ field × 100. Lower is better (1% = top of field).")

    st.caption("All insights derived from real data — no proxy values.")


# ═══════════════════════════════════════════════════════════════
# Main app
# ═══════════════════════════════════════════════════════════════

# ─── Athlete selector ───────────────────────────────────────────
@st.cache_data(ttl=300)
def load_athlete_list():
    db = get_read_client()
    return db.table("athletes").select(
        "id, name_display, weapon, age_category, last_refreshed, name_ftl"
    ).eq("active", True).order("name_display").execute().data or []


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
    name_ftl = selected.get("name_ftl") or selected_name
    with st.spinner(f"Collecting data for {selected_name}..."):
        summary = collect_athlete(athlete_id=athlete_id, name_ftl=name_ftl)
    st.success(
        f"Done — {summary['events_updated']} events updated, "
        f"{summary['events_skipped']} skipped, "
        f"{len(summary.get('errors', []))} errors."
    )
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
    st.info(
        "No events collected yet for this athlete. "
        "Click **Refresh Data Now** in the sidebar to pull their history."
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
k2.metric("Pool Win %",          f"{pool_kpi.get('pool_win_pct') or '—'}%"
          if pool_kpi else "—")
k3.metric("Touch Diff / Bout",   pool_kpi.get("touch_diff_per_bout") or "—"
          if pool_kpi else "—")
k4.metric("Advanced to DE %",    f"{event_pool_kpi.get('advanced_to_de_pct') or '—'}%"
          if event_pool_kpi else "—")
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
