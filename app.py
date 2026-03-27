"""
Allez Fencing Dashboard — main Streamlit app.
"""

import streamlit as st
from datetime import datetime, timezone
from database.client import get_read_client
from metrics.calculator import calc_all_metrics
from collectors.ftl_collector import collect_athlete
from collectors.ukratings_collector import fetch_annual_stats

st.set_page_config(
    page_title="Allez Fencing Dashboard",
    page_icon="🤺",
    layout="wide",
)

# ─── Athlete selector ───────────────────────────────────────────
@st.cache_data(ttl=300)
def load_athlete_list():
    db = get_read_client()
    return db.table("athletes").select(
        "id, name_display, weapon, age_category, last_refreshed"
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
    if selected.get("ftl_fencer_id"):
        with st.spinner(f"Collecting data for {selected_name}..."):
            summary = collect_athlete(
                athlete_id=athlete_id,
                ftl_fencer_id=selected["ftl_fencer_id"],
                name_ftl=selected.get("name_ftl") or selected_name,
            )
            if selected.get("uk_ratings_id") and selected.get("weapon"):
                fetch_annual_stats(
                    athlete_id=athlete_id,
                    uk_ratings_id=selected["uk_ratings_id"],
                    weapon=selected["weapon"],
                )
        st.success(
            f"Done — {summary['events']} events, "
            f"{summary['pool_bouts']} pool bouts, "
            f"{summary['de_bouts']} DE bouts collected."
        )
        st.cache_data.clear()
        st.rerun()
    else:
        st.sidebar.error("No FTL fencer ID configured for this athlete.")

# ─── Load metrics ───────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_metrics(aid: str):
    return calc_all_metrics(aid)

metrics = load_metrics(athlete_id)
athlete = metrics["athlete"]

# ─── Header ─────────────────────────────────────────────────────
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    weapon_emoji = {"foil": "🤺", "epee": "⚔️", "sabre": "🗡️"}.get(
        (athlete or {}).get("weapon", ""), "🤺"
    )
    st.title(f"{weapon_emoji} {selected_name}")
    if athlete:
        st.caption(
            f"{athlete.get('age_category', '')} · "
            f"{athlete.get('weapon', '').capitalize()} · "
            f"{athlete.get('club', 'Allez Fencing')}"
        )

# ─── No data state ───────────────────────────────────────────────
if not metrics["has_pool_data"] and not metrics["has_de_data"]:
    st.info(
        "No bout data collected yet for this athlete. "
        "Click **Refresh Data Now** in the sidebar to pull their FTL history, "
        "or wait for the automatic weekend refresh."
    )
    if metrics["annual_stats"]:
        st.subheader("UK Ratings Annual Totals")
        _render_annual_stats(metrics["annual_stats"])
    st.stop()

# ─── Summary KPI row ────────────────────────────────────────────
pool = metrics.get("pool", {})
de   = metrics.get("de", {})
events = metrics.get("events", [])

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Events Competed", len(events))
k2.metric("Pool Win %",      f"{pool.get('pool_win_pct', '—')}%" if pool else "—")
k3.metric("Touch Diff / Bout", pool.get("touch_diff_per_bout", "—") if pool else "—")
k4.metric("DE Win %",        f"{de.get('de_win_pct', '—')}%" if de else "—")
k5.metric("Career Pool Bouts", pool.get("total_pool_bouts", "—") if pool else "—")

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

with tab_pool:
    _render_pool_tab(pool, metrics.get("pool_bouts", []), metrics.get("volatility", {}))

with tab_de:
    _render_de_tab(de, metrics.get("de_bouts", []))

with tab_rivals:
    _render_rivals_tab(metrics.get("rivals", []))

with tab_monthly:
    _render_monthly_tab(metrics.get("month_stats", {}))

with tab_coaching:
    _render_coaching_tab(metrics)


# ═══════════════════════════════════════════════════════════════
# Render functions — placeholders until full UI build in Phase 3
# ═══════════════════════════════════════════════════════════════

def _render_annual_stats(annual_stats: list[dict]):
    import pandas as pd
    df = pd.DataFrame(annual_stats)[["year", "pool_w", "pool_l", "de_w", "de_l"]]
    df.columns = ["Year", "Pool W", "Pool L", "DE W", "DE L"]
    st.dataframe(df, hide_index=True)


def _render_event_history(events: list[dict]):
    if not events:
        st.info("No event history available.")
        return
    import pandas as pd
    rows = []
    for ev in events:
        tournament = ev.get("tournaments") or {}
        rows.append({
            "Date":       ev.get("date", "—"),
            "Tournament": tournament.get("name", ev.get("event_name", "—")),
            "Event":      ev.get("event_name", "—"),
            "Country":    tournament.get("country", "GBR"),
            "Place":      ev.get("placement", "—"),
            "Field":      ev.get("field_size", "—"),
        })
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def _render_pool_tab(pool: dict, pool_bouts: list, volatility: dict):
    if not pool:
        st.info("No pool bout data available yet.")
        return
    c1, c2, c3 = st.columns(3)
    c1.metric("Win %",             f"{pool.get('pool_win_pct')}%")
    c2.metric("Touch Differential", pool.get("touch_diff"))
    c3.metric("Big Loss Rate",     f"{pool.get('big_loss_rate')}%" if pool.get('big_loss_rate') is not None else "—")

    if volatility:
        st.caption(
            f"Consistency: SD {volatility.get('career_sd')}% career · "
            f"SD {volatility.get('recent_sd')}% recent 5 events"
        )


def _render_de_tab(de: dict, de_bouts: list):
    if not de:
        st.info("No DE bout data available yet.")
        return
    c1, c2 = st.columns(2)
    c1.metric("DE Win %",      f"{de.get('de_win_pct')}%")
    c2.metric("DE Bouts",      de.get("total_de_bouts"))

    import pandas as pd
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
        st.info("Not enough data yet to identify repeat opponents (need 2+ encounters).")
        return
    import pandas as pd
    rows = [{
        "Opponent":  r["name"],
        "Bouts":     r["total"],
        "Wins":      r["wins"],
        "Losses":    r["losses"],
        "Win %":     f"{r['win_pct']}%",
        "Touch Diff": r["touch_diff"],
    } for r in rivals]
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)


def _render_monthly_tab(month_stats: dict):
    if not month_stats:
        st.info("Monthly data requires pool bout records with dates.")
        return
    import plotly.graph_objects as go
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
    pool  = metrics.get("pool", {})
    de    = metrics.get("de", {})
    trend = metrics.get("trend", {})
    nvr   = metrics.get("nvr", {})

    if not pool and not de:
        st.info("Coaching intelligence requires bout-level data.")
        return

    st.subheader("Performance Summary")

    if trend:
        direction = trend.get("direction", "stable")
        delta     = trend.get("delta", 0)
        icon = "📈" if direction == "up" else ("📉" if direction == "down" else "➡️")
        st.write(
            f"{icon} **Trend:** Pool win rate is **{direction}** "
            f"({'+' if delta >= 0 else ''}{delta}pp vs prior 3 events)"
        )

    if nvr and nvr.get("gap") is not None:
        gap = nvr["gap"]
        fp  = nvr.get("first_pct")
        rp  = nvr.get("repeat_pct")
        st.write(
            f"**New vs Repeat Opponents:** {fp}% vs new opponents, "
            f"{rp}% vs repeat ({abs(gap)}pp {'advantage' if gap > 0 else 'disadvantage'} "
            f"against first-time opponents)"
        )

    if pool.get("big_loss_rate") is not None:
        blr = pool["big_loss_rate"]
        severity = "High" if blr > 40 else ("Moderate" if blr > 20 else "Low")
        st.write(f"**Big Loss Rate:** {blr}% of losses are by 3+ touches ({severity})")

    st.caption("All insights derived from real bout data — no proxy values.")
