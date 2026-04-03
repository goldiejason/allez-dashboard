"""
intelligence/engine.py — Rule-Based Coaching Intelligence Engine

Generates personalised, data-driven coaching narratives for fencers by
applying a library of ~20 analytical rules to the full metrics dict
produced by metrics.calculator.calc_all_metrics().

Design principles:
  - Every insight is anchored to a specific data point with a confidence tier.
  - Insights below the INSUFFICIENT tier are suppressed entirely — no coaching
    value can be extracted from 1-4 bouts.
  - Rules fire in priority order; each rule may emit one Insight.
  - Insights carry a severity (STRENGTH / CONCERN / INFO) and a category so
    the caller can group or filter them.
  - The engine is stateless: construct once, call generate() per athlete.

Usage:
    from intelligence.engine import CoachingEngine
    engine = CoachingEngine()
    report = engine.generate(metrics, athlete_name="PANGA Daniel")
    # report.insights   → list[Insight]
    # report.summary    → one-paragraph narrative string
    # report.priorities → top-3 action items
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ── Thresholds ──────────────────────────────────────────────────────────────

# Pool win rate bands
POOL_ELITE     = 75.0
POOL_STRONG    = 60.0
POOL_AVERAGE   = 50.0
POOL_WEAK      = 40.0

# Advanced-to-DE bands
DE_ADV_STRONG  = 70.0
DE_ADV_AVERAGE = 50.0

# Touch differential per bout
TD_EXCELLENT   =  1.5
TD_POSITIVE    =  0.5
TD_NEGATIVE    = -0.5
TD_POOR        = -1.5

# Big loss rate (losses by 3+ touches)
BLR_HIGH       = 40.0
BLR_MODERATE   = 20.0

# DE win rate
DE_STRONG      = 60.0
DE_AVERAGE     = 40.0

# Close bout rate (within 2 touches either way)
CLOSE_HIGH     = 55.0

# Dominant win percentage among wins
DOM_HIGH       = 60.0
DOM_LOW        = 25.0

# New vs repeat opponent gap (percentage points)
NVR_MEANINGFUL = 10.0

# Volatility score (std dev of win pct across events)
VOLATILITY_HIGH    = 20.0
VOLATILITY_MODERATE = 10.0

# Placement trend (pp improvement in recent 5 vs career average)
TREND_STRONG   = 8.0
TREND_NEGATIVE = 8.0

# Resilience score bands
RESILIENCE_STRONG = 0.60
RESILIENCE_WEAK   = 0.35

# Peer benchmark
PEER_ELITE     = 80.0
PEER_STRONG    = 60.0
PEER_WEAK      = 40.0


# ── Data structures ──────────────────────────────────────────────────────────

@dataclass
class Insight:
    """A single coaching observation derived from one or more data points."""
    category:   str            # "pool" | "de" | "mental" | "trend" | "benchmark" | "coverage"
    severity:   str            # "STRENGTH" | "CONCERN" | "INFO"
    headline:   str            # one-line summary (shown in UI)
    detail:     str            # full coaching-grade explanation
    data_ref:   str            # which metric(s) drove this insight
    priority:   int = 0        # lower = higher priority


@dataclass
class CoachingReport:
    """Full coaching output for one athlete."""
    athlete_name: str
    insights:     list[Insight] = field(default_factory=list)
    summary:      str           = ""
    priorities:   list[str]     = field(default_factory=list)
    coverage_note: str          = ""


# ── Engine ───────────────────────────────────────────────────────────────────

class CoachingEngine:
    """
    Rule-based coaching intelligence engine.

    Construct once and reuse across athletes — the engine is fully stateless.
    Call generate(metrics, athlete_name) to produce a CoachingReport.
    """

    def generate(self, metrics: dict, athlete_name: str = "This athlete") -> CoachingReport:
        """
        Run all rules against the metrics dict and return a CoachingReport.

        Parameters
        ----------
        metrics :       Full dict returned by calc_all_metrics().
        athlete_name :  Display name for narrative text.
        """
        report = CoachingReport(athlete_name=athlete_name)

        # Pull metric groups
        pool        = metrics.get("pool", {}) or {}
        de          = metrics.get("de", {}) or {}
        de_coaching = metrics.get("de_coaching", {}) or {}
        nvr         = metrics.get("nvr", {}) or {}
        trend       = metrics.get("trend", {}) or {}
        resilience  = metrics.get("resilience", {}) or {}
        volatility  = metrics.get("volatility", {}) or {}
        coverage    = metrics.get("coverage", {}) or {}
        peer        = metrics.get("peer_benchmarks", {}) or {}
        placement   = metrics.get("placement_progression", {}) or {}
        events      = metrics.get("events", []) or []
        annual      = metrics.get("annual_stats", []) or []

        # event-level pool (always present — derived from events table flags)
        from metrics.calculator import calc_event_pool_metrics
        event_pool = calc_event_pool_metrics(events)

        # Fire all rules
        self._rule_coverage(report, coverage)
        self._rule_pool_win_rate(report, pool, event_pool, athlete_name)
        self._rule_touch_differential(report, pool)
        self._rule_advanced_to_de(report, event_pool, athlete_name)
        self._rule_big_loss_rate(report, pool)
        self._rule_de_win_rate(report, de, athlete_name)
        self._rule_de_coaching_margins(report, de_coaching)
        self._rule_de_round_weakness(report, de_coaching)
        self._rule_new_vs_repeat(report, nvr, athlete_name)
        self._rule_trend(report, trend, athlete_name)
        self._rule_resilience(report, resilience)
        self._rule_volatility(report, volatility, athlete_name)
        self._rule_placement_progression(report, placement, athlete_name)
        self._rule_peer_benchmark(report, peer, athlete_name)
        self._rule_close_bouts(report, de_coaching)
        self._rule_touch_efficiency(report, de_coaching)
        self._rule_annual_trend(report, annual, athlete_name)
        self._rule_event_volume(report, events, coverage)

        # Sort by priority then severity weight
        _severity_weight = {"CONCERN": 0, "STRENGTH": 1, "INFO": 2}
        report.insights.sort(key=lambda i: (i.priority, _severity_weight.get(i.severity, 3)))

        # Build summary and priorities
        report.summary    = self._build_summary(report, athlete_name)
        report.priorities = self._build_priorities(report)
        report.coverage_note = self._coverage_note(coverage)

        return report

    # ── Rule implementations ──────────────────────────────────────────────

    def _rule_coverage(self, report: CoachingReport, coverage: dict) -> None:
        """Flag data completeness so coaches understand the intelligence quality."""
        tier = coverage.get("coverage_tier", "")
        if tier in ("LOW", "INSUFFICIENT"):
            pool_pct = coverage.get("pool_coverage_pct", 0)
            date_pct = coverage.get("date_coverage_pct", 0)
            report.insights.append(Insight(
                category="coverage",
                severity="INFO",
                headline="Limited data — insights based on partial history",
                detail=(
                    f"Pool bout data covers {pool_pct}% of recorded events; "
                    f"date information is available for {date_pct}% of events. "
                    "Run a full data refresh to improve coaching insight quality."
                ),
                data_ref="coverage.coverage_tier",
                priority=0,
            ))
        elif tier == "PARTIAL":
            report.insights.append(Insight(
                category="coverage",
                severity="INFO",
                headline="Partial data coverage — some insights may be incomplete",
                detail=(
                    f"Pool data is available for {coverage.get('pool_coverage_pct', 0)}% "
                    "of events. Insights marked with a confidence band reflect this."
                ),
                data_ref="coverage.coverage_tier",
                priority=1,
            ))

    def _rule_pool_win_rate(
        self, report: CoachingReport, pool: dict, event_pool: dict, name: str
    ) -> None:
        """Assess overall pool phase performance."""
        wp = pool.get("pool_win_pct")
        if wp is None:
            wp = event_pool.get("pool_win_pct")
        tier = pool.get("confidence_tier", "")
        n    = pool.get("n", 0)

        if wp is None or tier == "INSUFFICIENT":
            return

        conf_label = f"({tier.lower()} confidence, {n} bouts)" if tier else ""

        if wp >= POOL_ELITE:
            report.insights.append(Insight(
                category="pool",
                severity="STRENGTH",
                headline=f"Elite pool performance — {wp}% win rate",
                detail=(
                    f"{name} wins {wp}% of pool bouts {conf_label}. "
                    "This places them among the top tier in the pool phase. "
                    "Focus on maintaining this standard under pressure at higher-stakes events."
                ),
                data_ref="pool.pool_win_pct",
                priority=2,
            ))
        elif wp >= POOL_STRONG:
            report.insights.append(Insight(
                category="pool",
                severity="STRENGTH",
                headline=f"Strong pool win rate — {wp}%",
                detail=(
                    f"{name} wins {wp}% of pool bouts {conf_label}. "
                    "This is consistently above average. "
                    "Identify the 1-2 opponent archetypes that produce losses and address them tactically."
                ),
                data_ref="pool.pool_win_pct",
                priority=3,
            ))
        elif wp >= POOL_AVERAGE:
            report.insights.append(Insight(
                category="pool",
                severity="INFO",
                headline=f"Pool win rate at average — {wp}%",
                detail=(
                    f"{name} wins {wp}% of pool bouts {conf_label}. "
                    "Performance is solid but there is a clear margin to improve. "
                    "Small gains here (converting 1 more bout per pool) compound significantly over a season."
                ),
                data_ref="pool.pool_win_pct",
                priority=4,
            ))
        elif wp >= POOL_WEAK:
            report.insights.append(Insight(
                category="pool",
                severity="CONCERN",
                headline=f"Pool win rate below average — {wp}%",
                detail=(
                    f"{name} wins only {wp}% of pool bouts {conf_label}. "
                    "This is the primary performance constraint. "
                    "Prioritise pool tactics: distance management, first-action success rate, "
                    "and mental reset between bouts."
                ),
                data_ref="pool.pool_win_pct",
                priority=1,
            ))
        else:
            report.insights.append(Insight(
                category="pool",
                severity="CONCERN",
                headline=f"Critical pool performance gap — {wp}% win rate",
                detail=(
                    f"{name} wins only {wp}% of pool bouts {conf_label}. "
                    "This is a fundamental technical or tactical issue requiring immediate coaching attention. "
                    "Consider whether the fencer is competing at an appropriate level, and review "
                    "first-action patterns, distance control, and target selection."
                ),
                data_ref="pool.pool_win_pct",
                priority=1,
            ))

    def _rule_touch_differential(self, report: CoachingReport, pool: dict) -> None:
        """Assess touch scoring efficiency vs touches conceded."""
        tdpb = pool.get("touch_diff_per_bout")
        tier = pool.get("confidence_tier", "")
        if tdpb is None or tier == "INSUFFICIENT":
            return

        if tdpb >= TD_EXCELLENT:
            report.insights.append(Insight(
                category="pool",
                severity="STRENGTH",
                headline=f"Excellent touch differential — +{tdpb:.1f} per bout",
                detail=(
                    f"Scoring +{tdpb:.1f} touches more than conceded per bout reflects dominant "
                    "attacking efficiency. This fencer controls the scoring tempo effectively."
                ),
                data_ref="pool.touch_diff_per_bout",
                priority=5,
            ))
        elif tdpb >= TD_POSITIVE:
            report.insights.append(Insight(
                category="pool",
                severity="INFO",
                headline=f"Positive touch differential — +{tdpb:.1f} per bout",
                detail=(
                    f"A positive differential of +{tdpb:.1f} per bout shows the fencer is scoring "
                    "more than they concede. Seek to widen this through cleaner second-intention actions."
                ),
                data_ref="pool.touch_diff_per_bout",
                priority=6,
            ))
        elif tdpb >= TD_NEGATIVE:
            report.insights.append(Insight(
                category="pool",
                severity="CONCERN",
                headline=f"Marginal touch differential — {tdpb:+.1f} per bout",
                detail=(
                    f"At {tdpb:+.1f} touches per bout, the fencer is giving away nearly as many "
                    "touches as they score. Review target selection and parry-riposte conversion rates."
                ),
                data_ref="pool.touch_diff_per_bout",
                priority=3,
            ))
        else:
            report.insights.append(Insight(
                category="pool",
                severity="CONCERN",
                headline=f"Negative touch differential — {tdpb:+.1f} per bout",
                detail=(
                    f"Conceding {abs(tdpb):.1f} more touches than scored per bout indicates "
                    "the fencer is being outscored regularly. Defensive actions and counter-attack "
                    "timing may need fundamental review."
                ),
                data_ref="pool.touch_diff_per_bout",
                priority=2,
            ))

    def _rule_advanced_to_de(
        self, report: CoachingReport, event_pool: dict, name: str
    ) -> None:
        """Assess the rate of advancing from pools to the DE tableau."""
        adv = event_pool.get("advanced_to_de_pct")
        adv_n = event_pool.get("advanced_to_de_count")
        total_n = event_pool.get("events_with_pool")

        if adv is None or total_n is None or total_n < 3:
            return

        if adv >= DE_ADV_STRONG:
            report.insights.append(Insight(
                category="pool",
                severity="STRENGTH",
                headline=f"Consistently advances to DE — {adv}% of events",
                detail=(
                    f"{name} progresses to the direct elimination tableau in {adv}% of events "
                    f"({adv_n} of {total_n}). This is a strong conversion rate that reflects "
                    "reliable pool phase execution."
                ),
                data_ref="event_pool.advanced_to_de_pct",
                priority=5,
            ))
        elif adv >= DE_ADV_AVERAGE:
            report.insights.append(Insight(
                category="pool",
                severity="INFO",
                headline=f"Advancing to DE in {adv}% of events",
                detail=(
                    f"Progressing to the tableau in {adv}% of events is above average. "
                    "Identify the events where elimination occurs in pools — common factors "
                    "include fatigue, venue travel, or particularly strong fields."
                ),
                data_ref="event_pool.advanced_to_de_pct",
                priority=6,
            ))
        else:
            report.insights.append(Insight(
                category="pool",
                severity="CONCERN",
                headline=f"Failing to advance to DE in {100 - adv}% of events",
                detail=(
                    f"{name} is eliminated in pools at {100 - adv}% of events "
                    f"({total_n - (adv_n or 0)} of {total_n}). "
                    "This is a significant barrier to competitive development. "
                    "Pool tactics, pacing strategy, and bout-to-bout composure require targeted work."
                ),
                data_ref="event_pool.advanced_to_de_pct",
                priority=2,
            ))

    def _rule_big_loss_rate(self, report: CoachingReport, pool: dict) -> None:
        """Flag a high rate of losses by large margins (3+ touches)."""
        blr  = pool.get("big_loss_rate")
        blr_n = pool.get("big_loss_n", 0)
        tier  = pool.get("confidence_tier", "")
        if blr is None or tier == "INSUFFICIENT":
            return

        if blr > BLR_HIGH:
            report.insights.append(Insight(
                category="pool",
                severity="CONCERN",
                headline=f"High big-loss rate — {blr}% of defeats by 3+ touches",
                detail=(
                    f"{blr}% of this fencer's pool losses ({blr_n} bouts) are by a margin of "
                    "3 touches or more. Conceding heavily against stronger opponents erodes indicator "
                    "and undermines seeding. Work on sustained defensive pressure and accepting the "
                    "tactical loss at 4-1 down rather than allowing a 5-0 result."
                ),
                data_ref="pool.big_loss_rate",
                priority=3,
            ))
        elif blr > BLR_MODERATE:
            report.insights.append(Insight(
                category="pool",
                severity="INFO",
                headline=f"Moderate big-loss rate — {blr}% of defeats by 3+ touches",
                detail=(
                    f"About {blr}% of pool defeats are by a margin of 3 or more touches. "
                    "While manageable, reducing this to below 20% would meaningfully improve indicator "
                    "and seeding outcomes over a full season."
                ),
                data_ref="pool.big_loss_rate",
                priority=6,
            ))

    def _rule_de_win_rate(
        self, report: CoachingReport, de: dict, name: str
    ) -> None:
        """Assess overall direct elimination win rate."""
        wr   = de.get("de_win_pct")
        tier = de.get("confidence_tier", "")
        n    = de.get("n", 0)

        if wr is None or tier == "INSUFFICIENT":
            return

        conf_label = f"({tier.lower()} confidence, {n} bouts)"

        if wr >= DE_STRONG:
            report.insights.append(Insight(
                category="de",
                severity="STRENGTH",
                headline=f"Strong DE performance — {wr}% win rate",
                detail=(
                    f"{name} wins {wr}% of DE bouts {conf_label}. "
                    "Performing well under the added pressure of elimination bouts is a significant "
                    "competitive advantage. Maintain mental preparation routines."
                ),
                data_ref="de.de_win_pct",
                priority=3,
            ))
        elif wr >= DE_AVERAGE:
            report.insights.append(Insight(
                category="de",
                severity="INFO",
                headline=f"Average DE win rate — {wr}%",
                detail=(
                    f"{name} wins {wr}% of DE bouts {conf_label}. "
                    "There is a meaningful gap to close. Focus on first-period aggression "
                    "in DE: research shows early leads in 15-touch bouts are statistically very hard to overcome."
                ),
                data_ref="de.de_win_pct",
                priority=5,
            ))
        else:
            report.insights.append(Insight(
                category="de",
                severity="CONCERN",
                headline=f"DE win rate below average — {wr}%",
                detail=(
                    f"{name} wins only {wr}% of DE bouts {conf_label}. "
                    "Despite advancing through pools, the fencer struggles to convert in the tableau. "
                    "Key areas: 15-touch tactical preparation, video analysis of early exits, "
                    "and practising comeback scenarios from a 5-point deficit."
                ),
                data_ref="de.de_win_pct",
                priority=2,
            ))

    def _rule_de_coaching_margins(
        self, report: CoachingReport, de_coaching: dict
    ) -> None:
        """Analyse win/loss margin patterns in DE — dominant vs close."""
        tier   = de_coaching.get("confidence_tier", "")
        n      = de_coaching.get("n", 0)
        dom_w  = de_coaching.get("dominant_win_pct")
        close_w = de_coaching.get("close_win_pct")
        close_l = de_coaching.get("close_loss_pct")
        dom_l  = de_coaching.get("dominant_loss_pct")

        if tier == "INSUFFICIENT" or n < 5:
            return

        # Dominant losses flag
        if dom_l is not None and dom_l > BLR_HIGH:
            report.insights.append(Insight(
                category="de",
                severity="CONCERN",
                headline=f"High rate of heavy DE defeats — {dom_l:.0f}% of DE losses by 5+ touches",
                detail=(
                    f"{dom_l:.0f}% of DE losses are by 5 or more touches. "
                    "Being comprehensively beaten in elimination bouts typically indicates the fencer "
                    "is facing opponents 1-2 levels above them, or is collapsing under pressure "
                    "after falling behind. Introduce 'damage limitation' tactical drills."
                ),
                data_ref="de_coaching.dominant_loss_pct",
                priority=3,
            ))

        # Close losses flag — could be winning these
        if close_l is not None and close_w is not None and close_l > close_w + 15:
            report.insights.append(Insight(
                category="de",
                severity="CONCERN",
                headline="Losing close DE bouts more than winning them",
                detail=(
                    f"Of tight DE bouts (decided within 2 touches), "
                    f"{close_l:.0f}% go against this fencer vs {close_w:.0f}% in their favour. "
                    "The margin between winning and losing at this level is small. "
                    "Focus on mental preparation for the final priority period and physical conditioning "
                    "to ensure no late-bout fatigue is a factor."
                ),
                data_ref="de_coaching.close_loss_pct",
                priority=2,
            ))
        elif close_w is not None and close_w > CLOSE_HIGH:
            report.insights.append(Insight(
                category="de",
                severity="STRENGTH",
                headline=f"Clutch performer — wins {close_w:.0f}% of close DE bouts",
                detail=(
                    f"Winning {close_w:.0f}% of bouts decided within 2 touches demonstrates "
                    "exceptional mental composure under pressure. This is a genuine competitive edge."
                ),
                data_ref="de_coaching.close_win_pct",
                priority=5,
            ))

    def _rule_de_round_weakness(
        self, report: CoachingReport, de_coaching: dict
    ) -> None:
        """Surface specific round-level weaknesses in the DE tableau."""
        tier  = de_coaching.get("confidence_tier", "")
        rrwr  = de_coaching.get("round_win_rates", {})

        if tier == "INSUFFICIENT" or not rrwr:
            return

        # Find the worst round by win rate (minimum 3 bouts)
        weak_round = None
        weak_rate  = 100.0
        for rnd, data in rrwr.items():
            if data.get("n", 0) >= 3:
                wr = data.get("win_rate", 100)
                if wr < weak_rate:
                    weak_rate  = wr
                    weak_round = rnd

        if weak_round and weak_rate < 40:
            report.insights.append(Insight(
                category="de",
                severity="CONCERN",
                headline=f"Consistent early exit at {weak_round} — {weak_rate:.0f}% win rate",
                detail=(
                    f"Win rate in {weak_round} bouts is only {weak_rate:.0f}%. "
                    "A pattern of early tableau exits at a consistent round suggests the fencer "
                    "faces a specific calibre of opponent at this stage where they are under-prepared. "
                    "Analyse video from these bouts and simulate the tactical patterns in training."
                ),
                data_ref=f"de_coaching.round_win_rates.{weak_round}",
                priority=2,
            ))

        # Find the best round (strength)
        strong_round = None
        strong_rate  = 0.0
        for rnd, data in rrwr.items():
            if data.get("n", 0) >= 3:
                wr = data.get("win_rate", 0)
                if wr > strong_rate:
                    strong_rate  = wr
                    strong_round = rnd

        if strong_round and strong_rate >= 70:
            report.insights.append(Insight(
                category="de",
                severity="STRENGTH",
                headline=f"Strong {strong_round} performer — {strong_rate:.0f}% win rate",
                detail=(
                    f"Winning {strong_rate:.0f}% of {strong_round} bouts is an identifiable strength. "
                    "Understanding what tactical patterns drive this success can help replicate the "
                    "approach at other rounds."
                ),
                data_ref=f"de_coaching.round_win_rates.{strong_round}",
                priority=6,
            ))

    def _rule_new_vs_repeat(
        self, report: CoachingReport, nvr: dict, name: str
    ) -> None:
        """Assess whether performance differs against new vs repeat opponents."""
        gap  = nvr.get("gap")
        fp   = nvr.get("first_pct")
        rp   = nvr.get("repeat_pct")
        tier = nvr.get("confidence_tier", "")

        if gap is None or tier == "INSUFFICIENT":
            return

        if gap > NVR_MEANINGFUL:
            report.insights.append(Insight(
                category="mental",
                severity="INFO",
                headline=f"+{gap:.0f}pp advantage vs new opponents",
                detail=(
                    f"{name} wins {fp}% against first-time opponents vs {rp}% against those "
                    "they have faced before. This is normal at youth level — opponents adapt. "
                    "Develop a wider tactical repertoire so repeat opponents cannot prepare "
                    "a specific counter strategy."
                ),
                data_ref="nvr.gap",
                priority=6,
            ))
        elif gap < -NVR_MEANINGFUL:
            report.insights.append(Insight(
                category="mental",
                severity="CONCERN",
                headline=f"{abs(gap):.0f}pp disadvantage vs new opponents",
                detail=(
                    f"{name} wins only {fp}% against new opponents vs {rp}% against known ones. "
                    "This pattern suggests difficulty reading unfamiliar styles early in a bout. "
                    "Work on 'first bout intelligence' — gathering information in the initial exchange "
                    "before committing to an attacking scheme."
                ),
                data_ref="nvr.gap",
                priority=4,
            ))

    def _rule_trend(
        self, report: CoachingReport, trend: dict, name: str
    ) -> None:
        """Flag directional performance trend over recent events."""
        direction = trend.get("direction")
        delta     = trend.get("delta", 0)

        if direction is None or direction == "stable":
            return

        if direction == "up" and delta >= 8:
            report.insights.append(Insight(
                category="trend",
                severity="STRENGTH",
                headline=f"Strong upward trajectory — +{delta}pp in pool win rate",
                detail=(
                    f"{name} has improved pool win rate by {delta} percentage points across recent events. "
                    "This is a significant improvement signal. Identify what changed — new training focus, "
                    "physical development, or tactical adjustment — and reinforce it."
                ),
                data_ref="trend.delta",
                priority=4,
            ))
        elif direction == "down" and delta >= 8:
            report.insights.append(Insight(
                category="trend",
                severity="CONCERN",
                headline=f"Declining form — pool win rate down {delta}pp",
                detail=(
                    f"{name}'s pool win rate has declined by {delta} percentage points across recent events. "
                    "Investigate contributing factors: competition level increase, physical fatigue, "
                    "psychological factors, or a tactical pattern opponents have identified."
                ),
                data_ref="trend.delta",
                priority=2,
            ))

    def _rule_resilience(
        self, report: CoachingReport, resilience: dict
    ) -> None:
        """Assess come-from-behind bout performance."""
        score = resilience.get("resilience_score")
        tier  = resilience.get("confidence_tier", "")

        if score is None or tier == "INSUFFICIENT":
            return

        if score >= RESILIENCE_STRONG:
            report.insights.append(Insight(
                category="mental",
                severity="STRENGTH",
                headline=f"Resilient competitor — {score:.2f} resilience score",
                detail=(
                    f"A resilience score of {score:.2f} (maximum 1.0) indicates the fencer "
                    "performs relatively well even after early-bout deficits. "
                    "This composure under pressure is a key competitive differentiator."
                ),
                data_ref="resilience.resilience_score",
                priority=5,
            ))
        elif score < RESILIENCE_WEAK:
            report.insights.append(Insight(
                category="mental",
                severity="CONCERN",
                headline=f"Low resilience score — {score:.2f}",
                detail=(
                    f"A resilience score of {score:.2f} suggests the fencer struggles to recover "
                    "from early deficits. This is often a mental pattern rather than a purely "
                    "technical one. Introduce comeback training scenarios: practise from 0-3 down "
                    "and build the psychological habit of resetting after a deficit."
                ),
                data_ref="resilience.resilience_score",
                priority=3,
            ))

    def _rule_volatility(
        self, report: CoachingReport, volatility: dict, name: str
    ) -> None:
        """Flag high event-to-event win rate variance."""
        vol = volatility.get("win_pct_std")
        if vol is None:
            return

        if vol >= VOLATILITY_HIGH:
            report.insights.append(Insight(
                category="trend",
                severity="CONCERN",
                headline=f"Inconsistent performance — {vol:.1f}pp event-to-event swing",
                detail=(
                    f"{name} has a standard deviation of {vol:.1f} percentage points in pool win "
                    "rate across events. This high variance points to context-sensitivity: "
                    "likely performing well in comfortable environments and dropping off at "
                    "unfamiliar venues, higher-level competitions, or after long travel. "
                    "Build a consistent pre-competition routine to reduce contextual performance swings."
                ),
                data_ref="volatility.win_pct_std",
                priority=4,
            ))
        elif vol >= VOLATILITY_MODERATE:
            report.insights.append(Insight(
                category="trend",
                severity="INFO",
                headline=f"Some performance variability — {vol:.1f}pp event-to-event swing",
                detail=(
                    f"A standard deviation of {vol:.1f}pp is moderate. "
                    "The fencer has good events and weaker ones. "
                    "Tracking contextual factors (venue, competition level, warm-up quality) "
                    "would help identify which conditions produce peak performance."
                ),
                data_ref="volatility.win_pct_std",
                priority=7,
            ))

    def _rule_placement_progression(
        self, report: CoachingReport, placement: dict, name: str
    ) -> None:
        """Assess whether placement percentile is improving over time."""
        trend_delta = placement.get("trend_delta_pp")
        best_pct    = placement.get("best_pct")
        recent_avg  = placement.get("recent_avg_pct")
        career_avg  = placement.get("career_avg_pct")
        n           = placement.get("n", 0)

        if trend_delta is None or n < 5:
            return

        # Percentile: lower = better. Negative delta = improvement.
        if trend_delta < -TREND_STRONG:
            report.insights.append(Insight(
                category="trend",
                severity="STRENGTH",
                headline=f"Placement improving — {abs(trend_delta):.1f}pp gain in recent events",
                detail=(
                    f"Recent average placement percentile ({recent_avg}%) is {abs(trend_delta):.1f} "
                    f"percentage points better than career average ({career_avg}%). "
                    "The fencer is consistently finishing higher in the field. "
                    f"Best ever performance sits at {best_pct}% — a realistic near-term target."
                ),
                data_ref="placement_progression.trend_delta_pp",
                priority=4,
            ))
        elif trend_delta > TREND_NEGATIVE:
            report.insights.append(Insight(
                category="trend",
                severity="CONCERN",
                headline=f"Placement regressing — {trend_delta:.1f}pp worse in recent events",
                detail=(
                    f"Recent placement percentile ({recent_avg}%) has regressed {trend_delta:.1f}pp "
                    f"from career average ({career_avg}%). "
                    "This may reflect moving up an age category, competing at higher-level events, "
                    "or a form dip. Investigate with the athlete's broader training context."
                ),
                data_ref="placement_progression.trend_delta_pp",
                priority=3,
            ))

    def _rule_peer_benchmark(
        self, report: CoachingReport, peer: dict, name: str
    ) -> None:
        """Compare pool and DE win rates against age-category and weapon cohort."""
        cohort  = peer.get("cohort_size", 0)
        pool_r  = peer.get("pool_win_pct_rank")
        de_r    = peer.get("de_win_pct_rank")
        label   = peer.get("label", "")

        if cohort < 3 or pool_r is None:
            return

        if pool_r >= PEER_ELITE:
            report.insights.append(Insight(
                category="benchmark",
                severity="STRENGTH",
                headline=f"Top {100 - pool_r:.0f}% in cohort pool performance",
                detail=(
                    f"{name} is in the {pool_r:.0f}th percentile for pool win rate among "
                    f"{cohort} athletes in the same weapon and age category ({label}). "
                    "This benchmarks them as a leading performer in their cohort."
                ),
                data_ref="peer_benchmarks.pool_win_pct_rank",
                priority=5,
            ))
        elif pool_r < PEER_WEAK:
            report.insights.append(Insight(
                category="benchmark",
                severity="CONCERN",
                headline=f"Below cohort average — {pool_r:.0f}th percentile (pool win rate)",
                detail=(
                    f"Pool win rate places {name} at the {pool_r:.0f}th percentile of {cohort} "
                    f"athletes in the same weapon and age category ({label}). "
                    "Peer benchmarking confirms the pool phase is the primary development priority."
                ),
                data_ref="peer_benchmarks.pool_win_pct_rank",
                priority=2,
            ))

        if de_r is not None and de_r >= PEER_ELITE:
            report.insights.append(Insight(
                category="benchmark",
                severity="STRENGTH",
                headline=f"Elite DE performer in cohort — {de_r:.0f}th percentile",
                detail=(
                    f"DE win rate sits at the {de_r:.0f}th percentile of the same-cohort group. "
                    "This is a rare and valuable ability — excelling in elimination bouts predicts "
                    "podium finishes when pool phase is solid."
                ),
                data_ref="peer_benchmarks.de_win_pct_rank",
                priority=5,
            ))

    def _rule_close_bouts(
        self, report: CoachingReport, de_coaching: dict
    ) -> None:
        """Assess proportion of DE bouts decided by a close margin."""
        tier = de_coaching.get("confidence_tier", "")
        cbr  = de_coaching.get("close_bout_rate")

        if cbr is None or tier == "INSUFFICIENT":
            return

        if cbr >= CLOSE_HIGH:
            report.insights.append(Insight(
                category="de",
                severity="INFO",
                headline=f"Frequent tight DE bouts — {cbr:.0f}% within 2 touches",
                detail=(
                    f"{cbr:.0f}% of DE bouts are decided by 2 or fewer touches. "
                    "This high rate of close contests means small improvements in fitness, "
                    "priority period management, and mental composure have an outsized impact on results. "
                    "Prioritise these marginal-gain areas."
                ),
                data_ref="de_coaching.close_bout_rate",
                priority=6,
            ))

    def _rule_touch_efficiency(
        self, report: CoachingReport, de_coaching: dict
    ) -> None:
        """Compare average touches scored vs conceded in DE bouts."""
        tier   = de_coaching.get("confidence_tier", "")
        avg_ts = de_coaching.get("avg_ts")
        avg_tr = de_coaching.get("avg_tr")

        if avg_ts is None or avg_tr is None or tier == "INSUFFICIENT":
            return

        diff = round(avg_ts - avg_tr, 1)
        if diff >= 2.0:
            report.insights.append(Insight(
                category="de",
                severity="STRENGTH",
                headline=f"DE scoring efficiency — +{diff:.1f} average touch margin",
                detail=(
                    f"Averaging {avg_ts:.1f} touches scored vs {avg_tr:.1f} conceded in DE bouts "
                    "indicates a strong attacking conversion rate in the tableau format."
                ),
                data_ref="de_coaching.avg_ts",
                priority=6,
            ))
        elif diff <= -2.0:
            report.insights.append(Insight(
                category="de",
                severity="CONCERN",
                headline=f"DE scoring deficit — {diff:.1f} average touch margin",
                detail=(
                    f"Conceding {avg_tr:.1f} touches vs scoring {avg_ts:.1f} in DE bouts "
                    "reveals the fencer is being outscored in the tableau. "
                    "Defensive efficiency and attack quality under elimination pressure need attention."
                ),
                data_ref="de_coaching.avg_tr",
                priority=3,
            ))

    def _rule_annual_trend(
        self, report: CoachingReport, annual: list, name: str
    ) -> None:
        """Detect year-on-year improvement or regression from annual stats."""
        if len(annual) < 2:
            return

        sorted_years = sorted(annual, key=lambda r: r.get("year", 0))
        latest = sorted_years[-1]
        prev   = sorted_years[-2]

        pool_now  = latest.get("pool_win_pct")
        pool_prev = prev.get("pool_win_pct")
        de_now    = latest.get("de_win_pct")
        de_prev   = prev.get("de_win_pct")

        year_now  = latest.get("year", "current")
        year_prev = prev.get("year", "previous")

        if pool_now is not None and pool_prev is not None:
            delta = round(pool_now - pool_prev, 1)
            if delta >= 8:
                report.insights.append(Insight(
                    category="trend",
                    severity="STRENGTH",
                    headline=f"Year-on-year pool improvement — +{delta}pp ({year_prev}→{year_now})",
                    detail=(
                        f"Pool win rate has increased from {pool_prev}% to {pool_now}% year on year. "
                        "This multi-season progression confirms sustained development rather than a "
                        "one-off performance spike."
                    ),
                    data_ref="annual_stats",
                    priority=5,
                ))
            elif delta <= -8:
                report.insights.append(Insight(
                    category="trend",
                    severity="CONCERN",
                    headline=f"Year-on-year pool regression — {delta}pp ({year_prev}→{year_now})",
                    detail=(
                        f"Pool win rate has dropped from {pool_prev}% to {pool_now}% year on year. "
                        "This may reflect moving up an age group, higher competition level, or "
                        "a training programme issue. Review the transition in context with the athlete."
                    ),
                    data_ref="annual_stats",
                    priority=3,
                ))

        if de_now is not None and de_prev is not None:
            delta_de = round(de_now - de_prev, 1)
            if delta_de >= 10:
                report.insights.append(Insight(
                    category="de",
                    severity="STRENGTH",
                    headline=f"Strong DE year-on-year gain — +{delta_de}pp ({year_prev}→{year_now})",
                    detail=(
                        f"DE win rate grew from {de_prev}% to {de_now}% over the past year. "
                        "Improvement in elimination bouts typically lags pool development by 12-18 months, "
                        "so this is a highly positive sign."
                    ),
                    data_ref="annual_stats",
                    priority=5,
                ))

    def _rule_event_volume(
        self, report: CoachingReport, events: list, coverage: dict
    ) -> None:
        """Flag insufficient competition volume for meaningful development."""
        n_events = len(events)
        if n_events >= 10:
            return

        if n_events < 3:
            report.insights.append(Insight(
                category="coverage",
                severity="INFO",
                headline=f"Very low competition volume — {n_events} events recorded",
                detail=(
                    "With fewer than 3 events in the database, all insights should be treated "
                    "as highly provisional. Encourage the athlete to compete more regularly — "
                    "development requires volume of high-quality competitive exposure."
                ),
                data_ref="events",
                priority=0,
            ))
        elif n_events < 10:
            report.insights.append(Insight(
                category="coverage",
                severity="INFO",
                headline=f"Limited competition history — {n_events} events",
                detail=(
                    f"Only {n_events} events are recorded. Some statistical insights require "
                    "at least 20 events for reliable confidence. Metrics are presented with "
                    "appropriate confidence tiers."
                ),
                data_ref="events",
                priority=1,
            ))

    # ── Summary and priorities ────────────────────────────────────────────

    def _build_summary(self, report: CoachingReport, name: str) -> str:
        """Build a one-paragraph narrative summary from the top insights."""
        concerns   = [i for i in report.insights if i.severity == "CONCERN"]
        strengths  = [i for i in report.insights if i.severity == "STRENGTH"]

        parts: list[str] = []

        if strengths:
            top_str = strengths[0].headline.lower()
            parts.append(f"{name} shows clear strengths, most notably {top_str}.")

        if concerns:
            top_con = concerns[0].headline.lower()
            parts.append(f"The primary development priority is {top_con}.")

        if len(strengths) >= 2:
            others = "; ".join(i.headline.lower() for i in strengths[1:3])
            parts.append(f"Additional strengths include: {others}.")

        if len(concerns) >= 2:
            others = "; ".join(i.headline.lower() for i in concerns[1:3])
            parts.append(f"Secondary areas for improvement: {others}.")

        if not parts:
            parts.append(
                f"Insufficient data is available to generate a reliable summary for {name}. "
                "Run a full data refresh to populate coaching insights."
            )

        return " ".join(parts)

    def _build_priorities(self, report: CoachingReport) -> list[str]:
        """Return the top-3 actionable coaching priorities."""
        concerns = [i for i in report.insights if i.severity == "CONCERN"]
        priorities: list[str] = []

        for c in concerns[:3]:
            # Extract the first sentence of detail as a concise action item
            first_sentence = c.detail.split(".")[0].strip() + "."
            priorities.append(f"{c.headline}: {first_sentence}")

        if not priorities:
            # Fall back to top INFO items if no concerns
            for i in report.insights[:3]:
                if i.severity == "INFO":
                    priorities.append(i.headline)

        return priorities[:3]

    def _coverage_note(self, coverage: dict) -> str:
        """Return a short coverage quality note for display."""
        tier = coverage.get("coverage_tier", "UNKNOWN")
        if tier == "FULL":
            return "Data quality: Full coverage — all metrics at maximum reliability."
        elif tier == "GOOD":
            return "Data quality: Good — most metrics are reliable."
        elif tier == "PARTIAL":
            return "Data quality: Partial — some insights may have limited sample sizes."
        elif tier in ("LOW", "INSUFFICIENT"):
            return "Data quality: Low — insights are indicative only; run a full refresh."
        return "Data quality: Unknown."
