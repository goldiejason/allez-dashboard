"""
core/attributor.py — Bout Attribution Engine

Resolves a UK Ratings DE bout (which carries only tournament name + round)
to the correct event row in the database.

The attribution uses a composite key rather than tournament name alone:

  Primary key:   normalised_tournament_name + athlete_weapon
  Secondary key: normalised_tournament_name + event_name word-set

When the primary key is still ambiguous (rare — requires same athlete to
fence multiple weapons at the same tournament) the attributor falls back
to the first matching event and emits a structured WARNING rather than a
silent arbitrary assignment (the old AMB-1 behaviour).

When NO event is found for a tournament (AMB-2), the bout is written to
the staged_bouts table instead of being silently discarded.  A nightly
reconciliation pass (scripts/reconcile_staged_bouts.py) retries staged
bouts as the tournament database grows.

Usage:
    from core.attributor import BoutAttributor
    attributor = BoutAttributor(db, athlete_id, weapon, competition_history, event_id_by_ukr)
    event_id = attributor.resolve(bout)
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


# ── Resolution result ──────────────────────────────────────────────────────

@dataclass
class AttributionResult:
    event_id:    str
    strategy:    str   # "ukr_id" | "weapon_filter" | "first_match" | "staged"
    ambiguous:   bool
    event_count: int   # how many events matched the tournament name


# ── BoutAttributor ─────────────────────────────────────────────────────────

class BoutAttributor:
    """
    Resolves DE bouts to event rows using weapon-aware disambiguation.

    Parameters
    ----------
    db                : Supabase write client
    athlete_id        : UUID string
    weapon            : athlete's weapon ("foil", "epee", "sabre")
    competition_history: parsed competition history rows from UK Ratings
                         (list of dicts with keys: uk_tourney_id, tournament_name,
                          event_name, placement, field_size)
    event_id_by_ukr   : mapping {uk_ratings_tourney_id (int) → event_id (str)}
    """

    def __init__(
        self,
        db,
        athlete_id: str,
        weapon: str,
        competition_history: list[dict],
        event_id_by_ukr: dict[int, str],
    ):
        self.db          = db
        self.athlete_id  = athlete_id
        self.weapon      = (weapon or "").lower()
        self._tourney_to_events = self._build_tourney_map(competition_history, event_id_by_ukr)

    # ── Internal: build the event map ─────────────────────────────────────

    @staticmethod
    def _normalise(name: str) -> str:
        """Match normalisation used in ukratings_collector._normalize_tourney_name."""
        name = re.sub(r"\s+U\d+[A-Z](?:,\s*U\d+[A-Z])*\s*$", "", name)
        m = re.match(r"^Event\s+(\d+)\s*[-–]\s*(.+)$", name.strip())
        if m:
            name = f"{m.group(2).strip()} – Event {m.group(1)}"
        name = name.replace("–", "-").replace("—", "-")
        name = re.sub(r"\b(\d{2})/(\d{2})\b", r"\1-\2", name)
        name = re.sub(
            r"\b(20\d{2})-(20\d{2})\b",
            lambda x: f"{x.group(1)[2:]}-{x.group(2)[2:]}",
            name,
        )
        return name.strip().lower()

    def _build_tourney_map(
        self,
        competition_history: list[dict],
        event_id_by_ukr: dict[int, str],
    ) -> dict[str, list[dict]]:
        """
        Build:  normalised_tournament_name → list of {event_id, event_name}

        Each entry carries the event_name (e.g. "U-14 Men's Foil") so
        weapon-based disambiguation can compare against it.
        """
        mapping: dict[str, list[dict]] = {}
        for comp in competition_history:
            event_id = event_id_by_ukr.get(comp["uk_tourney_id"])
            if not event_id:
                continue
            norm = self._normalise(comp["tournament_name"])
            if norm not in mapping:
                mapping[norm] = []
            entry = {"event_id": event_id, "event_name": comp.get("event_name", "")}
            # deduplicate by event_id
            if not any(e["event_id"] == event_id for e in mapping[norm]):
                mapping[norm].append(entry)
        return mapping

    # ── Disambiguation helpers ────────────────────────────────────────────

    def _filter_by_weapon(self, candidates: list[dict]) -> list[dict]:
        """
        Narrow candidates to those whose event_name contains the athlete's weapon.
        e.g. weapon='foil', event_names=['U-14 Men\'s Foil', 'U-14 Men\'s Epee']
        → returns only the Foil entry.
        """
        if not self.weapon:
            return candidates
        weapon_filtered = [
            c for c in candidates
            if self.weapon in c["event_name"].lower()
        ]
        return weapon_filtered if weapon_filtered else candidates

    # ── Stage unmatched bout ──────────────────────────────────────────────

    def _stage_bout(self, bout: dict, reason: str) -> None:
        """
        Write an unattributed DE bout to staged_bouts for later reconciliation.
        Silently no-ops if the table is absent.
        """
        try:
            self.db.table("staged_bouts").upsert({
                "athlete_id":       self.athlete_id,
                "opponent_name":    bout.get("opponent_name", ""),
                "tournament_name":  bout.get("tournament_name", ""),
                "round_text":       bout.get("round_text", ""),
                "ts":               bout.get("ts"),
                "tr":               bout.get("tr"),
                "result":           bout.get("result"),
                "reason":           reason,
            }, on_conflict="athlete_id,opponent_name,tournament_name,round_text").execute()
            logger.info(
                f"  Staged unattributed DE bout: "
                f"{bout.get('opponent_name')} @ '{bout.get('tournament_name')}' "
                f"({bout.get('round_text')}) — reason: {reason}"
            )
        except Exception as exc:
            logger.warning(
                f"  staged_bouts write failed ({exc}) — "
                f"DE bout vs '{bout.get('opponent_name')}' @ "
                f"'{bout.get('tournament_name')}' is unattributed"
            )

    # ── Public API ────────────────────────────────────────────────────────

    def resolve(self, bout: dict) -> Optional[AttributionResult]:
        """
        Resolve a parsed DE bout dict to an event_id.

        The bout dict must contain:
          tournament_name, opponent_name, round_text, ts, tr, result

        Returns AttributionResult or None (bout staged).
        """
        norm = self._normalise(bout["tournament_name"])
        candidates = self._tourney_to_events.get(norm, [])

        # ── No candidates → stage and return None ────────────────────────
        if not candidates:
            self._stage_bout(
                bout,
                reason=f"tournament '{bout['tournament_name']}' not in events DB",
            )
            return None

        # ── Single candidate → unambiguous ───────────────────────────────
        if len(candidates) == 1:
            return AttributionResult(
                event_id    = candidates[0]["event_id"],
                strategy    = "ukr_id",
                ambiguous   = False,
                event_count = 1,
            )

        # ── Multiple candidates → try weapon-based disambiguation ────────
        narrowed = self._filter_by_weapon(candidates)

        if len(narrowed) == 1:
            logger.debug(
                f"  BoutAttributor: weapon disambiguation resolved "
                f"'{bout['tournament_name']}' → {narrowed[0]['event_name']}"
            )
            return AttributionResult(
                event_id    = narrowed[0]["event_id"],
                strategy    = "weapon_filter",
                ambiguous   = False,
                event_count = len(candidates),
            )

        # ── Still ambiguous after weapon filter → use first + warn ───────
        event_id = narrowed[0]["event_id"]
        event_names = [c["event_name"] for c in candidates]
        logger.warning(
            f"  Ambiguous tournament '{bout['tournament_name']}' maps to "
            f"{len(candidates)} events {event_names} — "
            f"DE bout vs '{bout['opponent_name']}' ({bout['round_text']}) "
            f"filed under '{narrowed[0]['event_name']}' ({event_id[:8]}…). "
            f"Manual check advised."
        )
        return AttributionResult(
            event_id    = event_id,
            strategy    = "first_match",
            ambiguous   = True,
            event_count = len(candidates),
        )
