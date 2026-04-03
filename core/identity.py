"""
core/identity.py — Identity Resolution Service

Resolves an external source name (FTL pool page, event results page) to
a confirmed athlete identity using a priority-ordered strategy chain:

  Strategy 1 — Exact word-set match
    All words in the stored name_ftl appear in the candidate name.
    Zero-cost, zero false positives.  e.g. "PANGA Daniel" is in
    {"PANGA", "Daniel"} ⊆ {"PANGA", "Daniel"} ✓

  Strategy 2 — Surname-only prefix match
    The first token of name_ftl (the surname) matches the first token
    of the candidate.  Handles middle-initial variants:
    name_ftl="PANGA Daniel", candidate="PANGA Daniel J" ✓

  Strategy 3 — Normalised fuzzy match
    SequenceMatcher on Unicode-normalised, lower-cased tokens.
    Threshold is configurable (default 0.82) — conservative enough to
    avoid false positives while catching common variants:
    "MALASENKOVS Michael" vs "MALASENKOV Michael" ✓

  Strategy 4 — Alias lookup
    If this (source, source_name) pair has been confirmed before, the
    match result is retrieved from the athlete_aliases table instantly.
    Strategy 4 is checked FIRST — it is listed last only for conceptual
    clarity.

When a fuzzy match (Strategy 3) produces a new positive, the result is
written to athlete_aliases so future runs use the O(1) alias path.

When no match is found, the failure is recorded in unresolved_identities
for manual review and incremental correction.

Usage:
    from core.identity import IdentityResolver
    resolver = IdentityResolver(db)
    match = resolver.find_in_list(
        target_name="PANGA Daniel",
        candidates=["PANGA Daniel J", "SMITH John", "JONES Amy"],
        context="pool_data",
        athlete_id="uuid-of-panga",
    )
    # match.index  → 0
    # match.score  → 1.0
    # match.strategy → "word_set"
"""

import logging
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Optional

logger = logging.getLogger(__name__)

# Minimum fuzzy similarity to accept a match
FUZZY_THRESHOLD = 0.82


# ── Match result ───────────────────────────────────────────────────────────

@dataclass
class MatchResult:
    index:    int            # position in the candidates list
    name:     str            # the matched candidate name
    score:    float          # similarity score (1.0 = exact)
    strategy: str            # "alias" | "word_set" | "surname" | "fuzzy"


# ── Normalisation helpers ──────────────────────────────────────────────────

def _ascii_lower(text: str) -> str:
    """Strip Unicode accents and lower-case.  'Élodie' → 'elodie'."""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()


def _tokens(text: str) -> set[str]:
    return set(_ascii_lower(text).split())


def _fuzzy(a: str, b: str) -> float:
    return SequenceMatcher(None, _ascii_lower(a), _ascii_lower(b)).ratio()


# ── Core matching strategies (stateless, no DB) ───────────────────────────

def match_word_set(target: str, candidate: str) -> bool:
    """
    All tokens of target must appear in candidate (case-insensitive, ASCII).
    This is the original _name_matches logic, extracted here for reuse.
    """
    return _tokens(target).issubset(_tokens(candidate))


def match_surname(target: str, candidate: str) -> bool:
    """
    First token of target matches first token of candidate.
    Handles 'PANGA Daniel' vs 'PANGA Daniel J' (middle-initial variant).
    """
    t_parts = _ascii_lower(target).split()
    c_parts = _ascii_lower(candidate).split()
    return bool(t_parts) and bool(c_parts) and t_parts[0] == c_parts[0]


def match_fuzzy(target: str, candidate: str, threshold: float = FUZZY_THRESHOLD) -> tuple[bool, float]:
    """Return (matched, score) for a fuzzy name match."""
    score = _fuzzy(target, candidate)
    return score >= threshold, score


# ── IdentityResolver (stateful — uses DB for alias cache) ─────────────────

class IdentityResolver:
    """
    Stateful resolver that wraps the four strategies and manages the alias
    and unresolved-identity tables.

    Pass a Supabase write client on construction.  The DB interactions are
    fully optional — if the athlete_aliases or unresolved_identities tables
    do not exist (pre-migration), the resolver degrades to pure in-memory
    matching with no persistence.
    """

    def __init__(self, db, fuzzy_threshold: float = FUZZY_THRESHOLD):
        self.db = db
        self.threshold = fuzzy_threshold
        # In-memory alias cache: (source, source_name) → confirmed_name
        self._alias_cache: dict[tuple[str, str], str] = {}
        self._cache_loaded = False

    # ── Internal: alias table ────────────────────────────────────────────

    def _load_aliases(self) -> None:
        """
        Populate in-memory alias cache from the athlete_aliases table.
        Called once on first use.  Gracefully no-ops if the table is absent.
        """
        if self._cache_loaded:
            return
        try:
            rows = (
                self.db.table("athlete_aliases")
                .select("source, source_name, confirmed_name")
                .limit(10000)
                .execute()
                .data or []
            )
            for row in rows:
                key = (row["source"], row["source_name"])
                self._alias_cache[key] = row["confirmed_name"]
            logger.debug(f"IdentityResolver: loaded {len(rows)} aliases from DB")
        except Exception as exc:
            logger.debug(f"IdentityResolver: alias table not available ({exc}) — using in-memory only")
        self._cache_loaded = True

    def _save_alias(self, source: str, source_name: str, confirmed_name: str) -> None:
        """
        Persist a newly discovered fuzzy match to athlete_aliases.
        Silently no-ops if the table is absent.
        """
        key = (source, source_name)
        if key in self._alias_cache:
            return  # already known
        self._alias_cache[key] = confirmed_name
        try:
            self.db.table("athlete_aliases").upsert({
                "source":         source,
                "source_name":    source_name,
                "confirmed_name": confirmed_name,
            }, on_conflict="source,source_name").execute()
            logger.info(
                f"IdentityResolver: saved alias [{source}] "
                f"'{source_name}' → '{confirmed_name}'"
            )
        except Exception as exc:
            logger.debug(f"IdentityResolver: alias save failed ({exc})")

    def _record_unresolved(
        self,
        source: str,
        source_name: str,
        context: str,
        best_candidate: Optional[str],
        best_score: float,
    ) -> None:
        """
        Write an unresolved identity to the unresolved_identities table.
        Silently no-ops if the table is absent.
        """
        try:
            self.db.table("unresolved_identities").upsert({
                "source":         source,
                "source_name":    source_name,
                "context":        context,
                "best_candidate": best_candidate,
                "best_score":     round(best_score, 3),
            }, on_conflict="source,source_name,context").execute()
        except Exception:
            pass  # table may not exist yet; the log warning is sufficient

    # ── Public API ────────────────────────────────────────────────────────

    def find_in_list(
        self,
        target: str,
        candidates: list[str],
        source: str = "ftl",
        context: str = "",
        athlete_id: Optional[str] = None,
    ) -> Optional[MatchResult]:
        """
        Find target in a list of candidate names.

        Args:
            target:     The name we are looking for (e.g. 'PANGA Daniel').
            candidates: List of names from the external source.
            source:     Source identifier for alias persistence ('ftl', 'ftl_pool').
            context:    Free-text context for unresolved logging (event name, etc.).
            athlete_id: Optional, for logging / future enrichment.

        Returns:
            MatchResult or None if no match found.
        """
        if not candidates:
            return None

        self._load_aliases()

        # ── Strategy 0: alias lookup (fastest) ──────────────────────────
        cached = self._alias_cache.get((source, target))
        if cached:
            for i, c in enumerate(candidates):
                if match_word_set(cached, c) or match_word_set(c, cached):
                    return MatchResult(index=i, name=c, score=1.0, strategy="alias")

        # ── Strategy 1: exact word-set ───────────────────────────────────
        for i, c in enumerate(candidates):
            if match_word_set(target, c):
                return MatchResult(index=i, name=c, score=1.0, strategy="word_set")

        # ── Strategy 2: surname-only prefix ─────────────────────────────
        for i, c in enumerate(candidates):
            if match_surname(target, c):
                logger.debug(
                    f"IdentityResolver: surname match '{target}' → '{c}' "
                    f"(context={context})"
                )
                return MatchResult(index=i, name=c, score=0.9, strategy="surname")

        # ── Strategy 3: fuzzy ────────────────────────────────────────────
        best_score = 0.0
        best_idx   = -1
        for i, c in enumerate(candidates):
            ok, score = match_fuzzy(target, c, self.threshold)
            if score > best_score:
                best_score = score
                best_idx   = i

        if best_idx >= 0 and best_score >= self.threshold:
            matched = candidates[best_idx]
            logger.info(
                f"IdentityResolver: fuzzy match '{target}' → '{matched}' "
                f"(score={best_score:.3f}, context={context})"
            )
            self._save_alias(source, target, matched)
            return MatchResult(index=best_idx, name=matched, score=best_score, strategy="fuzzy")

        # ── No match ─────────────────────────────────────────────────────
        best_candidate = candidates[best_idx] if best_idx >= 0 else None
        logger.warning(
            f"IdentityResolver: '{target}' not found in {len(candidates)} candidates "
            f"(best score={best_score:.3f}, context={context})"
        )
        self._record_unresolved(
            source=source,
            source_name=target,
            context=context,
            best_candidate=best_candidate,
            best_score=best_score,
        )
        return None
