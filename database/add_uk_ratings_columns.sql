-- ═══════════════════════════════════════════════════════════════
-- Migration: add UK Ratings columns for full event + DE scraping
-- Run once in Supabase SQL Editor.  Safe to re-run (IF NOT EXISTS / idempotent).
-- ═══════════════════════════════════════════════════════════════

-- ── athletes: weapon code so the scraper knows which URL to call ──
ALTER TABLE athletes
  ADD COLUMN IF NOT EXISTS uk_ratings_weapon_code integer;

COMMENT ON COLUMN athletes.uk_ratings_weapon_code
  IS 'UK Ratings weapon code: 34=foil, 35=epee, 36=sabre';

-- ── events: link each event back to the UK Ratings tourney detail ID ──
-- One athlete at one event = one tourney detail ID on UK Ratings.
-- This becomes the primary key for fast idempotent upserts.
ALTER TABLE events
  ADD COLUMN IF NOT EXISTS uk_ratings_tourney_id integer;

CREATE INDEX IF NOT EXISTS idx_events_uk_tourney
  ON events(uk_ratings_tourney_id);

COMMENT ON COLUMN events.uk_ratings_tourney_id
  IS 'UK Ratings /tourneys/tourneydetail/{id} — unique per athlete-event';

-- ── de_bouts: add unique constraint to prevent duplicate insertions ──
-- Covers the case where the scraper is re-run; uses ON CONFLICT DO NOTHING.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'de_bouts_event_round_opponent_key'
  ) THEN
    ALTER TABLE de_bouts
      ADD CONSTRAINT de_bouts_event_round_opponent_key
      UNIQUE (event_id, round, opponent_name);
  END IF;
END $$;

-- ── Seed Daniel Panga's UK Ratings identifiers ─────────────────────
UPDATE athletes
SET
  uk_ratings_id          = 62457,
  uk_ratings_weapon_code = 34
WHERE name_display ILIKE '%panga%'
  AND (uk_ratings_id IS NULL OR uk_ratings_weapon_code IS NULL);

-- ── Verify ─────────────────────────────────────────────────────────
SELECT
  name_display,
  uk_ratings_id,
  uk_ratings_weapon_code,
  weapon
FROM athletes
WHERE name_display ILIKE '%panga%';
