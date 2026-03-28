-- ═══════════════════════════════════════════════════════════════
-- Migration: add FTL pool aggregate columns to events table
-- Run once in Supabase SQL Editor.  Safe to re-run (IF NOT EXISTS / idempotent).
--
-- These columns are written by collectors/ftl_collector.py via the
--   /pools/results/data/{event_id}/{pool_id_seed}  API endpoint.
-- Without them the FTL collector writes silently fail and the dashboard
-- shows dashes for all pool metrics.
-- ═══════════════════════════════════════════════════════════════

-- ── Pool ID seed ───────────────────────────────────────────────
-- Cached from /events/results/{event_id} HTML; used as the second
-- path segment when fetching pool standings.
ALTER TABLE events
  ADD COLUMN IF NOT EXISTS pool_id_seed text;

COMMENT ON COLUMN events.pool_id_seed
  IS 'FTL pool ID seed — discovered once from the event HTML, cached for re-runs';

-- ── Pool aggregate stats ───────────────────────────────────────
-- Collected from /pools/results/data/{event_id}/{pool_id_seed}.
-- Represent the athlete''s totals across all pools in the event.

ALTER TABLE events
  ADD COLUMN IF NOT EXISTS pool_v integer;    -- victories
COMMENT ON COLUMN events.pool_v
  IS 'Pool victories (wins in pool round)';

ALTER TABLE events
  ADD COLUMN IF NOT EXISTS pool_l integer;    -- losses
COMMENT ON COLUMN events.pool_l
  IS 'Pool losses (bouts lost in pool round)';

ALTER TABLE events
  ADD COLUMN IF NOT EXISTS pool_ts integer;   -- touches scored
COMMENT ON COLUMN events.pool_ts
  IS 'Pool touches scored (total across all pool bouts)';

ALTER TABLE events
  ADD COLUMN IF NOT EXISTS pool_tr integer;   -- touches received
COMMENT ON COLUMN events.pool_tr
  IS 'Pool touches received (total across all pool bouts)';

ALTER TABLE events
  ADD COLUMN IF NOT EXISTS pool_ind integer;  -- indicator = ts - tr
COMMENT ON COLUMN events.pool_ind
  IS 'Pool indicator (touches scored minus touches received)';

ALTER TABLE events
  ADD COLUMN IF NOT EXISTS advanced_to_de boolean;
COMMENT ON COLUMN events.advanced_to_de
  IS 'Whether the athlete advanced from pools to the DE tableau';

-- ── Verify ─────────────────────────────────────────────────────
SELECT
  column_name,
  data_type,
  is_nullable
FROM information_schema.columns
WHERE table_name = 'events'
  AND column_name IN (
    'pool_id_seed', 'pool_v', 'pool_l',
    'pool_ts', 'pool_tr', 'pool_ind', 'advanced_to_de'
  )
ORDER BY column_name;
