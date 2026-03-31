-- ============================================================
-- Cleanup: corrupted events with date-string event_names
-- ============================================================
-- Root cause: _parse_competition_history was using a hardcoded
-- tables[1] index.  For some athletes this resolved to the
-- "Upcoming Registered Events" table (headers: Tournament Name,
-- Start Date, Event Name, Difficulty) rather than the actual
-- Competition History table.  This stored the Start Date column
-- value (e.g. "11.04.2026") as the event_name.
--
-- Fixed in ukratings_collector.py: content-based table detection.
-- Run this once to remove the 43 corrupted rows, then re-run
-- UK Ratings collection for the affected athletes.
--
-- Affected athletes (43 corrupted events total):
--   Michael Malasenkovs  — 19 events
--   Daniel Polyakov      —  6 events
--   Jagroop Shergill     —  6 events
--   Ajith Badhrinath     —  4 events
--   Dexter Taylor        —  4 events
--   Mustafa Akhtar       —  2 events
--   Teodora Petre        —  2 events
--
-- Safe to run: these corrupted events have no pool_bouts or
-- de_bouts attached (they were future/unplayed registrations).
-- ============================================================

-- Preview first (should return 43 rows):
SELECT id, athlete_id, event_name, date, uk_ratings_tourney_id
FROM events
WHERE event_name ~ '^\d{2}\.\d{2}\.\d{4}$'
ORDER BY athlete_id, date;

-- Delete (uncomment when ready):
-- DELETE FROM events
-- WHERE event_name ~ '^\d{2}\.\d{2}\.\d{4}$';

-- After running the delete, trigger a manual UK Ratings refresh
-- for the 7 affected athletes from the dashboard, or wait for
-- the next scheduled Saturday/Sunday run.
