-- ═══════════════════════════════════════════════════════════════
-- Allez Fencing Club — Full Athlete Roster Seed
-- 43 athletes (10 Juniors + 33 Seniors) as of March 2026.
--
-- UK Ratings IDs discovered 2026-03-27 via /search/ endpoint.
-- Safe to re-run (idempotent).
--
-- ⚠  TWO AMBIGUOUS PROFILES — please verify before running:
--
--   Mustafa Akhtar  : used uk_id=58629 (BF 150458, more recent).
--                     Other candidate: uk_id=53622 (BF 145467).
--                     Check: ukratings.co.uk/tourneys/athletedetail/34/58629/None
--
--   Jasper Kidd     : used uk_id=65493 (BF 157320, more recent).
--                     Other candidate: uk_id=39848 (BF 131574).
--                     Check: ukratings.co.uk/tourneys/athletedetail/34/65493/None
--
-- Athletes NOT found on UK Ratings (not BF-registered or not on foil):
--   Oscar Clifforth, Bo Dickinson, Sebastian Hoyos, Thomas Kerslake,
--   Zach Lohia, Tomiwa Martins-Afolabi, Jonny Sullivan, Alistair Dedman,
--   Joshua Elsworth, Ted Hales-Green, Nehaa Hampanna, Elliott Heap,
--   Daniel Juras, Prady Kathiseran, Sid Kathiseran, Archie Lueng,
--   Arya Santhanam, Raghav Sharma, Tommy Stevens, Sam Trower, Max Young.
-- ═══════════════════════════════════════════════════════════════


-- ── Step 1: Athletes WITH UK Ratings IDs ────────────────────────
-- Uses ON CONFLICT (uk_ratings_id) so existing records are updated
-- rather than duplicated.

INSERT INTO athletes
  (name_display, weapon, club, active, uk_ratings_id, uk_ratings_weapon_code)
VALUES
  -- Juniors
  ('Alfie Hobbs',          'foil', 'Allez Fencing', true, 71918, 34),
  ('Tobias Love',          'foil', 'Allez Fencing', true, 76002, 34),
  ('Mikael Wasi',          'foil', 'Allez Fencing', true, 72059, 34),
  -- Seniors
  ('Anita Abramenkova',    'foil', 'Allez Fencing', true, 47360, 34),
  ('Sadit Ahammod',        'foil', 'Allez Fencing', true, 63419, 34),
  ('Mustafa Akhtar',       'foil', 'Allez Fencing', true, 58629, 34),
  ('Ajith Badhrinath',     'foil', 'Allez Fencing', true, 65339, 34),
  ('Matthew Bajulaiye',    'foil', 'Allez Fencing', true, 74934, 34),
  ('Yusuf Coates',         'foil', 'Allez Fencing', true, 56721, 34),
  ('Jacob Courtney',       'foil', 'Allez Fencing', true, 62734, 34),
  ('Felix Fetherston',     'foil', 'Allez Fencing', true, 59945, 34),
  ('Gerardo Gonnella',     'foil', 'Allez Fencing', true, 76852, 34),
  ('Grayson Harper',       'foil', 'Allez Fencing', true, 62397, 34),
  ('Dominik Juras',        'foil', 'Allez Fencing', true, 47946, 34),
  ('Jasper Kidd',          'foil', 'Allez Fencing', true, 65493, 34),
  ('Finley Lethbridge',    'foil', 'Allez Fencing', true, 65916, 34),
  ('Michael Malasenkovs',  'foil', 'Allez Fencing', true, 55457, 34),
  ('Teodora Petre',        'foil', 'Allez Fencing', true, 64924, 34),
  ('Daniel Polyakov',      'foil', 'Allez Fencing', true, 56363, 34),
  ('Jagroop Shergill',     'foil', 'Allez Fencing', true, 44876, 34),
  ('Dexter Taylor',        'foil', 'Allez Fencing', true, 62247, 34),
  -- Daniel Panga (already in DB; this upsert keeps his data intact)
  ('Daniel Jason Panga',   'foil', 'Allez Fencing', true, 62457, 34)
ON CONFLICT (uk_ratings_id) DO UPDATE SET
  club                   = EXCLUDED.club,
  active                 = EXCLUDED.active,
  uk_ratings_weapon_code = EXCLUDED.uk_ratings_weapon_code;


-- ── Step 2: Athletes WITHOUT UK Ratings IDs ─────────────────────
-- Insert only if a record with that display name doesn't already exist.

INSERT INTO athletes (name_display, weapon, club, active)
SELECT v.name_display, 'foil'::text, 'Allez Fencing'::text, true
FROM (VALUES
  -- Juniors
  ('Oscar Clifforth'),
  ('Bo Dickinson'),
  ('Sebastian Hoyos'),
  ('Thomas Kerslake'),
  ('Zach Lohia'),
  ('Tomiwa Martins-Afolabi'),
  ('Jonny Sullivan'),
  -- Seniors
  ('Alistair Dedman'),
  ('Joshua Elsworth'),
  ('Ted Hales-Green'),
  ('Nehaa Hampanna'),
  ('Elliott Heap'),
  ('Daniel Juras'),
  ('Prady Kathiseran'),
  ('Sid Kathiseran'),
  ('Archie Lueng'),
  ('Arya Santhanam'),
  ('Raghav Sharma'),
  ('Tommy Stevens'),
  ('Sam Trower'),
  ('Max Young')
) AS v(name_display)
WHERE NOT EXISTS (
  SELECT 1 FROM athletes a WHERE a.name_display = v.name_display
);


-- ── Verify ───────────────────────────────────────────────────────
SELECT
  name_display,
  weapon,
  CASE
    WHEN uk_ratings_id IS NOT NULL THEN '✓ uk_id=' || uk_ratings_id::text
    ELSE '— no UK Ratings'
  END AS uk_ratings_status
FROM athletes
WHERE club = 'Allez Fencing'
ORDER BY
  CASE WHEN uk_ratings_id IS NOT NULL THEN 0 ELSE 1 END,
  name_display;
