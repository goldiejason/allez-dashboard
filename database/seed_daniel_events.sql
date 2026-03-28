-- ═══════════════════════════════════════════════════════════════
-- Allez Dashboard — Seed Daniel Panga's 21 known competition events
-- Run this in the Supabase SQL Editor.
-- Safe to re-run: tournaments upsert on name; events upsert on
--   (athlete_id, tournament_id, event_name).
-- ftl_event_id is preserved if already set (not overwritten with NULL).
-- ═══════════════════════════════════════════════════════════════

-- ── Step 0: add unique constraint on tournament name (safe) ─────────────────
DO $$
BEGIN
  ALTER TABLE tournaments ADD CONSTRAINT tournaments_name_key UNIQUE (name);
EXCEPTION WHEN duplicate_table OR duplicate_object THEN
  NULL; -- already exists
END;
$$;

-- ── Step 1: upsert all tournaments ───────────────────────────────────────────
INSERT INTO tournaments (name, date_start, date_end, country, city, is_international, circuit) VALUES
  ('ERYC & BYCQ 2024 Foil',                        '2024-01-27', '2024-01-27', 'GBR', 'England',    false, 'ERYC'),
  ('British Youth Championships 2024',              '2024-05-04', '2024-05-06', 'GBR', 'England',    false, 'BYC'),
  ('LPJS Cambridge Sword Foil',                     '2024-09-22', '2024-09-22', 'GBR', 'Cambridge',  false, 'LPJS'),
  ('FCL LPJS November 2024',                        '2024-11-24', '2024-11-24', 'GBR', 'England',    false, 'LPJS'),
  ('Cambridge Sword Series 24/25 – Event 1',        '2024-12-08', '2024-12-08', 'GBR', 'Cambridge',  false, 'Cambridge Sword Series'),
  ('ERYC & BYCQ 2025 Foil',                        '2025-01-25', '2025-01-25', 'GBR', 'England',    false, 'ERYC'),
  ('Cambridge Foil Series 2024–25 – Event 2',       '2025-03-02', '2025-03-02', 'GBR', 'Cambridge',  false, 'Cambridge Sword Series'),
  ('LPJS London Foil',                              '2025-03-22', '2025-03-23', 'GBR', 'London',     false, 'LPJS'),
  ('British Youth Championships 2025',              '2025-05-03', '2025-05-05', 'GBR', 'England',    false, 'BYC'),
  ('Cambridge Sword Series 24/25 – Event 3',        '2025-06-01', '2025-06-01', 'GBR', 'Cambridge',  false, 'Cambridge Sword Series'),
  ('LPJS Cambridge Sword',                          '2025-10-18', '2025-10-18', 'GBR', 'Cambridge',  false, 'LPJS'),
  ('Leon Paul U14 Open',                            '2025-11-15', '2025-11-16', 'GBR', 'London',     false, NULL),
  ('LPJS FCL Foil',                                 '2025-11-23', '2025-11-23', 'GBR', 'England',    false, 'LPJS'),
  ('LPJS Cambridge Christmas Challenge Foil',       '2025-12-06', '2025-12-06', 'GBR', 'Cambridge',  false, 'LPJS'),
  ('Eastern Region Youth Championships',            '2026-01-24', '2026-01-25', 'GBR', 'England',    false, 'ERYC'),
  ('St Benedict''s LPJS Foil 2026',                 '2026-02-21', '2026-02-21', 'GBR', 'England',    false, 'LPJS'),
  ('Newham Swords Junior Foil Series 25/26 – Event 4', '2026-03-07', '2026-03-07', 'GBR', 'London', false, NULL),
  ('Mini Marathon 2025',                            '2025-06-27', '2025-06-29', 'FRA', 'Paris',      true,  NULL),
  ('Challenge Wratislavia 2025',                    '2025-03-27', '2025-03-31', 'POL', 'Wrocław',    true,  NULL),
  ('Challenge Wratislavia 2026',                    '2026-03-19', '2026-03-23', 'POL', 'Wrocław',    true,  NULL)
ON CONFLICT (name) DO UPDATE SET
  date_start       = EXCLUDED.date_start,
  date_end         = EXCLUDED.date_end,
  country          = EXCLUDED.country,
  city             = EXCLUDED.city,
  is_international = EXCLUDED.is_international,
  circuit          = EXCLUDED.circuit;

-- ── Step 2: upsert Daniel's events ───────────────────────────────────────────
-- Daniel's athlete UUID: be990426-d27c-482f-be0b-33bb737b3a34
-- ftl_event_id: only set where confirmed; COALESCE keeps existing value on re-run.

INSERT INTO events (athlete_id, tournament_id, event_name, date, placement, weapon, ftl_event_id)
SELECT
  'be990426-d27c-482f-be0b-33bb737b3a34',
  t.id,
  e.event_name,
  e.date::date,
  e.placement,
  'foil',
  e.ftl_event_id
FROM (VALUES
  -- England — U-12 era
  ('ERYC & BYCQ 2024 Foil',                        'U-12 Men''s Foil',  '2024-01-27',  5,  NULL::text),
  ('British Youth Championships 2024',              'U-12 Men''s Foil',  '2024-05-04',  38, NULL),
  ('LPJS Cambridge Sword Foil',                     'U-12 Men''s Foil',  '2024-09-22',  19, NULL),
  ('FCL LPJS November 2024',                        'U-12 Men''s Foil',  '2024-11-24',  28, NULL),
  ('Cambridge Sword Series 24/25 – Event 1',        'U-12 Men''s Foil',  '2024-12-08',  7,  NULL),
  ('ERYC & BYCQ 2025 Foil',                        'U-12 Men''s Foil',  '2025-01-25',  3,  NULL),
  ('Cambridge Foil Series 2024–25 – Event 2',       'U-12 Men''s Foil',  '2025-03-02',  19, NULL),
  ('LPJS London Foil',                              'U-12 Men''s Foil',  '2025-03-22',  12, NULL),
  ('British Youth Championships 2025',              'U-12 Men''s Foil',  '2025-05-03',  32, NULL),
  ('Cambridge Sword Series 24/25 – Event 3',        'U-12 Men''s Foil',  '2025-06-01',  11, NULL),
  -- England — U-14 era
  ('LPJS Cambridge Sword',                          'U-14 Men''s Foil',  '2025-10-18',  12, NULL),
  ('Leon Paul U14 Open',                            'U-14 Men''s Foil',  '2025-11-15',  23, NULL),
  ('LPJS FCL Foil',                                 'U-14 Men''s Foil',  '2025-11-23',  15, NULL),
  ('LPJS Cambridge Christmas Challenge Foil',       'U-14 Men''s Foil',  '2025-12-06',  22, NULL),
  ('Eastern Region Youth Championships',            'U-14 Men''s Foil',  '2026-01-24',  2,  NULL),
  ('St Benedict''s LPJS Foil 2026',                 'U-14 Men''s Foil',  '2026-02-21',  11, NULL),
  ('Newham Swords Junior Foil Series 25/26 – Event 4', 'U-14 Men''s Foil', '2026-03-07', 11, NULL),
  -- Paris — Mini Marathon 2025
  ('Mini Marathon 2025',                            'U-12 Men''s Foil',  '2025-06-27',  29, NULL),
  ('Mini Marathon 2025',                            'U-13 Men''s Foil',  '2025-06-27',  40, NULL),
  -- Poland
  ('Challenge Wratislavia 2025',                    'U-13 Boys'' Foil',  '2025-03-27',  80, NULL),
  ('Challenge Wratislavia 2026',                    'U-13 Boys'' Foil',  '2026-03-19',  32, '513D1ACD0B9E46279C9BF8FBA08A51CA')
) AS e(tournament_name, event_name, date, placement, ftl_event_id)
JOIN tournaments t ON t.name = e.tournament_name
ON CONFLICT (athlete_id, tournament_id, event_name) DO UPDATE SET
  placement     = EXCLUDED.placement,
  date          = EXCLUDED.date,
  -- Preserve any existing ftl_event_id (don't overwrite with NULL)
  ftl_event_id  = COALESCE(EXCLUDED.ftl_event_id, events.ftl_event_id);

-- ── Verify ────────────────────────────────────────────────────────────────────
SELECT
  t.name              AS tournament,
  t.country,
  e.date,
  e.event_name,
  e.placement,
  CASE WHEN e.ftl_event_id IS NOT NULL THEN '✓ ' || LEFT(e.ftl_event_id, 8) || '…'
       ELSE '— pending'
  END                 AS ftl_status
FROM events e
JOIN tournaments t ON t.id = e.tournament_id
WHERE e.athlete_id = 'be990426-d27c-482f-be0b-33bb737b3a34'
ORDER BY e.date;
