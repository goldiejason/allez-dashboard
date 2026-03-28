-- ═══════════════════════════════════════════════════════════════
-- Update FTL event IDs for Daniel Panga's 20 known FTL events
-- Discovered 2026-03-27 by scanning FTL tournament schedules.
-- Cambridge Sword Series 24/25 – Event 3 (Jun 2025) not on FTL.
-- Safe to re-run: only updates rows where value differs.
-- ═══════════════════════════════════════════════════════════════

UPDATE events e
SET ftl_event_id = x.ftl_event_id,
    field_size   = x.field_size
FROM (VALUES
  -- England U-12 era
  ('ERYC & BYCQ 2024 Foil',                          'U-12 Men''s Foil', '4DAFCE133DC04518921BBB952CF4052C', 14),
  ('British Youth Championships 2024',               'U-12 Men''s Foil', '3E917A5025434229AEAE850B1FF6962A', 77),
  ('LPJS Cambridge Sword Foil',                      'U-12 Men''s Foil', 'B3150B9DCF1E4874A7506F696BDA4ACF', 32),
  ('FCL LPJS November 2024',                         'U-12 Men''s Foil', 'B688F1DCEFAD46D988778B90164FB91F', 43),
  ('Cambridge Sword Series 24/25 – Event 1',         'U-12 Men''s Foil', 'BEB329F6F16E4E05A6840A37B2E4651B', 25),
  ('ERYC & BYCQ 2025 Foil',                          'U-12 Men''s Foil', 'E0D77396B7AE4019AF9F9235835553C2', 9),
  ('Cambridge Foil Series 2024–25 – Event 2',        'U-12 Men''s Foil', '9035DD0D6BF54441857F784C00E7823D', 25),
  ('LPJS London Foil',                               'U-12 Men''s Foil', 'C46F1E7A14FD4B2F9495413CDA28567E', 31),
  ('British Youth Championships 2025',               'U-12 Men''s Foil', '21F0B52DBBFF4CCBBBFBE3EB8ED356E8', 83),
  -- England U-14 era
  ('LPJS Cambridge Sword',                           'U-14 Men''s Foil', '5AB0E2B738F94B8591B79162F4686356', 28),
  ('Leon Paul U14 Open',                             'U-14 Men''s Foil', '0B28344DE1AC43E7890E3AC3B0EA1A66', 54),
  ('LPJS FCL Foil',                                  'U-14 Men''s Foil', 'DB87908553FE4089AF0F0999EB6DC518', 33),
  ('LPJS Cambridge Christmas Challenge Foil',        'U-14 Men''s Foil', '4F3495EE880E41AAB18F409D3919518E', 27),
  ('Eastern Region Youth Championships',             'U-14 Men''s Foil', '40132BCA05714B08ADEC2FB5BA2EE1DD', 15),
  ('St Benedict''s LPJS Foil 2026',                  'U-14 Men''s Foil', '0B46E82914D84C2ABBE2B00173850C46', 33),
  ('Newham Swords Junior Foil Series 25/26 – Event 4', 'U-14 Men''s Foil', '85387212B20F48C39A408016169A5F05', 19),
  -- Mini Marathon 2025 (Paris) — two age categories
  ('Mini Marathon 2025',                             'U-12 Men''s Foil', 'D3E99709148C4725BF0BF4EAC062900F', 84),
  ('Mini Marathon 2025',                             'U-13 Men''s Foil', '1E44E379FC8548D7A839A37D1B63FAE7', 77),
  -- Poland
  ('Challenge Wratislavia 2025',                     'U-13 Boys'' Foil', '0484994E2E6E43A5A0DBDC76DF42ABF7', 200),
  ('Challenge Wratislavia 2026',                     'U-13 Boys'' Foil', '513D1ACD0B9E46279C9BF8FBA08A51CA', NULL)
) AS x(tournament_name, event_name, ftl_event_id, field_size)
JOIN tournaments t ON t.name = x.tournament_name
WHERE e.athlete_id    = 'be990426-d27c-482f-be0b-33bb737b3a34'
  AND e.tournament_id = t.id
  AND e.event_name    = x.event_name
  AND (e.ftl_event_id IS DISTINCT FROM x.ftl_event_id
       OR (x.field_size IS NOT NULL AND e.field_size IS DISTINCT FROM x.field_size));

-- ── Verify ────────────────────────────────────────────────────────────────────
SELECT
  t.name              AS tournament,
  e.event_name,
  e.date,
  e.placement,
  e.field_size,
  CASE WHEN e.ftl_event_id IS NOT NULL
       THEN '✓ ' || LEFT(e.ftl_event_id, 8) || '…'
       ELSE '— no FTL'
  END                 AS ftl_status
FROM events e
JOIN tournaments t ON t.id = e.tournament_id
WHERE e.athlete_id = 'be990426-d27c-482f-be0b-33bb737b3a34'
ORDER BY e.date;
