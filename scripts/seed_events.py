#!/usr/bin/env python3
"""
Seed Daniel Panga's 21 known competition results into Supabase.

Run from the allez-dashboard folder:
    python3 scripts/seed_events.py

This script is idempotent — it upserts tournaments and events, so re-running
is safe. FTL event IDs are discovered by navigating FTL and are stored here
for reproducibility. Re-running is safe — existing IDs are preserved via COALESCE.

Note: Cambridge Sword Series 24/25 – Event 3 (Jun 2025) has no FTL presence.
"""

import sys
import os

# Allow running from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.client import get_write_client

# ── Daniel's athlete UUID (inserted in a previous step) ───────────────────────
DANIEL_ID = "be990426-d27c-482f-be0b-33bb737b3a34"

# ── Known FTL event IDs ────────────────────────────────────────────────────────
# Key = (tournament_name, event_name); value = FTL event GUID
# Discovered 2026-03-27 by scanning FTL tournament schedules.
# Cambridge Sword Series 24/25 – Event 3 (Jun 2025) has no FTL presence.
FTL_EVENT_IDS: dict[tuple[str, str], str] = {
    # England — U-12 era
    ("ERYC & BYCQ 2024 Foil",                        "U-12 Men's Foil"): "4DAFCE133DC04518921BBB952CF4052C",
    ("British Youth Championships 2024",             "U-12 Men's Foil"): "3E917A5025434229AEAE850B1FF6962A",
    ("LPJS Cambridge Sword Foil",                    "U-12 Men's Foil"): "B3150B9DCF1E4874A7506F696BDA4ACF",
    ("FCL LPJS November 2024",                       "U-12 Men's Foil"): "B688F1DCEFAD46D988778B90164FB91F",
    ("Cambridge Sword Series 24/25 – Event 1",       "U-12 Men's Foil"): "BEB329F6F16E4E05A6840A37B2E4651B",
    ("ERYC & BYCQ 2025 Foil",                        "U-12 Men's Foil"): "E0D77396B7AE4019AF9F9235835553C2",
    ("Cambridge Foil Series 2024–25 – Event 2",      "U-12 Men's Foil"): "9035DD0D6BF54441857F784C00E7823D",
    ("LPJS London Foil",                             "U-12 Men's Foil"): "C46F1E7A14FD4B2F9495413CDA28567E",
    ("British Youth Championships 2025",             "U-12 Men's Foil"): "21F0B52DBBFF4CCBBBFBE3EB8ED356E8",
    # England — U-14 era
    ("LPJS Cambridge Sword",                         "U-14 Men's Foil"): "5AB0E2B738F94B8591B79162F4686356",
    ("Leon Paul U14 Open",                           "U-14 Men's Foil"): "0B28344DE1AC43E7890E3AC3B0EA1A66",
    ("LPJS FCL Foil",                                "U-14 Men's Foil"): "DB87908553FE4089AF0F0999EB6DC518",
    ("LPJS Cambridge Christmas Challenge Foil",      "U-14 Men's Foil"): "4F3495EE880E41AAB18F409D3919518E",
    ("Eastern Region Youth Championships",           "U-14 Men's Foil"): "40132BCA05714B08ADEC2FB5BA2EE1DD",
    ("St Benedict's LPJS Foil 2026",                 "U-14 Men's Foil"): "0B46E82914D84C2ABBE2B00173850C46",
    ("Newham Swords Junior Foil Series 25/26 – Event 4", "U-14 Men's Foil"): "85387212B20F48C39A408016169A5F05",
    # Paris — Mini Marathon 2025
    ("Mini Marathon 2025",                           "U-12 Men's Foil"): "D3E99709148C4725BF0BF4EAC062900F",
    ("Mini Marathon 2025",                           "U-13 Men's Foil"): "1E44E379FC8548D7A839A37D1B63FAE7",
    # Poland
    ("Challenge Wratislavia 2025",                   "U-13 Boys' Foil"): "0484994E2E6E43A5A0DBDC76DF42ABF7",
    ("Challenge Wratislavia 2026",                   "U-13 Boys' Foil"): "513D1ACD0B9E46279C9BF8FBA08A51CA",
}

# ── Tournament definitions ─────────────────────────────────────────────────────
# Each dict will be upserted into the tournaments table.
TOURNAMENTS = [
    {
        "name": "ERYC & BYCQ 2024 Foil",
        "date_start": "2024-01-27",
        "date_end":   "2024-01-27",
        "country":    "GBR",
        "city":       "England",
        "is_international": False,
        "circuit":    "ERYC",
    },
    {
        "name": "British Youth Championships 2024",
        "date_start": "2024-05-04",
        "date_end":   "2024-05-06",
        "country":    "GBR",
        "city":       "England",
        "is_international": False,
        "circuit":    "BYC",
    },
    {
        "name": "LPJS Cambridge Sword Foil",
        "date_start": "2024-09-22",
        "date_end":   "2024-09-22",
        "country":    "GBR",
        "city":       "Cambridge",
        "is_international": False,
        "circuit":    "LPJS",
    },
    {
        "name": "FCL LPJS November 2024",
        "date_start": "2024-11-24",
        "date_end":   "2024-11-24",
        "country":    "GBR",
        "city":       "England",
        "is_international": False,
        "circuit":    "LPJS",
    },
    {
        "name": "Cambridge Sword Series 24/25 – Event 1",
        "date_start": "2024-12-08",
        "date_end":   "2024-12-08",
        "country":    "GBR",
        "city":       "Cambridge",
        "is_international": False,
        "circuit":    "Cambridge Sword Series",
    },
    {
        "name": "ERYC & BYCQ 2025 Foil",
        "date_start": "2025-01-25",
        "date_end":   "2025-01-25",
        "country":    "GBR",
        "city":       "England",
        "is_international": False,
        "circuit":    "ERYC",
    },
    {
        "name": "Cambridge Foil Series 2024–25 – Event 2",
        "date_start": "2025-03-02",
        "date_end":   "2025-03-02",
        "country":    "GBR",
        "city":       "Cambridge",
        "is_international": False,
        "circuit":    "Cambridge Sword Series",
    },
    {
        "name": "LPJS London Foil",
        "date_start": "2025-03-22",
        "date_end":   "2025-03-23",
        "country":    "GBR",
        "city":       "London",
        "is_international": False,
        "circuit":    "LPJS",
    },
    {
        "name": "British Youth Championships 2025",
        "date_start": "2025-05-03",
        "date_end":   "2025-05-05",
        "country":    "GBR",
        "city":       "England",
        "is_international": False,
        "circuit":    "BYC",
    },
    {
        "name": "Cambridge Sword Series 24/25 – Event 3",
        "date_start": "2025-06-01",
        "date_end":   "2025-06-01",
        "country":    "GBR",
        "city":       "Cambridge",
        "is_international": False,
        "circuit":    "Cambridge Sword Series",
    },
    {
        "name": "LPJS Cambridge Sword",
        "date_start": "2025-10-18",
        "date_end":   "2025-10-18",
        "country":    "GBR",
        "city":       "Cambridge",
        "is_international": False,
        "circuit":    "LPJS",
    },
    {
        "name": "Leon Paul U14 Open",
        "date_start": "2025-11-15",
        "date_end":   "2025-11-16",
        "country":    "GBR",
        "city":       "London",
        "is_international": False,
        "circuit":    None,
    },
    {
        "name": "LPJS FCL Foil",
        "date_start": "2025-11-23",
        "date_end":   "2025-11-23",
        "country":    "GBR",
        "city":       "England",
        "is_international": False,
        "circuit":    "LPJS",
    },
    {
        "name": "LPJS Cambridge Christmas Challenge Foil",
        "date_start": "2025-12-06",
        "date_end":   "2025-12-06",
        "country":    "GBR",
        "city":       "Cambridge",
        "is_international": False,
        "circuit":    "LPJS",
    },
    {
        "name": "Eastern Region Youth Championships",
        "date_start": "2026-01-24",
        "date_end":   "2026-01-25",
        "country":    "GBR",
        "city":       "England",
        "is_international": False,
        "circuit":    "ERYC",
    },
    {
        "name": "St Benedict's LPJS Foil 2026",
        "date_start": "2026-02-21",
        "date_end":   "2026-02-21",
        "country":    "GBR",
        "city":       "England",
        "is_international": False,
        "circuit":    "LPJS",
    },
    {
        "name": "Newham Swords Junior Foil Series 25/26 – Event 4",
        "date_start": "2026-03-07",
        "date_end":   "2026-03-07",
        "country":    "GBR",
        "city":       "London",
        "is_international": False,
        "circuit":    None,
    },
    {
        "name": "Mini Marathon 2025",
        "date_start": "2025-06-27",
        "date_end":   "2025-06-29",
        "country":    "FRA",
        "city":       "Paris",
        "is_international": True,
        "circuit":    None,
    },
    {
        "name": "Challenge Wratislavia 2025",
        "date_start": "2025-03-27",
        "date_end":   "2025-03-31",
        "country":    "POL",
        "city":       "Wrocław",
        "is_international": True,
        "circuit":    None,
    },
    {
        "name": "Challenge Wratislavia 2026",
        "date_start": "2026-03-19",
        "date_end":   "2026-03-23",
        "country":    "POL",
        "city":       "Wrocław",
        "is_international": True,
        "circuit":    None,
    },
]

# ── Event definitions ──────────────────────────────────────────────────────────
# placement = integer (1 = gold). None where tied or unknown.
# "3rd (tied)" → 3
EVENTS = [
    # England events
    {"tournament": "ERYC & BYCQ 2024 Foil",                        "event_name": "U-12 Men's Foil", "date": "2024-01-27", "placement": 5,  "weapon": "foil"},
    {"tournament": "British Youth Championships 2024",              "event_name": "U-12 Men's Foil", "date": "2024-05-04", "placement": 38, "weapon": "foil"},
    {"tournament": "LPJS Cambridge Sword Foil",                     "event_name": "U-12 Men's Foil", "date": "2024-09-22", "placement": 19, "weapon": "foil"},
    {"tournament": "FCL LPJS November 2024",                        "event_name": "U-12 Men's Foil", "date": "2024-11-24", "placement": 28, "weapon": "foil"},
    {"tournament": "Cambridge Sword Series 24/25 – Event 1",        "event_name": "U-12 Men's Foil", "date": "2024-12-08", "placement": 7,  "weapon": "foil"},
    {"tournament": "ERYC & BYCQ 2025 Foil",                        "event_name": "U-12 Men's Foil", "date": "2025-01-25", "placement": 3,  "weapon": "foil"},  # tied
    {"tournament": "Cambridge Foil Series 2024–25 – Event 2",       "event_name": "U-12 Men's Foil", "date": "2025-03-02", "placement": 19, "weapon": "foil"},
    {"tournament": "LPJS London Foil",                              "event_name": "U-12 Men's Foil", "date": "2025-03-22", "placement": 12, "weapon": "foil"},
    {"tournament": "British Youth Championships 2025",              "event_name": "U-12 Men's Foil", "date": "2025-05-03", "placement": 32, "weapon": "foil"},
    {"tournament": "Cambridge Sword Series 24/25 – Event 3",        "event_name": "U-12 Men's Foil", "date": "2025-06-01", "placement": 11, "weapon": "foil"},
    {"tournament": "LPJS Cambridge Sword",                          "event_name": "U-14 Men's Foil", "date": "2025-10-18", "placement": 12, "weapon": "foil"},
    {"tournament": "Leon Paul U14 Open",                            "event_name": "U-14 Men's Foil", "date": "2025-11-15", "placement": 23, "weapon": "foil"},
    {"tournament": "LPJS FCL Foil",                                 "event_name": "U-14 Men's Foil", "date": "2025-11-23", "placement": 15, "weapon": "foil"},
    {"tournament": "LPJS Cambridge Christmas Challenge Foil",       "event_name": "U-14 Men's Foil", "date": "2025-12-06", "placement": 22, "weapon": "foil"},
    {"tournament": "Eastern Region Youth Championships",            "event_name": "U-14 Men's Foil", "date": "2026-01-24", "placement": 2,  "weapon": "foil"},
    {"tournament": "St Benedict's LPJS Foil 2026",                  "event_name": "U-14 Men's Foil", "date": "2026-02-21", "placement": 11, "weapon": "foil"},
    {"tournament": "Newham Swords Junior Foil Series 25/26 – Event 4", "event_name": "U-14 Men's Foil", "date": "2026-03-07", "placement": 11, "weapon": "foil"},
    # Paris events
    {"tournament": "Mini Marathon 2025",                            "event_name": "U-12 Men's Foil", "date": "2025-06-27", "placement": 29, "weapon": "foil"},
    {"tournament": "Mini Marathon 2025",                            "event_name": "U-13 Men's Foil", "date": "2025-06-27", "placement": 40, "weapon": "foil"},
    # Poland events
    {"tournament": "Challenge Wratislavia 2025",                    "event_name": "U-13 Boys' Foil", "date": "2025-03-27", "placement": 80, "weapon": "foil"},
    {"tournament": "Challenge Wratislavia 2026",                    "event_name": "U-13 Boys' Foil", "date": "2026-03-19", "placement": 32, "weapon": "foil"},
]


def parse_placement(raw) -> int | None:
    """Convert placement strings like '3rd (tied)' to integer 3."""
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    # strip ordinal suffixes and extra words
    import re
    m = re.search(r"\d+", str(raw))
    return int(m.group()) if m else None


def seed():
    db = get_write_client()

    # ── 1. Upsert tournaments ──────────────────────────────────────────────────
    print("Seeding tournaments...")
    tournament_id_map: dict[str, str] = {}

    for t in TOURNAMENTS:
        res = db.table("tournaments").upsert(
            t,
            on_conflict="name",          # tournaments.name has no unique constraint by default;
                                         # we rely on the name being unique for now.
            ignore_duplicates=False,
        ).execute()
        # Re-fetch to get the UUID (upsert doesn't always return it cleanly)
        row = db.table("tournaments").select("id").eq("name", t["name"]).single().execute()
        tournament_id_map[t["name"]] = row.data["id"]
        print(f"  ✓ {t['name']}  → {row.data['id']}")

    # ── 2. Upsert events ───────────────────────────────────────────────────────
    print("\nSeeding events...")
    for ev in EVENTS:
        tournament_name = ev["tournament"]
        tournament_id   = tournament_id_map.get(tournament_name)

        ftl_event_id = FTL_EVENT_IDS.get((tournament_name, ev["event_name"]))

        row = {
            "athlete_id":    DANIEL_ID,
            "tournament_id": tournament_id,
            "event_name":    ev["event_name"],
            "date":          ev["date"],
            "placement":     ev["placement"],
            "weapon":        ev["weapon"],
            "ftl_event_id":  ftl_event_id,   # NULL until discovered
        }

        # Upsert on the unique constraint (athlete_id, tournament_id, event_name)
        db.table("events").upsert(
            row,
            on_conflict="athlete_id,tournament_id,event_name",
            ignore_duplicates=False,
        ).execute()

        ftl_tag = f"  [FTL: {ftl_event_id[:8]}…]" if ftl_event_id else "  [FTL: pending]"
        print(f"  ✓ {ev['date']}  {tournament_name} — {ev['event_name']}  #{ev['placement']}{ftl_tag}")

    print(f"\n✅  Done — {len(TOURNAMENTS)} tournaments, {len(EVENTS)} events seeded for Daniel Panga.")


if __name__ == "__main__":
    seed()
