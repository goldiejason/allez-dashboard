-- ═══════════════════════════════════════════════════════════════
-- Allez Dashboard — Supabase Schema
-- Run this in the Supabase SQL Editor to initialise the database.
-- ═══════════════════════════════════════════════════════════════

-- ── Extensions ─────────────────────────────────────────────────
create extension if not exists "uuid-ossp";

-- ── 1. athletes ─────────────────────────────────────────────────
-- Master registry of all Allez Fencing club members.
create table if not exists athletes (
    id              uuid primary key default uuid_generate_v4(),
    name_display    text not null,          -- "Daniel Jason Panga"
    name_ftl        text,                   -- Exact name as stored on FTL (e.g. "PANGA Daniel")
    uk_ratings_id   integer unique,         -- UK Ratings athlete ID (null if not registered)
    ftl_fencer_id   text unique,            -- FTL internal fencer ID (discovered from their profile page)
    weapon          text check (weapon in ('foil', 'epee', 'sabre')),
    club            text default 'Allez Fencing',
    age_category    text,                   -- e.g. "U14", "U17", "Senior"
    hand            text check (hand in ('right', 'left')),
    active          boolean default true,   -- false = no longer competing
    last_refreshed  timestamptz,            -- when data was last pulled from FTL/UK Ratings
    created_at      timestamptz default now()
);

comment on table athletes is 'Master registry of all Allez Fencing athletes';

-- ── 2. tournaments ──────────────────────────────────────────────
-- Lookup table mapping known tournaments to their FTL IDs.
-- One row per physical tournament (e.g. "Challenge Wratislavia 2026").
create table if not exists tournaments (
    id                  uuid primary key default uuid_generate_v4(),
    name                text not null,              -- "Challenge Wratislavia 2026"
    ftl_tournament_id   text unique,                -- FTL tournament ID from URL
    date_start          date,
    date_end            date,
    country             text,                       -- "GBR", "POL", "FRA"
    city                text,
    is_international    boolean default false,
    circuit             text,                       -- "LPJS", "BYC", "Cambridge Sword Series", null
    created_at          timestamptz default now()
);

comment on table tournaments is 'Known tournaments with FTL IDs for data collection';

-- ── 3. events ───────────────────────────────────────────────────
-- Each row = one athlete at one event (their result).
create table if not exists events (
    id              uuid primary key default uuid_generate_v4(),
    athlete_id      uuid not null references athletes(id) on delete cascade,
    tournament_id   uuid references tournaments(id),
    event_name      text not null,          -- "U-14 Men's Foil"
    date            date,                   -- exact date from FTL
    placement       integer,                -- 1 = gold, 2 = silver, etc.
    field_size      integer,                -- total number of fencers in the event
    ftl_event_id    text,                   -- FTL event ID (from URL, for data collection)
    weapon          text check (weapon in ('foil', 'epee', 'sabre')),
    created_at      timestamptz default now(),
    unique (athlete_id, tournament_id, event_name)
);

comment on table events is 'One row per athlete-event appearance with placement';

-- ── 4. pool_bouts ───────────────────────────────────────────────
-- Individual pool bouts — the core data powering all analytics.
-- One row per bout within a pool round.
create table if not exists pool_bouts (
    id                  uuid primary key default uuid_generate_v4(),
    event_id            uuid not null references events(id) on delete cascade,
    pool_number         integer,            -- which pool within the event (1, 2, 3...)
    bout_order          integer,            -- sequential order within the pool
    opponent_name       text not null,
    opponent_club       text,
    opponent_country    text default 'GBR',
    ts                  integer not null,   -- touches scored (by our athlete)
    tr                  integer not null,   -- touches received
    result              boolean not null,   -- true = won, false = lost
    created_at          timestamptz default now()
);

comment on table pool_bouts is 'Individual pool bout results — foundation of all analytics';

-- ── 5. de_bouts ─────────────────────────────────────────────────
-- Direct elimination bouts (tableau). One row per DE bout.
create table if not exists de_bouts (
    id                  uuid primary key default uuid_generate_v4(),
    event_id            uuid not null references events(id) on delete cascade,
    round               text not null,      -- "T64", "T32", "T16", "QF", "SF", "F", "Bronze"
    opponent_name       text not null,
    opponent_club       text,
    opponent_country    text default 'GBR',
    ts                  integer not null,   -- touches scored
    tr                  integer not null,   -- touches received
    result              boolean not null,   -- true = won, false = lost
    created_at          timestamptz default now()
);

comment on table de_bouts is 'Direct elimination bout results from FTL tableaux';

-- ── 6. annual_stats ─────────────────────────────────────────────
-- Year-level pool W/L totals from UK Ratings.
-- This is the ONLY data we pull from UK Ratings (everything else from FTL).
create table if not exists annual_stats (
    id          uuid primary key default uuid_generate_v4(),
    athlete_id  uuid not null references athletes(id) on delete cascade,
    year        integer not null,
    pool_w      integer default 0,
    pool_l      integer default 0,
    de_w        integer default 0,
    de_l        integer default 0,
    created_at  timestamptz default now(),
    unique (athlete_id, year)
);

comment on table annual_stats is 'UK Ratings annual pool and DE win/loss totals';

-- ── Indexes ─────────────────────────────────────────────────────
create index if not exists idx_events_athlete    on events(athlete_id);
create index if not exists idx_events_tournament on events(tournament_id);
create index if not exists idx_events_date       on events(date desc);
create index if not exists idx_pool_bouts_event  on pool_bouts(event_id);
create index if not exists idx_de_bouts_event    on de_bouts(event_id);
create index if not exists idx_annual_stats_ath  on annual_stats(athlete_id, year desc);

-- ── Row Level Security ──────────────────────────────────────────
-- Enable RLS on all tables (required for publishable key to work safely)
alter table athletes      enable row level security;
alter table tournaments   enable row level security;
alter table events        enable row level security;
alter table pool_bouts    enable row level security;
alter table de_bouts      enable row level security;
alter table annual_stats  enable row level security;

-- Public read policy (dashboard can read without auth)
create policy "Public read athletes"    on athletes    for select using (true);
create policy "Public read tournaments" on tournaments for select using (true);
create policy "Public read events"      on events      for select using (true);
create policy "Public read pool_bouts"  on pool_bouts  for select using (true);
create policy "Public read de_bouts"    on de_bouts    for select using (true);
create policy "Public read annual_stats" on annual_stats for select using (true);

-- Write restricted to service_role (collection scripts only — never from the browser)
-- service_role bypasses RLS by default, so no additional write policy needed.
