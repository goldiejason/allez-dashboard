-- ═══════════════════════════════════════════════════════════════════
-- Migration 001 — Identity resolution + bout attribution support
-- Run this in the Supabase SQL Editor.
-- ═══════════════════════════════════════════════════════════════════

-- ── athlete_aliases ──────────────────────────────────────────────
-- Caches confirmed fuzzy name matches so subsequent pipeline runs
-- use an O(1) alias lookup instead of paying the fuzzy-match cost.
-- source:         "ftl" | "ftl_pool"
-- source_name:    the name as it appeared on the external source
-- confirmed_name: the name_ftl stored in our athletes table

create table if not exists athlete_aliases (
    id             uuid primary key default uuid_generate_v4(),
    source         text not null,       -- which source (ftl, ftl_pool)
    source_name    text not null,       -- name as seen on external source
    confirmed_name text not null,       -- name_ftl in our athletes table
    created_at     timestamptz default now(),
    unique (source, source_name)
);

comment on table athlete_aliases is
    'Confirmed name mappings from external sources to our athlete name_ftl values';

-- ── unresolved_identities ────────────────────────────────────────
-- Audit queue for names that could not be matched to any athlete.
-- Populated automatically by IdentityResolver when all strategies fail.
-- Human review: check source_name against athlete list and correct
-- the name_ftl column on the athletes table, then delete the row here.

create table if not exists unresolved_identities (
    id             uuid primary key default uuid_generate_v4(),
    source         text not null,
    source_name    text not null,
    context        text,                -- event name / tournament context
    best_candidate text,                -- closest candidate found (if any)
    best_score     numeric(5,3),        -- fuzzy score of the closest candidate
    created_at     timestamptz default now(),
    resolved_at    timestamptz,         -- null = pending review
    unique (source, source_name, context)
);

comment on table unresolved_identities is
    'Names from external sources that could not be matched — pending manual review';

-- ── staged_bouts ─────────────────────────────────────────────────
-- DE bouts that could not be attributed to an event row at collection
-- time.  BoutAttributor writes here instead of silently discarding.
-- Reconciliation: scripts/reconcile_staged_bouts.py retries these
-- bouts each run as the tournament database grows.

create table if not exists staged_bouts (
    id              uuid primary key default uuid_generate_v4(),
    athlete_id      uuid not null references athletes(id) on delete cascade,
    opponent_name   text not null,
    tournament_name text not null,
    round_text      text not null,
    ts              integer,
    tr              integer,
    result          boolean,
    reason          text,               -- why attribution failed
    created_at      timestamptz default now(),
    resolved_at     timestamptz,        -- null = pending reconciliation
    resolved_event_id uuid references events(id),
    unique (athlete_id, opponent_name, tournament_name, round_text)
);

comment on table staged_bouts is
    'DE bouts awaiting event attribution — reconciled automatically by nightly job';

-- ── RLS: service_role can manage all three tables ────────────────
alter table athlete_aliases       enable row level security;
alter table unresolved_identities enable row level security;
alter table staged_bouts          enable row level security;

-- service_role bypasses RLS — no policies needed for write access.
-- anon role gets read access to staged_bouts for dashboard visibility.
create policy "anon read staged_bouts"
    on staged_bouts for select using (true);
