-- Run once in Supabase > SQL Editor.
create table if not exists public.game_state (
  id text primary key,
  payload jsonb not null,
  updated_at timestamptz not null default now()
);
alter table public.game_state enable row level security;
