-- Run this in your Supabase SQL Editor to create the bids table
-- Go to: supabase.com → your project → SQL Editor → paste this → Run

create table if not exists bids (
  id              bigserial primary key,
  created_at      timestamptz default now(),
  customer_name   text,
  project_name    text,
  estimator_name  text,
  complexity      text,
  tech_level      text,
  total_hours     numeric(10,2),
  total_price     numeric(12,2),
  bid_data        jsonb
);

-- Index for fast sorting by date
create index if not exists bids_created_at_idx on bids (created_at desc);

-- Allow public read/write (internal app — no auth yet)
alter table bids enable row level security;
create policy "allow_all" on bids for all using (true) with check (true);
