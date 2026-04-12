# Database Migrations

Run these in the Supabase SQL editor when the app gains new persistent stores.

## 2026-04-04: `declines` table

Persists every declined RFQ so Decline Reasons + Near Misses survive restarts.

```sql
CREATE TABLE IF NOT EXISTS declines (
  id BIGSERIAL PRIMARY KEY,
  parlay_id TEXT,
  reason TEXT NOT NULL,
  detail TEXT,
  known_legs JSONB DEFAULT '[]'::jsonb,
  unknown_line_ids JSONB DEFAULT '[]'::jsonb,
  unknown_details JSONB DEFAULT '[]'::jsonb,
  is_limit BOOLEAN DEFAULT false,
  declined_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_declines_declined_at ON declines(declined_at DESC);
CREATE INDEX IF NOT EXISTS idx_declines_parlay_id ON declines(parlay_id);
CREATE INDEX IF NOT EXISTS idx_declines_reason ON declines(reason);
```

After running, restart the service or trigger a redeploy — the code will start writing every decline to this table and will load up to 2000 rows on startup.

## 2026-04-11: `line_cache` table

Persistent lineId→team/market mapping so enrichment can resolve historical
line_ids even after PX purges events from the mm namespace.

```sql
CREATE TABLE IF NOT EXISTS line_cache (
  line_id TEXT PRIMARY KEY,
  sport TEXT,
  px_event_id TEXT,
  px_event_name TEXT,
  market_type TEXT,
  market_name TEXT,
  is_dnb BOOLEAN DEFAULT false,
  selection TEXT,
  team_name TEXT,
  line NUMERIC,
  home_team TEXT,
  away_team TEXT,
  odds_api_sport TEXT,
  odds_api_market TEXT,
  odds_api_selection TEXT,
  competitor_id TEXT,
  start_time TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_line_cache_sport ON line_cache(sport);
CREATE INDEX IF NOT EXISTS idx_line_cache_px_event_id ON line_cache(px_event_id);
CREATE INDEX IF NOT EXISTS idx_line_cache_updated_at ON line_cache(updated_at DESC);
```

After running, restart the service — the line manager will start persisting the
lineIndex to this table on every seed, and lookupLine will fall back to it for
line_ids not in the current in-memory index.

## Existing tables

The app also uses:
- `parlay_orders` — quotes, confirmations, settlements, P&L
- `matched_parlays` — all matched parlays across all SPs (market intelligence)
