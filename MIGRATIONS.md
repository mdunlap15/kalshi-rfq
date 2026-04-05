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

## Existing tables

The app also uses:
- `parlay_orders` — quotes, confirmations, settlements, P&L
- `matched_parlays` — all matched parlays across all SPs (market intelligence)
