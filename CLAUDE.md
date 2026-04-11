# ProphetX Parlay Service Provider

Automated market maker for parlay bets on ProphetX (PX). Receives RFQs via WebSocket, prices them using de-vigged sportsbook odds, and submits offers back.

## User Context

- **Timezone**: US Eastern (ET)
- **Operator**: Mike — runs the parlay SP, monitors via dashboard

## Architecture

```
index.js                  Entry point — Express server + async startup sequence
config.js                 Env vars, pricing defaults, sport mappings
services/
  prophetx.js             PX API client (auth, events, markets, offers, confirmations)
  websocket.js            Pusher WebSocket — RFQ/confirm/settle event handlers
  pricer.js               Pricing engine — fair value + vig → offer (American odds)
  odds-feed.js            SharpAPI (primary) + The Odds API (fallback) — de-vigged odds
  line-manager.js         Maps PX line_ids to Odds API events, team name matching
  order-tracker.js        Exposure tracking, P&L, market intelligence, decline stats
  db.js                   Supabase client (parlay_orders, matched_parlays tables)
  logger.js               Simple leveled logger (debug/info/warn/error)
client/
  index.html              Dashboard SPA (Live, Analytics, History, Market Intel, Config tabs)
```

## Deployment

- **Platform**: Railway (auto-deploys on push to main)
- **Runtime**: Node.js (`npm start` → `node index.js`)
- **Dev**: `npm run dev` → `node --watch index.js`
- **No build step** — vanilla JS, no TypeScript, no bundler

## Environment Variables (set in Railway)

| Variable | Required | Description |
|---|---|---|
| `PX_ACCESS_KEY` | Yes | ProphetX partner API access key |
| `PX_SECRET_KEY` | Yes | ProphetX partner API secret key |
| `PX_BASE_URL` | No | Default: `https://cash.api.prophetx.co` (production) |
| `SHARP_ODDS_API_KEY` | Yes | SharpAPI key (primary odds source, DK+FD free tier) |
| `THE_ODDS_API_KEY` | No | The Odds API key (fallback for NCAAB, alt lines from Pinnacle) |
| `SUPABASE_URL` | No | Supabase project URL |
| `SUPABASE_SERVICE_KEY` | No | Supabase service role key |
| `DEFAULT_VIG` | No | Default: 0.001 (0.1%) |
| `MAX_RISK_PER_PARLAY` | No | Default: 500 |
| `MAX_EXPOSURE_PER_TEAM` | No | Default: 50 |
| `MAX_LEGS` | No | Default: 8 |
| `STALE_PRICE_MINUTES` | No | Default: 15 |
| `REFRESH_INTERVAL_MINUTES` | No | Default: 2 |
| `SUPPORTED_SPORTS` | No | Default: `basketball_nba,basketball_ncaab,baseball_mlb,icehockey_nhl,tennis,soccer` |
| `LOG_LEVEL` | No | Default: `info` |

## RFQ Flow

1. **Startup**: Auth with PX → fetch odds → seed lines (match PX events to Odds API) → connect WebSocket → register supported lines
2. **price.ask.new** (broadcast): RFQ arrives → `shouldDecline()` checks legs known + correlation + exposure → `priceParlay()` gets fair probs, applies vig → `submitOffer()` sends American odds back via callback URL
3. **price.confirm.new** (private): PX asks to confirm → re-validate pricing (5% drift check) → accept/reject
4. **order.matched** (broadcast): Any SP's parlay gets filled — tracked for market intelligence
5. **order.settled** / **parlay.settled** (private): Settlement → P&L recording

## Pricing Logic

- **De-vig**: For each leg, average fair probabilities across sportsbooks using `deVig2Way()` (proportional removal)
- **Parlay fair prob**: Product of individual leg fair probs
- **Offered prob**: `fairParlayProb * (1 + vig)` (makes price worse for bettor)
- **Odds format**: PX uses American odds throughout. `decimalToAmerican()` in pricer.js handles conversion
- **Alt lines**: If RFQ has a spread/total not matching the primary line, fetches alt lines from The Odds API on demand (Pinnacle)
- **Stale check**: Declines if odds cache is older than `stalePriceMinutes`
- **Started check**: Declines if event has already started

## Correlation Rules (pricer.js `shouldDecline`)

- **Blocked**: Spread + moneyline on same game (highly correlated)
- **Blocked**: Two of same market type on same game
- **Allowed**: Spread/moneyline + total on same game

## Odds Sources

- **SharpAPI** (`api.sharpapi.io`): Primary source for NBA, MLB, NHL, tennis, soccer. Free tier covers DraftKings + FanDuel
- **The Odds API** (`api.the-odds-api.com`): Fallback for NCAAB. Also used on-demand for alternate spread/total lines (Pinnacle, DK, FD)

## Team Name Matching (line-manager.js)

PX and odds APIs use different team names. Matching strategies (in order):
1. Override map (`TEAM_NAME_OVERRIDES` — NHL abbreviations like WAS, CBJ, MTL)
2. Exact normalized match
3. Substring containment
4. Last N words match (e.g., "Red Sox" matches "Boston Red Sox")

## API Endpoints (Express)

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Railway health check (always 200) |
| `/status` | GET | Full service status JSON |
| `/orders` | GET | Recent orders with P&L |
| `/market-intel` | GET | All matched parlays across SPs |
| `/refresh-odds` | POST | Manual odds refresh |
| `/refresh-lines` | POST | Manual line re-seed |
| `/pause` | POST | Stop responding to RFQs |
| `/resume` | POST | Resume RFQ handling |
| `/reconnect` | POST | Force WebSocket reconnect |
| `/odds-events` | GET | Debug: cached odds events |
| `/lines` | GET | Debug: registered line index |

## Database (Supabase)

- **parlay_orders**: Our quotes, confirmations, settlements, P&L
- **matched_parlays**: All matched parlays across all SPs (market intelligence)
- Upserts on `parlay_id` for orders

## Key Gotchas

- **American odds, not decimal**: PX rejects decimal odds with "invalid odds" 400. All odds submitted must be American integers (e.g., +150, -200). Fixed in commit 10c1469.
- **config import order**: Services that use `config` must import at top of file, not lazily. The "key is not defined" bug was caused by config imported at bottom of websocket.js. Fixed in commit 10c1469.
- **valid_until is nanoseconds**: PX expects `valid_until` in nanoseconds, not milliseconds or seconds.
- **callback_url is absolute**: `submitOffer` and `confirmOrder` use the callback URL from the RFQ directly (not relative to baseUrl).
- **Both channels are private-prefixed**: PX WebSocket channels are both `private-*` — the broadcast one has "broadcast" in the name.
- **Back-to-back/doubleheader matching**: Odds cache stores arrays per team pair, matched by closest `commenceTime` to handle same-day series.
- **Full-game only**: Line manager filters out first half, quarter, period, inning, player props — only registers full-game moneyline/spread/total.
- **max_risk enforcement**: PX sandbox may not enforce max_risk limits. A $2,447 order was confirmed despite max_risk=500. Open question for Alec (PX contact).

## Conventions

- CommonJS (`require`/`module.exports`), no ES modules
- No TypeScript, no build step
- `node-fetch@2` (CommonJS compatible)
- Logging: `log.info('Category', 'message', optionalData)`
- **NEVER `git push` without explicit user approval.** Push auto-deploys to Railway production. Commit freely, but the push must always be gated on the user typing "push" (or equivalent) in chat. Do NOT push after completing work, do NOT push as part of a batched command, do NOT assume earlier approval carries over to a new commit. Every single push requires a fresh green-light.
