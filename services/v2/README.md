# v2 pricing engine

A unified model that replaces most of the ad-hoc vig stack with a
coherent four-stage pipeline:

```
RFQ legs
  ↓
[1] FairProbEstimator
     - de-vig books (existing logic, reused)
     - calibration correction (new — from historical outcomes)
     - uncertainty std (new — from book count + disagreement)
  ↓
[2] Correlation
     - same-event block correlation (primary case)
     - cross-event correlation (same-team multi-game, same-slate favorites)
  ↓
[3] ParlayCombiner
     - multivariate combiner accounting for correlation
     - propagates uncertainty from legs to parlay
  ↓
[4] EV-targeted vig
     - single knob: target_edge (e.g. 2% expected EV per $ of stake)
     - solves for vig given (parlay_prob, std, correlation_penalty, template_count)
     - replaces DEFAULT_VIG + VIG_BY_SPORT + VIG_FAVORITE_* + VIG_LONGSHOT_* + SGP_*
```

## What retires (vs v1)

| v1 knob | Where it goes in v2 |
|---|---|
| `DEFAULT_VIG` | target_edge (single global) |
| `VIG_BY_SPORT` | emerges from per-sport calibration + uncertainty |
| `VIG_FAVORITE_SLOPE/FLOOR` | emerges from uncertainty growing on extreme probabilities |
| `VIG_SERIES_MIN`, `VIG_MMA_MIN` | emerges from thin-market uncertainty + low-book calibration uncertainty |
| `VIG_LONGSHOT_THRESHOLD/MAX_ADD` | emerges from parlay-fair-prob × uncertainty |
| `SGP_VIG_MULTIPLIER` | folded into correlation matrix |
| `SGP_CORRELATION_POSITIVE/NEGATIVE` | same |
| `DEVIG_FAV_MAX_SHARE` | emerges from per-book calibration weights |
| `NBA_SERIES_FAV_CAP_ODDS` | explicit EV cap on extreme favorites from EV solver |

## What stays

- `TEMPLATE_RAMP_*` (exposure dimension, orthogonal to pricing)
- `MAX_RISK_*`, `MAX_EXPOSURE_*`, `MAX_LEGS`, `MAX_ODDS` (hard limits)
- `SGP_ALLOWED_COMBOS` (structural allow list)
- Infra: PX / Odds API keys, Supabase

## Empirical basis (Apr 7-22, 800 event-deduped outcomes)

Calibration bias discovered:
- MLB spread (run line): +13.3pp overconfident (books bias)
- NHL ML/spread/total: 5-7pp under-confident (books bias, inverse direction)
- MLB moneyline, NBA totals: within ±3pp (well-calibrated baseline)
- Book consensus (naive avg) shows same patterns → de-vig isn't the leak

Template concentration (orthogonal to pricing):
- April 18 cliff: 2 parlay templates × 6-9 copies = 80% of day's losses
- Fix: v1 `TEMPLATE_RAMP_*` (already shipped, keeps in v2)

## Rollout plan

1. **Phase 0 — shadow mode (this commit).** V2 computes alongside V1.
   Pricer logs `v2_offered_odds` in meta. No bettor-visible change. Gather
   side-by-side data for N days.
2. **Phase 1 — A/B.** Split RFQs 50/50 by parlayId hash. Measure fill
   rate, EV per quote, CLV between arms.
3. **Phase 2 — cutover.** When V2 matches or beats V1 across a week,
   flip all traffic. Delete V1 vig stack.

## File layout

- `calibration-trainer.js` — loads historical outcomes, fits
  (sport, market, odds-band) correction factors + uncertainty
- `fair-prob-estimator.js` — de-vig + calibration + uncertainty
- `correlation.js` — correlation matrix + multivariate parlay combiner
- `ev-vig.js` — target-edge vig solver
- `index.js` — `priceParlayV2(legs, opts)` — single entry point

## Feature flag

All gated behind `PRICING_V2_ENABLED` (default `false`). With flag off,
none of this code runs on the hot path.

## Current status: WIP, shadow-mode only, not yet wired into pricer.
