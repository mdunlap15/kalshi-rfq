# Parlay SP Dashboard — Assessment & Roadmap

## What You Have

### 1. ORDERS (Default Landing Page)

**Summary Stats Bar** — P&L, Record, Win Rate, Active positions, Portfolio Risk vs limit, To Win total, Balance from PX, Filled count.

*Assessment:* This is your at-a-glance health check. Shows whether you're making money, how much capital is at work, and where you stand vs risk limits. **Keep as-is.**

**All Quotes Table** — Every RFQ you've quoted, with Fair/Pinnacle/Offered odds, Winner odds, My Risk, To Win, Status, P&L. Filterable by search, status, sport, legs, odds range, date range. Sortable on all columns. Expandable rows show per-leg detail with Pinnacle/Fair/My Odds and event times.

*Assessment:* This is your most-used table. The filters are strong. **Improvement needed:** The "My Risk" and "To Win" columns should also appear on unconfirmed quotes (currently only show for confirmed). Would be useful to see estimated risk even on quotes that weren't filled.

**Settled Positions (Wager Log)** — Settled orders with My Risk, Result, P&L, Cumulative P&L. Filterable by search and date.

*Assessment:* Useful for tracking actual performance. **Improvement needed:** Add sport and result filters (like the Portfolio settled table has). Currently only has search + date.

---

### 2. PORTFOLIO

**Performance Summary** — P&L, Record, Win Rate, Active, Total Risk, To Win, Balance, Settled count in a compact stat bar.

*Assessment:* Duplicates the Orders summary bar. **Consider removing** one or making them show different metrics (e.g., Portfolio shows settled-only stats, Orders shows live stats).

**Open/Settled Positions Tabs** — Tabbed view of your active and settled positions.

*Assessment:* Good location for positions. The Open tab has sorting and search. **Improvement needed:** Add sport filter to Open Positions (currently only has search + date). The Settled tab here is a duplicate of the Portfolio Settled in the same section — **remove one**.

**P&L by Sport** — Record, Staked, P&L, ROI% per sport.

*Assessment:* Critical for identifying which sports your pricing model works best for. With only a few settlements, not yet actionable. **Keep — will become very valuable.**

**P&L by Leg Count** — Record, Avg Odds, P&L, ROI% by 2-leg, 3-leg, etc.

*Assessment:* Important for understanding correlation risk. If 4+ leg parlays consistently lose money, you'd tighten or stop quoting them. **Keep.**

**Edge Analysis** — Compares fair probability vs offered odds on settled parlays, shows avg vig edge, expected vs actual bettor win rate.

*Assessment:* This is your pricing model validation tool. If actual bettor win rate is much higher than expected, your fair values are miscalibrated. **Keep — becomes critical with more data.**

**Quote Competitiveness** — Win rate on quoted parlays, our odds vs winner's odds, gap analysis.

*Assessment:* Tells you whether you're losing on price or speed. Currently shows 61% win rate which is good. The "Why" column (Price/Speed/Won) is useful. **Keep but improve:** Add a summary showing avg gap on losses vs wins so you can tune vig.

---

### 3. EXPOSURE

**Team Exposure** — Net exposure per team with weighted risk, notional payout, parlays count. Expandable to show underlying parlays.

*Assessment:* Now uses net exposure model which correctly accounts for offsetting positions. **Keep.** However, the display could be clearer — show "Gross Risk" alongside "Net Exposure" so you can see the offset effect. Currently only shows `risk` and `netExposure` but the column labeling could be better.

**Game Exposure** — Net exposure per game with Gross Risk, Stakes Held, Net Exposure. Color-coded risk bars. Expandable rows.

*Assessment:* **This is your most important risk tool.** It answers "what's the most I can lose from any single game outcome?" The net model means offsetting positions reduce exposure, freeing up capacity. **Keep and improve:** Add game start time prominence — games that have already started should be flagged differently. Add a filter for sport.

---

### 4. ANALYTICS

**Daily Volume & P&L** — Grouped bar chart (blue = risk volume, green/red = P&L) by day, with summary table.

*Assessment:* Good for tracking activity trends. **Keep.** Will become more useful over time.

**Cumulative P&L** — Line chart showing running P&L.

*Assessment:* The fundamental "is this working?" chart. **Keep.** Needs more data points to be useful.

**P&L by Sport (Chart)** — Horizontal bar chart.

*Assessment:* Duplicates the Portfolio table version. **Consider removing** — the table is more informative. Or keep only the chart and remove the table version.

**P&L by Leg Count (Chart)** — Horizontal bar chart.

*Assessment:* Same duplication issue. **Consider removing.**

**Model Calibration** — Expected vs actual bettor win rate by probability bucket.

*Assessment:* **Very valuable** once you have 20+ settled parlays. Shows whether your de-vigged fair values actually predict outcomes correctly. **Keep.**

**Competitive Gap** — Scatter plot of pricing gap with running average.

*Assessment:* Similar to the Quote Competitiveness table but visual. **Consider removing** — the table version in Portfolio is more actionable.

**Fill Rate vs Vig** — Bar chart showing fill rate at different vig levels.

*Assessment:* Useful for tuning vig. Currently all at one vig level (4%), so not actionable yet. **Keep for when you experiment with different vig levels.**

**Exposure Concentration** — Horizontal bars showing team exposure.

*Assessment:* Duplicates the Team Exposure table. **Consider removing.**

---

### 5. MARKET INTEL

**Decline Reasons** — Why RFQs were declined, with counts and expandable details.

*Assessment:* Useful for identifying coverage gaps. Shows which sports/events you're missing. **Keep.** Currently shows MLS matches not matching — the odds sources may need expanding.

**Near Misses** — RFQs where all legs were known but couldn't price (no fair value).

*Assessment:* These represent lost revenue. If you see patterns (e.g., always missing fair values for tennis spreads), you know where to add odds sources. **Keep.**

**Recent Matched Parlays** — All matched parlays from all SPs, showing whether you quoted, at what price, and who won.

*Assessment:* Market intelligence gold. Shows the competitive landscape. **Keep.** The search and result filters are useful.

---

### 6. CONFIG

**Odds Cache** — Freshness of cached odds per sport.

*Assessment:* Useful for debugging stale prices. **Keep.**

**Registered Lines** — Count of lines by sport/market type.

*Assessment:* Quick sanity check. **Keep but low priority.**

**Settlement Polling** — Manual trigger for settlement checks.

*Assessment:* Essential for sandbox testing. **Keep.**

**Configuration** — Current config values.

*Assessment:* Reference only. **Keep.**

**Definitions** — Glossary of terms.

*Assessment:* Helpful for onboarding but you know these by now. **Low priority.**

**Activity Log** — System event feed.

*Assessment:* Useful for debugging. **Keep.**

---

## What to Remove or Consolidate

1. **Duplicate P&L by Sport** — Exists as both a table (Portfolio) and chart (Analytics). Keep the table, remove the chart.
2. **Duplicate P&L by Leg Count** — Same duplication. Keep the table.
3. **Exposure Concentration chart** — Duplicates Team Exposure table. Remove.
4. **Competitive Gap chart** — Duplicates Quote Competitiveness table. Remove.
5. **Duplicate Portfolio Summary** — Orders and Portfolio both have summary bars showing the same data. Differentiate them or remove from Portfolio.
6. **Duplicate Settled Positions** — Exists in both Orders (Wager Log) and Portfolio (Settled tab). Keep in Portfolio, remove from Orders since Orders should focus on quoting activity.

---

## What to Build Next (Priority Order)

### High Priority

1. **Fix the settled P&L sign issue** — You mentioned a settled loss disappeared. Need to verify P&L calculations are consistently correct after all the refactoring. Do a reconciliation of all settled parlays against PX's reported profit.

2. **Real-time position monitoring** — When games are in progress, show which legs have won/lost so far. Currently we only know the final settlement. Mid-game visibility would let you see your live exposure changing.

3. **Automated vig optimization** — Currently vig is fixed at 4%. Build a system that adjusts vig dynamically based on:
   - Current portfolio exposure (raise vig when concentrated, lower when diversified)
   - Competitive win rate (lower vig if losing too many quotes)
   - Sport/market-specific edges

4. **Production readiness** — You're still on sandbox. Before going to production:
   - Verify all risk controls work correctly
   - Set appropriate risk limits (MAX_RISK_PER_PARLAY, MAX_EXPOSURE_PER_GAME_PCT)
   - Confirm PX enforces max_risk in production
   - Set up alerts for when exposure limits are approached

### Medium Priority

5. **Probability-weighted daily P&L projection** — You asked about this earlier. Show the expected P&L distribution for today based on current positions and fair probabilities. "Based on current positions, your expected P&L today is +$X with 95% confidence interval of -$Y to +$Z."

6. **Better Pinnacle data quality** — The +1597 Pinnacle odds bug needs investigation. Some events are getting wrong Pinnacle matches. Add sanity checks on Pinnacle odds (reject if they deviate >50% from our fair value).

7. **More sports/markets** — First half lines, player props (if PX adds them), more soccer leagues. Each additional market type increases quote coverage.

8. **Historical performance export** — CSV/Excel export of settled orders, P&L by day, exposure snapshots. For tax reporting and deeper analysis.

### Lower Priority

9. **Multi-tier pricing** — Instead of one offer per RFQ, send 2-3 tiers with different vig/risk levels. PX already supports multiple offers per quote.

10. **Alerting** — Email/Slack notifications for: large fills, settlements, exposure limit approaching, service disconnects.

11. **Backtesting** — Use historical odds data to simulate what your P&L would have been with different vig/risk parameters.

---

## Key Metrics to Watch

| Metric | Current | Target | Why |
|---|---|---|---|
| Win Rate (quotes) | 61% | 60-70% | Too high = leaving money on table. Too low = vig too high |
| Settlement Win Rate | 100% (3W-0L) | 60-70% | Should win more than lose since vig gives you edge |
| P&L | +$371 | Positive trend | The whole point |
| Coverage Rate | ~15% | 30%+ | Quote more parlays = more fills |
| Max Game Exposure | Varies | <10% of bankroll | No single game should threaten you |
| Avg Response Time | Unknown | <500ms | Speed matters for winning quotes |
