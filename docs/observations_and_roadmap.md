# BharatTwin — Observations & Roadmap

> Captured during extended build session (2026-03-17).
> These are working observations from real implementation, not aspirational planning.

---

## System Diagnostics (current state)

### Factor Backtest (5,008 days, real data wired)
- Direction accuracy: 42.5% baseline → **46.2% after autoresearch** (+3.7pp on test)
- Test set: **45.0%** (generalizes, confirmed by multi-scale interleaved split)
- Error: 0.88pp (stable across all optimization)
- Big move days (>2%): 34.1% → **45.6% after optimization** (+11.5pp)
- HOLD rate: 43.8% — model is uncertain nearly half the time
- Bull accuracy: 44.7% vs Bear accuracy: 39.6% — asymmetric, worse at calling bear days
- PnL proxy: +226.6pp cumulative over 20 years — positive directional edge exists
- Worst years: 2008 (34.6%), 2009 (34.2%) — GFC chaos
- Best years: 2023 (48.8%), 2017 (46.8%) — trending markets

### Vectorized Evaluator (Phase 1 complete)
- 0.04s per evaluation (375x faster than loop-based)
- Block bootstrap: 200 scenarios in 9.4s
- Bootstrap composite: p10=30.76, median=31.68, p90=32.48 — spread of 1.72 (narrow = stable)

### Agent Replay (4 cases, LLM-based)
- Event direction: 3/3 correct
- Avg error: 0.53pp on events, 1.03pp on no-event
- #1 agent: dealer_hedging (0.43pp avg)
- Conviction=5 predictions average 0.30pp error
- dii_mf: 100% direction accuracy but worst magnitude (too conservative)

### Data Health
- bhavcopy_raw: 536,816 rows (96 stocks × 5,010 dates)
- market_state: 5,010 rows with Nifty/BN/VIX + repo rate
- USDINR: 2,004 dates (Sep 2017+)
- Futures OI: Nifty 3,825 dates + BankNifty 3,726 dates
- Real breadth from stock data: 5,009 dates
- Events: 2 (25 more ready in insert script)
- Live predictions scored: 0

### Readiness: RESEARCH-OK (not production-ok)
- No stored agent_decisions — no live predictions have been scored
- Only 2 events in event_store — need 5+ for replay validation

---

## Data Gap Assessment

### What exists but isn't wired yet
| Data | In DB | In Backtest | Impact if wired |
|------|-------|-------------|-----------------|
| Real breadth (101 stocks) | Yes | **Yes (wired)** | Was highest — now done |
| Morningstar moat/valuation | Yes (76 stocks) | No | Medium — regime filter |
| Futures OI (2010+) | Yes (3,700 dates) | **Yes (wired)** | Done |
| USDINR (2017+) | Yes (2,004 dates) | **Yes (wired)** | Done |
| Repo rate | Yes (all dates) | No (in DB, not in signal) | Low — changes too slowly |
| FII/DII monthly | JSON ready, not loaded | No | Low |

### What's missing
| Data | Gap | Impact | Effort | Decision |
|------|-----|--------|--------|----------|
| Options PCR/max pain historical | No data | Deferred | Paid vendor ₹5K/mo | Phase 2+ |
| FII/DII daily flows | API returns current only | Low current impact | Scraping or paid | Deferred |
| USDINR pre-2017 | RBI WAF-blocked | Low | RBI CSV download | Nice-to-have |
| Futures OI pre-2010 | Kite doesn't have it | Negligible | NSE archives | Not worth it |

### Strategic assessment
The backtest currently uses Nifty close + VIX + momentum + real breadth + USDINR + OI. The data we have massively exceeds what the backtest consumes. Missing data doesn't affect current accuracy numbers. When new signals are wired, they only fire for periods where data exists.

---

## Autoresearch Insights

### What 10,000 experiments taught us
- **Momentum is king**: weight consistently optimizes to 0.40-0.48 (double the initial 0.25)
- **VIX regime is overrated**: weight drops from 0.15 to 0.07-0.12 — VIX level alone is a weak predictor
- **Mean reversion is real**: threshold settles at 2.5-3.0 (not 2.0) — large moves DO revert
- **Direction thresholds should be asymmetric**: buy threshold ~0.13 vs sell ~-0.10 — model should be quicker to call sell
- **Breadth bull threshold is high**: ~80-85% breadth needed to confirm bullish — only extreme breadth is signal
- **VIX crisis threshold is lower than assumed**: ~22 not 30 — markets enter fear mode earlier

### Bootstrap robustness
- p10 composite = 30.76 (lower tail)
- p90 composite = 32.48 (upper tail)
- Spread of 1.72 = narrow = parameters are not fragile
- Phase 2 goal: optimize for p10 (worst-case), not mean

### Multi-scale interleaved split
- Better than temporal split (which biased by era)
- Uses 6 window sizes (3/5/10/20/30/60 days) with majority vote
- Both train and test contain every market regime
- Prevents alignment with any single market cycle

---

## Use Cases (honest assessment)

### Tier 1: Works now (research tool)
- **Pre-market briefing**: 10 agent perspectives in 30 seconds
- **Event impact simulator**: feed in RBI hike / election / budget → instant structured analysis
- **Market education**: personas teach market microstructure

### Tier 2: Works with 3-6 months of live data
- **Quantitative signal overlay**: confirmation/warning alongside human strategy
- **Regime detection dashboard**: "we're in VIX spike regime, dealer hedging dominates"
- **Backtesting-as-a-service**: 20 years, 96 stocks, composition history — reference platform for Indian quant research

### Tier 3: Works at scale (12+ months, edge proven)
- **Daily prediction subscription** (₹999/month with public scoreboard)
- **API for fintech platforms** (Zerodha, Groww, Dhan market insights)
- **Sell-side research augmentation** (automated morning note first drafts)
- **Policy simulation** (RBI/SEBI — "what if we hike 50bps?")

### What it's NOT
- Not a trading bot (doesn't place orders)
- Not financial advice (it's a simulation)
- Not a replacement for human judgment (it's structured input)
- Not accurate enough to trade blindly (46% ≠ edge after costs)

---

## Expansion Roadmap (indices and correlations)

### Index expansion (sequenced)
1. **BankNifty** — data exists, same agents, heavier FINBK weight. Config change after 30 days live.
2. **Sectoral scoring** — track per-sector accuracy. The sector_rotation agent already produces views we don't score.
3. **Midcap 150** — unlocks operator/dabba agents (designed for mid/small-cap dynamics, wasted on Nifty 50).

### Correlation signals (by expected impact)
| Signal | Mechanism | Impact | Available |
|--------|-----------|--------|-----------|
| **FII F&O index long/short ratio** | Direct positioning | **Highest** | NSE daily publish |
| **SGX/GIFT Nifty pre-market** | 9 AM gap prediction | High (intraday only) | Kite |
| **US S&P 500 overnight** | FII risk-off spillover | Medium | Free API |
| **DXY (Dollar Index)** | EM outflows proxy | Medium | Free API |
| **US 10Y yield** | Rate differential impact | Low-medium | Free API |
| **Crude oil** | CAD / OMC impact | Low-medium | Kite |
| **China A50 / Hang Seng** | EM rotation | Low | Free API |

### What NOT to expand to
- Individual stock predictions — different problem entirely
- Crypto — different microstructure, no FII/DII
- Global markets — removes the India-specific moat

---

## Accuracy Improvement Priorities

### Ranked by actual expected impact
| Priority | What | Time | Impact |
|----------|------|------|--------|
| 1 | **Start daily cron** | 30 min | Unlocks everything — live data |
| 2 | **Run event insert script** | 10 min | 25 more replay cases |
| 3 | **Sector-level breadth** | 2 hrs | Better rotation signal from existing data |
| 4 | **Opening candle signal** | 1 hr | Gap-up/gap-down as 1 extra signal |
| 5 | **FII F&O long/short ratio** | 1 day | Highest-impact new data source |
| 6 | **News RSS auto-detection** | 1 day | Removes manual event entry |
| 7 | Intraday candles | Don't | Trap — 78x data for same question |
| 8 | More regimes | Don't | Fragments data, reduces optimization reliability |
| 9 | More macro data sources | Days | Diminishing returns — VIX weight is already low |

### Why shorter candles are a trap
- Agents predict next-day direction, not intraday
- 5-min candles = 78x more data per day for the same predictive question
- Factor engine would need complete rewrite
- Intraday edge requires tick-level execution we don't have
- **Exception**: opening 15-min candle as a gap signal — 1 extra candle/day, high value

### Why live data can't be replaced by backtesting
1. LLM agents have hindsight contamination (trained on data including outcomes)
2. Factor parameters may not transfer to 2026-2027 (non-stationarity)
3. Market structure keeps changing (retail explosion, weekly F&O, T+1 settlement)
4. Bootstrap robustness helps but doesn't replace forward validation

---

## Monte Carlo Research Plan Status

| Phase | Status | Summary |
|-------|--------|---------|
| **Phase 0** (Product Truthfulness) | ~90% | Docs honest, readiness levels in health.py. API labels remaining. |
| **Phase 1** (Vectorized Evaluator) | **Done** | 0.04s eval, block bootstrap, canonical metrics. |
| **Phase 2** (Bootstrap Autoresearch) | Ready to start | Wire bootstrap into optimizer, use p10 as fitness. |
| **Phase 3** (Swarm as Scored Path) | Blocked | Needs live predictions first. |
| **Phase 4** (Optimize Swarm Weights) | Blocked | Needs 100+ scored predictions. |
| **Phase 5** (Amplification Calibration) | Blocked | Needs 200+ scored predictions. |
| **Phase 6** (Ensemble Research Question) | Blocked | Needs both systems independently scored. |

### The bottleneck
Phases 3-6 all require live scored predictions. The cron job is the single blocker for the entire research program beyond Phase 2.

---

## The Real Moat

It's not prediction accuracy. It's:
- **India-specific data assembly** (20 years, 96 stocks, composition history, operator/dabba personas, regime-conditional weights)
- **Transparent scoring** (public scoreboard, every call tracked including misses)
- **Structural authenticity** (models the actual participants — FII/DII/dealer/operator/dabba — not generic agents)

Nobody else has this combination for Indian markets. MiroFish is generic. Global tools don't model SIP flows or Rajkot satta culture. That specificity is the moat.

---

## What Success Looks Like

### 30 days from now
- Daily predictions running, 20+ scored days, first public accuracy report

### 90 days from now
- 60+ scored days, BankNifty added, Phase 2 bootstrap-autoresearch complete
- Enough data to answer: "does the LLM system add alpha over the factor model?"

### 6 months from now
- 120+ scored predictions, ensemble question answered (Phase 6)
- Public track record on X/Twitter with scoreboard link
- First paying users or API integrations

### 12 months from now
- 250+ scored predictions across Nifty + BankNifty + sectors
- Autoresearch has re-optimized 3x on rolling live data
- The project can truthfully say where it has edge and where it doesn't
