# Phase 1 Plan — BharatTwin Production Pipeline

> Phase 0 (PoC) complete. This document defines Phase 1: daily automated pipeline.

## Phase 0 Scorecard

| Metric | Result |
|--------|--------|
| Replay cases | 4 (2 SELL, 1 BUY, 1 no-event) |
| Event direction accuracy | 3/3 (100%) |
| Overall direction (incl no-event) | 3/4 (75%) |
| Average error | 0.65pp |
| RMSE | 0.73pp |
| In-range | 4/4 (100%) |
| Event case error | 0.53pp |
| Best agent | dealer_hedging (0.43pp avg) |
| Worst agent | dii_mf / corp_earnings (1.32pp avg) |
| Test coverage | 21 tests, all passing |
| Codebase | 9,429 LOC, 68 files |
| Swarm | 1M agents, 10 archetypes, 7 regime profiles |
| Data | 5,010 trading days (20 years) |
| LLM | Sarvam 105B live (free) |
| Autoresearch | 42% → 46.5% direction (1000 experiments, 70s) |
| F&O data | PCR, max pain, OI chain via Kite MCP |

## Phase 1 Goals

1. **Daily automated pipeline** — run every market morning, produce predictions
2. **ANTHROPIC_API_KEY integration** — actual Claude API calls instead of in-context
3. **Historical validation** — 20+ replay cases for statistical significance
4. **Live tracking** — predict → observe → score → learn loop

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│  Ingest      │───▸│  Build State  │───▸│  Factors    │
│  (6:30 PM)   │    │  (6:45 PM)   │    │  (6:50 PM)  │
└─────────────┘    └──────────────┘    └─────────────┘
                                              │
                          ┌───────────────────┘
                          ▼
                   ┌─────────────┐    ┌─────────────┐
                   │  Events     │───▸│  10 Agents  │
                   │  (manual)   │    │  (7:00 PM)  │
                   └─────────────┘    └─────────────┘
                                              │
                          ┌───────────────────┘
                          ▼
                   ┌─────────────┐    ┌─────────────┐
                   │  Consensus  │───▸│  Store + Log │
                   │  Aggregator │    │  (7:05 PM)  │
                   └─────────────┘    └─────────────┘
                                              │
                          ┌───────────────────┘
                          ▼
                   ┌─────────────┐
                   │  Score T-1  │
                   │  (9:30 AM)  │
                   └─────────────┘
```

## Phase 1 Tasks

### P1.1 — API Mode Activation ✅ DONE
- [x] Sarvam 105B API key set (free tier, 60 RPM)
- [x] Test API mode with single agent (dealer_hedging: SELL -1.80%, actual -2.29%)
- [x] Multi-LLM provider: Sarvam / Claude / OpenAI-compatible
- [x] JSON parsing fix for reasoning models (brace-depth matching)
- **Model:** Sarvam 105B (free) or claude-sonnet-4-6

### P1.2 — Daily Scheduler
- [ ] Cron job: `python -m src.pipeline <yesterday> --mode api` at 19:00 IST
- [ ] Cron job: `python -m src.pipeline --score` at 09:30 IST (score yesterday's prediction)
- [ ] Alert on pipeline failure (email/webhook)
- [ ] Idempotent: re-running same date is safe (upsert)

### P1.3 — Scoring Engine ✅ DONE
- [x] src/scoring.py — compare predictions to actuals
- [x] Per-agent rolling accuracy tracking
- [x] src/calibration.py — full agent calibration report
- [x] src/autoresearch.py — Karpathy-style parameter optimization (1000 experiments, 70s)

### P1.4 — Historical Replay Expansion
- [ ] 10 more event cases: demonetization 2016, budget 2020 (COVID), RBI cuts 2020,
      US-China tariffs 2018, IL&FS crisis 2018, NBFC crisis 2019, COVID crash Mar 2020,
      COVID recovery Apr 2020, Russia-Ukraine Feb 2022, Adani short Jan 2023
- [ ] 10 no-event days: random selection from different market regimes
- [ ] Target: 24+ cases for statistical significance

### P1.5 — Data Enrichment ✅ MOSTLY DONE
- [x] 20-year index backfill: 5,010 days (Jan 2006 — Mar 2026) via Zerodha
- [x] Morningstar fundamentals: moat, P/E, P/B, fair value for Nifty heavyweights
- [x] F&O derivatives: OI chain, PCR, max pain, ATM straddle via Kite MCP
- [ ] News/event auto-ingestion: RSS feed or API (not yet built)

### P1.6 — Production Database
- [ ] Migration from local Docker to managed Postgres (Supabase/Neon/RDS)
- [ ] Backup strategy
- [ ] Connection pooling

## Success Criteria for Phase 1

| Metric | Target |
|--------|--------|
| Event direction accuracy | > 80% (on 20+ cases) |
| Event avg error | < 1.0pp |
| No-event direction accuracy | > 55% |
| Daily pipeline uptime | > 95% |
| Cases tested | 24+ |
| Automated scoring | Yes |

## Timeline

- **Week 1:** P1.1 (API mode), P1.3 (scoring engine)
- **Week 2:** P1.4 (10 more replays), P1.2 (scheduler)
- **Week 3:** P1.5 (data enrichment), P1.6 (production DB)
- **Week 4:** Validation, tuning, go-live for daily predictions
