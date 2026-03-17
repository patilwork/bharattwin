# Phase 1 Plan — BharatTwin Production Pipeline

> Phase 0 (PoC) complete. This document defines Phase 1: daily automated pipeline.

## Phase 0 Scorecard

| Metric | Result |
|--------|--------|
| Replay cases | 4 (2 SELL, 1 BUY, 1 no-event) |
| Direction accuracy | 4/4 (100%) |
| Average error | 0.65pp |
| RMSE | 0.73pp |
| In-range | 4/4 (100%) |
| Event case error | 0.53pp |
| Best agent | dealer_hedging (0.43pp avg) |
| Worst agent | dii_mf / corp_earnings (1.32pp avg) |
| Test coverage | 21 tests, all passing |
| Codebase | 6,195 LOC, 51 files |

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
                   │  Events     │───▸│  8 Agents   │
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

### P1.1 — API Mode Activation
- [ ] Set ANTHROPIC_API_KEY in env
- [ ] Test API mode with single agent
- [ ] Run full 8-agent pipeline via API
- [ ] Compare API responses vs in-context responses quality
- **Model:** claude-sonnet-4-6 (agents), claude-haiku-4-5 (extraction)

### P1.2 — Daily Scheduler
- [ ] Cron job: `python -m src.pipeline <yesterday> --mode api` at 19:00 IST
- [ ] Cron job: `python -m src.pipeline --score` at 09:30 IST (score yesterday's prediction)
- [ ] Alert on pipeline failure (email/webhook)
- [ ] Idempotent: re-running same date is safe (upsert)

### P1.3 — Scoring Engine
- [ ] After market close, compare consensus vs actual Nifty return
- [ ] Store score in agent_decisions table
- [ ] Track per-agent rolling accuracy
- [ ] Adaptive conviction: multiply agent conviction by rolling accuracy factor

### P1.4 — Historical Replay Expansion
- [ ] 10 more event cases: demonetization 2016, budget 2020 (COVID), RBI cuts 2020,
      US-China tariffs 2018, IL&FS crisis 2018, NBFC crisis 2019, COVID crash Mar 2020,
      COVID recovery Apr 2020, Russia-Ukraine Feb 2022, Adani short Jan 2023
- [ ] 10 no-event days: random selection from different market regimes
- [ ] Target: 24+ cases for statistical significance

### P1.5 — Data Enrichment
- [ ] Zerodha historical index prices via cron (auto-backfill)
- [ ] Morningstar fundamentals refresh weekly
- [ ] Derivatives data: Nifty OI, PCR, max pain (from Zerodha or NSE)
- [ ] News/event ingestion: RSS feed or API for auto event detection

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
