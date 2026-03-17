# AGENTS.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## What This Project Is

BharatTwin is an agent-based swarm intelligence engine for simulating Nifty 50 market dynamics. It defines 10 Indian market participant archetypes (FII quant, retail momentum, dealer hedging, operator, dabba speculator, etc.) as LLM-backed agents, then statistically amplifies their decisions to a 1-million-agent swarm. Consensus uses regime-conditional impact weights and is scored against actual outcomes in a replay scoreboard. Currently a research prototype — live daily predictions are the next milestone.

## Prerequisites

- Python 3.11+
- PostgreSQL 16 on port 5434 (non-standard port to avoid conflicts)
- Dev dependencies: `pip install -e ".[dev]"`

Start the database:
```bash
docker run -d --name bharattwin_db \
  -e POSTGRES_DB=bharattwin \
  -e POSTGRES_USER=bharattwin \
  -e POSTGRES_PASSWORD=devpassword \
  -p 5434:5432 postgres:16
```

Run Alembic migrations after starting the DB:
```bash
alembic upgrade head
```

## Environment Variables

Copy `.env.example` to `.env`. Key variables:

- `DATABASE_URL` — defaults to `postgresql://bharattwin:devpassword@localhost:5434/bharattwin`
- `ANTHROPIC_API_KEY` — for running agents in API mode; without it agents fall back to `prompt` mode
- `LLM_PROVIDER` — `anthropic` (default), `sarvam`, or `openai_compat`
- `SARVAM_API_KEY` / `SARVAM_MODEL` — for Sarvam 105B (India-built alternative)
- `RAW_LAKE_ROOT` — local cache for downloaded data files (default: `data/raw`)

## Commands

**Run the full pipeline for a date:**
```bash
python -m src.pipeline 2026-03-16
python -m src.pipeline 2026-03-16 --with-event   # include event_store lookup
python -m src.pipeline 2026-03-16 --mode prompt  # generate prompts only (no LLM)
```

**Dashboard and scoring:**
```bash
python -m src.dashboard --full    # market overview + replay scoreboard
python -m src.scoring             # score yesterday's prediction vs actual
python -m src.scoring 2026-03-16  # score specific date
```

**Health check:**
```bash
python -m src.health              # checks DB, data coverage, API key, agents
```

**Backtest and calibration:**
```bash
python -m src.backtest --replay-only
python -m src.calibration         # per-agent accuracy analysis across all replay cases
```

**LLM provider status:**
```bash
python -m src.agents.llm_providers
```

**Tests:**
```bash
pytest                            # run all tests
pytest tests/unit/test_agents.py  # unit tests only (no DB needed)
pytest tests/unit/test_agents.py::test_incontext_replay_consensus  # single test
```

**Lint / typecheck:**
```bash
ruff check src/
mypy src/
```

## Architecture

### Pipeline Flow

```
NSE bhavcopy + FII/DII flows + RBI FX + Zerodha index prices
  → src/ingestion/          (ingest to DB)
  → src/stores/market_state.py  (assemble market_state row)
  → src/factors/engine.py       (compute momentum, breadth, volatility)
  → src/agents/runner.py        (run 8+2 archetypes via LLM)
  → ConsensusResult             (conviction-weighted aggregation)
  → agent_decisions table       (stored for scoring)
  → src/scoring.py              (compare vs actual next-day return)
```

### Two-Tier Agent System

**Tier 1 — Archetypes (10 LLM-backed agents):**
Each persona in `src/agents/personas/` has a unique `SYSTEM_PROMPT` and `PersonaConfig` describing its biases, focus areas, and risk profile. `BaseAgent` builds (system, user) prompt pairs from the formatted `market_state` and optional `event`, then calls the LLM and parses the JSON `AgentDecision`.

**Tier 2 — Swarm Amplification (statistical, no LLM calls):**
`src/swarm/generator.py` procedurally generates up to 1M variant agents across 12 personality dimensions. `src/swarm/runner.py::amplify_to_swarm()` takes archetype `AgentDecision` outputs and projects them to a population using structured noise (experience, risk tolerance, herd sensitivity, etc.) plus `VOLUME_WEIGHT` (market participation share) and `IMPACT_WEIGHT` (price impact per rupee) for realistic aggregation.

### Key Data Stores (PostgreSQL)

- **`bhavcopy_raw`** — NSE EOD OHLCV per symbol per date (EQ series)
- **`market_state`** — one row per trading day; typed columns for Nifty/BankNifty/VIX + JSONB maps: `returns_map`, `flow_map`, `macro_map`, `factor_map`, `regime_state`
- **`flow_store`** — FII/DII buy/sell/net flows by participant and segment
- **`calendar_store`** — trading calendar with expiry flags (monthly/weekly for Nifty, BankNifty, FinNifty)
- **`event_store`** — market events with source tier, factor tags, and expected market channel
- **`agent_decisions`** — per-agent `AgentDecision` JSON and `ConsensusResult` JSON keyed by `(run_date, agent_id, session_id)`

`session_id` follows the pattern `CM_YYYY-MM-DD`. The `market_state` row uses `COALESCE` on upsert so that index prices from Zerodha (populated separately) are never overwritten by a NULL from bhavcopy.

### Agent Decision Schema

`AgentDecision` (Pydantic): `direction` (BUY/SELL/HOLD), `confidence_pct`, `nifty_return` (low/base/high %), `conviction` (1–5), `thesis`, `sector_views`, `key_factors`, `risks`.

Consensus uses conviction-weighted aggregation with a HOLD-respect rule: HOLD wins unless directional conviction is >1.5× the HOLD conviction mass.

### Replay Cases

`src/replay/` contains hard-coded historical market states + curated agent responses for 4 events (RBI hike May 2022, Election June 2024, Exit Poll June 2024, Live Mar 2026). These are used for regression testing — tests assert direction correctness and error bounds (0.5–2pp depending on event type).

### Ingestion

All ingesters inherit from `src/ingestion/base.py::BaseIngester`, which handles rate-limiting, retries, and raw lake caching (`data/raw/`). NSE bhavcopy supports both post-2023 (`BhavCopy_NSE_CM_...`) and pre-2023 column formats. Zerodha index prices are loaded via MCP and stored directly into `market_state` typed columns.

### Adding a New Agent Persona

1. Create `src/agents/personas/<agent_id>.py` with `SYSTEM_PROMPT` string and a `PersonaConfig` instance.
2. Register it in `src/agents/personas/__init__.py` in `ALL_PERSONAS` and `PERSONA_BY_ID`.
3. Add the archetype string to `ARCHETYPES` in `src/swarm/generator.py` and add entries to `VOLUME_WEIGHT` and `IMPACT_WEIGHT` in `src/swarm/runner.py`.
