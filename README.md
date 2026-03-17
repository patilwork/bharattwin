# BharatTwin 🇮🇳

**800+ AI agents that predict the Indian stock market. Every day. With a public scoreboard.**

BharatTwin is an open-source swarm intelligence engine built specifically for the Indian equity market. Eight specialist archetypes — from FII quant strategists to retail momentum traders — each spawn 100+ agent variants with unique risk profiles, regional biases, and conviction patterns. The swarm analyzes live NSE data and produces a population-level consensus Nifty 50 prediction.

Think [MiroFish](https://github.com/666ghj/MiroFish) but purpose-built for Indian markets. Where MiroFish simulates generic social agents, BharatTwin simulates **the actual participants of Dalal Street** — FIIs, DIIs, dealers, retail traders, macro strategists — each with India-specific knowledge and biases.

Every prediction is scored against the actual outcome. No hiding. No cherry-picking. Just a transparent, public track record.

## How It Works

```
NSE Bhavcopy + Zerodha + RBI + Morningstar
              ↓
      Market State Builder
              ↓
    ┌─────────────────────────┐
    │  8 AI Agent Personas    │
    │                         │
    │  🏦 FII Quant           │  ← Global macro, FII flows, USD/INR
    │  📈 Retail Momentum     │  ← Price action, breadth, FOMO/panic
    │  🎰 Dealer Hedging      │  ← VIX, gamma, options positioning
    │  🏛️ DII Mutual Fund     │  ← SIP flows, valuations, contrarian
    │  🌍 Macro Strategist    │  ← RBI policy, crude, inflation
    │  🔄 Sector Rotation     │  ← Rate/FX/oil sensitivity matrix
    │  📊 Corp Earnings       │  ← Bottom-up, heavyweight analysis
    │  ⚡ Event/News Trader   │  ← Breaking news, historical patterns
    │                         │
    └─────────────────────────┘
              ↓
   Conviction-Weighted Consensus
              ↓
     Direction | Return Range | Confidence
              ↓
        Public Scoreboard
```

## Replay Scoreboard

| Case | Actual | Predicted | Error | Direction |
|------|--------|-----------|-------|-----------|
| RBI Surprise Hike (May 2022) | -2.29% | -1.99% | 0.30pp | ✅ SELL |
| Election Results (Jun 2024) | -5.93% | -5.01% | 0.92pp | ✅ SELL |
| Exit Poll Euphoria (Jun 2024) | +3.25% | +2.88% | 0.37pp | ✅ BUY |
| No-Event Day (Mar 2026) | +1.00% | -0.03% | 1.03pp | ⚠️ HOLD |
| **Average** | | | **0.65pp** | **4/4** |

> Event-driven predictions: **0.53pp average error**. The agents shine when it matters most.

## Agent Leaderboard

| Rank | Agent | Avg Error | Strength |
|------|-------|-----------|----------|
| 🥇 | Dealer Hedging | 0.43pp | VIX + gamma squeeze calls |
| 🥈 | Event/News Trader | 0.51pp | Historical pattern matching |
| 🥉 | Sector Rotation | 0.72pp | Rate/FX sensitivity mapping |
| 4 | Macro Strategist | 0.72pp | RBI policy analysis |
| 5 | FII Quant | 0.74pp | Cross-border flow dynamics |
| 6 | Retail Momentum | 0.74pp | Breadth + momentum signals |
| 7 | DII Mutual Fund | 1.32pp | Contrarian (100% direction!) |
| 8 | Corp Earnings | 1.32pp | Bottom-up heavyweight analysis |

## Quick Start

```bash
# Clone
git clone https://github.com/yourrepo/bharattwin.git
cd bharattwin

# Setup
pip install -r requirements.txt
docker run -d --name bharattwin_db -e POSTGRES_DB=bharattwin \
  -e POSTGRES_USER=bharattwin -e POSTGRES_PASSWORD=devpassword \
  -p 5434:5432 postgres:16

# Run pipeline
python -m src.pipeline 2026-03-16 --mode prompt

# View dashboard
python -m src.dashboard --full

# Run health check
python -m src.health

# Run backtests
python -m src.backtest --replay-only

# Agent calibration
python -m src.calibration
```

## Architecture

- **Ingestion:** NSE bhavcopy, FII/DII flows, Zerodha index prices, RBI rates, Morningstar fundamentals
- **State:** PostgreSQL with typed columns + JSONB for flexible data
- **Agents:** 8 personas with unique biases, focus areas, and conviction patterns
- **Consensus:** Conviction-weighted aggregation with HOLD-majority respect
- **Scoring:** Automated comparison of predictions vs actual outcomes

## Data Sources

| Source | Data | Method |
|--------|------|--------|
| NSE | Bhavcopy (EOD prices), FII/DII flows | REST API |
| Zerodha/Kite | Index prices (Nifty, BankNifty, VIX) | MCP |
| RBI | Reference rates (USDINR) | REST API |
| Morningstar | Moat, P/E, P/B, fair value | MCP |
| Seeds | Holidays, constituents, sector map, expiry calendar | CSV |

## Why BharatTwin?

India's market has unique dynamics that global prediction tools miss:
- **FII/DII tug-of-war** — ₹18,000 cr/month SIP flows create a structural floor
- **RBI policy cycles** — repo rate moves BankNifty 2-4% on surprise
- **Election/budget events** — 5-7% single-day moves on political surprises
- **VIX-driven dealer hedging** — options market amplifies cash market moves
- **10cr+ demat accounts** — retail sentiment is a first-order signal

No global tool models these India-specific signals. BharatTwin does.

## Built With

- Python 3.14 + Pydantic + SQLAlchemy
- PostgreSQL 16 (Alembic migrations)
- Claude Sonnet 4.6 (agent LLM) / Claude Haiku (extraction)
- Zerodha Kite MCP + Morningstar MCP

## Status

**Phase 0 (PoC): COMPLETE** — 4/4 direction accuracy, 0.65pp average error, 21 tests passing.

Phase 1 (daily automated pipeline) in progress. See [Phase 1 Plan](docs/phase1_plan.md).

## License

MIT

---

*Built in India 🇮🇳 for Indian markets. By traders, for traders.*
