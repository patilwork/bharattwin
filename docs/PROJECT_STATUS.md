# BharatTwin Super-Quant — Project Status & Resume Guide

> Master handoff doc. Read this first in a fresh session. Last updated 2026-07-14.
> Branch: `superquant-foundation` (pushed to github.com/patilwork/bharattwin).

---

## 0. TL;DR — where we are
We validated ONE real edge: a **cross-sectional momentum + value** long-only
equity strategy on Indian stocks. It's about as proven as a backtest can make it
(9yr, every regime, survivorship-robust, permutation p<0.001) but the in-sample
**Deflated Sharpe is 0.82 (<0.95)** — real signal, imprecise magnitude on a short
sample. A **live 25-stock paper book is running** (the out-of-sample proof).
Realistic edge: **~4-6%/yr alpha over a liquid-equity benchmark, Sharpe ~0.9,
25-40% drawdowns** — good, not get-rich. No real money until the paper track
confirms (~2-3 months). Automation is built ~60%.

**Immediate next task (user-chosen): build TIER 1 upgrades (§7).**

---

## 1. Environment — how to bring it back up
```bash
# Postgres 16 (Homebrew) — local, trust auth
export PATH="$(brew --prefix)/opt/postgresql@16/bin:$PATH"
brew services start postgresql@16          # if not running
# Databases: bharattwin (app) + dawn (restored fundamental/price panel)

cd ~/Developer/bharattwin
export DATABASE_URL="postgresql://localhost:5432/bharattwin"
export DAWN_URL="postgresql://localhost:5432/dawn"
# venv: Python 3.14, deps installed directly (NOT editable — code imports as src.*,
# run from repo root). Recreate if missing: python3 -m venv .venv && \
#   .venv/bin/pip install sqlalchemy alembic psycopg2-binary pandas pydantic \
#   python-dateutil pytz httpx scipy pytest
.venv/bin/python scripts/<script>.py       # run anything
.venv/bin/alembic upgrade head             # migrations (currently at 0005)
.venv/bin/python -m pytest -q               # tests (26 pass)
```
- `.env` in repo is **gitignored** (holds DATABASE_URL, LLM_PROVIDER; NO secret key yet).
- Dawn dump: `~/Developer/dawn_08072026.dump` (226MB, restored). Dawn SOURCE repo:
  `~/Developer/Weekly` (weekly-reckoner / Kredere dawn1) — has the XBRL fetcher/parser.

## 1a. Connectors
- **Kite MCP** connected this session (server id `bf2a3ee1-...`, user TC6386,
  kolarcapital@gmail.com). READ-ONLY use only — we place NO live orders.
  Tools: get_ltp, get_historical_data, search_instruments, get_holdings, etc.
  Re-auth via the `login` tool if session expires (returns a URL for the user).
- Kite MCP is interactive; unattended automation later needs **Kite Connect** API
  (dev key ~₹2000/mo, own token).

## 1b. ⚠️ OPEN SECURITY TODO (user must do)
A live Sarvam API key was hardcoded in `scripts/daily_predict.sh` (public repo).
Scrubbed from working tree (loads from .env now) but **still in git history** —
**rotate at console.sarvam.ai**, then add to `.env` as `SARVAM_API_KEY=`.

---

## 2. What BharatTwin is (baseline, pre-this-session)
Indian-equities agent-swarm + factor engine predicting next-day Nifty direction.
~46% direction accuracy (its own docs: "not edge after costs"). Block-bootstrap MC
built. 10 LLM archetype agents. Data died with a Colima VM wipe; DB rebuilt fresh
(empty except schema) this session. Key insight from operating guide: **next-day
index direction is a no-edge product; the real edge is cross-sectional selection.**

## 3. The Dawn data (our fundamental/price panel — local `dawn` DB)
- `security_fundamentals`: 29,027 rows, 1,187 symbols, 2021-Q2→2026-Q1 (20 quarters).
  Well-populated: shares_outstanding 99.9%, bvps/net_worth 52%, ttm_eps 32%.
  **Sparse: pat/revenue/cfo/debt/opprofit ~3.7%** (the deep-fundamental gap).
- `stock_prices_daily`: 2.2M rows, 2,921 symbols, 2016-01→2026-07 (~10.5yr).
- `corporate_actions`: splits/bonuses (for adjustment), dividends. `announcements`
  (governance events). `security_ratios` (recent snapshots only — recompute needed).
- 212 "dead" names (delisted/suspended, price series ended early) — captured
  survivorship signal.

---

## 4. THE VALIDATED EDGE (all findings)
Strategy = cross-sectional **momentum(12-1) + value(E/P, P/B)** composite, liquid
universe (₹2000cr+ mktcap), top-quintile / top-25, equal-weight, monthly, long-only.

| Test | Result | Doc |
|---|---|---|
| Cross-sectional IC | momentum t=3.21 (5yr), **4.04 (9yr)**, positive EVERY year incl 2018 crisis + 2020 COVID | xsection_backtest, extended |
| Permutation null | **p<0.001** all factors — signal is real, not luck | montecarlo |
| Deflated Sharpe | composite **0.82 (<0.95)** — real but imprecise on short sample | montecarlo/backtest |
| Bootstrap | Sharpe 5th-pctile >0 all strategies; P(losing yr)<1% | montecarlo |
| Survivorship | **~0 impact** on long-only momentum (holds winners; delisted are losers) | extended |
| Weighting | **equal-weight optimal** — random-MC 63rd pctile; inv-vol/signal WORSE (DeMiguel 1/N) | weight_schemes |
| Rebalance freq | **monthly best** (Sharpe 1.71 vs weekly 1.57); weekly = bull artifact + 2.5x cost | rebalance_frequency |
| Leverage/concentration | LOWER median (vol drag) + RAISE ruin. 10 names 2x = -21% median, 52% ruin. **No 50% without huge ruin** | expectancy_sim |
| 2nd signal | **VALUE is the uncorrelated diversifier** (momentum⟂value_pb +0.14; blend Sharpe 0.81→1.16). Reversal fails. low-vol/low-beta uncorrelated but LOST in bull (regime) | signal_hunt |
| Corroboration | independently re-derived the canon (Asness Value&Momentum, DeMiguel 1/N, Lopez de Prado DSR, Narang costs, SMB≈0 India) | corroboration |

**Realistic economics:** ~4-6%/yr alpha, ~15-18% total gross (mostly beta), Sharpe
~0.6-0.9, drawdowns -25 to -40%. On ₹10L: ~₹1.5-1.8L/yr expected of which ~₹40-60k
is skill. NOT an income engine; a satellite sleeve IF proven live.

---

## 5. What's built (code inventory, all on branch superquant-foundation)
**Migrations** (db/migrations/versions/): 0004 forecast_ledger + forecast_score
(append-only prediction spine), 0005 paper_portfolio + paper_portfolio_pnl.

**src/**: `costs.py` (India cost engine — STT/exchange/SEBI/stamp/GST/brokerage/
slippage; futures ~6bps, delivery ~25bps). `ledger.py` (append-only forecast ledger:
input snapshot+hash, model_version, data-quality grade; record_forecast/record_score,
idempotent). `papertrack.py` (compute_portfolio from Dawn + record with live Kite
entries + score_portfolio). Fixes: `scoring.py` FLAT_BAND_PCT=0.25 (standardized
threshold, fixed double-credit bug), `pipeline.py` (ledger wired in), survivorship
liveness filter in `scripts/xsection_backtest.py`.

**scripts/**: xsection_backtest.py, xsection_montecarlo.py, expectancy_sim.py,
weight_schemes.py, rebalance_frequency.py, xsection_extended.py, signal_hunt.py,
paper_rebalance.py (monthly form/score runner), daily_predict.sh (key-scrubbed).

**docs/**: superquant_operating_guide.md (14-domain research synthesis),
quant_resources_catalog.md (47-resource 3-layer crawl), plus results docs:
xsection_backtest_results, xsection_montecarlo_results, expectancy_ruin_results,
weight_schemes_results, rebalance_frequency_results, extended_survivorship_results,
signal_hunt_results, corroboration, and THIS file.

**LIVE STATE:** `paper_portfolio` id=1 recorded (25 names, ₹1L, 2026-07-03 signal,
live Kite entries). First week (to 2026-07-14): -1.25% vs Nifty -0.84% = -0.41%
excess (7 days = pure noise, machinery-proof only).

---

## 6. Two structural bottlenecks (the honest walls)
1. **Data depth** — improving the edge (quality, forensic, EBIT/EV) needs deep XBRL
   fundamentals. NSE Integrated-Filing API serves only **Dec-2024+**; full 2021-24
   history needs paid data (Prowess/CMIE, Capitaline) or heavy BSE scraping.
2. **Time** — proving the edge needs the live paper track to accrue ~2-3 months.
   No code shortcut. Nothing to build; it just has to run.

---

## 7. ROADMAP / OPEN ITEMS (do in this order)

### TIER 1 — free, now, price-based (← NEXT TASK the user approved)
- [ ] **12-2 momentum + Frog-in-the-Pan path-quality** (canon's preferred spec vs our 12-1)
- [ ] **Trend/tail-risk overlay (GVMT)** — moving-average de-risk to cut -30% drawdowns
- [ ] **Combinatorial Purged CV** (Lopez de Prado) — stronger than our permutation test
- [ ] **Low-vol/low-beta REGIME conditioning** — test if low-vol's negative t flips
      positive in bear/high-vol regimes (validates the Frazzini-Pedersen reconciliation
      → potential regime-switched 3rd diversifier)

### MORNINGSTAR MCP — tested 2026-07-14 (partial unblock for LIVE overlay only)
Morningstar MCP (server 03b08385-...) covers Indian NSE+BSE stocks. Tested:
- WORKS: ROIC-TTM `STA4Z` (Reliance 6.79%), Quantitative Fair Value `QV009` (₹1278),
  moat/star-rating (already used by BharatTwin). Packaged metrics resolve.
- FAILS: raw line items (revenue EQX1P, net income EQVHZ, gross profit EQ46Q, EBITDA
  margin EQB5T) ERROR; and NO time-series history returns (latest snapshot only,
  date-range calls all error).
- VERDICT: usable as a LIVE current-snapshot quality/value OVERLAY on the 25-stock
  book (ROIC + fair-value + moat gate → "cheap-AND-good" screen, free, no paid data).
  NOT a historical-backtest source (Tier 3 still needs Prowess/Capitaline). Forensic/
  governance events NOT in Morningstar (still need NSE announcements).
- ACTIONABLE NOW: add a Morningstar ROIC+fair-value quality gate to papertrack's
  live picks (per-stock id-lookup + data-tool, ~25 calls/month, flaky so retry).

### TIER 2 — fetch run (Dawn's fetcher), RECENT quarters only (~2025-26)
- [ ] Backfill deep fundamentals via `calc/fetchers.py fetch_all_integrated_filings`
      (NSE Integrated Filing, Dec-2024+ only) → enables as LIVE OVERLAYS (not deep backtest):
- [ ] **Quality factor** (ROIC, gross-profitability, Piotroski, Sloan accruals)
- [ ] **EBIT/EV + FCF-yield value** (upgrade from P/B — canon-preferred)
- [ ] **Forensic accounting** (CFO/PAT divergence, Beneish M-score)
- [ ] **Governance events** via `fetch_announcements` + keyword/LLM extraction
      (auditor/CFO resignations, qualified opinions)

### TIER 3 — needs PAID data / heavy scrape (decision required)
- [ ] Full-history (2021-2024) deep fundamentals → Prowess/CMIE or Capitaline
      (unlocks a properly-BACKTESTED quality/forensic sleeve — the guide's strongest edge)
- [ ] Promoter pledge % (shareholding-pattern source)

### AUTOMATION (the "system that makes money")
- [ ] **Phase A** — automated monthly orchestrator: data refresh → signal → order
      tickets → ledger → score last month. Scheduled (cron). Still PAPER.
- [ ] **Phase B** — kill-switch + monitoring: data-quality gate, drawdown halt,
      position/exposure limits, alerts. Before any real money.
- [ ] **Phase C** — live execution (GATED on: paper proof ~2-3mo + Kite Connect
      setup + SEBI retail-algo compliance). I build/generate orders; USER places
      the live trade (I never do).

### PAPER TRACK MAINTENANCE (ongoing)
- [ ] Monthly: fetch Kite LTPs for held names → score_portfolio(id) → re-form next
      portfolio → record. (`scripts/paper_rebalance.py`; entry prices via Kite MCP LTP.)

### HOUSEKEEPING
- [ ] Rotate Sarvam key (user) — §1b
- [ ] Fix crontab path (still points to nonexistent `/Developer/niftwin/bharattwin`;
      hold until key rotated + a real pipeline is ready)
- [ ] Nothing committed uses the paper book for real money — keep it that way until proven.

---

## 8. Hard truths to carry forward
- The edge is REAL but MODEST (~4-6% alpha) and UNPROVEN LIVE. Do not oversize.
- No config reaches 50% return without ~50%+ ruin probability. Leverage lowers the median.
- The remaining alpha is gated on DATA DEPTH (costs money), not cleverness.
- Automation is a discipline machine, not a money machine — it executes a real edge
  without behavioral error; it creates no edge itself.
- Kite = read-only. Live orders are the user's action, never the agent's.
