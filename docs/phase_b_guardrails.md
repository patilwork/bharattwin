# Phase B — risk guardrails + kill-switch

> `src/guardrails.py`, `scripts/guardrails_check.py`, wired into the orchestrator and
> the FRIDAY HUD. The layer that must PASS before a book is trusted (paper) and,
> later, before any real order (Phase C). PAPER-SAFE — reports and blocks recording;
> never places or cancels anything.

## Pre-registered limits (`guardrails.LIMITS`)
| limit | value | severity | rationale |
|---|---|---|---|
| max_name_weight | 8% | BLOCK | single-position concentration cap |
| min_holdings | 15 | BLOCK | diversification floor |
| max_sector_weight | 45% | WARN | sector concentration (momentum naturally clusters) |
| min_price | ₹10 | WARN | penny-stock floor |
| min_mcap_cr | ₹2000cr | WARN | liquidity floor |
| max_gross | 1.0 | BLOCK | **no leverage** (gross ≤ 100%) |
| max_stale_days | 5 | BLOCK | data-freshness gate — don't trade on stale data |
| min_universe | 200 | BLOCK | universe-coverage gate |
| drawdown_halt | -25% | HALT | live-book loss vs entry that trips the kill-switch |

Limits are pre-registered here, not tuned per book.

## The three severities
- **BLOCK** — the orchestrator refuses to record the book (stale data, thin universe,
  over-concentration, leverage).
- **WARN** — record but flag (soft concentration, borderline individual names).
- **HALT** — a live book breached the drawdown limit → **trip the kill-switch**.

## Kill-switch (`logs/killswitch.json`)
A persistent latch. Once tripped (by a drawdown HALT) it **BLOCKS all new recording**
across every book until a human clears it: `python scripts/guardrails_check.py
--reset-killswitch`. This is the circuit-breaker that a real-money system needs — one
book blowing through its drawdown limit freezes the whole apparatus, not just itself.

## Wiring
- **Orchestrator** (`run_monthly`): evaluates each proposed book before step 5; a
  blocking verdict prevents recording (`recorded.blocked_by_guardrails`), and a
  drawdown HALT trips the latch.
- **CLI** (`scripts/guardrails_check.py`): audits every live book against the limits,
  shows current MTM and any breaches, manages the latch.
- **HUD**: a `RISK · OK/WARN/BLOCK` pill in the header (or `⛔ KILL-SWITCH TRIPPED`),
  a status dot per book in the compare strip, and a `GUARDRAILS` line per book.

## Current state (all 5 live books)
All within limits — `RISK · OK`, kill-switch clear. Sector check confirmed the books
are genuinely diversified (Core's largest sector is Financial Services at 28%, spread
across 11 sectors), so no concentration flag fires. Verified the checks *do* fire on
bad input (stale data, thin universe, oversized position, sector >45%, drawdown ≤-25%).

## Honest limits / Phase-C upgrade path
- **Drawdown is MVP** — current loss vs entry, not true peak-to-trough (needs a daily
  equity series; a Phase-C refinement).
- **Kill-switch is a JSON latch**, not an append-only audit record — fine for paper;
  Phase C should persist risk events to a DB table (like the forecast ledger).
- Everything here is paper-safe. Phase C (live execution) adds: order-level pre-trade
  checks (ADV/impact sizing), broker-side limits, and the human-in-the-loop gate where
  the agent generates orders and the USER places them.
