# Multi-book paper track — pre-registered variant set

> Added 2026-07-14. Why we now run three books instead of one, and the discipline
> that keeps it from re-introducing overfitting.

## The three books (`papertrack.VARIANTS`)
| Book | strategy | overlay | gate | role |
|---|---|---|---|---|
| **Core** ★ | `momentum_value_composite_v1` | off | off | **primary** — the clean OOS proof of the validated edge |
| Trend overlay | `momentum_value_trend_v1` | **on** | off | attribution: marginal value of the Tier-1.2 de-risk |
| Quality gate | `momentum_value_quality_v1` | off | **on** | attribution: marginal value of the Tier-2 forensic screen |

Each satellite differs from Core by **exactly one knob** (enforced by
`tests/unit/test_variants_registry.py`), so any performance gap is attributable to
that one change.

## The discipline (this matters)
- **Core is pre-registered as primary.** We never reallocate capital to whichever
  book looks best live — that would smuggle back the selection bias the DSR/CPCV/PBO
  work exists to kill. The satellites *measure* the overlays; they don't *compete*.
- **They're correlated by construction** (shared momentum+value core), so this is
  attribution, not three independent bets. Modest power gain, high interpretability.
- **Small set, not a fleet.** Three deliberate contrasts, not a sweep.

## What differs today (2026-07-03 signal)
- **Core = Trend** in holdings (regime is risk-on → 100% exposure → no de-risk). Trend
  only diverges from Core in a downturn (when it goes to cash). Until then, ~identical.
- **Quality differs immediately**: the gate drops all 9 forensically-flagged Core
  names (RAYMOND, IDEA, DIACABS, ASHOKA, CUPID, FCL, GMRAIRPORT, V2RETAIL, TATAINVEST)
  and backfills with clean names (VEDL, KOTAKBANK, RECLTD, BANKBARODA, …). 16/25 shared.

## Inception & honesty
- **Core (id=1)** has run since 2026-07-03 (entered at that day's live Kite prices).
- **Trend (id=4)** and **Quality (id=5)** were launched 2026-07-14, entered at that
  day's live Kite closes. Their P&L starts at ~0 and grows forward — NOT backdated
  (backdating would violate the ledger's "never retroactively construct" principle).
- Consequence: Core has an ~11-day head start of realised return; compare **forward**
  performance from each book's own inception, not absolute levels.
- All three share the same signal data date (2026-07-03 Dawn panel). The signal→entry
  gap for the new books (07-03 → 07-14) is a stale-data artifact, recorded honestly
  (holdings carry `entry_source: kite_ltp`; `created_ts` is the real record date).

## How to run / view
- Form/refresh all three: `python scripts/orchestrate_monthly.py --all [--dry-run]`
- Watch them side by side: the JARVIS HUD (`scripts/jarvis_server.py`) now has a
  book-switcher and a compare strip showing all three returns at a glance.
