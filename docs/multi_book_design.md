# Multi-book paper track — pre-registered variant set

> Added 2026-07-14. Why we now run three books instead of one, and the discipline
> that keeps it from re-introducing overfitting.

## The five books (`papertrack.VARIANTS`)
| Book | strategy | the one knob | role |
|---|---|---|---|
| **Core** ★ | `momentum_value_composite_v1` | — (baseline) | **primary** — clean OOS proof of the validated edge |
| Trend overlay | `momentum_value_trend_v1` | overlay on | marginal value of the Tier-1.2 de-risk |
| Quality gate | `momentum_value_quality_v1` | forensic gate | value of the Tier-2 red-flag exclusion (a left-tail filter) |
| Quality tilt | `momentum_value_qtilt_v1` | +quality z-leg | **live test of quality as the 3rd edge** (can't be backtested — no fundamental history) |
| Small-cap tilt | `momentum_value_smallcap_v1` | mcap ≤ ₹8000cr | where the factor edge is strongest (efficacy, not size premium) |

Each satellite differs from Core by **exactly one knob** (enforced by
`tests/unit/test_variants_registry.py`), so any performance gap is attributable to
that one change. Quality-tilt and Small-cap encode the two live experiments from
docs/third_edge_and_cap_results.md. Capacity note: the small-cap book only makes
sense at small AUM.

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
All three books share a **2026-07-03 inception** and are entered at 07-03 prices.
This is legitimate, not backdating: the entire construction uses only information
available on/before 07-03 — momentum(12-1) through 07-03, value from as-of
fundamentals, the regime MA through 07-03, and the quality gate off the 2026-03-31
fundamental snapshot. Nothing after 07-03 touches which names are picked. Measuring
that 07-03 book forward (07-03 → today → onward) is genuine out-of-sample
performance. (What *would* be illegitimate: using post-07-03 data to pick names,
cherry-picking the start date, or tuning the variant rules after seeing returns —
none of which happens; the rules are mechanical and pre-registered.)

- **Core (id=1)** entered at 07-03 live Kite prices.
- **Trend (id=6) / Quality (id=7)** re-recorded 2026-07-14 but entered at **07-03**
  prices: shared names reuse Core's exact 07-03 entries (so Trend ≡ Core until the
  overlay triggers, and Quality differs *only* by the 9-name swap); the 9 new Quality
  names use the 07-03 Dawn close. `created_ts` (the DB write date) is NOT the entry
  date — entries are as-of 07-03.
- First 11 days (07-03 → 07-14, live marks): Core -0.08%, Trend -0.08% (identical),
  Quality -0.21% — i.e. dropping the flagged names has cost ~13bps so far. Pure noise
  at 11 days, but now measured honestly and accruing forward.

## How to run / view
- Form/refresh all three: `python scripts/orchestrate_monthly.py --all [--dry-run]`
- Watch them side by side: the JARVIS HUD (`scripts/jarvis_server.py`) now has a
  book-switcher and a compare strip showing all three returns at a glance.
