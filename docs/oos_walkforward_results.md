# Out-of-sample walk-forward — freeze the spec, simulate forward

> `scripts/oos_walkforward.py`. Freeze the strategy at a cutoff, measure the
> composite ONLY on the months after it. Multiple cutoffs because a single recent
> one is too short to read. No look-ahead — each month's signal is PIT; the cutoff
> just marks where OOS counting starts.

| Freeze at → forward | months | comp%/yr | bench%/yr | excess/yr | Sharpe | mean IC | IC_t |
|---|---|---|---|---|---|---|---|
| **Full sample** (ref) | 60 | 44.5 | 23.0 | +18.0 | 1.68 | 0.049 | 3.81 |
| after 2024-12-31 | 17 | 31.9 | 2.4 | **+28.4** | 1.09 | 0.052 | **2.09** |
| after 2025-06-30 | 11 | 25.1 | 4.0 | **+19.3** | 1.04 | 0.056 | **2.30** |
| after 2025-12-31 | 5 | 41.8 | 32.5 | +5.4 | 1.15 | 0.002 | 0.08 |
| after 2026-03-31 (requested) | 2 | 28.3 | 30.4 | -1.6 | — | -0.020 | n/a |

## The good news — the edge survives a real hold-out
At the **statistically meaningful horizons** (11-17 forward months, frozen at end-2024 /
mid-2025), the strategy holds up **out-of-sample**:
- **Significant cross-sectional IC** (t 2.09 and 2.30, both > 2) on genuinely forward months.
- **Strong excess** (+28% and +19%/yr) — actually *higher* than the full-sample average
  (+18%), because 2025 was a hard, flat year for the broad market (benchmark +2-4%/yr)
  yet the composite still made 25-32%. Alpha in a flat-market year is the best kind of
  OOS evidence — it's not just riding beta.

This is arguably the **strongest validation to date** — stronger than the in-sample DSR
(0.82) or even CPCV, because these months were truly forward of a frozen cutoff and the
signal still discriminated (positive, significant IC).

## The yellow flag — recent IC is decaying
The **last ~5 months** tell a softer story: freezing at 2025-12 gives mean IC ≈ 0
(0.002, IC_t 0.08), and the 2026-03 window (only 2 measurable months) is slightly
negative. The *returns* held (the book still made money), but the cross-sectional
*signal* — the thing that actually is the edge — has been weak very recently. Read it as:
- Most likely **short-window noise** (2-5 months is not a verdict), OR
- **Regime cooling** — momentum/value can go quiet when the market broadens/rotates, OR
- An **early warning** the live paper track will resolve.

## Your literal request (March-2026 cutoff)
Only 2 forward months are measurable (data ends mid-July; the composite needs a full
month to score). Excess -1.6%, IC slightly negative — but **n=2 is pure noise**; the
Sharpe of 10.7 there is a meaningless small-sample artifact. Nothing to conclude from it
either way. The 2024/2025 cutoffs are the real test, and they pass.

## Verdict
- **Medium-horizon out-of-sample: the edge is real and significant** — the frozen spec
  beat the market with real IC on forward months, including a flat-market year. Encouraging.
- **Very recent (≤5mo): IC has softened toward zero** — watch it; this is exactly what the
  live 5-book paper track is for. Don't over-read 2-5 months in either direction.
- Consistency across cutoffs (positive, significant at 11-17mo; noisy at 2-5mo) is the
  honest summary — the strategy is validated OOS at horizons long enough to judge, and
  under active live watch at the horizons that are still too short.
