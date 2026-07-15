# 3-year month-by-month track record (backtest)

> `scripts/track_record.py`. The live tradeable strategy (momentum+value, ₹2000cr+
> universe, top-25 equal-weight, monthly, costed) simulated PIT walk-forward for every
> month, Aug 2023 → Jun 2026 (35 months), vs NIFTY 500 TRI, on ₹10L.

## Backtest result
| | Strategy | Nifty 500 |
|---|---|---|
| total return | +252% | +40% |
| CAGR | 54.0% | 12.1% |
| volatility | 30.0% | 15.5% |
| Sharpe | 1.60 | 0.82 |
| max drawdown | -14.5% | -17.7% |
| % up months | 63% | 66% |
| ₹10L becomes | **₹35.2L** | ₹14.0L |

Per year excess vs Nifty 500: 2023 +25.3%, 2024 +55.9%, 2025 +13.0%, 2026 +23.8%.
Hit rate: 66% of months beat Nifty 500. Best month +28.0%, worst -14.5%.

## Why this is NOT a forward expectation (read this)
1. **In-sample** — the spec was chosen knowing this history. The honest version is the
   frozen-cutoff OOS test (oos_walkforward_results.md): ~19-28% excess at 11-17mo, with
   IC decaying to ~0 in the last 5 months.
2. **Regime** — 2023-26 was a once-in-a-decade Indian small/mid-cap bull; 2024 alone is
   +72% strat / +56% excess. No real bear in the window, so the -14.5% maxDD massively
   understates true crash risk (small caps fall 40-60% in bears).
3. **Costs optimistic** — model ~30-45bps; real small-cap slippage at size 150-300bps.
   Capacity-limited (the edge lives in small caps — cap_tilt_analysis).
4. **No execution friction** — no missed fills, gaps, or behavioral error.

## The honest forward number
~4-6%/yr alpha over a liquid benchmark, Sharpe ~0.9, drawdowns -25 to -40%. Modest and
real — NOT 54% CAGR. The gap between the backtest and this is the entire point of the
live paper track: the backtest seduces, the live record decides.
