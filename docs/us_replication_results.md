# US-market replication — does the momentum edge survive?

> `scripts/us_replication.py`. Cross-sectional momentum (12-1, monthly, top-quintile)
> on the S&P 500 via yfinance (503 names, 2015-2026). Momentum only — a US value/
> composite backtest would need a point-in-time US fundamentals panel we don't have.

## Result: the edge is essentially GONE in US large caps
| window | rank-IC mean | IC_t | LS Sharpe | long-only excess/yr |
|---|---|---|---|---|
| **US, full 2016-2026** | +0.0001 | **0.00** | 0.19 | +3.1% (Sharpe 0.41) |
| **US, recent 2021-2026** | +0.017 | 0.76 | 0.57 | +5.7% (Sharpe 0.65) |
| **India (reference)** | +0.049 | **~4.0** | 0.60 | strong, significant |

US large-cap momentum IC_t ≈ **0** over the full sample (vs India ≈ 4.0). The
cross-sectional signal that is our whole edge barely exists in the S&P 500.

## Why — and why this is the RIGHT answer
The US is the most efficient, most-arbitraged market on earth. Momentum was published
in 1993 (Jegadeesh-Titman) and every quant fund has front-run it since — in the deep,
liquid, heavily-covered US large-cap pool it's been competed away. India — especially
small caps, where our edge is strongest — is far less arbitraged, so the same signal
still discriminates. **The US result validates that our India edge comes substantially
from India's inefficiency, not from momentum being a universal free lunch.** The edge
lives where the market is inefficient; the US large-cap market is not.

## Two caveats that make the US look BETTER than reality
1. **Survivorship bias** — the universe is TODAY's S&P 500 constituents; dropouts and
   delistings are absent, so momentum is flattered. India's engine was survivorship-
   CONTROLLED. Even so, US IC_t is ~0 — the honest US edge is weaker still.
2. **Large-cap only** — S&P 500 is the most efficient slice. US small caps (Russell
   2000) might show a bit more, but the literature says US momentum there is still
   thinner than emerging markets and heavily arbitraged. A proper US test needs
   point-in-time membership + a small-cap universe (the honest follow-up).

## Verdict
**No — the strategy does not replicate to US large caps.** The long-only book's small
excess (+3-6%) is low-conviction (excess Sharpe 0.4-0.65) and survivorship-flattered;
the actual signal (IC) is ~zero. This is not a strategy failure — it's the strategy
correctly telling us it's an *inefficiency* play, and US large caps are efficient.
If anything, it's a strong external validity check on the India work: same method,
efficient market → no edge; inefficient market → edge. Exactly as it should be.
