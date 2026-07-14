# Extended (9yr) + Survivorship-Aware Momentum + Second-Signal Test

Dawn prices 2016-2026 (adjusted). Momentum extended to 2017-2026 (113 monthly
obs). Value can't extend (fundamentals start 2021). Net of cost.

## 1+2. Extended momentum + survivorship
| variant | n | mean IC | IC t | ann% | Sharpe |
|---|---|---|---|---|---|
| survivorship-aware (liveness filter) | 113 | 0.0532 | **4.04** | 28.1 | 1.19 |
| naive ffill (biased) | 113 | 0.0532 | 4.03 | 28.1 | 1.19 |

**IC by year (every year positive, incl. crises):**
2017 +.060 · 2018 +.069 · 2019 +.104 · **2020 +.010 (COVID)** · 2021 +.062 ·
2022 +.008 · 2023 +.092 · 2024 +.020 · 2025 +.048 · 2026 +.068

Key findings:
- **Momentum is stronger over 9yr, not weaker: IC t = 4.04** (more data, more power),
  and **positive every single year including the 2018 NBFC crisis and 2020 COVID crash.**
  Sharpe 1.19 is the honest cross-regime number (below the 5yr bull's 1.5-1.7).
- **Survivorship drag ≈ 0.0pp.** The fix made NO difference — because long-only
  momentum holds WINNERS, and delisted names are LOSERS it never holds. Momentum
  long-only is naturally survivorship-resistant. (Value would be more exposed —
  cheap stocks that delist — but value can't extend here anyway.)

## 3. Second signal: short-term reversal — FAILED as a diversifier
| signal | IC t | ann% | Sharpe | corr with momentum |
|---|---|---|---|---|
| reversal (1-month) | 1.84 (n.s.) | 16.8 | 0.69 | **+0.88** |

- Reversal is weak (IC t < 2) AND +0.88 correlated with momentum.
- The real obstacle: **long-only equity strategies all correlate ~0.9 via shared
  market beta**, regardless of signal. True diversification needs market-NEUTRAL
  (long-short) construction — which single-stock shorting restrictions make hard
  in India. A second *uncorrelated* sleeve likely needs a different asset/structure
  (e.g. hedged, or a different market), not just a different equity signal.

## Net
- Momentum edge is now well-validated across 9yr and multiple crises; survivorship
  is a much smaller threat for long-only momentum than feared.
- "Add a second uncorrelated signal" is harder than hoped: beta dominates long-only
  correlation. Diversification requires hedging/market-neutrality, not just another
  long-only factor.
- True 20yr needs a survivor-only Kite backfill to 2006 → more bias, not worth it.
