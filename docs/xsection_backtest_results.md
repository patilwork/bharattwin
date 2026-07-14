# Cross-Sectional Factor Backtest — Hardened (v2)

Dawn panel, 2021-06→2026-06, 61 monthly rebalances. Prices back-adjusted for
splits/bonuses (210 symbols, 234 events). Net of 24.6bps delivery + 25bps impact
proxy. PIT-lagged 90d. Composite is long-only top-quintile, turnover-costed.

## Single factors — IC by year (persistence test)
| Factor | mean IC | IC t | LS ann% | per-year IC |
|---|---|---|---|---|
| **momentum** | 0.048 | **3.21** | +17.9 | +.075/+.007/+.092/+.020/+.048/+.068 — **positive every year** |
| value_ey | 0.050 | 3.77 | +21.2 | positive except **2026 −0.035** |
| value_pb | 0.044 | 2.57 | +23.4 | 2021 +0.24 (rebound), **2026 −0.048** |
| low_vol | 0.003 | 0.17 | −34.1 | no signal |
| size | 0.007 | 0.42 | +16.3 | no signal |

## Long-only composite (momentum + value)
| | ann return | Sharpe | max_dd |
|---|---|---|---|
| composite | +39.9% | 1.70 | −18.8% |
| benchmark (EW universe) | +21.5% | 1.08 | |
| **excess** | **+18.4%** | **1.48** | |

- **Deflated Sharpe (composite abs): 0.821** — below the 0.95 bar
- **Deflated Sharpe (excess vs universe): 0.755** — below the 0.95 bar

## Verdict: momentum robust; composite promising, NOT yet proven
- **Momentum is the defensible signal** — IC t=3.21, positive in all 6 years,
  survives price adjustment. This is the real candidate.
- **Value is real but decaying** — strong historically, negative IC in 2026;
  more survivorship-exposed (cheap names that blew up are missing).
- **Composite economics are large (+18% excess) but DSR 0.76–0.82 < 0.95** — after
  correcting for 5 factors tested + short sample + non-normality, we cannot yet
  reject luck at 95%. Suggestive, not conclusive.
- **Survivorship bias remains unquantified** and likely inflates all levels.

## What proves it (turn 0.82 DSR → conviction)
1. Add delisted universe (kills the survivorship inflation) — needs a fuller
   historical constituent+price source than Dawn currently has.
2. Live out-of-sample paper track via the forecast ledger (~2-3 months) — the
   real DSR-independent test.
3. Momentum-only sleeve first (most robust); add value as a diversifier, not core.
4. Volume/ADV data → real impact costs (the +25bps proxy is a guess).
