# Monte Carlo + Multi-Strategy Robustness Panel

Dawn panel, 2021-06→2026-06. Block bootstrap (3000 draws, block=3) + permutation
null test (1000). Net of cost. Seed 42.

## Bootstrap distributions (long-only top-quintile)
| strategy | pt Sharpe | Sh median | Sh 5% | Sh 95% | CAGR med | CAGR 5% | maxDD med | P(ann<0) |
|---|---|---|---|---|---|---|---|---|
| momentum | 1.29 | 1.29 | 0.51 | 2.19 | 29.1% | 9.3% | −23.3% | 0.6% |
| value_ey | 1.59 | 1.57 | 0.95 | 2.35 | 43.6% | 21.3% | −14.5% | 0.1% |
| value_pb | 1.25 | 1.32 | 0.67 | 2.02 | 36.6% | 14.2% | −20.0% | 0.2% |
| composite | 1.50 | 1.52 | 0.81 | 2.34 | 36.8% | 16.7% | −19.5% | 0.0% |

**All four have Sharpe 5th-percentile > 0 and P(losing year) < 1%.** Robust in-sample.

## Permutation null test — is the IC real or luck?
| factor | observed IC | null mean | null 95% | p-value |
|---|---|---|---|---|
| momentum | 0.0479 | ~0 | 0.0064 | **0.0000** |
| value_ey | 0.0503 | ~0 | 0.0091 | **0.0000** |
| value_pb | 0.0436 | ~0 | 0.0081 | **0.0000** |

**All three ICs are far outside the null (p < 0.001).** The cross-sectional
ranking has genuine predictive information — not luck.

## Reconciling permutation (p<0.001) with Deflated Sharpe (0.82 < 0.95)
They measure different things and are BOTH right:
- **Permutation** uses thousands of stock-months -> huge power -> the factor
  signal is unambiguously real.
- **Deflated Sharpe** uses only 60 monthly return obs -> low power -> the
  tradeable Sharpe's *magnitude* isn't pinned down at 95% with 5yr of history.
- Synthesis: **the edge is real; we're uncertain exactly how big the tradeable
  Sharpe is.** More encouraging than the DSR alone implied.

## Honest caveats the MC does NOT fix
- **Survivorship** — permutation tests ranking *within the surviving universe*;
  it cannot detect the bias from missing delisted names. Still the #1 open risk.
- **Stationarity** — the bootstrap treats all months as exchangeable, so it
  hides value_ey's DECAY (negative IC in 2026). Momentum is temporally stable
  (positive every year); value looks best on bootstrap but is fading.
- Strategies are **correlated** (all long similar momentum/value names) — this is
  robustness confirmation, not genuine diversification.

## Takeaways
- **Momentum is the pick**: real (p<0.001), stable across years, adjustment-proof.
- Composite is a fine blend; run it primary, log momentum-only as a shadow.
- Multiple *uncorrelated* strategies would need a different edge source
  (we only have momentum+value, which co-move). Survivorship fix > more strategies.
