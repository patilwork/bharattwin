# Weighting-Scheme Comparison — why equal-weight (and MC-ing the weights)

Same top-quintile momentum+value names each month; only the weighting varies.
Dawn panel, 60 rebalances, net of cost.

| scheme | ann% | Sharpe |
|---|---|---|
| equal (1/N) | 38.0 | **1.51** |
| inverse-vol | 25.9 | 1.15 |
| signal-tilt (softmax z) | 36.3 | 0.77 |

Random-weight Monte Carlo (2000 Dirichlet weightings): Sharpe p5=1.36,
median=1.48, p95=1.61. **Equal-weight (1.51) sits at the 63rd percentile.**

## Conclusions
- **Weighting barely matters** — the random spread is tiny (1.36–1.61), so the
  edge is in STOCK SELECTION, not weighting. Adding weighting parameters = pure
  overfitting surface for ~zero gain.
- **Equal-weight is robust, not lucky** (63rd pctile), and beats both "smart"
  schemes — the DeMiguel-Garlappi-Uppal 1/N result, live.
- inverse-vol underweighted the high-vol momentum winners (bull regime); signal-
  tilt concentrated risk and cut Sharpe.
- The best random weighting (1.61) beats EW by only 0.10 Sharpe and is
  unharvestable (look-ahead). Not worth chasing.
- **Spend effort on selection + a second uncorrelated signal, not weighting.**
