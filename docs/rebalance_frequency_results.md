# Rebalance Frequency — monthly vs fortnightly vs weekly

Same composite momentum+value top-quintile, long-only, equal-weight. Only the
rebalance frequency changes. Turnover-based cost (49.6bps round-trip = delivery +
25bps impact proxy). Dawn panel 2021-2026.

| freq | gross% | net% | net Sharpe | annual turnover | cost drag |
|---|---|---|---|---|---|
| monthly | 46.0 | 44.3 | **1.71** | 245% | 1.7% |
| fortnightly | 49.2 | 46.6 | 1.60 | 363% | 2.6% |
| weekly | 60.2 | **56.0** | 1.57 | 532% | 4.2% |

## Verdict: stick with MONTHLY
- Weekly earns higher net RETURN (56% vs 44%) but LOWER Sharpe (1.57 vs 1.71) —
  the extra return is more-than-proportionally more risk.
- Weekly cost drag 2.5x monthly; churns the book 5.3x/yr -> highly sensitive to
  the impact-cost assumption (which is a guess, no volume data). Monthly is robust.
- The weekly advantage is a 2021-26 momentum-BULL artifact; fast rebalancing gets
  whipsawed in choppy/mean-reverting regimes. Sharpe (regime-robust metric) favours
  monthly.
- Revisit weekly ONLY with real ADV/impact data AND cross-regime confirmation.
