# Cross-Sectional Factor Backtest — First Edge Evidence

Dawn panel (free primary data), 2021-06 → 2026-06, 61 monthly rebalances,
net of 24.6bps delivery round-trip, PIT-lagged 90d, min price ₹10, quintiles.

| Factor | mean IC | IC t | LS ann% (net) | LS Sharpe | Long-only ann% |
|---|---|---|---|---|---|
| momentum (12-1m) | 0.048 | 3.17 | +17.2 | 1.32 | +8.5 |
| value_ey (E/P)   | 0.049 | 3.67 | +14.1 | 1.08 | +12.1 |
| value_pb (1/PB)  | 0.044 | 2.49 | +17.7 | 1.27 | +10.7 |
| low_vol          | 0.007 | 0.37 | -28.8 | -1.16 | -13.0 |
| size             | 0.005 | 0.29 | +12.5 | 0.90 | +9.4 |

**Momentum + value are statistically detectable (IC t > 2).** low_vol/size are not.

## PROMISING, NOT PROVEN — the caveats that could inflate this
1. **Survivorship bias (biggest):** delisted names largely absent → upward bias,
   worst for value (cheap names that went to zero are missing).
2. **Unadjusted prices:** only `close`, no split/dividend adjustment; winsorization
   is a crude guard (adds noise to momentum — arguably conservative — but messy).
3. **Single-regime window:** 2021-2026 was largely a mid/small-cap bull; momentum
   and value both flatter in trending bulls. Low power for regime robustness.
4. **No deflated Sharpe / multiple-testing correction yet** (5 factors tested).
5. **Monthly-IC autocorrelation** may inflate the t-stats.
6. **Liquidity/impact not modelled** — only flat delivery bps, not ADV-scaled impact;
   the biggest premia live in illiquid names where impact bites.
7. **Long-short is a paper construct in India** (single-stock shorting is intraday-only);
   the tradeable version is long-only (still positive: momentum +8.5%, value +11-12%).

## To turn promising → proven
- corporate-action adjust prices (dawn.corporate_actions), add delisted universe
- deflated Sharpe + purged/embargoed CV + PBO
- ADV-scaled impact costs; long-only composite (momentum+value); turnover control
- extend history; regime-split performance
