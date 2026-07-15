# Market-neutral (index-hedged) variant — results

> `scripts/market_neutral_test.py`, run on the Dawn panel, 2021-07 → 2026-06 (60
> months, the window where the value leg exists). Returns are IN-SAMPLE and this is
> a bull-heavy window — treat the *relative* comparison (Sharpe, beta, drawdown),
> not the absolute levels, as the signal.

| Variant | ann% | vol% | Sharpe | maxDD% | resid β |
|---|---|---|---|---|---|
| **1. long-only composite** (what we run) | 44.4 | 23.8 | **1.68** | -19.0 | **1.13** |
| **2. hedged vs NIFTY 50** (tradeable) | 31.9 | 19.9 | 1.51 | -12.1 | **-0.14** |
| 3. excess vs equal-wt universe (ideal, not tradeable) | 17.9 | 11.1 | 1.55 | -5.1 | 0.08 |

## What it says
- **Hedging works mechanically.** Shorting Nifty futures at the book's trailing beta
  collapses residual market beta from **1.13 → ~0** and cuts the drawdown -19% → -12%.
  The book *is* mid/small-cap tilted (β>1 to large-cap Nifty).
- **But it's not a free Sharpe upgrade with the only tradeable hedge.** Nifty-hedged
  Sharpe slightly *falls* (1.68 → 1.51). Two reasons: (a) **cap-tilt basis** — our
  book is mid/small, Nifty 50 is large-cap, so Nifty is an imperfect hedge and leaves
  basis risk; (b) you give up the mid/small-cap **beta premium**, which was strong in
  2021-26, plus a small futures-roll drag.
- **The pure cross-sectional alpha is real and smooth** (row 3: Sharpe 1.55, maxDD
  only -5%, β 0.08) — but it's hedged against the *equal-weight universe*, which has
  no liquid future. It bounds what a perfectly cap-matched hedge could achieve and
  shows the alpha isn't a beta artifact.

## The honest conclusion
Beta-hedging **changes the product**, it doesn't strictly improve it:
- Long-only = beta + alpha → higher return, bigger crash risk (here -19%, but -38%
  over the full 9yr incl. 2018/2020).
- Nifty-hedged = market-neutral → lower return, smaller drawdown, ~zero beta, but the
  large-vs-mid basis means Sharpe doesn't rise here.

**Two caveats that matter:**
1. This window (2021-26) is bull-heavy, so long-only's -19% maxDD *understates* its real
   crash risk. In a genuine bear the hedge's drawdown protection would look far better
   than it does here — the hedge is insurance you're not seeing the payoff for in an
   up-market sample.
2. For *our* book, the **Tier-1.2 trend overlay is a better downside tool than a
   continuous Nifty short**: it de-risks to cash only in downtrends (keeping the
   mid/small beta premium in up-markets), costs nothing to carry, and has no cap-tilt
   basis. The overlay lifted Calmar 0.75 → 1.19 without giving up return.

## Bottom line
The market-neutral variant is genuinely *executable* (Nifty futures) and does isolate
alpha + kill beta — worth it **only** if the goal is a low-drawdown, market-neutral
pure-alpha sleeve (a Sharpe/absolute-return product for capital that must avoid
equity drawdowns). For maximising risk-adjusted return of *this* book, long-only +
the trend overlay dominates the continuous hedge. Leverage can't help either (see
expectancy_sim); an uncorrelated 2nd edge is the only thing that raises Sharpe for free.
