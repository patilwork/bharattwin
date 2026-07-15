# Momentum formation-window sweep — is 12-1 right?

> `scripts/momentum_lookback_sweep.py`. "F-S momentum" = P(t-S)/P(t-F)-1. India Dawn
> panel, 2017-2026, monthly, survivorship-aware. Long-only top-quintile.

## Formation length (skip = 1 month)
| spec | mean IC | IC_t | LO Sharpe |
|---|---|---|---|
| 2-1 | 0.009 | 0.83 | 0.74 |
| 3-1 | 0.023 | 2.08 | 0.94 |
| 6-1 | 0.036 | 2.78 | 0.99 |
| 9-1 | 0.043 | 2.96 | 1.11 |
| **12-1** (our spec) | 0.050 | **3.29** | 1.15 |
| 18-1 | 0.028 | 1.73 | 1.04 |
| 24-1 | 0.022 | 1.45 | 1.04 |

A clear hump: weak at 1-2 months (short-term reversal zone), momentum builds 3→12,
**peaks around 12**, decays 18-24. 12-1 sits at/near the top.

## Skip effect (formation = 12 months)
| spec | IC_t | LO Sharpe |
|---|---|---|
| 12-0 (no skip) | 2.51 | 1.19 |
| 12-1 | 3.29 | 1.15 |
| 12-2 | 3.73 | 1.25 |
| 12-3 | 3.70 | 1.24 |

Skipping the most recent 1-2 months helps (dodges 1-month reversal). Here 12-2/12-3
edge 12-1 slightly — but the earlier survivorship-aware daily test had 12-1 ≥ 12-2
(4.04 vs 3.98). So 12-1 vs 12-2 is WITHIN NOISE (methodology-dependent); they're tied.

## The real finding: it's a PLATEAU, not a knife-edge
The exact lookback barely matters — everything from ~6 to ~12 months works well and
similarly. That ROBUSTNESS is itself the good news: the edge is not a single lucky
parameter (which would scream overfitting), it's a broad, stable phenomenon. 12-1 is a
well-chosen, standard point on a flat optimum.

## What NOT to do
Don't chase the single highest number (e.g. switch to 12-2 for +0.4 IC_t). The CPCV/PBO
work already showed spec-selection generalises only because the whole momentum family is
robust — picking the in-sample winner is overfitting. 12-1 stays; a 12-2/12-3 tilt is
defensible but not worth re-forming the live book for.
