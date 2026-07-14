# TIER 1 Upgrades — Results (free, price-based refinements)

> Run 2026-07-14 on the survivorship-aware Dawn panel (2017-2026, 113 monthly
> rebalances, split/bonus adjusted, delivery+impact costed). Four canon-preferred
> refinements tested head-to-head against the validated core spec. Scripts:
> `scripts/tier1_momentum_spec.py`, `tier1_trend_overlay.py`, `tier1_purged_cv.py`,
> `tier1_regime_lowvol.py`.

## Bottom line — what to adopt
| # | Refinement | Verdict | Action |
|---|---|---|---|
| 1.1 | 12-2 momentum / Frog-in-the-Pan | **REJECT** — 12-1 already optimal, FIP adds nothing | keep 12-1 |
| 1.2 | Trend / tail-risk overlay | **ADOPT** — cuts maxDD -38%→-19%, Sharpe 1.19→1.40 | add market-MA switch |
| 1.3 | Combinatorial Purged CV | **PASS** — 100% OOS paths Sharpe>0, PBO 29% | edge is path-robust |
| 1.4 | Low-vol/low-beta regime switch | **BENCH** — directional only, not tradeable | leave benched |

Net: the momentum spec was already right; the one real *upgrade* is the **trend
overlay** (a drawdown-cutter, not an alpha-adder). CPCV upgrades our confidence in
the existing edge. Low-vol stays on the shelf.

---

## 1.1 Momentum spec + Frog-in-the-Pan
| spec | n | meanIC | IC_t | ann% | Sharpe |
|---|---|---|---|---|---|
| **mom_12_1 (core)** | 113 | 0.0532 | **4.04** | 28.1 | **1.19** |
| mom_12_2 | 113 | 0.0512 | 3.98 | 26.5 | 1.13 |
| fip_blend_12_1 | 113 | 0.0046 | 0.57 | 18.3 | 0.80 |

FIP path-quality interaction (within top-momentum quintile): continuous-info
(low-ID) winners beat discrete (high-ID) winners by only **+2.6pp/yr** (Sharpe
1.18 vs 1.16).

**Read:** 12-1 wins on both IC_t and Sharpe. The extra skip month (12-2) is
marginally worse. Blending in Frog-in-the-Pan information-discreteness *dilutes*
momentum (IC_t collapses to 0.57). The path-quality tilt is real but tiny — not
worth the extra complexity. **Our existing 12-1 spec is already the canon-optimal
one for this market.** A clean, useful negative result.

## 1.2 Trend / tail-risk overlay  ← the adoptable win
Base = survivorship-aware momentum top-quintile, over the window that contains the
2018 NBFC and 2020 COVID crashes.

| variant | ann% | Sharpe | maxDD% | worst mo% | Calmar |
|---|---|---|---|---|---|
| base (fully invested) | 28.2 | 1.19 | -37.6 | -25.9 | 0.75 |
| **A. market-MA timing (to cash)** | 23.9 | **1.37** | **-22.0** | -11.3 | 1.08 |
| A. market-MA timing (half-in) | 26.5 | 1.37 | -28.9 | -12.9 | 0.92 |
| B. stock own-MA filter | 23.2 | 1.18 | -34.4 | -21.9 | 0.67 |
| **C. market + stock combined** | 23.1 | **1.40** | **-19.4** | **-10.4** | **1.19** |

Market risk-on 69% of months; stock filter keeps 86% of winners on average.

**Read:** the **market-level 200-day MA switch** is the whole story — it cuts max
drawdown roughly **in half** (-38%→-19% combined), halves the worst month
(-26%→-10%), and lifts Sharpe 1.19→1.40 / Calmar 0.75→1.19, for ~5pp/yr of return
given up. The stock own-MA filter alone (B) barely helps. **Adopt the market-MA
timing switch** as the book's de-risk rule (it also composes naturally with the
Tier-1.4 idea of switching a defensive sleeve on when risk-off — but see 1.4).

## 1.3 Combinatorial Purged CV (Lopez de Prado)
N=8 groups, k=2 test → 28 purged paths, 1-month embargo. Menu of 6 price-based
configs.

Full-sample Sharpe: mom_9_1 1.20, **mom_12_1 1.19**, mom_6_1 1.15, mom_12_2 1.15,
reversal 0.70, low_vol 0.55.

**OOS Sharpe distribution (core mom_12_1, 28 paths):** full-sample 1.19 → OOS mean
1.30 (std 0.89), 5th-pctile **0.28**, min **0.02**, **100% of paths Sharpe > 0**.

**PBO (spec selection overfitting):** in-sample winner is always a momentum variant
(mom_9_1 18×, mom_12_1 8×, mom_6_1 2× — never reversal/low-vol). **PBO = 29%** (well
below the 50% coin-flip line).

**Read:** the edge is **path-robust**, not a single-window artifact — every held-out
combination is profitable, and the 5th-percentile OOS Sharpe is still positive. This
is a strictly stronger statement than the earlier single-path permutation test.
PBO 29% means choosing the spec on the backtest still generalises. The momentum
family dominates every split; the weak factors are never selected.

## 1.4 Low-vol / low-beta regime conditioning
Testing the Frazzini-Pedersen "Betting Against Beta" claim: does the defensive
factor's negative full-sample IC flip positive in bear / high-vol regimes?

**LOW_VOL** (Q5-Q1 = defensive minus aggressive):
| regime | n | meanIC | IC_t | LS%/mo |
|---|---|---|---|---|
| ALL | 113 | 0.0226 | 1.63 | -1.25 |
| bull (>MA) | 78 | 0.0233 | 1.44 | -1.51 |
| bear (<MA) | 35 | 0.0211 | 0.78 | **-0.66** |
| low-vol regime | 42 | 0.0257 | 1.19 | -0.94 |
| high-vol regime | 71 | 0.0208 | 1.15 | -1.43 |

**LOW_BETA:** uniformly weak (ALL IC_t 0.81); bear LS -0.65 vs bull -1.38.

**Read:** the FP effect is present only **directionally** — the defensive tilt hurts
about **half as much** in bear markets (LS -1.51%→-0.66% for low-vol; -1.38%→-0.65%
for low-beta) — but the IC never turns clearly positive and never reaches
significance in any regime. Not enough to deploy as a switched-on sleeve. The
honest conclusion: on this ~9yr Indian sample the **trend overlay (1.2) is a better
defensive tool than a low-vol sleeve**. Low-vol/low-beta stays benched. (Corroborates
FP in sign, not in tradeable magnitude — consistent with our earlier corroboration doc.)

---

## Net effect on the strategy
- **Signal unchanged:** momentum 12-1 + value composite stays as-is (1.1 confirmed
  it, 1.3 stress-tested it).
- **New risk rule to wire in:** market equal-weight index vs its 200-day MA →
  de-risk the book to cash when below. Turns a -38% max drawdown into ~-19% for
  ~5pp/yr of return. This is the concrete deliverable to fold into `papertrack` /
  the monthly rebalance and into any future automation kill-switch (Phase B).
- **Confidence upgrade:** CPCV (100% positive OOS paths, PBO 29%) is the strongest
  robustness evidence to date, complementing permutation p<0.001 and DSR 0.82.
- **Benched:** low-vol/low-beta defensive sleeve — revisit only with a longer sample
  that includes a sustained bear market.
