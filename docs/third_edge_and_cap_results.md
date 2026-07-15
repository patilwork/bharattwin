# Third-edge hunt + cap-tilt research

> Scripts: `scripts/third_edge_hunt.py`, `scripts/cap_tilt_analysis.py`. Knowledge
> repo mined (operating guide + 47-resource catalog) to pick candidates. In-sample,
> bull-heavy windows — read the structure, not the absolute levels.

## Q1 — did we find a third uncorrelated edge?

### Backtestable price factors: NO
Tested the genuinely-distinct, price-computable candidates (the low-vol/low-beta/
reversal family was already rejected). Long-short (beta-stripped), 2019-2026:

| signal | LS Sharpe | t | corr→mom | corr→val | verdict |
|---|---|---|---|---|---|
| momentum | 1.20 | 3.27 | — | 0.15 | base |
| value (P/B) | 1.58 | 3.34 | 0.15 | — | base |
| long-term reversal | 0.49 | 1.32 | **-0.18** | 0.29 | uncorrelated but too weak |
| seasonality | -0.24 | -0.65 | 0.42 | 0.20 | loses + correlated |
| anti-lottery (low-MAX) | -0.66 | -1.79 | -0.19 | -0.41 | loses this regime |
| residual momentum | 0.61 | 1.65 | **0.59** | -0.56 | just disguised momentum |

**Blend test** (does adding it to the long-only mom+value book help?): base 2-way
Sharpe **1.50**; +LTR 1.44, +seasonality 1.16, +low-MAX 1.30, +resid-mom 1.50. **None
clears the base.** The candidates with the right correlation profile (LTR) are too
weak; the ones that pay (resid-mom) are correlated momentum. Same story as low-vol:
the uncorrelated defensive factors lose in a bull regime.

### The theoretical third edge is QUALITY — and we can't backtest it
The knowledge repo points unambiguously at **quality/profitability (QMJ family)** —
operating profitability (`operating_profit/total_assets`, Novy-Marx) and low accruals
(`(pat−cfo)/total_assets`, Sloan). The guide: *"QMJ has documented India alpha,"* and
it's structurally orthogonal to value/momentum. Confirmed on the current snapshot
(cross-sectional Spearman, n=982):

| | vs momentum | vs value |
|---|---|---|
| quality composite | **+0.10** (uncorrelated) | **-0.16** (mildly opposite) |
| operating profitability | +0.14 | -0.17 |
| low accruals | -0.07 | -0.14 |

That −0.16 to value is the sweet spot: "cheap" (value) and "good" (quality) offset
each other's failure modes (value traps vs quality bubbles) — the classic
cheap-AND-good complementarity.

**The catch:** our deep fundamentals are ONE annual snapshot (2026-03-31, no history),
so quality can be *formed* but not *backtested* over 2016-2026 — any full-sample run is
look-ahead. So quality is a **candidate we can only prove forward, live** — which is
exactly what the Tier-2 quality machinery + the live **Quality-gated paper book (id=7)**
are for. Verdict: **not a proven third edge; a structurally-valid candidate now on the
live track.** (Note: the current book uses a forensic *gate* — a left-tail filter — not
a profitability *tilt*; a truer test is a mom+value+quality **tilt** book. See below.)

## Q2 — does a mid/small-cap focus add edge? YES

India prior: **SMB ≈ 0** (no size *premium*). So we tested factor *efficacy* by cap —
is the mom+value signal sharper where the market is less arbitraged? Within the
₹2000cr+ universe, split into cap terciles, net of cap-escalating cost:

| bucket | ~mcap band (₹cr) | IC_t | net ann% | Sharpe | maxDD | cost |
|---|---|---|---|---|---|---|
| Large | 26k–1.4M | 2.48 | 25.3 | 1.19 | -22% | 45bps |
| Mid | 8.6k–26k | 3.08 | 21.7 | 0.99 | -24% | 85bps |
| **Small** | 2k–8.6k | **4.11** | **43.1** | **1.49** | -18% | **175bps** |

**The edge strengthens monotonically going down in size** — IC_t 2.48 → 4.11, net
Sharpe 1.19 → 1.49 — and it *survives* a punishing 175bps small-cap round-trip. This
is a factor-**efficacy** effect, not a size premium (consistent with SMB≈0): small
caps don't return more per unit risk on their own, but the momentum+value signal
**discriminates better** there. It empirically confirms the smart-beta finding — our
edge over large-cap factor ETFs comes from the small-cap pond.

**Caveats:** in-sample, bull-heavy (small-caps crash harder in bears — the -18% here
understates it); **capacity-limited** (a small-cap tilt doesn't scale — fine at
personal AUM, not at size); **cost-fragile** (at 175bps it holds; if real slippage is
250bps+ it erodes). So: *keep the broad universe incl. small/mid rather than
restricting to large caps, and a modest small-cap tilt is defensible — eyes open on
capacity, cost, and bear fragility.*

## Bottom line
- **Third edge:** no new backtestable one exists; momentum + value remains the durable
  pair. **Quality is the only structurally-valid candidate** (orthogonal, India-
  documented) — but unprovable historically, so it lives or dies on the forward track.
- **Cap tilt:** a real, actionable edge — the signal is strongest in small/mid caps.
  This is *the* thing that distinguishes our DIY book from buyable large-cap factor ETFs.
- **Actionable next step:** run a mom+value+**quality-tilt** book (4th variant) as the
  live third-edge test, and consider a small-cap-tilted variant — both capacity-scaled.
