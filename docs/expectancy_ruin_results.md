# Expectancy + Ruin Simulator — "Is 50% possible?"

Momentum long-only sleeve, re-centred to a REALISTIC 16%/yr gross (not the ~29%
in-sample), vol 22.7%. Block bootstrap, 5-year paths, 5000 sims, ₹10L, leverage
financing 8.5%/yr. Ruin = equity ever hits 30% of start.

| config | median CAGR | CAGR 5–95% | med maxDD | tail DD | P(5y loss) | **P(RUIN)** |
|---|---|---|---|---|---|---|
| 25 names, 1.0x (base) | **12.0%** | −6..31% | −30% | −52% | 13% | **0.2%** |
| 10 names, 1.0x | 7.5% | −21..40% | −50% | −77% | 32% | 6.1% |
| 25 names, 1.5x | 12.5% | −15..44% | −45% | −72% | 24% | 3.1% |
| 10 names, 1.5x | 1.0% | −21..51% | −70% | −92% | 49% | 29% |
| 10 names, 2.0x | −21% | −21..60% | −84% | −98% | 64% | 52% |
| 5 names, 2.0x | −21% | −21..45% | −97% | −100% | 84% | 80% |

## The verdict on 50%
- **No config has a 50% median.** The lucky 95th-percentile path reaches 44–60%,
  but the *typical* outcome of chasing it is a **negative** median and a coin-flip
  (or worse) chance of ruin.
- **Concentration LOWERS the median** (10 names: 7.5% vs 12%) — volatility drag
  eats compound growth — while pushing P(ruin) from 0.2% → 6%.
- **Leverage barely moves the median** (financing + drag) but explodes the tail:
  25 names 1.5x = same ~12% median, but tail DD −72% and P(ruin) 3%.
- **Stacking both is self-destruction:** 10 names 2.0x has a −21% median and
  52% ruin. You are far more likely to blow up than to hit 50%.

## Why (the math)
Median compound return < arithmetic mean by ~½·vol². Leverage and concentration
both raise vol, so they raise the *spread* (the 95th pctile looks exciting) while
*lowering* the median and raising ruin. You can buy a lottery ticket to 50%; you
cannot buy an expectation of it.

## The only honest lever
Raising return without raising ruin requires **uncorrelated edges**, not more
leverage/concentration on the one edge we have. That's why "fix survivorship,
then find a second uncorrelated signal" beats "crank the risk dials."
