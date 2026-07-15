# Smart-beta reality check — DIY vs buy-the-ETF

> `scripts/smart_beta_test.py`, 2021-07 → 2026-06 (60 months). **IN-SAMPLE and
> bull-heavy** — the absolute returns are not forward-realistic; read the *spreads*
> and the *source* of the edge, not the levels.

## The reframe
Our momentum+value composite **is smart beta** — rules-based, long-only, factor-tilted.
So the honest benchmark isn't the plain index; it's the off-the-shelf factor ETF you
could just buy (Nifty Momentum 30, Alpha 50, Alpha Low-Vol 30, Quality 30, Value 20,
multi-factor — all exist in India at ~0.3-0.5% expense, zero effort, tax-efficient, no
key-person risk). If we can't beat those, buy them and skip the pipeline.

## Results
| Series | ann% | vol% | Sharpe | maxDD% |
|---|---|---|---|---|
| **OUR composite** (broad ₹2000cr+, mom+value) | 44.4 | 23.8 | **1.68** | -19.0 |
| large-cap momentum sleeve (ETF proxy) | 20.6 | 21.1 | 1.00 | -27.3 |
| NIFTY50 VALUE 20 (real smart-beta value) | 10.4 | 14.0 | 0.78 | -17.5 |
| NIFTY MIDCAP 150 (mid beta) | 18.0 | 17.0 | 1.06 | -20.3 |
| NIFTY 500 (broad beta) | 12.4 | 14.8 | 0.87 | -17.7 |

**Our composite's excess (alpha) over each buyable comparator:**
| vs | excess/yr | info-ratio |
|---|---|---|
| large-cap momentum ETF proxy | +18.1% | 1.53 |
| NIFTY50 VALUE 20 | +27.7% | 1.47 |
| NIFTY MIDCAP 150 | +20.6% | 1.51 |
| NIFTY 500 | +25.8% | 1.63 |

## What it actually means
- **In-sample, our book beats every buyable smart-beta option by a wide margin** (info
  ratios 1.5+). But this is the same data the strategy was shaped on and a bull-heavy
  window — the 44%/yr and IR 1.5 will shrink hard out-of-sample. Our standing forward
  estimate is ~4-6% alpha, NOT 18%.
- **Where the edge comes from: the small-cap pond.** The ETF proxy is large-cap only;
  our universe reaches down to ₹2000cr. Single-factor large-cap momentum is nothing
  special here (Sharpe 1.00, -27% DD). The excess over it (+18%, IR 1.53) is mostly
  access to the smaller-cap names the ETFs can't/don't hold, plus the multi-factor blend.
- **Single-factor large-cap value is weak** (VALUE 20 Sharpe 0.78) — validating our
  multi-factor + broad-universe choice over any one off-the-shelf single-factor fund.

## Strategic takeaway (the honest fork)
- **Passive investor, or at scale → just buy smart-beta ETFs.** One momentum + one
  value/quality captures the factor premium for ~0.4% with zero effort and no fragility.
  Most of our "alpha vs Nifty" is exactly this premium — buyable by anyone.
- **Our DIY only earns its keep because, at small personal AUM, it can fish the
  small-cap pond the ETFs can't** — plus multi-factor blending and the trend/quality
  overlays. That edge is real but **capacity-constrained** (small-caps don't scale),
  **cost-sensitive** (slippage likely understated in the model), **more fragile**, and
  **unproven live**.
- **Sensible real-world shape:** core = cheap smart-beta ETFs (factor beta); satellite =
  our DIY small-cap sleeve, IF it proves out on the paper track. Don't pay yourself for
  beta you could buy for 0.4%.

Ties to the earlier answers: leverage can't add alpha, shorting isn't executable,
index-hedging is a Sharpe/insurance play — and here, most of the headline return is
factor beta you can buy. The only durable DIY edge is the small-cap-universe access +
overlays, and its real size is unknown until the live track runs.
