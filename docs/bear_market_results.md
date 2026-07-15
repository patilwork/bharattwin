# Bear-market stress test

> `scripts/bear_market_analysis.py`. The momentum book over 2017-2026 (the only window
> with real bears: the 2018 NBFC/small-cap crash and the 2020 COVID crash). CORE =
> fully-invested momentum top-quintile; +OVERLAY = Tier-1.2 trend de-risk to cash below
> the 200d MA; BENCHMARK = equal-weight universe.

## Drawdown & worst month
| | CORE | +OVERLAY | BENCHMARK |
|---|---|---|---|
| full-period max drawdown | -30.6% | **-19.7%** | -48.6% |
| worst single month | -25.5% | -10.9% | -29.1% |

Two things stand out: (1) even fully-invested CORE momentum draws down LESS than the
market (-30.6% vs -48.6%) — momentum holds winners and rotates out of losers; (2) the
overlay cuts it further to -19.7%.

## Crash episodes (cumulative through the window)
| episode | CORE | +OVERLAY | BENCH |
|---|---|---|---|
| 2018 NBFC / small-cap crash | -12.9% | **-14.9%** | -17.5% |
| 2020 COVID crash | -18.1% | **-3.6%** | -25.8% |
| 2020 — the -29% month (Mar) | -25.5% | **0.0%** | -29.1% |

## Worst 6 market months — did the overlay protect?
In **5 of the 6 worst months the overlay was in CASH (0.0%)** while the market fell
8-29%. Only Feb-2022 caught it risk-on (-8%). Sharp crashes → the overlay is out.

## The honest read
- **Sharp crash (COVID-style): the overlay protects powerfully.** March 2020 it sat in
  cash (0%) while the market fell 29% and CORE fell 25%. That's the seatbelt working.
- **Choppy grinding decline (2018-style): the overlay WHIPSAWS.** In 2018 the overlay
  actually lost MORE than CORE (-14.9% vs -12.9%) — it sold low, the market bounced, it
  rebought higher, repeat. This is the classic trend-following weakness.
- **CORE momentum is naturally defensive** vs the market (holds winners), but it is
  still long-only equity (~beta 1) — it ALWAYS bleeds in a bear, just less than the index.

## The deepest UNTESTED risk
Our sample has a V-shaped crash (2020) and a choppy decline (2018) — but **no prolonged
grinding bear** (like US 2000-2003 or 2008). That is the scenario where momentum crashes
hardest (violent loser-rallies) AND the trend overlay whipsaws worst. It is the single
biggest unknown, and no amount of this-sample backtesting can close it — only living
through one, or a much longer/foreign-market history, will.

## Bottom line
Expect ~-20% to -30% drawdowns as normal, with the overlay turning V-crashes into shallow
dips but offering little (or negative) help in choppy grinds. Plan position size for a
-40%+ event that hasn't shown up in the data yet. This is why Phase B (the -25% kill-
switch) exists, and why real capital waits for the live track.
