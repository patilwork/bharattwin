# Survivorship Bias Analysis — the #1 caveat, resolved for momentum

## The problem, quantified
- Dawn panel = "stocks that still trade today": of 1,114 fundamental names, only
  ~31 ever go "dead", and most of those are ETF/rights junk from one ingest date.
  Essentially ZERO genuine delistings captured.
- **Morningstar confirms ~2,784 obsolete (delisted/merged/liquidated) Indian
  equities exist** (screener: Status=Obsolete, Exchange Country=India). These are
  the missing names — overwhelmingly penny collapses (last price ₹0.1–5) and
  famous bankruptcies (ABG Shipyard, Amtek Auto, Adhunik Metaliks).
- So the backtest universe is structurally survivorship-biased. The missing names
  are LOW-momentum (falling to zero) and often CHEAP (value traps).

## Stress test (scripts/survivorship_stress.py)
Inject synthetic about-to-delist names each month (cheap + falling, −70% terminal)
at various delisting rates; measure long-only top-quintile drag.

| strategy | base ann% | drag @1%/yr | drag @3%/yr | drag @5%/yr |
|---|---|---|---|---|
| **momentum** | 29.5 | 0.0 | −0.1 | −0.1 |
| value | 34.8 | −9.4 | −23.6 | −41.4 |
| composite | 34.1 | −2.3 | −6.3 | −10.6 |

## Conclusion — this changes the strategy
- **Momentum long-only is survivorship-robust.** The biggest remaining caveat does
  NOT threaten it: we never hold names collapsing toward zero (they're low-momentum,
  bottom quintile). This upgrades momentum from "promising" to "robust".
- **Value's edge is fragile** — a realistic 3%/yr delisting rate more than halves it;
  5%/yr makes it negative. Combined with value_ey's observed decay (negative 2026 IC),
  value should be DE-EMPHASISED, not treated as co-equal.
- **Recommendation: tilt the composite toward momentum** (or run momentum-primary
  with value as a small diversifier), and start the live OOS track with that weighting.

## Caveat on the caveat
- Momentum's protection assumes collapses are GRADUAL (visible as falling momentum).
  A sudden fraud gap (−50% overnight before momentum reflects it) could still hit a
  held name — but most delistings are slow declines.
- The stress test is a BOUND (synthetic injection), informed by Morningstar's real
  delisted profile, not a full reconstruction (which would need price history for
  2,784 names). A full fix = ingest delisted price paths from Morningstar (future).
