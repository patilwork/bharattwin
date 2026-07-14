#!/usr/bin/env python3
"""
TIER 1.1 — Momentum specification + Frog-in-the-Pan path quality.

Our validated edge uses plain 12-1 momentum (return t-252..t-21). The canon
prefers two refinements; this tests whether either beats our spec on the SAME
survivorship-aware Dawn panel (2017-2026), so we only adopt what actually helps:

  A. 12-1 vs 12-2 lookback. 12-1 skips the most-recent month (reversal noise).
     12-2 skips two months — some evidence the extra skip is cleaner in markets
     with slower information diffusion. We test both head-to-head.

  B. Frog-in-the-Pan (Da, Gurun & Warachka 2014). Information Discreteness
     ID = sign(PRET) * (%neg_days - %pos_days) over the formation window.
       - CONTINUOUS info (many small same-sign moves) -> low/negative ID
         -> under-reaction is stronger -> momentum continues harder.
       - DISCRETE info (few big jumps) -> high ID -> weaker continuation.
     Test: within momentum, does tilting toward low-ID (continuous) names lift
     IC and the winner-quintile forward return? We measure (i) a double sort
     momentum x low-ID, and (ii) a blended score momentum_z + (-ID)_z.

Everything is survivorship-aware (liveness filter, delist-mid-hold realises the
loss to last traded price) and turnover-costed, matching the core engine.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/tier1_momentum_spec.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from xsection_montecarlo import load
from src.costs import round_trip_cost

MIN_PRICE, MIN_NAMES, NQ = 10.0, 30, 5
FWD_WINSOR = (-0.40, 0.80)
COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025
LIVE_DAYS = 15
START = "2017-01-31"


def _z(s: pd.Series) -> pd.Series:
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def momentum(win_p: pd.DataFrame, live, skip: int) -> pd.Series:
    """Formation return from t-252 to t-`skip` (skip = 21 for 12-1, 42 for 12-2)."""
    seg = win_p.iloc[-252:-skip]
    return seg.apply(
        lambda c: c.dropna().iloc[-1] / c.dropna().iloc[0] - 1
        if c.dropna().shape[0] > 200 else np.nan
    ).reindex(live)


def info_discreteness(win_p: pd.DataFrame, live, skip: int) -> pd.Series:
    """FIP ID = sign(PRET) * (%neg - %pos) of daily returns over the formation window.
    Low/negative ID = continuous information = stronger expected continuation."""
    seg = win_p.iloc[-252:-skip]
    dret = seg.pct_change()

    def _id(c: pd.Series):
        r = c.dropna()
        if r.shape[0] < 100 or r.iloc[0] == 0:
            return np.nan
        pret = r.iloc[-1] / r.iloc[0] - 1
        pos = (r > 0).mean()
        neg = (r < 0).mean()
        return np.sign(pret) * (neg - pos)

    return dret.apply(_id).reindex(live)


def run(px: pd.DataFrame):
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    pxm = px.ffill().reindex(px.ffill().index.union(rebal)).ffill().reindex(rebal)

    specs = ["mom_12_1", "mom_12_2", "fip_blend_12_1"]
    ic = {k: [] for k in specs}
    ret = {k: [] for k in specs}
    # extra: within top-momentum quintile, split forward return by ID tercile
    fip_hi, fip_lo = [], []   # discrete (high ID) vs continuous (low ID) winner returns

    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        winlive = px.loc[t - pd.Timedelta(days=LIVE_DAYS):t]
        live = winlive.columns[winlive.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        live = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1].reindex(live) / price_t.reindex(live) - 1.0).clip(*FWD_WINSOR)

        win_p = px.loc[:t]
        m121 = momentum(win_p, live, 21)
        m122 = momentum(win_p, live, 42)
        idv = info_discreteness(win_p, live, 21)

        sig = {
            "mom_12_1": m121,
            "mom_12_2": m122,
            # blend: standardised momentum + standardised (-ID); low ID adds score
            "fip_blend_12_1": pd.concat([_z(m121), _z(-idv)], axis=1).mean(axis=1, skipna=True),
        }
        for k, s in sig.items():
            d = pd.concat([s.rename("s"), fwd.rename("f")], axis=1).dropna()
            if len(d) < MIN_NAMES:
                continue
            ic[k].append(d["s"].rank().corr(d["f"].rank()))
            q = pd.qcut(d["s"].rank(method="first"), NQ, labels=False)
            ret[k].append(d["f"][q == NQ - 1].mean() - COST)

        # FIP interaction: hold the top-momentum quintile, split by ID within it
        dm = pd.concat([m121.rename("m"), idv.rename("id"), fwd.rename("f")], axis=1).dropna()
        if len(dm) >= MIN_NAMES:
            qq = pd.qcut(dm["m"].rank(method="first"), NQ, labels=False)
            winners = dm[qq == NQ - 1]
            if len(winners) >= 8:
                idr = winners["id"].rank(method="first")
                lo = winners["f"][idr <= idr.median()].mean()   # low ID = continuous
                hi = winners["f"][idr > idr.median()].mean()     # high ID = discrete
                fip_lo.append(lo - COST)
                fip_hi.append(hi - COST)

    return rebal, ic, ret, pd.Series(fip_lo), pd.Series(fip_hi)


def stats(r: pd.Series) -> tuple[float, float]:
    r = r.dropna()
    if len(r) < 6 or r.std() == 0:
        return np.nan, np.nan
    ann = ((1 + r).prod() ** (12 / len(r)) - 1) * 100
    sharpe = r.mean() / r.std() * np.sqrt(12)
    return ann, sharpe


def ic_t(icl) -> tuple[float, float]:
    s = pd.Series(icl).dropna()
    if len(s) < 6 or s.std() == 0:
        return np.nan, np.nan
    return s.mean(), s.mean() / (s.std() / np.sqrt(len(s)))


def main():
    print("Loading Dawn prices (2016-2026, split/bonus adjusted) ...")
    px, _ = load()
    rebal, ic, ret, fip_lo, fip_hi = run(px)

    print("\n" + "=" * 78)
    print(f"TIER 1.1  MOMENTUM SPEC + FROG-IN-THE-PAN  —  {rebal[0].date()}→{rebal[-1].date()} "
          f"({len(rebal)} rebalances)")
    print("survivorship-aware, delivery+impact cost, top-quintile long-only")
    print("=" * 78)
    print(f"{'spec':<18}{'n':>4}{'meanIC':>9}{'IC_t':>7}{'ann%':>8}{'Sharpe':>8}")
    print("-" * 78)
    for k in ["mom_12_1", "mom_12_2", "fip_blend_12_1"]:
        m, t = ic_t(ic[k])
        a, sh = stats(pd.Series(ret[k]))
        print(f"{k:<18}{len(pd.Series(ic[k]).dropna()):>4}{m:>9.4f}{t:>7.2f}{a:>8.1f}{sh:>8.2f}")

    print("\nFIP interaction — within the top-momentum quintile, split by info-discreteness:")
    la, lsh = stats(fip_lo)
    ha, hsh = stats(fip_hi)
    print(f"  continuous winners (low ID) : ann {la:+.1f}%  Sharpe {lsh:.2f}")
    print(f"  discrete   winners (high ID): ann {ha:+.1f}%  Sharpe {hsh:.2f}")
    print(f"  FIP spread (continuous - discrete): {la - ha:+.1f}pp/yr")

    print("-" * 78)
    base_m, base_t = ic_t(ic["mom_12_1"])
    print("READ:")
    print(f"  * 12-2 beats 12-1 only if its IC_t and Sharpe are both higher (baseline "
          f"12-1 IC_t={base_t:.2f}).")
    print("  * FIP helps if the blend lifts IC/Sharpe AND continuous winners beat discrete")
    print("    ones by a positive spread. If the spread is ~0, path quality adds nothing here.")
    print("  * Adopt into the core composite ONLY the refinement that clears both bars.")


if __name__ == "__main__":
    main()
