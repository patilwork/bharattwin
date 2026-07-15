#!/usr/bin/env python3
"""
Does shifting focus to mid/small caps add edge?

India-specific prior (Agarwalla-Jacob-Varma, in our KB): SMB ≈ 0 — there is NO
size *premium* in India. So the question is NOT "do small caps return more" (they
don't, risk-adjusted) but "is the momentum+value SIGNAL more EFFICACIOUS in smaller
caps?" — i.e. is the factor IC higher where the market is less arbitraged? The
smart-beta test hinted our edge comes from the small-cap pond; this measures it
directly, net of the higher cost small caps carry.

Method: at each rebalance split the liquid (₹2000cr+) universe into cap terciles
(Large/Mid/Small by market cap). Within each, measure the mom+value composite's
rank-IC and long-only top-quintile Sharpe — GROSS and NET of a cap-escalating cost
(small caps trade dearer). Survivorship-aware.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/cap_tilt_analysis.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from xsection_montecarlo import load, pit
from src.costs import round_trip_cost

MIN_PRICE, MIN_NAMES, NQ = 10.0, 20, 5
FWD_WINSOR = (-0.40, 0.80)
BASE_COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4
# cap-escalating round-trip cost (delivery + impact proxy): small caps trade dearer
COST = {"Large": BASE_COST + 0.002, "Mid": BASE_COST + 0.006, "Small": BASE_COST + 0.015}
START = "2021-06-30"       # value leg needs fundamentals (2021+)


def _z(s):
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def run(px, f):
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    bvps, eps, shr = (pit(f, c, rebal) for c in ["bvps", "ttm_eps", "shares_outstanding"])

    buckets = ["Large", "Mid", "Small"]
    ic = {b: [] for b in buckets}
    ret = {b: [] for b in buckets}
    prev = {b: set() for b in buckets}
    mcap_ranges = {b: [] for b in buckets}

    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        win = px.loc[t - pd.Timedelta(days=15):t]
        live = win.columns[win.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        live = price_t[price_t >= MIN_PRICE].index
        mcap = (price_t.reindex(live) * shr.loc[t].reindex(live) / 1e7).dropna()   # ₹ cr
        liquid = mcap[mcap >= 2000].index
        if len(liquid) < 3 * MIN_NAMES:
            continue

        seg = px.loc[:t].iloc[-252:-21]
        mom = seg.apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1
                        if c.dropna().shape[0] > 200 else np.nan).reindex(liquid)
        ey = (eps.loc[t] / price_t).reindex(liquid)
        pb = -(price_t / bvps.loc[t]).reindex(liquid)
        comp_all = pd.concat([_z(mom), _z(ey), _z(pb)], axis=1).mean(axis=1, skipna=True)

        # split liquid names into cap terciles
        mc = mcap.reindex(liquid).dropna()
        q = pd.qcut(mc.rank(method="first"), 3, labels=["Small", "Mid", "Large"])
        for b in buckets:
            names = mc.index[q == b]
            fwd = (pxm.loc[t1].reindex(names) / price_t.reindex(names) - 1.0).clip(*FWD_WINSOR)
            comp = comp_all.reindex(names)
            d = pd.concat([comp.rename("s"), fwd.rename("f")], axis=1).dropna()
            if len(d) < MIN_NAMES:
                continue
            ic[b].append(d["s"].rank().corr(d["f"].rank()))
            k = max(len(d) // NQ, 1)
            hold = set(d["s"].sort_values(ascending=False).head(k).index)
            turn = 1.0 - len(hold & prev[b]) / len(hold) if prev[b] else 1.0
            ret[b].append(d["f"].reindex(list(hold)).mean() - turn * COST[b])
            prev[b] = hold
            mcap_ranges[b].append((mc.reindex(names).min(), mc.reindex(names).max()))

    return ic, ret, mcap_ranges


def stats(r):
    r = pd.Series(r).dropna()
    if len(r) < 6:
        return np.nan, np.nan, np.nan
    ann = ((1 + r).prod() ** (12 / len(r)) - 1) * 100
    sh = r.mean() / r.std() * np.sqrt(12) if r.std() else np.nan
    dd = ((1 + r).cumprod() / (1 + r).cumprod().cummax() - 1).min() * 100
    return ann, sh, dd


def ic_t(v):
    s = pd.Series(v).dropna()
    return s.mean(), (s.mean() / (s.std() / np.sqrt(len(s))) if s.std() else np.nan)


def main():
    print("Loading Dawn ...")
    px, f = load()
    ic, ret, mcr = run(px, f)

    print("\n" + "=" * 84)
    print("CAP-TILT ANALYSIS — where does the mom+value edge live? (2021+, ₹2000cr+ universe)")
    print("India prior: SMB≈0 (no size premium) — so we test factor EFFICACY (IC) by cap, net cost")
    print("=" * 84)
    print(f"{'bucket':<8}{'~mcap band (₹cr)':>22}{'meanIC':>9}{'IC_t':>7}{'net ann%':>10}{'Sharpe':>8}{'maxDD%':>8}")
    print("-" * 84)
    for b in ["Large", "Mid", "Small"]:
        m, tt = ic_t(ic[b])
        ann, sh, dd = stats(ret[b])
        bands = [x for pair in mcr[b] for x in pair]
        lo = np.percentile([p[0] for p in mcr[b]], 50) if mcr[b] else np.nan
        hi = np.percentile([p[1] for p in mcr[b]], 50) if mcr[b] else np.nan
        band = f"{lo:,.0f}-{hi:,.0f}"
        print(f"{b:<8}{band:>22}{m:>9.4f}{tt:>7.2f}{ann:>10.1f}{sh:>8.2f}{dd:>8.1f}")
    print("-" * 84)
    print("cost applied (round-trip): Large {:.0f}bps, Mid {:.0f}bps, Small {:.0f}bps"
          .format(COST['Large']*1e4, COST['Mid']*1e4, COST['Small']*1e4))
    print("READ: if IC_t and net Sharpe RISE from Large→Small, the edge is a factor-EFFICACY")
    print("effect (signal works better where the market is less arbitraged) — a real reason to")
    print("tilt smaller, capacity permitting. If the small-cap net Sharpe collapses once the")
    print("higher cost bites, the 'edge' was illiquidity you can't actually harvest.")


if __name__ == "__main__":
    main()
