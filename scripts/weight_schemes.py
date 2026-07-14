#!/usr/bin/env python3
"""
Weighting-scheme comparison — "why equal-weight, and does MC-ing the weights help?"

Holds the SAME top-quintile momentum+value names each month, varies only HOW they
are weighted:
  equal      1/N
  inv_vol    proportional to 1/trailing-vol (risk-parity-ish; no return estimate)
  signal     proportional to composite conviction (softmax of z)
  random-MC  a distribution of random (Dirichlet) weightings -> shows whether
             equal-weight is lucky or typical, and whether ANY fixed rule reliably
             beats it (spoiler: picking the best random one is look-ahead).

Reuses the validated loaders from xsection_montecarlo. Net of cost. Seed 11.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/weight_schemes.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from xsection_montecarlo import load, pit, z
from src.costs import round_trip_cost

REPORT_LAG_DAYS, MIN_PRICE, MIN_NAMES, NQ = 90, 10.0, 30, 5
FWD_WINSOR = (-0.40, 0.80)
COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025
N_RANDOM = 2000
RNG = np.random.default_rng(11)


def collect_topq():
    """Per rebalance, the top-quintile names' (fwd return, trailing vol, composite z)."""
    px, f = load()
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp("2021-06-30")) & (m <= px.index.max())]
    pxm = px.reindex(px.index.union(rebal)).ffill().reindex(rebal)
    dret = px.pct_change()
    bvps, eps = pit(f, "bvps", rebal), pit(f, "ttm_eps", rebal)

    months = []
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        price_t = pxm.loc[t]
        liquid = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1] / pxm.loc[t] - 1.0).clip(*FWD_WINSOR).reindex(liquid)
        win = px.loc[:t]
        mom = win.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1 if c.dropna().shape[0] > 200 else np.nan) if len(win) > 260 else pd.Series(dtype=float)
        vol = dret.loc[:t].iloc[-120:].std()
        comp = pd.concat([z(mom.reindex(liquid)),
                          z((eps.loc[t]/price_t).reindex(liquid)),
                          z((-(price_t/bvps.loc[t])).reindex(liquid))], axis=1).mean(axis=1, skipna=True)
        d = pd.concat([comp.rename("z"), fwd.rename("fwd"), vol.reindex(liquid).rename("vol")], axis=1).dropna()
        if len(d) < MIN_NAMES:
            continue
        q = pd.qcut(d["z"].rank(method="first"), NQ, labels=False)
        top = d[q == NQ - 1]
        months.append((top["fwd"].values, top["vol"].values, top["z"].values))
    return months


def series_for(months, wfun):
    r = []
    for fwd, vol, zsc in months:
        w = wfun(vol, zsc)
        w = w / w.sum()
        r.append(float(w @ fwd) - COST)
    return np.array(r)


def sharpe(r):
    return r.mean() / r.std() * np.sqrt(12) if r.std() else np.nan


def ann(r):
    return ((1 + r).prod() ** (12 / len(r)) - 1) * 100


def main():
    print("Collecting top-quintile members per month from Dawn ...")
    months = collect_topq()
    n_avg = np.mean([len(m[0]) for m in months])
    print(f"{len(months)} rebalances, avg {n_avg:.0f} names/quintile\n")

    schemes = {
        "equal": lambda vol, zsc: np.ones_like(vol),
        "inv_vol": lambda vol, zsc: 1.0 / np.where(vol > 0, vol, np.nan),
        "signal": lambda vol, zsc: np.exp(zsc - zsc.max()),   # softmax tilt to conviction
    }
    print(f"{'scheme':<12}{'ann%':>8}{'Sharpe':>8}")
    print("-" * 30)
    fixed = {}
    for name, fn in schemes.items():
        r = series_for(months, fn)
        fixed[name] = sharpe(r)
        print(f"{name:<12}{ann(r):>8.1f}{sharpe(r):>8.2f}")
    print("-" * 30)

    # random-weight Monte Carlo: Dirichlet weights each month, N_RANDOM strategies
    rnd_sh = []
    for _ in range(N_RANDOM):
        r = []
        for fwd, vol, zsc in months:
            w = RNG.dirichlet(np.ones(len(fwd)))
            r.append(float(w @ fwd) - COST)
        rnd_sh.append(sharpe(np.array(r)))
    rnd_sh = np.array(rnd_sh)
    eq = fixed["equal"]
    pct_eq = (rnd_sh < eq).mean() * 100
    print(f"\nRANDOM-WEIGHT MONTE CARLO ({N_RANDOM} random weightings):")
    print(f"  Sharpe distribution: p5={np.percentile(rnd_sh,5):.2f}  median={np.median(rnd_sh):.2f}  p95={np.percentile(rnd_sh,95):.2f}")
    print(f"  equal-weight Sharpe {eq:.2f} sits at the {pct_eq:.0f}th percentile of random weightings")
    print(f"  inv_vol {fixed['inv_vol']:.2f} | signal {fixed['signal']:.2f}")
    print("-" * 60)
    print("READ: if equal-weight sits mid-pack and the random spread is narrow, the edge")
    print("is in the STOCK SELECTION, not the weighting — so add no weighting parameters")
    print("to overfit. inv_vol/signal only matter if they beat EW by more than the random")
    print("spread. You cannot harvest the best random weighting: that is look-ahead.")


if __name__ == "__main__":
    main()
