#!/usr/bin/env python3
"""
Rebalance-frequency test — "should we rebalance weekly instead of monthly?"

Same composite momentum+value top-quintile, long-only, equal-weight. Only the
REBALANCE FREQUENCY changes: monthly / fortnightly / weekly. Costs are
TURNOVER-BASED (the whole point — faster rebalancing trades more), so this
shows whether the extra trading pays for itself or just feeds the cost engine.

A 12-1 month momentum signal changes slowly, so the hypothesis is that weekly
rebalancing raises annual turnover and cost without adding signal. We measure it.

Reuses loaders from xsection_montecarlo. Seed-free (deterministic).

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/rebalance_frequency.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from xsection_montecarlo import load, pit, z
from src.costs import round_trip_cost

MIN_PRICE, MIN_NAMES, NQ = 10.0, 30, 5
FWD_WINSOR = (-0.40, 0.80)
RT_COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025  # per unit turnover, round trip

FREQS = [("monthly", "ME", 12), ("fortnightly", "2W-FRI", 26), ("weekly", "W-FRI", 52)]


def run_freq(px, f, rule, ppy):
    idx = px.resample(rule).last().index
    rebal = idx[(idx >= pd.Timestamp("2021-06-30")) & (idx <= px.index.max())]
    pxm = px.reindex(px.index.union(rebal)).ffill().reindex(rebal)
    dret = px.pct_change()
    bvps, eps = pit(f, "bvps", rebal), pit(f, "ttm_eps", rebal)

    gross, net, turns = [], [], []
    prev = set()
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        price_t = pxm.loc[t]
        liquid = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1] / pxm.loc[t] - 1.0).clip(*FWD_WINSOR).reindex(liquid)
        win = px.loc[:t]
        mom = win.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1 if c.dropna().shape[0] > 200 else np.nan) if len(win) > 260 else pd.Series(dtype=float)
        comp = pd.concat([z(mom.reindex(liquid)),
                          z((eps.loc[t]/price_t).reindex(liquid)),
                          z((-(price_t/bvps.loc[t])).reindex(liquid))], axis=1).mean(axis=1, skipna=True)
        d = pd.concat([comp.rename("z"), fwd.rename("fwd")], axis=1).dropna()
        if len(d) < MIN_NAMES:
            continue
        q = pd.qcut(d["z"].rank(method="first"), NQ, labels=False)
        hold = set(d.index[q == NQ - 1])
        g = d["fwd"][d.index.isin(hold)].mean()
        turnover = 1.0 - (len(hold & prev) / len(hold)) if prev else 1.0
        gross.append(g)
        net.append(g - turnover * RT_COST)
        turns.append(turnover)
        prev = hold

    gross, net, turns = map(np.array, (gross, net, turns))
    def annret(r):
        return ((1 + r).prod() ** (ppy / len(r)) - 1) * 100
    def sharpe(r):
        return r.mean() / r.std() * np.sqrt(ppy) if r.std() else np.nan
    return {
        "gross_ann": annret(gross), "net_ann": annret(net),
        "gross_sh": sharpe(gross), "net_sh": sharpe(net),
        "avg_turn": turns.mean() * 100, "ann_turn": turns.mean() * ppy * 100,
        "cost_drag": annret(gross) - annret(net), "n": len(net),
    }


def main():
    print("Loading Dawn panel ...")
    px, f = load()
    print(f"per-round-trip cost = {RT_COST*1e4:.1f}bps (delivery + impact proxy)\n")
    print(f"{'freq':<12}{'gross%':>8}{'net%':>8}{'net_Sh':>8}{'avgTurn':>9}{'annTurn':>9}{'costDrag':>9}")
    print("-" * 64)
    for name, rule, ppy in FREQS:
        s = run_freq(px, f, rule, ppy)
        print(f"{name:<12}{s['gross_ann']:>8.1f}{s['net_ann']:>8.1f}{s['net_sh']:>8.2f}"
              f"{s['avg_turn']:>8.0f}%{s['ann_turn']:>8.0f}%{s['cost_drag']:>8.1f}%")
    print("-" * 64)
    print("gross% = before cost. net% = after turnover cost. costDrag = gross-net (annual).")
    print("annTurn = annualised turnover (how much of the book you churn per year).")
    print("READ: if net% and net_Sh don't RISE going monthly->weekly, the extra trading is")
    print("pure cost. A 12-1 month signal is slow; weekly should mostly add turnover.")


if __name__ == "__main__":
    main()
