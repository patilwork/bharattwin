#!/usr/bin/env python3
"""
Momentum formation-window sweep — is 12-1 the right lookback, or something else?

"F-S momentum" = return from F months ago to S months ago: P(t-S) / P(t-F) - 1.
12-1 is F=12, S=1 (the Jegadeesh-Titman standard: 12-month trend, skip the most
recent month to dodge short-term reversal). This sweeps F and S on the India Dawn
panel (2017-2026, survivorship-aware, monthly) and reports the rank-IC t-stat and
long-only top-quintile Sharpe for each — the momentum "term structure".

Expected shape: F=1 negative (short-term REVERSAL), F=3-12 positive (momentum),
decaying and eventually flipping negative at long horizons (DeBondt-Thaler reversal).

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/momentum_lookback_sweep.py
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


def sweep(px, specs):
    pxm = px.resample("ME").last().ffill()
    live_daily = px.resample("ME").last()          # non-ffilled: liveness (traded that month)
    idx = pxm.index
    start = idx.searchsorted(pd.Timestamp("2017-01-31"))
    out = {s: {"ic": [], "lo": [], "prev": set()} for s in specs}

    for i in range(max(start, 26), len(idx) - 1):
        price_t = pxm.iloc[i]
        live = live_daily.iloc[i].dropna().index               # traded this month
        liquid = price_t.reindex(live)[price_t.reindex(live) >= MIN_PRICE].index
        fwd = (pxm.iloc[i + 1].reindex(liquid) / price_t.reindex(liquid) - 1.0).clip(*FWD_WINSOR)

        for (F, S) in specs:
            if i - F < 0:
                continue
            p_end = pxm.iloc[i - S] if S > 0 else price_t
            mom = (p_end.reindex(liquid) / pxm.iloc[i - F].reindex(liquid) - 1.0)
            d = pd.concat([mom.rename("m"), fwd.rename("f")], axis=1).dropna()
            if len(d) < MIN_NAMES:
                continue
            out[(F, S)]["ic"].append(d["m"].rank().corr(d["f"].rank()))
            hold = set(d["m"].sort_values(ascending=False).head(max(len(d)//NQ, 1)).index)
            turn = 1.0 - len(hold & out[(F, S)]["prev"]) / len(hold) if out[(F, S)]["prev"] else 1.0
            out[(F, S)]["lo"].append(d["f"].reindex(list(hold)).mean() - turn * COST)
            out[(F, S)]["prev"] = hold
    return out


def ic_t(v):
    s = pd.Series(v).dropna()
    return (s.mean(), s.mean() / (s.std() / np.sqrt(len(s)))) if len(s) > 6 and s.std() else (np.nan, np.nan)


def losharpe(v):
    s = pd.Series(v).dropna()
    if len(s) < 6 or s.std() == 0:
        return np.nan, np.nan
    return ((1 + s).prod() ** (12 / len(s)) - 1) * 100, s.mean() / s.std() * np.sqrt(12)


def main():
    print("Loading Dawn (2016-2026) ...")
    px, _ = load()

    print("\n" + "=" * 70)
    print("MOMENTUM TERM STRUCTURE — formation length F (skip S=1), India 2017-2026")
    print("=" * 70)
    fs = [(1, 1), (2, 1), (3, 1), (6, 1), (9, 1), (12, 1), (18, 1), (24, 1)]
    res = sweep(px, fs)
    print(f"{'spec (F-S)':<12}{'meanIC':>9}{'IC_t':>7}{'LO ann%':>9}{'LO Sharpe':>11}")
    print("-" * 70)
    for (F, S) in fs:
        m, t = ic_t(res[(F, S)]["ic"]); a, sh = losharpe(res[(F, S)]["lo"])
        tag = "  <- our spec" if (F, S) == (12, 1) else ("  reversal zone" if t < -1 else "")
        print(f"{f'{F}-{S} mom':<12}{m:>9.4f}{t:>7.2f}{a:>9.1f}{sh:>11.2f}{tag}")

    print("\n" + "=" * 70)
    print("SKIP effect — formation F=12, vary skip S (dodge short-term reversal)")
    print("=" * 70)
    sk = [(12, 0), (12, 1), (12, 2), (12, 3)]
    res2 = sweep(px, sk)
    print(f"{'spec (F-S)':<12}{'meanIC':>9}{'IC_t':>7}{'LO ann%':>9}{'LO Sharpe':>11}")
    print("-" * 70)
    for (F, S) in sk:
        m, t = ic_t(res2[(F, S)]["ic"]); a, sh = losharpe(res2[(F, S)]["lo"])
        print(f"{f'{F}-{S} mom':<12}{m:>9.4f}{t:>7.2f}{a:>9.1f}{sh:>11.2f}")

    print("-" * 70)
    print("READ: expect F=1 NEGATIVE (short-term reversal), a broad momentum PLATEAU around")
    print("F=6-12 (all similar — momentum isn't knife-edge on the exact lookback), decaying")
    print("at F=18-24. Skip S>=1 helps by dodging the 1-month reversal; S too big loses signal.")
    print("If 6-12 are all similar, 12-1 is a fine, standard choice — not uniquely optimal, but")
    print("robust. Picking the single best F here would be overfitting (CPCV PBO already warned).")


if __name__ == "__main__":
    main()
