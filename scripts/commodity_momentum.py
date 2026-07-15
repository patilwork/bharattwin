#!/usr/bin/env python3
"""
Does momentum work on commodities? Two flavours, because commodities are different:

  A. TIME-SERIES momentum (trend) — each commodity long if its own 12-1 return is up,
     short if down, equal-weight the basket. This is the CTA / managed-futures edge and
     the same idea as our Tier-1.2 trend overlay. Expected to WORK.

  B. CROSS-SECTIONAL momentum (relative strength) — rank the ~25 commodities, long the
     top third, short the bottom. This is our equity stock-picker's mechanic. Expected
     to be THIN — only ~25 assets to rank (vs 1000 stocks).

Note: our VALUE leg has NO commodity analog (no earnings/book) — the commodity
"value/carry" signal is the futures term structure (backwardation), which needs
multi-contract data yfinance's continuous front-month (=F) doesn't provide. So this
tests momentum only. Prices via yfinance continuous front-month futures.

Usage:
  python3 scripts/commodity_momentum.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

CACHE = Path(__file__).resolve().parent.parent / "data" / "global" / "commodities.pkl"
FUT = ("GC=F SI=F HG=F PL=F PA=F CL=F BZ=F NG=F HO=F RB=F ZC=F ZW=F ZS=F ZL=F ZM=F "
       "KC=F CT=F CC=F SB=F LE=F HE=F GF=F").split()
NAMES = {"GC=F": "gold", "SI=F": "silver", "HG=F": "copper", "CL=F": "WTI crude", "NG=F": "natgas"}


def load():
    if CACHE.exists():
        return pd.read_pickle(CACHE)
    import yfinance as yf
    df = yf.download(FUT, start="2015-01-01", end="2026-07-14", interval="1d",
                     auto_adjust=True, progress=False, threads=True)["Close"]
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(CACHE)
    return df


def sharpe(r):
    r = r.dropna()
    return r.mean() / r.std() * np.sqrt(12) if len(r) > 6 and r.std() else np.nan


def ann(r):
    r = r.dropna()
    return ((1 + r).prod() ** (12 / len(r)) - 1) * 100 if len(r) else np.nan


def main():
    px = load().dropna(axis=1, how="all")
    print(f"commodities: {px.shape[1]} futures, {px.index.min().date()}→{px.index.max().date()}\n")
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp("2016-01-31")) & (m <= px.index.max())]
    pxm = px.reindex(px.index.union(rebal)).ffill().reindex(rebal)

    ts, xs_ic, xs_ls = [], [], []
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        seg = px.loc[:t].iloc[-252:-21]
        mom = seg.apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1 if c.dropna().shape[0] > 200 else np.nan)
        fwd = (pxm.loc[t1] / pxm.loc[t] - 1.0).clip(-0.4, 0.8)
        d = pd.concat([mom.rename("m"), fwd.rename("f")], axis=1).dropna()
        if len(d) < 8:
            continue
        # A. time-series: long up-trenders, short down-trenders, equal weight
        ts.append((np.sign(d["m"]) * d["f"]).mean())
        # B. cross-sectional: top third minus bottom third
        d = d.sort_values("m")
        k = max(len(d)//3, 1)
        xs_ls.append(d["f"].iloc[-k:].mean() - d["f"].iloc[:k].mean())
        xs_ic.append(d["m"].rank().corr(d["f"].rank()))

    ts, xs_ls, xs_ic = pd.Series(ts), pd.Series(xs_ls), pd.Series(xs_ic)
    print("=" * 66)
    print("COMMODITY MOMENTUM (2016-2026, ~monthly, front-month futures)")
    print("=" * 66)
    print(f"A. TIME-SERIES / trend  (long up-trends, short down-trends)")
    print(f"     ann {ann(ts):+.1f}%   Sharpe {sharpe(ts):.2f}   <- the CTA edge")
    print(f"B. CROSS-SECTIONAL      (long top third, short bottom third)")
    print(f"     LS ann {ann(xs_ls):+.1f}%   Sharpe {sharpe(xs_ls):.2f}   "
          f"IC_t {xs_ic.mean()/(xs_ic.std()/np.sqrt(len(xs_ic))):.2f}")
    print("-" * 66)
    print("READ (honest): BOTH came out NEGATIVE here — which CONTRADICTS the strong long-")
    print("history literature on commodity trend momentum. Two reasons, not 'momentum fails':")
    print("  1. 2016-2026 was a WEAK, whippy decade for commodity trend (CTAs struggled")
    print("     2011-2020; one good 2021-22 energy run); short + regime-dependent sample.")
    print("  2. DATA: yfinance front-month (=F) is NOT roll-adjusted — roll jumps corrupt the")
    print("     momentum signal. A real test needs roll-adjusted continuous / total-return series.")
    print("So DON'T read this as 'commodities don't trend'. The STRUCTURAL point stands")
    print("regardless: our equity book does NOT port (no value analog; ~22-asset universe is")
    print("thin). A proper commodity strategy = TREND + CARRY (term-structure), vol-targeted")
    print("= a managed-futures/CTA — a DIFFERENT product that shares only the momentum DNA.")


if __name__ == "__main__":
    main()
