#!/usr/bin/env python3
"""
US-market replication — does the cross-sectional momentum edge survive in the most
efficient, most-arbitraged market on earth?

Mirrors the India momentum engine (12-1, monthly, top-quintile long + long-short,
rank-IC) on a US universe (S&P 500) from yfinance. Momentum only: value/composite
would need a point-in-time US fundamentals panel we don't have (same limit as India).

TWO BIG CAVEATS (why this is not apples-to-apples with India):
  1. SURVIVORSHIP BIAS — the universe is TODAY's S&P 500 constituents. Names that
     dropped out or delisted are absent, and today's members are the survivors/winners.
     This flatters momentum. India's engine was survivorship-CONTROLLED (liveness
     filter). So expect the US numbers to read optimistically; the honest US edge is
     weaker than shown here.
  2. LARGE-CAP ONLY — S&P 500 is all large caps. India's edge was strongest in SMALL
     caps (factor efficacy where the market is less arbitraged). US large caps are the
     most efficient slice, so this is the HARDEST case — the weakest place to find edge.

Data cached under data/us/ (gitignored). Refetch by deleting the cache.

Usage:
  python3 scripts/us_replication.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
CACHE = REPO / "data" / "us" / "us_close.pkl"
UNIV = REPO / "data" / "us" / "sp500.txt"
MIN_PRICE, MIN_NAMES, NQ = 5.0, 30, 5
FWD_WINSOR = (-0.40, 0.80)
US_COST = 0.0015          # ~15bps round-trip (liquid US large-cap; far cheaper than India)


def load_close() -> pd.DataFrame:
    if CACHE.exists():
        return pd.read_pickle(CACHE)
    import yfinance as yf
    syms = UNIV.read_text().split()
    df = yf.download(syms, start="2015-01-01", end="2026-07-14", interval="1d",
                     auto_adjust=True, progress=False, threads=True)["Close"]
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(CACHE)
    return df


def backtest(close: pd.DataFrame, start="2016-01-31"):
    m = close.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(start)) & (m <= close.index.max())]
    pxm = close.reindex(close.index.union(rebal)).ffill().reindex(rebal)

    ic, ls, lo, bench, dates = [], [], [], [], []
    prev = set()
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        price_t = pxm.loc[t]
        live = price_t[price_t >= MIN_PRICE].dropna().index
        fwd = (pxm.loc[t1].reindex(live) / price_t.reindex(live) - 1.0).clip(*FWD_WINSOR)
        seg = close.loc[:t].iloc[-252:-21]
        mom = seg.apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1
                        if c.dropna().shape[0] > 200 else np.nan).reindex(live)
        d = pd.concat([mom.rename("m"), fwd.rename("f")], axis=1).dropna()
        if len(d) < MIN_NAMES:
            continue
        ic.append(d["m"].rank().corr(d["f"].rank()))
        q = pd.qcut(d["m"].rank(method="first"), NQ, labels=False)
        ls.append(d["f"][q == NQ-1].mean() - d["f"][q == 0].mean())
        hold = set(d["m"].sort_values(ascending=False).head(max(len(d)//NQ, 1)).index)
        turn = 1.0 - len(hold & prev)/len(hold) if prev else 1.0
        lo.append(d["f"].reindex(list(hold)).mean() - turn*US_COST)
        bench.append(d["f"].mean())
        dates.append(t1); prev = hold
    idx = pd.DatetimeIndex(dates)
    return (pd.Series(ic, idx), pd.Series(ls, idx), pd.Series(lo, idx), pd.Series(bench, idx))


def stat(s, ann=False):
    s = s.dropna()
    if ann:
        return ((1+s).prod()**(12/len(s))-1)*100, s.mean()/s.std()*np.sqrt(12) if s.std() else np.nan
    return s.mean(), s.mean()/(s.std()/np.sqrt(len(s))) if s.std() else np.nan


def main():
    print("Loading US (S&P 500) closes ...")
    close = load_close()
    print(f"universe {close.shape[1]} names, {close.index.min().date()}→{close.index.max().date()}\n")

    for label, start in [("FULL 2016-2026", "2016-01-31"), ("recent 2021-2026", "2021-01-31")]:
        ic, ls, lo, bench = backtest(close, start)
        ic_m, ic_t = stat(ic)
        ls_ann, ls_sh = stat(ls, ann=True)
        lo_ann, lo_sh = stat(lo, ann=True)
        bn_ann, bn_sh = stat(bench, ann=True)
        exc = pd.Series(lo.values - bench.values, index=lo.index)
        ex_ann, ex_sh = stat(exc, ann=True)
        print("=" * 76)
        print(f"US MOMENTUM (12-1, monthly, S&P 500) — {label}  [{len(ic)} months]")
        print("=" * 76)
        print(f"  rank-IC mean {ic_m:+.4f}  IC_t {ic_t:.2f}   (India momentum IC_t ~4.0)")
        print(f"  long-short (Q5-Q1)   ann {ls_ann:+.1f}%   Sharpe {ls_sh:.2f}")
        print(f"  long-only top-quintile ann {lo_ann:+.1f}%   Sharpe {lo_sh:.2f}")
        print(f"  equal-weight universe   ann {bn_ann:+.1f}%   Sharpe {bn_sh:.2f}")
        print(f"  EXCESS (alpha vs eq-wt) ann {ex_ann:+.1f}%   Sharpe {ex_sh:.2f}\n")

    print("-" * 76)
    print("READ vs INDIA: India momentum IC_t ~4.0 (survivorship-controlled). If US IC_t is")
    print("much lower — and it's inflated here by survivorship + large-cap-only — the edge is")
    print("weaker in the US, as theory predicts (more efficient market). A positive but thin,")
    print("survivorship-flattered US IC would say: the anomaly exists but is largely arbitraged")
    print("away in US large caps; the real US test needs point-in-time membership + small caps.")


if __name__ == "__main__":
    main()
