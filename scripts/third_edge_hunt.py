#!/usr/bin/env python3
"""
Third-edge hunt — a factor that is (a) uncorrelated to BOTH momentum and value,
(b) genuinely pays, and (c) lifts the momentum+value blend.

Methodology (locked from signal_hunt): work on LONG-SHORT (top-minus-bottom
quintile) returns — they strip market beta and isolate the signal. A candidate is
a real third edge only if its LS return is positive (t>~2), its correlation to
BOTH momentum-LS and value-LS is low, AND adding it to the long-only mom+value
book raises Sharpe.

Candidates (price-computable, genuinely distinct from momentum/value — the low-
beta/low-vol/reversal ones were already rejected):
  - LTR   long-term reversal: contrarian on the 5yr→1yr past return (DeBondt-Thaler)
  - SEAS  seasonality: same-calendar-month historical average return (Heston-Sadka)
  - LOMAX anti-lottery: short the highest max-daily-return names (Bali-Cakici-Whitelaw)
  - RESMOM residual momentum: 12-1 momentum of market-residual returns (Blitz)

Survivorship-aware, turnover-costed. LS uses Q5-Q1; blend test uses the long-only
top-quintile of the combined z-score.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/third_edge_hunt.py
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

MIN_PRICE, MIN_NAMES, NQ = 10.0, 30, 5
FWD_WINSOR = (-0.40, 0.80)
COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025
LIVE_DAYS = 15
START = "2019-01-31"      # LTR/seasonality need multi-year history


def _z(s):
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def _ls(sig, fwd):
    d = pd.concat([sig.rename("s"), fwd.rename("f")], axis=1).dropna()
    if len(d) < MIN_NAMES:
        return np.nan
    q = pd.qcut(d["s"].rank(method="first"), NQ, labels=False)
    return d["f"][q == NQ - 1].mean() - d["f"][q == 0].mean()


def run(px, f):
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    dret = px.pct_change()
    mret = dret.clip(-0.2, 0.2).mean(axis=1)                  # equal-weight market proxy
    bvps = pit(f, "bvps", rebal)
    # monthly return panel for seasonality
    pxmly = pxf.resample("ME").last()
    mret_panel = pxmly.pct_change()

    sigs = ["momentum", "value_pb", "LTR", "SEAS", "LOMAX", "RESMOM"]
    ls = {k: [] for k in sigs}
    zc = {k: [] for k in sigs}          # store z-scores per rebalance for blend
    fwds, dates = [], []

    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        win = px.loc[t - pd.Timedelta(days=LIVE_DAYS):t]
        live = win.columns[win.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        live = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1].reindex(live) / price_t.reindex(live) - 1.0).clip(*FWD_WINSOR)

        hist = px.loc[:t]
        # momentum 12-1
        mom = hist.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1
                                        if c.dropna().shape[0] > 200 else np.nan).reindex(live)
        # value (P/B)
        vpb = -(price_t / bvps.loc[t]).reindex(live)
        # long-term reversal: -(return t-1260d..t-252d), i.e. buy 5y->1y losers
        ltr_win = hist.iloc[-1260:-252]
        ltr = -ltr_win.apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1
                             if c.dropna().shape[0] > 400 else np.nan).reindex(live)
        # seasonality: avg same-calendar-month return in prior years
        mm = t.month
        past = mret_panel.loc[:t].iloc[:-1]
        seas = past[past.index.month == mm].mean().reindex(live)
        # anti-lottery: -(max daily return last 21d)
        lomax = -dret.loc[:t].iloc[-21:].max().reindex(live)
        # residual momentum: 12-1 momentum of (ret - beta*mkt) over the window
        dwin = dret.loc[:t].iloc[-252:-21]
        mwin = mret.loc[dwin.index]
        varm = mwin.var()
        def _rm(c):
            cc = c.dropna()
            if cc.shape[0] < 150 or varm == 0:
                return np.nan
            b = cc.cov(mwin.reindex(cc.index)) / varm
            resid = cc - b * mwin.reindex(cc.index)
            return resid.sum()               # cumulative residual (approx log-mom)
        with np.errstate(invalid="ignore", divide="ignore"):
            resmom = dwin.apply(_rm).reindex(live)

        raw = {"momentum": mom, "value_pb": vpb, "LTR": ltr,
               "SEAS": seas, "LOMAX": lomax, "RESMOM": resmom}
        for k, s in raw.items():
            ls[k].append(_ls(s, fwd))
            zc[k].append(_z(s))
        fwds.append(fwd); dates.append(t1)

    lsdf = pd.DataFrame(ls, index=pd.DatetimeIndex(dates))
    return lsdf, zc, fwds, pd.DatetimeIndex(dates)


def sharpe(r):
    r = pd.Series(r).dropna()
    return r.mean() / r.std() * np.sqrt(12) if len(r) > 6 and r.std() else np.nan


def tstat(r):
    r = pd.Series(r).dropna()
    return r.mean() / (r.std() / np.sqrt(len(r))) if len(r) > 6 and r.std() else np.nan


def blend_book(zc, fwds, keys):
    """Long-only top-quintile of the equal-weighted combined z-score of `keys`."""
    ret = []
    prev = set()
    for zrow, fwd in zip(zip(*[zc[k] for k in keys]), fwds):
        comb = pd.concat(list(zrow), axis=1).mean(axis=1, skipna=True)
        comb = comb[comb.index.isin(fwd.dropna().index)].dropna()
        if len(comb) < MIN_NAMES:
            ret.append(np.nan); continue
        k = max(len(comb)//NQ, 1)
        hold = set(comb.sort_values(ascending=False).head(k).index)
        turn = 1.0 - len(hold & prev)/len(hold) if prev else 1.0
        ret.append(fwd.reindex(list(hold)).mean() - turn*COST)
        prev = hold
    return sharpe(ret)


def main():
    print("Loading Dawn ...")
    px, f = load()
    lsdf, zc, fwds, dates = run(px, f)

    print("\n" + "=" * 78)
    print(f"THIRD-EDGE HUNT — {dates[0].date()}→{dates[-1].date()} ({len(lsdf)} months)")
    print("LS = top-minus-bottom quintile (beta-stripped); a third edge must be")
    print("uncorrelated to BOTH momentum & value, pay (t>~2), and lift the blend.")
    print("=" * 78)
    print(f"{'signal':<10}{'LS ann%':>9}{'LS Sharpe':>11}{'t':>7}{'corr→mom':>10}{'corr→val':>10}")
    print("-" * 78)
    for k in lsdf.columns:
        s = lsdf[k]
        cm = s.corr(lsdf["momentum"]); cv = s.corr(lsdf["value_pb"])
        ann = s.mean()*12*100
        tag = ""
        if k not in ("momentum", "value_pb"):
            uncorr = abs(cm) < 0.3 and abs(cv) < 0.3
            pays = tstat(s) > 1.5
            tag = "  <-- CANDIDATE" if (uncorr and pays) else ("  (uncorr, weak)" if uncorr else "  (correlated)")
        print(f"{k:<10}{ann:>9.1f}{sharpe(s):>11.2f}{tstat(s):>7.2f}{cm:>10.2f}{cv:>10.2f}{tag}")

    print("-" * 78)
    print("BLEND TEST — does adding the candidate to the long-only mom+value book help?")
    base = blend_book(zc, fwds, ["momentum", "value_pb"])
    print(f"  momentum + value_pb (2-way, base)         Sharpe {base:.2f}")
    for k in ["LTR", "SEAS", "LOMAX", "RESMOM"]:
        s3 = blend_book(zc, fwds, ["momentum", "value_pb", k])
        verdict = "HELPS" if s3 > base + 0.03 else ("~flat" if s3 > base - 0.03 else "HURTS")
        print(f"  + {k:<6} (3-way)                            Sharpe {s3:.2f}   {verdict}")
    print("-" * 78)
    print("READ: adopt a third factor only if it's uncorrelated to both AND the 3-way")
    print("blend Sharpe clears the 2-way base. Uncorrelated-but-weak or correlated = skip.")


if __name__ == "__main__":
    main()
