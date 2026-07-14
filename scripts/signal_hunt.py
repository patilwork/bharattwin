#!/usr/bin/env python3
"""
Second-signal hunt — find a signal genuinely UNCORRELATED to momentum.

My earlier reversal test was wrong-headed: it compared LONG-ONLY returns, which
all correlate ~0.9 through shared market beta. The correct diversification test is
on LONG-SHORT (top-minus-bottom quintile) returns, which strip market beta and
isolate the pure signal. Two signals can co-move 0.9 long-only yet be ~0 correlated
in signal space.

Candidates (survivorship-aware liveness filter applied):
  momentum   12-1 month
  reversal   -1 month
  low_beta   low market beta (betting-against-beta)
  low_idvol  low idiosyncratic volatility
  value_ey   earnings yield   (2021+ only — fundamentals)
  value_pb   inverse P/B      (2021+ only)

Reports each signal's standalone long-short Sharpe/IC-t, the correlation MATRIX of
the long-short returns, and whether a diversified blend beats momentum alone.

NOTE: single-stock shorting is impractical in India, so long-short is a research
construct. But signals that are LS-uncorrelated still diversify the *selection* in
a long-only book, cutting idiosyncratic risk. That is the usable benefit.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/signal_hunt.py
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

MIN_PRICE, MIN_NAMES, NQ = 10.0, 40, 5
FWD_WINSOR = (-0.40, 0.80)
LS_COST = 2 * (round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025)  # both legs
LIVE_DAYS = 15


def ls_returns(px, f, start="2017-01-31"):
    """Long-short (top-minus-bottom quintile) monthly return series per signal."""
    pxf = px.ffill()
    dret = px.pct_change()
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(start)) & (m <= px.index.max())]
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    bvps, eps = pit(f, "bvps", rebal), pit(f, "ttm_eps", rebal)

    sig_names = ["momentum", "reversal", "low_beta", "low_idvol", "value_ey", "value_pb"]
    out = {s: [] for s in sig_names}
    dates = []
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        win = px.loc[t - pd.Timedelta(days=LIVE_DAYS):t]
        live = win.columns[win.notna().any()]                      # survivorship: live only
        price_t = pxm.loc[t].reindex(live)
        live = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1].reindex(live) / price_t.reindex(live) - 1.0).clip(*FWD_WINSOR)

        R = dret.loc[:t].iloc[-120:].reindex(columns=live)
        R = R.dropna(axis=1, thresh=100)
        mkt = R.mean(axis=1)
        md = mkt - mkt.mean()
        Rd = R.sub(R.mean())
        beta = Rd.mul(md, axis=0).mean() / (md.pow(2).mean())
        resid = R.sub(pd.DataFrame(np.outer(mkt, beta), index=R.index, columns=R.columns))
        idvol = resid.std()
        pwin = px.loc[:t]
        mom = pwin.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1 if c.dropna().shape[0] > 200 else np.nan).reindex(live)

        sigs = {
            "momentum": mom,
            "reversal": -(price_t / pxf.loc[:t].iloc[-21].reindex(live) - 1.0),
            "low_beta": -beta.reindex(live),
            "low_idvol": -idvol.reindex(live),
            "value_ey": (eps.loc[t] / price_t).reindex(live),
            "value_pb": (-(price_t / bvps.loc[t])).reindex(live),
        }
        dates.append(t1)
        for s, v in sigs.items():
            d = pd.concat([v.rename("s"), fwd.rename("f")], axis=1).dropna()
            if len(d) < MIN_NAMES:
                out[s].append(np.nan); continue
            q = pd.qcut(d["s"].rank(method="first"), NQ, labels=False)
            out[s].append(d["f"][q == NQ-1].mean() - d["f"][q == 0].mean() - LS_COST)
    df = pd.DataFrame(out, index=pd.DatetimeIndex(dates))
    return df


def stats(s):
    s = s.dropna()
    if len(s) < 6:
        return np.nan, np.nan
    return s.mean() / s.std() * np.sqrt(12), s.mean() / (s.std()/np.sqrt(len(s)))  # Sharpe, t


def main():
    print("Loading Dawn panel + computing long-short signal returns (2017-2026) ...")
    px, f = load()
    ls = ls_returns(px, f)

    print("\n=== standalone LONG-SHORT signal quality ===")
    print(f"{'signal':<11}{'n':>4}{'ann%':>8}{'LS_Sharpe':>10}{'t-stat':>8}")
    keep = []
    for s in ls.columns:
        r = ls[s].dropna()
        sh, t = stats(r)
        ann = ((1+r).prod()**(12/len(r))-1)*100 if len(r) else np.nan
        flag = "  <-- real" if (t and abs(t) > 2) else ""
        print(f"{s:<11}{len(r):>4}{ann:>8.1f}{sh:>10.2f}{t:>8.2f}{flag}")
        if t and abs(t) > 2:
            keep.append(s)

    print("\n=== LONG-SHORT return CORRELATION matrix (beta-stripped) ===")
    corr = ls.corr()
    cols = list(ls.columns)
    print("           " + "".join(f"{c[:8]:>9}" for c in cols))
    for r in cols:
        print(f"{r:<11}" + "".join(f"{corr.loc[r,c]:>9.2f}" for c in cols))

    print("\n=== momentum's correlation to each candidate (the key row) ===")
    for s in ls.columns:
        if s == "momentum":
            continue
        c = ls["momentum"].corr(ls[s])
        tag = "UNCORRELATED" if abs(c) < 0.3 else ("negatively corr" if c < -0.3 else "correlated")
        print(f"  momentum vs {s:<10} corr = {c:+.2f}   {tag}")

    # diversified blend: momentum + best uncorrelated real signal
    print("\n=== diversification test ===")
    mom_sh, _ = stats(ls["momentum"])
    # a usable diversifier must be PROFITABLE (positive Sharpe), meaningful (|t|>1.5),
    # and uncorrelated to momentum (|corr|<0.4). Blend on the common (overlapping) window.
    for s in ls.columns:
        if s == "momentum":
            continue
        sh, t = stats(ls[s])
        c = ls["momentum"].corr(ls[s])
        if sh and sh > 0 and t and abs(t) > 1.5 and abs(c) < 0.4:
            common = pd.concat([ls["momentum"], ls[s]], axis=1).dropna()
            blend = common.mean(axis=1)
            bsh, _ = stats(blend)
            m_only, _ = stats(common["momentum"])   # momentum Sharpe on the SAME window
            print(f"  momentum+{s:<9} blend Sharpe {bsh:.2f}  vs momentum-alone {m_only:.2f} "
                  f"(same window) -> {'IMPROVES' if bsh > m_only else 'no gain'}  [corr {c:+.2f}]")
    print("-" * 70)
    print("READ: an uncorrelated + real LS signal diversifies stock SELECTION even in a")
    print("long-only book. Value is the classic momentum diversifier — check its corr.")


if __name__ == "__main__":
    main()
