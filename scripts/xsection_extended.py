#!/usr/bin/env python3
"""
Extended + survivorship-aware backtest, and a second-signal (reversal) test.

Three things at once:
  1. GO BACK further — momentum needs no fundamentals, so extend to 2017-2026
     (~9yr) using Dawn's price history, adding the 2018 NBFC crisis and 2020
     COVID crash (the regimes that test momentum's crash risk). (Value can't
     extend — fundamentals start 2021.)
  2. SURVIVORSHIP fix — a LIVENESS filter: a name is only in the universe at t
     if it actually traded within ~15d of t. Dead names are dropped (not
     forward-filled at a flat price), and a name that delists mid-hold realises
     its loss to the last traded price. Reported vs the naive ffill version so
     the bias is quantified.
  3. SECOND SIGNAL — short-term reversal (buy last month's losers), which is
     documented to be ~uncorrelated / negatively correlated to 12-1 momentum.
     We measure its IC and the correlation of its returns to momentum's.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/xsection_extended.py
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
LIVE_DAYS = 15   # must have traded within this many calendar days of t


def backtest(px, signal="momentum", survivorship_aware=True, start="2017-01-31"):
    pxf = px.ffill()
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(start)) & (m <= px.index.max())]
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)  # prices at rebalance dates

    ic, ret = [], []
    ic_year = {}
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        # liveness: actually traded within LIVE_DAYS of t (survivorship fix)
        if survivorship_aware:
            win = px.loc[t - pd.Timedelta(days=LIVE_DAYS):t]
            live = win.columns[win.notna().any()]
        else:
            live = px.columns
        price_t = pxm.loc[t].reindex(live)
        live = price_t[price_t >= MIN_PRICE].index

        # forward return; delisted-mid-hold names realise loss to last traded price
        fwd = (pxm.loc[t1].reindex(live) / price_t.reindex(live) - 1.0).clip(*FWD_WINSOR)

        win_p = px.loc[:t]
        if signal == "momentum":
            sig = win_p.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1 if c.dropna().shape[0] > 200 else np.nan).reindex(live)
        else:  # reversal: negative of last ~1-month return
            sig = -(pxm.loc[t].reindex(live) / pxf.loc[:t].iloc[-21].reindex(live) - 1.0)

        d = pd.concat([sig.rename("s"), fwd.rename("f")], axis=1).dropna()
        if len(d) < MIN_NAMES:
            continue
        ic_v = d["s"].rank().corr(d["f"].rank())
        ic.append(ic_v)
        ic_year.setdefault(t.year, []).append(ic_v)
        q = pd.qcut(d["s"].rank(method="first"), NQ, labels=False)
        ret.append(d["f"][q == NQ - 1].mean() - COST)

    ic, ret = pd.Series(ic).dropna(), pd.Series(ret).dropna()
    return {
        "ic_mean": ic.mean(),
        "ic_t": ic.mean() / (ic.std() / np.sqrt(len(ic))) if ic.std() else np.nan,
        "ann": ((1 + ret).prod() ** (12 / len(ret)) - 1) * 100,
        "sharpe": ret.mean() / ret.std() * np.sqrt(12) if ret.std() else np.nan,
        "n": len(ret), "ret": ret, "ic_year": {y: np.nanmean(v) for y, v in ic_year.items()},
    }


def main():
    print("Loading Dawn prices (2016-2026, split/bonus adjusted) ...")
    px, _ = load()

    print("\n=== 1+2. EXTENDED MOMENTUM: survivorship-aware vs naive ffill (2017-2026) ===")
    print(f"{'variant':<28}{'n':>4}{'meanIC':>8}{'IC_t':>7}{'ann%':>8}{'Sharpe':>8}")
    aware = backtest(px, "momentum", survivorship_aware=True)
    naive = backtest(px, "momentum", survivorship_aware=False)
    for label, s in [("survivorship-aware", aware), ("naive ffill (biased)", naive)]:
        print(f"{label:<28}{s['n']:>4}{s['ic_mean']:>8.4f}{s['ic_t']:>7.2f}{s['ann']:>8.1f}{s['sharpe']:>8.2f}")
    print(f"\nsurvivorship drag: ann {naive['ann']-aware['ann']:+.1f}pp, Sharpe {naive['sharpe']-aware['sharpe']:+.2f} "
          f"(naive ffill is optimistically biased by this much)")
    print("momentum IC by year (survivorship-aware):")
    print("  " + "  ".join(f"{y}:{v:+.3f}" for y, v in sorted(aware['ic_year'].items())))

    print("\n=== 3. SECOND SIGNAL: short-term reversal (2017-2026, survivorship-aware) ===")
    rev = backtest(px, "reversal", survivorship_aware=True)
    print(f"{'reversal':<28}{rev['n']:>4}{rev['ic_mean']:>8.4f}{rev['ic_t']:>7.2f}{rev['ann']:>8.1f}{rev['sharpe']:>8.2f}")
    # correlation of the two strategies' monthly returns (diversification test)
    j = pd.concat([aware["ret"].reset_index(drop=True), rev["ret"].reset_index(drop=True)], axis=1).dropna()
    corr = j.iloc[:, 0].corr(j.iloc[:, 1])
    print(f"\ncorrelation(momentum returns, reversal returns) = {corr:+.2f}")
    if corr < 0.3:
        # 50/50 combined
        comb = 0.5 * aware["ret"].reset_index(drop=True) + 0.5 * rev["ret"].reset_index(drop=True)
        comb = comb.dropna()
        csh = comb.mean() / comb.std() * np.sqrt(12)
        print(f"50/50 momentum+reversal blend Sharpe = {csh:.2f} "
              f"(vs momentum {aware['sharpe']:.2f}) — diversification {'HELPS' if csh > aware['sharpe'] else 'does not help'}")
    print("-" * 72)
    print("READ: if the survivorship-aware momentum still has IC_t>2, the edge survives")
    print("dropping the ffill bias. If reversal has real IC AND low corr to momentum, it's")
    print("the uncorrelated 2nd signal we wanted. Caveat: 212 dead names captured, but fully")
    print("purged delistings still absent; and true 20yr needs a survivor-only Kite backfill.")


if __name__ == "__main__":
    main()
