#!/usr/bin/env python3
"""
Market-neutral variant — does hedging out beta with Nifty futures isolate the alpha?

Our book is long-only, so ~most of its return is market beta and it eats the full
crash (-25..-40% drawdowns). Single-stock shorting isn't executable in India, but
NIFTY 50 futures are liquid (~6bps). This tests the one tradeable "long-short":
hold the long-only momentum+value composite and SHORT Nifty futures at the book's
trailing beta, rolled monthly, net of futures cost.

Three series, same rebalance dates (2021+, where value is available):
  1. long-only composite            — what we run today (beta + alpha)
  2. hedged vs NIFTY 50 (tradeable) — short Nifty futures at ex-ante trailing beta
  3. excess vs equal-weight universe — the "ideal" cross-sectional alpha (not a
     tradeable hedge; the universe has no liquid future, but it bounds the pure
     stock-selection signal and shows the Nifty basis/cap-tilt drag)

Read: hedging should collapse residual beta to ~0, cut the drawdown hard, and lift
Sharpe — but give up the beta return (lower total return). It's a Sharpe play, not
a bigger-returns play, and only worth it if the alpha clears the futures cost +
the large/mid-cap basis between our book and Nifty 50.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/market_neutral_test.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from xsection_montecarlo import load, pit, DAWN_URL
from src.costs import round_trip_cost

MIN_PRICE, MIN_NAMES, NQ = 10.0, 30, 5
FWD_WINSOR = (-0.40, 0.80)
COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025
FUT_COST_M = round_trip_cost(1_000_000, "index_futures").total_bps / 1e4   # per monthly roll
BETA_WIN = 12          # trailing months for the ex-ante hedge ratio
START = "2021-06-30"


def _z(s):
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def composite_and_bench(px, f):
    """Monthly net long-only composite (momentum+value) return + equal-weight
    universe benchmark, on month-end rebalances from START."""
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    bvps, eps = pit(f, "bvps", rebal), pit(f, "ttm_eps", rebal)

    comp_ret, bench_ret, dates, prev = [], [], [], set()
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        win = px.loc[t - pd.Timedelta(days=15):t]
        live = win.columns[win.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        liquid = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1].reindex(liquid) / price_t.reindex(liquid) - 1.0).clip(*FWD_WINSOR)

        seg = px.loc[:t].iloc[-252:-21]
        mom = seg.apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1
                        if c.dropna().shape[0] > 200 else np.nan).reindex(liquid)
        ey = (eps.loc[t] / price_t).reindex(liquid)
        pb = -(price_t / bvps.loc[t]).reindex(liquid)
        comp = pd.concat([_z(mom), _z(ey), _z(pb)], axis=1).mean(axis=1, skipna=True)
        comp = comp[comp.index.isin(fwd.dropna().index)].dropna()
        if len(comp) < MIN_NAMES:
            continue
        k = max(len(comp) // NQ, 1)
        hold = set(comp.sort_values(ascending=False).head(k).index)
        gross = fwd.reindex(list(hold)).mean()
        turn = 1.0 - len(hold & prev) / len(hold) if prev else 1.0
        comp_ret.append(gross - turn * COST)
        bench_ret.append(fwd.mean())
        dates.append(t1)
        prev = hold
    idx = pd.DatetimeIndex(dates)
    return pd.Series(comp_ret, idx), pd.Series(bench_ret, idx)


def nifty_monthly(rebal_idx):
    eng = create_engine(DAWN_URL)
    n = pd.read_sql(text("SELECT date, tri FROM benchmark_tri_daily WHERE index_name='NIFTY 50'"), eng)
    eng.dispose()
    n["date"] = pd.to_datetime(n["date"])
    s = n.set_index("date")["tri"].sort_index()
    at = s.reindex(s.index.union(rebal_idx)).ffill().reindex(rebal_idx)
    return at.pct_change().reindex(rebal_idx)


def stats(r, mkt=None):
    r = r.dropna()
    eq = (1 + r).cumprod()
    dd = (eq / eq.cummax() - 1).min()
    out = {
        "ann": ((1 + r).prod() ** (12 / len(r)) - 1) * 100,
        "vol": r.std() * np.sqrt(12) * 100,
        "sharpe": r.mean() / r.std() * np.sqrt(12) if r.std() else np.nan,
        "maxdd": dd * 100,
    }
    if mkt is not None:
        d = pd.concat([r, mkt], axis=1).dropna()
        out["beta"] = np.cov(d.iloc[:, 0], d.iloc[:, 1])[0, 1] / np.var(d.iloc[:, 1]) if d.iloc[:, 1].var() else np.nan
    return out


def main():
    print("Loading Dawn (prices + fundamentals) ...")
    px, f = load()
    comp, bench = composite_and_bench(px, f)
    rebal = comp.index
    # market monthly returns aligned to the same forward windows
    m = px.resample("ME").last().index
    rb = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    nif = nifty_monthly(rb).reindex(rebal)

    # ex-ante trailing beta of the book to Nifty (default 1.0 until we have BETA_WIN obs)
    beta = pd.Series(index=rebal, dtype=float)
    for i, t in enumerate(rebal):
        if i < BETA_WIN:
            beta.iloc[i] = 1.0
        else:
            w = pd.concat([comp.iloc[i-BETA_WIN:i], nif.iloc[i-BETA_WIN:i]], axis=1).dropna()
            beta.iloc[i] = (np.cov(w.iloc[:, 0], w.iloc[:, 1])[0, 1] / np.var(w.iloc[:, 1])
                            if len(w) > 6 and w.iloc[:, 1].var() else 1.0)

    hedged = comp - beta * nif - beta.abs() * FUT_COST_M    # short Nifty at trailing beta, roll cost
    excess = comp - bench                                    # ideal cross-sectional alpha

    print("\n" + "=" * 84)
    print(f"MARKET-NEUTRAL VARIANT — {rebal[0].date()}→{rebal[-1].date()} ({len(comp)} months, 2021+)")
    print(f"long-only momentum+value composite | Nifty hedge cost {FUT_COST_M*1e4:.1f}bps/roll | "
          f"trailing beta {BETA_WIN}m")
    print("=" * 84)
    print(f"{'variant':<34}{'ann%':>8}{'vol%':>7}{'Sharpe':>8}{'maxDD%':>8}{'resid β':>9}")
    print("-" * 84)
    rows = [
        ("1. long-only composite", comp),
        ("2. hedged vs NIFTY 50 (tradeable)", hedged),
        ("3. excess vs equal-wt universe", excess),
    ]
    for name, s in rows:
        st = stats(s, nif)
        print(f"{name:<34}{st['ann']:>8.1f}{st['vol']:>7.1f}{st['sharpe']:>8.2f}"
              f"{st['maxdd']:>8.1f}{st.get('beta', float('nan')):>9.2f}")
    print("-" * 84)
    print(f"avg trailing hedge beta applied: {beta.mean():.2f}")
    lo, hd = stats(comp), stats(hedged)
    print("READ: hedging should push residual β→~0 and cut maxDD hard. If Sharpe RISES while")
    print("total return FALLS, the beta-hedge is doing its job (isolating alpha) — it's a")
    print("risk-adjusted upgrade, not a return upgrade. If Sharpe FALLS, the Nifty cap-tilt")
    print("basis + futures cost are eating more than the beta removal is worth.")
    print(f"  long-only Sharpe {lo['sharpe']:.2f} (maxDD {lo['maxdd']:.0f}%) vs "
          f"hedged Sharpe {hd['sharpe']:.2f} (maxDD {hd['maxdd']:.0f}%)")


if __name__ == "__main__":
    main()
