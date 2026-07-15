#!/usr/bin/env python3
"""
Smart-beta reality check — does our DIY book beat what you could just BUY?

Our momentum+value composite IS smart beta (rules-based, long-only, factor-tilted).
So the honest benchmark isn't the plain index — it's the off-the-shelf smart-beta
product. If we can't beat a cheap factor ETF net of effort, the rational move is to
buy the ETF. This compares, over the same 2021+ window:

  - our composite (broad ₹2000cr+ universe, momentum+value, top-25)
  - a LARGE-CAP MOMENTUM sleeve we build (top-150 by mcap -> top-30 momentum,
    monthly) — a proxy for a Nifty-Momentum-30 / Alpha style ETF (Dawn has no
    momentum-index TRI locally, so we construct the closest tradeable comparator)
  - NIFTY50 VALUE 20   — a REAL smart-beta value index (ETF-able)
  - NIFTY MIDCAP 150   — mid-cap beta (how much of our edge is just cap tilt?)
  - NIFTY 500          — broad beta baseline

The question: is our broad multi-factor book's return EXCESS over the best
smart-beta comparator real and big enough to justify DIY? (Info ratio of that
excess is the tell.) In-sample, bull-heavy window — read the spreads, not levels.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/smart_beta_test.py
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
from market_neutral_test import composite_and_bench, START
from src.costs import round_trip_cost

MIN_PRICE, NQ = 10.0, 5
FWD_WINSOR = (-0.40, 0.80)
COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025


def large_cap_momentum(px, f, top_mcap=150, hold=30):
    """ETF proxy: top-`top_mcap` by market cap, then top-`hold` by 12-1 momentum,
    equal-weight, monthly, turnover-costed. Approximates a large-cap momentum ETF."""
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    shr = pit(f, "shares_outstanding", rebal)

    ret, dates, prev = [], [], set()
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        win = px.loc[t - pd.Timedelta(days=15):t]
        live = win.columns[win.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        live = price_t[price_t >= MIN_PRICE].index
        mcap = (price_t.reindex(live) * shr.loc[t].reindex(live)).dropna()
        bigcaps = mcap.sort_values(ascending=False).head(top_mcap).index
        fwd = (pxm.loc[t1].reindex(bigcaps) / price_t.reindex(bigcaps) - 1.0).clip(*FWD_WINSOR)
        seg = px.loc[:t].iloc[-252:-21]
        mom = seg.apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1
                        if c.dropna().shape[0] > 200 else np.nan).reindex(bigcaps)
        d = pd.concat([mom.rename("m"), fwd.rename("f")], axis=1).dropna()
        if len(d) < hold:
            continue
        picks = set(d["m"].sort_values(ascending=False).head(hold).index)
        gross = d["f"].reindex(list(picks)).mean()
        turn = 1.0 - len(picks & prev) / len(picks) if prev else 1.0
        ret.append(gross - turn * COST); dates.append(t1); prev = picks
    return pd.Series(ret, pd.DatetimeIndex(dates))


def index_monthly(index_name, rebal_idx):
    eng = create_engine(DAWN_URL)
    n = pd.read_sql(text("SELECT date, tri FROM benchmark_tri_daily WHERE index_name=:i"),
                    eng, params={"i": index_name})
    eng.dispose()
    n["date"] = pd.to_datetime(n["date"])
    s = n.set_index("date")["tri"].sort_index()
    at = s.reindex(s.index.union(rebal_idx)).ffill().reindex(rebal_idx)
    return at.pct_change().reindex(rebal_idx)


def stats(r):
    r = r.dropna()
    eq = (1 + r).cumprod()
    dd = (eq / eq.cummax() - 1).min()
    return {"ann": ((1 + r).prod() ** (12 / len(r)) - 1) * 100,
            "vol": r.std() * np.sqrt(12) * 100,
            "sharpe": r.mean() / r.std() * np.sqrt(12) if r.std() else np.nan,
            "maxdd": dd * 100}


def info_ratio(book, bench):
    d = pd.concat([book, bench], axis=1).dropna()
    ex = d.iloc[:, 0] - d.iloc[:, 1]
    return ex.mean() * 12 * 100, (ex.mean() / ex.std() * np.sqrt(12) if ex.std() else np.nan)


def main():
    print("Loading Dawn (prices + fundamentals) ...")
    px, f = load()
    comp, _ = composite_and_bench(px, f)
    lcm = large_cap_momentum(px, f).reindex(comp.index)
    rebal = comp.index

    series = {
        "OUR composite (broad, mom+value)": comp,
        "large-cap momentum sleeve (ETF proxy)": lcm,
        "NIFTY50 VALUE 20 (smart-beta value)": index_monthly("NIFTY50 VALUE 20", rebal),
        "NIFTY MIDCAP 150 (mid beta)": index_monthly("NIFTY MIDCAP 150", rebal),
        "NIFTY 500 (broad beta)": index_monthly("NIFTY 500", rebal),
    }

    print("\n" + "=" * 82)
    print(f"SMART-BETA REALITY CHECK — {rebal[0].date()}→{rebal[-1].date()} ({len(comp)} months, 2021+)")
    print("in-sample, bull-heavy window — read the SPREADS, not the absolute levels")
    print("=" * 82)
    print(f"{'series':<40}{'ann%':>8}{'vol%':>7}{'Sharpe':>8}{'maxDD%':>8}")
    print("-" * 82)
    for name, s in series.items():
        st = stats(s)
        print(f"{name:<40}{st['ann']:>8.1f}{st['vol']:>7.1f}{st['sharpe']:>8.2f}{st['maxdd']:>8.1f}")

    print("-" * 82)
    print("OUR composite EXCESS (alpha) vs each smart-beta / index comparator:")
    for name, s in series.items():
        if name.startswith("OUR"):
            continue
        ex_ann, ir = info_ratio(comp, s)
        print(f"  vs {name:<40} {ex_ann:>+7.1f}%/yr   info-ratio {ir:>5.2f}")
    print("-" * 82)
    print("READ: our DIY book only earns its keep if the EXCESS over the best BUYABLE")
    print("smart-beta option is positive AND has a healthy info-ratio (>~0.5). If the excess")
    print("is thin or the info-ratio is low, the rational move is to buy the factor ETF and")
    print("skip the pipeline/key-person risk. Excess over broad/mid beta = the factor premium")
    print("anyone can buy; excess over the ETF proxy = our TRUE marginal edge.")


if __name__ == "__main__":
    main()
