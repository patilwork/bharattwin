#!/usr/bin/env python3
"""
TIER 1.2 — Trend / tail-risk overlay to cut the -25/-40% drawdowns.

The validated momentum book is long-only and fully invested; it eats the full
market crash (2018 NBFC, 2020 COVID). A trend overlay de-risks when the market
trend is down. Two independent, canon-standard rules, tested on the SAME
survivorship-aware momentum top-quintile book over 2017-2026 (the window that
actually contains the crashes worth cutting):

  A. MARKET timing (time-series momentum / GVMT style). Build an equal-weight
     universe price index; when it closes below its 200-day moving average at a
     rebalance date, scale book exposure down (to cash, and a milder half-in
     variant). This is the classic absolute-momentum "risk-off" switch.

  B. STOCK trend filter. Only hold a momentum winner if it is itself above its
     own 200-day MA at t; names below MA are dropped to cash. Removes falling
     knives from the winner basket without any market call.

  C. Both combined.

The bar: a good overlay cuts max drawdown MATERIALLY while keeping most of the
return, i.e. it RAISES risk-adjusted return (Sharpe / return-per-unit-drawdown).
If it only shaves return without cutting drawdown, it is market-timing noise.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/tier1_trend_overlay.py
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
LIVE_DAYS = 15
MA_DAYS = 200
START = "2017-01-31"


def market_index(px: pd.DataFrame) -> pd.Series:
    """Equal-weight daily total-return index of the liquid universe (crude Nifty proxy)."""
    dret = px.pct_change()
    # only count names actually trading that day; winsorise daily to kill data spikes
    mkt = dret.clip(-0.20, 0.20).mean(axis=1, skipna=True)
    return (1 + mkt.fillna(0)).cumprod()


def run(px: pd.DataFrame):
    idx = market_index(px)
    idx_ma = idx.rolling(MA_DAYS, min_periods=100).mean()

    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    idx_at = idx.reindex(idx.index.union(rebal)).ffill().reindex(rebal)
    idxma_at = idx_ma.reindex(idx_ma.index.union(rebal)).ffill().reindex(rebal)
    # each name's own 200d MA, sampled at rebalance dates
    own_ma = pxf.rolling(MA_DAYS, min_periods=100).mean()
    own_ma_at = own_ma.reindex(own_ma.index.union(rebal)).ffill().reindex(rebal)

    rows = []   # per rebalance: base gross, market risk-on flag, stock-filtered gross
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        winlive = px.loc[t - pd.Timedelta(days=LIVE_DAYS):t]
        live = winlive.columns[winlive.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        live = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1].reindex(live) / price_t.reindex(live) - 1.0).clip(*FWD_WINSOR)

        seg = px.loc[:t].iloc[-252:-21]
        mom = seg.apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1
                        if c.dropna().shape[0] > 200 else np.nan).reindex(live)
        d = pd.concat([mom.rename("m"), fwd.rename("f")], axis=1).dropna()
        if len(d) < MIN_NAMES:
            continue
        k_top = max(len(d) // NQ, 1)
        winners = d["m"].sort_values(ascending=False).head(k_top).index

        base_gross = d["f"].reindex(winners).mean()

        # stock trend filter: keep only winners above their own 200d MA at t
        above = own_ma_at.loc[t].reindex(winners)
        keep = [s for s in winners if price_t.get(s, np.nan) >= above.get(s, np.inf)]
        # names filtered out sit in cash (0 return); exposure = kept / total winners
        stock_gross = (d["f"].reindex(keep).sum() / len(winners)) if len(winners) else 0.0

        risk_on = bool(idx_at.loc[t] >= idxma_at.loc[t]) if pd.notna(idxma_at.loc[t]) else True
        rows.append({"t1": t1, "base": base_gross, "stock": stock_gross, "risk_on": risk_on,
                     "kept_frac": len(keep) / len(winners) if len(winners) else 0.0})

    df = pd.DataFrame(rows).set_index("t1")
    # apply per-strategy turnover cost approximately as flat round-trip on the base
    df["base"] -= COST
    df["stock"] -= COST
    return df


def metrics(r: pd.Series) -> dict:
    r = r.dropna()
    if len(r) < 6:
        return {}
    eq = (1 + r).cumprod()
    dd = (eq / eq.cummax() - 1).min()
    return {
        "ann": ((1 + r).prod() ** (12 / len(r)) - 1) * 100,
        "sharpe": r.mean() / r.std() * np.sqrt(12) if r.std() else np.nan,
        "maxdd": dd * 100,
        "worst": r.min() * 100,
        "calmar": (((1 + r).prod() ** (12 / len(r)) - 1)) / abs(dd) if dd else np.nan,
    }


def main():
    print("Loading Dawn prices (2016-2026, split/bonus adjusted) ...")
    px, _ = load()
    df = run(px)

    base = df["base"]
    # A. market timing: cash when risk_off (full de-risk, and half-in variant)
    mkt_full = df["base"].where(df["risk_on"], 0.0)
    mkt_half = df["base"].where(df["risk_on"], df["base"] * 0.5)
    # B. stock trend filter
    stock = df["stock"]
    # C. both: market switch applied on top of the stock-filtered book
    both = df["stock"].where(df["risk_on"], 0.0)

    variants = [
        ("base (fully invested)", base),
        ("A. market-MA timing (to cash)", mkt_full),
        ("A. market-MA timing (half-in)", mkt_half),
        ("B. stock own-MA filter", stock),
        ("C. market + stock combined", both),
    ]

    print("\n" + "=" * 92)
    print(f"TIER 1.2  TREND / TAIL-RISK OVERLAY  —  {df.index[0].date()}→{df.index[-1].date()} "
          f"({len(df)} months, incl. 2018 & 2020 crashes)")
    print("base = survivorship-aware momentum top-quintile long-only, turnover-costed")
    print("=" * 92)
    print(f"{'variant':<34}{'ann%':>8}{'Sharpe':>8}{'maxDD%':>9}{'worst%':>8}{'Calmar':>8}")
    print("-" * 92)
    for name, r in variants:
        mo = metrics(r)
        if mo:
            print(f"{name:<34}{mo['ann']:>8.1f}{mo['sharpe']:>8.2f}{mo['maxdd']:>9.1f}"
                  f"{mo['worst']:>8.1f}{mo['calmar']:>8.2f}")
    frac_on = df["risk_on"].mean() * 100
    print("-" * 92)
    print(f"market risk-on {frac_on:.0f}% of months | stock filter keeps "
          f"{df['kept_frac'].mean()*100:.0f}% of winners on average")
    print("READ: adopt the overlay that RAISES Calmar (return/maxDD) and Sharpe while cutting")
    print("maxDD. A big drawdown cut with only a small return give-up = keep. If Sharpe AND")
    print("Calmar both fall, the timing is destroying more than it saves — reject.")


if __name__ == "__main__":
    main()
