#!/usr/bin/env python3
"""
3-year month-by-month track record — what the live strategy would have produced.

Simulates the ACTUAL tradeable book (momentum+value composite, ₹2000cr+ universe,
top-25 equal-weight, monthly, turnover-costed — the same construction as the live
paper books) walk-forward for every month over the last ~3 years, PIT (each month's
picks use only prior data). Benchmarked against NIFTY 500 TRI (a buyable passive).

Outputs the monthly ledger, cumulative equity on ₹10L, a drawdown profile, per-year
breakdown, and summary stats. JSON via --json (used to render the equity curve).

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/track_record.py [--json]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from xsection_montecarlo import load, pit, DAWN_URL
from src.costs import round_trip_cost

MIN_PRICE, MIN_MCAP_CR, MIN_NAMES, NQ, MAX_HOLD = 10.0, 2000.0, 30, 5, 25
FWD_WINSOR = (-0.40, 0.80)
COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025
CAPITAL = 1_000_000.0
YEARS_BACK = 3


def _z(s):
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def track(px, f):
    m = px.resample("ME").last().index
    end = px.index.max()
    start = end - pd.DateOffset(years=YEARS_BACK) - pd.DateOffset(months=1)
    rebal = m[(m >= start) & (m <= end)]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    bvps, eps, shr = (pit(f, c, rebal) for c in ["bvps", "ttm_eps", "shares_outstanding"])

    rows, prev = [], set()
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        win = px.loc[t - pd.Timedelta(days=15):t]
        live = win.columns[win.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        mcap = (price_t.reindex(live) * shr.loc[t].reindex(live) / 1e7).dropna()
        liquid = mcap[(price_t.reindex(mcap.index) >= MIN_PRICE) & (mcap >= MIN_MCAP_CR)].index
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
        k = min(max(len(comp) // NQ, 1), MAX_HOLD)
        hold = set(comp.sort_values(ascending=False).head(k).index)
        turn = 1.0 - len(hold & prev) / len(hold) if prev else 1.0
        r = fwd.reindex(list(hold)).mean() - turn * COST
        rows.append({"month": t1, "strat": float(r), "n": len(hold), "turnover": turn})
        prev = hold

    df = pd.DataFrame(rows).set_index("month")
    df["nifty500"] = nifty500(df.index)
    return df.dropna(subset=["nifty500"])


def nifty500(idx):
    eng = create_engine(DAWN_URL)
    n = pd.read_sql(text("SELECT date, tri FROM benchmark_tri_daily WHERE index_name='NIFTY 500'"), eng)
    eng.dispose()
    n["date"] = pd.to_datetime(n["date"])
    s = n.set_index("date")["tri"].sort_index()
    at = s.reindex(s.index.union(idx)).ffill().reindex(idx)
    return at.pct_change().reindex(idx)


def summarize(r, label):
    n = len(r)
    eq = (1 + r).cumprod()
    dd = (eq / eq.cummax() - 1)
    return {
        "label": label, "months": n,
        "total_pct": (eq.iloc[-1] - 1) * 100,
        "cagr": (eq.iloc[-1] ** (12 / n) - 1) * 100,
        "vol": r.std() * np.sqrt(12) * 100,
        "sharpe": r.mean() / r.std() * np.sqrt(12) if r.std() else np.nan,
        "maxdd": dd.min() * 100,
        "best": r.max() * 100, "worst": r.min() * 100,
        "pos_months": (r > 0).mean() * 100,
    }


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    print("Loading Dawn ...", file=sys.stderr)
    px, f = load()
    df = track(px, f)
    df["excess"] = df["strat"] - df["nifty500"]
    df["cum_strat"] = (1 + df["strat"]).cumprod()
    df["cum_nifty"] = (1 + df["nifty500"]).cumprod()
    df["equity"] = CAPITAL * df["cum_strat"]
    df["dd"] = df["cum_strat"] / df["cum_strat"].cummax() - 1

    if args.json:
        out = {"months": [{"m": d.strftime("%Y-%m"), "strat": round(row.strat*100, 2),
                           "nifty": round(row.nifty500*100, 2), "cum_strat": round(row.cum_strat, 4),
                           "cum_nifty": round(row.cum_nifty, 4), "equity": round(row.equity, 0),
                           "dd": round(row.dd*100, 2)} for d, row in df.iterrows()],
               "strat": summarize(df["strat"], "strategy"), "nifty": summarize(df["nifty500"], "nifty500")}
        print(json.dumps(out, default=str)); return

    print("\n" + "=" * 78)
    print(f"3-YEAR TRACK RECORD — {df.index[0].strftime('%b %Y')}→{df.index[-1].strftime('%b %Y')} "
          f"({len(df)} months) | momentum+value, ₹2000cr+, top-25, monthly, costed")
    print("=" * 78)
    print(f"{'month':<9}{'strat%':>8}{'nifty%':>8}{'excess%':>9}{'equity ₹':>13}{'drawdn%':>9}")
    print("-" * 78)
    for d, row in df.iterrows():
        print(f"{d.strftime('%Y-%m'):<9}{row.strat*100:>8.2f}{row.nifty500*100:>8.2f}"
              f"{row.excess*100:>9.2f}{row.equity:>13,.0f}{row.dd*100:>9.1f}")

    print("-" * 78)
    print("PER CALENDAR YEAR (strategy vs Nifty 500):")
    for yr, g in df.groupby(df.index.year):
        s = (1 + g["strat"]).prod() - 1; nf = (1 + g["nifty500"]).prod() - 1
        print(f"  {yr}: strat {s*100:>+7.1f}%   nifty500 {nf*100:>+7.1f}%   excess {(s-nf)*100:>+7.1f}%  ({len(g)}mo)")

    print("-" * 78)
    st, nf = summarize(df["strat"], "strategy"), summarize(df["nifty500"], "nifty500")
    print(f"{'':<14}{'STRATEGY':>12}{'NIFTY 500':>12}")
    for k, lab in [("total_pct", "total return"), ("cagr", "CAGR"), ("vol", "volatility"),
                   ("sharpe", "Sharpe"), ("maxdd", "max drawdown"), ("pos_months", "% up months")]:
        suf = "" if k == "sharpe" else "%"
        print(f"  {lab:<12}{st[k]:>11.2f}{suf}{nf[k]:>11.2f}{suf}")
    print(f"\n  ₹10,00,000 → ₹{df['equity'].iloc[-1]:,.0f}  (strategy)   vs   "
          f"₹{CAPITAL*df['cum_nifty'].iloc[-1]:,.0f}  (Nifty 500)")
    print(f"  hit rate: {(df['excess'] > 0).mean()*100:.0f}% of months beat Nifty 500 | "
          f"best {st['best']:+.1f}% / worst {st['worst']:+.1f}%")
    print("-" * 78)
    print("CAVEAT: PIT walk-forward but the spec was chosen on this history (see")
    print("oos_walkforward for the frozen-cutoff test). In-sample returns run hot vs live")
    print("(~4-6% forward alpha is the honest estimate, not the headline CAGR here).")


if __name__ == "__main__":
    main()
