#!/usr/bin/env python3
"""
Long-horizon track record — the tradeable book walked forward as far as the data
honestly allows, benchmarked against real buyable indices, with every rupee of
transaction cost itemised.

This is track_record.py extended along three axes:
  1. HORIZON. track_record hard-codes YEARS_BACK=3. Here the start is discovered:
     we walk back to the first month the composite can actually be formed. Prices
     reach 2016-01, but the VALUE leg needs fundamentals — ttm_eps starts 2018-03-31
     (109 symbols) and only reaches breadth in 2019 — so the composite cannot be
     run for 10 years. --min-names guards the tail; --momentum-only runs the price-
     only book, which CAN go back ~10y, for context.
  2. INDEX COMPARISON. NIFTY 500 TRI and NIFTY 50 TRI, both from benchmark_tri_daily.
     That table currently ends 2026-06-18, so recent months have NO index. We mark
     them NaN rather than forward-filling a stale level into a live comparison.
  3. COSTS. track_record collapses cost into one 49.6bps constant. Here each
     rebalance's traded notional is run through src.costs.round_trip_cost so STT,
     stamp duty, exchange, SEBI, brokerage, GST and slippage are tracked in rupees,
     plus a separate impact proxy. Equity compounds, so cost scales with the book.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/long_track.py
  ... --capital 1000000          # starting figure (default ₹10L)
  ... --years 10                 # requested horizon (clipped to what data supports)
  ... --momentum-only            # drop the value legs (no fundamentals => longer history)
  ... --brokerage-pct 0.0003 --brokerage-cap 20   # model a broker that charges
  ... --impact-bps 25            # market-impact proxy on top of modelled slippage
  ... --json out.json            # machine-readable (feeds the FRIDAY dashboard)
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from xsection_montecarlo import load, pit, DAWN_URL          # noqa: E402
from src import costs as costmod                              # noqa: E402

MIN_PRICE, MIN_MCAP_CR, MIN_NAMES, NQ, MAX_HOLD = 10.0, 2000.0, 30, 5, 25
FWD_WINSOR = (-0.40, 0.80)
BENCHMARKS = ["NIFTY 500", "NIFTY 50"]
INDEX_STALE_TOL_DAYS = 7      # a month-end more than this past the index's last date has no benchmark


def _z(s):
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def cost_for(notional: float, seg: str, impact_bps: float, slippage_bps: float) -> dict:
    """Itemised round-trip cost on `notional` of traded value, in rupees."""
    if notional <= 0:
        return {k: 0.0 for k in ("stt", "exchange", "sebi", "stamp", "brokerage",
                                 "gst", "slippage", "impact", "total")}
    c = costmod.round_trip_cost(notional, seg, slippage_bps=slippage_bps)
    impact = notional * (impact_bps / 1e4) * 2      # both legs, same convention as slippage
    return {"stt": c.stt, "exchange": c.exchange, "sebi": c.sebi, "stamp": c.stamp,
            "brokerage": c.brokerage, "gst": c.gst, "slippage": c.slippage,
            "impact": round(impact, 2), "total": round(c.total + impact, 2)}


def benchmarks(idx: pd.DatetimeIndex) -> pd.DataFrame:
    """Monthly TRI returns per index, NaN where the index has no data for that
    month-end (rather than a forward-filled stale level)."""
    eng = create_engine(DAWN_URL)
    n = pd.read_sql(text("SELECT index_name, date, tri FROM benchmark_tri_daily "
                         "WHERE index_name = ANY(:b)"), eng, params={"b": BENCHMARKS})
    eng.dispose()
    n["date"] = pd.to_datetime(n["date"])
    out = {}
    for name, g in n.groupby("index_name"):
        s = g.set_index("date")["tri"].sort_index()
        last = s.index.max()
        at = s.reindex(s.index.union(idx)).ffill().reindex(idx)
        at[idx > last + pd.Timedelta(days=INDEX_STALE_TOL_DAYS)] = np.nan
        out[name] = at.pct_change().reindex(idx)
    return pd.DataFrame(out, index=idx)


def track(px, f, years, capital, momentum_only, seg, impact_bps, slippage_bps):
    m = px.resample("ME").last().index
    end = px.index.max()
    start = end - pd.DateOffset(years=years) - pd.DateOffset(months=1)
    rebal = m[(m >= start) & (m <= end)]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    bvps, eps, shr = (pit(f, c, rebal) for c in ["bvps", "ttm_eps", "shares_outstanding"])

    rows, prev, equity, skipped = [], set(), float(capital), []
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        win = px.loc[t - pd.Timedelta(days=15):t]
        live = win.columns[win.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        shr_t = shr.loc[t].reindex(live) if t in shr.index else pd.Series(dtype=float)
        mcap = (price_t * shr_t / 1e7).dropna()
        liquid = mcap[(price_t.reindex(mcap.index) >= MIN_PRICE) & (mcap >= MIN_MCAP_CR)].index
        if len(liquid) < MIN_NAMES:
            skipped.append((t.strftime("%Y-%m"), f"universe {len(liquid)}<{MIN_NAMES}")); continue
        fwd = (pxm.loc[t1].reindex(liquid) / price_t.reindex(liquid) - 1.0).clip(*FWD_WINSOR)

        seg_px = px.loc[:t].iloc[-252:-21]
        mom = seg_px.apply(lambda c: c.dropna().iloc[-1] / c.dropna().iloc[0] - 1
                           if c.dropna().shape[0] > 200 else np.nan).reindex(liquid)
        legs = [_z(mom)]
        if not momentum_only:
            legs.append(_z((eps.loc[t] / price_t).reindex(liquid)))
            legs.append(_z(-(price_t / bvps.loc[t]).reindex(liquid)))
        comp = pd.concat(legs, axis=1).mean(axis=1, skipna=True)
        comp = comp[comp.index.isin(fwd.dropna().index)].dropna()
        if len(comp) < MIN_NAMES:
            skipped.append((t.strftime("%Y-%m"), f"scored {len(comp)}<{MIN_NAMES}")); continue

        k = min(max(len(comp) // NQ, 1), MAX_HOLD)
        hold = set(comp.sort_values(ascending=False).head(k).index)
        buys, sells = sorted(hold - prev), sorted(prev - hold)
        turn = 1.0 - len(hold & prev) / len(hold) if prev else 1.0

        traded = equity * turn                       # ₹ sold and re-bought this rebalance
        cb = cost_for(traded, seg, impact_bps, slippage_bps)
        gross = float(fwd.reindex(list(hold)).mean())
        cost_pct = cb["total"] / equity if equity else 0.0
        net = gross - cost_pct
        equity *= (1 + net)

        rows.append({"month": t1, "strat": net, "gross": gross, "n": len(hold),
                     "turnover": turn, "traded": traded, "cost": cb["total"],
                     "cost_pct": cost_pct, "equity": equity,
                     "buys": buys, "sells": sells, "cb": cb})
        prev = hold

    if not rows:
        raise SystemExit("no valid rebalances — check data coverage")
    df = pd.DataFrame(rows).set_index("month")
    bm = benchmarks(df.index)
    for c in bm.columns:
        df[c] = bm[c]
    return df, skipped


def summarize(r: pd.Series, label: str) -> dict:
    r = r.dropna()
    n = len(r)
    if not n:
        return {"label": label, "months": 0}
    eq = (1 + r).cumprod()
    dd = eq / eq.cummax() - 1
    return {"label": label, "months": n,
            "total_pct": (eq.iloc[-1] - 1) * 100,
            "cagr": (eq.iloc[-1] ** (12 / n) - 1) * 100,
            "vol": r.std() * np.sqrt(12) * 100,
            "sharpe": r.mean() / r.std() * np.sqrt(12) if r.std() else np.nan,
            "maxdd": dd.min() * 100,
            "best": r.max() * 100, "worst": r.min() * 100,
            "pos_months": (r > 0).mean() * 100}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, default=1_000_000.0)
    ap.add_argument("--years", type=int, default=10)
    ap.add_argument("--momentum-only", action="store_true")
    ap.add_argument("--segment", default="equity_delivery")
    ap.add_argument("--brokerage-pct", type=float, default=None)
    ap.add_argument("--brokerage-cap", type=float, default=None)
    ap.add_argument("--impact-bps", type=float, default=25.0)
    ap.add_argument("--slippage-bps", type=float, default=1.5)
    ap.add_argument("--json", default=None, help="write JSON to this path")
    args = ap.parse_args()

    # optional broker override — default equity_delivery brokerage is 0 (free delivery)
    if args.brokerage_pct is not None or args.brokerage_cap is not None:
        r = costmod.RATES[args.segment]
        costmod.RATES[args.segment] = replace(
            r,
            brokerage_pct=args.brokerage_pct if args.brokerage_pct is not None else r.brokerage_pct,
            brokerage_cap=args.brokerage_cap if args.brokerage_cap is not None else r.brokerage_cap)

    print("Loading Dawn panel ...", file=sys.stderr)
    px, f = load()
    df, skipped = track(px, f, args.years, args.capital, args.momentum_only,
                        args.segment, args.impact_bps, args.slippage_bps)

    df["cum_strat"] = (1 + df["strat"]).cumprod()
    df["dd"] = df["cum_strat"] / df["cum_strat"].cummax() - 1
    for b in BENCHMARKS:
        df["cum_" + b] = (1 + df[b].fillna(0)).cumprod()

    unit = costmod.round_trip_cost(1_000_000, args.segment, slippage_bps=args.slippage_bps)
    rt_bps = unit.total_bps + args.impact_bps * 2
    tot = {k: float(sum(r["cb"][k] for _, r in df.iterrows()))
           for k in ("stt", "exchange", "sebi", "stamp", "brokerage", "gst", "slippage", "impact", "total")}

    if args.json:
        out = {
            "meta": {"start": df.index[0].strftime("%Y-%m"), "end": df.index[-1].strftime("%Y-%m"),
                     "months": len(df), "capital": args.capital,
                     "strategy": "momentum" if args.momentum_only else "momentum+value",
                     "segment": args.segment, "impact_bps": args.impact_bps,
                     "slippage_bps": args.slippage_bps, "round_trip_bps": round(rt_bps, 2),
                     "skipped": skipped},
            "months": [{"m": d.strftime("%Y-%m"), "strat": round(r.strat * 100, 3),
                        "gross": round(r.gross * 100, 3),
                        "n500": None if pd.isna(r["NIFTY 500"]) else round(r["NIFTY 500"] * 100, 3),
                        "n50": None if pd.isna(r["NIFTY 50"]) else round(r["NIFTY 50"] * 100, 3),
                        "cum_strat": round(r.cum_strat, 5),
                        "cum_n500": round(r["cum_NIFTY 500"], 5), "cum_n50": round(r["cum_NIFTY 50"], 5),
                        "equity": round(r.equity, 0), "dd": round(r.dd * 100, 2),
                        "turnover": round(r.turnover * 100, 1), "n": int(r.n),
                        "traded": round(r.traded, 0), "cost": round(r.cost, 0),
                        "cb": {k: round(v, 2) for k, v in r["cb"].items()},
                        "buys": r.buys, "sells": r.sells} for d, r in df.iterrows()],
            "summary": {"strat": summarize(df["strat"], "strategy"),
                        "gross": summarize(df["gross"], "strategy (gross)"),
                        "n500": summarize(df["NIFTY 500"], "NIFTY 500"),
                        "n50": summarize(df["NIFTY 50"], "NIFTY 50")},
            "costs": tot,
        }
        Path(args.json).write_text(json.dumps(out, default=str))
        print(f"wrote {args.json}", file=sys.stderr)

    W = 92
    print("\n" + "=" * W)
    print(f"LONG TRACK RECORD — {df.index[0]:%b %Y} → {df.index[-1]:%b %Y} ({len(df)} months) | "
          f"{'momentum' if args.momentum_only else 'momentum+value'}, ₹{MIN_MCAP_CR:.0f}cr+, top-{MAX_HOLD}, monthly")
    print(f"cost model: {args.segment} round-trip {rt_bps:.1f}bps "
          f"(STT+stamp+exch+SEBI+GST+brokerage {unit.total_bps - args.slippage_bps*2:.1f} + "
          f"slippage {args.slippage_bps*2:.1f} + impact {args.impact_bps*2:.1f})")
    print("=" * W)
    print(f"{'month':<9}{'net%':>7}{'N500%':>8}{'N50%':>8}{'equity ₹':>14}{'dd%':>7}{'turn%':>7}{'cost ₹':>10}")
    print("-" * W)
    for d, r in df.iterrows():
        n500 = "   n/a" if pd.isna(r["NIFTY 500"]) else f"{r['NIFTY 500']*100:>7.2f}"
        n50 = "   n/a" if pd.isna(r["NIFTY 50"]) else f"{r['NIFTY 50']*100:>7.2f}"
        print(f"{d:%Y-%m}  {r.strat*100:>6.2f}{n500:>8}{n50:>8}{r.equity:>14,.0f}"
              f"{r.dd*100:>7.1f}{r.turnover*100:>7.0f}{r.cost:>10,.0f}")

    print("-" * W)
    print("PER CALENDAR YEAR:")
    for yr, g in df.groupby(df.index.year):
        s = (1 + g["strat"]).prod() - 1
        b5 = (1 + g["NIFTY 500"].dropna()).prod() - 1 if g["NIFTY 500"].notna().any() else np.nan
        bs = f"{b5*100:>+7.1f}%" if not pd.isna(b5) else "    n/a"
        ex = f"{(s-b5)*100:>+7.1f}%" if not pd.isna(b5) else "    n/a"
        print(f"  {yr}: strat {s*100:>+7.1f}%   nifty500 {bs}   excess {ex}  ({len(g)}mo)")

    print("-" * W)
    st = summarize(df["strat"], "strategy"); gr = summarize(df["gross"], "gross")
    b5 = summarize(df["NIFTY 500"], "n500"); b1 = summarize(df["NIFTY 50"], "n50")
    print(f"{'':<16}{'STRAT(net)':>12}{'STRAT(gross)':>14}{'NIFTY 500':>12}{'NIFTY 50':>12}")
    for k, lab in [("total_pct", "total return"), ("cagr", "CAGR"), ("vol", "volatility"),
                   ("sharpe", "Sharpe"), ("maxdd", "max drawdown"), ("pos_months", "% up months")]:
        sfx = "" if k == "sharpe" else "%"
        def cell(d):
            return f"{d[k]:>11.2f}{sfx}" if d.get("months") else f"{'n/a':>12}"
        print(f"  {lab:<14}{cell(st)}{cell(gr):>14}{cell(b5)}{cell(b1)}")
    print(f"  benchmark months available: NIFTY 500 {b5.get('months',0)}/{len(df)}, "
          f"NIFTY 50 {b1.get('months',0)}/{len(df)}")

    print("-" * W)
    print(f"TRANSACTION COSTS on ₹{args.capital:,.0f} starting capital, {len(df)} rebalances:")
    for k in ("stt", "stamp", "exchange", "sebi", "brokerage", "gst", "slippage", "impact"):
        print(f"  {k:<12} ₹{tot[k]:>14,.0f}   ({tot[k]/tot['total']*100 if tot['total'] else 0:>5.1f}% of cost)")
    print(f"  {'TOTAL':<12} ₹{tot['total']:>14,.0f}")
    print(f"  traded value over the period: ₹{df['traded'].sum():,.0f} | "
          f"avg turnover {df['turnover'].mean()*100:.0f}%/month")
    fin = df["equity"].iloc[-1]
    print(f"\n  ₹{args.capital:,.0f} → ₹{fin:,.0f} (net of ₹{tot['total']:,.0f} costs)")
    if skipped:
        print(f"\n  skipped {len(skipped)} month(s) for insufficient data: "
              f"{', '.join(m for m, _ in skipped[:8])}{' ...' if len(skipped) > 8 else ''}")
    print("-" * W)
    print("CAVEAT: PIT walk-forward, but the spec was chosen on this history — in-sample")
    print("returns run hot vs live (~4-6%/yr forward alpha is the honest estimate).")


if __name__ == "__main__":
    main()
