#!/usr/bin/env python3
"""
Out-of-sample / walk-forward test — freeze the strategy at a cutoff, simulate forward.

Our monthly backtest is ALREADY walk-forward (each month's picks use only prior data,
scored on the next month's unseen return). What this adds: an explicit HOLD-OUT — the
strategy spec (momentum 12-1 + value, top-quintile, ₹2000cr+, equal-weight, monthly)
was chosen knowing the whole sample, so we freeze at a cutoff and ask "does it keep
working on the months AFTER the cutoff?"

For each cutoff we report the OOS window after it: composite vs equal-weight-universe
benchmark, excess (alpha), Sharpe, and mean rank-IC. Multiple cutoffs because a single
recent one (2026-03-31 → today) is only ~4 months = pure noise; the earlier cutoffs
give statistically meaningful forward windows so the recent one has context.

No look-ahead: every month's signal uses prices/fundamentals available at that month
only; the cutoff just marks where we start *counting* OOS performance.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/oos_walkforward.py
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
START = "2021-06-30"
CUTOFFS = ["2024-12-31", "2025-06-30", "2025-12-31", "2026-03-31"]


def _z(s):
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def series(px, f):
    """Monthly PIT walk-forward: composite net return, benchmark return, rank-IC."""
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    bvps, eps = pit(f, "bvps", rebal), pit(f, "ttm_eps", rebal)

    comp_ret, bench_ret, ic, form_date = [], [], [], []
    prev = set()
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
        d = pd.concat([comp.rename("s"), fwd.rename("f")], axis=1).dropna()
        ic.append(d["s"].rank().corr(d["f"].rank()))
        k = max(len(comp) // NQ, 1)
        hold = set(comp.sort_values(ascending=False).head(k).index)
        turn = 1.0 - len(hold & prev) / len(hold) if prev else 1.0
        comp_ret.append(fwd.reindex(list(hold)).mean() - turn * COST)
        bench_ret.append(fwd.mean())
        form_date.append(t)                    # formation month (signal date)
        prev = hold
    idx = pd.DatetimeIndex(form_date)
    return (pd.Series(comp_ret, idx), pd.Series(bench_ret, idx), pd.Series(ic, idx))


def stats(cr, br, icv):
    n = len(cr)
    if n < 2:
        return None
    ann = lambda s: ((1 + s).prod() ** (12 / n) - 1) * 100
    sh = cr.mean() / cr.std() * np.sqrt(12) if cr.std() else np.nan
    ex = cr - br
    ic_t = icv.mean() / (icv.std() / np.sqrt(n)) if n > 3 and icv.std() else np.nan
    return {"n": n, "comp": ann(cr), "bench": ann(br), "excess": ann(ex),
            "sharpe": sh, "ic": icv.mean(), "ic_t": ic_t}


def main():
    print("Loading Dawn ...")
    px, f = load()
    cr, br, icv = series(px, f)

    print("\n" + "=" * 88)
    print(f"OUT-OF-SAMPLE WALK-FORWARD — full series {cr.index[0].date()}→{cr.index[-1].date()} "
          f"({len(cr)} months)")
    print("freeze the spec at each cutoff; measure the composite on the months AFTER it")
    print("=" * 88)
    print(f"{'window':<34}{'n':>4}{'comp%':>8}{'bench%':>8}{'excess%':>9}{'Sharpe':>8}{'meanIC':>9}{'IC_t':>7}")
    print("-" * 88)
    full = stats(cr, br, icv)
    print(f"{'FULL SAMPLE (reference)':<34}{full['n']:>4}{full['comp']:>8.1f}{full['bench']:>8.1f}"
          f"{full['excess']:>9.1f}{full['sharpe']:>8.2f}{full['ic']:>9.4f}{full['ic_t']:>7.2f}")
    print("-" * 88)
    for cut in CUTOFFS:
        c = pd.Timestamp(cut)
        mask = cr.index > c
        oos = stats(cr[mask], br[mask], icv[mask])
        if not oos:
            print(f"{'OOS after ' + cut:<34}  (too few forward months)"); continue
        note = "  ← your March-2026 cutoff" if cut == "2026-03-31" else ""
        short = "  (short, noisy)" if oos["n"] < 6 else ""
        print(f"{'OOS after ' + cut:<34}{oos['n']:>4}{oos['comp']:>8.1f}{oos['bench']:>8.1f}"
              f"{oos['excess']:>9.1f}{oos['sharpe']:>8.2f}{oos['ic']:>9.4f}{oos['ic_t']:>7.2f}{note}{short}")

    print("-" * 88)
    print("READ: the edge holds OUT-OF-SAMPLE if excess>0 and mean-IC stays positive on the")
    print("months after the cutoff, at every horizon. Longer windows (2024/2025 cutoffs) are")
    print("the statistically meaningful test; the 2026-03 window is ~4 months = noise, shown")
    print("only because you asked. Consistency across cutoffs > any single number.")


if __name__ == "__main__":
    main()
