#!/usr/bin/env python3
"""
Monte Carlo + multi-strategy robustness panel for the cross-sectional sleeve.

Answers two questions honestly:
  1. Monte Carlo — turn the backtest's point estimates into DISTRIBUTIONS.
     Block-bootstrap each strategy's monthly long-only return series -> CI on
     annualised Sharpe / CAGR / max-drawdown, and P(annual return < 0).
     Permutation null test -> a real p-value for each factor's IC.
  2. Multiple strategies — a PRE-SPECIFIED panel (momentum-only, value_ey-only,
     value_pb-only, composite) scored side by side. Not a hunt for the best
     looking one; the point is which constructions are robust vs fragile.

Same data + factor logic as scripts/xsection_backtest.py (Dawn panel, adjusted
prices, PIT-lagged fundamentals, net of cost). Seeded for reproducibility.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/xsection_montecarlo.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.costs import round_trip_cost

DAWN_URL = os.environ.get("DAWN_URL", "postgresql://localhost:5432/dawn")
REPORT_LAG_DAYS, MIN_PRICE, MIN_NAMES, NQ = 90, 10.0, 30, 5
FWD_WINSOR = (-0.40, 0.80)
COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025   # round-trip + impact proxy
N_BOOT, BLOCK, N_PERM = 3000, 3, 1000
RNG = np.random.default_rng(42)


def load():
    eng = create_engine(DAWN_URL)
    px = pd.read_sql(text("SELECT symbol,date,close FROM stock_prices_daily WHERE symbol IS NOT NULL AND close>0"), eng)
    px["date"] = pd.to_datetime(px["date"])
    px = px[px["symbol"].str.match(r"^[A-Z][A-Z0-9&-]{1,}$", na=False)]
    px = px.pivot_table(index="date", columns="symbol", values="close", aggfunc="last").sort_index()
    ca = pd.read_sql(text("""SELECT symbol,ex_date,ca_type,ratio_from,ratio_to FROM corporate_actions
                             WHERE ca_type IN ('split','bonus') AND ratio_from IS NOT NULL AND ratio_to IS NOT NULL"""), eng)
    ca["ex_date"] = pd.to_datetime(ca["ex_date"])
    f = pd.read_sql(text("""SELECT symbol,period_end,basis,ttm_eps,bvps,shares_outstanding
                            FROM security_fundamentals WHERE symbol ~ '^[A-Z][A-Z0-9&-]{1,}$'"""), eng)
    eng.dispose()
    for _, r in ca.iterrows():
        s = r["symbol"]
        if s in px.columns and r["ratio_from"] and r["ratio_to"]:
            fac = (r["ratio_to"]/r["ratio_from"]) if r["ca_type"]=="split" else (r["ratio_to"]/(r["ratio_from"]+r["ratio_to"]))
            if 0 < fac < 1.0:
                px.loc[px.index < r["ex_date"], s] *= fac
    f["period_end"] = pd.to_datetime(f["period_end"])
    f["available"] = f["period_end"] + pd.Timedelta(days=REPORT_LAG_DAYS)
    f["basis_rank"] = (f["basis"] == "consolidated").astype(int)
    f = f.sort_values(["symbol", "available", "basis_rank"])
    return px, f


def pit(f, field, rebal):
    sub = f.dropna(subset=[field])
    w = sub.pivot_table(index="available", columns="symbol", values=field, aggfunc="last").sort_index()
    return w.reindex(w.index.union(rebal)).ffill().reindex(rebal)


def z(s):
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def build_returns():
    """Return {strategy: monthly net long-only return series} + {factor: IC list}
       + the per-rebalance (factor, fwd) pairs for permutation testing."""
    px, f = load()
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp("2021-06-30")) & (m <= px.index.max())]
    pxm = px.reindex(px.index.union(rebal)).ffill().reindex(rebal)
    bvps, eps = pit(f, "bvps", rebal), pit(f, "ttm_eps", rebal)

    strats = ["momentum", "value_ey", "value_pb", "composite"]
    ret = {k: [] for k in strats}
    ic = {k: [] for k in ["momentum", "value_ey", "value_pb"]}
    perm_pairs = {k: [] for k in ["momentum", "value_ey", "value_pb"]}

    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        price_t = pxm.loc[t]
        liquid = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1] / pxm.loc[t] - 1.0).clip(*FWD_WINSOR).reindex(liquid)
        win = px.loc[:t]
        mom = win.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1 if c.dropna().shape[0] > 200 else np.nan) if len(win) > 260 else pd.Series(dtype=float)
        raw = {
            "momentum": mom.reindex(liquid),
            "value_ey": (eps.loc[t] / price_t).reindex(liquid),
            "value_pb": (-(price_t / bvps.loc[t])).reindex(liquid),
        }
        zz = {k: z(v) for k, v in raw.items()}
        comp = pd.concat(list(zz.values()), axis=1).mean(axis=1, skipna=True)

        def topret(score):
            v = pd.concat([score, fwd], axis=1).dropna()
            if len(v) < MIN_NAMES:
                return np.nan
            q = pd.qcut(v.iloc[:, 0].rank(method="first"), NQ, labels=False)
            return v.iloc[:, 1][q == NQ - 1].mean() - COST  # long-only, one round-trip

        for k in ["momentum", "value_ey", "value_pb"]:
            ret[k].append(topret(raw[k]))
            v = pd.concat([raw[k], fwd], axis=1).dropna()
            if len(v) >= MIN_NAMES:
                ic[k].append(v.iloc[:, 0].rank().corr(v.iloc[:, 1].rank()))
                perm_pairs[k].append((v.iloc[:, 0].values, v.iloc[:, 1].values))
        ret["composite"].append(topret(comp))
    return {k: pd.Series(v).dropna() for k, v in ret.items()}, ic, perm_pairs


def block_bootstrap(r, n_boot, block):
    """Block bootstrap the monthly return series -> arrays of annualised
    Sharpe, CAGR%, and max drawdown%."""
    r = r.values
    n = len(r)
    n_blocks = int(np.ceil(n / block))
    shp, cagr, mdd = [], [], []
    for _ in range(n_boot):
        starts = RNG.integers(0, n, n_blocks)
        samp = np.concatenate([r[s:s + block] for s in starts])[:n]
        mu, sd = samp.mean(), samp.std()
        shp.append(mu / sd * np.sqrt(12) if sd else np.nan)
        cagr.append(((1 + samp).prod() ** (12 / n) - 1) * 100)
        eq = (1 + samp).cumprod()
        mdd.append((eq / np.maximum.accumulate(eq) - 1).min() * 100)
    return np.array(shp), np.array(cagr), np.array(mdd)


def permutation_ic_pvalue(pairs, observed_mean_ic, n_perm):
    """Null: factor has no cross-sectional info. Shuffle fwd returns within each
    cross-section, recompute mean IC. p = P(null mean IC >= observed)."""
    from scipy.stats import rankdata
    null = []
    for _ in range(n_perm):
        ics = []
        for fac, fwd in pairs:
            shuf = RNG.permutation(fwd)
            rf, rs = rankdata(fac), rankdata(shuf)
            ics.append(np.corrcoef(rf, rs)[0, 1])
        null.append(np.nanmean(ics))
    null = np.array(null)
    return float((null >= observed_mean_ic).mean()), null


def main():
    print("Building strategy return series from Dawn panel ...")
    rets, ic, perm = build_returns()

    print("\n" + "=" * 88)
    print("MONTE CARLO + MULTI-STRATEGY ROBUSTNESS PANEL")
    print(f"block bootstrap: {N_BOOT} draws, block={BLOCK} | permutation: {N_PERM} | seed 42")
    print("=" * 88)
    hdr = f"{'strategy':<11} {'pt_Sh':>6} {'Sh_med':>7} {'Sh_5%':>7} {'Sh_95%':>7} {'CAGR_med':>9} {'CAGR_5%':>8} {'mDD_med':>8} {'P(ann<0)':>9}"
    print(hdr); print("-" * 88)
    for k in ["momentum", "value_ey", "value_pb", "composite"]:
        r = rets[k]
        pt_sh = r.mean() / r.std() * np.sqrt(12) if r.std() else np.nan
        sh, cg, dd = block_bootstrap(r, N_BOOT, BLOCK)
        p_neg = (cg < 0).mean()
        print(f"{k:<11} {pt_sh:>6.2f} {np.nanmedian(sh):>7.2f} {np.nanpercentile(sh,5):>7.2f} "
              f"{np.nanpercentile(sh,95):>7.2f} {np.nanmedian(cg):>9.1f} {np.nanpercentile(cg,5):>8.1f} "
              f"{np.nanmedian(dd):>8.1f} {p_neg:>9.1%}")
    print("-" * 88)

    print("\nPERMUTATION NULL TEST — is each factor's IC real or luck?")
    print(f"{'factor':<11} {'obs_meanIC':>11} {'null_mean':>10} {'null_95%':>9} {'p-value':>9}")
    print("-" * 88)
    for k in ["momentum", "value_ey", "value_pb"]:
        obs = float(np.nanmean(ic[k]))
        p, null = permutation_ic_pvalue(perm[k], obs, N_PERM)
        print(f"{k:<11} {obs:>11.4f} {null.mean():>10.4f} {np.nanpercentile(null,95):>9.4f} {p:>9.4f}")
    print("-" * 88)
    print("READ: Sh_5% > 0 => strategy survives even at the pessimistic tail of the bootstrap.")
    print("P(ann<0) = bootstrap probability of a losing year. Permutation p<0.05 => IC unlikely luck.")
    print("Caveats unchanged: survivorship, ~5yr window, no volume/ADV impact model.")


if __name__ == "__main__":
    main()
