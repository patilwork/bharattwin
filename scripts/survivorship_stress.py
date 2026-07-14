#!/usr/bin/env python3
"""
Survivorship stress test — bound the bias from delisted names Dawn is missing.

Morningstar confirms ~2,784 obsolete Indian equities exist; the Dawn panel has
essentially none (it is "stocks that still trade today"). Those delisted names
are overwhelmingly penny collapses (last prices ₹0.1-5) and famous bankruptcies
(ABG Shipyard, Amtek Auto, Adhunik Metaliks) -> i.e. LOW-momentum, often CHEAP
(value-trap) names.

This test injects synthetic about-to-delist names into each monthly rebalance
and measures how much each strategy's long-only top-quintile return degrades.
Injected names are cheap (top-decile value score) and falling (bottom-decile
momentum), with a catastrophic terminal return — the realistic profile.

Expectation: value gets hit (traps look cheap -> enter its top quintile),
momentum is largely protected (collapsing names are low-momentum -> never held).
Quantifying that differential is the point.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/survivorship_stress.py
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
COST = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4 + 0.0025
DELIST_TERMINAL = -0.70          # a name in its final month before delisting
RNG = np.random.default_rng(7)


def load():
    eng = create_engine(DAWN_URL)
    px = pd.read_sql(text("SELECT symbol,date,close FROM stock_prices_daily WHERE symbol IS NOT NULL AND close>0"), eng)
    px["date"] = pd.to_datetime(px["date"])
    px = px[px["symbol"].str.match(r"^[A-Z][A-Z0-9&-]{1,}$", na=False)]
    px = px.pivot_table(index="date", columns="symbol", values="close", aggfunc="last").sort_index()
    ca = pd.read_sql(text("""SELECT symbol,ex_date,ca_type,ratio_from,ratio_to FROM corporate_actions
                             WHERE ca_type IN ('split','bonus') AND ratio_from IS NOT NULL AND ratio_to IS NOT NULL"""), eng)
    ca["ex_date"] = pd.to_datetime(ca["ex_date"])
    f = pd.read_sql(text("""SELECT symbol,period_end,basis,ttm_eps,bvps FROM security_fundamentals
                            WHERE symbol ~ '^[A-Z][A-Z0-9&-]{1,}$'"""), eng)
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
    return px, f.sort_values(["symbol", "available", "basis_rank"])


def pit(f, field, rebal):
    sub = f.dropna(subset=[field])
    w = sub.pivot_table(index="available", columns="symbol", values=field, aggfunc="last").sort_index()
    return w.reindex(w.index.union(rebal)).ffill().reindex(rebal)


def top_q_return(score: pd.Series, fwd: pd.Series) -> float:
    v = pd.concat([score, fwd], axis=1).dropna()
    if len(v) < MIN_NAMES:
        return np.nan
    q = pd.qcut(v.iloc[:, 0].rank(method="first"), NQ, labels=False)
    return v.iloc[:, 1][q == NQ - 1].mean() - COST


def run(delist_annual_rate: float) -> dict:
    px, f = load()
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp("2021-06-30")) & (m <= px.index.max())]
    pxm = px.reindex(px.index.union(rebal)).ffill().reindex(rebal)
    bvps, eps = pit(f, "bvps", rebal), pit(f, "ttm_eps", rebal)
    monthly_rate = delist_annual_rate / 12.0

    out = {k: {"base": [], "stress": []} for k in ["momentum", "value", "composite"]}
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        price_t = pxm.loc[t]
        liquid = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1] / pxm.loc[t] - 1.0).clip(*FWD_WINSOR).reindex(liquid)
        win = px.loc[:t]
        mom = win.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1 if c.dropna().shape[0] > 200 else np.nan) if len(win) > 260 else pd.Series(dtype=float)
        z_mom = (lambda s: (s - s.mean()) / s.std())(mom.reindex(liquid))
        ey = (eps.loc[t] / price_t).reindex(liquid)
        pb = -(price_t / bvps.loc[t]).reindex(liquid)
        z_val = pd.concat([(ey - ey.mean())/ey.std(), (pb - pb.mean())/pb.std()], axis=1).mean(axis=1)
        z_comp = pd.concat([z_mom, z_val], axis=1).mean(axis=1, skipna=True)

        scores = {"momentum": z_mom, "value": z_val, "composite": z_comp}
        for k, s in scores.items():
            out[k]["base"].append(top_q_return(s, fwd))

        # inject about-to-delist names: cheap (high value z), falling (low mom z), terminal loss
        n_inj = int(round(monthly_rate * len(liquid)))
        if n_inj > 0:
            inj_val = RNG.uniform(1.5, 3.5, n_inj)     # very cheap -> high value score
            inj_mom = RNG.uniform(-3.5, -1.5, n_inj)   # already crashing -> low momentum
            inj_comp = (inj_val + inj_mom) / 2
            inj_fwd = pd.Series(DELIST_TERMINAL, index=[f"_DELIST_{j}" for j in range(n_inj)])
            for k, injs in {"momentum": inj_mom, "value": inj_val, "composite": inj_comp}.items():
                s_aug = pd.concat([scores[k], pd.Series(injs, index=inj_fwd.index)])
                f_aug = pd.concat([fwd, inj_fwd])
                out[k]["stress"].append(top_q_return(s_aug, f_aug))

    res = {}
    for k in out:
        b = pd.Series(out[k]["base"]).dropna()
        s = pd.Series(out[k]["stress"]).dropna()
        res[k] = {"base_ann": b.mean()*12*100, "stress_ann": s.mean()*12*100,
                  "drag_pp": (b.mean()-s.mean())*12*100}
    return res


def main():
    print("=" * 74)
    print("SURVIVORSHIP STRESS TEST — inject about-to-delist names (cheap+falling)")
    print(f"terminal return per delisting = {DELIST_TERMINAL*100:.0f}%")
    print("=" * 74)
    print("Morningstar confirms ~2,784 obsolete Indian equities exist; Dawn has ~0.")
    print("Delisted names are penny collapses -> low momentum, often cheap (traps).\n")
    for rate in [0.01, 0.03, 0.05]:
        r = run(rate)
        print(f"--- delisting rate {rate*100:.0f}%/yr ---")
        print(f"{'strategy':<11}{'base ann%':>11}{'stressed ann%':>15}{'drag pp':>10}")
        for k in ["momentum", "value", "composite"]:
            d = r[k]
            print(f"{k:<11}{d['base_ann']:>11.1f}{d['stress_ann']:>15.1f}{d['drag_pp']:>10.1f}")
        print()
    print("READ: momentum long-only is structurally protected (collapsing names are")
    print("low-momentum -> never in its top quintile). Value takes the survivorship")
    print("hit (traps look cheap -> enter its top quintile). Composite in between.")


if __name__ == "__main__":
    main()
