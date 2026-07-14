#!/usr/bin/env python3
"""
TIER 1.4 — Low-vol / low-beta REGIME conditioning.

Earlier finding: low-vol had a NEGATIVE IC t-stat over 2017-2026 — it lost,
because the sample is dominated by a bull market where high-beta names ran. But
Frazzini-Pedersen (Betting Against Beta) says low-beta wins on a RISK-ADJUSTED
basis and especially in stressed / falling markets. If low-vol/low-beta IC flips
POSITIVE in bear / high-vol regimes, it becomes a regime-switched 3rd
diversifier (defensive sleeve to turn on when the trend overlay goes risk-off).

Method (all PIT, survivorship-aware):
  - Market proxy = equal-weight universe daily index.
  - Regime measured AT t (known info):
      TREND : index >= its 200d MA (bull)  vs  < MA (bear)
      VOL   : trailing 60d market vol  above vs below its expanding median
  - Factors: low_vol = -(120d stock return vol);  low_beta = -(120d beta to mkt).
  - For each factor compute the per-rebalance rank IC and the top-minus-bottom
    quintile forward spread, then group those by the regime at t and compare.

The bet pays off only if the defensive factor's IC is materially HIGHER (ideally
positive) in bear / high-vol months than in bull / low-vol months.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/tier1_regime_lowvol.py
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
MA_DAYS, VOL_WIN, BETA_WIN = 200, 60, 120
START = "2017-01-31"


def run(px: pd.DataFrame):
    dret = px.pct_change()
    mkt = dret.clip(-0.20, 0.20).mean(axis=1, skipna=True)
    idx = (1 + mkt.fillna(0)).cumprod()
    idx_ma = idx.rolling(MA_DAYS, min_periods=100).mean()
    mkt_vol = mkt.rolling(VOL_WIN, min_periods=30).std()
    mkt_vol_med = mkt_vol.expanding(min_periods=60).median()

    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)

    def at(s):
        return s.reindex(s.index.union(rebal)).ffill().reindex(rebal)

    idx_at, idxma_at, vol_at, volmed_at = at(idx), at(idx_ma), at(mkt_vol), at(mkt_vol_med)

    rows = []
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        winlive = px.loc[t - pd.Timedelta(days=LIVE_DAYS):t]
        live = winlive.columns[winlive.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        live = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1].reindex(live) / price_t.reindex(live) - 1.0).clip(*FWD_WINSOR)

        window = dret.loc[:t].iloc[-BETA_WIN:]
        mkt_w = mkt.loc[window.index]
        var_m = mkt_w.var()
        vol = window.iloc[-VOL_WIN:].std().reindex(live)
        with np.errstate(invalid="ignore", divide="ignore"):
            beta = window.reindex(columns=live).apply(
                lambda c: c.cov(mkt_w) / var_m if var_m and c.notna().sum() >= 40 else np.nan)

        facs = {"low_vol": -vol, "low_beta": -beta}
        bull = bool(idx_at.loc[t] >= idxma_at.loc[t]) if pd.notna(idxma_at.loc[t]) else True
        hivol = bool(vol_at.loc[t] >= volmed_at.loc[t]) if pd.notna(volmed_at.loc[t]) else False

        rec = {"t": t, "bull": bull, "hivol": hivol}
        for name, fac in facs.items():
            d = pd.concat([fac.rename("s"), fwd.rename("f")], axis=1).dropna()
            if len(d) < MIN_NAMES:
                rec[f"{name}_ic"] = np.nan; rec[f"{name}_ls"] = np.nan; continue
            rec[f"{name}_ic"] = d["s"].rank().corr(d["f"].rank())
            q = pd.qcut(d["s"].rank(method="first"), NQ, labels=False)
            rec[f"{name}_ls"] = d["f"][q == NQ - 1].mean() - d["f"][q == 0].mean()
        rows.append(rec)
    return pd.DataFrame(rows)


def summ(s: pd.Series):
    s = s.dropna()
    if len(s) < 4:
        return np.nan, np.nan, 0
    t = s.mean() / (s.std() / np.sqrt(len(s))) if s.std() else np.nan
    return s.mean(), t, len(s)


def main():
    print("Loading Dawn prices (2016-2026, split/bonus adjusted) ...")
    px, _ = load()
    df = run(px)

    print("\n" + "=" * 84)
    print(f"TIER 1.4  LOW-VOL / LOW-BETA REGIME CONDITIONING  —  "
          f"{df['t'].min().date()}→{df['t'].max().date()} ({len(df)} rebalances)")
    print("=" * 84)

    for fac in ["low_vol", "low_beta"]:
        icc, lsc = f"{fac}_ic", f"{fac}_ls"
        print(f"\n{fac.upper()}   (IC = rank-corr with fwd ret; LS = Q5-Q1 monthly spread)")
        print(f"{'regime':<22}{'n':>4}{'meanIC':>9}{'IC_t':>7}{'meanLS%':>9}")
        print("-" * 55)
        splits = [
            ("ALL", df),
            ("TREND: bull (>MA)", df[df["bull"]]),
            ("TREND: bear (<MA)", df[~df["bull"]]),
            ("VOL: low-vol regime", df[~df["hivol"]]),
            ("VOL: high-vol regime", df[df["hivol"]]),
        ]
        for label, sub in splits:
            m, t, n = summ(sub[icc])
            lsm, _, _ = summ(sub[lsc])
            print(f"{label:<22}{n:>4}{m:>9.4f}{t:>7.2f}{lsm*100:>9.2f}")

    print("-" * 84)
    print("READ: the Frazzini-Pedersen bet is CONFIRMED if low-vol/low-beta IC and LS spread")
    print("go clearly POSITIVE (or much less negative) in the bear / high-vol regime vs bull /")
    print("low-vol. A positive bear-regime IC => a defensive sleeve worth switching ON when the")
    print("Tier-1.2 trend overlay flags risk-off. A flat/negative result in every regime => the")
    print("factor is just weak in this market and should stay benched.")


if __name__ == "__main__":
    main()
