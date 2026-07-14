#!/usr/bin/env python3
"""
TIER 1.3 — Combinatorial Purged Cross-Validation (Lopez de Prado).

The permutation test already showed the signal isn't luck (p<0.001). CPCV is a
different, stronger question: is the Sharpe ROBUST out-of-sample across many
held-out paths, and does *choosing* the best spec overfit? Two outputs:

  1. OOS SHARPE DISTRIBUTION (fixed core spec). Split the 113 monthly returns
     into N contiguous groups; for every C(N,k) choice of k test groups, compute
     the core strategy's Sharpe on the held-out test months only, with the
     adjacent train months PURGED/EMBARGOED (the 12-month formation window makes
     neighbours leak). C(8,2)=28 backtest paths -> a distribution of OOS Sharpe,
     not one number. Report mean, dispersion, %positive, 5th percentile.

  2. PBO — Probability of Backtest Overfitting (Bailey, Borwein, Lopez de Prado,
     Zhu 2017). Over a MENU of momentum/reversal/low-vol configs: in each split
     pick the config with the best TRAIN Sharpe, then look at that config's RANK
     out-of-sample. PBO = fraction of splits where the in-sample winner lands
     below the OOS median (logit<0). Low PBO => picking on backtest generalises.

All configs are survivorship-aware, top-quintile long-only, turnover-costed —
identical machinery to the core engine — over 2017-2026.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/tier1_purged_cv.py
"""
from __future__ import annotations

import sys
from itertools import combinations
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
START = "2017-01-31"
N_GROUPS = 8
K_TEST = 2
EMBARGO = 1          # months dropped from train on each side of a test block
CORE = "mom_12_1"


def build_config_returns(px: pd.DataFrame) -> pd.DataFrame:
    """Monthly net top-quintile long-only return for each candidate config."""
    m = px.resample("ME").last().index
    rebal = m[(m >= pd.Timestamp(START)) & (m <= px.index.max())]
    pxf = px.ffill()
    pxm = pxf.reindex(pxf.index.union(rebal)).ffill().reindex(rebal)
    dret = px.pct_change()

    configs = ["mom_6_1", "mom_9_1", "mom_12_1", "mom_12_2", "reversal", "low_vol"]
    out = {c: [] for c in configs}
    dates = []
    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        winlive = px.loc[t - pd.Timedelta(days=LIVE_DAYS):t]
        live = winlive.columns[winlive.notna().any()]
        price_t = pxm.loc[t].reindex(live)
        live = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1].reindex(live) / price_t.reindex(live) - 1.0).clip(*FWD_WINSOR)

        w = px.loc[:t]

        def mom(form, skip):
            seg = w.iloc[-form:-skip]
            return seg.apply(lambda c: c.dropna().iloc[-1]/c.dropna().iloc[0]-1
                             if c.dropna().shape[0] > form * 0.75 else np.nan).reindex(live)

        sig = {
            "mom_6_1": mom(126, 21),
            "mom_9_1": mom(189, 21),
            "mom_12_1": mom(252, 21),
            "mom_12_2": mom(252, 42),
            "reversal": -(price_t / pxf.loc[:t].iloc[-21].reindex(live) - 1.0),
            "low_vol": -dret.loc[:t].iloc[-120:].std().reindex(live),
        }
        dates.append(t1)
        for c, s in sig.items():
            d = pd.concat([s.rename("s"), fwd.rename("f")], axis=1).dropna()
            if len(d) < MIN_NAMES:
                out[c].append(np.nan); continue
            k_top = max(len(d) // NQ, 1)
            hold = d["s"].sort_values(ascending=False).head(k_top).index
            out[c].append(d["f"].reindex(hold).mean() - COST)

    return pd.DataFrame(out, index=pd.DatetimeIndex(dates))


def sharpe(r: pd.Series) -> float:
    r = r.dropna()
    if len(r) < 4 or r.std() == 0:
        return np.nan
    return r.mean() / r.std() * np.sqrt(12)


def cpcv(ret: pd.DataFrame):
    n = len(ret)
    bounds = np.linspace(0, n, N_GROUPS + 1).astype(int)
    groups = [list(range(bounds[g], bounds[g + 1])) for g in range(N_GROUPS)]

    core_oos, pbo_logits, sel_counter = [], [], {}
    for combo in combinations(range(N_GROUPS), K_TEST):
        test_idx = sorted(sum((groups[g] for g in combo), []))
        test_set = set(test_idx)
        # embargo: drop train months within EMBARGO of any test month
        emb = set()
        for j in test_idx:
            for e in range(-EMBARGO, EMBARGO + 1):
                emb.add(j + e)
        train_idx = [j for j in range(n) if j not in test_set and j not in emb]

        train, test = ret.iloc[train_idx], ret.iloc[test_idx]
        # 1. core spec OOS Sharpe on this path
        core_oos.append(sharpe(test[CORE]))
        # 2. PBO: pick best-on-train, rank it OOS
        tr_sh = train.apply(sharpe)
        te_sh = test.apply(sharpe)
        if tr_sh.notna().sum() < 2 or te_sh.notna().sum() < 2:
            continue
        best = tr_sh.idxmax()
        sel_counter[best] = sel_counter.get(best, 0) + 1
        ranks = te_sh.rank()                      # 1=worst .. C=best
        omega = ranks[best] / (te_sh.notna().sum() + 1)   # relative rank in (0,1)
        omega = min(max(omega, 1e-3), 1 - 1e-3)
        pbo_logits.append(np.log(omega / (1 - omega)))

    return np.array(core_oos), np.array(pbo_logits), sel_counter


def main():
    print("Loading Dawn prices (2016-2026, split/bonus adjusted) ...")
    px, _ = load()
    ret = build_config_returns(px)
    core_oos, logits, sel = cpcv(ret)

    full = {c: sharpe(ret[c]) for c in ret.columns}
    core_oos = core_oos[~np.isnan(core_oos)]
    pbo = float((logits < 0).mean()) if len(logits) else np.nan

    print("\n" + "=" * 80)
    print(f"TIER 1.3  COMBINATORIAL PURGED CV  —  {ret.index[0].date()}→{ret.index[-1].date()} "
          f"({len(ret)} months)")
    print(f"N={N_GROUPS} groups, k={K_TEST} test  ->  {len(list(combinations(range(N_GROUPS), K_TEST)))} "
          f"purged paths | embargo {EMBARGO}mo")
    print("=" * 80)

    print("\nFull-sample Sharpe by config (in-sample point estimates):")
    for c, s in sorted(full.items(), key=lambda kv: -(kv[1] if pd.notna(kv[1]) else -9)):
        star = "  <- CORE" if c == CORE else ""
        print(f"   {c:<12}{s:>7.2f}{star}")

    print(f"\n1. OOS SHARPE DISTRIBUTION for core spec ({CORE}), across {len(core_oos)} purged paths:")
    print(f"   full-sample Sharpe : {full[CORE]:.2f}")
    print(f"   OOS mean           : {core_oos.mean():.2f}   (std {core_oos.std():.2f})")
    print(f"   OOS 5th percentile : {np.percentile(core_oos, 5):.2f}")
    print(f"   OOS min            : {core_oos.min():.2f}")
    print(f"   %paths Sharpe > 0  : {(core_oos > 0).mean()*100:.0f}%")

    print("\n2. PBO — probability of backtest overfitting (spec selection):")
    print(f"   in-sample winner picked: " +
          ", ".join(f"{k} {v}x" for k, v in sorted(sel.items(), key=lambda kv: -kv[1])))
    print(f"   PBO = P(IS-best ranks below OOS median) = {pbo*100:.0f}%")

    print("-" * 80)
    print("READ: OOS mean Sharpe close to full-sample + high %positive + 5th-pctile>0 means the")
    print("edge is path-robust, not one lucky window. PBO<50% means selecting the spec on the")
    print("backtest still generalises (low overfit). PBO near/above 50% would mean our config")
    print("choice is coin-flip noise. Consistent IS-winner (mom_12_1) reinforces spec stability.")


if __name__ == "__main__":
    main()
