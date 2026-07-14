#!/usr/bin/env python3
"""
Expectancy + ruin simulator — "what return, and what chance of blowing up?"

Monte-Carlo simulates the momentum long-only strategy across configurations
(concentration x leverage), so every expected-return number comes with its
drawdown and RUIN probability attached. Answers "is 50% possible?" honestly:
you can raise the median, but watch what happens to P(ruin).

Method:
  - Empirical monthly return series = the validated momentum long-only sleeve
    (from scripts/xsection_montecarlo.build_returns), re-centred to a REALISTIC
    forward gross (default 16%/yr, vs the ~29% in-sample) while keeping the
    real shape/fat-tails (momentum crashes preserved).
  - Concentration: fewer names -> idiosyncratic vol scales ~sqrt(25/n) (more
    risk for the SAME expected alpha — the conservative, honest assumption).
  - Leverage L: r_lev = L*r - (L-1)*financing; margin/ruin if equity <= 30% of
    start (forced-liquidation / would-quit threshold).
  - Block bootstrap (block=3), 5-year paths, 5000 sims per config.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/expectancy_sim.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))         # import sibling script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # repo root
from xsection_montecarlo import build_returns

CAPITAL = 1_000_000.0        # ₹10 lakh
TARGET_GROSS = 0.16          # realistic forward annual gross (haircut from ~29% in-sample)
FIN_ANNUAL = 0.085           # cost of leverage (~8.5%/yr, futures/margin)
YEARS, MONTHS = 5, 60
N_SIM, BLOCK = 5000, 3
RUIN_LEVEL = 0.30            # equity <= 30% of start = ruin (forced liquidation)
BASE_NAMES = 25
RNG = np.random.default_rng(7)

CONFIGS = [
    ("25 names, 1.0x  (base/recommended)", 25, 1.0),
    ("10 names, 1.0x  (concentrated)",     10, 1.0),
    ("25 names, 1.5x  (levered)",          25, 1.5),
    ("10 names, 1.5x  (concentrated+lev)", 10, 1.5),
    ("10 names, 2.0x  (aggressive)",       10, 2.0),
    ("5 names, 2.0x   (swing-for-fences)",  5, 2.0),
]


def recentre(r: np.ndarray, target_annual: float) -> np.ndarray:
    """Shift monthly returns to a target annual mean, preserving vol & tail shape."""
    tgt_m = (1 + target_annual) ** (1 / 12) - 1
    return r - r.mean() + tgt_m


def sim_config(base: np.ndarray, n_names: int, lev: float):
    fin_m = (1 + FIN_ANNUAL) ** (1 / 12) - 1
    vol_mult = np.sqrt(BASE_NAMES / n_names)
    mu = base.mean()
    n = len(base)
    n_blocks = int(np.ceil(MONTHS / BLOCK))

    cagrs, mdds, ruined, final_eq = [], [], 0, []
    for _ in range(N_SIM):
        starts = RNG.integers(0, n, n_blocks)
        path = np.concatenate([base[s:s + BLOCK] for s in starts])[:MONTHS]
        # concentration: scale idiosyncratic (demeaned) part
        path = (path - mu) * vol_mult + mu
        # leverage + financing
        path = lev * path - (lev - 1) * fin_m
        eq = CAPITAL
        peak = CAPITAL
        is_ruin = False
        for r in path:
            eq *= (1 + r)
            peak = max(peak, eq)
            if eq <= RUIN_LEVEL * CAPITAL:
                is_ruin = True
                eq = RUIN_LEVEL * CAPITAL  # forced out
                break
        dd = eq / peak - 1 if not is_ruin else -(1 - RUIN_LEVEL)
        # recompute true path max-dd
        equ = CAPITAL * np.cumprod(1 + path)
        mdd = (equ / np.maximum.accumulate(equ) - 1).min()
        cagr = (eq / CAPITAL) ** (1 / YEARS) - 1
        cagrs.append(cagr); mdds.append(mdd); final_eq.append(eq)
        if is_ruin:
            ruined += 1
    cagrs, mdds, final_eq = map(np.array, (cagrs, mdds, final_eq))
    return {
        "cagr_med": np.median(cagrs) * 100,
        "cagr_p5": np.percentile(cagrs, 5) * 100,
        "cagr_p95": np.percentile(cagrs, 95) * 100,
        "mdd_med": np.median(mdds) * 100,
        "mdd_p5": np.percentile(mdds, 5) * 100,     # worst-tail drawdown
        "p_ruin": ruined / N_SIM * 100,
        "p_down5y": (final_eq < CAPITAL).mean() * 100,
        "eq_med": np.median(final_eq),
    }


def main():
    print("Recomputing momentum monthly series from Dawn ...")
    rets, _, _ = build_returns()
    base = recentre(rets["momentum"].values, TARGET_GROSS)
    ann = ((1 + base.mean()) ** 12 - 1) * 100
    volann = base.std() * np.sqrt(12) * 100
    print(f"base momentum sleeve re-centred to {ann:.1f}%/yr gross, vol {volann:.1f}%/yr "
          f"({len(base)} monthly obs) | capital ₹{CAPITAL:,.0f} | {YEARS}yr | {N_SIM} sims")
    print("=" * 100)
    print(f"{'config':<36}{'medCAGR':>8}{'CAGR5-95%':>13}{'medMaxDD':>9}{'tailDD':>8}{'P(5y loss)':>11}{'P(RUIN)':>9}")
    print("-" * 100)
    for name, n, lev in CONFIGS:
        s = sim_config(base, n, lev)
        rng = f"{s['cagr_p5']:.0f}..{s['cagr_p95']:.0f}%"
        print(f"{name:<36}{s['cagr_med']:>7.1f}%{rng:>13}"
              f"{s['mdd_med']:>8.0f}%{s['mdd_p5']:>7.0f}%{s['p_down5y']:>10.1f}%{s['p_ruin']:>8.1f}%")
    print("-" * 100)
    print("medCAGR = median annualised return. tailDD = 5th-pctile (worst) drawdown.")
    print("P(RUIN) = chance equity ever falls to 30% of start over 5yr (forced liquidation).")
    print(f"All on realistic {TARGET_GROSS*100:.0f}%/yr gross assumption — NOT the ~29% in-sample.")
    print("Leverage financing 8.5%/yr. Concentration adds vol for the SAME expected alpha.")
    print("\nREAD: chasing a higher median CAGR pushes P(RUIN) up fast. There is no 50% median")
    print("without a large ruin probability. Uncorrelated edges — not leverage — are the only")
    print("honest way to raise return without raising ruin.")


if __name__ == "__main__":
    main()
