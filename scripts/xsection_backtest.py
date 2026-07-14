#!/usr/bin/env python3
"""
Cross-sectional factor backtest on the Dawn panel — the first honest "is there
edge?" test for the super-quant stock-selection sleeve.

Data (local `dawn` DB, free primary sources):
  security_fundamentals  — ttm_eps, bvps, shares_outstanding (PIT via period_end + lag)
  stock_prices_daily     — daily close (momentum, low-vol, forward returns)

Factors (limited to what Dawn actually covers well):
  momentum   12-1 month price momentum (skip most recent month)
  low_vol    negative trailing 120d volatility
  value_pb   inverse Price/Book  (low P/B = cheap)
  value_ey   earnings yield = ttm_eps / price
  size       -log(market cap)   (small = high score)

Honesty controls:
  - PIT: a fundamental for period_end P is only usable after P + REPORT_LAG_DAYS
    (there is no filing_date in the data, so we assume a conservative lag).
  - Costs: long-short spread is charged 2x equity-delivery round-trip; long-only
    top-quintile is charged 1x turnover.
  - Forward returns winsorized to blunt split/corporate-action artifacts (only
    `close` is available, not adjusted prices).
  - Penny/illiquid filter (min price), junk-symbol filter.

NOT controlled yet (stated, not hidden):
  - Survivorship: delisted names largely absent from the panel -> upward bias.
  - Splits/dividends: close is unadjusted; winsorization is a crude guard.
  - ~5yr fundamental window (2021+) -> few regimes, low statistical power.

Usage:
  DATABASE_URL=postgresql://localhost:5432/dawn python3 scripts/xsection_backtest.py
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
REPORT_LAG_DAYS = 90          # fundamentals known only this long after period_end
MIN_PRICE = 10.0              # drop penny stocks
MIN_NAMES = 30               # skip rebalances with too few names to rank
N_QUANTILES = 5
FWD_WINSOR = (-0.40, 0.80)    # clip monthly forward returns
COST_BPS = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4  # per round trip


def _engine():
    return create_engine(DAWN_URL)


def load_prices(eng) -> pd.DataFrame:
    df = pd.read_sql(
        text("SELECT symbol, date, close FROM stock_prices_daily WHERE symbol IS NOT NULL AND close > 0"),
        eng,
    )
    df["date"] = pd.to_datetime(df["date"])
    # junk-symbol filter
    df = df[df["symbol"].str.match(r"^[A-Z][A-Z0-9&-]{1,}$", na=False)]
    px = df.pivot_table(index="date", columns="symbol", values="close", aggfunc="last").sort_index()
    return px


def load_fundamentals(eng) -> pd.DataFrame:
    f = pd.read_sql(
        text(
            """SELECT symbol, period_end, basis, ttm_eps, bvps, shares_outstanding
               FROM security_fundamentals
               WHERE symbol ~ '^[A-Z][A-Z0-9&-]{1,}$'"""
        ),
        eng,
    )
    f["period_end"] = pd.to_datetime(f["period_end"])
    f["available"] = f["period_end"] + pd.Timedelta(days=REPORT_LAG_DAYS)
    # prefer consolidated over standalone when both exist for same (symbol, available)
    f["basis_rank"] = (f["basis"] == "consolidated").astype(int)
    f = f.sort_values(["symbol", "available", "basis_rank"])
    return f


def pit_panel(f: pd.DataFrame, field: str, rebal_dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Point-in-time panel: value of `field` known as of each rebalance date."""
    sub = f.dropna(subset=[field])
    wide = sub.pivot_table(index="available", columns="symbol", values=field, aggfunc="last").sort_index()
    # forward-fill each symbol's last-known value up to each rebalance date
    return wide.reindex(wide.index.union(rebal_dates)).ffill().reindex(rebal_dates)


def zscore(s: pd.Series) -> pd.Series:
    s = s.replace([np.inf, -np.inf], np.nan)
    mu, sd = s.mean(), s.std()
    return (s - mu) / sd if sd and sd > 0 else s * 0.0


def spearman_ic(factor: pd.Series, fwd: pd.Series) -> float:
    d = pd.concat([factor, fwd], axis=1).dropna()
    if len(d) < MIN_NAMES:
        return np.nan
    return d.iloc[:, 0].rank().corr(d.iloc[:, 1].rank())


def main() -> None:
    eng = _engine()
    print(f"Loading Dawn panel from {DAWN_URL} ...")
    px = load_prices(eng)
    f = load_fundamentals(eng)
    eng.dispose()

    # month-end rebalance calendar within price coverage
    m_idx = px.resample("ME").last().index
    rebal = m_idx[(m_idx >= pd.Timestamp("2021-06-30")) & (m_idx <= px.index.max())]
    pxm = px.reindex(px.index.union(rebal)).ffill().reindex(rebal)

    bvps = pit_panel(f, "bvps", rebal)
    eps = pit_panel(f, "ttm_eps", rebal)
    shr = pit_panel(f, "shares_outstanding", rebal)

    # daily log for momentum / vol
    dret = px.pct_change()
    factors = ["momentum", "low_vol", "value_pb", "value_ey", "size"]
    ic_rows = {k: [] for k in factors}
    ls_rows = {k: [] for k in factors}          # long-short quintile, net of cost
    lo_rows = {k: [] for k in factors}          # long-only top quintile minus universe, net

    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        price_t = pxm.loc[t]
        liquid = price_t[price_t >= MIN_PRICE].index

        # forward 1-month return (winsorized), net computed later
        fwd = (pxm.loc[t1] / pxm.loc[t] - 1.0).clip(*FWD_WINSOR)

        # --- factor construction (PIT) ---
        win = px.loc[:t]
        mom = win.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1] / c.dropna().iloc[0] - 1 if c.dropna().shape[0] > 200 else np.nan) if len(win) > 260 else pd.Series(dtype=float)
        vol = dret.loc[:t].iloc[-120:].std()
        pb = price_t / bvps.loc[t]
        ey = eps.loc[t] / price_t
        mcap = price_t * shr.loc[t]

        raw = {
            "momentum": mom.reindex(liquid),
            "low_vol": (-vol).reindex(liquid),
            "value_pb": (-pb).reindex(liquid),       # low P/B -> high score
            "value_ey": ey.reindex(liquid),
            "size": (-np.log(mcap.reindex(liquid).where(mcap.reindex(liquid) > 0))),
        }
        fwd_l = fwd.reindex(liquid)

        for k, fac in raw.items():
            fac = fac.replace([np.inf, -np.inf], np.nan)
            valid = pd.concat([fac, fwd_l], axis=1).dropna()
            if len(valid) < MIN_NAMES:
                continue
            fac_v, fwd_v = valid.iloc[:, 0], valid.iloc[:, 1]
            ic_rows[k].append(spearman_ic(fac_v, fwd_v))

            q = pd.qcut(fac_v.rank(method="first"), N_QUANTILES, labels=False)
            top = fwd_v[q == N_QUANTILES - 1].mean()
            bot = fwd_v[q == 0].mean()
            uni = fwd_v.mean()
            ls_rows[k].append((top - bot) - 2 * COST_BPS)     # both legs traded
            lo_rows[k].append((top - uni) - COST_BPS)

    # --- report ---
    print("\n" + "=" * 78)
    print(f"CROSS-SECTIONAL FACTOR BACKTEST — Dawn panel, {rebal[0].date()}→{rebal[-1].date()} "
          f"({len(rebal)} monthly rebalances)")
    print(f"cost/round-trip (delivery) = {COST_BPS*1e4:.1f}bps | PIT lag {REPORT_LAG_DAYS}d | "
          f"min price ₹{MIN_PRICE:.0f} | {N_QUANTILES} quantiles")
    print("=" * 78)
    hdr = f"{'factor':<10} {'n':>4} {'meanIC':>8} {'IC_t':>7} {'IC_IR':>7} "\
          f"{'LS_ann%':>9} {'LS_Sh':>7} {'LOnly_ann%':>11}"
    print(hdr); print("-" * 78)
    for k in factors:
        ic = pd.Series(ic_rows[k]).dropna()
        ls = pd.Series(ls_rows[k]).dropna()
        lo = pd.Series(lo_rows[k]).dropna()
        if len(ic) < 6:
            print(f"{k:<10} {len(ic):>4}  (insufficient)")
            continue
        ic_t = ic.mean() / (ic.std() / np.sqrt(len(ic))) if ic.std() else np.nan
        ic_ir = ic.mean() / ic.std() if ic.std() else np.nan
        ls_ann = ls.mean() * 12 * 100
        ls_sh = (ls.mean() / ls.std() * np.sqrt(12)) if ls.std() else np.nan
        lo_ann = lo.mean() * 12 * 100
        print(f"{k:<10} {len(ic):>4} {ic.mean():>8.4f} {ic_t:>7.2f} {ic_ir:>7.3f} "
              f"{ls_ann:>9.2f} {ls_sh:>7.2f} {lo_ann:>11.2f}")
    print("-" * 78)
    print("meanIC>0 & |IC_t|>2 => statistically detectable cross-sectional signal.")
    print("LS_ann% = annualised top-minus-bottom quintile, net of cost. LS_Sh = its Sharpe.")
    print("Caveats: survivorship (delisted absent), unadjusted close, ~4-5yr window.")


if __name__ == "__main__":
    main()
