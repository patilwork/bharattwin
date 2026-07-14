#!/usr/bin/env python3
"""
Cross-sectional factor backtest on the Dawn panel — hardened "is there edge?" test.

v2 hardening over v1:
  - back-adjust prices for splits & bonuses (dawn.corporate_actions) -> kills
    the fake momentum/returns from unadjusted discrete price jumps
  - long-only tradeable composite (momentum + value), turnover-costed, vs the
    equal-weight universe (single-stock shorting isn't practical in India)
  - Deflated Sharpe Ratio (Bailey & Lopez de Prado) correcting for the number
    of factors tested, sample length, skew & kurtosis
  - regime-split IC by calendar year (is the signal one-regime or persistent?)

Data (local `dawn` DB, free primary sources):
  security_fundamentals  — ttm_eps, bvps, shares_outstanding (PIT via period_end + lag)
  stock_prices_daily     — daily close
  corporate_actions      — split / bonus ratios for back-adjustment

Still NOT controlled (stated, not hidden):
  - Survivorship: delisted names largely absent -> upward bias (worst for value).
  - Rights issues & dividends not price-adjusted (rights complex; div yield small).
  - No volume in the data -> ADV/impact costs cannot be modelled; a flat higher
    slippage is used for the composite as a crude proxy.
  - ~5yr window (2021+) -> limited regime coverage, low power.

Usage:
  DAWN_URL=postgresql://localhost:5432/dawn python3 scripts/xsection_backtest.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.costs import round_trip_cost

DAWN_URL = os.environ.get("DAWN_URL", "postgresql://localhost:5432/dawn")
REPORT_LAG_DAYS = 90
MIN_PRICE = 10.0
MIN_NAMES = 30
N_QUANTILES = 5
FWD_WINSOR = (-0.40, 0.80)
COST_BPS = round_trip_cost(1_000_000, "equity_delivery").total_bps / 1e4      # per round trip
COMPOSITE_SLIP = 0.0025  # extra crude impact proxy for illiquid names (no volume data)


def _engine():
    return create_engine(DAWN_URL)


def load_prices(eng) -> pd.DataFrame:
    df = pd.read_sql(text("SELECT symbol, date, close FROM stock_prices_daily WHERE symbol IS NOT NULL AND close > 0"), eng)
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["symbol"].str.match(r"^[A-Z][A-Z0-9&-]{1,}$", na=False)]
    return df.pivot_table(index="date", columns="symbol", values="close", aggfunc="last").sort_index()


def load_corporate_actions(eng) -> pd.DataFrame:
    ca = pd.read_sql(
        text("""SELECT symbol, ex_date, ca_type, ratio_from, ratio_to
                FROM corporate_actions
                WHERE ca_type IN ('split','bonus') AND ratio_from IS NOT NULL AND ratio_to IS NOT NULL
                  AND symbol IS NOT NULL"""),
        eng,
    )
    ca["ex_date"] = pd.to_datetime(ca["ex_date"])
    return ca


def adjust_prices(px: pd.DataFrame, ca: pd.DataFrame) -> pd.DataFrame:
    """Back-adjust: multiply prices strictly BEFORE each ex-date by the event's
    price factor so the series is continuous across splits/bonuses."""
    adj = px.copy()
    for _, r in ca.iterrows():
        sym = r["symbol"]
        if sym not in adj.columns:
            continue
        rf, rt = r["ratio_from"], r["ratio_to"]
        if not rf or not rt:
            continue
        if r["ca_type"] == "split":          # FV old->new; price divides by rf/rt
            factor = rt / rf
        else:                                 # bonus a:b  ->  price * b/(a+b)
            factor = rt / (rf + rt)
        if factor <= 0 or factor >= 1.0:      # only shrink historical prices
            continue
        mask = adj.index < r["ex_date"]
        adj.loc[mask, sym] = adj.loc[mask, sym] * factor
    return adj


def load_fundamentals(eng) -> pd.DataFrame:
    f = pd.read_sql(
        text("""SELECT symbol, period_end, basis, ttm_eps, bvps, shares_outstanding
                FROM security_fundamentals WHERE symbol ~ '^[A-Z][A-Z0-9&-]{1,}$'"""),
        eng,
    )
    f["period_end"] = pd.to_datetime(f["period_end"])
    f["available"] = f["period_end"] + pd.Timedelta(days=REPORT_LAG_DAYS)
    f["basis_rank"] = (f["basis"] == "consolidated").astype(int)
    return f.sort_values(["symbol", "available", "basis_rank"])


def pit_panel(f, field, rebal):
    sub = f.dropna(subset=[field])
    wide = sub.pivot_table(index="available", columns="symbol", values=field, aggfunc="last").sort_index()
    return wide.reindex(wide.index.union(rebal)).ffill().reindex(rebal)


def spearman_ic(fac, fwd):
    d = pd.concat([fac, fwd], axis=1).dropna()
    return d.iloc[:, 0].rank().corr(d.iloc[:, 1].rank()) if len(d) >= MIN_NAMES else np.nan


def deflated_sharpe(returns: pd.Series, n_trials: int, trial_sr_var: float) -> dict:
    """Bailey & Lopez de Prado Deflated Sharpe Ratio. `returns` are per-period.
    Deflates the observed SR by the expected max SR under `n_trials`."""
    r = returns.dropna()
    n = len(r)
    if n < 10 or r.std() == 0:
        return {"sr": np.nan, "sr0": np.nan, "dsr": np.nan}
    sr = r.mean() / r.std()
    sk = stats.skew(r)
    ku = stats.kurtosis(r, fisher=False)  # non-excess
    gamma = 0.5772156649
    e = np.e
    # expected max of N standard-normal trial SRs, scaled by cross-trial SR dispersion
    z1 = stats.norm.ppf(1 - 1.0 / n_trials)
    z2 = stats.norm.ppf(1 - 1.0 / (n_trials * e))
    sr0 = np.sqrt(max(trial_sr_var, 1e-12)) * ((1 - gamma) * z1 + gamma * z2)
    denom = np.sqrt(max(1 - sk * sr + ((ku - 1) / 4) * sr**2, 1e-9))
    dsr = stats.norm.cdf((sr - sr0) * np.sqrt(n - 1) / denom)
    return {"sr": sr, "sr0": sr0, "dsr": dsr}


def main() -> None:
    eng = _engine()
    print(f"Loading Dawn panel from {DAWN_URL} ...")
    px_raw = load_prices(eng)
    ca = load_corporate_actions(eng)
    f = load_fundamentals(eng)
    eng.dispose()

    px = adjust_prices(px_raw, ca)
    print(f"back-adjusted {ca['symbol'].nunique()} symbols for {len(ca)} split/bonus events")

    m_idx = px.resample("ME").last().index
    rebal = m_idx[(m_idx >= pd.Timestamp("2021-06-30")) & (m_idx <= px.index.max())]
    pxm = px.reindex(px.index.union(rebal)).ffill().reindex(rebal)
    dret = px.pct_change()
    bvps, eps, shr = (pit_panel(f, c, rebal) for c in ["bvps", "ttm_eps", "shares_outstanding"])

    factors = ["momentum", "value_ey", "value_pb", "low_vol", "size"]
    ic_rows = {k: [] for k in factors}
    ic_by_year = {k: {} for k in factors}
    ls_rows = {k: [] for k in factors}
    comp_ret, bench_ret, comp_dates = [], [], []
    prev_hold: set = set()

    for i in range(len(rebal) - 1):
        t, t1 = rebal[i], rebal[i + 1]
        yr = t.year
        price_t = pxm.loc[t]
        liquid = price_t[price_t >= MIN_PRICE].index
        fwd = (pxm.loc[t1] / pxm.loc[t] - 1.0).clip(*FWD_WINSOR).reindex(liquid)

        win = px.loc[:t]
        mom = win.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1] / c.dropna().iloc[0] - 1 if c.dropna().shape[0] > 200 else np.nan) if len(win) > 260 else pd.Series(dtype=float)
        vol = dret.loc[:t].iloc[-120:].std()
        raw = {
            "momentum": mom.reindex(liquid),
            "value_ey": (eps.loc[t] / price_t).reindex(liquid),
            "value_pb": (-(price_t / bvps.loc[t])).reindex(liquid),
            "low_vol": (-vol).reindex(liquid),
            "size": (-np.log((price_t * shr.loc[t]).where(price_t * shr.loc[t] > 0))).reindex(liquid),
        }

        z = {}
        for k, fac in raw.items():
            fac = fac.replace([np.inf, -np.inf], np.nan)
            valid = pd.concat([fac, fwd], axis=1).dropna()
            if len(valid) >= MIN_NAMES:
                ic = spearman_ic(valid.iloc[:, 0], valid.iloc[:, 1])
                ic_rows[k].append(ic)
                ic_by_year[k].setdefault(yr, []).append(ic)
                q = pd.qcut(valid.iloc[:, 0].rank(method="first"), N_QUANTILES, labels=False)
                ls_rows[k].append((valid.iloc[:, 1][q == N_QUANTILES - 1].mean()
                                   - valid.iloc[:, 1][q == 0].mean()) - 2 * COST_BPS)
            s = (fac - fac.mean()) / fac.std() if fac.std() else fac * 0
            z[k] = s

        # --- long-only tradeable composite: momentum + value (equal z-weight) ---
        comp = pd.concat([z["momentum"], z["value_ey"], z["value_pb"]], axis=1).mean(axis=1, skipna=True).dropna()
        comp = comp[comp.index.isin(fwd.dropna().index)]
        if len(comp) >= MIN_NAMES:
            k_top = max(int(len(comp) / N_QUANTILES), 1)
            hold = set(comp.sort_values(ascending=False).head(k_top).index)
            gross = fwd.reindex(list(hold)).mean()
            turnover = 1.0 - (len(hold & prev_hold) / len(hold)) if prev_hold else 1.0
            net = gross - turnover * (COST_BPS + COMPOSITE_SLIP)
            comp_ret.append(net)
            bench_ret.append(fwd.mean())
            comp_dates.append(t1)
            prev_hold = hold

    # ---------- report ----------
    print("\n" + "=" * 82)
    print(f"HARDENED CROSS-SECTIONAL BACKTEST — {rebal[0].date()}→{rebal[-1].date()} ({len(rebal)} rebalances)")
    print(f"adjusted prices | delivery cost {COST_BPS*1e4:.1f}bps + {COMPOSITE_SLIP*1e4:.0f}bps impact proxy | PIT {REPORT_LAG_DAYS}d")
    print("=" * 82)
    print(f"{'factor':<10} {'n':>3} {'meanIC':>8} {'IC_t':>6} {'LS_ann%':>8}   IC by year")
    print("-" * 82)
    for k in factors:
        ic = pd.Series(ic_rows[k]).dropna()
        if len(ic) < 6:
            print(f"{k:<10} {len(ic):>3}  (insufficient)"); continue
        ic_t = ic.mean() / (ic.std() / np.sqrt(len(ic))) if ic.std() else np.nan
        ls_ann = pd.Series(ls_rows[k]).dropna().mean() * 12 * 100
        yr_str = " ".join(f"{y}:{np.nanmean(v):+.3f}" for y, v in sorted(ic_by_year[k].items()))
        print(f"{k:<10} {len(ic):>3} {ic.mean():>8.4f} {ic_t:>6.2f} {ls_ann:>8.2f}   {yr_str}")

    # composite stats
    cr = pd.Series(comp_ret, index=pd.DatetimeIndex(comp_dates))
    br = pd.Series(bench_ret, index=pd.DatetimeIndex(comp_dates))
    excess = cr - br
    trial_srs = [pd.Series(ls_rows[k]).dropna().mean() / pd.Series(ls_rows[k]).dropna().std()
                 for k in factors if pd.Series(ls_rows[k]).dropna().std()]
    trial_var = float(np.var(trial_srs)) if len(trial_srs) > 1 else 0.25
    dsr_abs = deflated_sharpe(cr, n_trials=len(factors), trial_sr_var=trial_var)
    dsr_exc = deflated_sharpe(excess, n_trials=len(factors), trial_sr_var=trial_var)

    def ann(s):  # annualised return %, Sharpe
        return s.mean() * 12 * 100, (s.mean() / s.std() * np.sqrt(12) if s.std() else np.nan)

    c_ann, c_sh = ann(cr); b_ann, b_sh = ann(br); e_ann, e_sh = ann(excess)
    print("\n" + "-" * 82)
    print("LONG-ONLY COMPOSITE (momentum + value, top quintile, turnover-costed)")
    print(f"  composite:  {c_ann:+.2f}%/yr  Sharpe {c_sh:.2f}   max_dd {(cr.add(1).cumprod()/cr.add(1).cumprod().cummax()-1).min()*100:.1f}%")
    print(f"  benchmark:  {b_ann:+.2f}%/yr  Sharpe {b_sh:.2f}   (equal-weight universe)")
    print(f"  EXCESS:     {e_ann:+.2f}%/yr  Sharpe {e_sh:.2f}")
    print(f"  Deflated Sharpe (composite abs): {dsr_abs['dsr']:.3f}   (SR {dsr_abs['sr']:.3f} vs hurdle SR0 {dsr_abs['sr0']:.3f})")
    print(f"  Deflated Sharpe (excess vs univ): {dsr_exc['dsr']:.3f}")
    print("-" * 82)
    print("READ: IC_t>2 = detectable. DSR>0.95 = survives multiple-testing. Persistent")
    print("IC across years = not one-regime. Excess>0 = beats naive equal-weight buy.")
    print("CAVEATS REMAINING: survivorship (delisted absent, ~upward bias), no volume/ADV")
    print("impact model, rights/dividends unadjusted, ~5yr window.")


if __name__ == "__main__":
    main()
