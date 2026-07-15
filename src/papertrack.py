"""
Paper-track runner — forms the live momentum+value composite portfolio and
records it to the append-only paper_portfolio table. This starts the
out-of-sample clock that the backtest's Deflated Sharpe (0.82) cannot close.

Universe: liquid names (market-cap filtered) that have both price history and
fundamentals in the Dawn panel. Long-only, top-quintile of the composite,
equal-weighted. Same factor logic as scripts/xsection_backtest.py.

Entry prices default to the latest Dawn close but can be overridden with live
Kite LTPs (passed in by the caller, which has the MCP connection).
"""
from __future__ import annotations

import os
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from src import ledger  # reuse git_commit()

DAWN_URL = os.environ.get("DAWN_URL", "postgresql://localhost:5432/dawn")
BHARAT_URL = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/bharattwin")

STRATEGY = "momentum_value_composite_v1"            # Core (primary), fully invested, ungated
STRATEGY_TREND = "momentum_value_trend_v1"          # Core + Tier-1.2 trend overlay
STRATEGY_QUALITY = "momentum_value_quality_v1"      # Core + Tier-2 forensic gate
STRATEGY_QTILT = "momentum_value_qtilt_v1"          # Core + quality-score tilt (3rd-edge test)
STRATEGY_SMALLCAP = "momentum_value_smallcap_v1"    # Core + small-cap tilt (mcap ≤ ₹8000cr)
SMALLCAP_MAX_CR = 8000.0
MIN_PRICE = 10.0
MIN_MCAP_CR = 2000.0      # ₹2000 cr market-cap floor ≈ liquid (Nifty-500-ish)
REPORT_LAG_DAYS = 90
TOP_QUANTILE = 5          # rank threshold for the signal
MAX_HOLDINGS = 25         # concentration cap so a ~₹1L book is actually tradeable
TREND_MA_DAYS = 200       # Tier-1.2 trend overlay: equal-weight index vs its 200d MA

# The pre-registered book set. Core is PRIMARY — the clean out-of-sample proof of
# the validated edge. Trend and Quality are single-variable attribution satellites
# (each differs from Core by exactly one thing). Do NOT reallocate capital to the
# live leader — these measure the *marginal* value of each overlay, they do not
# compete. Correlated by construction (shared momentum+value core).
VARIANTS = [
    {"strategy": STRATEGY,          "label": "Core",          "overlay": False, "quality_gate": False, "quality_tilt": False, "mcap_max": None,            "primary": True},
    {"strategy": STRATEGY_TREND,    "label": "Trend overlay", "overlay": True,  "quality_gate": False, "quality_tilt": False, "mcap_max": None,            "primary": False},
    {"strategy": STRATEGY_QUALITY,  "label": "Quality gate",  "overlay": False, "quality_gate": True,  "quality_tilt": False, "mcap_max": None,            "primary": False},
    {"strategy": STRATEGY_QTILT,    "label": "Quality tilt",  "overlay": False, "quality_gate": False, "quality_tilt": True,  "mcap_max": None,            "primary": False},
    {"strategy": STRATEGY_SMALLCAP, "label": "Small-cap tilt","overlay": False, "quality_gate": False, "quality_tilt": False, "mcap_max": SMALLCAP_MAX_CR, "primary": False},
]


def _market_regime(px: pd.DataFrame, t: pd.Timestamp, ma_days: int = TREND_MA_DAYS) -> dict:
    """Tier-1.2 trend / tail-risk overlay. Build the equal-weight universe daily
    index up to t and compare its level to its `ma_days` moving average. Below the
    MA => risk-off => de-risk the book to cash (target_exposure 0). This turned a
    -38% max drawdown into ~-19% in the backtest (docs/tier1_upgrades_results.md).
    Uses only information available at t (no look-ahead)."""
    dret = px.loc[:t].pct_change().clip(-0.20, 0.20)
    idx = (1 + dret.mean(axis=1, skipna=True).fillna(0)).cumprod()
    if len(idx) < ma_days:
        return {"risk_on": True, "target_exposure": 1.0, "index_level": None,
                "index_ma": None, "ma_days": ma_days, "note": "insufficient history"}
    level = float(idx.iloc[-1])
    ma = float(idx.rolling(ma_days, min_periods=max(100, ma_days // 2)).mean().iloc[-1])
    risk_on = level >= ma
    return {"risk_on": bool(risk_on), "target_exposure": 1.0 if risk_on else 0.0,
            "index_level": round(level, 4), "index_ma": round(ma, 4), "ma_days": ma_days}


def _z(s: pd.Series) -> pd.Series:
    s = s.replace([np.inf, -np.inf], np.nan)
    return (s - s.mean()) / s.std() if s.std() else s * 0.0


def _adjust_prices(px: pd.DataFrame, ca: pd.DataFrame) -> pd.DataFrame:
    adj = px.copy()
    for _, r in ca.iterrows():
        sym = r["symbol"]
        if sym not in adj.columns or not r["ratio_from"] or not r["ratio_to"]:
            continue
        factor = (r["ratio_to"] / r["ratio_from"]) if r["ca_type"] == "split" else (r["ratio_to"] / (r["ratio_from"] + r["ratio_to"]))
        if 0 < factor < 1.0:
            adj.loc[adj.index < r["ex_date"], sym] *= factor
    return adj


def compute_portfolio(as_of: date | None = None, overlay: bool = False,
                      quality_gate: bool = False, quality_tilt: bool = False,
                      mcap_max: float | None = None, strategy: str | None = None) -> dict:
    """Form the composite top-quintile long-only portfolio as of `as_of`
    (default: latest available Dawn date).

    overlay=True applies the Tier-1.2 trend de-risk (scale book to cash below MA).
    quality_gate=True drops Tier-2 forensically-flagged names from the candidate pool.
    quality_tilt=True adds the (orthogonal) quality z-score as a 4th ranking leg —
      the live test of quality-as-third-edge (can't be backtested: no fundamental history).
    mcap_max caps the universe market cap (₹cr) = small-cap tilt (edge is strongest in
      smaller caps by factor efficacy; docs/third_edge_and_cap_results.md).
    strategy names the book. The regime is always computed (informational if overlay=False)."""
    strategy = strategy or STRATEGY
    eng = create_engine(DAWN_URL)
    px = pd.read_sql(text("SELECT symbol,date,close FROM stock_prices_daily WHERE symbol IS NOT NULL AND close>0"), eng)
    px["date"] = pd.to_datetime(px["date"])
    px = px[px["symbol"].str.match(r"^[A-Z][A-Z0-9&-]{1,}$", na=False)]
    px = px.pivot_table(index="date", columns="symbol", values="close", aggfunc="last").sort_index()

    ca = pd.read_sql(text("""SELECT symbol,ex_date,ca_type,ratio_from,ratio_to FROM corporate_actions
                             WHERE ca_type IN ('split','bonus') AND ratio_from IS NOT NULL AND ratio_to IS NOT NULL"""), eng)
    ca["ex_date"] = pd.to_datetime(ca["ex_date"])
    px = _adjust_prices(px, ca)

    f = pd.read_sql(text("""SELECT symbol,period_end,basis,ttm_eps,bvps,shares_outstanding
                            FROM security_fundamentals WHERE symbol ~ '^[A-Z][A-Z0-9&-]{1,}$'"""), eng)
    eng.dispose()
    f["period_end"] = pd.to_datetime(f["period_end"])
    f["available"] = f["period_end"] + pd.Timedelta(days=REPORT_LAG_DAYS)
    f["basis_rank"] = (f["basis"] == "consolidated").astype(int)
    f = f.sort_values(["symbol", "available", "basis_rank"])

    t = pd.Timestamp(as_of) if as_of else px.index.max()
    px_t = px.loc[:t]
    price_t = px_t.iloc[-1]

    def asof_field(field):
        sub = f.dropna(subset=[field])
        w = sub[sub["available"] <= t].groupby("symbol")[field].last()
        return w

    bvps, eps, shr = asof_field("bvps"), asof_field("ttm_eps"), asof_field("shares_outstanding")
    mcap_cr = (price_t * shr) / 1e7   # shares×price in ₹, ÷1e7 = ₹ crore

    # liquid universe (₹MIN_MCAP_CR floor; optional mcap_max ceiling = small-cap tilt)
    liquid = price_t[(price_t >= MIN_PRICE)].index
    liquid = mcap_cr.reindex(liquid).dropna()
    liquid = liquid[liquid >= MIN_MCAP_CR]
    if mcap_max is not None:
        liquid = liquid[liquid <= mcap_max]
    liquid = liquid.index

    # factors (PIT)
    mom = px_t.iloc[-252:-21].apply(lambda c: c.dropna().iloc[-1] / c.dropna().iloc[0] - 1 if c.dropna().shape[0] > 200 else np.nan)
    value_ey = (eps.reindex(liquid) / price_t.reindex(liquid))
    value_pb = -(price_t.reindex(liquid) / bvps.reindex(liquid))

    legs = [_z(mom.reindex(liquid)), _z(value_ey), _z(value_pb)]
    factor_names = ["momentum_12_1", "value_ey", "value_pb"]
    # Tier-2 quality TILT: add the (orthogonal) quality z-score as a 4th leg
    if quality_tilt:
        from src import quality
        qdf = quality.load_quality(t.date().isoformat())
        qz = _z(qdf["quality_score"].reindex(liquid)) if not qdf.empty else pd.Series(index=liquid, dtype=float)
        legs.append(qz)
        factor_names.append("quality_score")

    comp = pd.concat(legs, axis=1).mean(axis=1, skipna=True).dropna()

    if comp.empty:
        raise RuntimeError("no names passed the universe/factor filter")

    # Tier-2 quality gate: drop forensically-flagged names from the candidate pool
    gate = {"applied": quality_gate, "dropped": [], "n_dropped": 0}
    if quality_gate:
        from src import quality
        q = quality.load_quality(t.date().isoformat())
        flagged = set(q.index[q["red_flags"].map(len) > 0]) if not q.empty else set()
        dropped = [s for s in comp.index if s in flagged]
        comp = comp.drop(index=dropped)
        gate.update(dropped=dropped, n_dropped=len(dropped))
        if comp.empty:
            raise RuntimeError("quality gate removed the entire candidate pool")

    n_top = min(max(int(len(comp) / TOP_QUANTILE), 1), MAX_HOLDINGS)
    top = comp.sort_values(ascending=False).head(n_top)

    # Tier-1.2 trend overlay: scale the whole book by target_exposure (0 = flat/cash).
    # Regime is always computed; exposure only bites when overlay=True.
    regime = _market_regime(px_t, t)
    exposure = regime["target_exposure"] if overlay else 1.0
    weight = round(exposure / len(top), 6)      # per-name weight after de-risking
    cash_weight = round(1.0 - exposure, 6)      # 1.0 when risk-off (book in cash)

    holdings = []
    for sym, z in top.items():
        holdings.append({
            "symbol": sym,
            "weight": weight,
            "entry_price": round(float(price_t.get(sym, np.nan)), 2),
            "composite_z": round(float(z), 4),
            "mktcap_cr": round(float(mcap_cr.get(sym, np.nan)), 1),
        })

    return {
        "as_of": t.date().isoformat(),
        "strategy": strategy,
        "universe_size": int(len(comp)),
        "n_holdings": len(holdings),
        "holdings": holdings,
        "regime": regime,
        "overlay": overlay,
        "quality_gate": gate,
        "quality_tilt": quality_tilt,
        "mcap_max": mcap_max,
        "target_exposure": exposure,
        "cash_weight": cash_weight,
        "params": {"min_mcap_cr": MIN_MCAP_CR, "mcap_max_cr": mcap_max, "min_price": MIN_PRICE,
                   "top_quantile": TOP_QUANTILE, "max_holdings": MAX_HOLDINGS,
                   "report_lag_days": REPORT_LAG_DAYS, "trend_ma_days": TREND_MA_DAYS,
                   "overlay": overlay, "quality_gate": quality_gate, "quality_tilt": quality_tilt,
                   "factors": factor_names},
    }


def existing_portfolio_for(as_of_date: str, strategy: str = STRATEGY) -> int | None:
    """Return the id of any already-recorded book for this (as_of_date, strategy),
    regardless of model_version. The paper track must hold ONE book per formation
    date; a code change bumps model_version and would otherwise let a same-day
    duplicate slip past the DB uniqueness constraint (which includes model_version)."""
    eng = create_engine(BHARAT_URL)
    try:
        with eng.begin() as conn:
            row = conn.execute(text(
                "SELECT id FROM paper_portfolio WHERE as_of_date=:d AND strategy=:s ORDER BY id LIMIT 1"),
                {"d": as_of_date, "s": strategy}).fetchone()
    finally:
        eng.dispose()
    return row[0] if row else None


def record_portfolio(pf: dict, notional: float = 100_000.0, live_prices: dict | None = None,
                     force: bool = False) -> int | None:
    """Append the portfolio to paper_portfolio. If live_prices {symbol: ltp} is
    given (e.g. from Kite), it overrides the Dawn close as the entry price.
    Returns the new id, or None if already recorded (idempotent).

    A book for the same as_of_date already existing (under any model_version) blocks
    the insert and returns None unless force=True, so re-running the monthly job
    after a code change cannot silently double-count a formation date."""
    if not force:
        dup = existing_portfolio_for(pf["as_of"], pf["strategy"])
        if dup is not None:
            return None
    holdings = [dict(h) for h in pf["holdings"]]
    if live_prices:
        for h in holdings:
            if h["symbol"] in live_prices and live_prices[h["symbol"]]:
                h["entry_price"] = round(float(live_prices[h["symbol"]]), 2)
                h["entry_source"] = "kite_ltp"
            else:
                h["entry_source"] = "dawn_close"

    import json
    row = {
        "as_of_date": pf["as_of"], "strategy": pf["strategy"], "model_version": ledger.git_commit(),
        "universe_size": pf["universe_size"], "n_holdings": pf["n_holdings"], "notional": notional,
        "holdings": json.dumps(holdings), "params": json.dumps(pf["params"]),
    }
    eng = create_engine(BHARAT_URL)
    try:
        with eng.begin() as conn:
            res = conn.execute(text("""
                INSERT INTO paper_portfolio
                    (as_of_date, strategy, model_version, universe_size, n_holdings, notional, holdings, params)
                VALUES (:as_of_date, :strategy, :model_version, :universe_size, :n_holdings, :notional,
                        CAST(:holdings AS jsonb), CAST(:params AS jsonb))
                ON CONFLICT ON CONSTRAINT uq_paper_portfolio_asm DO NOTHING
                RETURNING id
            """), row).fetchone()
    finally:
        eng.dispose()
    return res[0] if res else None


def score_portfolio(portfolio_id: int, exit_prices: dict, exit_date: str,
                    benchmark_return_pct: float | None = None) -> dict | None:
    """Score a recorded paper portfolio against exit prices (e.g. live Kite LTPs).
    Equal-weighted holding returns, net of one round-trip delivery cost + impact
    proxy. Appends an immutable row to paper_portfolio_pnl (idempotent)."""
    import json
    from src.costs import round_trip_cost
    cost_pct = round_trip_cost(1_000_000, "equity_delivery").total_bps / 100 + 0.25  # + impact proxy %

    eng = create_engine(BHARAT_URL)
    try:
        with eng.begin() as conn:
            pf = conn.execute(text("SELECT holdings FROM paper_portfolio WHERE id=:i"), {"i": portfolio_id}).fetchone()
            if not pf:
                return None
            holdings = pf[0] if isinstance(pf[0], list) else json.loads(pf[0])
            # weight-aware: respect stored per-name weights (Tier-1.2 de-risking scales
            # them by exposure; the 1-sum(weights) remainder sits in cash at 0% return).
            # Legacy equal-weight books (weights sum ~1) reduce to the simple mean.
            rets, hp, w_scored, wr = [], [], 0.0, 0.0
            for h in holdings:
                ep, xp = h.get("entry_price"), exit_prices.get(h["symbol"])
                w = float(h.get("weight", 0.0) or 0.0)
                if ep and xp:
                    r = (xp - ep) / ep * 100
                    rets.append(r)
                    w_scored += w
                    wr += w * r
                    hp.append({"symbol": h["symbol"], "entry": ep, "exit": xp,
                               "ret_pct": round(r, 2), "weight": round(w, 6)})
            if not rets:
                return None
            import statistics
            exposure = sum(float(h.get("weight", 0.0) or 0.0) for h in holdings)
            if w_scored > 0 and exposure > 0:
                # renormalise over names we could actually price, then re-apply exposure
                # so the cash sleeve (incl. de-risking) correctly contributes 0%.
                gross = (wr / w_scored) * exposure
            else:
                gross = statistics.fmean(rets)   # legacy fallback (no usable weights)
            net = gross - cost_pct * exposure     # only the invested sleeve pays cost
            excess = (gross - benchmark_return_pct) if benchmark_return_pct is not None else None
            conn.execute(text("""
                INSERT INTO paper_portfolio_pnl
                    (portfolio_id, exit_date, gross_return_pct, net_return_pct, benchmark_return_pct, excess_return_pct, holdings_pnl)
                VALUES (:pid, :xd, :g, :n, :b, :e, CAST(:hp AS jsonb))
                ON CONFLICT ON CONSTRAINT uq_paper_pnl_portfolio_exit DO NOTHING
            """), {"pid": portfolio_id, "xd": exit_date, "g": round(gross, 4), "n": round(net, 4),
                   "b": benchmark_return_pct, "e": round(excess, 4) if excess is not None else None,
                   "hp": json.dumps(hp)})
    finally:
        eng.dispose()
    return {"portfolio_id": portfolio_id, "exit_date": exit_date, "gross_pct": round(gross, 3),
            "net_pct": round(net, 3), "benchmark_pct": benchmark_return_pct,
            "excess_pct": round(excess, 3) if excess is not None else None, "n_scored": len(rets)}
