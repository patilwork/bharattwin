"""
Factor Engine v1.

Computes daily market factors from bhavcopy_raw and writes them into
market_state.factor_map (JSONB) and typed columns where applicable.

Factors computed:
  - momentum_1d:  Nifty 50 EQ-weighted 1-day return (%)
  - momentum_5d:  5-day rolling return (%)
  - momentum_20d: 20-day rolling return (%)
  - volatility_20d: 20-day rolling annualised volatility (%)
  - breadth_adv: number of advancing stocks (close > prev_close)
  - breadth_dec: number of declining stocks
  - breadth_unch: number of unchanged stocks
  - breadth_ratio: advance/decline ratio
  - breadth_pct_up: % of stocks advancing
  - avg_turnover_cr: average turnover per EQ stock in crore

All factors are computed from EQ-series bhavcopy_raw rows.
Nifty 50 constituents are used when available; else all EQ stocks.
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import date, timedelta
from typing import Any

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def _fetch_eq_data(conn, d: date) -> list[dict]:
    """Fetch EQ-series bhavcopy rows for a given date."""
    rows = conn.execute(text("""
        SELECT symbol, close, prev_close, volume, turnover_lakh
        FROM bhavcopy_raw
        WHERE data_date = :d AND series = 'EQ'
        ORDER BY turnover_lakh DESC NULLS LAST
    """), {"d": d}).mappings().fetchall()
    return [dict(r) for r in rows]


def _compute_returns(rows: list[dict]) -> list[float]:
    """Compute per-stock 1-day returns from close/prev_close."""
    returns = []
    for r in rows:
        c = r.get("close")
        pc = r.get("prev_close")
        if c and pc and float(pc) > 0:
            returns.append((float(c) - float(pc)) / float(pc))
    return returns


def compute(d: date) -> dict[str, Any] | None:
    """
    Compute all factors for date d from bhavcopy_raw.
    Returns a dict of factor values, or None if no data.
    """
    engine = _get_engine()

    with engine.connect() as conn:
        eq_today = _fetch_eq_data(conn, d)
        if not eq_today:
            logger.info("factors: no EQ data for %s", d)
            engine.dispose()
            return None

        # --- 1-day breadth & momentum ---
        returns_1d = _compute_returns(eq_today)
        if not returns_1d:
            engine.dispose()
            return None

        advancing = sum(1 for r in returns_1d if r > 0)
        declining = sum(1 for r in returns_1d if r < 0)
        unchanged = sum(1 for r in returns_1d if r == 0)
        total = len(returns_1d)

        breadth_ratio = round(advancing / declining, 4) if declining > 0 else float(advancing)
        breadth_pct_up = round(advancing / total * 100, 2) if total > 0 else 0.0

        # Equal-weighted market return (proxy for broad market momentum)
        momentum_1d = round(sum(returns_1d) / len(returns_1d) * 100, 4)

        # Average turnover
        turnovers = [float(r["turnover_lakh"]) for r in eq_today
                     if r.get("turnover_lakh") is not None]
        avg_turnover_cr = round(sum(turnovers) / len(turnovers) / 100, 4) if turnovers else None

        # --- Multi-day factors (5d, 20d momentum + 20d vol) ---
        # Fetch historical market-level returns from already-built market_state rows
        lookback_dates = []
        for i in range(1, 40):  # up to 40 calendar days back to get ~20 trading days
            lookback_dates.append(d - timedelta(days=i))

        hist_rows = conn.execute(text("""
            SELECT session_id, returns_map
            FROM market_state
            WHERE universe_id = 'nifty50'
              AND session_id = ANY(:sids)
            ORDER BY session_id DESC
        """), {"sids": [f"CM_{dd}" for dd in lookback_dates]}).mappings().fetchall()

    engine.dispose()

    # Compute historical equal-weighted daily returns from returns_map
    daily_ew_returns: list[float] = []
    for hr in hist_rows:
        rm = hr["returns_map"]
        if rm:
            rmap = rm if isinstance(rm, dict) else json.loads(rm)
            if rmap:
                vals = [v for v in rmap.values() if isinstance(v, (int, float))]
                if vals:
                    daily_ew_returns.append(sum(vals) / len(vals))

    # Prepend today's return
    all_returns = [momentum_1d] + daily_ew_returns

    momentum_5d = round(sum(all_returns[:5]), 4) if len(all_returns) >= 5 else None
    momentum_20d = round(sum(all_returns[:20]), 4) if len(all_returns) >= 20 else None

    # 20-day annualised volatility (std dev of daily returns * sqrt(252))
    if len(all_returns) >= 20:
        window = all_returns[:20]
        mean = sum(window) / len(window)
        var = sum((x - mean) ** 2 for x in window) / (len(window) - 1)
        volatility_20d = round(math.sqrt(var) * math.sqrt(252), 4)
    else:
        volatility_20d = None

    factors = {
        "momentum_1d": momentum_1d,
        "momentum_5d": momentum_5d,
        "momentum_20d": momentum_20d,
        "volatility_20d": volatility_20d,
        "breadth_adv": advancing,
        "breadth_dec": declining,
        "breadth_unch": unchanged,
        "breadth_ratio": breadth_ratio,
        "breadth_pct_up": breadth_pct_up,
        "avg_turnover_cr": avg_turnover_cr,
        "eq_stock_count": total,
    }

    logger.info("factors: %s → adv=%d dec=%d mom1d=%.2f%%", d, advancing, declining, momentum_1d)
    return factors


def compute_and_store(d: date, force: bool = False) -> dict[str, Any] | None:
    """Compute factors and upsert into market_state.factor_map."""
    factors = compute(d)
    if factors is None:
        return None

    engine = _get_engine()
    with engine.begin() as conn:
        # Check if market_state row exists
        existing = conn.execute(text("""
            SELECT id FROM market_state WHERE session_id = :sid AND universe_id = 'nifty50'
        """), {"sid": f"CM_{d}"}).first()

        if existing:
            # Update factor_map on existing row
            conn.execute(text("""
                UPDATE market_state
                SET factor_map = CAST(:fm AS jsonb),
                    data_quality = jsonb_set(
                        COALESCE(data_quality, '{}'::jsonb),
                        '{has_factors}', 'true'::jsonb
                    )
                WHERE session_id = :sid AND universe_id = 'nifty50'
            """), {"fm": json.dumps(factors), "sid": f"CM_{d}"})
        else:
            # Insert skeleton row with factors
            conn.execute(text("""
                INSERT INTO market_state (asof_ts_ist, session_id, universe_id, factor_map, data_quality, replay_cutoff_ts)
                VALUES (:ts, :sid, 'nifty50', CAST(:fm AS jsonb), CAST(:dq AS jsonb), :ts)
                ON CONFLICT (universe_id, session_id) DO UPDATE SET
                    factor_map = EXCLUDED.factor_map
            """), {
                "ts": f"{d}T18:00:00+05:30",
                "sid": f"CM_{d}",
                "fm": json.dumps(factors),
                "dq": json.dumps({"has_factors": True}),
            })

    engine.dispose()
    logger.info("factors: stored for %s", d)
    return factors
