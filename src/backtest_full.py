"""
Full 20-year backtest — factor-based direction prediction on all 5,009 trading days.

No LLM calls. Uses computed factors (momentum, VIX regime, volatility) to simulate
what the agent archetypes would predict, then scores against actual next-day returns.

This establishes the baseline that the autoresearch loop optimizes against.

Usage:
    python -m src.backtest_full              # run full backtest
    python -m src.backtest_full --regime     # breakdown by market regime
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


@dataclass
class DayResult:
    date: str
    nifty_close: float
    next_close: float
    actual_return_pct: float
    predicted_direction: str  # BUY/SELL/HOLD
    predicted_return_pct: float
    direction_correct: bool
    error_pp: float
    regime: str  # bull/bear/sideways/crisis
    vix: float | None = None
    momentum_5d: float | None = None


# ─── Factor-Based Prediction Rules ──────────────────────────────────────────
# These simulate what the 10 agent archetypes would produce, using simple
# rules derived from their persona logic. The autoresearch loop optimizes
# the thresholds and weights below.

# Optimizable parameters (this is what autoresearch modifies)
PARAMS = {
    # Momentum thresholds
    "mom_1d_bull": 0.8,       # 1d return > this = bullish signal
    "mom_1d_bear": -0.8,      # 1d return < this = bearish signal
    "mom_5d_bull": 1.5,       # 5d momentum > this = trend up
    "mom_5d_bear": -1.5,

    # VIX regime
    "vix_fear": 20.0,         # VIX above this = elevated fear
    "vix_complacency": 13.0,  # VIX below this = complacent

    # Volatility
    "vol_high": 20.0,         # 20d vol above this = high vol regime

    # Mean reversion strength
    "mean_revert_threshold": 2.0,  # If 1d move > this, expect partial reversion

    # Signal weights (what autoresearch optimizes most)
    "w_momentum": 0.30,
    "w_mean_revert": 0.25,
    "w_vix_regime": 0.20,
    "w_trend": 0.15,
    "w_volatility": 0.10,
}


def _compute_signal(
    mom_1d: float,
    mom_5d: float | None,
    mom_20d: float | None,
    vix: float | None,
    vol_20d: float | None,
    params: dict = PARAMS,
) -> tuple[str, float]:
    """
    Compute a directional signal from factors.

    Returns (direction, expected_return_pct).
    """
    signals = {}

    # 1. Momentum signal: follow the trend
    if mom_1d > params["mom_1d_bull"]:
        signals["momentum"] = ("BUY", min(mom_1d * 0.3, 1.5))
    elif mom_1d < params["mom_1d_bear"]:
        signals["momentum"] = ("SELL", max(mom_1d * 0.3, -1.5))
    else:
        signals["momentum"] = ("HOLD", 0.0)

    # 2. Mean reversion: large moves tend to partially revert
    if abs(mom_1d) > params["mean_revert_threshold"]:
        revert_mag = -mom_1d * 0.3  # Expect 30% reversion
        signals["mean_revert"] = ("BUY" if revert_mag > 0 else "SELL", revert_mag)
    else:
        signals["mean_revert"] = ("HOLD", 0.0)

    # 3. VIX regime
    if vix is not None:
        if vix > params["vix_fear"]:
            signals["vix"] = ("SELL", -0.3)  # Fear = bearish
        elif vix < params["vix_complacency"]:
            signals["vix"] = ("BUY", 0.2)   # Complacency = mildly bullish
        else:
            signals["vix"] = ("HOLD", 0.0)
    else:
        signals["vix"] = ("HOLD", 0.0)

    # 4. Trend (5d momentum)
    if mom_5d is not None:
        if mom_5d > params["mom_5d_bull"]:
            signals["trend"] = ("BUY", mom_5d * 0.1)
        elif mom_5d < params["mom_5d_bear"]:
            signals["trend"] = ("SELL", mom_5d * 0.1)
        else:
            signals["trend"] = ("HOLD", 0.0)
    else:
        signals["trend"] = ("HOLD", 0.0)

    # 5. Volatility: high vol = reduce conviction, widen range
    if vol_20d is not None and vol_20d > params["vol_high"]:
        signals["volatility"] = ("HOLD", 0.0)  # High vol = uncertain
    else:
        signals["volatility"] = ("HOLD", 0.0)

    # Weighted combination
    dir_score = 0.0
    return_est = 0.0
    weights = {
        "momentum": params["w_momentum"],
        "mean_revert": params["w_mean_revert"],
        "vix": params["w_vix_regime"],
        "trend": params["w_trend"],
        "volatility": params["w_volatility"],
    }

    for sig_name, (direction, ret) in signals.items():
        w = weights.get(sig_name, 0.1)
        if direction == "BUY":
            dir_score += w
        elif direction == "SELL":
            dir_score -= w
        return_est += ret * w

    if dir_score > 0.15:
        final_dir = "BUY"
    elif dir_score < -0.15:
        final_dir = "SELL"
    else:
        final_dir = "HOLD"

    return final_dir, round(return_est, 4)


def _classify_regime(nifty: float, vix: float | None, mom_20d: float | None) -> str:
    """Classify the market regime for a given day."""
    if vix is not None and vix > 30:
        return "crisis"
    if mom_20d is not None:
        if mom_20d > 5:
            return "bull"
        elif mom_20d < -5:
            return "bear"
    return "sideways"


def run_full_backtest(params: dict | None = None) -> list[DayResult]:
    """
    Run the factor-based backtest on all available trading days.

    Returns list of DayResult for each day where we have data.
    """
    if params is None:
        params = PARAMS

    engine = create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT session_id, nifty50_close, banknifty_close, india_vix
            FROM market_state
            WHERE nifty50_close IS NOT NULL AND universe_id = 'nifty50'
            ORDER BY session_id
        """)).fetchall()

    engine.dispose()

    if len(rows) < 2:
        return []

    results: list[DayResult] = []

    # Compute rolling factors from the price series
    closes = [(r[0], float(r[1]), float(r[2]) if r[2] else None, float(r[3]) if r[3] else None) for r in rows]

    for i in range(1, len(closes) - 1):
        sid, nifty, bn, vix = closes[i]
        _, next_nifty, _, _ = closes[i + 1]
        _, prev_nifty, _, _ = closes[i - 1]

        actual_return = (next_nifty - nifty) / nifty * 100
        mom_1d = (nifty - prev_nifty) / prev_nifty * 100

        # 5d momentum
        if i >= 5:
            mom_5d = (nifty - closes[i - 5][1]) / closes[i - 5][1] * 100
        else:
            mom_5d = None

        # 20d momentum
        if i >= 20:
            mom_20d = (nifty - closes[i - 20][1]) / closes[i - 20][1] * 100
        else:
            mom_20d = None

        # 20d volatility
        if i >= 20:
            daily_returns = []
            for j in range(i - 19, i + 1):
                dr = (closes[j][1] - closes[j - 1][1]) / closes[j - 1][1] * 100
                daily_returns.append(dr)
            mean_dr = sum(daily_returns) / len(daily_returns)
            vol_20d = math.sqrt(sum((r - mean_dr) ** 2 for r in daily_returns) / 19) * math.sqrt(252)
        else:
            vol_20d = None

        # Predict
        pred_dir, pred_ret = _compute_signal(mom_1d, mom_5d, mom_20d, vix, vol_20d, params)

        # Score
        actual_dir = "BUY" if actual_return > 0.25 else ("SELL" if actual_return < -0.25 else "HOLD")
        dir_correct = (pred_dir == actual_dir) or (pred_dir == "HOLD" and abs(actual_return) < 0.5)
        error = abs(pred_ret - actual_return)

        regime = _classify_regime(nifty, vix, mom_20d)

        results.append(DayResult(
            date=sid.replace("CM_", ""),
            nifty_close=nifty,
            next_close=next_nifty,
            actual_return_pct=round(actual_return, 4),
            predicted_direction=pred_dir,
            predicted_return_pct=pred_ret,
            direction_correct=dir_correct,
            error_pp=round(error, 4),
            regime=regime,
            vix=vix,
            momentum_5d=round(mom_5d, 2) if mom_5d else None,
        ))

    return results


def score_results(results: list[DayResult]) -> dict:
    """Compute aggregate scores from backtest results."""
    if not results:
        return {}

    n = len(results)
    dir_correct = sum(1 for r in results if r.direction_correct)
    avg_error = sum(r.error_pp for r in results) / n
    rmse = math.sqrt(sum(r.error_pp ** 2 for r in results) / n)

    # Regime breakdown
    regimes: dict[str, list[DayResult]] = {}
    for r in results:
        regimes.setdefault(r.regime, []).append(r)

    regime_scores = {}
    for regime, rr in regimes.items():
        rn = len(rr)
        regime_scores[regime] = {
            "count": rn,
            "direction_pct": round(sum(1 for r in rr if r.direction_correct) / rn * 100, 1),
            "avg_error": round(sum(r.error_pp for r in rr) / rn, 2),
        }

    # Big move days (|return| > 2%)
    big_days = [r for r in results if abs(r.actual_return_pct) > 2.0]
    big_dir = sum(1 for r in big_days if r.direction_correct) / len(big_days) * 100 if big_days else 0

    return {
        "total_days": n,
        "direction_correct": dir_correct,
        "direction_pct": round(dir_correct / n * 100, 1),
        "avg_error_pp": round(avg_error, 2),
        "rmse_pp": round(rmse, 2),
        "regime_breakdown": regime_scores,
        "big_move_days": len(big_days),
        "big_move_direction_pct": round(big_dir, 1),
    }


def print_backtest_report(results: list[DayResult]) -> None:
    """Print full backtest report."""
    scores = score_results(results)

    print(f"\n{'=' * 65}")
    print(f"FULL BACKTEST REPORT — {scores['total_days']:,} trading days")
    print(f"{'=' * 65}")
    print(f"  Direction accuracy:  {scores['direction_pct']}% ({scores['direction_correct']:,}/{scores['total_days']:,})")
    print(f"  Average error:       {scores['avg_error_pp']}pp")
    print(f"  RMSE:                {scores['rmse_pp']}pp")
    print(f"  Big move days (>2%): {scores['big_move_direction_pct']}% direction ({scores['big_move_days']} days)")

    print(f"\n  Regime breakdown:")
    for regime, rs in sorted(scores["regime_breakdown"].items()):
        print(f"    {regime:<10} {rs['count']:>5} days | dir={rs['direction_pct']:>5.1f}% | err={rs['avg_error']:.2f}pp")

    # Year breakdown
    yearly: dict[str, list[DayResult]] = {}
    for r in results:
        year = r.date[:4]
        yearly.setdefault(year, []).append(r)

    print(f"\n  Yearly breakdown:")
    print(f"  {'Year':<6} {'Days':>5} {'Dir%':>6} {'AvgErr':>8} {'BigDays':>8}")
    print(f"  {'-' * 36}")
    for year in sorted(yearly.keys()):
        yr = yearly[year]
        yn = len(yr)
        yd = sum(1 for r in yr if r.direction_correct) / yn * 100
        ye = sum(r.error_pp for r in yr) / yn
        yb = sum(1 for r in yr if abs(r.actual_return_pct) > 2.0)
        print(f"  {year:<6} {yn:>5} {yd:>5.1f}% {ye:>7.2f}pp {yb:>7}")

    print(f"{'=' * 65}")


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.WARNING)

    print("Running 20-year factor-based backtest...")
    results = run_full_backtest()
    print_backtest_report(results)
