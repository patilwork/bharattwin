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
# v2: expanded from 13 to 35 parameters
PARAMS = {
    # ── Momentum thresholds ──
    "mom_1d_bull": 0.8,
    "mom_1d_bear": -0.8,
    "mom_5d_bull": 1.5,
    "mom_5d_bear": -1.5,
    "mom_20d_bull": 5.0,        # NEW: 20d trend confirmation
    "mom_20d_bear": -5.0,

    # ── VIX regime thresholds ──
    "vix_fear": 20.0,
    "vix_complacency": 13.0,
    "vix_crisis": 30.0,         # NEW: panic threshold
    "vix_spike_1d": 3.0,        # NEW: 1-day VIX jump = fear signal

    # ── Volatility ──
    "vol_high": 20.0,
    "vol_penalty": 0.5,         # NEW: reduce return estimate in high-vol

    # ── Mean reversion ──
    "mean_revert_threshold": 2.0,
    "mean_revert_scale": 0.3,   # NEW: tunable (was hardcoded 0.3)
    "oversold_bounce": 3.0,     # NEW: if 1d drop > this, expect bounce
    "overbought_reversal": 3.0, # NEW: if 1d rally > this, expect fade

    # ── Magnitude scales (were hardcoded) ──
    "momentum_return_scale": 0.3,  # NEW: mom_1d * this
    "trend_return_scale": 0.1,     # NEW: mom_5d * this
    "vix_bearish_return": -0.3,    # NEW: return when VIX > fear
    "vix_bullish_return": 0.2,     # NEW: return when VIX < complacency

    # ── Direction thresholds ──
    "dir_buy_threshold": 0.15,     # NEW: dir_score > this = BUY
    "dir_sell_threshold": -0.15,   # NEW: dir_score < this = SELL

    # ── Breadth signal ──
    "w_breadth": 0.10,            # NEW: breadth signal weight
    "breadth_bull": 60.0,         # NEW: breadth_pct_up > this = bullish
    "breadth_bear": 35.0,         # NEW: breadth_pct_up < this = bearish

    # ── 20d trend signal ──
    "w_trend_20d": 0.08,          # NEW: 20d trend weight

    # ── VIX change signal ──
    "w_vix_change": 0.07,         # NEW: 1-day VIX change weight

    # ── USDINR signal ──
    "usdinr_depreciation_threshold": 0.3,  # 1d INR weakening > this = bearish
    "usdinr_appreciation_threshold": -0.3, # 1d INR strengthening < this = bullish
    "w_usdinr": 0.05,

    # ── Futures OI signal ──
    "oi_rise_threshold": 3.0,     # OI up > 3% = positioning signal
    "oi_drop_threshold": -3.0,    # OI down > 3% = unwinding
    "w_oi_position": 0.05,

    # ── Signal weights ──
    "w_momentum": 0.22,
    "w_mean_revert": 0.18,
    "w_vix_regime": 0.12,
    "w_trend": 0.13,
    "w_volatility": 0.05,
    # Note: w_breadth, w_trend_20d, w_vix_change, w_usdinr, w_oi_position are separate above
}


def _compute_signal(
    mom_1d: float,
    mom_5d: float | None,
    mom_20d: float | None,
    vix: float | None,
    vix_prev: float | None,
    vol_20d: float | None,
    breadth_pct_up: float | None,
    params: dict = PARAMS,
    usdinr_change: float | None = None,
    oi_change_pct: float | None = None,
) -> tuple[str, float]:
    """
    Compute a directional signal from factors. v3 with 10 signal types.

    New in v3: real breadth from bhavcopy, USDINR change, futures OI change.
    Returns (direction, expected_return_pct).
    """
    signals = {}
    mom_scale = params.get("momentum_return_scale", 0.3)
    trend_scale = params.get("trend_return_scale", 0.1)
    mr_scale = params.get("mean_revert_scale", 0.3)

    # 1. Momentum: follow 1d move
    if mom_1d > params["mom_1d_bull"]:
        signals["momentum"] = ("BUY", min(mom_1d * mom_scale, 1.5))
    elif mom_1d < params["mom_1d_bear"]:
        signals["momentum"] = ("SELL", max(mom_1d * mom_scale, -1.5))
    else:
        signals["momentum"] = ("HOLD", 0.0)

    # 2. Mean reversion: large 1d moves partially revert
    if abs(mom_1d) > params["mean_revert_threshold"]:
        revert_mag = -mom_1d * mr_scale
        signals["mean_revert"] = ("BUY" if revert_mag > 0 else "SELL", revert_mag)
    elif mom_1d < -params.get("oversold_bounce", 3.0):
        # Oversold bounce: extra-large drops tend to bounce
        signals["mean_revert"] = ("BUY", abs(mom_1d) * mr_scale * 0.5)
    elif mom_1d > params.get("overbought_reversal", 3.0):
        # Overbought fade
        signals["mean_revert"] = ("SELL", -mom_1d * mr_scale * 0.5)
    else:
        signals["mean_revert"] = ("HOLD", 0.0)

    # 3. VIX regime
    vix_bear_ret = params.get("vix_bearish_return", -0.3)
    vix_bull_ret = params.get("vix_bullish_return", 0.2)
    if vix is not None:
        if vix > params.get("vix_crisis", 30.0):
            signals["vix"] = ("SELL", vix_bear_ret * 2.0)  # Crisis = extra bearish
        elif vix > params["vix_fear"]:
            signals["vix"] = ("SELL", vix_bear_ret)
        elif vix < params["vix_complacency"]:
            signals["vix"] = ("BUY", vix_bull_ret)
        else:
            signals["vix"] = ("HOLD", 0.0)
    else:
        signals["vix"] = ("HOLD", 0.0)

    # 4. Trend (5d momentum)
    if mom_5d is not None:
        if mom_5d > params["mom_5d_bull"]:
            signals["trend"] = ("BUY", mom_5d * trend_scale)
        elif mom_5d < params["mom_5d_bear"]:
            signals["trend"] = ("SELL", mom_5d * trend_scale)
        else:
            signals["trend"] = ("HOLD", 0.0)
    else:
        signals["trend"] = ("HOLD", 0.0)

    # 5. Volatility: high vol penalizes return estimate
    vol_pen = params.get("vol_penalty", 0.5)
    if vol_20d is not None and vol_20d > params["vol_high"]:
        signals["volatility"] = ("HOLD", 0.0)
    else:
        signals["volatility"] = ("HOLD", 0.0)

    # 6. NEW: Breadth signal
    if breadth_pct_up is not None:
        if breadth_pct_up > params.get("breadth_bull", 60.0):
            signals["breadth"] = ("BUY", 0.2)
        elif breadth_pct_up < params.get("breadth_bear", 35.0):
            signals["breadth"] = ("SELL", -0.2)
        else:
            signals["breadth"] = ("HOLD", 0.0)
    else:
        signals["breadth"] = ("HOLD", 0.0)

    # 7. NEW: 20d trend signal
    if mom_20d is not None:
        if mom_20d > params.get("mom_20d_bull", 5.0):
            signals["trend_20d"] = ("BUY", mom_20d * 0.02)
        elif mom_20d < params.get("mom_20d_bear", -5.0):
            signals["trend_20d"] = ("SELL", mom_20d * 0.02)
        else:
            signals["trend_20d"] = ("HOLD", 0.0)
    else:
        signals["trend_20d"] = ("HOLD", 0.0)

    # 8. NEW: VIX 1-day change signal
    if vix is not None and vix_prev is not None and vix_prev > 0:
        vix_change = vix - vix_prev
        spike_thresh = params.get("vix_spike_1d", 3.0)
        if vix_change > spike_thresh:
            signals["vix_change"] = ("SELL", -0.3)  # VIX spiking = fear incoming
        elif vix_change < -spike_thresh:
            signals["vix_change"] = ("BUY", 0.2)   # VIX collapsing = relief rally
        else:
            signals["vix_change"] = ("HOLD", 0.0)
    else:
        signals["vix_change"] = ("HOLD", 0.0)

    # 9. NEW: USDINR change — INR depreciation = bearish (FII outflows)
    if usdinr_change is not None:
        usdinr_bear = params.get("usdinr_depreciation_threshold", 0.3)
        usdinr_bull = params.get("usdinr_appreciation_threshold", -0.3)
        if usdinr_change > usdinr_bear:
            signals["usdinr"] = ("SELL", -0.2)  # INR weakening = FII selling
        elif usdinr_change < usdinr_bull:
            signals["usdinr"] = ("BUY", 0.15)   # INR strengthening = FII buying
        else:
            signals["usdinr"] = ("HOLD", 0.0)
    else:
        signals["usdinr"] = ("HOLD", 0.0)

    # 10. NEW: Futures OI change — rising OI + falling price = bearish buildup
    if oi_change_pct is not None:
        oi_rise = params.get("oi_rise_threshold", 3.0)
        oi_drop = params.get("oi_drop_threshold", -3.0)
        if oi_change_pct > oi_rise and mom_1d < 0:
            signals["oi_position"] = ("SELL", -0.2)   # Short buildup
        elif oi_change_pct > oi_rise and mom_1d > 0:
            signals["oi_position"] = ("BUY", 0.15)    # Long buildup
        elif oi_change_pct < oi_drop:
            signals["oi_position"] = ("HOLD", 0.0)    # Unwinding — uncertain
        else:
            signals["oi_position"] = ("HOLD", 0.0)
    else:
        signals["oi_position"] = ("HOLD", 0.0)

    # ── Weighted combination ──
    weights = {
        "momentum": params["w_momentum"],
        "mean_revert": params["w_mean_revert"],
        "vix": params["w_vix_regime"],
        "trend": params["w_trend"],
        "volatility": params["w_volatility"],
        "breadth": params.get("w_breadth", 0.10),
        "trend_20d": params.get("w_trend_20d", 0.08),
        "vix_change": params.get("w_vix_change", 0.07),
        "usdinr": params.get("w_usdinr", 0.05),
        "oi_position": params.get("w_oi_position", 0.05),
    }

    dir_score = 0.0
    return_est = 0.0
    for sig_name, (direction, ret) in signals.items():
        w = weights.get(sig_name, 0.05)
        if direction == "BUY":
            dir_score += w
        elif direction == "SELL":
            dir_score -= w
        return_est += ret * w

    # High-vol penalty on return magnitude
    if vol_20d is not None and vol_20d > params["vol_high"]:
        return_est *= vol_pen

    # Tunable direction thresholds
    buy_thresh = params.get("dir_buy_threshold", 0.15)
    sell_thresh = params.get("dir_sell_threshold", -0.15)

    if dir_score > buy_thresh:
        final_dir = "BUY"
    elif dir_score < sell_thresh:
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
        # Main index data
        rows = conn.execute(text("""
            SELECT session_id, nifty50_close, banknifty_close, india_vix,
                   usdinr_ref, repo_rate_pct, deriv_map
            FROM market_state
            WHERE nifty50_close IS NOT NULL AND universe_id = 'nifty50'
            ORDER BY session_id
        """)).fetchall()

        # Real breadth from bhavcopy_raw: advance/decline per date
        breadth_rows = conn.execute(text("""
            SELECT data_date,
                   SUM(CASE WHEN close > prev_close THEN 1 ELSE 0 END) as adv,
                   SUM(CASE WHEN close < prev_close THEN 1 ELSE 0 END) as dec,
                   COUNT(*) as total
            FROM bhavcopy_raw
            WHERE series = 'EQ' AND prev_close IS NOT NULL AND prev_close > 0
                  AND source = 'kite_historical'
            GROUP BY data_date
            ORDER BY data_date
        """)).fetchall()

    engine.dispose()

    if len(rows) < 2:
        return []

    # Build breadth lookup: date_str → (adv, dec, total, pct_up)
    breadth_by_date: dict[str, tuple] = {}
    for br in breadth_rows:
        dt = str(br[0])
        adv, dec, total = int(br[1]), int(br[2]), int(br[3])
        pct_up = adv / total * 100 if total > 0 else None
        breadth_by_date[dt] = (adv, dec, total, pct_up)

    results: list[DayResult] = []

    # Parse rows into tuples: (sid, nifty, bn, vix, usdinr, repo, deriv_map)
    closes = []
    for r in rows:
        deriv = r[6]
        if deriv and isinstance(deriv, str):
            deriv = json.loads(deriv)
        closes.append((
            r[0],                              # session_id
            float(r[1]),                       # nifty
            float(r[2]) if r[2] else None,     # banknifty
            float(r[3]) if r[3] else None,     # vix
            float(r[4]) if r[4] else None,     # usdinr
            float(r[5]) if r[5] else None,     # repo_rate
            deriv or {},                        # deriv_map
        ))

    for i in range(1, len(closes) - 1):
        sid, nifty, bn, vix, usdinr, repo, deriv = closes[i]
        _, next_nifty, _, _, _, _, _ = closes[i + 1]
        _, prev_nifty, _, _, prev_usdinr, prev_repo, _ = closes[i - 1]

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

        # Previous day VIX (for VIX change signal)
        vix_prev = closes[i - 1][3] if i >= 1 else None

        # ── REAL BREADTH from bhavcopy_raw ──
        date_str = sid.replace("CM_", "")
        br = breadth_by_date.get(date_str)
        if br:
            breadth_pct_up = br[3]  # Real % of stocks advancing
        elif i >= 5:
            # Fallback to index proxy if no stock data for this date
            up_days = sum(1 for j in range(i - 4, i + 1)
                          if closes[j][1] > closes[j - 1][1])
            breadth_pct_up = up_days / 5 * 100
        else:
            breadth_pct_up = None

        # ── USDINR change signal ──
        usdinr_change = None
        if usdinr and prev_usdinr and prev_usdinr > 0:
            usdinr_change = (usdinr - prev_usdinr) / prev_usdinr * 100

        # ── Futures OI change signal ──
        nifty_oi = None
        if isinstance(deriv, dict):
            nifty_oi = deriv.get("nifty_futures_oi")
        prev_deriv = closes[i - 1][6] if i >= 1 else {}
        prev_oi = None
        if isinstance(prev_deriv, dict):
            prev_oi = prev_deriv.get("nifty_futures_oi")
        oi_change_pct = None
        if nifty_oi and prev_oi and prev_oi > 0:
            oi_change_pct = (nifty_oi - prev_oi) / prev_oi * 100

        # Predict (v3: passes all real data)
        pred_dir, pred_ret = _compute_signal(
            mom_1d, mom_5d, mom_20d, vix, vix_prev, vol_20d, breadth_pct_up,
            params, usdinr_change=usdinr_change, oi_change_pct=oi_change_pct,
        )

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
