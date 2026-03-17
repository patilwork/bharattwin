"""
Vectorized Factor Engine — bootstrap-safe, numpy-based evaluation.

Replaces the Python-loop backtest with a vectorized implementation that:
1. Computes all features from raw price/state arrays (no pre-computed features)
2. Supports block-bootstrap scenario generation
3. Uses the same scoring semantics as backtest_full.py
4. Runs 5,000 days in ~0.5s instead of ~15s

Design principles (from monte_carlo_research_plan.md):
- Feature computation happens INSIDE the evaluation pass
- Bootstrap operates on raw contiguous blocks, then recomputes features
- No pre-computed features are resampled
- Path-derived signals (momentum, vol) are always computed fresh

Usage:
    from src.eval.factor_engine import evaluate, bootstrap_evaluate, load_market_data

    data = load_market_data()
    scores = evaluate(data, params)
    robust_scores = bootstrap_evaluate(data, params, n_scenarios=500)
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


@dataclass
class MarketData:
    """Raw market data arrays — the input to all evaluation."""
    dates: np.ndarray          # string dates (N,)
    nifty: np.ndarray          # float64 (N,)
    banknifty: np.ndarray      # float64 (N,), may have NaN
    vix: np.ndarray            # float64 (N,), may have NaN
    usdinr: np.ndarray         # float64 (N,), may have NaN
    repo_rate: np.ndarray      # float64 (N,), may have NaN
    nifty_futures_oi: np.ndarray  # float64 (N,), may have NaN
    breadth_pct_up: np.ndarray    # float64 (N,), may have NaN
    n: int = 0

    def __post_init__(self):
        self.n = len(self.nifty)


@dataclass
class EvalResult:
    """Evaluation output — matches the canonical metrics from the research plan."""
    n_days: int
    direction_pct: float
    avg_error_pp: float
    rmse_pp: float
    big_move_direction_pct: float
    big_move_count: int
    hold_rate: float
    bull_accuracy: float
    bear_accuracy: float
    regime_breakdown: dict[str, dict]
    pnl_proxy: float  # Simple: sum of (predicted_sign * actual_return)


@dataclass
class BootstrapResult:
    """Bootstrap evaluation output — distribution of EvalResults."""
    n_scenarios: int
    median_direction: float
    p10_direction: float
    p90_direction: float
    median_error: float
    p10_composite: float
    median_composite: float
    p90_composite: float
    scenario_scores: list[float]


def load_market_data() -> MarketData:
    """Load all raw market data from DB into numpy arrays."""
    engine = create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))

    with engine.connect() as conn:
        # Main index + macro data
        rows = conn.execute(text("""
            SELECT session_id, nifty50_close, banknifty_close, india_vix,
                   usdinr_ref, repo_rate_pct, deriv_map
            FROM market_state
            WHERE nifty50_close IS NOT NULL AND universe_id = 'nifty50'
            ORDER BY session_id
        """)).fetchall()

        # Real breadth
        breadth_rows = conn.execute(text("""
            SELECT data_date,
                   SUM(CASE WHEN close > prev_close THEN 1 ELSE 0 END)::float /
                   NULLIF(COUNT(*), 0) * 100 as pct_up
            FROM bhavcopy_raw
            WHERE series = 'EQ' AND prev_close IS NOT NULL AND prev_close > 0
                  AND source = 'kite_historical'
            GROUP BY data_date
        """)).fetchall()

    engine.dispose()

    # Build breadth lookup
    breadth_map = {str(r[0]): float(r[1]) if r[1] else float('nan') for r in breadth_rows}

    n = len(rows)
    dates = np.empty(n, dtype=object)
    nifty = np.empty(n, dtype=np.float64)
    banknifty = np.full(n, np.nan, dtype=np.float64)
    vix = np.full(n, np.nan, dtype=np.float64)
    usdinr = np.full(n, np.nan, dtype=np.float64)
    repo = np.full(n, np.nan, dtype=np.float64)
    futures_oi = np.full(n, np.nan, dtype=np.float64)
    breadth = np.full(n, np.nan, dtype=np.float64)

    for i, r in enumerate(rows):
        sid = r[0]
        dt = sid.replace("CM_", "")
        dates[i] = dt
        nifty[i] = float(r[1])
        if r[2]: banknifty[i] = float(r[2])
        if r[3]: vix[i] = float(r[3])
        if r[4]: usdinr[i] = float(r[4])
        if r[5]: repo[i] = float(r[5])

        # Futures OI from deriv_map
        dm = r[6]
        if dm:
            if isinstance(dm, str):
                dm = json.loads(dm)
            oi = dm.get("nifty_futures_oi")
            if oi: futures_oi[i] = float(oi)

        # Breadth
        if dt in breadth_map:
            breadth[i] = breadth_map[dt]

    return MarketData(
        dates=dates, nifty=nifty, banknifty=banknifty, vix=vix,
        usdinr=usdinr, repo_rate=repo, nifty_futures_oi=futures_oi,
        breadth_pct_up=breadth,
    )


def _compute_features(data: MarketData) -> dict[str, np.ndarray]:
    """
    Compute all derived features from raw arrays.

    This is the bootstrap-safe core: call this AFTER resampling raw blocks,
    and all path-derived features are correctly recomputed.
    """
    n = data.n
    nifty = data.nifty

    # Returns
    returns_1d = np.full(n, np.nan)
    returns_1d[1:] = (nifty[1:] - nifty[:-1]) / nifty[:-1] * 100

    # Actual next-day return (the target)
    actual_next = np.full(n, np.nan)
    actual_next[:-1] = (nifty[1:] - nifty[:-1]) / nifty[:-1] * 100

    # Momentum 5d
    mom_5d = np.full(n, np.nan)
    mom_5d[5:] = (nifty[5:] - nifty[:-5]) / nifty[:-5] * 100

    # Momentum 20d
    mom_20d = np.full(n, np.nan)
    mom_20d[20:] = (nifty[20:] - nifty[:-20]) / nifty[:-20] * 100

    # Volatility 20d (rolling std of returns * sqrt(252))
    vol_20d = np.full(n, np.nan)
    for i in range(20, n):
        window = returns_1d[i - 19:i + 1]
        if not np.any(np.isnan(window)):
            vol_20d[i] = np.std(window, ddof=1) * np.sqrt(252)

    # VIX change
    vix_change = np.full(n, np.nan)
    v = data.vix
    valid = ~np.isnan(v[:-1]) & ~np.isnan(v[1:]) & (v[:-1] > 0)
    vix_change[1:][valid] = v[1:][valid] - v[:-1][valid]

    # USDINR change
    usdinr_change = np.full(n, np.nan)
    u = data.usdinr
    valid_u = ~np.isnan(u[:-1]) & ~np.isnan(u[1:]) & (u[:-1] > 0)
    usdinr_change[1:][valid_u] = (u[1:][valid_u] - u[:-1][valid_u]) / u[:-1][valid_u] * 100

    # OI change
    oi_change = np.full(n, np.nan)
    oi = data.nifty_futures_oi
    valid_oi = ~np.isnan(oi[:-1]) & ~np.isnan(oi[1:]) & (oi[:-1] > 0)
    oi_change[1:][valid_oi] = (oi[1:][valid_oi] - oi[:-1][valid_oi]) / oi[:-1][valid_oi] * 100

    return {
        "returns_1d": returns_1d,
        "actual_next": actual_next,
        "mom_5d": mom_5d,
        "mom_20d": mom_20d,
        "vol_20d": vol_20d,
        "vix": data.vix,
        "vix_change": vix_change,
        "breadth": data.breadth_pct_up,
        "usdinr_change": usdinr_change,
        "oi_change": oi_change,
    }


def _predict_vectorized(features: dict[str, np.ndarray], params: dict) -> tuple[np.ndarray, np.ndarray]:
    """
    Vectorized prediction from features.

    Returns (direction_array, return_estimate_array).
    Direction: 1=BUY, 0=HOLD, -1=SELL
    """
    n = len(features["returns_1d"])
    dir_score = np.zeros(n)
    ret_est = np.zeros(n)

    mom = features["returns_1d"]
    mom_scale = params.get("momentum_return_scale", 0.3)
    mr_scale = params.get("mean_revert_scale", 0.3)

    # 1. Momentum
    w = params.get("w_momentum", 0.25)
    bull = mom > params.get("mom_1d_bull", 0.8)
    bear = mom < params.get("mom_1d_bear", -0.8)
    dir_score[bull] += w
    dir_score[bear] -= w
    ret_est += np.where(bull, np.minimum(mom * mom_scale, 1.5), 0)
    ret_est += np.where(bear, np.maximum(mom * mom_scale, -1.5), 0)

    # 2. Mean reversion
    w = params.get("w_mean_revert", 0.20)
    mr_thresh = params.get("mean_revert_threshold", 2.0)
    big_move = np.abs(mom) > mr_thresh
    revert = -mom * mr_scale
    dir_score += np.where(big_move & (revert > 0), w, 0)
    dir_score -= np.where(big_move & (revert < 0), w, 0)
    ret_est += np.where(big_move, revert * w, 0)

    # 3. VIX regime
    w = params.get("w_vix_regime", 0.15)
    vix = features["vix"]
    fear = vix > params.get("vix_fear", 20.0)
    complacent = vix < params.get("vix_complacency", 13.0)
    crisis = vix > params.get("vix_crisis", 30.0)
    dir_score -= np.where(fear & ~np.isnan(vix), w, 0)
    dir_score += np.where(complacent & ~np.isnan(vix), w, 0)
    dir_score -= np.where(crisis & ~np.isnan(vix), w, 0)  # Extra bearish

    # 4. Trend 5d
    w = params.get("w_trend", 0.15)
    m5 = features["mom_5d"]
    trend_scale = params.get("trend_return_scale", 0.1)
    t_bull = m5 > params.get("mom_5d_bull", 1.5)
    t_bear = m5 < params.get("mom_5d_bear", -1.5)
    dir_score += np.where(t_bull & ~np.isnan(m5), w, 0)
    dir_score -= np.where(t_bear & ~np.isnan(m5), w, 0)
    ret_est += np.where(t_bull & ~np.isnan(m5), m5 * trend_scale * w, 0)
    ret_est += np.where(t_bear & ~np.isnan(m5), m5 * trend_scale * w, 0)

    # 5. Breadth
    w = params.get("w_breadth", 0.10)
    br = features["breadth"]
    br_bull = br > params.get("breadth_bull", 60.0)
    br_bear = br < params.get("breadth_bear", 35.0)
    dir_score += np.where(br_bull & ~np.isnan(br), w, 0)
    dir_score -= np.where(br_bear & ~np.isnan(br), w, 0)

    # 6. 20d trend
    w = params.get("w_trend_20d", 0.08)
    m20 = features["mom_20d"]
    t20_bull = m20 > params.get("mom_20d_bull", 5.0)
    t20_bear = m20 < params.get("mom_20d_bear", -5.0)
    dir_score += np.where(t20_bull & ~np.isnan(m20), w, 0)
    dir_score -= np.where(t20_bear & ~np.isnan(m20), w, 0)

    # 7. VIX change
    w = params.get("w_vix_change", 0.07)
    vc = features["vix_change"]
    spike = params.get("vix_spike_1d", 3.0)
    dir_score -= np.where((vc > spike) & ~np.isnan(vc), w, 0)
    dir_score += np.where((vc < -spike) & ~np.isnan(vc), w, 0)

    # 8. USDINR
    w = params.get("w_usdinr", 0.05)
    uc = features["usdinr_change"]
    dep = params.get("usdinr_depreciation_threshold", 0.3)
    app = params.get("usdinr_appreciation_threshold", -0.3)
    dir_score -= np.where((uc > dep) & ~np.isnan(uc), w, 0)
    dir_score += np.where((uc < app) & ~np.isnan(uc), w, 0)

    # 9. OI position
    w = params.get("w_oi_position", 0.05)
    oc = features["oi_change"]
    oi_rise = params.get("oi_rise_threshold", 3.0)
    short_buildup = (oc > oi_rise) & (mom < 0) & ~np.isnan(oc)
    long_buildup = (oc > oi_rise) & (mom > 0) & ~np.isnan(oc)
    dir_score -= np.where(short_buildup, w, 0)
    dir_score += np.where(long_buildup, w, 0)

    # Direction thresholds
    buy_thresh = params.get("dir_buy_threshold", 0.15)
    sell_thresh = params.get("dir_sell_threshold", -0.15)

    direction = np.where(dir_score > buy_thresh, 1,
                np.where(dir_score < sell_thresh, -1, 0))

    return direction, ret_est


def _classify_regime(vix: np.ndarray, mom_20d: np.ndarray) -> np.ndarray:
    """Classify regime for each day."""
    n = len(vix)
    regime = np.full(n, 3, dtype=np.int8)  # 3=sideways default
    regime[~np.isnan(vix) & (vix > 30)] = 0  # crisis
    regime[~np.isnan(mom_20d) & (mom_20d < -5)] = 1  # bear
    regime[~np.isnan(mom_20d) & (mom_20d > 5)] = 2  # bull
    # crisis overrides
    regime[~np.isnan(vix) & (vix > 30)] = 0
    return regime


REGIME_NAMES = {0: "crisis", 1: "bear", 2: "bull", 3: "sideways"}


def evaluate(data: MarketData, params: dict) -> EvalResult:
    """
    Evaluate the factor engine on a MarketData dataset.

    This is the canonical evaluator — all systems use this scoring contract.
    """
    features = _compute_features(data)
    direction, ret_est = _predict_vectorized(features, params)

    actual = features["actual_next"]
    mom_1d = features["returns_1d"]

    # Mask: valid days (have actual return and at least 1d of history)
    valid = ~np.isnan(actual) & ~np.isnan(mom_1d)
    idx = np.where(valid)[0]

    if len(idx) == 0:
        return EvalResult(0, 0, 0, 0, 0, 0, 0, 0, 0, {}, 0)

    d = direction[idx]
    a = actual[idx]
    r = ret_est[idx]

    # Direction accuracy
    actual_dir = np.where(a > 0.25, 1, np.where(a < -0.25, -1, 0))
    correct = (d == actual_dir) | ((d == 0) & (np.abs(a) < 0.5))
    dir_pct = np.mean(correct) * 100

    # Error
    errors = np.abs(r - a)
    avg_err = np.mean(errors)
    rmse = np.sqrt(np.mean(errors ** 2))

    # Big move days
    big = np.abs(a) > 2.0
    big_count = int(np.sum(big))
    big_dir = np.mean(correct[big]) * 100 if big_count > 0 else 0

    # HOLD rate
    hold_rate = np.mean(d == 0) * 100

    # Bull vs bear accuracy
    bull_mask = actual_dir == 1
    bear_mask = actual_dir == -1
    bull_acc = np.mean(correct[bull_mask]) * 100 if np.any(bull_mask) else 0
    bear_acc = np.mean(correct[bear_mask]) * 100 if np.any(bear_mask) else 0

    # Regime breakdown
    regime = _classify_regime(features["vix"][idx], features["mom_20d"][idx])
    regime_bd = {}
    for code, name in REGIME_NAMES.items():
        mask = regime == code
        if np.any(mask):
            regime_bd[name] = {
                "count": int(np.sum(mask)),
                "direction_pct": round(float(np.mean(correct[mask]) * 100), 1),
                "avg_error": round(float(np.mean(errors[mask])), 2),
            }

    # PnL proxy: sum of signed returns where we predicted correctly
    pnl = float(np.sum(np.where(d != 0, np.sign(d) * a, 0)))

    return EvalResult(
        n_days=len(idx),
        direction_pct=round(float(dir_pct), 1),
        avg_error_pp=round(float(avg_err), 2),
        rmse_pp=round(float(rmse), 2),
        big_move_direction_pct=round(float(big_dir), 1),
        big_move_count=big_count,
        hold_rate=round(float(hold_rate), 1),
        bull_accuracy=round(float(bull_acc), 1),
        bear_accuracy=round(float(bear_acc), 1),
        regime_breakdown=regime_bd,
        pnl_proxy=round(pnl, 2),
    )


def composite_score(result: EvalResult) -> float:
    """Compute composite fitness from EvalResult."""
    inv_error = max(0, 100 - result.avg_error_pp * 50)
    return (
        result.direction_pct * 0.6 +
        inv_error * 0.2 +
        result.big_move_direction_pct * 0.2
    )


def bootstrap_evaluate(
    data: MarketData,
    params: dict,
    n_scenarios: int = 200,
    block_size: int = 60,
    seed: int = 42,
) -> BootstrapResult:
    """
    Bootstrap evaluation — generate scenarios from contiguous blocks,
    recompute features from scratch for each, evaluate.

    Block bootstrap preserves autocorrelation in price series.
    Features are recomputed inside each scenario — no leakage.

    Args:
        data: Raw market data.
        params: Factor parameters.
        n_scenarios: Number of bootstrap scenarios.
        block_size: Size of contiguous blocks (trading days). 60 ≈ 3 months.
        seed: Random seed.
    """
    rng = np.random.RandomState(seed)
    n = data.n
    n_blocks = max(1, n // block_size)

    scores = []

    for s in range(n_scenarios):
        # Sample block start indices with replacement
        block_starts = rng.randint(0, n - block_size, size=n_blocks)

        # Build scenario by concatenating blocks
        indices = np.concatenate([np.arange(start, start + block_size) for start in block_starts])
        indices = indices[:n]  # Trim to original length

        # Create scenario MarketData from sampled indices
        scenario = MarketData(
            dates=data.dates[indices],
            nifty=data.nifty[indices],
            banknifty=data.banknifty[indices],
            vix=data.vix[indices],
            usdinr=data.usdinr[indices],
            repo_rate=data.repo_rate[indices],
            nifty_futures_oi=data.nifty_futures_oi[indices],
            breadth_pct_up=data.breadth_pct_up[indices],
        )

        # Evaluate with fresh feature computation
        result = evaluate(scenario, params)
        scores.append(composite_score(result))

    scores_arr = np.array(scores)

    return BootstrapResult(
        n_scenarios=n_scenarios,
        median_direction=0,  # Filled by caller if needed
        p10_direction=0,
        p90_direction=0,
        median_error=0,
        p10_composite=round(float(np.percentile(scores_arr, 10)), 2),
        median_composite=round(float(np.percentile(scores_arr, 50)), 2),
        p90_composite=round(float(np.percentile(scores_arr, 90)), 2),
        scenario_scores=scores.copy(),
    )


def print_eval(result: EvalResult, title: str = "") -> None:
    """Print evaluation result in canonical format."""
    print(f"\n{'=' * 65}")
    if title:
        print(f"EVALUATION: {title}")
    print(f"{'=' * 65}")
    print(f"  Days:       {result.n_days:,}")
    print(f"  Direction:  {result.direction_pct}%")
    print(f"  Avg Error:  {result.avg_error_pp}pp")
    print(f"  RMSE:       {result.rmse_pp}pp")
    print(f"  Big Moves:  {result.big_move_direction_pct}% ({result.big_move_count} days)")
    print(f"  HOLD rate:  {result.hold_rate}%")
    print(f"  Bull acc:   {result.bull_accuracy}%  |  Bear acc: {result.bear_accuracy}%")
    print(f"  PnL proxy:  {result.pnl_proxy:+.1f}pp cumulative")
    print(f"\n  Regime breakdown:")
    for regime, stats in sorted(result.regime_breakdown.items()):
        print(f"    {regime:<10} {stats['count']:>5} days | dir={stats['direction_pct']:>5.1f}% | err={stats['avg_error']:.2f}pp")
    print(f"  Composite:  {composite_score(result):.2f}")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    import time
    from src.backtest_full import PARAMS

    print("Loading market data...")
    t0 = time.time()
    data = load_market_data()
    print(f"Loaded {data.n:,} days in {time.time()-t0:.1f}s")
    print(f"  Breadth coverage: {int(np.sum(~np.isnan(data.breadth_pct_up)))}/{data.n}")
    print(f"  VIX coverage: {int(np.sum(~np.isnan(data.vix)))}/{data.n}")
    print(f"  USDINR coverage: {int(np.sum(~np.isnan(data.usdinr)))}/{data.n}")
    print(f"  Futures OI coverage: {int(np.sum(~np.isnan(data.nifty_futures_oi)))}/{data.n}")

    print("\nEvaluating (vectorized)...")
    t0 = time.time()
    result = evaluate(data, PARAMS)
    eval_time = time.time() - t0
    print_eval(result, f"Factor Engine v3 ({eval_time:.2f}s)")

    print("\nRunning bootstrap (200 scenarios)...")
    t0 = time.time()
    bootstrap = bootstrap_evaluate(data, PARAMS, n_scenarios=200)
    boot_time = time.time() - t0
    print(f"\nBootstrap ({bootstrap.n_scenarios} scenarios, {boot_time:.1f}s):")
    print(f"  Composite: p10={bootstrap.p10_composite} median={bootstrap.median_composite} p90={bootstrap.p90_composite}")
    print(f"  Spread: {bootstrap.p90_composite - bootstrap.p10_composite:.2f}")
