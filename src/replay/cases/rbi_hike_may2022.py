"""
Replay Case: RBI surprise 40bps rate hike — May 4, 2022.

Self-contained: works without DB. Hard-codes T-1 market state (May 2, 2022)
and event context for the off-cycle MPC surprise hike.

Actual outcome: Nifty 50 fell -2.29% on May 4, 2022.
"""

from __future__ import annotations

import json
import logging
from datetime import date

from src.agents.runner import run_all
from src.agents.schemas import ConsensusResult

logger = logging.getLogger(__name__)

# --- T-1 Market State: May 2, 2022 (Monday close) ---
MARKET_STATE = {
    "session_id": "CM_2022-05-02",
    "universe_id": "nifty50",
    "nifty50_close": 17069.10,
    "banknifty_close": 36163.75,
    "india_vix": 20.28,
    "repo_rate_pct": 4.00,
    "usdinr_ref": 76.39,
    "crude_indian_basket_usd": 102.97,
    "returns_map": json.dumps({
        "RELIANCE": -1.23, "HDFCBANK": -0.87, "ICICIBANK": -1.45,
        "INFY": +0.32, "TCS": +0.15, "HINDUNILVR": -0.56,
        "ITC": +0.78, "KOTAKBANK": -1.12, "LT": -0.34,
        "AXISBANK": -1.67, "SBIN": -0.98, "BAJFINANCE": -2.10,
        "BHARTIARTL": +0.45, "ASIANPAINT": -0.23, "MARUTI": -0.89,
        "TITAN": -0.67, "HCLTECH": +0.12, "WIPRO": -0.45,
        "SUNPHARMA": +0.34, "ULTRACEMCO": -0.56,
    }),
    "flow_map": json.dumps({
        "fii_cash": {"buy_cr": 8234.5, "sell_cr": 10456.3, "net_cr": -2221.8},
        "dii_cash": {"buy_cr": 7123.4, "sell_cr": 5678.9, "net_cr": 1444.5},
    }),
    "macro_map": json.dumps({
        "fx": {"usdinr": 76.39, "eurinr": 80.52, "gbpinr": 95.67},
        "crude": {"indian_basket_usd": 102.97},
        "repo_rate_pct": 4.00,
    }),
    "factor_map": json.dumps({
        "momentum_1d": -0.62,
        "momentum_5d": -1.85,
        "momentum_20d": -3.47,
        "volatility_20d": 17.34,
        "breadth_adv": 12,
        "breadth_dec": 35,
        "breadth_unch": 3,
        "breadth_ratio": 0.3429,
        "breadth_pct_up": 24.0,
        "avg_turnover_cr": 8.45,
        "eq_stock_count": 50,
    }),
    "regime_state": json.dumps({
        "is_nifty_monthly_expiry": False,
        "is_banknifty_monthly_expiry": False,
        "is_nifty_weekly_expiry": False,
        "is_banknifty_weekly_expiry": False,
    }),
    "data_quality": json.dumps({
        "bhavcopy_eq_rows": 1847,
        "flow_rows": 2,
        "has_fx": True,
        "has_nifty_close": True,
        "has_banknifty_close": True,
        "has_vix": True,
        "has_factors": True,
    }),
}

# --- Event: RBI surprise hike (announced May 4, 2022 ~10:00 IST) ---
EVENT = {
    "headline": "RBI surprises with 40bps repo rate hike to 4.40% in unscheduled off-cycle MPC meeting",
    "event_type": "monetary_policy",
    "source_tier": 1,
    "raw_text": (
        "The Reserve Bank of India's Monetary Policy Committee, in an unscheduled meeting, "
        "unanimously decided to raise the repo rate by 40 basis points from 4.00% to 4.40%, "
        "effective immediately. The standing deposit facility (SDF) rate was also introduced at 3.75%. "
        "This is the first rate hike since August 2018 and comes amid inflation persistently above "
        "the 6% upper tolerance band. Governor Shaktikanta Das cited global commodity prices, "
        "supply chain disruptions from the Russia-Ukraine conflict, and elevated inflation expectations. "
        "CRR was also raised by 50bps to 4.50% to absorb excess liquidity."
    ),
    "extracted_entities": {
        "repo_rate_new": 4.40,
        "repo_rate_old": 4.00,
        "hike_bps": 40,
        "crr_new": 4.50,
        "crr_hike_bps": 50,
        "sdf_rate": 3.75,
    },
}

# Actual outcome
ACTUAL_NIFTY_RETURN_PCT = -2.29
ACTUAL_NIFTY_CLOSE_MAY4 = 16677.60


def run_replay(mode: str = "auto") -> ConsensusResult | list[tuple[str, str]]:
    """
    Run all 8 agents against the May 2, 2022 state + RBI hike event.

    Args:
        mode: "api", "prompt", or "auto"

    Returns:
        ConsensusResult (api mode) or list of prompt strings (prompt mode)
    """
    logger.info("=== RBI Hike May 2022 Replay ===")
    logger.info("T-1 state: Nifty=%.2f, VIX=%.2f, Repo=%.2f%%",
                MARKET_STATE["nifty50_close"], MARKET_STATE["india_vix"],
                MARKET_STATE["repo_rate_pct"])
    logger.info("Event: %s", EVENT["headline"])

    result = run_all(
        d=date(2022, 5, 2),
        market_state=MARKET_STATE,
        event=EVENT,
        mode=mode,
    )

    if isinstance(result, ConsensusResult):
        evaluate(result)

    return result


def evaluate(consensus: ConsensusResult) -> dict:
    """
    Compare consensus to actual outcome.

    Actual: Nifty fell -2.29% on May 4, 2022 (17069.10 → 16677.60).
    """
    predicted = consensus.avg_return_pct
    error = abs(predicted - ACTUAL_NIFTY_RETURN_PCT)
    direction_correct = (
        (consensus.consensus_direction.value == "SELL" and ACTUAL_NIFTY_RETURN_PCT < 0)
        or (consensus.consensus_direction.value == "BUY" and ACTUAL_NIFTY_RETURN_PCT > 0)
    )

    report = {
        "actual_return_pct": ACTUAL_NIFTY_RETURN_PCT,
        "predicted_return_pct": predicted,
        "error_pct": round(error, 4),
        "direction_correct": direction_correct,
        "consensus_direction": consensus.consensus_direction.value,
        "bull_count": consensus.bull_count,
        "bear_count": consensus.bear_count,
        "neutral_count": consensus.neutral_count,
        "predicted_range": {
            "low": consensus.return_range.low_pct,
            "base": consensus.return_range.base_pct,
            "high": consensus.return_range.high_pct,
        },
    }

    print("\n" + "=" * 60)
    print("REPLAY EVALUATION: RBI Hike May 4, 2022")
    print("=" * 60)
    print(f"Actual Nifty return:    {ACTUAL_NIFTY_RETURN_PCT:+.2f}%")
    print(f"Predicted (consensus):  {predicted:+.2f}%")
    print(f"Prediction error:       {error:.2f}pp")
    print(f"Direction correct:      {'YES' if direction_correct else 'NO'}")
    print(f"Consensus direction:    {consensus.consensus_direction.value}")
    print(f"Votes: Bull={consensus.bull_count} Bear={consensus.bear_count} Neutral={consensus.neutral_count}")
    print(f"Return range: [{consensus.return_range.low_pct:+.2f}%, {consensus.return_range.high_pct:+.2f}%]")
    print("=" * 60)

    for dec in consensus.decisions:
        print(f"  {dec.agent_id:20s} | {dec.direction.value:4s} | "
              f"conv={dec.conviction} | base={dec.nifty_return.base_pct:+.2f}%")

    print("=" * 60 + "\n")

    return report


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    mode = sys.argv[1] if len(sys.argv) > 1 else "prompt"
    run_replay(mode=mode)
