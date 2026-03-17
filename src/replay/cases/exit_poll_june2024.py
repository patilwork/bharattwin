"""
Replay Case: 2024 Exit Poll euphoria — June 3, 2024.

Self-contained: works without DB. Hard-codes T-1 market state (May 31, 2024)
and event context for exit poll results predicting a massive BJP/NDA majority.

Context: Exit polls released on Jun 1-2 (Saturday-Sunday after final phase of
voting on Jun 1) predicted 350-370 seats for NDA. Markets were closed over the
weekend. On Jun 3 (Monday), Nifty gapped up from 22,530 and rallied to close
at 23,264 (+3.25%). BankNifty surged +4.07%. VIX collapsed from 24.60 to 20.94.

This is a POSITIVE event case to validate the framework isn't bearish-biased.

Actual outcome: Nifty 50 rose +3.25% on June 3, 2024.
"""

from __future__ import annotations

import json
import logging
from datetime import date

from src.agents.runner import run_all
from src.agents.schemas import ConsensusResult

logger = logging.getLogger(__name__)

# --- T-1 Market State: May 31, 2024 (Friday close, pre-exit-polls) ---
MARKET_STATE = {
    "session_id": "CM_2024-05-31",
    "universe_id": "nifty50",
    "nifty50_close": 22530.70,
    "banknifty_close": 48983.95,
    "india_vix": 24.60,
    "repo_rate_pct": 6.50,
    "usdinr_ref": 83.28,
    "crude_indian_basket_usd": 81.10,
    "returns_map": json.dumps({
        "RELIANCE": -0.56, "HDFCBANK": -0.34, "ICICIBANK": -0.89,
        "INFY": +0.45, "TCS": +0.23, "HINDUNILVR": -0.12,
        "ITC": -0.67, "KOTAKBANK": -1.23, "LT": +0.34,
        "AXISBANK": -0.78, "SBIN": -1.45, "BAJFINANCE": -0.89,
        "BHARTIARTL": +0.56, "ASIANPAINT": -0.23, "MARUTI": -0.45,
        "TITAN": +0.12, "HCLTECH": +0.67, "WIPRO": +0.34,
        "SUNPHARMA": +0.89, "ULTRACEMCO": -0.34,
    }),
    "flow_map": json.dumps({
        "fii_cash": {"buy_cr": 9234.5, "sell_cr": 11456.3, "net_cr": -2221.8},
        "dii_cash": {"buy_cr": 8123.4, "sell_cr": 6678.9, "net_cr": 1444.5},
    }),
    "macro_map": json.dumps({
        "fx": {"usdinr": 83.28, "eurinr": 89.78, "gbpinr": 104.56},
        "crude": {"indian_basket_usd": 81.10},
        "repo_rate_pct": 6.50,
    }),
    "factor_map": json.dumps({
        "momentum_1d": -0.45,
        "momentum_5d": -1.23,
        "momentum_20d": +2.34,
        "volatility_20d": 16.78,
        "breadth_adv": 18,
        "breadth_dec": 30,
        "breadth_unch": 2,
        "breadth_ratio": 0.60,
        "breadth_pct_up": 36.0,
        "avg_turnover_cr": 9.56,
        "eq_stock_count": 50,
    }),
    "regime_state": json.dumps({
        "is_nifty_monthly_expiry": False,
        "is_banknifty_monthly_expiry": False,
        "is_nifty_weekly_expiry": False,
        "is_banknifty_weekly_expiry": False,
    }),
    "data_quality": json.dumps({
        "bhavcopy_eq_rows": 1845,
        "flow_rows": 2,
        "has_fx": True,
        "has_nifty_close": True,
        "has_banknifty_close": True,
        "has_vix": True,
        "has_factors": True,
    }),
}

# --- Event: Exit poll results (released Saturday-Sunday Jun 1-2) ---
EVENT = {
    "headline": "2024 Exit Polls predict massive NDA majority: 350-370 seats; BJP alone above 300",
    "event_type": "political",
    "source_tier": 2,
    "raw_text": (
        "Multiple exit polls released after the final phase of the 2024 Lok Sabha elections "
        "predict a landslide victory for the BJP-led NDA coalition. Key predictions: "
        "India Today-Axis My India: NDA 361-401 seats. News24-Today's Chanakya: NDA 371-401. "
        "Times Now-ETG: NDA 358-378. Republic-Matrize: NDA 353-368. Republic-PMARQ: NDA 359. "
        "All major exit polls predict BJP alone crossing 300 seats, well above the 272 majority "
        "mark. This implies a strong reform mandate for Modi 3.0 with likely policy continuity "
        "and acceleration of divestment, infrastructure capex, and digital India agenda. "
        "Markets were closed for the weekend (exit polls released Sat-Sun); Monday Jun 3 will "
        "be the first trading session to react."
    ),
    "extracted_entities": {
        "nda_predicted_low": 350,
        "nda_predicted_high": 401,
        "bjp_predicted_above": 300,
        "majority_mark": 272,
        "exit_poll_consensus": "landslide NDA",
        "policy_implication": "strong reform mandate, policy continuity",
    },
}

# Actual outcome
ACTUAL_NIFTY_RETURN_PCT = +3.25
ACTUAL_NIFTY_CLOSE_JUN3 = 23263.90
ACTUAL_NIFTY_OPEN_JUN3 = 23337.90  # Gapped up at open


def run_replay(mode: str = "auto") -> ConsensusResult | list[tuple[str, str]]:
    """Run all 8 agents against the May 31 state + exit poll event."""
    logger.info("=== Exit Poll June 2024 Replay ===")
    result = run_all(
        d=date(2024, 5, 31),
        market_state=MARKET_STATE,
        event=EVENT,
        mode=mode,
    )
    if isinstance(result, ConsensusResult):
        evaluate(result)
    return result


def evaluate(consensus: ConsensusResult) -> dict:
    """Compare consensus to actual +3.25%."""
    predicted = consensus.avg_return_pct
    error = abs(predicted - ACTUAL_NIFTY_RETURN_PCT)
    direction_correct = (
        (consensus.consensus_direction.value == "BUY" and ACTUAL_NIFTY_RETURN_PCT > 0)
        or (consensus.consensus_direction.value == "SELL" and ACTUAL_NIFTY_RETURN_PCT < 0)
    )

    print("\n" + "=" * 60)
    print("REPLAY EVALUATION: Exit Poll Euphoria June 3, 2024")
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
    return {"actual_return_pct": ACTUAL_NIFTY_RETURN_PCT, "predicted_return_pct": predicted, "error_pct": round(error, 4), "direction_correct": direction_correct}


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    mode = sys.argv[1] if len(sys.argv) > 1 else "prompt"
    run_replay(mode=mode)
