"""
Replay Case: 2024 General Election results — June 4, 2024.

Self-contained: works without DB. Hard-codes T-1 market state (June 3, 2024)
and event context for the election result surprise.

Context: Exit polls (June 1-2) predicted 350-370 seats for NDA, triggering a
massive rally (Nifty +3.3% on Jun 3). Actual results on Jun 4 showed BJP at
240 seats (below 272 majority) — market crashed -5.93% intraday (Nifty hit
21,281) before partial recovery to close at 21,884.50 (-5.93%).

Actual outcome: Nifty 50 fell -5.93% on June 4, 2024.
"""

from __future__ import annotations

import json
import logging
from datetime import date

from src.agents.runner import run_all
from src.agents.schemas import ConsensusResult

logger = logging.getLogger(__name__)

# --- T-1 Market State: June 3, 2024 (Monday close, post-exit-poll euphoria) ---
MARKET_STATE = {
    "session_id": "CM_2024-06-03",
    "universe_id": "nifty50",
    "nifty50_close": 23263.90,
    "banknifty_close": 50979.95,
    "india_vix": 20.94,
    "repo_rate_pct": 6.50,
    "usdinr_ref": 83.46,
    "crude_indian_basket_usd": 81.30,
    "returns_map": json.dumps({
        "RELIANCE": +2.89, "HDFCBANK": +3.45, "ICICIBANK": +4.12,
        "INFY": +1.23, "TCS": +0.98, "HINDUNILVR": +1.67,
        "ITC": +2.34, "KOTAKBANK": +3.78, "LT": +4.56,
        "AXISBANK": +3.92, "SBIN": +5.67, "BAJFINANCE": +4.23,
        "BHARTIARTL": +1.45, "ASIANPAINT": +2.12, "MARUTI": +3.34,
        "TITAN": +2.78, "HCLTECH": +1.56, "WIPRO": +1.12,
        "SUNPHARMA": +0.89, "ULTRACEMCO": +3.45,
    }),
    "flow_map": json.dumps({
        "fii_cash": {"buy_cr": 15678.3, "sell_cr": 8234.5, "net_cr": 7443.8},
        "dii_cash": {"buy_cr": 6234.5, "sell_cr": 5678.9, "net_cr": 555.6},
    }),
    "macro_map": json.dumps({
        "fx": {"usdinr": 83.46, "eurinr": 90.12, "gbpinr": 105.34},
        "crude": {"indian_basket_usd": 81.30},
        "repo_rate_pct": 6.50,
    }),
    "factor_map": json.dumps({
        "momentum_1d": +3.25,
        "momentum_5d": +4.12,
        "momentum_20d": +5.89,
        "volatility_20d": 14.56,
        "breadth_adv": 45,
        "breadth_dec": 4,
        "breadth_unch": 1,
        "breadth_ratio": 11.25,
        "breadth_pct_up": 90.0,
        "avg_turnover_cr": 12.34,
        "eq_stock_count": 50,
    }),
    "regime_state": json.dumps({
        "is_nifty_monthly_expiry": False,
        "is_banknifty_monthly_expiry": False,
        "is_nifty_weekly_expiry": False,
        "is_banknifty_weekly_expiry": False,
    }),
    "data_quality": json.dumps({
        "bhavcopy_eq_rows": 1823,
        "flow_rows": 2,
        "has_fx": True,
        "has_nifty_close": True,
        "has_banknifty_close": True,
        "has_vix": True,
        "has_factors": True,
    }),
}

# --- Event: Election results (counting day, June 4 2024) ---
EVENT = {
    "headline": "2024 General Election results: BJP wins 240 seats, loses single-party majority; NDA at 292",
    "event_type": "political",
    "source_tier": 1,
    "raw_text": (
        "The Election Commission of India declared results for the 2024 Lok Sabha elections. "
        "BJP won 240 seats, well below the 272 simple majority mark and far below the exit poll "
        "predictions of 350-370 for NDA. NDA coalition secured 292 seats total. Congress-led INDIA "
        "alliance won 234 seats, significantly outperforming expectations. Markets had priced in "
        "exit poll predictions of a massive BJP majority (350+), with Nifty rallying +3.3% on "
        "June 3 in euphoria. The actual results represent a major negative surprise: BJP will need "
        "coalition partners to form government, weakening its reform mandate. Key states Bihar, "
        "UP, Maharashtra saw unexpected swings. Modi will be PM for a third term but with "
        "constrained policy execution."
    ),
    "extracted_entities": {
        "bjp_seats": 240,
        "nda_seats": 292,
        "majority_mark": 272,
        "india_alliance_seats": 234,
        "exit_poll_nda_predicted": "350-370",
        "bjp_seat_shortfall_vs_majority": 32,
        "pm_continuity": True,
    },
}

# Actual outcome
ACTUAL_NIFTY_RETURN_PCT = -5.93
ACTUAL_NIFTY_CLOSE_JUN4 = 21884.50
ACTUAL_NIFTY_LOW_JUN4 = 21281.45  # Intraday low


def run_replay(mode: str = "auto") -> ConsensusResult | list[tuple[str, str]]:
    """Run all 8 agents against the June 3, 2024 state + election results event."""
    logger.info("=== Election June 2024 Replay ===")
    logger.info("T-1 state: Nifty=%.2f, VIX=%.2f",
                MARKET_STATE["nifty50_close"], MARKET_STATE["india_vix"])
    logger.info("Event: %s", EVENT["headline"])

    result = run_all(
        d=date(2024, 6, 3),
        market_state=MARKET_STATE,
        event=EVENT,
        mode=mode,
    )

    if isinstance(result, ConsensusResult):
        evaluate(result)

    return result


def evaluate(consensus: ConsensusResult) -> dict:
    """Compare consensus to actual outcome (-5.93%)."""
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
    }

    print("\n" + "=" * 60)
    print("REPLAY EVALUATION: Election Results June 4, 2024")
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
