"""
Scoring engine — compare predictions to actual outcomes.

After market close, fetches actual Nifty return and scores the previous
day's consensus prediction. Stores scores in agent_decisions table.

Usage:
    python -m src.scoring                    # score yesterday's prediction
    python -m src.scoring 2026-03-16         # score specific date
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from typing import Any

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def get_next_trading_day_close(d: date) -> float | None:
    """
    Get the Nifty close for the next trading day after d.
    This is the 'actual outcome' for a prediction made on date d.
    """
    engine = _get_engine()
    with engine.connect() as conn:
        # Find the next market_state row after d
        row = conn.execute(text("""
            SELECT session_id, nifty50_close
            FROM market_state
            WHERE universe_id = 'nifty50'
              AND session_id > :sid
              AND nifty50_close IS NOT NULL
            ORDER BY session_id
            LIMIT 1
        """), {"sid": f"CM_{d}"}).first()
    engine.dispose()

    if row:
        return float(row[1])
    return None


def score_date(d: date) -> dict | None:
    """
    Score the prediction for date d.

    d is the T-1 date (the state date). The prediction was for T (next day).
    We compare the consensus prediction to the actual T return.

    Returns score dict or None if insufficient data.
    """
    engine = _get_engine()

    # Get the market_state for d (T-1)
    with engine.connect() as conn:
        t1_row = conn.execute(text("""
            SELECT nifty50_close FROM market_state
            WHERE session_id = :sid AND universe_id = 'nifty50'
        """), {"sid": f"CM_{d}"}).first()

        if not t1_row or not t1_row[0]:
            logger.warning("scoring: no Nifty close for %s", d)
            engine.dispose()
            return None

        t1_close = float(t1_row[0])

        # Get T (next trading day) close
        t_row = conn.execute(text("""
            SELECT session_id, nifty50_close
            FROM market_state
            WHERE universe_id = 'nifty50'
              AND session_id > :sid
              AND nifty50_close IS NOT NULL
            ORDER BY session_id
            LIMIT 1
        """), {"sid": f"CM_{d}"}).first()

        if not t_row or not t_row[1]:
            logger.warning("scoring: no next-day Nifty close after %s", d)
            engine.dispose()
            return None

        t_close = float(t_row[1])
        t_session = t_row[0]

        # Get agent decisions for this date
        decisions = conn.execute(text("""
            SELECT agent_id, decision, consensus
            FROM agent_decisions
            WHERE run_date = :d
            ORDER BY agent_id
        """), {"d": d}).mappings().fetchall()

    engine.dispose()

    actual_pct = round((t_close - t1_close) / t1_close * 100, 4)

    score = {
        "prediction_date": str(d),
        "outcome_session": t_session,
        "t1_close": t1_close,
        "t_close": t_close,
        "actual_return_pct": actual_pct,
        "actual_direction": "BUY" if actual_pct > 0.25 else ("SELL" if actual_pct < -0.25 else "HOLD"),
    }

    if decisions:
        consensus = decisions[0]["consensus"]
        if isinstance(consensus, str):
            consensus = json.loads(consensus)

        predicted_pct = consensus.get("avg_return_pct", 0)
        predicted_dir = consensus.get("consensus_direction", "HOLD")

        score["predicted_return_pct"] = predicted_pct
        score["predicted_direction"] = predicted_dir
        score["error_pp"] = round(abs(predicted_pct - actual_pct), 4)
        score["direction_correct"] = (
            predicted_dir == score["actual_direction"]
            or (predicted_dir == "HOLD" and abs(actual_pct) < 1.0)
        )

        # Per-agent scores
        agent_scores = []
        for dec_row in decisions:
            dec = dec_row["decision"]
            if isinstance(dec, str):
                dec = json.loads(dec)
            agent_pred = dec.get("nifty_return", {}).get("base_pct", 0)
            agent_dir = dec.get("direction", "HOLD")
            agent_scores.append({
                "agent_id": dec_row["agent_id"],
                "predicted_pct": agent_pred,
                "direction": agent_dir,
                "error_pp": round(abs(agent_pred - actual_pct), 4),
                "direction_correct": (
                    agent_dir == score["actual_direction"]
                    or (agent_dir == "HOLD" and abs(actual_pct) < 1.0)
                ),
            })
        score["agent_scores"] = agent_scores
    else:
        score["predicted_return_pct"] = None
        score["predicted_direction"] = None
        score["error_pp"] = None
        score["direction_correct"] = None
        score["agent_scores"] = []

    return score


def score_all_available() -> list[dict]:
    """Score all dates that have both predictions and outcomes."""
    engine = _get_engine()

    with engine.connect() as conn:
        # Get all dates with agent decisions
        pred_dates = conn.execute(text("""
            SELECT DISTINCT run_date FROM agent_decisions ORDER BY run_date
        """)).fetchall()
    engine.dispose()

    scores = []
    for row in pred_dates:
        score = score_date(row[0])
        if score:
            scores.append(score)

    return scores


def print_score(score: dict) -> None:
    """Print a single date's score."""
    print(f"\n{'=' * 60}")
    print(f"SCORE: {score['prediction_date']} → {score['outcome_session']}")
    print(f"{'=' * 60}")
    print(f"  Nifty: {score['t1_close']:,.2f} → {score['t_close']:,.2f}")
    print(f"  Actual return: {score['actual_return_pct']:+.2f}%")

    if score.get("predicted_return_pct") is not None:
        print(f"  Predicted:     {score['predicted_return_pct']:+.2f}%")
        print(f"  Error:         {score['error_pp']:.2f}pp")
        print(f"  Direction:     {score['predicted_direction']} → {score['actual_direction']} "
              f"({'CORRECT' if score['direction_correct'] else 'WRONG'})")

        if score.get("agent_scores"):
            print(f"\n  Per-agent:")
            for a in sorted(score["agent_scores"], key=lambda x: x["error_pp"]):
                ok = "OK" if a["direction_correct"] else "XX"
                print(f"    {a['agent_id']:<20s} | {a['direction']:4s} | "
                      f"pred={a['predicted_pct']:+.2f}% | err={a['error_pp']:.2f}pp | {ok}")
    else:
        print(f"  No predictions found in agent_decisions table.")

    print(f"{'=' * 60}")


def compute_rolling_accuracy(n: int = 20) -> dict[str, float]:
    """
    Compute rolling accuracy factor for each agent over the last n scored dates.

    Returns: {agent_id: accuracy_factor} where 1.0 = baseline.
    Higher factor = more accurate historically = should get more weight.
    """
    scores = score_all_available()
    if not scores:
        return {}

    # Use last n scores
    recent = scores[-n:]

    agent_errors: dict[str, list[float]] = {}
    for score in recent:
        for a in score.get("agent_scores", []):
            agent_errors.setdefault(a["agent_id"], []).append(a["error_pp"])

    if not agent_errors:
        return {}

    # Compute accuracy factor: inverse of normalized error
    avg_errors = {aid: sum(errs) / len(errs) for aid, errs in agent_errors.items()}
    global_avg = sum(avg_errors.values()) / len(avg_errors)

    if global_avg == 0:
        return {aid: 1.0 for aid in avg_errors}

    return {aid: round(global_avg / max(err, 0.01), 4) for aid, err in avg_errors.items()}


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if len(sys.argv) >= 2:
        d = date.fromisoformat(sys.argv[1])
    else:
        d = date.today() - timedelta(days=1)

    score = score_date(d)
    if score:
        print_score(score)
    else:
        print(f"No data to score for {d}")

    # Also show all available scores
    all_scores = score_all_available()
    if all_scores:
        print(f"\n{'=' * 60}")
        print(f"ALL SCORED DATES ({len(all_scores)})")
        print(f"{'=' * 60}")
        for s in all_scores:
            if s.get("predicted_return_pct") is not None:
                print(f"  {s['prediction_date']}: actual={s['actual_return_pct']:+.2f}% "
                      f"pred={s['predicted_return_pct']:+.2f}% "
                      f"err={s['error_pp']:.2f}pp "
                      f"dir={'OK' if s['direction_correct'] else 'XX'}")
