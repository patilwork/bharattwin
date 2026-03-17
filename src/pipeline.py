"""
BharatTwin end-to-end pipeline.

Single entry point: ingest → build state → compute factors → run agents → store results.

Usage:
    python -m src.pipeline                          # today (T-1)
    python -m src.pipeline 2026-03-16               # specific date
    python -m src.pipeline 2026-03-16 --with-event  # look up event_store for that date
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta

from src.factors.engine import compute_and_store
from src.stores.event_store import get_by_date, to_agent_event
from src.stores.market_state import build, get

logger = logging.getLogger(__name__)


def run_pipeline(
    d: date,
    with_event: bool = False,
    agent_mode: str = "auto",
) -> dict:
    """
    Run the full pipeline for a given date.

    Steps:
        1. Build market_state (from bhavcopy, flows, macro, calendar)
        2. Compute factors
        3. Optionally look up events
        4. Run all agents
        5. Print/store results

    Returns summary dict.
    """
    summary = {"date": str(d), "steps": {}}

    # Step 1: Build market_state
    logger.info("pipeline: step 1 — build market_state for %s", d)
    state = build(d, force=True)
    if not state:
        logger.error("pipeline: no market_state for %s (not a trading day or no bhavcopy)", d)
        summary["steps"]["market_state"] = "SKIP — no data"
        return summary
    summary["steps"]["market_state"] = "OK"

    # Step 2: Compute factors
    logger.info("pipeline: step 2 — compute factors for %s", d)
    factors = compute_and_store(d, force=True)
    if factors:
        summary["steps"]["factors"] = {
            "momentum_1d": factors.get("momentum_1d"),
            "momentum_5d": factors.get("momentum_5d"),
            "momentum_20d": factors.get("momentum_20d"),
            "volatility_20d": factors.get("volatility_20d"),
            "breadth_adv": factors.get("breadth_adv"),
            "breadth_dec": factors.get("breadth_dec"),
        }
    else:
        summary["steps"]["factors"] = "SKIP — no EQ data"

    # Step 3: Fetch complete market_state (with factors now embedded)
    full_state = get(d)
    if not full_state:
        logger.error("pipeline: cannot retrieve built market_state for %s", d)
        return summary

    # Step 4: Look up events
    event = None
    if with_event:
        events = get_by_date(d)
        if events:
            event = to_agent_event(events[0])  # Use the first/primary event
            logger.info("pipeline: found event — %s", event["headline"][:60])
            summary["steps"]["event"] = event["headline"]
        else:
            logger.info("pipeline: no events for %s", d)
            summary["steps"]["event"] = "none"

    # Step 5: Run agents
    logger.info("pipeline: step 5 — run agents (mode=%s)", agent_mode)
    from src.agents.runner import run_all
    result = run_all(d=d, market_state=full_state, event=event, mode=agent_mode)

    if isinstance(result, list):
        # Prompt mode — return prompt strings
        summary["steps"]["agents"] = f"prompt mode — {len(result)} prompts generated"
        summary["prompts"] = [(aid, prompt[:200] + "...") for aid, prompt in result]
    else:
        # API mode — got ConsensusResult
        summary["steps"]["agents"] = "OK — consensus computed"
        summary["consensus"] = {
            "direction": result.consensus_direction.value,
            "avg_return_pct": result.avg_return_pct,
            "range": {
                "low": result.return_range.low_pct,
                "base": result.return_range.base_pct,
                "high": result.return_range.high_pct,
            },
            "bull": result.bull_count,
            "bear": result.bear_count,
            "neutral": result.neutral_count,
        }

        # Store to DB
        from src.agents.runner import store_results
        stored = store_results(d, result)
        summary["steps"]["store"] = f"OK — {stored} rows"

    return summary


def print_summary(summary: dict) -> None:
    """Pretty-print pipeline summary."""
    print(f"\n{'=' * 60}")
    print(f"PIPELINE SUMMARY: {summary['date']}")
    print(f"{'=' * 60}")

    for step, detail in summary.get("steps", {}).items():
        if isinstance(detail, dict):
            print(f"  {step}:")
            for k, v in detail.items():
                print(f"    {k}: {v}")
        else:
            print(f"  {step}: {detail}")

    if "consensus" in summary:
        c = summary["consensus"]
        print(f"\n  CONSENSUS: {c['direction']} | return={c['avg_return_pct']:+.2f}%")
        print(f"  Range: [{c['range']['low']:+.2f}%, {c['range']['high']:+.2f}%]")
        print(f"  Votes: Bull={c['bull']} Bear={c['bear']} Neutral={c['neutral']}")

    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BharatTwin pipeline")
    parser.add_argument("date", nargs="?", default=None, help="Date (YYYY-MM-DD)")
    parser.add_argument("--with-event", action="store_true", help="Look up events for the date")
    parser.add_argument("--mode", default="auto", choices=["api", "prompt", "auto"])
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    if args.date:
        d = date.fromisoformat(args.date)
    else:
        d = date.today() - timedelta(days=1)

    summary = run_pipeline(d, with_event=args.with_event, agent_mode=args.mode)
    print_summary(summary)
