"""
Backtesting harness — run agents across multiple historical dates.

Computes aggregate hit rate, tracking error, and per-agent performance.
Uses market_state from DB for each date, with optional event lookup.

Usage:
    python -m src.backtest                  # backtest all DB dates
    python -m src.backtest --replay-only    # backtest replay cases only
"""

from __future__ import annotations

import argparse
import io
import contextlib
import json
import logging
import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    date: date
    actual_pct: float | None
    predicted_pct: float | None
    direction_predicted: str | None
    direction_actual: str | None
    direction_correct: bool = False
    error_pp: float | None = None
    in_range: bool = False
    bull_count: int = 0
    bear_count: int = 0
    neutral_count: int = 0


def _actual_return(d: date, next_close: float | None, current_close: float | None) -> float | None:
    """Compute actual next-day return."""
    if current_close and next_close and current_close > 0:
        return round((next_close - current_close) / current_close * 100, 4)
    return None


def backtest_replays() -> list[BacktestResult]:
    """Backtest all replay cases (hard-coded market states + in-context responses)."""
    results = []

    replay_modules = [
        ("RBI Hike May 2022", "src.replay.run_incontext_007", "run_incontext_replay",
         "src.replay.cases.rbi_hike_may2022", date(2022, 5, 2)),
        ("Election June 2024", "src.replay.run_election_010", "run_election_replay",
         "src.replay.cases.election_june2024", date(2024, 6, 3)),
        ("Exit Poll June 2024", "src.replay.run_exitpoll_011", "run_exitpoll_replay",
         "src.replay.cases.exit_poll_june2024", date(2024, 5, 31)),
        ("Live Mar16→17", "src.replay.run_live_008", "run_live_simulation",
         "src.replay.run_live_008", date(2026, 3, 16)),
    ]

    for name, run_mod, run_fn, case_mod, d in replay_modules:
        try:
            import importlib
            rm = importlib.import_module(run_mod)
            cm = importlib.import_module(case_mod)

            fn = getattr(rm, run_fn)
            actual = getattr(cm, "ACTUAL_NIFTY_RETURN_PCT")

            with contextlib.redirect_stdout(io.StringIO()):
                consensus = fn()

            dir_actual = "BUY" if actual > 0.25 else ("SELL" if actual < -0.25 else "HOLD")
            dir_pred = consensus.consensus_direction.value
            dir_correct = (dir_pred == dir_actual) or (dir_pred == "HOLD" and abs(actual) < 1.0)

            error = abs(consensus.avg_return_pct - actual)
            in_range = consensus.return_range.low_pct <= actual <= consensus.return_range.high_pct

            results.append(BacktestResult(
                date=d,
                actual_pct=actual,
                predicted_pct=consensus.avg_return_pct,
                direction_predicted=dir_pred,
                direction_actual=dir_actual,
                direction_correct=dir_correct,
                error_pp=error,
                in_range=in_range,
                bull_count=consensus.bull_count,
                bear_count=consensus.bear_count,
                neutral_count=consensus.neutral_count,
            ))
        except Exception as e:
            logger.error("backtest: %s failed — %s", name, e)
            results.append(BacktestResult(date=d, actual_pct=None, predicted_pct=None,
                                          direction_predicted=None, direction_actual=None))

    return results


def print_backtest(results: list[BacktestResult]) -> None:
    """Print backtest report."""
    valid = [r for r in results if r.actual_pct is not None and r.predicted_pct is not None]

    print()
    print("=" * 95)
    print("BACKTEST REPORT")
    print(f"Total cases: {len(results)} | Valid: {len(valid)}")
    print("=" * 95)

    print(f"\n{'Date':<14} {'Actual':>8} {'Predicted':>10} {'Error':>8} {'Dir':>5} {'InRange':>8} {'Votes':<12}")
    print("-" * 70)

    for r in results:
        if r.actual_pct is not None:
            act = f"{r.actual_pct:+.2f}%"
            pred = f"{r.predicted_pct:+.2f}%" if r.predicted_pct is not None else "N/A"
            err = f"{r.error_pp:.2f}pp" if r.error_pp is not None else "N/A"
            d_ok = "YES" if r.direction_correct else "NO"
            ir = "YES" if r.in_range else "NO"
            votes = f"{r.bull_count}B/{r.bear_count}S/{r.neutral_count}H"
            print(f"{str(r.date):<14} {act:>8} {pred:>10} {err:>8} {d_ok:>5} {ir:>8} {votes:<12}")
        else:
            print(f"{str(r.date):<14} {'ERR':>8}")

    if valid:
        avg_error = sum(r.error_pp for r in valid) / len(valid)
        rmse = math.sqrt(sum(r.error_pp ** 2 for r in valid) / len(valid))
        dir_correct = sum(1 for r in valid if r.direction_correct)
        in_range = sum(1 for r in valid if r.in_range)

        print("-" * 70)
        print(f"{'SUMMARY':<14} {'':>8} {'':>10} {avg_error:>7.2f}pp {dir_correct}/{len(valid):>3} {in_range}/{len(valid):>6}")
        print(f"\n  Average error:    {avg_error:.2f}pp")
        print(f"  RMSE:             {rmse:.2f}pp")
        print(f"  Direction correct: {dir_correct}/{len(valid)} ({dir_correct/len(valid)*100:.0f}%)")
        print(f"  In range:         {in_range}/{len(valid)} ({in_range/len(valid)*100:.0f}%)")

        # Event vs no-event split
        event_cases = [r for r in valid if abs(r.actual_pct) > 2.0]
        no_event = [r for r in valid if abs(r.actual_pct) <= 2.0]

        if event_cases:
            avg_event = sum(r.error_pp for r in event_cases) / len(event_cases)
            print(f"\n  Event cases ({len(event_cases)}):   avg error {avg_event:.2f}pp")
        if no_event:
            avg_no = sum(r.error_pp for r in no_event) / len(no_event)
            print(f"  No-event ({len(no_event)}):     avg error {avg_no:.2f}pp")

    print(f"\n{'=' * 95}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BharatTwin backtesting")
    parser.add_argument("--replay-only", action="store_true", help="Only backtest replay cases")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)

    results = backtest_replays()
    print_backtest(results)
