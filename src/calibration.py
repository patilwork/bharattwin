"""
Agent calibration — per-agent accuracy analysis across all replay cases.

Computes:
  - Per-agent error (base_pct vs actual)
  - Per-agent direction accuracy
  - Conviction-accuracy correlation
  - Optimal conviction weights (based on historical accuracy)

Usage:
    python -m src.calibration
"""

from __future__ import annotations

import io
import contextlib
import json
import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ReplayCase:
    name: str
    actual_pct: float
    agent_responses: dict[str, dict]  # agent_id → response dict


@dataclass
class AgentScore:
    agent_id: str
    cases: int = 0
    total_error: float = 0.0
    direction_correct: int = 0
    total_conviction: int = 0
    errors: list[float] = field(default_factory=list)
    predictions: list[float] = field(default_factory=list)
    actuals: list[float] = field(default_factory=list)


def load_all_cases() -> list[ReplayCase]:
    """Load all replay cases with their agent responses."""
    cases = []

    # Case 1: RBI Hike May 2022
    from src.replay.run_incontext_007 import AGENT_RESPONSES as rbi_resp
    from src.replay.cases.rbi_hike_may2022 import ACTUAL_NIFTY_RETURN_PCT as rbi_actual
    cases.append(ReplayCase("RBI Hike May 2022", rbi_actual, rbi_resp))

    # Case 2: Election June 2024
    from src.replay.run_election_010 import AGENT_RESPONSES as elec_resp
    from src.replay.cases.election_june2024 import ACTUAL_NIFTY_RETURN_PCT as elec_actual
    cases.append(ReplayCase("Election June 2024", elec_actual, elec_resp))

    # Case 3: Exit Poll June 2024
    from src.replay.run_exitpoll_011 import AGENT_RESPONSES as exit_resp
    from src.replay.cases.exit_poll_june2024 import ACTUAL_NIFTY_RETURN_PCT as exit_actual
    cases.append(ReplayCase("Exit Poll June 2024", exit_actual, exit_resp))

    # Case 4: Live Mar16→17
    from src.replay.run_live_008 import AGENT_RESPONSES as live_resp, ACTUAL_NIFTY_RETURN_PCT as live_actual
    cases.append(ReplayCase("Live Mar16→17", live_actual, live_resp))

    return cases


def calibrate() -> dict[str, AgentScore]:
    """Run calibration across all cases, return per-agent scores."""
    cases = load_all_cases()
    scores: dict[str, AgentScore] = {}

    for case in cases:
        for agent_id, resp in case.agent_responses.items():
            if agent_id not in scores:
                scores[agent_id] = AgentScore(agent_id=agent_id)

            s = scores[agent_id]
            s.cases += 1

            base_pct = resp["nifty_return"]["base_pct"]
            error = abs(base_pct - case.actual_pct)
            s.total_error += error
            s.errors.append(error)
            s.predictions.append(base_pct)
            s.actuals.append(case.actual_pct)
            s.total_conviction += resp["conviction"]

            # Direction check
            direction = resp["direction"]
            actual_dir = "BUY" if case.actual_pct > 0.25 else ("SELL" if case.actual_pct < -0.25 else "HOLD")
            if direction == actual_dir:
                s.direction_correct += 1
            elif direction == "HOLD" and abs(case.actual_pct) < 1.0:
                s.direction_correct += 1  # HOLD is acceptable for small moves

    return scores


def print_calibration() -> None:
    """Print calibration report."""
    scores = calibrate()
    cases = load_all_cases()

    print()
    print("=" * 100)
    print("AGENT CALIBRATION REPORT")
    print(f"Cases: {len(cases)} | Agents: {len(scores)}")
    print("=" * 100)

    # Per-agent summary
    print(f"\n{'Agent':<22} {'Cases':>5} {'AvgErr':>8} {'RMSE':>8} {'DirAcc':>8} {'AvgConv':>8} {'Rank':>6}")
    print("-" * 68)

    ranked = sorted(scores.values(), key=lambda s: s.total_error / max(s.cases, 1))
    for rank, s in enumerate(ranked, 1):
        avg_err = s.total_error / s.cases if s.cases else 0
        rmse = math.sqrt(sum(e ** 2 for e in s.errors) / len(s.errors)) if s.errors else 0
        dir_acc = s.direction_correct / s.cases * 100 if s.cases else 0
        avg_conv = s.total_conviction / s.cases if s.cases else 0
        print(f"{s.agent_id:<22} {s.cases:>5} {avg_err:>7.2f}pp {rmse:>7.2f}pp {dir_acc:>7.0f}% {avg_conv:>7.1f} {rank:>5}.")

    # Per-case breakdown
    print(f"\n{'─' * 100}")
    print("PER-CASE BREAKDOWN")
    print(f"{'─' * 100}")

    for case in cases:
        print(f"\n  {case.name} (actual: {case.actual_pct:+.2f}%)")
        agents_sorted = sorted(
            case.agent_responses.items(),
            key=lambda x: abs(x[1]["nifty_return"]["base_pct"] - case.actual_pct)
        )
        for agent_id, resp in agents_sorted:
            base = resp["nifty_return"]["base_pct"]
            err = abs(base - case.actual_pct)
            conv = resp["conviction"]
            dir_ = resp["direction"]
            print(f"    {agent_id:<20s} | {dir_:4s} conv={conv} | pred={base:+.2f}% | err={err:.2f}pp")

    # Conviction-accuracy analysis
    print(f"\n{'─' * 100}")
    print("CONVICTION vs ACCURACY")
    print(f"{'─' * 100}")

    conv_buckets: dict[int, list[float]] = {1: [], 2: [], 3: [], 4: [], 5: []}
    for case in cases:
        for agent_id, resp in case.agent_responses.items():
            conv = resp["conviction"]
            err = abs(resp["nifty_return"]["base_pct"] - case.actual_pct)
            conv_buckets[conv].append(err)

    print(f"\n  {'Conviction':>10} {'Count':>7} {'AvgErr':>10} {'Insight'}")
    print(f"  {'-' * 60}")
    for conv in sorted(conv_buckets.keys()):
        errs = conv_buckets[conv]
        if errs:
            avg = sum(errs) / len(errs)
            insight = "most accurate" if avg == min(sum(v) / len(v) for v in conv_buckets.values() if v) else ""
            print(f"  {conv:>10} {len(errs):>7} {avg:>9.2f}pp  {insight}")

    # Optimal weights suggestion
    print(f"\n{'─' * 100}")
    print("SUGGESTED CONVICTION ADJUSTMENTS")
    print(f"{'─' * 100}")

    for s in ranked:
        avg_err = s.total_error / s.cases if s.cases else 0
        avg_conv = s.total_conviction / s.cases if s.cases else 0
        # Lower error = higher suggested weight
        if avg_err < 0.5:
            suggestion = f"increase weight (avg err {avg_err:.2f}pp — top tier)"
        elif avg_err < 1.0:
            suggestion = f"keep current weight (avg err {avg_err:.2f}pp — solid)"
        elif avg_err < 1.5:
            suggestion = f"slightly reduce weight (avg err {avg_err:.2f}pp — moderate)"
        else:
            suggestion = f"reduce weight (avg err {avg_err:.2f}pp — needs calibration)"
        print(f"  {s.agent_id:<22} → {suggestion}")

    print(f"\n{'=' * 100}")


if __name__ == "__main__":
    print_calibration()
