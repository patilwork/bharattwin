"""
Swarm runner — execute a swarm of agents and aggregate via population voting.

Two execution modes:
  1. FULL SWARM (API mode): Each agent gets an LLM call. Expensive but high-fidelity.
     - 800 agents × ~$0.003/call = ~$2.40 per prediction
     - Use claude-haiku for swarm agents (fast + cheap), claude-sonnet for archetypes

  2. ARCHETYPE + AMPLIFICATION (default): Run the 8 archetypes via LLM, then
     statistically amplify to the swarm using the variant profiles as noise.
     - 8 LLM calls (archetypes) + statistical projection to 800 agents
     - Cost: ~$0.04 per prediction (same as before)
     - Each variant's prediction = archetype prediction + persona-specific noise

The amplification approach is what makes BharatTwin practical:
  - You get "1,000 agents" for the cost of 8 LLM calls
  - The noise is structured (experience, risk tolerance, conviction bias)
  - Population statistics (mean, std, distribution shape) are meaningful

Usage:
    from src.swarm.runner import run_swarm
    result = run_swarm(market_state, event, n_per_archetype=100)
"""

from __future__ import annotations

import json
import math
import random
from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any

from src.agents.schemas import Direction, ReturnRange
from src.swarm.generator import SwarmAgent, generate_swarm, ARCHETYPES


@dataclass
class SwarmVote:
    """A single agent's vote in the swarm."""
    agent_id: str
    archetype: str
    direction: Direction
    base_pct: float
    conviction: int


@dataclass
class SwarmConsensus:
    """Aggregated swarm result."""
    total_agents: int
    buy_count: int
    sell_count: int
    hold_count: int
    buy_pct: float
    sell_pct: float
    hold_pct: float
    mean_return_pct: float
    median_return_pct: float
    std_return_pct: float
    range_low: float
    range_high: float
    consensus_direction: Direction
    conviction_score: float  # 0-100 based on voting margin
    votes: list[SwarmVote]


def _noise_for_variant(agent: SwarmAgent, archetype_base: float, archetype_conv: int) -> tuple[float, int, str]:
    """
    Generate noise for a swarm variant based on its profile.

    Returns (adjusted_return, adjusted_conviction, direction).
    """
    rng = random.Random(hash(agent.agent_id))

    # Conviction bias shifts
    conv_shift = {
        "cautious": -1,
        "moderate": 0,
        "aggressive": +1,
        "contrarian": 0,  # handled separately
    }

    # Risk tolerance affects magnitude
    risk_scale = {
        "very_low": 0.5,
        "low": 0.75,
        "medium": 1.0,
        "high": 1.25,
        "very_high": 1.5,
    }

    # Experience affects noise (more experience = less noise)
    exp_noise_scale = max(0.3, 1.0 - (agent.experience_years - 2) / 30)

    # Base noise: gaussian with std proportional to base magnitude
    noise_std = max(0.2, abs(archetype_base) * 0.3 * exp_noise_scale)
    noise = rng.gauss(0, noise_std)

    # Risk scale the return
    scale = risk_scale.get(agent.risk_tolerance, 1.0)
    adjusted = archetype_base * scale + noise

    # Conviction adjustment
    adjusted_conv = max(1, min(5, archetype_conv + conv_shift.get(agent.conviction_bias, 0)))

    # Contrarian: sometimes flips direction (20% chance)
    if agent.conviction_bias == "contrarian" and rng.random() < 0.20:
        adjusted = -adjusted * 0.5  # Flip but reduce magnitude
        adjusted_conv = max(1, adjusted_conv - 1)

    # Direction from return
    if adjusted > 0.25:
        direction = "BUY"
    elif adjusted < -0.25:
        direction = "SELL"
    else:
        direction = "HOLD"

    return round(adjusted, 4), adjusted_conv, direction


def amplify_to_swarm(
    archetype_decisions: dict[str, dict],
    n_per_archetype: int = 100,
) -> SwarmConsensus:
    """
    Take 8 archetype decisions and amplify to a full swarm.

    Args:
        archetype_decisions: {archetype_id: {direction, base_pct, conviction, ...}}
        n_per_archetype: Variants per archetype.

    Returns:
        SwarmConsensus with population-level statistics.
    """
    swarm = generate_swarm(n_per_archetype=n_per_archetype)
    votes: list[SwarmVote] = []

    for agent in swarm:
        arch_dec = archetype_decisions.get(agent.archetype)
        if not arch_dec:
            continue

        arch_base = arch_dec["nifty_return"]["base_pct"]
        arch_conv = arch_dec["conviction"]

        adj_return, adj_conv, direction = _noise_for_variant(agent, arch_base, arch_conv)

        votes.append(SwarmVote(
            agent_id=agent.agent_id,
            archetype=agent.archetype,
            direction=Direction(direction),
            base_pct=adj_return,
            conviction=adj_conv,
        ))

    # Aggregate
    returns = [v.base_pct for v in votes]
    returns_sorted = sorted(returns)
    n = len(returns)

    buy_count = sum(1 for v in votes if v.direction == Direction.BUY)
    sell_count = sum(1 for v in votes if v.direction == Direction.SELL)
    hold_count = sum(1 for v in votes if v.direction == Direction.HOLD)

    mean_ret = sum(returns) / n if n else 0
    median_ret = returns_sorted[n // 2] if n else 0
    std_ret = math.sqrt(sum((r - mean_ret) ** 2 for r in returns) / (n - 1)) if n > 1 else 0

    # Consensus: majority vote
    if buy_count > sell_count and buy_count > hold_count:
        consensus_dir = Direction.BUY
    elif sell_count > buy_count and sell_count > hold_count:
        consensus_dir = Direction.SELL
    else:
        consensus_dir = Direction.HOLD

    # Conviction score: margin of majority (0-100)
    max_votes = max(buy_count, sell_count, hold_count)
    conviction_score = round(max_votes / n * 100, 1) if n else 0

    # Range: 5th and 95th percentile
    p5_idx = max(0, int(n * 0.05))
    p95_idx = min(n - 1, int(n * 0.95))
    range_low = returns_sorted[p5_idx] if returns_sorted else 0
    range_high = returns_sorted[p95_idx] if returns_sorted else 0

    return SwarmConsensus(
        total_agents=n,
        buy_count=buy_count,
        sell_count=sell_count,
        hold_count=hold_count,
        buy_pct=round(buy_count / n * 100, 1) if n else 0,
        sell_pct=round(sell_count / n * 100, 1) if n else 0,
        hold_pct=round(hold_count / n * 100, 1) if n else 0,
        mean_return_pct=round(mean_ret, 4),
        median_return_pct=round(median_ret, 4),
        std_return_pct=round(std_ret, 4),
        range_low=round(range_low, 4),
        range_high=round(range_high, 4),
        consensus_direction=consensus_dir,
        conviction_score=conviction_score,
        votes=votes,
    )


def print_swarm_result(result: SwarmConsensus, title: str = "") -> None:
    """Pretty-print swarm consensus."""
    dir_emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}

    print(f"\n{'=' * 70}")
    if title:
        print(f"SWARM CONSENSUS — {title}")
    else:
        print("SWARM CONSENSUS")
    print(f"{'=' * 70}")
    print(f"  Agents: {result.total_agents}")
    print(f"  Direction: {dir_emoji.get(result.consensus_direction.value, '')} {result.consensus_direction.value} "
          f"(conviction: {result.conviction_score:.0f}%)")
    print(f"  Mean return:   {result.mean_return_pct:+.2f}%")
    print(f"  Median return: {result.median_return_pct:+.2f}%")
    print(f"  Std dev:       {result.std_return_pct:.2f}pp")
    print(f"  Range (5-95%): [{result.range_low:+.2f}%, {result.range_high:+.2f}%]")
    print(f"")
    print(f"  Votes: {result.buy_count} BUY ({result.buy_pct:.0f}%) | "
          f"{result.sell_count} SELL ({result.sell_pct:.0f}%) | "
          f"{result.hold_count} HOLD ({result.hold_pct:.0f}%)")

    # Per-archetype breakdown
    arch_votes: dict[str, list[SwarmVote]] = {}
    for v in result.votes:
        arch_votes.setdefault(v.archetype, []).append(v)

    print(f"\n  Per-archetype breakdown:")
    for arch in ARCHETYPES:
        avotes = arch_votes.get(arch, [])
        if not avotes:
            continue
        arch_mean = sum(v.base_pct for v in avotes) / len(avotes)
        arch_buy = sum(1 for v in avotes if v.direction == Direction.BUY)
        arch_sell = sum(1 for v in avotes if v.direction == Direction.SELL)
        arch_hold = sum(1 for v in avotes if v.direction == Direction.HOLD)
        print(f"    {arch:<20s}: mean={arch_mean:+.2f}% | "
              f"{arch_buy}B/{arch_sell}S/{arch_hold}H")

    print(f"{'=' * 70}")


if __name__ == "__main__":
    # Demo: amplify RBI hike replay to swarm
    from src.replay.run_incontext_007 import AGENT_RESPONSES as rbi_resp
    from src.replay.cases.rbi_hike_may2022 import ACTUAL_NIFTY_RETURN_PCT

    print("\nAmplifying RBI Hike replay to 800-agent swarm...")
    result = amplify_to_swarm(rbi_resp, n_per_archetype=100)
    print_swarm_result(result, "RBI Hike May 2022")
    print(f"\n  Actual: {ACTUAL_NIFTY_RETURN_PCT:+.2f}%")
    print(f"  Swarm error: {abs(result.mean_return_pct - ACTUAL_NIFTY_RETURN_PCT):.2f}pp")

    # Exit poll replay
    from src.replay.run_exitpoll_011 import AGENT_RESPONSES as exit_resp
    from src.replay.cases.exit_poll_june2024 import ACTUAL_NIFTY_RETURN_PCT as exit_actual

    print("\nAmplifying Exit Poll replay to 800-agent swarm...")
    result2 = amplify_to_swarm(exit_resp, n_per_archetype=100)
    print_swarm_result(result2, "Exit Poll June 2024")
    print(f"\n  Actual: {exit_actual:+.2f}%")
    print(f"  Swarm error: {abs(result2.mean_return_pct - exit_actual):.2f}pp")
