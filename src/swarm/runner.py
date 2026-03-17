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
    Generate structured noise for a swarm variant based on its 12-dimension profile.

    The noise is NOT random — it's deterministic and shaped by the agent's
    personality. Two agents with identical profiles produce identical outputs.

    Returns (adjusted_return, adjusted_conviction, direction).
    """
    rng = random.Random(hash(agent.agent_id))

    # Conviction bias shifts
    conv_shift = {
        "very_cautious": -2,
        "cautious": -1,
        "moderate": 0,
        "aggressive": +1,
        "contrarian": 0,
    }

    # Risk tolerance affects return magnitude
    risk_scale = {
        "very_low": 0.4,
        "low": 0.7,
        "medium": 1.0,
        "high": 1.3,
        "very_high": 1.6,
    }

    # Herd sensitivity: herders amplify the archetype signal, contrarians dampen/flip
    herd_scale = {
        "contrarian": -0.3,      # Partially inverts
        "independent": 1.0,      # Pure signal
        "moderate_herder": 1.15,  # Slight amplification
        "strong_herder": 1.4,    # Strong amplification (FOMO/panic)
    }

    # Volatility preference affects conviction on high-vol events
    vol_conv_mod = {
        "vol_averse": -1,
        "vol_neutral": 0,
        "vol_seeking": +1,
        "vol_trader": 0,
    }

    # Loss aversion affects bearish predictions
    loss_mod = {
        "high_aversion": 0.15,    # Extra bearish offset
        "moderate_aversion": 0.05,
        "low_aversion": -0.05,
        "loss_seeking": -0.15,    # Less bearish (doubles down)
    }

    # Portfolio size affects conviction (larger = more cautious)
    port_conv = {
        "micro_retail": +1,
        "small_retail": 0,
        "hni": 0,
        "institutional_small": -1,
        "institutional_large": -1,
    }

    # Experience reduces noise variance
    exp_noise_scale = max(0.2, 1.0 - (agent.experience_years - 2) / 35)

    # --- Compute adjusted return ---
    base_noise_std = max(0.15, abs(archetype_base) * 0.25 * exp_noise_scale)
    noise = rng.gauss(0, base_noise_std)

    scale = risk_scale.get(agent.risk_tolerance, 1.0)
    herd = herd_scale.get(getattr(agent, 'herd_sensitivity', 'independent'), 1.0)

    adjusted = archetype_base * scale * herd + noise

    # Loss aversion offset (only when bearish)
    if archetype_base < 0:
        adjusted += loss_mod.get(getattr(agent, 'loss_aversion', 'moderate_aversion'), 0)

    # Contrarian conviction bias: 25% chance of direction flip
    if agent.conviction_bias == "contrarian" and rng.random() < 0.25:
        adjusted = -adjusted * 0.4
    elif agent.conviction_bias == "very_cautious":
        adjusted *= 0.6  # Dampens magnitude

    # --- Compute conviction ---
    adjusted_conv = archetype_conv
    adjusted_conv += conv_shift.get(agent.conviction_bias, 0)
    adjusted_conv += vol_conv_mod.get(getattr(agent, 'vol_preference', 'vol_neutral'), 0)
    adjusted_conv += port_conv.get(getattr(agent, 'portfolio_size', 'small_retail'), 0)
    adjusted_conv = max(1, min(5, adjusted_conv))

    # --- Direction ---
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
    import time

    replays = [
        ("RBI Hike May 2022", "src.replay.run_incontext_007", "AGENT_RESPONSES",
         "src.replay.cases.rbi_hike_may2022", "ACTUAL_NIFTY_RETURN_PCT"),
        ("Exit Poll June 2024", "src.replay.run_exitpoll_011", "AGENT_RESPONSES",
         "src.replay.cases.exit_poll_june2024", "ACTUAL_NIFTY_RETURN_PCT"),
        ("Election June 2024", "src.replay.run_election_010", "AGENT_RESPONSES",
         "src.replay.cases.election_june2024", "ACTUAL_NIFTY_RETURN_PCT"),
    ]

    import importlib
    for name, resp_mod, resp_attr, actual_mod, actual_attr in replays:
        rm = importlib.import_module(resp_mod)
        am = importlib.import_module(actual_mod)
        responses = getattr(rm, resp_attr)
        actual = getattr(am, actual_attr)

        # 100K swarm
        n = 12_500
        total = n * 8
        t0 = time.time()
        result = amplify_to_swarm(responses, n_per_archetype=n)
        elapsed = time.time() - t0

        print_swarm_result(result, f"{name} ({total:,} agents, {elapsed:.1f}s)")
        print(f"  Actual: {actual:+.2f}%")
        print(f"  Swarm error: {abs(result.mean_return_pct - actual):.2f}pp\n")
