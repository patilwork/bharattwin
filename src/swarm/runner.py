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
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from src.agents.schemas import Direction, ReturnRange
from src.swarm.generator import SwarmAgent, generate_swarm, ARCHETYPES

# ─── Volume & Price Impact Weights ───────────────────────────────────────────
#
# Based on real Indian market participation data:
#   - NSE turnover breakdown (2024-25): Retail ~36%, FII ~18%, DII ~16%, Prop ~20%
#   - Dabba/operator shadow market: ~₹3L cr/day (estimated, not in official data)
#   - Price impact: institutional block orders > retail dispersed orders
#
# Two weight systems:
#   1. VOLUME_WEIGHT: how many agents per archetype (population representation)
#   2. IMPACT_WEIGHT: how much each archetype's vote counts in final consensus
#
# Volume determines how many variants are generated.
# Impact determines how much each vote moves the consensus.

VOLUME_WEIGHT = {
    # How many agents per archetype (sums to ~1.0, scaled to total_agents)
    "fii_quant":         0.12,   # ~12% — FII desks, global allocators
    "retail_momentum":   0.25,   # ~25% — largest participant group by headcount
    "dealer_hedging":    0.06,   # ~6%  — market makers, limited in number
    "dii_mf":            0.12,   # ~12% — MFs, insurance, pension
    "macro":             0.04,   # ~4%  — chief economists, strategists (few but influential)
    "sector_rotation":   0.04,   # ~4%  — PMS quants, sector analysts
    "corp_earnings":     0.05,   # ~5%  — fundamental analysts
    "event_news":        0.07,   # ~7%  — prop desk event traders
    "operator":          0.10,   # ~10% — operators, syndicates (Ahmedabad/Surat nexus)
    "dabba_speculator":  0.15,   # ~15% — tier-2 satta traders (Rajkot/Indore belt)
}

IMPACT_WEIGHT = {
    # DEFAULT price impact multiplier (used when no regime is detected)
    "fii_quant":         3.0,
    "retail_momentum":   0.5,
    "dealer_hedging":    2.5,
    "dii_mf":            2.0,
    "macro":             1.5,
    "sector_rotation":   1.0,
    "corp_earnings":     1.0,
    "event_news":        1.5,
    "operator":          0.3,
    "dabba_speculator":  0.2,
}

# ─── Regime-Conditional Impact Weights ───────────────────────────────────────
#
# Different market participants dominate in different conditions.
# The regime is detected from the market state BEFORE agents are called,
# then the appropriate weight profile is applied to the swarm consensus.
#
# Each regime multiplies the base IMPACT_WEIGHT by its modifier.
# Modifier > 1.0 = this agent matters MORE in this regime.
# Modifier < 1.0 = this agent matters LESS.

REGIME_IMPACT_MODIFIERS = {
    # ── EVENT DAY: policy announcement, election, surprise ──
    # Event traders and macro analysts dominate. Operators are noise.
    "event": {
        "fii_quant":         1.5,   # FIIs react fast to macro events
        "retail_momentum":   0.3,   # Retail is slow to react, just panics
        "dealer_hedging":    2.0,   # Gamma squeeze amplifies event moves
        "dii_mf":            0.5,   # DIIs react slowly (investment committee meets weekly)
        "macro":             2.5,   # Macro analyst is THE voice on policy events
        "sector_rotation":   1.5,   # Rotation map activates immediately
        "corp_earnings":     0.3,   # Earnings don't change on event day
        "event_news":        3.0,   # This is their moment — maximum relevance
        "operator":          0.1,   # Operators are irrelevant for Nifty events
        "dabba_speculator":  0.1,   # Dabba follows, doesn't lead on events
    },

    # ── VIX SPIKE (VIX > 22): fear regime, hedging-driven ──
    # Dealers and FIIs dominate via gamma hedging and block selling.
    "vix_spike": {
        "fii_quant":         2.0,   # FII risk-off selling accelerates
        "retail_momentum":   0.5,   # Retail panics but in dispersed, delayed way
        "dealer_hedging":    3.0,   # Dealer gamma hedging IS the market in high-VIX
        "dii_mf":            1.5,   # DII "buy the dip" is the counterforce
        "macro":             1.0,
        "sector_rotation":   0.5,   # Correlation goes to 1, sectors don't matter
        "corp_earnings":     0.3,   # Fundamentals irrelevant in panic
        "event_news":        1.5,   # Fast money trades the vol
        "operator":          0.1,   # Operators hide during VIX spikes
        "dabba_speculator":  0.3,   # Dabba gets margin-called, amplifies panic
    },

    # ── TRENDING BULL (mom_20d > 5%): momentum regime ──
    # Retail FOMO and DII SIPs dominate. Operators pump mid-caps.
    "bull_trend": {
        "fii_quant":         1.0,
        "retail_momentum":   2.0,   # Retail FOMO drives marginal buying
        "dealer_hedging":    0.8,   # Vol is low, dealers are less relevant
        "dii_mf":            1.5,   # SIP flows are steady, building floor
        "macro":             0.5,   # "Macro doesn't support it" but market goes up anyway
        "sector_rotation":   1.5,   # Rotation signals work well in bull markets
        "corp_earnings":     1.5,   # Earnings justify the rally (or don't)
        "event_news":        0.5,   # No crisis = event trader is bored
        "operator":          1.5,   # Operators pump actively in bull markets
        "dabba_speculator":  1.5,   # Dabba traders are max bullish, max leveraged
    },

    # ── TRENDING BEAR (mom_20d < -5%): risk-off regime ──
    # FII selling and dealer hedging dominate. DII provides floor.
    "bear_trend": {
        "fii_quant":         2.5,   # FII selling is THE driver in bear markets
        "retail_momentum":   0.8,   # Retail sells late but capitulation is real
        "dealer_hedging":    2.0,   # Gamma hedging amplifies downside
        "dii_mf":            2.0,   # DII "buying the dip" creates temporary floors
        "macro":             1.5,   # Macro narrative justifies the selloff
        "sector_rotation":   0.5,   # Everything falls together
        "corp_earnings":     1.0,   # Earnings downgrades confirm the trend
        "event_news":        1.0,
        "operator":          0.2,   # Operators can't pump in a bear market
        "dabba_speculator":  0.5,   # Dabba gets wiped out, stops trading
    },

    # ── SIDEWAYS / LOW VOL (VIX < 14): range-bound ──
    # Fundamentals and rotation matter most. Momentum traders are lost.
    "sideways": {
        "fii_quant":         1.0,
        "retail_momentum":   0.5,   # No momentum = momentum trader is useless
        "dealer_hedging":    0.5,   # Low vol = dealers aren't driving anything
        "dii_mf":            1.5,   # Steady SIP accumulation, the quiet bid
        "macro":             1.0,
        "sector_rotation":   2.0,   # Rotation WITHIN the range is the only game
        "corp_earnings":     2.0,   # Stock-picking matters when the index is flat
        "event_news":        0.3,   # No event, no edge
        "operator":          1.0,   # Operators work best in sideways (small-cap pumps)
        "dabba_speculator":  0.5,   # Bored, reduced activity
    },

    # ── EXPIRY DAY: weekly/monthly options expiry ──
    # Dealer gamma hedging and retail F&O positioning dominate.
    "expiry": {
        "fii_quant":         0.8,
        "retail_momentum":   1.5,   # Retail option buyers create gamma
        "dealer_hedging":    3.0,   # Max pain, gamma pin — dealers own this day
        "dii_mf":            0.5,   # DIIs don't care about weekly expiry
        "macro":             0.3,   # Macro irrelevant for intraday expiry dynamics
        "sector_rotation":   0.5,
        "corp_earnings":     0.3,
        "event_news":        0.5,
        "operator":          0.5,
        "dabba_speculator":  2.0,   # Dabba traders max active on expiry (satta!)
    },

    # ── CRISIS (VIX > 30): tail risk, circuit breaker territory ──
    # Only institutional flows matter. Shadow market goes dark.
    "crisis": {
        "fii_quant":         3.0,   # FII block selling moves everything
        "retail_momentum":   0.3,   # Retail is frozen or capitulating
        "dealer_hedging":    2.5,   # Gamma cascade is mechanical
        "dii_mf":            2.0,   # DII is the only buyer in a crisis
        "macro":             2.0,   # Policy response is the key signal
        "sector_rotation":   0.2,   # Correlation = 1, no rotation possible
        "corp_earnings":     0.2,   # Fundamentals are meaningless in panic
        "event_news":        2.0,   # The crisis IS the event
        "operator":          0.0,   # Operators go dark in a crisis
        "dabba_speculator":  0.0,   # Dabba shuts down, margin calls everywhere
    },
}


def detect_regime(market_state: dict | None = None, event: dict | None = None) -> str:
    """
    Detect the current market regime from the market state.

    Args:
        market_state: The T-1 market state dict.
        event: Optional event dict (if present, regime is "event").

    Returns:
        Regime string: "event", "vix_spike", "bull_trend", "bear_trend",
                       "sideways", "expiry", "crisis", or "default".
    """
    # Event takes highest priority
    if event and event.get("headline"):
        return "event"

    if market_state is None:
        return "default"

    import json

    # Extract signals
    vix = market_state.get("india_vix")
    if vix is not None:
        vix = float(vix)

    # Factor map
    fm = market_state.get("factor_map")
    if fm and isinstance(fm, str):
        fm = json.loads(fm)
    elif fm is None:
        fm = {}

    mom_20d = fm.get("momentum_20d")

    # Regime state (expiry flags)
    rs = market_state.get("regime_state")
    if rs and isinstance(rs, str):
        rs = json.loads(rs)
    elif rs is None:
        rs = {}

    is_expiry = (
        rs.get("is_nifty_weekly_expiry", False) or
        rs.get("is_nifty_monthly_expiry", False) or
        rs.get("is_banknifty_weekly_expiry", False)
    )

    # Priority order: crisis > vix_spike > expiry > bear > bull > sideways
    if vix is not None and vix > 30:
        return "crisis"
    if vix is not None and vix > 22:
        return "vix_spike"
    if is_expiry:
        return "expiry"
    if mom_20d is not None and mom_20d < -5:
        return "bear_trend"
    if mom_20d is not None and mom_20d > 5:
        return "bull_trend"
    if vix is not None and vix < 14:
        return "sideways"

    return "default"


def get_regime_impact_weights(regime: str) -> dict[str, float]:
    """
    Get the final impact weights for a given regime.

    Multiplies the base IMPACT_WEIGHT by the regime-specific modifier.
    """
    modifiers = REGIME_IMPACT_MODIFIERS.get(regime, {})
    if not modifiers:
        return IMPACT_WEIGHT.copy()

    return {
        arch: IMPACT_WEIGHT.get(arch, 1.0) * modifiers.get(arch, 1.0)
        for arch in set(list(IMPACT_WEIGHT.keys()) + list(modifiers.keys()))
    }


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
    regime: str = "default"
    votes: list[SwarmVote] = field(default_factory=list)


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
    total_agents: int = 1_000_000,
    use_volume_weights: bool = True,
    use_impact_weights: bool = True,
    market_state: dict | None = None,
    event: dict | None = None,
) -> SwarmConsensus:
    """
    Take archetype decisions and amplify to a volume-weighted swarm.

    Args:
        archetype_decisions: {archetype_id: {direction, base_pct, conviction, ...}}
        total_agents: Total swarm size (default 1M).
        use_volume_weights: Allocate agents proportional to real market participation.
        use_impact_weights: Weight votes by price impact in consensus calculation.

    Returns:
        SwarmConsensus with population-level statistics.
    """
    # Determine agent count per archetype based on volume weights
    available_archetypes = [a for a in ARCHETYPES if a in archetype_decisions]

    if use_volume_weights:
        total_weight = sum(VOLUME_WEIGHT.get(a, 0.05) for a in available_archetypes)
        agent_counts = {
            a: max(100, int(total_agents * VOLUME_WEIGHT.get(a, 0.05) / total_weight))
            for a in available_archetypes
        }
    else:
        n_per = total_agents // len(available_archetypes)
        agent_counts = {a: n_per for a in available_archetypes}

    # Generate swarm with per-archetype counts
    all_agents: list[SwarmAgent] = []
    for arch in available_archetypes:
        arch_swarm = generate_swarm(n_per_archetype=agent_counts[arch], archetypes=[arch])
        all_agents.extend(arch_swarm)

    votes: list[SwarmVote] = []
    for agent in all_agents:
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

    # --- Aggregate with regime-conditional impact weighting ---
    n = len(votes)

    # Detect regime and get dynamic weights
    regime = detect_regime(market_state, event) if (market_state or event) else "default"
    active_weights = get_regime_impact_weights(regime) if use_impact_weights else IMPACT_WEIGHT

    if use_impact_weights:
        # Impact-weighted mean return
        weighted_sum = 0.0
        total_impact = 0.0
        for v in votes:
            w = active_weights.get(v.archetype, 1.0)
            weighted_sum += v.base_pct * w
            total_impact += w
        mean_ret = weighted_sum / total_impact if total_impact else 0

        # Impact-weighted direction vote
        buy_impact = sum(active_weights.get(v.archetype, 1.0) for v in votes if v.direction == Direction.BUY)
        sell_impact = sum(active_weights.get(v.archetype, 1.0) for v in votes if v.direction == Direction.SELL)
        hold_impact = sum(active_weights.get(v.archetype, 1.0) for v in votes if v.direction == Direction.HOLD)
    else:
        mean_ret = sum(v.base_pct for v in votes) / n if n else 0
        buy_impact = sell_impact = hold_impact = 0  # Not used

    # Raw counts (for display)
    buy_count = sum(1 for v in votes if v.direction == Direction.BUY)
    sell_count = sum(1 for v in votes if v.direction == Direction.SELL)
    hold_count = sum(1 for v in votes if v.direction == Direction.HOLD)

    returns = [v.base_pct for v in votes]
    returns_sorted = sorted(returns)
    median_ret = returns_sorted[n // 2] if n else 0
    std_ret = math.sqrt(sum((r - mean_ret) ** 2 for r in returns) / (n - 1)) if n > 1 else 0

    # Consensus direction: impact-weighted if enabled, else raw majority
    if use_impact_weights:
        if buy_impact > sell_impact and buy_impact > hold_impact:
            consensus_dir = Direction.BUY
        elif sell_impact > buy_impact and sell_impact > hold_impact:
            consensus_dir = Direction.SELL
        else:
            consensus_dir = Direction.HOLD
        total_dir_impact = buy_impact + sell_impact + hold_impact
        conviction_score = round(max(buy_impact, sell_impact, hold_impact) / total_dir_impact * 100, 1) if total_dir_impact else 0
    else:
        if buy_count > sell_count and buy_count > hold_count:
            consensus_dir = Direction.BUY
        elif sell_count > buy_count and sell_count > hold_count:
            consensus_dir = Direction.SELL
        else:
            consensus_dir = Direction.HOLD
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
        regime=regime if (market_state or event) else "default",
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
    regime_emoji = {"event": "⚡", "vix_spike": "🔥", "bull_trend": "📈", "bear_trend": "📉",
                     "sideways": "➡️", "expiry": "🎯", "crisis": "🚨", "default": "⚙️"}
    print(f"  Agents: {result.total_agents:,} | Regime: {regime_emoji.get(result.regime, '')} {result.regime}")
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

        t0 = time.time()
        result = amplify_to_swarm(responses, total_agents=100_000)
        elapsed = time.time() - t0

        print_swarm_result(result, f"{name} ({result.total_agents:,} agents, {elapsed:.1f}s)")
        print(f"  Actual: {actual:+.2f}%")
        print(f"  Swarm error: {abs(result.mean_return_pct - actual):.2f}pp\n")
