"""
Swarm Agent Generator — procedurally generates hundreds of diverse agent personas.

Architecture:
  - 8 archetypes (the hand-crafted personas) serve as TEMPLATES
  - Each archetype spawns N variants with randomized:
    - Conviction tendencies (cautious ↔ aggressive)
    - Sector biases (weighted by experience)
    - Risk tolerance (low ↔ high)
    - Time horizon (1-day scalper ↔ 20-day swing)
    - Regional bias (Mumbai-focused, Chennai, Delhi, NRI)
    - Experience level (2yr retail ↔ 25yr institutional)
  - Total swarm: 8 archetypes × 50-150 variants = 400-1,200 agents
  - Consensus emerges from POPULATION voting, not individual calls

This is how BharatTwin draws a parallel to MiroFish:
  MiroFish: 700K generic social agents → emergent behavior
  BharatTwin: 1,000+ India market specialists → emergent consensus

The key insight: in MiroFish, agents interact socially (follow, like, repost).
In BharatTwin, agents interact via MARKET MECHANICS:
  - FII agents react to DII agent flows
  - Dealer agents react to retail order flow
  - Macro agents influence sector rotation agents
  - Event traders front-run earnings analysts

Usage:
    from src.swarm.generator import generate_swarm
    swarm = generate_swarm(n_per_archetype=100)  # 800 agents
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from typing import Any

from src.agents.schemas import PersonaConfig

# Archetype definitions — the 8 core personas
ARCHETYPES = [
    "fii_quant", "retail_momentum", "dealer_hedging", "dii_mf",
    "macro", "sector_rotation", "corp_earnings", "event_news",
]

# Variation dimensions
RISK_LEVELS = ["very_low", "low", "medium", "high", "very_high"]
TIME_HORIZONS = ["1d", "1-3d", "3-5d", "5-10d", "10-20d"]
EXPERIENCE_YEARS = list(range(2, 30))
CONVICTION_BIAS = ["cautious", "moderate", "aggressive", "contrarian"]

REGIONAL_FLAVORS = [
    ("Mumbai", "Dalal Street desk, reads ET and Mint"),
    ("Bangalore", "Tech-savvy, builds quant models, follows global tech earnings"),
    ("Delhi", "Politically connected, front-runs policy changes"),
    ("Chennai", "Conservative, value-oriented, reads The Hindu Business Line"),
    ("Ahmedabad", "Textile/pharma expertise, tracks SME IPOs"),
    ("Kolkata", "Old-school chartist, follows Sucheta Dalal"),
    ("NRI_US", "Tracks US pre-market for India cues, thinks in USD"),
    ("NRI_Dubai", "Tracks crude and Gulf remittances, bullish on India growth"),
    ("NRI_Singapore", "EM allocator, compares India vs China vs Vietnam"),
    ("Pune", "IT services background, tracks Infosys/TCS closely"),
]

SECTOR_SPECIALIZATIONS = [
    ["FINBK", "FINNBFC"],
    ["IT"],
    ["ENERGY_ONG", "ENERGY_POW"],
    ["CONAUT", "CONPFMCG"],
    ["CONPPHARM"],
    ["MATCEM", "MATSTEL"],
    ["INDMFG"],
    ["FINBK", "IT", "ENERGY_ONG"],  # Diversified
]


@dataclass
class SwarmAgent:
    """A single swarm agent — a variation of an archetype."""
    agent_id: str
    archetype: str
    role: str
    description: str
    risk_tolerance: str
    time_horizon: str
    conviction_bias: str
    experience_years: int
    regional_flavor: str
    regional_desc: str
    sector_focus: list[str]
    persona_config: PersonaConfig | None = None

    def to_system_prompt_modifier(self) -> str:
        """Generate the variation-specific system prompt addition."""
        return (
            f"\n\nYour specific profile:\n"
            f"- Experience: {self.experience_years} years in Indian markets\n"
            f"- Base: {self.regional_flavor} ({self.regional_desc})\n"
            f"- Risk tolerance: {self.risk_tolerance}\n"
            f"- Time horizon: {self.time_horizon}\n"
            f"- Conviction style: {self.conviction_bias}\n"
            f"- Sector focus: {', '.join(self.sector_focus)}\n"
        )


def _deterministic_seed(archetype: str, variant_idx: int) -> int:
    """Generate a deterministic seed for reproducible swarm generation."""
    h = hashlib.md5(f"{archetype}_{variant_idx}".encode()).hexdigest()
    return int(h[:8], 16)


def generate_swarm(
    n_per_archetype: int = 100,
    archetypes: list[str] | None = None,
    seed: int = 42,
) -> list[SwarmAgent]:
    """
    Generate a swarm of agent variants.

    Args:
        n_per_archetype: Number of variants per archetype (default 100).
        archetypes: Which archetypes to use (default: all 8).
        seed: Random seed for reproducibility.

    Returns:
        List of SwarmAgent instances.
    """
    if archetypes is None:
        archetypes = ARCHETYPES

    # Archetype metadata
    archetype_roles = {
        "fii_quant": "FII Quant Strategist",
        "retail_momentum": "Retail Momentum Trader",
        "dealer_hedging": "Options Dealer / Market Maker",
        "dii_mf": "DII / Mutual Fund Allocator",
        "macro": "Macro Strategist",
        "sector_rotation": "Sector Rotation Analyst",
        "corp_earnings": "Corporate Earnings Analyst",
        "event_news": "Event / News Trader",
    }

    swarm: list[SwarmAgent] = []

    for archetype in archetypes:
        for i in range(n_per_archetype):
            rng = random.Random(_deterministic_seed(archetype, i))

            risk = rng.choice(RISK_LEVELS)
            horizon = rng.choice(TIME_HORIZONS)
            conv_bias = rng.choice(CONVICTION_BIAS)
            exp_years = rng.choice(EXPERIENCE_YEARS)
            region, region_desc = rng.choice(REGIONAL_FLAVORS)
            sectors = rng.choice(SECTOR_SPECIALIZATIONS)

            agent_id = f"{archetype}_{i:04d}"
            role = archetype_roles.get(archetype, archetype)

            agent = SwarmAgent(
                agent_id=agent_id,
                archetype=archetype,
                role=f"{role} (variant {i})",
                description=f"{role} — {region}, {exp_years}yr exp, {conv_bias} style",
                risk_tolerance=risk,
                time_horizon=horizon,
                conviction_bias=conv_bias,
                experience_years=exp_years,
                regional_flavor=region,
                regional_desc=region_desc,
                sector_focus=sectors,
            )
            swarm.append(agent)

    return swarm


def swarm_summary(swarm: list[SwarmAgent]) -> dict:
    """Compute summary statistics of the swarm."""
    from collections import Counter

    archetype_counts = Counter(a.archetype for a in swarm)
    risk_counts = Counter(a.risk_tolerance for a in swarm)
    region_counts = Counter(a.regional_flavor for a in swarm)
    conv_counts = Counter(a.conviction_bias for a in swarm)
    avg_exp = sum(a.experience_years for a in swarm) / len(swarm) if swarm else 0

    return {
        "total_agents": len(swarm),
        "archetypes": dict(archetype_counts),
        "risk_distribution": dict(risk_counts),
        "regional_distribution": dict(region_counts),
        "conviction_distribution": dict(conv_counts),
        "avg_experience_years": round(avg_exp, 1),
    }


if __name__ == "__main__":
    # Demo: generate a 800-agent swarm
    swarm = generate_swarm(n_per_archetype=100)
    summary = swarm_summary(swarm)

    print(f"\n{'=' * 60}")
    print(f"BHARATTWIN SWARM — {summary['total_agents']} Agents")
    print(f"{'=' * 60}")
    print(f"\nArchetypes: {json.dumps(summary['archetypes'], indent=2)}")
    print(f"\nRisk distribution: {json.dumps(summary['risk_distribution'], indent=2)}")
    print(f"\nRegional distribution: {json.dumps(summary['regional_distribution'], indent=2)}")
    print(f"\nConviction styles: {json.dumps(summary['conviction_distribution'], indent=2)}")
    print(f"\nAvg experience: {summary['avg_experience_years']} years")

    # Show a few examples
    print(f"\n{'─' * 60}")
    print("Sample agents:")
    for a in swarm[:5]:
        print(f"  {a.agent_id}: {a.description}")
        print(f"    {a.to_system_prompt_modifier().strip()}")
        print()
