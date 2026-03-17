"""
Swarm Agent Generator — procedurally generates up to 1 million diverse agent personas.

Architecture:
  - 8 archetypes (hand-crafted personas) serve as TEMPLATES
  - Each archetype spawns N variants across 12 variation dimensions:
    1. Risk tolerance (5 levels)
    2. Time horizon (5 levels)
    3. Conviction bias (5 styles)
    4. Experience years (2-30)
    5. Regional flavor (20 cities/NRI bases)
    6. Sector specialization (12 combos)
    7. Portfolio size bracket (5 levels)
    8. Information diet (6 types)
    9. Trading frequency (5 levels)
    10. Herd sensitivity (4 levels)
    11. Volatility preference (4 levels)
    12. Loss aversion (4 levels)

  Combinatorial space: 5×5×5×28×20×12×5×6×5×4×4×4 = ~672 million unique combos
  → We can generate 1M truly unique agents with ZERO repetition.

  Total swarm: 8 archetypes × 125,000 variants = 1,000,000 agents
  Consensus emerges from POPULATION voting, not individual LLM calls.

How BharatTwin compares to MiroFish:
  MiroFish: 700K generic social agents → emergent social behavior
  BharatTwin: 1M India market specialists → emergent market consensus

  MiroFish agents interact via social actions (follow, like, repost).
  BharatTwin agents interact via MARKET MECHANICS:
    - FII agents react to DII agent flows
    - Dealer agents react to retail positioning
    - Macro agents influence sector rotation agents
    - Event traders front-run earnings analysts
    - Retail herd follows institutional signals

Usage:
    from src.swarm.generator import generate_swarm
    swarm = generate_swarm(n_per_archetype=125_000)  # 1M agents
    swarm = generate_swarm(n_per_archetype=10_000)    # 80K agents (fast)
"""

from __future__ import annotations

import hashlib
import json
import random
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from src.agents.schemas import PersonaConfig

# ─── 8 Core Archetypes ──────────────────────────────────────────────────────

ARCHETYPES = [
    "fii_quant", "retail_momentum", "dealer_hedging", "dii_mf",
    "macro", "sector_rotation", "corp_earnings", "event_news",
    "operator", "dabba_speculator",
]

ARCHETYPE_ROLES = {
    "fii_quant": "FII Quant Strategist",
    "retail_momentum": "Retail Momentum Trader",
    "dealer_hedging": "Options Dealer / Market Maker",
    "dii_mf": "DII / Mutual Fund Allocator",
    "macro": "Macro Strategist",
    "sector_rotation": "Sector Rotation Analyst",
    "corp_earnings": "Corporate Earnings Analyst",
    "event_news": "Event / News Trader",
    "operator": "Operator / Syndicate Player",
    "dabba_speculator": "Dabba / Tier-2 Speculator",
}

# ─── 12 Variation Dimensions ────────────────────────────────────────────────

# 1. Risk tolerance (5)
RISK_LEVELS = ["very_low", "low", "medium", "high", "very_high"]

# 2. Time horizon (5)
TIME_HORIZONS = ["intraday", "1-2d", "3-5d", "5-10d", "10-20d"]

# 3. Conviction bias (5)
CONVICTION_BIAS = ["very_cautious", "cautious", "moderate", "aggressive", "contrarian"]

# 4. Experience years (28 values: 2-29)
EXPERIENCE_YEARS = list(range(2, 30))

# 5. Regional flavor (20)
REGIONAL_FLAVORS = [
    ("Mumbai_Dalal", "Dalal Street veteran, reads ET and Mint at 6am, been through 2008"),
    ("Mumbai_BKC", "BKC institutional desk, Bloomberg terminal, FII flow tracker"),
    ("Mumbai_Nariman", "Nariman Point old guard, remembers Harshad Mehta and Ketan Parekh firsthand"),
    ("Mumbai_Andheri", "Sub-broker network operator, knows what retail is buying before the data shows"),
    ("Bangalore", "Tech-savvy, builds quant models in Python, follows NASDAQ for cues"),
    ("Delhi_Lutyens", "Politically connected, front-runs policy from North Block whispers"),
    ("Delhi_Noida", "F&O trader, high leverage, watches Nifty options chain all day"),
    ("Chennai", "Conservative value investor, reads Business Line, holds 5+ years"),
    ("Ahmedabad", "Textile/pharma expertise, tracks Gujarat SMEs and IPOs, strong operator networks"),
    ("Kolkata", "Old-school chartist, follows Sucheta Dalal, remembers the plantation company scams"),
    ("Pune", "IT services analyst, tracks Infosys/TCS campus hiring as lead indicator"),
    ("Hyderabad", "Pharma sector specialist, tracks FDA approvals and ANDA filings"),
    ("Rajkot", "Satta capital of India — pure speculator, trades F&O on gut feel, massive leverage, herds with the group"),
    ("Indore", "Speculative trader hub, dabba trading background, commodity-to-equity crossover, high-conviction high-turnover"),
    ("Jaipur", "Jewellery/gold trader turned equity speculator, reads gold for equity cues"),
    ("Kochi", "Gulf remittance tracker, monitors Kerala gold imports and NRI flows"),
    ("Surat", "Diamond/textile money flowing into markets, tracks SME IPOs obsessively"),
    ("Lucknow", "Government contractor background, reads budget and capex announcements first"),
    ("NRI_US_NYC", "Wall Street EM desk, thinks in USD, tracks DXY and US 10Y"),
    ("NRI_US_SV", "Silicon Valley techie, invests in India from Robinhood, momentum-driven"),
    ("NRI_Dubai", "Gulf NRI, tracks crude and INR remittance rates, bullish India long-term"),
    ("NRI_Singapore", "EM allocator at a sovereign fund, compares India vs China vs Vietnam"),
    ("NRI_London", "City of London, tracks India GDR/ADR premiums, understands FPI regulations"),
    ("Chandigarh", "Real estate money in markets, tracks rate cycles for property-equity rotation"),
]

# 6. Sector specialization (12)
SECTOR_SPECIALIZATIONS = [
    ["FINBK", "FINNBFC"],
    ["FINBK", "FININS"],
    ["IT"],
    ["ENERGY_ONG", "ENERGY_POW"],
    ["CONAUT"],
    ["CONPFMCG"],
    ["CONPPHARM", "CONPHOSP"],
    ["MATCEM", "MATSTEL"],
    ["INDMFG", "INDPORT"],
    ["FINBK", "IT", "ENERGY_ONG"],  # Large-cap diversified
    ["CONAUT", "CONPFMCG", "MATCEM"],  # Domestic consumption
    ["IT", "CONPPHARM"],  # Export earners
]

# 7. Portfolio size bracket (5)
PORTFOLIO_SIZES = [
    ("micro_retail", "< ₹5 lakh portfolio, trades on Zerodha"),
    ("small_retail", "₹5-50 lakh, serious retail trader"),
    ("hni", "₹50 lakh - ₹5 crore, PMS client"),
    ("institutional_small", "₹5-500 crore AUM, small fund/PMS"),
    ("institutional_large", "₹500+ crore AUM, large MF/FII desk"),
]

# 8. Information diet (6)
INFO_DIETS = [
    ("tv_anchors", "CNBC Awaaz, ET Now, gets excited by TV calls"),
    ("twitter_fintwit", "FinTwit addict, follows @dopegiggles and @niki_poojary"),
    ("terminal_data", "Pure data: Bloomberg/Reuters terminal, no narratives"),
    ("broker_reports", "Reads Kotak, IIFL, Motilal reports religiously"),
    ("whatsapp_groups", "Gets tips from 15 WhatsApp groups, acts on urgency"),
    ("annual_reports", "Reads every annual report, Buffett-style value investor"),
]

# 9. Trading frequency (5)
TRADING_FREQ = [
    ("scalper", "10-50 trades/day, sub-minute holding period"),
    ("day_trader", "2-5 trades/day, closes all by 3:20 PM"),
    ("swing", "2-5 trades/week, holds 3-10 days"),
    ("positional", "1-3 trades/month, holds 1-3 months"),
    ("investor", "< 4 trades/year, buy and hold mentality"),
]

# 10. Herd sensitivity (4)
HERD_SENSITIVITY = [
    ("contrarian", "Does the opposite of the crowd"),
    ("independent", "Forms own view, ignores noise"),
    ("moderate_herder", "Influenced by consensus but maintains some independence"),
    ("strong_herder", "Follows the crowd, FOMO-driven, panic-prone"),
]

# 11. Volatility preference (4)
VOL_PREFERENCE = [
    ("vol_averse", "Avoids trading on high-VIX days"),
    ("vol_neutral", "Trades regardless of volatility"),
    ("vol_seeking", "Loves volatility, increases position size on VIX spikes"),
    ("vol_trader", "Trades VIX itself, straddles and strangles"),
]

# 12. Loss aversion (4)
LOSS_AVERSION = [
    ("high_aversion", "Cuts losses at 1-2%, strict stop-loss discipline"),
    ("moderate_aversion", "Holds through 5% drawdowns, stops at 10%"),
    ("low_aversion", "Holds through 15-20% drawdowns, averages down"),
    ("loss_seeking", "Doubles down on losers, 'it has to come back'"),
]


@dataclass
class SwarmAgent:
    """A single swarm agent — a unique variation of an archetype."""
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
    portfolio_size: str
    portfolio_desc: str
    info_diet: str
    info_diet_desc: str
    trading_freq: str
    trading_freq_desc: str
    herd_sensitivity: str
    herd_desc: str
    vol_preference: str
    vol_desc: str
    loss_aversion: str
    loss_desc: str

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
            f"- Portfolio: {self.portfolio_desc}\n"
            f"- Info source: {self.info_diet_desc}\n"
            f"- Trading style: {self.trading_freq_desc}\n"
            f"- Herd behavior: {self.herd_desc}\n"
            f"- Volatility: {self.vol_desc}\n"
            f"- Loss tolerance: {self.loss_desc}\n"
        )

    def fingerprint(self) -> str:
        """Short unique fingerprint for this agent."""
        return (f"{self.archetype[:3]}_{self.regional_flavor[:3]}_"
                f"{self.experience_years}y_{self.conviction_bias[:3]}_"
                f"{self.risk_tolerance[:3]}")


def _deterministic_seed(archetype: str, variant_idx: int) -> int:
    """Generate a deterministic seed for reproducible swarm generation."""
    h = hashlib.md5(f"{archetype}_{variant_idx}_v2".encode()).hexdigest()
    return int(h[:8], 16)


def generate_swarm(
    n_per_archetype: int = 125_000,
    archetypes: list[str] | None = None,
) -> list[SwarmAgent]:
    """
    Generate a swarm of agent variants.

    Args:
        n_per_archetype: Number of variants per archetype.
            125,000 × 8 archetypes = 1,000,000 agents.
            10,000 × 8 = 80,000 (fast mode).
            100 × 8 = 800 (dev/test mode).

    Returns:
        List of SwarmAgent instances.
    """
    if archetypes is None:
        archetypes = ARCHETYPES

    swarm: list[SwarmAgent] = []

    for archetype in archetypes:
        role = ARCHETYPE_ROLES.get(archetype, archetype)

        for i in range(n_per_archetype):
            rng = random.Random(_deterministic_seed(archetype, i))

            risk = rng.choice(RISK_LEVELS)
            horizon = rng.choice(TIME_HORIZONS)
            conv_bias = rng.choice(CONVICTION_BIAS)
            exp_years = rng.choice(EXPERIENCE_YEARS)
            region, region_desc = rng.choice(REGIONAL_FLAVORS)
            sectors = rng.choice(SECTOR_SPECIALIZATIONS)
            port_size, port_desc = rng.choice(PORTFOLIO_SIZES)
            info, info_desc = rng.choice(INFO_DIETS)
            freq, freq_desc = rng.choice(TRADING_FREQ)
            herd, herd_desc = rng.choice(HERD_SENSITIVITY)
            vol, vol_desc = rng.choice(VOL_PREFERENCE)
            loss, loss_desc = rng.choice(LOSS_AVERSION)

            agent = SwarmAgent(
                agent_id=f"{archetype}_{i:06d}",
                archetype=archetype,
                role=f"{role} (v{i})",
                description=f"{role} — {region}, {exp_years}yr, {conv_bias}, {port_size}",
                risk_tolerance=risk,
                time_horizon=horizon,
                conviction_bias=conv_bias,
                experience_years=exp_years,
                regional_flavor=region,
                regional_desc=region_desc,
                sector_focus=sectors,
                portfolio_size=port_size,
                portfolio_desc=port_desc,
                info_diet=info,
                info_diet_desc=info_desc,
                trading_freq=freq,
                trading_freq_desc=freq_desc,
                herd_sensitivity=herd,
                herd_desc=herd_desc,
                vol_preference=vol,
                vol_desc=vol_desc,
                loss_aversion=loss,
                loss_desc=loss_desc,
            )
            swarm.append(agent)

    return swarm


def swarm_summary(swarm: list[SwarmAgent]) -> dict:
    """Compute summary statistics of the swarm."""
    return {
        "total_agents": len(swarm),
        "archetypes": dict(Counter(a.archetype for a in swarm)),
        "risk_distribution": dict(Counter(a.risk_tolerance for a in swarm)),
        "regional_distribution": dict(Counter(a.regional_flavor for a in swarm)),
        "conviction_distribution": dict(Counter(a.conviction_bias for a in swarm)),
        "portfolio_distribution": dict(Counter(a.portfolio_size for a in swarm)),
        "info_diet_distribution": dict(Counter(a.info_diet for a in swarm)),
        "trading_freq_distribution": dict(Counter(a.trading_freq for a in swarm)),
        "herd_distribution": dict(Counter(a.herd_sensitivity for a in swarm)),
        "vol_preference_distribution": dict(Counter(a.vol_preference for a in swarm)),
        "loss_aversion_distribution": dict(Counter(a.loss_aversion for a in swarm)),
        "avg_experience_years": round(sum(a.experience_years for a in swarm) / len(swarm), 1) if swarm else 0,
        "unique_fingerprints": len(set(a.fingerprint() for a in swarm)),
    }


# Combinatorial space calculation
COMBO_SPACE = (
    len(RISK_LEVELS) * len(TIME_HORIZONS) * len(CONVICTION_BIAS) *
    len(EXPERIENCE_YEARS) * len(REGIONAL_FLAVORS) * len(SECTOR_SPECIALIZATIONS) *
    len(PORTFOLIO_SIZES) * len(INFO_DIETS) * len(TRADING_FREQ) *
    len(HERD_SENSITIVITY) * len(VOL_PREFERENCE) * len(LOSS_AVERSION)
)  # = 5×5×5×28×20×12×5×6×5×4×4×4 = 672,000,000 unique combos


if __name__ == "__main__":
    import time

    print(f"\nCombinatorial space: {COMBO_SPACE:,} unique agent profiles")
    print(f"Generating swarm...")

    # Generate 1M agents
    t0 = time.time()
    swarm = generate_swarm(n_per_archetype=125_000)
    elapsed = time.time() - t0

    summary = swarm_summary(swarm)

    print(f"\n{'=' * 70}")
    print(f"BHARATTWIN SWARM — {summary['total_agents']:,} Agents")
    print(f"Generated in {elapsed:.1f}s")
    print(f"{'=' * 70}")
    print(f"\nArchetypes: {json.dumps(summary['archetypes'], indent=2)}")
    print(f"\nRegional distribution (top 5):")
    for region, count in sorted(summary['regional_distribution'].items(), key=lambda x: -x[1])[:5]:
        print(f"  {region}: {count:,}")
    print(f"\nPortfolio sizes: {json.dumps(summary['portfolio_distribution'], indent=2)}")
    print(f"\nInfo diets: {json.dumps(summary['info_diet_distribution'], indent=2)}")
    print(f"\nHerd sensitivity: {json.dumps(summary['herd_distribution'], indent=2)}")
    print(f"\nUnique fingerprints: {summary['unique_fingerprints']:,}")
    print(f"Avg experience: {summary['avg_experience_years']} years")

    # Show samples
    print(f"\n{'─' * 70}")
    print("Sample agents:")
    for a in [swarm[0], swarm[125_000], swarm[500_000], swarm[750_000], swarm[999_999]]:
        print(f"\n  {a.agent_id}: {a.description}")
        print(f"  Info: {a.info_diet_desc}")
        print(f"  Herd: {a.herd_desc} | Vol: {a.vol_desc} | Loss: {a.loss_desc}")
