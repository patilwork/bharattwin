"""
BharatTwin Agent Framework v1.

Usage:
    from src.agents import run_all, BaseAgent, ALL_PERSONAS, PERSONA_BY_ID

    # Prompt mode (no API key needed):
    prompts = run_all(date(2022, 5, 2), state, event, mode="prompt")

    # API mode (requires ANTHROPIC_API_KEY):
    consensus = run_all(date(2022, 5, 2), state, event, mode="api")
"""

from src.agents.base import BaseAgent
from src.agents.personas import ALL_PERSONAS, PERSONA_BY_ID
from src.agents.runner import run_all, store_results
from src.agents.schemas import (
    AgentDecision,
    ConsensusResult,
    Direction,
    PersonaConfig,
    ReturnRange,
    SectorView,
)

__all__ = [
    "BaseAgent",
    "ALL_PERSONAS",
    "PERSONA_BY_ID",
    "run_all",
    "store_results",
    "AgentDecision",
    "ConsensusResult",
    "Direction",
    "PersonaConfig",
    "ReturnRange",
    "SectorView",
]
