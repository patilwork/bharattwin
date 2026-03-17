"""
Agent decision schemas — Pydantic models for structured LLM output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Direction(str, Enum):
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"


class ReturnRange(BaseModel):
    low_pct: float = Field(description="Bear-case Nifty return (%)")
    base_pct: float = Field(description="Base-case Nifty return (%)")
    high_pct: float = Field(description="Bull-case Nifty return (%)")


class SectorView(BaseModel):
    direction: Direction
    reasoning: str


class AgentDecision(BaseModel):
    agent_id: str
    agent_role: str
    direction: Direction
    confidence_pct: float = Field(ge=0, le=100)
    nifty_return: ReturnRange
    sector_views: dict[str, SectorView] = Field(default_factory=dict)
    thesis: str
    key_factors: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    conviction: int = Field(ge=1, le=5)
    raw_response: str = ""


class ConsensusResult(BaseModel):
    date: date
    decisions: list[AgentDecision]
    consensus_direction: Direction
    avg_return_pct: float
    return_range: ReturnRange
    bull_count: int = 0
    bear_count: int = 0
    neutral_count: int = 0


@dataclass
class PersonaConfig:
    """Configuration for a single agent persona."""
    agent_id: str
    role: str
    description: str
    focus_areas: list[str]
    biases: list[str]
    sector_focus: list[str] = field(default_factory=list)
    risk_tolerance: str = "medium"  # low / medium / high
    time_horizon: str = "1-5 days"
    model: str = "claude-sonnet-4-6"
