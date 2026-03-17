"""
Tests for agent framework.

Covers:
  1. Persona registry (10 archetypes)
  2. Formatter output
  3. Prompt mode (10 prompt strings returned)
  4. Schema validation
  5. Response parsing
  6. Aggregation (incl HOLD-majority)
  7. Replay cases (RBI hike, election, exit poll)
"""

from __future__ import annotations

import json
from datetime import date

import pytest

from src.agents.schemas import (
    AgentDecision,
    ConsensusResult,
    Direction,
    PersonaConfig,
    ReturnRange,
    SectorView,
)
from src.agents.personas import ALL_PERSONAS, PERSONA_BY_ID
from src.agents.formatter import format_market_state, format_event_context, load_sector_sensitivities
from src.agents.base import BaseAgent
from src.agents.runner import run_all, _aggregate

# Re-use the replay case's hard-coded state
from src.replay.cases.rbi_hike_may2022 import MARKET_STATE, EVENT


# --- 1. Persona Registry ---

def test_persona_registry_count():
    """All 10 personas registered."""
    assert len(ALL_PERSONAS) == 10
    assert len(PERSONA_BY_ID) == 10


def test_persona_ids():
    """Expected agent IDs present."""
    expected = {
        "fii_quant", "retail_momentum", "dealer_hedging", "dii_mf",
        "macro", "sector_rotation", "corp_earnings", "event_news",
        "operator", "dabba_speculator",
    }
    assert set(PERSONA_BY_ID.keys()) == expected


def test_persona_config_fields():
    """Each persona has required fields populated."""
    for p in ALL_PERSONAS:
        assert isinstance(p, PersonaConfig)
        assert p.agent_id
        assert p.role
        assert p.description
        assert len(p.focus_areas) >= 3
        assert len(p.biases) >= 2
        assert p.model == "claude-sonnet-4-6"


# --- 2. Formatter ---

def test_format_market_state_readable():
    """Formatter produces readable text with all sections."""
    output = format_market_state(MARKET_STATE)
    assert "## Index Levels" in output
    assert "17069.1" in output
    assert "## Macro" in output
    assert "76.39" in output
    assert "## Institutional Flows" in output
    assert "## Computed Factors" in output
    assert "momentum_1d" in output
    assert "## Regime" in output
    assert "## Top Movers" in output
    assert "## Sector Sensitivity Matrix" in output


def test_format_market_state_na_handling():
    """Missing fields show N/A, not empty."""
    sparse = {"session_id": "CM_2022-01-01"}
    output = format_market_state(sparse)
    assert "N/A" in output


def test_format_event_context():
    """Event formatting includes headline."""
    output = format_event_context(EVENT)
    assert "RBI surprises" in output
    assert "40bps" in output


def test_format_event_none():
    """None event returns 'No specific event'."""
    output = format_event_context(None)
    assert "No specific event" in output


def test_load_sector_sensitivities():
    """Sector mapping loads 19+ sectors with expected fields."""
    sectors = load_sector_sensitivities()
    assert len(sectors) >= 19
    assert "FINBK" in sectors
    assert sectors["FINBK"]["rate_sensitive"] == "high"
    assert sectors["IT"]["fx_sensitive"] == "high"


# --- 3. Prompt Mode ---

def test_prompt_mode_returns_10_prompts():
    """run_all in prompt mode returns 10 (agent_id, prompt_string) tuples."""
    result = run_all(
        d=date(2022, 5, 2),
        market_state=MARKET_STATE,
        event=EVENT,
        mode="prompt",
    )
    assert isinstance(result, list)
    assert len(result) == 10
    for agent_id, prompt_str in result:
        assert agent_id in PERSONA_BY_ID
        assert isinstance(prompt_str, str)
        assert "SYSTEM PROMPT" in prompt_str
        assert "USER MESSAGE" in prompt_str
        assert "17069.1" in prompt_str  # market state included


def test_prompt_mode_single_agent():
    """run_all with agents filter returns only selected agent."""
    result = run_all(
        d=date(2022, 5, 2),
        market_state=MARKET_STATE,
        event=EVENT,
        mode="prompt",
        agents=["dealer_hedging"],
    )
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0][0] == "dealer_hedging"


def test_build_prompt_structure():
    """BaseAgent.build_prompt returns (system, user) with correct content."""
    persona = PERSONA_BY_ID["fii_quant"]
    agent = BaseAgent(persona)
    system, user = agent.build_prompt(MARKET_STATE, EVENT)

    assert "FII Quant Strategist" in system
    assert "Indian equity market" in system
    assert "JSON" in system
    assert "17069.1" in user
    assert "RBI surprises" in user


# --- 4. Schema Validation ---

def test_agent_decision_valid():
    """AgentDecision accepts valid data."""
    dec = AgentDecision(
        agent_id="test",
        agent_role="Test Agent",
        direction=Direction.SELL,
        confidence_pct=85.0,
        nifty_return=ReturnRange(low_pct=-3.5, base_pct=-2.0, high_pct=-0.5),
        thesis="Test thesis",
        key_factors=["factor1"],
        risks=["risk1"],
        conviction=4,
    )
    assert dec.direction == Direction.SELL
    assert dec.conviction == 4


def test_agent_decision_bounds():
    """AgentDecision rejects out-of-bound values."""
    with pytest.raises(Exception):
        AgentDecision(
            agent_id="test", agent_role="Test", direction=Direction.BUY,
            confidence_pct=150.0,  # > 100
            nifty_return=ReturnRange(low_pct=0, base_pct=1, high_pct=2),
            thesis="t", conviction=3,
        )

    with pytest.raises(Exception):
        AgentDecision(
            agent_id="test", agent_role="Test", direction=Direction.BUY,
            confidence_pct=50,
            nifty_return=ReturnRange(low_pct=0, base_pct=1, high_pct=2),
            thesis="t", conviction=6,  # > 5
        )


# --- 5. Response Parsing ---

def test_parse_response_clean_json():
    """Parses clean JSON response."""
    persona = PERSONA_BY_ID["macro"]
    agent = BaseAgent(persona)
    raw = json.dumps({
        "direction": "SELL",
        "confidence_pct": 80,
        "nifty_return": {"low_pct": -3.0, "base_pct": -1.8, "high_pct": -0.5},
        "sector_views": {"FINBK": {"direction": "SELL", "reasoning": "Rate hike"}},
        "thesis": "RBI hike negative for sentiment",
        "key_factors": ["repo hike", "off-cycle surprise"],
        "risks": ["market already priced in"],
        "conviction": 4,
    })
    dec = agent._parse_response(raw)
    assert dec.agent_id == "macro"
    assert dec.direction == Direction.SELL
    assert dec.conviction == 4


def test_parse_response_with_fences():
    """Parses JSON wrapped in ```json ... ``` fences."""
    persona = PERSONA_BY_ID["fii_quant"]
    agent = BaseAgent(persona)
    raw = '```json\n{"direction":"BUY","confidence_pct":60,"nifty_return":{"low_pct":-0.5,"base_pct":0.5,"high_pct":1.5},"thesis":"test","key_factors":[],"risks":[],"conviction":2}\n```'
    dec = agent._parse_response(raw)
    assert dec.direction == Direction.BUY
    assert dec.agent_id == "fii_quant"


# --- 6. Aggregation ---

def test_aggregate_consensus():
    """Conviction-weighted aggregation produces correct consensus."""
    decisions = [
        AgentDecision(
            agent_id="a", agent_role="A", direction=Direction.SELL,
            confidence_pct=90, conviction=5,
            nifty_return=ReturnRange(low_pct=-4.0, base_pct=-2.5, high_pct=-1.0),
            thesis="bearish", key_factors=["x"], risks=["y"],
        ),
        AgentDecision(
            agent_id="b", agent_role="B", direction=Direction.SELL,
            confidence_pct=70, conviction=3,
            nifty_return=ReturnRange(low_pct=-3.0, base_pct=-1.5, high_pct=0.0),
            thesis="bearish", key_factors=["x"], risks=["y"],
        ),
        AgentDecision(
            agent_id="c", agent_role="C", direction=Direction.BUY,
            confidence_pct=40, conviction=2,
            nifty_return=ReturnRange(low_pct=-0.5, base_pct=0.5, high_pct=1.5),
            thesis="contrarian", key_factors=["x"], risks=["y"],
        ),
    ]
    consensus = _aggregate(date(2022, 5, 2), decisions)
    assert consensus.consensus_direction == Direction.SELL
    assert consensus.bear_count == 2
    assert consensus.bull_count == 1
    assert consensus.avg_return_pct < 0  # weighted toward bearish


def test_aggregate_hold_majority():
    """When HOLD conviction dominates, consensus should be HOLD, not forced directional."""
    decisions = [
        AgentDecision(
            agent_id="a", agent_role="A", direction=Direction.SELL,
            confidence_pct=55, conviction=2,
            nifty_return=ReturnRange(low_pct=-2.0, base_pct=-0.5, high_pct=0.8),
            thesis="mildly bearish", key_factors=["x"], risks=["y"],
        ),
        AgentDecision(
            agent_id="b", agent_role="B", direction=Direction.SELL,
            confidence_pct=62, conviction=3,
            nifty_return=ReturnRange(low_pct=-2.5, base_pct=-0.8, high_pct=0.5),
            thesis="bearish", key_factors=["x"], risks=["y"],
        ),
        AgentDecision(
            agent_id="c", agent_role="C", direction=Direction.BUY,
            confidence_pct=60, conviction=3,
            nifty_return=ReturnRange(low_pct=-0.5, base_pct=0.5, high_pct=1.5),
            thesis="contrarian", key_factors=["x"], risks=["y"],
        ),
        AgentDecision(
            agent_id="d", agent_role="D", direction=Direction.HOLD,
            confidence_pct=50, conviction=2,
            nifty_return=ReturnRange(low_pct=-1.5, base_pct=0.2, high_pct=1.5),
            thesis="neutral", key_factors=["x"], risks=["y"],
        ),
        AgentDecision(
            agent_id="e", agent_role="E", direction=Direction.HOLD,
            confidence_pct=45, conviction=1,
            nifty_return=ReturnRange(low_pct=-1.5, base_pct=0.0, high_pct=1.2),
            thesis="no data", key_factors=["x"], risks=["y"],
        ),
        AgentDecision(
            agent_id="f", agent_role="F", direction=Direction.HOLD,
            confidence_pct=55, conviction=2,
            nifty_return=ReturnRange(low_pct=-1.0, base_pct=0.3, high_pct=1.2),
            thesis="neutral", key_factors=["x"], risks=["y"],
        ),
        AgentDecision(
            agent_id="g", agent_role="G", direction=Direction.HOLD,
            confidence_pct=48, conviction=2,
            nifty_return=ReturnRange(low_pct=-1.0, base_pct=0.2, high_pct=1.0),
            thesis="neutral", key_factors=["x"], risks=["y"],
        ),
        AgentDecision(
            agent_id="h", agent_role="H", direction=Direction.HOLD,
            confidence_pct=30, conviction=1,
            nifty_return=ReturnRange(low_pct=-1.0, base_pct=0.0, high_pct=1.0),
            thesis="no event", key_factors=["x"], risks=["y"],
        ),
    ]
    consensus = _aggregate(date(2026, 3, 16), decisions)
    # 5 HOLD / 2 SELL / 1 BUY — HOLD should win, not SELL
    assert consensus.consensus_direction == Direction.HOLD
    assert consensus.neutral_count == 5
    assert consensus.bear_count == 2
    assert consensus.bull_count == 1


# --- 7. Replay Case ---

def test_replay_prompt_mode():
    """Replay case runs in prompt mode without errors."""
    from src.replay.cases.rbi_hike_may2022 import run_replay
    result = run_replay(mode="prompt")
    assert isinstance(result, list)
    assert len(result) == 10


# --- 8. In-Context Replay ---

def test_incontext_replay_consensus():
    """In-context replay produces correct consensus direction and reasonable error."""
    from src.replay.run_incontext_007 import run_incontext_replay
    from src.replay.cases.rbi_hike_may2022 import ACTUAL_NIFTY_RETURN_PCT

    consensus = run_incontext_replay()

    # Direction must be SELL (actual was -2.29%)
    assert consensus.consensus_direction == Direction.SELL
    # All 8 agents should be bearish for this event
    assert consensus.bear_count == 8
    assert consensus.bull_count == 0
    # Prediction error should be under 1 percentage point
    error = abs(consensus.avg_return_pct - ACTUAL_NIFTY_RETURN_PCT)
    assert error < 1.0, f"Prediction error {error:.2f}pp exceeds 1pp threshold"
    # Actual should fall within the predicted range
    assert consensus.return_range.low_pct <= ACTUAL_NIFTY_RETURN_PCT <= consensus.return_range.high_pct


def test_election_replay_consensus():
    """Election 2024 replay: direction correct and error under 2pp."""
    from src.replay.run_election_010 import run_election_replay
    from src.replay.cases.election_june2024 import ACTUAL_NIFTY_RETURN_PCT

    consensus = run_election_replay()

    assert consensus.consensus_direction == Direction.SELL
    assert consensus.bear_count == 8
    error = abs(consensus.avg_return_pct - ACTUAL_NIFTY_RETURN_PCT)
    assert error < 2.0, f"Prediction error {error:.2f}pp exceeds 2pp threshold"
    assert consensus.return_range.low_pct <= ACTUAL_NIFTY_RETURN_PCT <= consensus.return_range.high_pct


def test_exitpoll_replay_consensus():
    """Exit poll 2024 replay (BUY case): direction correct and error under 1pp."""
    from src.replay.run_exitpoll_011 import run_exitpoll_replay
    from src.replay.cases.exit_poll_june2024 import ACTUAL_NIFTY_RETURN_PCT

    consensus = run_exitpoll_replay()

    assert consensus.consensus_direction == Direction.BUY
    assert consensus.bull_count == 8
    error = abs(consensus.avg_return_pct - ACTUAL_NIFTY_RETURN_PCT)
    assert error < 1.0, f"Prediction error {error:.2f}pp exceeds 1pp threshold"
    assert consensus.return_range.low_pct <= ACTUAL_NIFTY_RETURN_PCT <= consensus.return_range.high_pct
