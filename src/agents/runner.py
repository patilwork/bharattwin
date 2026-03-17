"""
Agent runner — orchestrates all personas against a market state snapshot.

Produces a ConsensusResult with conviction-weighted aggregation.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import create_engine, text

from src.agents.base import BaseAgent
from src.agents.personas import ALL_PERSONAS, PERSONA_BY_ID
from src.agents.schemas import (
    AgentDecision,
    ConsensusResult,
    Direction,
    ReturnRange,
)

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


def run_all(
    d: date,
    market_state: dict,
    event: dict | None = None,
    mode: str = "auto",
    agents: list[str] | None = None,
) -> ConsensusResult | list[tuple[str, str]]:
    """
    Run all (or selected) agents against the market state for date d.

    Args:
        d: The date for this run (T-1, the state date).
        market_state: Market state dict (from DB or hard-coded).
        event: Optional event context dict.
        mode: "api", "prompt", or "auto".
        agents: Optional list of agent_ids to run (default: all 8).

    Returns:
        - ConsensusResult if API mode produced AgentDecisions
        - list of (agent_id, prompt_string) if prompt mode
    """
    personas = ALL_PERSONAS
    if agents:
        personas = [PERSONA_BY_ID[aid] for aid in agents if aid in PERSONA_BY_ID]
        if not personas:
            raise ValueError(f"No valid agent IDs in {agents}. Available: {list(PERSONA_BY_ID.keys())}")

    results: list[AgentDecision] = []
    prompts: list[tuple[str, str]] = []

    for persona in personas:
        agent = BaseAgent(persona)
        logger.info("Running agent: %s (%s) mode=%s", persona.agent_id, persona.role, mode)

        try:
            result = agent.run(market_state, event, mode=mode)
        except (ValueError, RuntimeError) as e:
            logger.error("Agent %s failed: %s — skipping", persona.agent_id, e)
            continue

        if isinstance(result, AgentDecision):
            results.append(result)
        elif isinstance(result, str):
            prompts.append((persona.agent_id, result))
        else:
            logger.warning("Unexpected result type from %s: %s", persona.agent_id, type(result))

    # If we got prompt strings, return them
    if prompts and not results:
        return prompts

    # If we got decisions, aggregate
    if results:
        return _aggregate(d, results)

    raise RuntimeError("No results from any agent")


def _aggregate(d: date, decisions: list[AgentDecision]) -> ConsensusResult:
    """
    Conviction-weighted aggregation of agent decisions.

    Weights: conviction (1-5) normalized to sum to 1.
    """
    total_conviction = sum(dec.conviction for dec in decisions)
    if total_conviction == 0:
        total_conviction = len(decisions)  # equal weight fallback

    # Direction counts
    bull_count = sum(1 for d_ in decisions if d_.direction == Direction.BUY)
    bear_count = sum(1 for d_ in decisions if d_.direction == Direction.SELL)
    neutral_count = sum(1 for d_ in decisions if d_.direction == Direction.HOLD)

    # Conviction-weighted average return
    weighted_base = 0.0
    weighted_low = 0.0
    weighted_high = 0.0
    for dec in decisions:
        w = dec.conviction / total_conviction
        weighted_base += dec.nifty_return.base_pct * w
        weighted_low += dec.nifty_return.low_pct * w
        weighted_high += dec.nifty_return.high_pct * w

    avg_return_pct = round(weighted_base, 4)

    # Consensus direction: conviction-weighted with HOLD respect
    # Step 1: compute conviction mass per direction
    buy_conviction = sum(dec.conviction for dec in decisions if dec.direction == Direction.BUY)
    sell_conviction = sum(dec.conviction for dec in decisions if dec.direction == Direction.SELL)
    hold_conviction = sum(dec.conviction for dec in decisions if dec.direction == Direction.HOLD)

    # Step 2: if HOLD has the most conviction mass, respect it
    if hold_conviction >= buy_conviction and hold_conviction >= sell_conviction:
        # HOLD wins unless directional conviction is overwhelming
        directional_conviction = buy_conviction + sell_conviction
        if directional_conviction > hold_conviction * 1.5:
            # Directional agents are 1.5x more convicted than neutral — override HOLD
            consensus_dir = Direction.BUY if buy_conviction > sell_conviction else Direction.SELL
        else:
            consensus_dir = Direction.HOLD
    elif bull_count > bear_count and bull_count > neutral_count:
        consensus_dir = Direction.BUY
    elif bear_count > bull_count and bear_count > neutral_count:
        consensus_dir = Direction.SELL
    else:
        # Tiebreak: conviction-weighted direction score
        dir_score = (buy_conviction - sell_conviction) / total_conviction
        if dir_score > 0.1:
            consensus_dir = Direction.BUY
        elif dir_score < -0.1:
            consensus_dir = Direction.SELL
        else:
            consensus_dir = Direction.HOLD

    return ConsensusResult(
        date=d,
        decisions=decisions,
        consensus_direction=consensus_dir,
        avg_return_pct=avg_return_pct,
        return_range=ReturnRange(
            low_pct=round(weighted_low, 4),
            base_pct=round(weighted_base, 4),
            high_pct=round(weighted_high, 4),
        ),
        bull_count=bull_count,
        bear_count=bear_count,
        neutral_count=neutral_count,
    )


def store_results(d: date, consensus: ConsensusResult, session_id: str | None = None) -> int:
    """
    Store agent decisions and consensus into agent_decisions table.

    Returns number of rows stored.
    """
    if session_id is None:
        session_id = f"run_{d}_{uuid.uuid4().hex[:8]}"

    engine = create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))
    stored = 0

    try:
        with engine.begin() as conn:
            consensus_json = consensus.model_dump(mode="json")

            for dec in consensus.decisions:
                conn.execute(text("""
                    INSERT INTO agent_decisions (run_date, agent_id, agent_role, session_id, decision, consensus)
                    VALUES (:run_date, :agent_id, :agent_role, :session_id,
                            CAST(:decision AS jsonb), CAST(:consensus AS jsonb))
                    ON CONFLICT (run_date, agent_id, session_id) DO UPDATE SET
                        decision = EXCLUDED.decision,
                        consensus = EXCLUDED.consensus,
                        run_ts_ist = now()
                """), {
                    "run_date": d,
                    "agent_id": dec.agent_id,
                    "agent_role": dec.agent_role,
                    "session_id": session_id,
                    "decision": json.dumps(dec.model_dump(mode="json")),
                    "consensus": json.dumps(consensus_json),
                })
                stored += 1

        logger.info("agent_decisions: stored %d rows for %s session %s", stored, d, session_id)
    finally:
        engine.dispose()

    return stored
