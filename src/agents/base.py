"""
BaseAgent — runs a single persona against a market state snapshot.

Supports three modes:
  - "api":    Call Anthropic API, parse JSON response → AgentDecision
  - "prompt": Return formatted prompt strings for manual execution
  - "auto":   Try API, fall back to prompt if no API key
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from src.agents.formatter import format_event_context, format_market_state
from src.agents.prompts.system_base import SYSTEM_BASE
from src.agents.schemas import AgentDecision, PersonaConfig

logger = logging.getLogger(__name__)


def _has_api_key() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


class BaseAgent:
    """Single-agent wrapper: builds prompts, optionally calls Anthropic API."""

    def __init__(self, persona: PersonaConfig, system_prompt: str | None = None):
        self.persona = persona
        # Use provided system prompt or load from persona module
        if system_prompt is not None:
            self._persona_system = system_prompt
        else:
            self._persona_system = self._load_persona_system()

    def _load_persona_system(self) -> str:
        """Import the persona module's SYSTEM_PROMPT."""
        import importlib
        mod = importlib.import_module(f"src.agents.personas.{self.persona.agent_id}")
        return getattr(mod, "SYSTEM_PROMPT", "")

    def build_prompt(
        self,
        market_state: dict,
        event: dict | None = None,
    ) -> tuple[str, str]:
        """
        Build (system_message, user_message) for this agent.

        Always available regardless of API key.
        """
        system = f"{SYSTEM_BASE}\n\n## Your Persona: {self.persona.role}\n\n{self._persona_system}"

        user_parts = [
            f"# Market State Snapshot",
            f"Date: {market_state.get('session_id', 'unknown')}",
            "",
            format_market_state(market_state),
            "",
            format_event_context(event),
            "",
            "Based on this market state and your persona, provide your structured prediction.",
        ]
        user = "\n".join(user_parts)

        return system, user

    def run(
        self,
        market_state: dict,
        event: dict | None = None,
        mode: str = "auto",
    ) -> AgentDecision | str:
        """
        Run this agent against a market state.

        Returns:
          - AgentDecision if mode="api" (or "auto" with key available)
          - Formatted prompt string if mode="prompt" (or "auto" without key)

        Raises:
          - RuntimeError if mode="api" but no API key
        """
        system, user = self.build_prompt(market_state, event)

        if mode == "prompt":
            return self._format_prompt_output(system, user)

        if mode == "auto":
            if not _has_api_key():
                logger.info("agent %s: no API key, returning prompt", self.persona.agent_id)
                return self._format_prompt_output(system, user)
            mode = "api"

        if mode == "api":
            if not _has_api_key():
                raise RuntimeError(
                    f"ANTHROPIC_API_KEY not set — cannot run agent '{self.persona.agent_id}' in API mode"
                )
            return self._call_api(system, user)

        raise ValueError(f"Unknown mode: {mode}")

    def _call_api(self, system: str, user: str) -> AgentDecision:
        """Call Anthropic API and parse response into AgentDecision."""
        import anthropic

        client = anthropic.Anthropic()
        response = client.messages.create(
            model=self.persona.model,
            max_tokens=2048,
            system=system,
            messages=[{"role": "user", "content": user}],
        )

        raw_text = response.content[0].text
        return self._parse_response(raw_text)

    def _parse_response(self, raw: str) -> AgentDecision:
        """Parse LLM JSON response into AgentDecision, handling markdown fences."""
        # Strip ```json ... ``` wrappers
        cleaned = raw.strip()
        match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", cleaned, re.DOTALL)
        if match:
            cleaned = match.group(1).strip()

        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.error("agent %s: failed to parse JSON: %s\nRaw: %s", self.persona.agent_id, e, raw[:500])
            raise ValueError(f"Failed to parse agent response as JSON: {e}") from e

        # Inject agent metadata
        data["agent_id"] = self.persona.agent_id
        data["agent_role"] = self.persona.role
        data["raw_response"] = raw

        return AgentDecision(**data)

    def _format_prompt_output(self, system: str, user: str) -> str:
        """Format prompts for manual copy-paste execution."""
        divider = "=" * 72
        return (
            f"{divider}\n"
            f"AGENT: {self.persona.agent_id} ({self.persona.role})\n"
            f"{divider}\n\n"
            f"--- SYSTEM PROMPT ---\n{system}\n\n"
            f"--- USER MESSAGE ---\n{user}\n"
            f"{divider}\n"
        )
