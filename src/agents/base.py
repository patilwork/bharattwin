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
    from src.agents.llm_providers import has_api_key, get_provider
    # Check current provider first, then anthropic as fallback
    return has_api_key() or has_api_key("anthropic")


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
        """
        Call LLM API with retry on parse failure.

        Strategy:
          1. Try full prompt
          2. On parse failure, retry with condensed prompt (shorter context)
          3. On second failure, raise
        """
        from src.agents.llm_providers import call_llm, get_provider

        # Attempt 1: full prompt
        raw_text = call_llm(system=system, user=user, model=self.persona.model)
        try:
            return self._parse_response(raw_text)
        except ValueError:
            pass  # Fall through to retry

        # Attempt 2: condensed prompt — strip sector matrix and top movers,
        # add stronger JSON instruction
        logger.info("agent %s: retrying with condensed prompt", self.persona.agent_id)
        condensed_user = self._condense_prompt(user)
        raw_text = call_llm(system="", user=condensed_user, model=self.persona.model)
        return self._parse_response(raw_text)

    def _condense_prompt(self, user: str) -> str:
        """
        Create a shorter version of the prompt for retry.

        Removes verbose sections (sector matrix, full top movers list) and
        adds an aggressive JSON-only instruction.
        """
        lines = user.split("\n")
        condensed = []
        skip_section = False

        for line in lines:
            # Skip the sector sensitivity matrix (large table)
            if "## Sector Sensitivity Matrix" in line:
                skip_section = True
                condensed.append("(Sector sensitivity data omitted for brevity)")
                continue
            if skip_section and line.startswith("## "):
                skip_section = False
            if skip_section:
                continue

            # Keep top movers but limit to 5
            condensed.append(line)

        result = "\n".join(condensed)

        # Prepend persona context and append strong JSON instruction
        result = (
            f"You are {self.persona.role}. {self.persona.description}\n\n"
            f"{result}\n\n"
            "CRITICAL: Your entire response must be a single valid JSON object.\n"
            "Do NOT include any text, explanation, or reasoning outside the JSON.\n"
            "Do NOT use markdown fences.\n"
            "Start with {{ and end with }}.\n"
            "Schema: {{\"direction\":\"BUY/SELL/HOLD\",\"confidence_pct\":number,"
            "\"nifty_return\":{{\"low_pct\":number,\"base_pct\":number,\"high_pct\":number}},"
            "\"sector_views\":{{\"SECTOR\":{{\"direction\":\"BUY/SELL/HOLD\",\"reasoning\":\"...\"}}}},"
            "\"thesis\":\"...\",\"key_factors\":[\"...\"],\"risks\":[\"...\"],\"conviction\":1-5}}"
        )
        return result

    def _parse_response(self, raw: str) -> AgentDecision:
        """Parse LLM JSON response into AgentDecision, handling markdown fences and reasoning."""
        cleaned = raw.strip()

        # Strategy 1: Extract ```json ... ``` fenced block
        match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", cleaned, re.DOTALL)
        if match:
            cleaned = match.group(1).strip()
        elif cleaned[0:1] != "{":
            # Strategy 2: Find the first top-level JSON object by brace matching
            start = cleaned.find("{")
            if start >= 0:
                depth = 0
                end = start
                for i in range(start, len(cleaned)):
                    if cleaned[i] == "{":
                        depth += 1
                    elif cleaned[i] == "}":
                        depth -= 1
                        if depth == 0:
                            end = i + 1
                            break
                cleaned = cleaned[start:end].strip()

        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            # Attempt repair: if JSON is truncated, try to close it
            repaired = self._try_repair_json(cleaned)
            if repaired:
                try:
                    data = json.loads(repaired)
                    logger.info("agent %s: repaired truncated JSON", self.persona.agent_id)
                except json.JSONDecodeError:
                    logger.error("agent %s: failed to parse JSON: %s\nRaw: %s", self.persona.agent_id, e, raw[:500])
                    raise ValueError(f"Failed to parse agent response as JSON: {e}") from e
            else:
                logger.error("agent %s: failed to parse JSON: %s\nRaw: %s", self.persona.agent_id, e, raw[:500])
                raise ValueError(f"Failed to parse agent response as JSON: {e}") from e

        # Inject agent metadata
        data["agent_id"] = self.persona.agent_id
        data["agent_role"] = self.persona.role
        data["raw_response"] = raw

        return AgentDecision(**data)

    @staticmethod
    def _try_repair_json(text: str) -> str | None:
        """
        Try to repair truncated JSON by closing open braces/brackets.

        Sarvam 105B sometimes truncates output mid-JSON. If the JSON has
        all required top-level fields but is just missing closing braces,
        we can repair it.
        """
        if not text or text[0] != "{":
            return None

        # Check if required fields are present
        required = ["direction", "confidence_pct", "nifty_return", "thesis", "conviction"]
        if not all(f'"{f}"' in text for f in required):
            return None

        # Truncate at the last complete key-value pair
        # Find the last complete string value (ending with ")
        last_quote = text.rfind('"')
        if last_quote < 10:
            return None

        # Try progressively closing the JSON
        for suffix in [
            '"}]}',      # close string, array, object
            '"}}',       # close string, nested object, object
            '"]}}',      # close string, array, nested object, object
            '"}]}}',     # deeper nesting
            '"}',        # just close string and object
        ]:
            candidate = text[:last_quote + 1] + suffix
            try:
                json.loads(candidate)
                return candidate
            except json.JSONDecodeError:
                continue

        # Brute force: count open braces and close them
        opens = text.count("{") - text.count("}")
        open_brackets = text.count("[") - text.count("]")
        if opens > 0 or open_brackets > 0:
            # Truncate to last complete value
            truncated = text[:last_quote + 1]
            truncated += "]" * max(0, open_brackets)
            truncated += "}" * max(0, opens)
            try:
                json.loads(truncated)
                return truncated
            except json.JSONDecodeError:
                pass

        return None

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
