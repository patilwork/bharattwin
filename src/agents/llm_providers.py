"""
LLM Provider abstraction — supports Claude (Anthropic), Sarvam, and OpenAI-compatible APIs.

BharatTwin can use multiple LLM backends for agent inference:
  1. Anthropic Claude (default) — claude-sonnet-4-6 for agents
  2. Sarvam 105B — India-built, 22-language, MoE (9B active params, 128K context)
  3. Any OpenAI-compatible API (Groq, Together, Ollama, etc.)

Sarvam 105B is particularly fitting for BharatTwin:
  - Built in India, trained on Indian data
  - 22 Indian languages (can process Hindi financial news natively)
  - Apache 2.0 license
  - Cloud API available at api.sarvam.ai (no local download needed)

Usage:
    # In env or .env:
    LLM_PROVIDER=sarvam           # or "anthropic" or "openai_compat"
    SARVAM_API_KEY=your_key       # from console.sarvam.ai
    SARVAM_MODEL=sarvam-105b-chat # or sarvam-30b-chat

    # Or Anthropic (default):
    LLM_PROVIDER=anthropic
    ANTHROPIC_API_KEY=your_key
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# Provider config
PROVIDERS = {
    "anthropic": {
        "env_key": "ANTHROPIC_API_KEY",
        "default_model": "claude-sonnet-4-6",
        "base_url": None,  # Uses anthropic SDK default
    },
    "sarvam": {
        "env_key": "SARVAM_API_KEY",
        "default_model": "sarvam-105b",
        "base_url": "https://api.sarvam.ai/v1",
    },
    "openai_compat": {
        "env_key": "OPENAI_API_KEY",
        "default_model": "gpt-4o",
        "base_url": None,  # Set via OPENAI_BASE_URL
    },
}


def get_provider() -> str:
    """Get the configured LLM provider."""
    return os.environ.get("LLM_PROVIDER", "anthropic").lower()


def get_api_key(provider: str | None = None) -> str:
    """Get the API key for the configured provider."""
    if provider is None:
        provider = get_provider()
    config = PROVIDERS.get(provider, {})
    key = os.environ.get(config.get("env_key", ""), "")
    return key


def has_api_key(provider: str | None = None) -> bool:
    """Check if an API key is available."""
    return bool(get_api_key(provider))


def call_llm(
    system: str,
    user: str,
    provider: str | None = None,
    model: str | None = None,
    max_tokens: int = 2048,
) -> str:
    """
    Call the configured LLM and return the response text.

    Args:
        system: System prompt.
        user: User message.
        provider: Override provider (default: from env).
        model: Override model (default: from provider config).
        max_tokens: Max response tokens.

    Returns:
        Raw response text from the LLM.
    """
    if provider is None:
        provider = get_provider()

    if not has_api_key(provider):
        raise RuntimeError(f"No API key for provider '{provider}'. "
                           f"Set {PROVIDERS[provider]['env_key']} in your environment.")

    config = PROVIDERS[provider]
    # For non-anthropic providers, always use the provider's default model
    # (persona configs specify claude models which won't work on Sarvam/OpenAI)
    if provider != "anthropic":
        model = os.environ.get("LLM_MODEL", config["default_model"])
    else:
        model = model or os.environ.get("LLM_MODEL", config["default_model"])

    if provider == "anthropic":
        return _call_anthropic(system, user, model, max_tokens)
    elif provider in ("sarvam", "openai_compat"):
        base_url = os.environ.get("OPENAI_BASE_URL", config.get("base_url"))
        api_key = get_api_key(provider)
        return _call_openai_compat(system, user, model, max_tokens, base_url, api_key)
    else:
        raise ValueError(f"Unknown provider: {provider}")


def _call_anthropic(system: str, user: str, model: str, max_tokens: int) -> str:
    """Call Anthropic Claude API."""
    import anthropic
    client = anthropic.Anthropic()
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return response.content[0].text


def _call_openai_compat(system: str, user: str, model: str, max_tokens: int,
                         base_url: str | None, api_key: str) -> str:
    """Call any OpenAI-compatible API (Sarvam, Groq, Together, Ollama, etc.)."""
    import httpx

    url = f"{base_url}/chat/completions" if base_url else "https://api.openai.com/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    is_sarvam = "sarvam" in (model or "").lower() or "sarvam" in (base_url or "").lower()

    if is_sarvam:
        # Sarvam 105B strategy: two-message conversation
        # Message 1 (user): market context
        # Message 2 (assistant): acknowledge
        # Message 3 (user): demand JSON only
        #
        # This forces the model to separate reasoning (done in msg 1 processing)
        # from output (pure JSON in msg 3 response).
        context = f"{system}\n\n---\n\n{user}" if system else user

        json_demand = (
            "Based on the market data above, provide your prediction as a single JSON object. "
            "RULES:\n"
            "1. Your ENTIRE response must be valid JSON — nothing else\n"
            "2. No markdown, no explanation, no reasoning\n"
            "3. Start with { and end with }\n"
            "4. Required fields: direction, confidence_pct, nifty_return (with low_pct/base_pct/high_pct), "
            "sector_views (3+ sectors), thesis, key_factors, risks, conviction"
        )

        payload = {
            "model": model,
            "messages": [
                {"role": "user", "content": context},
                {"role": "assistant", "content": "I have analyzed the market data. I will now provide my prediction as a JSON object."},
                {"role": "user", "content": json_demand},
            ],
            "reasoning_effort": "medium",
            "temperature": 0,
            "max_tokens": 4096,
        }
    else:
        merged_user = f"{system}\n\n---\n\n{user}" if system else user
        payload = {
            "model": model,
            "messages": [
                {"role": "user", "content": merged_user},
            ],
        }

    response = httpx.post(url, headers=headers, json=payload, timeout=180)
    response.raise_for_status()

    data = response.json()
    msg = data["choices"][0]["message"]

    # Sarvam 105B is a reasoning model:
    #   - content: final answer (should have JSON)
    #   - reasoning_content: chain-of-thought (may have JSON at the end)
    content = msg.get("content") or ""
    reasoning = msg.get("reasoning_content") or ""

    # Strategy: find whichever field contains a JSON object
    # Check content first (preferred), then reasoning
    for text in [content, reasoning]:
        text = text.strip()
        if not text:
            continue
        # If it starts with { or contains a { — likely has JSON
        brace_pos = text.find("{")
        if brace_pos >= 0:
            return text

    # Nothing has JSON — return whatever we got
    return content or reasoning or ""


# Convenience: check what's available
def available_providers() -> list[str]:
    """List providers with API keys configured."""
    return [p for p in PROVIDERS if has_api_key(p)]


def print_provider_status() -> None:
    """Print status of all providers."""
    print("\nLLM Provider Status:")
    current = get_provider()
    for name, config in PROVIDERS.items():
        has_key = has_api_key(name)
        marker = ">>>" if name == current else "   "
        status = "READY" if has_key else "NO KEY"
        key_env = config["env_key"]
        model = config["default_model"]
        print(f"  {marker} {name:<15} [{status:<6}] model={model} key_env={key_env}")


if __name__ == "__main__":
    print_provider_status()
    print(f"\n  Active provider: {get_provider()}")
    print(f"  Available: {available_providers() or 'none (prompt mode only)'}")
