"""
Market state formatter — converts raw state dict into LLM-readable text.

All agents receive identical formatted context; differentiation comes from persona/system prompts.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_SEEDS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "seeds"


@lru_cache(maxsize=1)
def load_sector_sensitivities() -> dict[str, dict]:
    """Parse sector_mapping.csv and return {sector_code: {name, rate, fx, oil, notes}}."""
    path = _SEEDS_DIR / "sector_mapping.csv"
    if not path.exists():
        logger.warning("sector_mapping.csv not found at %s", path)
        return {}

    result = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            result[row["sector_code"]] = {
                "name": row["sector_name"],
                "rate_sensitive": row["is_rate_sensitive"],
                "fx_sensitive": row["is_fx_sensitive"],
                "oil_sensitive": row["is_oil_sensitive"],
                "notes": row.get("notes", ""),
            }
    return result


def _safe_json(val: Any) -> dict | list:
    """Parse JSONB field — handles str, dict, and None."""
    if val is None:
        return {}
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return {}


def format_market_state(state: dict) -> str:
    """
    Format a market_state row (or hard-coded dict) into readable sections.

    Expected keys: nifty50_close, banknifty_close, india_vix, repo_rate_pct,
    usdinr_ref, crude_indian_basket_usd, returns_map, flow_map, macro_map,
    factor_map, regime_state, data_quality.
    """
    lines: list[str] = []

    # --- Index Levels ---
    lines.append("## Index Levels (T-1 Close)")
    lines.append(f"- Nifty 50:   {state.get('nifty50_close') or 'N/A'}")
    lines.append(f"- BankNifty:  {state.get('banknifty_close') or 'N/A'}")
    lines.append(f"- India VIX:  {state.get('india_vix') or 'N/A'}")
    lines.append("")

    # --- Macro ---
    lines.append("## Macro")
    lines.append(f"- Repo Rate:           {state.get('repo_rate_pct') or 'N/A'}%")
    lines.append(f"- USDINR (RBI ref):    {state.get('usdinr_ref') or 'N/A'}")
    lines.append(f"- Crude (Indian basket): ${state.get('crude_indian_basket_usd') or 'N/A'}")
    macro_map = _safe_json(state.get("macro_map"))
    if isinstance(macro_map, dict):
        fx = macro_map.get("fx", {})
        for ccy in ["eurinr", "gbpinr", "jpyinr"]:
            val = fx.get(ccy)
            if val:
                lines.append(f"- {ccy.upper()}:  {val}")
    lines.append("")

    # --- Flows ---
    lines.append("## Institutional Flows (₹ crore)")
    flow_map = _safe_json(state.get("flow_map"))
    if flow_map:
        for key in sorted(flow_map.keys()):
            f = flow_map[key]
            if isinstance(f, dict):
                net = f.get("net_cr", "N/A")
                buy = f.get("buy_cr", "N/A")
                sell = f.get("sell_cr", "N/A")
                label = key.upper().replace("_", " ")
                lines.append(f"- {label}: Buy {buy} | Sell {sell} | Net {net}")
    else:
        lines.append("- N/A")
    lines.append("")

    # --- Factors ---
    lines.append("## Computed Factors")
    factor_map = _safe_json(state.get("factor_map"))
    if factor_map:
        for k in ["momentum_1d", "momentum_5d", "momentum_20d", "volatility_20d",
                   "breadth_adv", "breadth_dec", "breadth_ratio", "breadth_pct_up",
                   "avg_turnover_cr", "eq_stock_count"]:
            val = factor_map.get(k)
            lines.append(f"- {k}: {val if val is not None else 'N/A'}")
    else:
        lines.append("- N/A (no factor data)")
    lines.append("")

    # --- Regime ---
    lines.append("## Regime / Calendar Flags")
    regime = _safe_json(state.get("regime_state"))
    if regime:
        for k, v in regime.items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- N/A")
    lines.append("")

    # --- Top Movers (from returns_map) ---
    lines.append("## Top Movers (1-day returns, %)")
    returns_map = _safe_json(state.get("returns_map"))
    if returns_map and isinstance(returns_map, dict):
        sorted_returns = sorted(returns_map.items(), key=lambda x: abs(float(x[1])), reverse=True)
        top = sorted_returns[:10]
        for sym, ret in top:
            lines.append(f"- {sym}: {ret:+.2f}%" if isinstance(ret, (int, float)) else f"- {sym}: {ret}")
    else:
        lines.append("- N/A")
    lines.append("")

    # --- Morningstar Fundamentals (if available in macro_map) ---
    macro_map_parsed = _safe_json(state.get("macro_map"))
    ms_data = macro_map_parsed.get("morningstar", {}) if isinstance(macro_map_parsed, dict) else {}
    if ms_data and ms_data.get("stocks"):
        from src.ingestion.morningstar.fundamentals import format_for_agents
        lines.append(format_for_agents(ms_data["stocks"]))
        lines.append("")

    # --- Sector Sensitivities ---
    sectors = load_sector_sensitivities()
    if sectors:
        lines.append("## Sector Sensitivity Matrix")
        lines.append("| Sector | Rate | FX | Oil | Notes |")
        lines.append("|--------|------|----|-----|-------|")
        for code, s in sectors.items():
            lines.append(f"| {s['name']} | {s['rate_sensitive']} | {s['fx_sensitive']} | {s['oil_sensitive']} | {s['notes']} |")
        lines.append("")

    return "\n".join(lines)


def format_event_context(event: dict | None) -> str:
    """Format an event dict into a prompt section."""
    if not event:
        return "## Event Context\nNo specific event for this session."

    lines = ["## Event Context (T — Today)"]
    lines.append(f"- **Headline:** {event.get('headline', 'N/A')}")
    lines.append(f"- **Type:** {event.get('event_type', 'N/A')}")
    lines.append(f"- **Source tier:** {event.get('source_tier', 'N/A')}")
    if event.get("raw_text"):
        lines.append(f"- **Details:** {event['raw_text']}")
    if event.get("extracted_entities"):
        lines.append(f"- **Entities:** {event['extracted_entities']}")
    return "\n".join(lines)
