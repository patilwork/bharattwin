"""
Morningstar fundamental data integration.

Fetches valuation, moat, and rating data for Nifty 50 heavyweights
via Morningstar MCP and stores in market_state.

This module is designed for Claude Code in-context usage:
  1. Claude calls Morningstar MCP tools to fetch data
  2. Passes results to store_fundamentals() for DB storage

Morningstar IDs for top Nifty 50 stocks:
  RELIANCE:   0P0000B1W1
  HDFCBANK:   0P0000C3NZ
  ICICIBANK:  0P0000BIOD
  INFY:       0P0000AKOJ
  TCS:        0P0000BEC4

Datapoint IDs:
  RR01Y  — Morningstar Rating Overall (1-5 stars)
  LT181  — Economic Moat (None/Narrow/Wide)
  ST201  — Fair Value Uncertainty (Low/Medium/High/Very High)
  QV009  — Quantitative Fair Value Estimate (INR)
  OS603  — Price/Fair Value ratio
  HS05X  — P/E Ratio TTM
  ST408  — P/B Ratio Current
  STA65  — Forward Dividend Yield %
  ST159  — Market Cap (mil) Daily
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from typing import Any

from sqlalchemy import create_engine, text

from src.utils.time_utils import IST

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"

# Nifty 50 heavyweight Morningstar IDs
MORNINGSTAR_IDS = {
    "RELIANCE": "0P0000B1W1",
    "HDFCBANK": "0P0000C3NZ",
    "ICICIBANK": "0P0000BIOD",
    "INFY": "0P0000AKOJ",
    "TCS": "0P0000BEC4",
}

# Datapoints to fetch
DATAPOINT_IDS = [
    "RR01Y",  # Star rating
    "LT181",  # Economic moat
    "ST201",  # Fair value uncertainty
    "QV009",  # Quant fair value estimate
    "OS603",  # Price / fair value
    "HS05X",  # P/E TTM
    "ST408",  # P/B current
    "STA65",  # Fwd dividend yield
    "ST159",  # Market cap
]

# Human-readable labels
DATAPOINT_LABELS = {
    "RR01Y": "star_rating",
    "LT181": "economic_moat",
    "ST201": "fair_value_uncertainty",
    "QV009": "quant_fair_value_inr",
    "OS603": "price_to_fair_value",
    "HS05X": "pe_ttm",
    "ST408": "pb_current",
    "STA65": "fwd_dividend_yield_pct",
    "ST159": "market_cap_mil",
}


def parse_morningstar_response(raw: dict) -> dict[str, dict[str, Any]]:
    """
    Parse the raw Morningstar data tool response into a clean dict.

    Args:
        raw: The "result" dict from morningstar-data-tool response.

    Returns:
        {symbol: {star_rating: 4, economic_moat: "Narrow", pe_ttm: 18.5, ...}}
    """
    # Reverse lookup: morningstar_id → symbol
    id_to_symbol = {v: k for k, v in MORNINGSTAR_IDS.items()}

    parsed = {}
    for ms_id, data in raw.items():
        symbol = id_to_symbol.get(ms_id, ms_id)
        stock_data = {}

        for val in data.get("values", []):
            dp_id = val.get("datapointId")
            value = val.get("value")
            label = DATAPOINT_LABELS.get(dp_id, dp_id)

            # Type conversion
            if value is not None:
                try:
                    if dp_id in ("RR01Y",):
                        stock_data[label] = int(value)
                    elif dp_id in ("LT181", "ST201"):
                        stock_data[label] = str(value)
                    else:
                        stock_data[label] = float(value)
                except (ValueError, TypeError):
                    stock_data[label] = value

        parsed[symbol] = stock_data

    return parsed


def store_fundamentals(d: date, fundamentals: dict[str, dict[str, Any]]) -> int:
    """
    Store Morningstar fundamentals into market_state.macro_map under a
    'morningstar' key.

    Args:
        d: The market_state date to attach to.
        fundamentals: Output of parse_morningstar_response().

    Returns:
        1 if stored, 0 if no market_state row found.
    """
    engine = create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))

    with engine.begin() as conn:
        # Read existing macro_map
        row = conn.execute(text("""
            SELECT macro_map FROM market_state
            WHERE session_id = :sid AND universe_id = 'nifty50'
        """), {"sid": f"CM_{d}"}).first()

        if not row:
            logger.warning("morningstar: no market_state row for %s", d)
            engine.dispose()
            return 0

        macro_map = row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")

        # Add Morningstar data
        macro_map["morningstar"] = {
            "fetched_at": datetime.now(IST).isoformat(),
            "stocks": fundamentals,
            "summary": _compute_summary(fundamentals),
        }

        conn.execute(text("""
            UPDATE market_state
            SET macro_map = CAST(:mm AS jsonb)
            WHERE session_id = :sid AND universe_id = 'nifty50'
        """), {"mm": json.dumps(macro_map), "sid": f"CM_{d}"})

    engine.dispose()
    logger.info("morningstar: stored fundamentals for %s (%d stocks)", d, len(fundamentals))
    return 1


def _compute_summary(fundamentals: dict[str, dict]) -> dict:
    """Compute aggregate summary from individual stock data."""
    pe_values = []
    pb_values = []
    undervalued = 0
    overvalued = 0
    moat_counts = {"None": 0, "Narrow": 0, "Wide": 0}

    for sym, data in fundamentals.items():
        pe = data.get("pe_ttm")
        if pe and pe > 0:
            pe_values.append(pe)

        pb = data.get("pb_current")
        if pb and pb > 0:
            pb_values.append(pb)

        pfv = data.get("price_to_fair_value")
        if pfv:
            if pfv < 0.9:
                undervalued += 1
            elif pfv > 1.1:
                overvalued += 1

        moat = data.get("economic_moat", "NA")
        if moat in moat_counts:
            moat_counts[moat] += 1

    return {
        "avg_pe_ttm": round(sum(pe_values) / len(pe_values), 2) if pe_values else None,
        "avg_pb": round(sum(pb_values) / len(pb_values), 2) if pb_values else None,
        "undervalued_count": undervalued,
        "overvalued_count": overvalued,
        "moat_distribution": moat_counts,
        "stocks_covered": len(fundamentals),
    }


def format_for_agents(fundamentals: dict[str, dict]) -> str:
    """Format Morningstar data as a section for agent prompts."""
    lines = ["## Morningstar Fundamental Data (Top Nifty 50 Heavyweights)"]

    summary = _compute_summary(fundamentals)
    if summary["avg_pe_ttm"]:
        lines.append(f"- Avg P/E (TTM): {summary['avg_pe_ttm']:.1f}x")
    if summary["avg_pb"]:
        lines.append(f"- Avg P/B: {summary['avg_pb']:.2f}x")
    lines.append(f"- Undervalued (P/FV < 0.9): {summary['undervalued_count']}/{summary['stocks_covered']}")
    lines.append(f"- Overvalued (P/FV > 1.1): {summary['overvalued_count']}/{summary['stocks_covered']}")
    lines.append(f"- Moat: {summary['moat_distribution']}")
    lines.append("")

    lines.append("| Stock | Stars | Moat | P/E | P/B | P/FV | FV Uncert |")
    lines.append("|-------|-------|------|-----|-----|------|-----------|")

    for sym in ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFY", "TCS"]:
        data = fundamentals.get(sym, {})
        stars = data.get("star_rating", "N/A")
        moat = data.get("economic_moat", "N/A")
        pe = f"{data['pe_ttm']:.1f}" if data.get("pe_ttm") else "N/A"
        pb = f"{data['pb_current']:.2f}" if data.get("pb_current") else "N/A"
        pfv = f"{data['price_to_fair_value']:.2f}" if data.get("price_to_fair_value") else "N/A"
        unc = data.get("fair_value_uncertainty", "N/A")
        lines.append(f"| {sym} | {stars} | {moat} | {pe} | {pb} | {pfv} | {unc} |")

    return "\n".join(lines)
