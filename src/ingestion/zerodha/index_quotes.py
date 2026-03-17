"""
Zerodha/Kite index quote fetcher.

Fetches latest Nifty 50, Bank Nifty, and India VIX values via the Kite MCP
and upserts them into market_state (nifty50_close, banknifty_close, india_vix).

Instrument tokens (NSE):
  - Nifty 50:    256265
  - Bank Nifty:  260105
  - India VIX:   264969

Usage:
  This module is designed to be called from within Claude Code via MCP tool calls.
  For programmatic access, use fetch_and_store() which calls the Kite MCP under the hood.
  For standalone scripts, use the manual path: fetch quotes via Kite API, then call store().

Note: Kite requires an active session (login via MCP or access token).
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

# Kite instrument tokens
TOKENS = {
    "NIFTY 50": 256265,
    "NIFTY BANK": 260105,
    "INDIA VIX": 264969,
}

# Column mapping: instrument name → market_state column
_COL_MAP = {
    "NIFTY 50": "nifty50_close",
    "NIFTY BANK": "banknifty_close",
    "INDIA VIX": "india_vix",
}


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def store(d: date, quotes: dict[str, float]) -> int:
    """
    Store index quotes into market_state for date d.

    Args:
        d: Trading date
        quotes: Dict mapping instrument name → closing value
                e.g. {"NIFTY 50": 22456.80, "NIFTY BANK": 48123.45, "INDIA VIX": 13.25}

    Returns:
        Number of rows affected (0 or 1)
    """
    engine = _get_engine()

    # Build SET clause for available quotes
    updates = {}
    for name, value in quotes.items():
        col = _COL_MAP.get(name)
        if col and value is not None:
            updates[col] = float(value)

    if not updates:
        logger.warning("zerodha_quotes: no valid quotes to store for %s", d)
        engine.dispose()
        return 0

    session_id = f"CM_{d}"

    with engine.begin() as conn:
        # Check if row exists
        existing = conn.execute(text("""
            SELECT id FROM market_state
            WHERE session_id = :sid AND universe_id = 'nifty50'
        """), {"sid": session_id}).first()

        if existing:
            # Build dynamic UPDATE
            set_parts = [f"{col} = :{col}" for col in updates]
            set_clause = ", ".join(set_parts)
            params = {**updates, "sid": session_id}
            conn.execute(text(f"""
                UPDATE market_state SET {set_clause}
                WHERE session_id = :sid AND universe_id = 'nifty50'
            """), params)
            affected = 1
        else:
            # Insert skeleton row with index values
            cols = ["asof_ts_ist", "session_id", "universe_id", "replay_cutoff_ts"] + list(updates.keys())
            placeholders = [":asof_ts_ist", ":session_id", ":universe_id", ":replay_cutoff_ts"] + [f":{c}" for c in updates]
            params = {
                "asof_ts_ist": f"{d}T15:30:00+05:30",
                "session_id": session_id,
                "universe_id": "nifty50",
                "replay_cutoff_ts": f"{d}T15:30:00+05:30",
                **updates,
            }
            conn.execute(text(f"""
                INSERT INTO market_state ({', '.join(cols)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT (universe_id, session_id) DO UPDATE SET
                    {', '.join(f'{c} = EXCLUDED.{c}' for c in updates)}
            """), params)
            affected = 1

    engine.dispose()
    logger.info("zerodha_quotes: stored %s for %s → %s", list(updates.keys()), d, updates)
    return affected


def parse_ltp_response(ltp_data: dict[str, Any]) -> dict[str, float]:
    """
    Parse the response from Kite get_ltp MCP call.

    Kite LTP response format:
      {"NSE:NIFTY 50": {"instrument_token": 256265, "last_price": 22456.80}, ...}

    Returns: {"NIFTY 50": 22456.80, ...}
    """
    quotes = {}
    for key, val in ltp_data.items():
        # Key format: "NSE:NIFTY 50" or just instrument name
        name = key.split(":")[-1].strip()
        if isinstance(val, dict):
            price = val.get("last_price") or val.get("close") or val.get("ltp")
        elif isinstance(val, (int, float)):
            price = val
        else:
            continue
        if price is not None:
            quotes[name] = float(price)
    return quotes


def parse_ohlc_response(ohlc_data: dict[str, Any]) -> dict[str, float]:
    """
    Parse the response from Kite get_ohlc MCP call.

    OHLC response has close price which is more accurate for EOD.
    Format: {"NSE:NIFTY 50": {"ohlc": {"close": 22456.80, ...}, "last_price": ...}, ...}

    Returns: {"NIFTY 50": 22456.80, ...} (uses close if available, else last_price)
    """
    quotes = {}
    for key, val in ohlc_data.items():
        name = key.split(":")[-1].strip()
        if isinstance(val, dict):
            ohlc = val.get("ohlc", {})
            price = ohlc.get("close") or val.get("last_price")
        elif isinstance(val, (int, float)):
            price = val
        else:
            continue
        if price is not None:
            quotes[name] = float(price)
    return quotes
