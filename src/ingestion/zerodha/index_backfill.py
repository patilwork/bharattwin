"""
Zerodha index price backfill.

Fetches historical daily OHLC for Nifty 50, BankNifty, and India VIX
from the Kite MCP and upserts into market_state.

Usage (from Claude Code):
    1. Call Kite get_historical_data for each instrument token
    2. Pass the results to store_historical()

Instrument tokens:
    Nifty 50:   256265
    BankNifty:  260105
    India VIX:  264969
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"

# Column mapping for each instrument
INSTRUMENTS = {
    256265: {"name": "NIFTY 50", "column": "nifty50_close"},
    260105: {"name": "NIFTY BANK", "column": "banknifty_close"},
    264969: {"name": "INDIA VIX", "column": "india_vix"},
}


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def store_historical(
    nifty_data: list[dict],
    banknifty_data: list[dict],
    vix_data: list[dict],
) -> dict[str, int]:
    """
    Store historical index data into market_state.

    Each data list contains dicts with at least "date" and "close" keys
    (as returned by Kite get_historical_data).

    Returns: {"updated": N, "skipped": M}
    """
    # Build lookup: date_str → {nifty, banknifty, vix}
    prices: dict[str, dict[str, float]] = {}

    for row in nifty_data:
        d = _extract_date(row["date"])
        prices.setdefault(d, {})["nifty50_close"] = float(row["close"])

    for row in banknifty_data:
        d = _extract_date(row["date"])
        prices.setdefault(d, {})["banknifty_close"] = float(row["close"])

    for row in vix_data:
        d = _extract_date(row["date"])
        prices.setdefault(d, {})["india_vix"] = float(row["close"])

    engine = _get_engine()
    updated = 0
    skipped = 0

    with engine.begin() as conn:
        for date_str in sorted(prices.keys()):
            p = prices[date_str]
            sid = f"CM_{date_str}"

            n = p.get("nifty50_close")
            b = p.get("banknifty_close")
            v = p.get("india_vix")

            result = conn.execute(text("""
                UPDATE market_state
                SET nifty50_close = COALESCE(:n, nifty50_close),
                    banknifty_close = COALESCE(:b, banknifty_close),
                    india_vix = COALESCE(:v, india_vix)
                WHERE session_id = :sid AND universe_id = 'nifty50'
            """), {"n": n, "b": b, "v": v, "sid": sid})

            if result.rowcount > 0:
                updated += 1
                logger.info("index_backfill: %s → Nifty=%s BankNifty=%s VIX=%s",
                            sid, n, b, v)
            else:
                skipped += 1
                logger.debug("index_backfill: %s — no market_state row, skipped", sid)

    engine.dispose()
    logger.info("index_backfill: updated=%d skipped=%d", updated, skipped)
    return {"updated": updated, "skipped": skipped}


def _extract_date(date_val: str | Any) -> str:
    """Extract YYYY-MM-DD from various date formats."""
    s = str(date_val)
    return s[:10]  # Works for ISO 8601 and "YYYY-MM-DDTHH:MM:SS+05:30"
