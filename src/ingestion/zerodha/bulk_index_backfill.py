"""
Bulk index backfill — parse Zerodha historical data files and store to DB.

Handles the large JSON files from Kite get_historical_data MCP calls.
Designed for multi-year backfills.

Usage:
    python -m src.ingestion.zerodha.bulk_index_backfill <nifty_file> <banknifty_file> <vix_file>
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date
from typing import Any

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def parse_kite_historical_file(filepath: str) -> list[dict]:
    """Parse a Kite historical data JSON file into a list of {date, close} dicts."""
    with open(filepath) as f:
        raw = json.load(f)

    # Handle the MCP wrapper format: [{"type": "text", "text": "[{...}, ...]"}]
    if isinstance(raw, list) and raw and isinstance(raw[0], dict) and "text" in raw[0]:
        data = json.loads(raw[0]["text"])
    elif isinstance(raw, list) and raw and isinstance(raw[0], dict) and "date" in raw[0]:
        data = raw
    else:
        raise ValueError(f"Unexpected format in {filepath}")

    results = []
    for row in data:
        d = str(row["date"])[:10]  # Extract YYYY-MM-DD
        results.append({
            "date": d,
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
        })

    return results


def bulk_store_index_prices(
    nifty_data: list[dict],
    banknifty_data: list[dict],
    vix_data: list[dict],
) -> dict[str, int]:
    """
    Bulk upsert index prices into market_state.

    Creates skeleton market_state rows if they don't exist.
    Uses COALESCE to never overwrite existing data with NULL.

    Returns: {created: N, updated: M, skipped: K}
    """
    # Build lookup by date
    nifty = {d["date"]: d["close"] for d in nifty_data}
    banknifty = {d["date"]: d["close"] for d in banknifty_data}
    vix = {d["date"]: d["close"] for d in vix_data}

    all_dates = sorted(set(list(nifty.keys()) + list(banknifty.keys()) + list(vix.keys())))

    engine = _get_engine()
    stats = {"created": 0, "updated": 0, "total_dates": len(all_dates)}

    with engine.begin() as conn:
        for dt_str in all_dates:
            sid = f"CM_{dt_str}"
            n = nifty.get(dt_str)
            b = banknifty.get(dt_str)
            v = vix.get(dt_str)

            # Try update first
            result = conn.execute(text("""
                UPDATE market_state
                SET nifty50_close = COALESCE(:n, nifty50_close),
                    banknifty_close = COALESCE(:b, banknifty_close),
                    india_vix = COALESCE(:v, india_vix)
                WHERE session_id = :sid AND universe_id = 'nifty50'
            """), {"n": n, "b": b, "v": v, "sid": sid})

            if result.rowcount == 0:
                # Insert skeleton row
                conn.execute(text("""
                    INSERT INTO market_state (
                        asof_ts_ist, session_id, universe_id,
                        nifty50_close, banknifty_close, india_vix,
                        replay_cutoff_ts
                    ) VALUES (
                        :ts, :sid, 'nifty50', :n, :b, :v, :ts
                    )
                    ON CONFLICT (universe_id, session_id) DO UPDATE SET
                        nifty50_close = COALESCE(EXCLUDED.nifty50_close, market_state.nifty50_close),
                        banknifty_close = COALESCE(EXCLUDED.banknifty_close, market_state.banknifty_close),
                        india_vix = COALESCE(EXCLUDED.india_vix, market_state.india_vix)
                """), {"ts": f"{dt_str}T15:30:00+05:30", "sid": sid, "n": n, "b": b, "v": v})
                stats["created"] += 1
            else:
                stats["updated"] += 1

    engine.dispose()
    logger.info("bulk_index: %d dates — %d created, %d updated",
                stats["total_dates"], stats["created"], stats["updated"])
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if len(sys.argv) < 4:
        print("Usage: python -m src.ingestion.zerodha.bulk_index_backfill <nifty.json> <banknifty.json> <vix.json>")
        sys.exit(1)

    nifty = parse_kite_historical_file(sys.argv[1])
    banknifty = parse_kite_historical_file(sys.argv[2])
    vix = parse_kite_historical_file(sys.argv[3])

    print(f"Parsed: Nifty={len(nifty)} days, BankNifty={len(banknifty)} days, VIX={len(vix)} days")

    stats = bulk_store_index_prices(nifty, banknifty, vix)
    print(f"Stored: {stats}")
