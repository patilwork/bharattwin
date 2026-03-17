"""
Derivatives data ingester — Nifty F&O OI, PCR, max pain via Zerodha Kite MCP.

Fetches option chain OI for Nifty weekly/monthly expiry, computes:
  - Total call OI / put OI
  - Put-Call Ratio (PCR)
  - Max pain strike
  - Nifty futures OI
  - ATM IV proxy (from ATM option prices)
  - Key OI concentration strikes

This data feeds the dealer_hedging agent with real derivatives positioning.

Usage (from Claude Code):
    1. Call Kite search_instruments to find current expiry options
    2. Call Kite get_quotes for option chain + futures
    3. Pass results to parse_and_store()

Instrument naming convention:
    Weekly:  NFO:NIFTY2632423500CE  (NIFTY + YYMDD + strike + CE/PE)
    Monthly: NFO:NIFTY26MAR23500CE  (NIFTY + YYMMM + strike + CE/PE)
    Futures: NFO:NIFTY26MARFUT
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import date, datetime
from typing import Any

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"

# Nifty lot size
NIFTY_LOT_SIZE = 65

# Strike range around ATM to fetch (in points)
STRIKE_RANGE = 2000  # ±2000 points from ATM
STRIKE_STEP = 50     # Nifty option strikes are 50-point apart


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def parse_option_chain(quotes: dict[str, dict], spot: float) -> dict[str, Any]:
    """
    Parse Kite get_quotes response for an option chain into structured derivatives data.

    Args:
        quotes: Raw response from Kite get_quotes (keys like "NFO:NIFTY2632423500CE")
        spot: Current Nifty spot/futures price

    Returns:
        Structured derivatives dict with PCR, max pain, OI distribution, etc.
    """
    calls: dict[int, dict] = {}   # strike → {oi, volume, last_price}
    puts: dict[int, dict] = {}
    futures_data: dict[str, Any] = {}

    for key, data in quotes.items():
        if "FUT" in key:
            futures_data = {
                "last_price": data.get("last_price"),
                "oi": data.get("oi", 0),
                "volume": data.get("volume", 0),
                "oi_day_high": data.get("oi_day_high", 0),
                "oi_day_low": data.get("oi_day_low", 0),
            }
            continue

        # Parse strike and type from instrument key
        oi = data.get("oi", 0)
        volume = data.get("volume", 0)
        last_price = data.get("last_price", 0)

        if key.endswith("CE"):
            strike = _extract_strike(key)
            if strike:
                calls[strike] = {"oi": oi, "volume": volume, "last_price": last_price}
        elif key.endswith("PE"):
            strike = _extract_strike(key)
            if strike:
                puts[strike] = {"oi": oi, "volume": volume, "last_price": last_price}

    if not calls and not puts:
        return {}

    # --- Compute PCR ---
    total_call_oi = sum(c["oi"] for c in calls.values())
    total_put_oi = sum(p["oi"] for p in puts.values())
    pcr_oi = round(total_put_oi / total_call_oi, 4) if total_call_oi > 0 else 0

    total_call_vol = sum(c["volume"] for c in calls.values())
    total_put_vol = sum(p["volume"] for p in puts.values())
    pcr_volume = round(total_put_vol / total_call_vol, 4) if total_call_vol > 0 else 0

    # --- Max Pain ---
    max_pain_strike = _compute_max_pain(calls, puts)

    # --- OI concentration ---
    # Top 5 call OI strikes (resistance) and top 5 put OI strikes (support)
    top_call_oi = sorted(calls.items(), key=lambda x: x[1]["oi"], reverse=True)[:5]
    top_put_oi = sorted(puts.items(), key=lambda x: x[1]["oi"], reverse=True)[:5]

    # --- ATM IV proxy ---
    atm_strike = _nearest_strike(spot, list(calls.keys()))
    atm_call_price = calls.get(atm_strike, {}).get("last_price", 0)
    atm_put_price = puts.get(atm_strike, {}).get("last_price", 0)
    straddle_price = atm_call_price + atm_put_price
    straddle_pct = round(straddle_price / spot * 100, 2) if spot > 0 else 0

    return {
        "spot": spot,
        "futures": futures_data,
        "pcr_oi": pcr_oi,
        "pcr_volume": pcr_volume,
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "total_call_volume": total_call_vol,
        "total_put_volume": total_put_vol,
        "max_pain_strike": max_pain_strike,
        "atm_strike": atm_strike,
        "atm_straddle_price": round(straddle_price, 2),
        "atm_straddle_pct": straddle_pct,
        "top_call_oi_strikes": [{"strike": s, "oi": d["oi"]} for s, d in top_call_oi],
        "top_put_oi_strikes": [{"strike": s, "oi": d["oi"]} for s, d in top_put_oi],
        "call_oi_by_strike": {str(s): d["oi"] for s, d in sorted(calls.items())},
        "put_oi_by_strike": {str(s): d["oi"] for s, d in sorted(puts.items())},
    }


def _extract_strike(instrument_key: str) -> int | None:
    """
    Extract strike price from instrument key.

    Formats:
      Weekly:  NFO:NIFTY2632423500CE  → NIFTY + 26324 (YYMDD) + 23500
      Monthly: NFO:NIFTY26MAR23500CE  → NIFTY + 26MAR + 23500
    """
    import re
    sym = instrument_key.split(":")[-1]
    sym = sym[:-2]  # Remove CE or PE

    # Monthly format: NIFTY26MAR23500 — month name then strike
    match = re.match(r'(?:BANK)?NIFTY\d{2}[A-Z]{3}(\d+)$', sym)
    if match:
        return int(match.group(1))

    # Weekly format: NIFTY2632423500 — YYMDD then strike
    # Date part is exactly 5 digits (YYMDD), rest is strike
    match = re.match(r'(?:BANK)?NIFTY(\d{5})(\d+)$', sym)
    if match:
        return int(match.group(2))

    return None


def _nearest_strike(spot: float, strikes: list[int]) -> int:
    """Find the strike nearest to spot."""
    if not strikes:
        return 0
    return min(strikes, key=lambda s: abs(s - spot))


def _compute_max_pain(calls: dict[int, dict], puts: dict[int, dict]) -> int:
    """
    Compute max pain — the strike at which total option buyer loss is maximized
    (equivalently, where option writers' liability is minimized).

    For each potential expiry price P:
      - Call writers pay max(0, P - strike) × OI for each call
      - Put writers pay max(0, strike - P) × OI for each put
      - Max pain = P that minimizes total writer payout
    """
    all_strikes = sorted(set(list(calls.keys()) + list(puts.keys())))
    if not all_strikes:
        return 0

    min_pain = float("inf")
    max_pain_strike = all_strikes[0]

    for expiry_price in all_strikes:
        total_pain = 0

        # Call pain: for each call, if expiry > strike, writer pays
        for strike, data in calls.items():
            if expiry_price > strike:
                total_pain += (expiry_price - strike) * data["oi"]

        # Put pain: for each put, if expiry < strike, writer pays
        for strike, data in puts.items():
            if expiry_price < strike:
                total_pain += (strike - expiry_price) * data["oi"]

        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = expiry_price

    return max_pain_strike


def store_derivatives(d: date, deriv_data: dict) -> int:
    """
    Store derivatives data into market_state.deriv_map.

    Args:
        d: Trading date
        deriv_data: Output of parse_option_chain()

    Returns:
        1 if stored, 0 if no market_state row
    """
    engine = _get_engine()

    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE market_state
            SET deriv_map = CAST(:dm AS jsonb),
                data_quality = jsonb_set(
                    COALESCE(data_quality, '{}'::jsonb),
                    '{has_derivatives}', 'true'::jsonb
                )
            WHERE session_id = :sid AND universe_id = 'nifty50'
        """), {"dm": json.dumps(deriv_data), "sid": f"CM_{d}"})

    engine.dispose()

    if result.rowcount > 0:
        logger.info("derivatives: stored for %s — PCR=%.2f maxpain=%d",
                     d, deriv_data.get("pcr_oi", 0), deriv_data.get("max_pain_strike", 0))
        return 1
    return 0


def format_for_agents(deriv_data: dict) -> str:
    """Format derivatives data as a section for agent prompts."""
    if not deriv_data:
        return "## Derivatives Data\nN/A"

    lines = ["## Derivatives Positioning (Nifty F&O)"]

    # Futures
    fut = deriv_data.get("futures", {})
    if fut:
        lines.append(f"- Nifty Futures: {fut.get('last_price', 'N/A')} | "
                      f"OI: {fut.get('oi', 0):,} contracts")

    # PCR
    pcr = deriv_data.get("pcr_oi", 0)
    pcr_signal = ""
    if pcr > 1.2:
        pcr_signal = " (bearish extreme — contrarian bullish)"
    elif pcr > 0.9:
        pcr_signal = " (mildly bearish)"
    elif pcr < 0.5:
        pcr_signal = " (bullish extreme — contrarian bearish)"
    elif pcr < 0.7:
        pcr_signal = " (retail call buying — caution)"
    lines.append(f"- PCR (OI): {pcr:.2f}{pcr_signal}")
    lines.append(f"- PCR (Volume): {deriv_data.get('pcr_volume', 0):.2f}")

    # OI totals
    lines.append(f"- Total Call OI: {deriv_data.get('total_call_oi', 0):,}")
    lines.append(f"- Total Put OI: {deriv_data.get('total_put_oi', 0):,}")

    # Max pain
    lines.append(f"- Max Pain Strike: {deriv_data.get('max_pain_strike', 'N/A')}")

    # ATM straddle
    lines.append(f"- ATM Straddle: ₹{deriv_data.get('atm_straddle_price', 0):.0f} "
                  f"({deriv_data.get('atm_straddle_pct', 0):.1f}% of spot)")

    # Key strikes
    lines.append(f"\n  **Resistance (Top Call OI):**")
    for item in deriv_data.get("top_call_oi_strikes", []):
        lines.append(f"  - {item['strike']:,}: {item['oi']:,} OI")

    lines.append(f"  **Support (Top Put OI):**")
    for item in deriv_data.get("top_put_oi_strikes", []):
        lines.append(f"  - {item['strike']:,}: {item['oi']:,} OI")

    return "\n".join(lines)
