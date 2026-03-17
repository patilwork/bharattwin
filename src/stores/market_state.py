"""
Market state builder v1.

Assembles a market_state row from available ingested data sources for a given date:
  - bhavcopy_raw       → nifty50_close, banknifty_close, returns_map
  - macro_map (JSONB)  → usdinr_ref, crude prices, repo_rate
  - calendar_store     → session metadata, expiry flags
  - flow_store         → flow_map (FII/DII)

replay_cutoff_ts is the LATEST cutoff across all constituent sources
(i.e. earliest point at which all data would be available for replay).

EOD data is available after 18:00 IST (bhavcopy typically published by 18:00).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from typing import Any

from sqlalchemy import create_engine, text

from src.utils.time_utils import IST, eod_cutoff

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"

# Nifty 50 and BankNifty index symbols in bhavcopy EQ segment
# (NSE books index values as part of the CM bhavcopy under specific symbols)
_NIFTY50_SYMBOLS = {"NIFTY 50", "NIFTY50", "Nifty 50"}
_BANKNIFTY_SYMBOLS = {"NIFTY BANK", "BANKNIFTY", "Nifty Bank"}


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def build(d: date, force: bool = False) -> dict[str, Any] | None:
    """
    Build and upsert a market_state row for date d.

    Pulls from:
      1. bhavcopy_raw (EOD prices — uses index closes from Zerodha/Kite or bhavcopy EQ)
      2. market_state.macro_map (already populated by RBI ingester)
      3. calendar_store (session metadata)
      4. flow_store (FII/DII flows)

    Returns the assembled state dict, or None if insufficient data.
    """
    engine = _get_engine()

    with engine.connect() as conn:
        # 1. Check calendar (must be a trading day)
        cal = conn.execute(
            text("SELECT * FROM calendar_store WHERE date = :d"), {"d": d}
        ).mappings().first()

        if not cal or not cal["is_trading_day"]:
            logger.info("market_state: %s is not a trading day — skipping", d)
            return None

        # 2. Pull bhavcopy EQ data for index proxies
        # NSE bhavcopy doesn't include index OHLC directly — we use the NIFTY ETFs
        # or fall back to pre-loaded values in macro_map.
        # For now: pull top-volume EQ stocks as a proxy sanity check.
        bhav_rows = conn.execute(text("""
            SELECT symbol, series, close, prev_close, volume, turnover_lakh
            FROM bhavcopy_raw
            WHERE data_date = :d AND series = 'EQ'
            ORDER BY turnover_lakh DESC NULLS LAST
            LIMIT 100
        """), {"d": d}).mappings().fetchall()

        # 3. Pull existing macro_map (RBI FX rates already stored here)
        ms_existing = conn.execute(text("""
            SELECT macro_map, nifty50_close, banknifty_close, india_vix
            FROM market_state
            WHERE session_id = :sid
        """), {"sid": f"CM_{d}"}).mappings().first()

        # 4. Pull flow_store for FII/DII
        flow_rows = conn.execute(text("""
            SELECT participant, segment, buy_crore, sell_crore, net_crore
            FROM flow_store
            WHERE date = :d
        """), {"d": d}).mappings().fetchall()

    # --- Assemble state ---
    session_id = f"CM_{d}"
    asof_ts = eod_cutoff(d).isoformat()
    replay_cutoff = eod_cutoff(d).isoformat()  # EOD data available after 18:00 IST

    # Carry forward existing nifty/banknifty/vix if already set
    nifty50_close = float(ms_existing["nifty50_close"]) if ms_existing and ms_existing["nifty50_close"] else None
    banknifty_close = float(ms_existing["banknifty_close"]) if ms_existing and ms_existing["banknifty_close"] else None
    india_vix = float(ms_existing["india_vix"]) if ms_existing and ms_existing["india_vix"] else None

    # Build returns_map from top bhavcopy stocks
    returns_map: dict[str, float] = {}
    for row in bhav_rows:
        if row["prev_close"] and row["close"] and float(row["prev_close"]) > 0:
            ret = (float(row["close"]) - float(row["prev_close"])) / float(row["prev_close"]) * 100
            returns_map[row["symbol"]] = round(ret, 4)

    # Build flow_map from flow_store
    flow_map: dict[str, Any] = {}
    for row in flow_rows:
        key = f"{row['participant'].lower()}_{row['segment'].lower()}"
        flow_map[key] = {
            "buy_cr": float(row["buy_crore"]) if row["buy_crore"] else None,
            "sell_cr": float(row["sell_crore"]) if row["sell_crore"] else None,
            "net_cr": float(row["net_crore"]) if row["net_crore"] else None,
        }

    # Merge existing macro_map
    existing_macro = {}
    if ms_existing and ms_existing["macro_map"]:
        existing_macro = ms_existing["macro_map"] if isinstance(ms_existing["macro_map"], dict) else json.loads(ms_existing["macro_map"])

    usdinr_ref = existing_macro.get("fx", {}).get("usdinr")

    # Calendar flags → regime_state
    regime_state = {
        "is_nifty_monthly_expiry": cal["is_nifty_monthly_expiry"],
        "is_banknifty_monthly_expiry": cal["is_banknifty_monthly_expiry"],
        "is_finnifty_monthly_expiry": cal["is_finnifty_monthly_expiry"],
        "is_nifty_weekly_expiry": cal["is_nifty_weekly_expiry"],
        "is_banknifty_weekly_expiry": cal["is_banknifty_weekly_expiry"],
    }

    # Data quality summary
    data_quality = {
        "bhavcopy_eq_rows": len(bhav_rows),
        "flow_rows": len(flow_rows),
        "has_fx": bool(usdinr_ref),
        "has_nifty_close": nifty50_close is not None,
        "has_banknifty_close": banknifty_close is not None,
        "has_vix": india_vix is not None,
    }

    source_audit = {
        "bhavcopy": f"nse/bhavcopy/{d}",
        "fx": "rbi/reference_rates" if usdinr_ref else None,
        "flows": "nse/fii_dii" if flow_rows else None,
        "builder_version": "v1",
        "built_at": datetime.now(IST).isoformat(),
    }

    state = {
        "asof_ts_ist": asof_ts,
        "session_id": session_id,
        "universe_id": "nifty50",
        "nifty50_close": nifty50_close,
        "banknifty_close": banknifty_close,
        "india_vix": india_vix,
        "repo_rate_pct": existing_macro.get("repo_rate_pct"),
        "usdinr_ref": usdinr_ref,
        "crude_indian_basket_usd": existing_macro.get("crude", {}).get("indian_basket_usd") if isinstance(existing_macro.get("crude"), dict) else None,
        "returns_map": json.dumps(returns_map),
        "deriv_map": json.dumps({}),  # populated by derivatives ingester (Phase 1)
        "flow_map": json.dumps(flow_map),
        "macro_map": json.dumps(existing_macro),
        "regime_state": json.dumps(regime_state),
        "data_quality": json.dumps(data_quality),
        "source_audit": json.dumps(source_audit),
        "replay_cutoff_ts": replay_cutoff,
    }

    # Upsert into market_state
    with engine.begin() as conn:
        if force:
            conflict_action = """DO UPDATE SET
                nifty50_close = EXCLUDED.nifty50_close,
                banknifty_close = EXCLUDED.banknifty_close,
                india_vix = EXCLUDED.india_vix,
                repo_rate_pct = EXCLUDED.repo_rate_pct,
                usdinr_ref = EXCLUDED.usdinr_ref,
                crude_indian_basket_usd = EXCLUDED.crude_indian_basket_usd,
                returns_map = EXCLUDED.returns_map,
                flow_map = EXCLUDED.flow_map,
                macro_map = EXCLUDED.macro_map,
                regime_state = EXCLUDED.regime_state,
                data_quality = EXCLUDED.data_quality,
                source_audit = EXCLUDED.source_audit,
                replay_cutoff_ts = EXCLUDED.replay_cutoff_ts"""
        else:
            conflict_action = "DO NOTHING"

        conn.execute(text(f"""
            INSERT INTO market_state (
                asof_ts_ist, session_id, universe_id,
                nifty50_close, banknifty_close, india_vix,
                repo_rate_pct, usdinr_ref, crude_indian_basket_usd,
                returns_map, deriv_map, flow_map, macro_map,
                regime_state, data_quality, source_audit, replay_cutoff_ts
            ) VALUES (
                :asof_ts_ist, :session_id, :universe_id,
                :nifty50_close, :banknifty_close, :india_vix,
                :repo_rate_pct, :usdinr_ref, :crude_indian_basket_usd,
                CAST(:returns_map AS jsonb), CAST(:deriv_map AS jsonb),
                CAST(:flow_map AS jsonb), CAST(:macro_map AS jsonb),
                CAST(:regime_state AS jsonb), CAST(:data_quality AS jsonb),
                CAST(:source_audit AS jsonb),
                :replay_cutoff_ts
            )
            ON CONFLICT (universe_id, session_id) {conflict_action}
        """), state)

    engine.dispose()
    logger.info("market_state: upserted %s", session_id)
    return state


def get(d: date) -> dict | None:
    """Fetch a market_state row for date d."""
    engine = _get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM market_state WHERE session_id = :sid"),
            {"sid": f"CM_{d}"}
        ).mappings().first()
    engine.dispose()
    return dict(row) if row else None
