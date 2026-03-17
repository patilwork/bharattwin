#!/usr/bin/env python3
"""
Load Kite historical stock data into bhavcopy_raw table.

Reads all JSON files from data/kite_historical/, parses them, computes
prev_close, and inserts into the existing bhavcopy_raw table.

Usage:
    python3 scripts/load_kite_to_db.py
"""

import json
import logging
import os
import re
import sys
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

DB_URL = os.environ.get("DATABASE_URL", "postgresql://bharattwin:devpassword@localhost:5434/bharattwin")
DATA_DIR = Path(__file__).parent.parent / "data" / "kite_historical"
INDICES_DIR = Path(__file__).parent.parent / "data" / "kite_historical_indices"

SKIP_FILES = {"instrument_tokens.json", "morningstar_fundamentals.json", "morningstar_fundamentals_all.json",
              "fii_dii_flows.json", "fetch_manifest.json", "batch_status.json"}


def parse_kite_file(filepath: Path) -> list[dict]:
    """Parse a Kite historical data JSON file."""
    with open(filepath) as f:
        raw = json.load(f)

    # Handle MCP wrapper format
    if isinstance(raw, list) and raw and isinstance(raw[0], dict) and "text" in raw[0]:
        data = json.loads(raw[0]["text"])
    elif isinstance(raw, list) and raw and isinstance(raw[0], dict) and "date" in raw[0]:
        data = raw
    else:
        return []

    return data or []


def extract_symbol(filename: str) -> str:
    """Extract stock symbol from filename like RELIANCE_2006_2007.json."""
    name = filename.replace(".json", "")
    # Remove trailing _YYYY_YYYY or _YYYY patterns
    name = re.sub(r'_\d{4}(_\d{4})?(_all)?$', '', name)
    return name


def load_stock_data():
    """Load all stock OHLCV data into bhavcopy_raw."""
    engine = create_engine(DB_URL)

    # Get existing data dates to avoid duplicates
    with engine.connect() as conn:
        existing = conn.execute(text(
            "SELECT data_date, symbol FROM bhavcopy_raw"
        )).fetchall()
    existing_set = {(str(r[0]), r[1]) for r in existing}
    logger.info(f"Existing bhavcopy_raw rows: {len(existing_set)}")

    # Find all stock JSON files
    files = sorted(DATA_DIR.glob("*.json"))
    files = [f for f in files if f.name not in SKIP_FILES and not f.name.startswith("morningstar")]

    # Group by symbol and merge all chunks
    stock_data: dict[str, list[dict]] = {}
    for f in files:
        symbol = extract_symbol(f.name)
        if not symbol:
            continue
        data = parse_kite_file(f)
        if data:
            stock_data.setdefault(symbol, []).extend(data)

    logger.info(f"Parsed {len(stock_data)} stocks from {len(files)} files")

    # Deduplicate and sort per stock
    total_inserted = 0
    total_skipped = 0

    for symbol, candles in sorted(stock_data.items()):
        # Deduplicate by date
        by_date = {}
        for c in candles:
            dt = c["date"][:10]
            by_date[dt] = c
        sorted_dates = sorted(by_date.keys())

        rows_to_insert = []
        prev_close = None

        for dt in sorted_dates:
            c = by_date[dt]
            close = c.get("close")

            if (dt, symbol) in existing_set:
                total_skipped += 1
                prev_close = close
                continue

            rows_to_insert.append({
                "data_date": dt,
                "symbol": symbol,
                "series": "EQ",
                "open": c.get("open"),
                "high": c.get("high"),
                "low": c.get("low"),
                "close": close,
                "prev_close": prev_close,
                "volume": c.get("volume", 0),
                "turnover_lakh": 0,
                "source": "kite_historical",
            })
            prev_close = close

        if rows_to_insert:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO bhavcopy_raw (data_date, symbol, series, open, high, low, close, prev_close, volume, turnover_lakh, source)
                    VALUES (:data_date, :symbol, :series, :open, :high, :low, :close, :prev_close, :volume, :turnover_lakh, :source)
                    ON CONFLICT DO NOTHING
                """), rows_to_insert)
            total_inserted += len(rows_to_insert)

        if len(rows_to_insert) > 0:
            logger.info(f"  {symbol}: {len(rows_to_insert)} rows inserted, {len(sorted_dates) - len(rows_to_insert)} skipped")

    engine.dispose()
    logger.info(f"\nTOTAL: {total_inserted:,} rows inserted, {total_skipped:,} skipped")
    return total_inserted


def load_repo_rates():
    """Load RBI repo rate history into market_state.repo_rate_pct."""
    REPO_RATES = [
        ("2006-01-01", 6.25), ("2006-06-09", 6.50), ("2006-07-25", 7.00), ("2006-10-31", 7.25),
        ("2007-01-31", 7.50), ("2007-03-30", 7.75), ("2008-06-12", 8.00), ("2008-07-29", 8.50),
        ("2008-09-16", 9.00), ("2008-10-20", 8.00), ("2008-11-03", 7.50), ("2008-12-08", 6.50),
        ("2009-01-05", 5.50), ("2009-03-05", 5.00), ("2009-04-21", 4.75),
        ("2010-03-19", 5.00), ("2010-04-20", 5.25), ("2010-07-02", 5.50), ("2010-07-27", 5.75),
        ("2010-09-16", 6.00), ("2010-11-02", 6.25), ("2011-01-25", 6.50), ("2011-03-17", 6.75),
        ("2011-05-03", 7.25), ("2011-06-16", 7.50), ("2011-07-26", 8.00), ("2011-09-16", 8.25),
        ("2011-10-25", 8.50), ("2012-04-17", 8.00), ("2013-05-03", 7.25), ("2014-01-28", 8.00),
        ("2015-01-15", 7.75), ("2015-03-04", 7.50), ("2015-06-02", 7.25), ("2015-09-29", 6.75),
        ("2016-04-05", 6.50), ("2016-10-04", 6.25), ("2017-08-02", 6.00),
        ("2018-06-06", 6.25), ("2018-08-01", 6.50),
        ("2019-02-07", 6.25), ("2019-04-04", 6.00), ("2019-06-06", 5.75),
        ("2019-08-07", 5.40), ("2019-10-04", 5.15),
        ("2020-03-27", 4.40), ("2020-05-22", 4.00),
        ("2022-05-04", 4.40), ("2022-06-08", 4.90), ("2022-08-05", 5.40),
        ("2022-09-30", 5.90), ("2022-12-07", 6.25), ("2023-02-08", 6.50),
        ("2025-02-07", 6.25),
    ]

    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        all_dates = [r[0] for r in conn.execute(text(
            "SELECT session_id FROM market_state WHERE universe_id = 'nifty50' ORDER BY session_id"
        )).fetchall()]

    updated = 0
    with engine.begin() as conn:
        for sid in all_dates:
            date_str = sid.replace("CM_", "")
            rate = [r for d, r in REPO_RATES if d <= date_str]
            if rate:
                conn.execute(text("""
                    UPDATE market_state SET repo_rate_pct = :rate
                    WHERE session_id = :sid AND universe_id = 'nifty50'
                """), {"rate": rate[-1], "sid": sid})
                updated += 1

    engine.dispose()
    logger.info(f"Repo rates: updated {updated} market_state rows")
    return updated


def load_usdinr():
    """Load USDINR from kite_historical_indices into market_state."""
    files = sorted(INDICES_DIR.glob("USDINR*.json"))
    if not files:
        logger.warning("No USDINR files found")
        return 0

    all_data = {}
    for f in files:
        data = parse_kite_file(f)
        for d in data:
            dt = d["date"][:10]
            all_data[dt] = d["close"]

    engine = create_engine(DB_URL)
    updated = 0
    with engine.begin() as conn:
        for dt, rate in sorted(all_data.items()):
            sid = f"CM_{dt}"
            result = conn.execute(text("""
                UPDATE market_state SET usdinr_ref = :rate
                WHERE session_id = :sid AND universe_id = 'nifty50'
            """), {"rate": rate, "sid": sid})
            if result.rowcount > 0:
                updated += 1

    engine.dispose()
    logger.info(f"USDINR: updated {updated} market_state rows")
    return updated


def load_futures_oi():
    """Load Nifty/BankNifty futures OI from kite_historical_indices."""
    for prefix in ["NIFTY_FUT", "BANKNIFTY_FUT"]:
        files = sorted(INDICES_DIR.glob(f"{prefix}*.json"))
        if not files:
            continue

        all_data = {}
        for f in files:
            data = parse_kite_file(f)
            for d in data:
                dt = d["date"][:10]
                all_data[dt] = {"oi": d.get("oi", 0), "close": d.get("close")}

        col = "nifty_futures_oi" if "NIFTY" in prefix else "banknifty_futures_oi"
        engine = create_engine(DB_URL)
        updated = 0
        with engine.begin() as conn:
            for dt, info in sorted(all_data.items()):
                sid = f"CM_{dt}"
                # Read existing deriv_map, merge, write back
                row = conn.execute(text(
                    "SELECT deriv_map FROM market_state WHERE session_id = :sid AND universe_id = 'nifty50'"
                ), {"sid": sid}).first()
                if row:
                    existing = row[0] if isinstance(row[0], dict) else (json.loads(row[0]) if row[0] else {})
                    existing[col] = info["oi"]
                    conn.execute(text("""
                        UPDATE market_state SET deriv_map = CAST(:dm AS jsonb)
                        WHERE session_id = :sid AND universe_id = 'nifty50'
                    """), {"dm": json.dumps(existing), "sid": sid})
                    updated += 1

        engine.dispose()
        logger.info(f"{prefix}: updated {updated} rows")


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("BHARATTWIN DATA LOADER")
    logger.info("=" * 60)

    logger.info("\n1. Loading stock OHLCV data...")
    inserted = load_stock_data()

    logger.info("\n2. Loading repo rates...")
    load_repo_rates()

    logger.info("\n3. Loading USDINR...")
    load_usdinr()

    logger.info("\n4. Loading futures OI...")
    load_futures_oi()

    # Summary
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        bhav = conn.execute(text("SELECT count(*), count(distinct data_date), count(distinct symbol) FROM bhavcopy_raw")).first()
        ms = conn.execute(text("SELECT count(*) FROM market_state WHERE repo_rate_pct IS NOT NULL")).scalar()
        usdinr = conn.execute(text("SELECT count(*) FROM market_state WHERE usdinr_ref IS NOT NULL")).scalar()
    engine.dispose()

    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"bhavcopy_raw: {bhav[0]:,} rows | {bhav[1]} dates | {bhav[2]} stocks")
    logger.info(f"market_state with repo_rate: {ms}")
    logger.info(f"market_state with usdinr: {usdinr}")
