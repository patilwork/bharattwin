"""
Historical bhavcopy backfill — populate bhavcopy_raw for recent trading days.

Usage:
    python -m src.ingestion.nse.backfill              # last 20 trading days
    python -m src.ingestion.nse.backfill 2025-02-01 2025-03-12
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, timedelta

from sqlalchemy import create_engine, text

from src.ingestion.nse.bhavcopy import BhavCopyIngester
from src.utils.calendar import is_trading_day

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"


def _existing_dates(engine) -> set[date]:
    """Get dates already in bhavcopy_raw."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT DISTINCT data_date FROM bhavcopy_raw")).fetchall()
            return {row[0] for row in rows}
    except Exception:
        return set()


def _recent_trading_days(n: int = 20) -> list[date]:
    """Get the last n trading days before today."""
    days = []
    d = date.today() - timedelta(days=1)
    while len(days) < n and d > date(2023, 1, 1):
        if is_trading_day(d):
            days.append(d)
        d -= timedelta(days=1)
    return sorted(days)


def backfill(start_date: date | None = None, end_date: date | None = None, limit: int = 20) -> dict:
    """
    Backfill bhavcopy data for a date range.

    Args:
        start_date: First date to backfill (default: 20 trading days ago).
        end_date: Last date to backfill (default: yesterday).
        limit: Max number of days to process (safety limit).

    Returns:
        Summary dict with counts of success/skip/fail.
    """
    engine = create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))
    existing = _existing_dates(engine)
    engine.dispose()

    # Build date list
    if start_date and end_date:
        all_dates = []
        d = start_date
        while d <= end_date:
            if is_trading_day(d):
                all_dates.append(d)
            d += timedelta(days=1)
    else:
        all_dates = _recent_trading_days(limit)

    # Apply limit
    all_dates = all_dates[:limit]

    logger.info("backfill: %d trading days to process, %d already in DB",
                len(all_dates), len(existing))

    stats = {"success": 0, "skipped": 0, "failed": 0, "dates_processed": []}

    with BhavCopyIngester() as ingester:
        for d in all_dates:
            if d in existing:
                logger.info("backfill: %s already exists — skipping", d)
                stats["skipped"] += 1
                continue

            try:
                logger.info("backfill: processing %s", d)
                dq = ingester.run(d)
                if dq.passed:
                    stats["success"] += 1
                    stats["dates_processed"].append(str(d))
                    logger.info("backfill: %s OK (%d rows)", d, dq.row_count)
                else:
                    stats["failed"] += 1
                    logger.warning("backfill: %s DQ failed: %s", d, dq.errors)
            except Exception as e:
                stats["failed"] += 1
                logger.error("backfill: %s error: %s", d, e)

            # Rate limit: 3 seconds between requests (NSE-friendly)
            time.sleep(3)

    logger.info("backfill complete: success=%d skipped=%d failed=%d",
                stats["success"], stats["skipped"], stats["failed"])
    return stats


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    if len(sys.argv) >= 3:
        s = date.fromisoformat(sys.argv[1])
        e = date.fromisoformat(sys.argv[2])
        backfill(s, e)
    else:
        backfill()
