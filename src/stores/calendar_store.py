"""
Calendar store population and query utilities.

Populates the calendar_store table from seed CSV files.
One row per calendar date covering the full replay range (2016-01-01 to present+1yr).
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta

from sqlalchemy import create_engine, text

from src.utils.calendar import (
    expiry_date,
    is_trading_day,
    weekly_expiry_dates,
)

logger = logging.getLogger(__name__)

_DEFAULT_DB = "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"

# Full range needed for all 5 case studies + current
_RANGE_START = date(2016, 1, 1)
_RANGE_END = date(2027, 12, 31)


def _get_engine():
    return create_engine(os.environ.get("DATABASE_URL", _DEFAULT_DB))


def populate(start: date = _RANGE_START, end: date = _RANGE_END,
             force: bool = False) -> int:
    """
    Populate calendar_store for all dates in [start, end].
    Skips existing rows unless force=True.
    Returns count of rows inserted.
    """
    engine = _get_engine()

    # Pre-compute weekly expiry sets for performance
    weekly_nifty: set[date] = set()
    weekly_banknifty: set[date] = set()
    for year in range(start.year, end.year + 1):
        weekly_nifty.update(weekly_expiry_dates(year, "NIFTY"))
        weekly_banknifty.update(weekly_expiry_dates(year, "BANKNIFTY"))

    inserted = 0
    d = start
    rows = []

    while d <= end:
        trading = is_trading_day(d)

        # Monthly expiry flags
        nifty_mo = expiry_date(d.year, d.month, "NIFTY") == d
        bnifty_mo = expiry_date(d.year, d.month, "BANKNIFTY") == d
        finnifty_mo = expiry_date(d.year, d.month, "FINNIFTY") == d

        rows.append({
            "date": d,
            "is_trading_day": trading,
            "holiday_name": None,  # populated by seed CSV loader separately
            "exchange": "NSE",
            "is_nifty_monthly_expiry": nifty_mo,
            "is_banknifty_monthly_expiry": bnifty_mo,
            "is_finnifty_monthly_expiry": finnifty_mo,
            "is_nifty_weekly_expiry": d in weekly_nifty,
            "is_banknifty_weekly_expiry": d in weekly_banknifty,
        })
        d += timedelta(days=1)

    # Batch insert
    with engine.begin() as conn:
        for row in rows:
            conflict = "DO NOTHING" if not force else """DO UPDATE SET
                is_trading_day = EXCLUDED.is_trading_day,
                is_nifty_monthly_expiry = EXCLUDED.is_nifty_monthly_expiry,
                is_banknifty_monthly_expiry = EXCLUDED.is_banknifty_monthly_expiry,
                is_finnifty_monthly_expiry = EXCLUDED.is_finnifty_monthly_expiry,
                is_nifty_weekly_expiry = EXCLUDED.is_nifty_weekly_expiry,
                is_banknifty_weekly_expiry = EXCLUDED.is_banknifty_weekly_expiry"""

            result = conn.execute(text(f"""
                INSERT INTO calendar_store (
                    date, is_trading_day, exchange,
                    is_nifty_monthly_expiry, is_banknifty_monthly_expiry,
                    is_finnifty_monthly_expiry,
                    is_nifty_weekly_expiry, is_banknifty_weekly_expiry
                ) VALUES (
                    :date, :is_trading_day, :exchange,
                    :is_nifty_monthly_expiry, :is_banknifty_monthly_expiry,
                    :is_finnifty_monthly_expiry,
                    :is_nifty_weekly_expiry, :is_banknifty_weekly_expiry
                )
                ON CONFLICT (date) {conflict}
            """), row)
            inserted += result.rowcount

    engine.dispose()
    logger.info("calendar_store: inserted/updated %d rows (%s to %s)", inserted, start, end)
    return inserted


def get(d: date) -> dict | None:
    """Fetch a single calendar_store row for date d."""
    engine = _get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM calendar_store WHERE date = :d"), {"d": d}
        ).mappings().first()
    engine.dispose()
    return dict(row) if row else None


def verify_case_study_dates() -> dict[str, dict]:
    """
    Quick sanity check for all 5 case study T-1 dates.
    Returns dict of date → calendar_store row.
    """
    case_dates = {
        "demonetisation_t1": date(2016, 11, 8),    # Nov 9 was the event
        "covid_crash_t1": date(2020, 3, 20),        # Mar 23 circuit breaker
        "ukraine_t1": date(2022, 2, 23),            # Feb 24 invasion
        "rbi_hike_t1": date(2022, 5, 2),            # May 4 hike (May 3 = Eid)
        "election_t1": date(2024, 6, 3),            # Jun 4 result day
    }
    results = {}
    for label, d in case_dates.items():
        row = get(d)
        results[label] = row or {"date": d, "error": "not found"}
    return results
