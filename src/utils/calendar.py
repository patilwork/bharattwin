"""Indian trading calendar: holidays, trading days, expiry dates."""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

_SEEDS = Path(__file__).parent.parent.parent / "data" / "seeds"
_HOLIDAYS_FILE = _SEEDS / "nse_holidays.csv"
_EXPIRY_FILE = _SEEDS / "expiry_calendar.csv"


def _load_holidays() -> set[date]:
    holidays: set[date] = set()
    if not _HOLIDAYS_FILE.exists():
        return holidays
    with open(_HOLIDAYS_FILE) as f:
        for row in csv.DictReader(
            (line for line in f if not line.lstrip().startswith("#"))
        ):
            holidays.add(date.fromisoformat(row["date"]))
    return holidays


def _load_expiries() -> dict[tuple[int, int, str], date]:
    expiries: dict[tuple[int, int, str], date] = {}
    if not _EXPIRY_FILE.exists():
        return expiries
    with open(_EXPIRY_FILE) as f:
        for row in csv.DictReader(f):
            if row["expiry_type"] == "monthly":
                key = (int(row["year"]), int(row["month"]), row["underlying"])
                expiries[key] = date.fromisoformat(row["expiry_date"])
    return expiries


_HOLIDAYS: set[date] | None = None
_EXPIRIES: dict[tuple[int, int, str], date] | None = None


def _holidays() -> set[date]:
    global _HOLIDAYS
    if _HOLIDAYS is None:
        _HOLIDAYS = _load_holidays()
    return _HOLIDAYS


def _expiries() -> dict[tuple[int, int, str], date]:
    global _EXPIRIES
    if _EXPIRIES is None:
        _EXPIRIES = _load_expiries()
    return _EXPIRIES


def is_trading_day(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    return d not in _holidays()


def next_trading_day(d: date) -> date:
    candidate = d + timedelta(days=1)
    while not is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def prev_trading_day(d: date) -> date:
    candidate = d - timedelta(days=1)
    while not is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def expiry_date(year: int, month: int, underlying: str = "NIFTY") -> date | None:
    key = (year, month, underlying.upper())
    if key in _expiries():
        return _expiries()[key]
    return _last_weekday_of_month(year, month, weekday=3)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    delta = (last_day.weekday() - weekday) % 7
    candidate = last_day - timedelta(days=delta)
    while not is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def weekly_expiry_dates(year: int, underlying: str = "NIFTY") -> list[date]:
    weekday_map = {"NIFTY": 3, "BANKNIFTY": 2, "FINNIFTY": 1}
    weekday = weekday_map.get(underlying.upper(), 3)
    result = []
    d = date(year, 1, 1)
    end = date(year, 12, 31)
    while d.weekday() != weekday:
        d += timedelta(days=1)
    while d <= end:
        expiry = d
        while not is_trading_day(expiry):
            expiry -= timedelta(days=1)
        result.append(expiry)
        d += timedelta(weeks=1)
    return result


def trading_days_between(start: date, end: date) -> list[date]:
    result = []
    d = start
    while d <= end:
        if is_trading_day(d):
            result.append(d)
        d += timedelta(days=1)
    return result
