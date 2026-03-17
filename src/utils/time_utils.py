"""IST time normalization and market hours utilities."""

from datetime import date, datetime, time
import pytz

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc

_MARKET_OPEN = time(9, 15)
_MARKET_CLOSE = time(15, 30)


def to_ist(dt: datetime) -> datetime:
    """Convert any tz-aware datetime to IST. Naive datetimes assumed UTC."""
    if dt.tzinfo is None:
        dt = UTC.localize(dt)
    return dt.astimezone(IST)


def ist_now() -> datetime:
    return datetime.now(IST)


def ist_midnight(d: date) -> datetime:
    return IST.localize(datetime(d.year, d.month, d.day, 0, 0, 0))


def market_open_close(d: date) -> tuple[datetime, datetime]:
    open_dt = IST.localize(datetime(d.year, d.month, d.day, 9, 15, 0))
    close_dt = IST.localize(datetime(d.year, d.month, d.day, 15, 30, 0))
    return open_dt, close_dt


def is_market_hours(dt: datetime) -> bool:
    dt_ist = to_ist(dt)
    t = dt_ist.time()
    return _MARKET_OPEN <= t <= _MARKET_CLOSE


def eod_cutoff(d: date) -> datetime:
    """EOD bhavcopy available ~18:00 IST."""
    return IST.localize(datetime(d.year, d.month, d.day, 18, 0, 0))


def rbi_fx_cutoff(d: date) -> datetime:
    """RBI reference rates published ~12:30 IST; use 13:00 as cutoff."""
    return IST.localize(datetime(d.year, d.month, d.day, 13, 0, 0))


def format_ist(dt: datetime) -> str:
    return to_ist(dt).isoformat()
