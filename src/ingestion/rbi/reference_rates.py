"""
RBI Reference Rate Ingester (USDINR, EURINR, GBPINR, JPYINR).

Source: RBI Reference Rate Archive
URL: https://www.rbi.org.in/scripts/referenceratearchive.aspx

Published: ~12:30 IST on each business day (FBIL methodology).
replay_cutoff_ts: 13:00 IST on the publication date.

Technical notes:
  - ASP.NET form — requires POST with __VIEWSTATE for date range queries
  - Single-date fetch: can use GET with query param ?date=DD/MMM/YYYY
  - Response: HTML table with currency pairs and rates
  - RBI site is stable but slow; timeout 30s is usually sufficient
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any

import pandas as pd

from src.ingestion.base import BaseIngester, DQResult
from src.stores import raw_lake
from src.utils.calendar import is_trading_day
from src.utils.time_utils import IST, rbi_fx_cutoff

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.rbi.org.in/scripts/referenceratearchive.aspx"
# Reasonable FX bounds for sanity checks (INR per foreign unit)
_FX_BOUNDS = {
    "USDINR": (60.0, 100.0),
    "EURINR": (65.0, 115.0),
    "GBPINR": (70.0, 130.0),
    "JPYINR": (0.40, 0.90),
}


class RbiReferenceRateIngester(BaseIngester):
    """RBI daily FX reference rate ingester."""

    source_id = "rbi/reference_rates"
    rate_limit_secs = 3.0   # RBI site is slow; be gentle

    def fetch(self, date_: date) -> bytes:
        if not is_trading_day(date_):
            raise ValueError(f"rbi_rates: {date_} is not a trading day")

        filename = f"rbi_ref_rates_{date_.strftime('%Y%m%d')}.html"
        cached = raw_lake.get_path(self.source_id, date_, filename)
        if cached:
            logger.info("rbi_rates: serving from cache %s", cached)
            return cached.read_bytes()

        # RBI archive GET endpoint: date in DD/MMM/YYYY format
        date_str = date_.strftime("%d/%b/%Y")  # e.g. "02/May/2022"
        url = f"{_BASE_URL}?date={date_str}"

        resp = self._get(url, retries=3)
        raw = resp.content

        raw_lake.store(self.source_id, date_, filename, raw)
        logger.info("rbi_rates: stored %d bytes", len(raw))
        return raw

    def parse(self, raw: bytes, date_: date) -> pd.DataFrame:
        """Extract FX rates from RBI HTML table."""
        try:
            from lxml import html as lxml_html
            tree = lxml_html.fromstring(raw)
            tables = tree.xpath("//table")
        except ImportError:
            # Fallback: pandas read_html
            tables = None

        records = []

        if tables is not None:
            # Find the rates table — usually has "Currency" or "SDR" in header
            for table in tables:
                rows = table.xpath(".//tr")
                for row in rows:
                    cells = [c.text_content().strip() for c in row.xpath(".//td|.//th")]
                    if len(cells) < 2:
                        continue
                    text = " ".join(cells).upper()
                    if "USD" in text or "EUR" in text or "GBP" in text or "JPY" in text:
                        records = self._parse_table_rows(rows, date_)
                        if records:
                            break
                if records:
                    break
        else:
            # Regex fallback for plain text scraping
            records = self._regex_parse(raw.decode("utf-8", errors="replace"), date_)

        if not records:
            # Try pandas read_html as last resort
            try:
                dfs = pd.read_html(raw)
                for df in dfs:
                    parsed = self._parse_pandas_table(df, date_)
                    if parsed:
                        records = parsed
                        break
            except Exception:
                pass

        return pd.DataFrame(records)

    def _parse_table_rows(self, rows: list[Any], date_: date) -> list[dict]:
        """Parse lxml table rows into FX records."""
        currency_map = {
            "USD": "USDINR", "US DOLLAR": "USDINR",
            "EUR": "EURINR", "EURO": "EURINR",
            "GBP": "GBPINR", "POUND": "GBPINR", "STERLING": "GBPINR",
            "JPY": "JPYINR", "YEN": "JPYINR", "JAPANESE": "JPYINR",
        }
        records = []
        for row in rows:
            cells = [c.text_content().strip() for c in row.xpath(".//td")]
            if len(cells) < 2:
                continue
            label = cells[0].upper()
            pair = None
            for key, val in currency_map.items():
                if key in label:
                    pair = val
                    break
            if pair is None:
                continue
            # Extract numeric rate from remaining cells
            for cell in cells[1:]:
                cleaned = re.sub(r"[^\d.]", "", cell)
                if cleaned:
                    try:
                        rate = float(cleaned)
                        if 0.1 < rate < 200:  # sanity
                            records.append(self._make_record(pair, rate, date_))
                            break
                    except ValueError:
                        pass
        return records

    def _regex_parse(self, text: str, date_: date) -> list[dict]:
        """Regex fallback parser for plain text."""
        patterns = [
            (r"USD.*?(\d{2,3}\.\d{2,4})", "USDINR"),
            (r"EUR.*?(\d{2,3}\.\d{2,4})", "EURINR"),
            (r"GBP.*?(\d{2,3}\.\d{2,4})", "GBPINR"),
            (r"JPY.*?(\d\.\d{4})", "JPYINR"),
        ]
        records = []
        for pattern, pair in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                try:
                    rate = float(m.group(1))
                    records.append(self._make_record(pair, rate, date_))
                except ValueError:
                    pass
        return records

    def _parse_pandas_table(self, df: pd.DataFrame, date_: date) -> list[dict]:
        records = []
        pair_map = {"USD": "USDINR", "EUR": "EURINR", "GBP": "GBPINR", "JPY": "JPYINR"}
        for _, row in df.iterrows():
            row_str = " ".join(str(v) for v in row.values).upper()
            for key, pair in pair_map.items():
                if key in row_str:
                    for val in row.values:
                        try:
                            rate = float(str(val).replace(",", ""))
                            if 0.1 < rate < 200:
                                records.append(self._make_record(pair, rate, date_))
                                break
                        except (ValueError, TypeError):
                            pass
                    break
        return records

    def _make_record(self, pair: str, rate: float, date_: date) -> dict:
        return {
            "date": date_,
            "currency_pair": pair,
            "rate": rate,
            "source": "RBI Reference Rate",
            "publication_time": rbi_fx_cutoff(date_).isoformat(),
            "replay_cutoff_ts": rbi_fx_cutoff(date_).isoformat(),
            "methodology": "FBIL — windowed interbank quotes, outlier-filtered",
        }

    def validate(self, parsed: pd.DataFrame, date_: date) -> DQResult:
        dq = DQResult(passed=True, row_count=len(parsed))
        checks: dict[str, bool] = {}

        # 1. At least USD/INR present
        if "currency_pair" in parsed.columns:
            pairs = set(parsed["currency_pair"].tolist())
            checks["usdinr_present"] = "USDINR" in pairs
            if not checks["usdinr_present"]:
                dq.errors.append("USDINR not found in response")
        else:
            checks["has_data"] = len(parsed) > 0
            if not checks["has_data"]:
                dq.errors.append("No rates parsed")

        # 2. Rates within reasonable bounds
        if "currency_pair" in parsed.columns and "rate" in parsed.columns:
            checks["rates_in_bounds"] = True
            for _, row in parsed.iterrows():
                pair = row["currency_pair"]
                rate = row["rate"]
                lo, hi = _FX_BOUNDS.get(pair, (0.01, 1000))
                if not (lo <= rate <= hi):
                    checks["rates_in_bounds"] = False
                    dq.errors.append(
                        f"{pair} rate {rate} outside expected range [{lo}, {hi}]"
                    )

        # 3. No future dates
        checks["no_future_date"] = date_ <= date.today()

        dq.checks = checks
        dq.passed = len(dq.errors) == 0
        return dq

    def store_db(self, parsed: pd.DataFrame, date_: date) -> int:
        """Store FX rates into market_state.macro_map JSONB."""
        from sqlalchemy import create_engine, text
        import json
        import os

        db_url = os.environ.get(
            "DATABASE_URL",
            "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"
        )
        engine = create_engine(db_url)

        # Build macro_map patch for the day
        rates = {
            row["currency_pair"]: row["rate"]
            for _, row in parsed.iterrows()
            if "currency_pair" in row and "rate" in row
        }
        macro_patch = {"fx": {k.lower(): v for k, v in rates.items()}}
        replay_cutoff = rbi_fx_cutoff(date_).isoformat()

        with engine.begin() as conn:
            # Upsert: if market_state row exists for this date, merge macro_map
            # Otherwise insert a skeleton row (will be filled by market_state builder)
            result = conn.execute(text("""
                INSERT INTO market_state
                    (asof_ts_ist, session_id, universe_id, macro_map, replay_cutoff_ts)
                VALUES (
                    :ts, :session_id, 'nifty50',
                    :macro_map::jsonb,
                    :replay_cutoff
                )
                ON CONFLICT (universe_id, session_id) DO UPDATE SET
                    macro_map = market_state.macro_map || EXCLUDED.macro_map,
                    replay_cutoff_ts = LEAST(market_state.replay_cutoff_ts, EXCLUDED.replay_cutoff_ts)
            """), {
                "ts": f"{date_}T13:00:00+05:30",
                "session_id": f"CM_{date_}",
                "macro_map": json.dumps(macro_patch),
                "replay_cutoff": replay_cutoff,
            })
            inserted = result.rowcount

        engine.dispose()
        return inserted
