"""
NSE FII/DII Daily Flow Ingester.

Source: NSE FII/DII trading activity report
URL: https://www.nseindia.com/api/fiidiiTradeReact?type=fiiDii&date=DD-MMM-YYYY

Fields: date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net (cash segment)

IMPORTANT: All FII data is PROVISIONAL.
Per NSE methodology: FII flows compiled from PANs via NSDL; subject to custodial
confirmation and revision. Always mark as provisional=True on insert.

Known issues:
  - The API endpoint changes periodically; fallback to reports page CSV
  - Dates use DD-MMM-YYYY format (e.g. 02-May-2022)
  - Data available after ~19:00 IST on trading days
  - Weekends/holidays return empty or error — check before fetching
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

import pandas as pd

from src.ingestion.base import BaseIngester, DQResult
from src.stores import raw_lake
from src.utils.calendar import is_trading_day

logger = logging.getLogger(__name__)

_API_URL = "https://www.nseindia.com/api/fiidiiTradeReact?type=fiiDii&date={date_str}"
_SESSION_URL = "https://www.nseindia.com"
# Referrer NSE requires for API calls
_REFERER = "https://www.nseindia.com/reports/fii-dii"


class FiiDiiIngester(BaseIngester):
    """NSE FII/DII daily flow ingester (cash segment)."""

    source_id = "nse/fii_dii"
    rate_limit_secs = 2.0

    def _warm_session(self) -> None:
        try:
            client = self._get_client()
            client.get(_SESSION_URL, timeout=15)
            # Hit the reports page to set referrer cookies
            client.get(_REFERER, timeout=15)
            logger.debug("fii_dii: session warmed")
        except Exception as e:
            logger.warning("fii_dii: session warm-up failed (non-fatal): %s", e)

    def fetch(self, date_: date) -> bytes:
        if not is_trading_day(date_):
            raise ValueError(f"fii_dii: {date_} is not a trading day")

        filename = f"fii_dii_{date_.strftime('%Y%m%d')}.json"
        cached = raw_lake.get_path(self.source_id, date_, filename)
        if cached:
            logger.info("fii_dii: serving from cache %s", cached)
            return cached.read_bytes()

        self._warm_session()

        # NSE API uses DD-MMM-YYYY format
        date_str = date_.strftime("%d-%b-%Y")  # e.g. "02-May-2022"
        url = _API_URL.format(date_str=date_str)

        client = self._get_client()
        client.headers.update({"Referer": _REFERER})

        resp = self._get(url, retries=3)
        raw = resp.content

        raw_lake.store(self.source_id, date_, filename, raw)
        logger.info("fii_dii: stored %d bytes", len(raw))
        return raw

    def parse(self, raw: bytes, date_: date) -> pd.DataFrame:
        data = json.loads(raw)

        # NSE API returns a list of dicts with keys like:
        # date, buyValue, sellValue, netValue, category (FII/DII)
        # Structure varies — handle both list and dict responses
        if isinstance(data, dict):
            rows = data.get("data", data.get("fiidiiData", [data]))
        elif isinstance(data, list):
            rows = data
        else:
            raise ValueError(f"fii_dii: unexpected response structure: {type(data)}")

        records = []
        for row in rows:
            # Normalise keys (different API versions use different key names)
            category = (
                row.get("category") or row.get("type") or row.get("investor", "")
            ).strip().upper()

            # Normalise: "FII/FPI" → "FII", "FPI" → "FII"
            if "FII" in category or "FPI" in category:
                participant = "FII"
            elif "DII" in category:
                participant = "DII"
            else:
                continue

            def _num(key_candidates: list[str]) -> float | None:
                for k in key_candidates:
                    v = row.get(k)
                    if v is not None:
                        try:
                            return float(str(v).replace(",", ""))
                        except (ValueError, TypeError):
                            pass
                return None

            buy = _num(["buyValue", "buy_value", "BUY", "grossPurchase"])
            sell = _num(["sellValue", "sell_value", "SELL", "grossSales"])
            net = _num(["netValue", "net_value", "NET", "netPurchaseSales"])

            # Derive net if missing
            if net is None and buy is not None and sell is not None:
                net = buy - sell

            records.append({
                "date": date_,
                "participant": participant,
                "segment": "cash",
                "buy_crore": buy,
                "sell_crore": sell,
                "net_crore": net,
                "is_provisional": True,   # Always provisional per NSE methodology
                "source_ref": _API_URL,
                "raw_response": json.dumps(row),
            })

        return pd.DataFrame(records)

    def validate(self, parsed: pd.DataFrame, date_: date) -> DQResult:
        dq = DQResult(passed=True, row_count=len(parsed))
        checks: dict[str, bool] = {}

        # 1. We expect exactly 2 rows: one FII, one DII
        checks["expected_participants"] = len(parsed) == 2
        if not checks["expected_participants"]:
            dq.warnings.append(
                f"Expected 2 rows (FII + DII), got {len(parsed)}"
            )

        # 2. No null net values
        if "net_crore" in parsed.columns:
            null_nets = parsed["net_crore"].isna().sum()
            checks["no_null_net"] = null_nets == 0
            if null_nets > 0:
                dq.errors.append(f"{null_nets} null net_crore values")

        # 3. net = buy - sell (within rounding tolerance)
        if all(c in parsed.columns for c in ["buy_crore", "sell_crore", "net_crore"]):
            derived = parsed["buy_crore"] - parsed["sell_crore"]
            diff = (derived - parsed["net_crore"]).abs()
            checks["net_reconciles"] = (diff < 1.0).all()  # within Rs 1 crore
            if not checks["net_reconciles"]:
                dq.warnings.append("net_crore does not reconcile with buy - sell")

        # 4. No future dates
        checks["no_future_date"] = date_ <= date.today()
        if not checks["no_future_date"]:
            dq.errors.append(f"Date {date_} is in the future")

        # 5. Provisional flag always set
        if "is_provisional" in parsed.columns:
            checks["all_provisional"] = parsed["is_provisional"].all()

        dq.checks = checks
        dq.passed = len(dq.errors) == 0
        return dq

    def store_db(self, parsed: pd.DataFrame, date_: date) -> int:
        from sqlalchemy import create_engine, text
        import os

        db_url = os.environ.get(
            "DATABASE_URL",
            "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"
        )
        engine = create_engine(db_url)

        rows = parsed.where(pd.notnull(parsed), None).to_dict(orient="records")
        inserted = 0
        with engine.begin() as conn:
            for row in rows:
                result = conn.execute(text("""
                    INSERT INTO flow_store
                        (date, participant, segment, buy_crore, sell_crore,
                         net_crore, is_provisional, source_ref)
                    VALUES
                        (:date, :participant, :segment, :buy_crore, :sell_crore,
                         :net_crore, :is_provisional, :source_ref)
                    ON CONFLICT (date, participant, segment) DO UPDATE SET
                        buy_crore     = EXCLUDED.buy_crore,
                        sell_crore    = EXCLUDED.sell_crore,
                        net_crore     = EXCLUDED.net_crore,
                        is_provisional = EXCLUDED.is_provisional,
                        source_ref    = EXCLUDED.source_ref
                """), {k: v for k, v in row.items() if k != "raw_response"})
                inserted += result.rowcount

        engine.dispose()
        return inserted
