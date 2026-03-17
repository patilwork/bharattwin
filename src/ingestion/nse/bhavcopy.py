"""
NSE Equity Bhavcopy Ingester (EOD).

Downloads the daily bhavcopy ZIP from NSE archives, parses the CSV,
runs DQ checks, saves to raw lake, and inserts into market_state staging.

URL pattern (current as of 2024):
  https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip

Known issues:
  - URL pattern changed in 2023 (old: BhavCopy.zip, new: BhavCopy_NSE_CM_... pattern)
  - NSE archives are less aggressively protected than the live site
  - Session cookie from nseindia.com sometimes required for archives
  - Returns 403 if user-agent looks like a bot without a valid referrer
"""

from __future__ import annotations

import io
import logging
import zipfile
from datetime import date
from typing import Any

import pandas as pd

from src.ingestion.base import BaseIngester, DQResult
from src.stores import raw_lake
from src.utils.calendar import is_trading_day

logger = logging.getLogger(__name__)

# Known URL patterns — newest first
_URL_PATTERNS = [
    # Post-2023 pattern
    "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{date_str}_F_0000.csv.zip",
    # Pre-2023 pattern (may still work for historical backfill)
    "https://nsearchives.nseindia.com/content/cm/bhav/cm{date_dmy}bhav.csv.zip",
]

# Session cookie warm-up URL (NSE main site sets cookies that archives respect)
_SESSION_URL = "https://www.nseindia.com"

# Required columns — we check after mapping, using normalised names
_REQUIRED_COLS_NORM = {"symbol", "series", "close", "volume"}

# Post-2023 NSE bhavcopy format (BhavCopy_NSE_CM_..._F_0000.csv)
_COL_MAP_NEW = {
    "TckrSymb": "symbol",
    "SctySrs": "series",
    "OpnPric": "open",
    "HghPric": "high",
    "LwPric": "low",
    "ClsPric": "close",
    "PrvsClsgPric": "prev_close",
    "TtlTradgVol": "volume",
    "TtlTrfVal": "turnover_lakh",
    "TtlNbOfTxsExctd": "trades",
    "ISIN": "isin",
}

# Pre-2023 NSE bhavcopy format (cmDDMMYYbhav.csv)
_COL_MAP_OLD = {
    "SYMBOL": "symbol",
    "SERIES": "series",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
    "CLOSE": "close",
    "PREVCLOSE": "prev_close",
    "TOTTRDQTY": "volume",
    "TOTTRDVAL": "turnover_lakh",
    "TOTALTRADES": "trades",
    "ISIN": "isin",
}


class BhavCopyIngester(BaseIngester):
    """NSE equity EOD bhavcopy ingester."""

    source_id = "nse/bhavcopy"
    rate_limit_secs = 2.0   # be gentle with NSE

    def _warm_session(self) -> None:
        """Hit NSE main site to get session cookies (reduces 403s on archives)."""
        try:
            client = self._get_client()
            client.get(_SESSION_URL, timeout=15)
            logger.debug("bhavcopy: session warmed")
        except Exception as e:
            logger.warning("bhavcopy: session warm-up failed (non-fatal): %s", e)

    def _build_urls(self, date_: date) -> list[str]:
        date_str = date_.strftime("%Y%m%d")          # 20220502
        date_dmy = date_.strftime("%d%m%y")          # 020522 (old pattern)
        return [p.format(date_str=date_str, date_dmy=date_dmy) for p in _URL_PATTERNS]

    def fetch(self, date_: date) -> bytes:
        if not is_trading_day(date_):
            raise ValueError(f"bhavcopy: {date_} is not a trading day")

        # Check raw lake cache first
        filename = f"bhavcopy_{date_.strftime('%Y%m%d')}.zip"
        cached = raw_lake.get_path(self.source_id, date_, filename)
        if cached:
            logger.info("bhavcopy: serving from cache %s", cached)
            return cached.read_bytes()

        self._warm_session()

        for url in self._build_urls(date_):
            try:
                logger.info("bhavcopy: trying %s", url)
                resp = self._get(url, retries=3)
                raw = resp.content
                # Save to raw lake
                raw_lake.store(self.source_id, date_, filename, raw)
                logger.info("bhavcopy: stored %d bytes to raw lake", len(raw))
                return raw
            except Exception as e:
                logger.warning("bhavcopy: URL failed %s — %s", url, e)
                continue

        raise RuntimeError(f"bhavcopy: all URLs failed for {date_}")

    def parse(self, raw: bytes, date_: date) -> pd.DataFrame:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, dtype=str)

        # Normalise column names (strip whitespace)
        df.columns = [c.strip() for c in df.columns]

        # Auto-detect format: post-2023 uses TckrSymb; pre-2023 uses SYMBOL
        col_map = _COL_MAP_NEW if "TckrSymb" in df.columns else _COL_MAP_OLD
        keep = {c for c in col_map if c in df.columns}
        df = df[list(keep)].rename(columns=col_map)

        # Type conversions
        for col in ["open", "high", "low", "close", "prev_close", "turnover_lakh"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in ["volume", "trades"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        df["data_date"] = date_
        df["source"] = self.source_id

        return df

    def validate(self, parsed: pd.DataFrame, date_: date) -> DQResult:
        dq = DQResult(passed=True, row_count=len(parsed))
        checks: dict[str, bool] = {}

        # 1. Non-empty
        checks["non_empty"] = len(parsed) > 0
        if not checks["non_empty"]:
            dq.errors.append("Empty bhavcopy — zero rows parsed")

        # 2. Required columns present
        checks["required_cols"] = _REQUIRED_COLS_NORM.issubset(set(parsed.columns))
        if not checks["required_cols"]:
            missing = _REQUIRED_COLS_NORM - set(parsed.columns)
            dq.errors.append(f"Missing columns: {missing}")

        # 3. No null closes in EQ series
        if "close" in parsed.columns and "series" in parsed.columns:
            eq = parsed[parsed["series"] == "EQ"]
            null_closes = eq["close"].isna().sum()
            checks["no_null_eq_close"] = null_closes == 0
            if null_closes > 0:
                dq.warnings.append(f"{null_closes} null closes in EQ series")
            dq.null_counts["close_eq"] = int(null_closes)

        # 4. Positive volumes
        if "volume" in parsed.columns:
            zero_vol = (parsed["volume"].fillna(0) == 0).sum()
            checks["mostly_positive_volume"] = zero_vol < len(parsed) * 0.1
            if not checks["mostly_positive_volume"]:
                dq.warnings.append(f"{zero_vol} zero-volume rows (>{10}%)")

        # 5. Reasonable row count (EQ segment typically 1800-2200 rows)
        eq_count = len(parsed[parsed["series"] == "EQ"]) if "series" in parsed.columns else len(parsed)
        checks["reasonable_eq_count"] = 1000 < eq_count < 5000
        if not checks["reasonable_eq_count"]:
            dq.warnings.append(f"Unusual EQ row count: {eq_count} (expected 1800-2200)")

        dq.checks = checks
        dq.passed = len(dq.errors) == 0 and all(checks.values())
        return dq

    def store_db(self, parsed: pd.DataFrame, date_: date) -> int:
        """
        Insert bhavcopy rows into the DB.

        For now inserts into a staging table (bhavcopy_raw).
        Phase 1 will fold this into market_state via the factor engine.
        """
        from sqlalchemy import create_engine, text
        import os

        db_url = os.environ.get(
            "DATABASE_URL",
            "postgresql://bharattwin:devpassword@localhost:5434/bharattwin"
        )
        engine = create_engine(db_url)

        # Ensure staging table exists
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bhavcopy_raw (
                    id          BIGSERIAL PRIMARY KEY,
                    data_date   DATE NOT NULL,
                    symbol      TEXT NOT NULL,
                    series      TEXT,
                    open        NUMERIC(12,2),
                    high        NUMERIC(12,2),
                    low         NUMERIC(12,2),
                    close       NUMERIC(12,2),
                    prev_close  NUMERIC(12,2),
                    volume      BIGINT,
                    turnover_lakh NUMERIC(16,2),
                    trades      BIGINT,
                    isin        TEXT,
                    source      TEXT,
                    ingested_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE UNIQUE INDEX IF NOT EXISTS uq_bhavcopy_date_symbol_series
                    ON bhavcopy_raw(data_date, symbol, series);
            """))

        # Insert with ON CONFLICT DO NOTHING (idempotent)
        rows = parsed.where(pd.notnull(parsed), None).to_dict(orient="records")
        inserted = 0
        with engine.begin() as conn:
            for row in rows:
                result = conn.execute(text("""
                    INSERT INTO bhavcopy_raw
                        (data_date, symbol, series, open, high, low, close,
                         prev_close, volume, turnover_lakh, trades, isin, source)
                    VALUES
                        (:data_date, :symbol, :series, :open, :high, :low, :close,
                         :prev_close, :volume, :turnover_lakh, :trades, :isin, :source)
                    ON CONFLICT (data_date, symbol, series) DO NOTHING
                """), row)
                inserted += result.rowcount

        engine.dispose()
        return inserted
