"""
Abstract base class for all ingestion pipelines.

Every ingester must implement:
  fetch(date)  → raw bytes
  parse(raw)   → DataFrame
  validate(df) → DQResult
  store(df, date)

Anti-scraping defaults (NSE-friendly):
  - 1 req/sec max
  - Browser-like headers with session cookie support
  - Exponential backoff on 403/429/503
"""

from __future__ import annotations

import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Browser-like headers NSE expects
_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


@dataclass
class DQResult:
    passed: bool
    checks: dict[str, bool] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    row_count: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (
            f"DQ {status} | rows={self.row_count} | "
            f"checks={sum(self.checks.values())}/{len(self.checks)} passed | "
            f"errors={len(self.errors)} warnings={len(self.warnings)}"
        )


class BaseIngester(ABC):
    """Abstract base for all BharatTwin data ingesters."""

    #: Override in subclass — used for raw lake path and logging
    source_id: str = "unknown"
    #: Seconds between requests (respect NSE rate limits)
    rate_limit_secs: float = 1.0

    def __init__(self) -> None:
        self._client: httpx.Client | None = None
        self._last_request_ts: float = 0.0

    def _get_client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(
                headers=_DEFAULT_HEADERS,
                follow_redirects=True,
                timeout=30.0,
            )
        return self._client

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < self.rate_limit_secs:
            time.sleep(self.rate_limit_secs - elapsed)

    def _get(self, url: str, retries: int = 3, **kwargs: Any) -> httpx.Response:
        """GET with rate limiting and exponential backoff on 4xx/5xx."""
        self._rate_limit()
        client = self._get_client()
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                resp = client.get(url, **kwargs)
                self._last_request_ts = time.monotonic()
                if resp.status_code in (403, 429, 503):
                    wait = 2 ** attempt * 5
                    logger.warning(
                        "%s: HTTP %d from %s — backoff %ds",
                        self.source_id, resp.status_code, url, wait,
                    )
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except httpx.HTTPError as e:
                last_exc = e
                wait = 2 ** attempt * 3
                logger.warning("%s: request error %s — retry in %ds", self.source_id, e, wait)
                time.sleep(wait)
        raise RuntimeError(
            f"{self.source_id}: all {retries} retries failed for {url}"
        ) from last_exc

    @abstractmethod
    def fetch(self, date_: date) -> bytes:
        """Download raw data for the given date. Returns raw bytes."""

    @abstractmethod
    def parse(self, raw: bytes, date_: date) -> Any:
        """Parse raw bytes into a DataFrame or structured object."""

    @abstractmethod
    def validate(self, parsed: Any, date_: date) -> DQResult:
        """Run data quality checks. Return DQResult."""

    @abstractmethod
    def store_db(self, parsed: Any, date_: date) -> int:
        """Persist parsed data to the database. Return row count inserted."""

    def run(self, date_: date, force: bool = False) -> DQResult:
        """
        Full pipeline: fetch → parse → validate → store.
        Skips if already ingested unless force=True.
        """
        from src.stores import raw_lake

        logger.info("%s: starting ingestion for %s", self.source_id, date_)

        raw = self.fetch(date_)
        logger.info("%s: fetched %d bytes", self.source_id, len(raw))

        parsed = self.parse(raw, date_)
        dq = self.validate(parsed, date_)
        logger.info("%s: %s", self.source_id, dq)

        if not dq.passed:
            logger.error("%s: DQ FAILED — skipping DB store. Errors: %s",
                         self.source_id, dq.errors)
            return dq

        rows = self.store_db(parsed, date_)
        logger.info("%s: stored %d rows to DB", self.source_id, rows)
        return dq

    def close(self) -> None:
        if self._client and not self._client.is_closed:
            self._client.close()

    def __enter__(self) -> "BaseIngester":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
